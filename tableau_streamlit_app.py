import os
import json
import io
from pathlib import Path
import time
import uuid
from datetime import datetime
import sqlite3
import base64

# Third-party imports
import streamlit as st
import pandas as pd
import tableauserverclient as TSC
import plotly.graph_objects as go
from streamlit.runtime.scriptrunner import get_script_run_ctx

# Local imports
from user_management import UserManagement
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from report_formatter_new import ReportFormatter
from report_manager_new import ReportManager

# ReportLab imports
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import TableStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

def get_session():
    """Get the current session state"""
    ctx = get_script_run_ctx()
    if ctx is None:
        return None
    return ctx.session_id

def init_session_state():
    """Initialize session state variables if they don't exist"""
    session_id = get_session()
    
    # Fix superadmin permissions if needed
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE users 
                SET permission_type = 'superadmin', 
                    role = 'superadmin'
                WHERE username = 'superadmin'
            """)
            conn.commit()
    except Exception as e:
        print(f"Error fixing superadmin permissions: {str(e)}")
    
    # Try to load persisted state
    try:
        if os.path.exists(f'.streamlit/session_{session_id}.json'):
            with open(f'.streamlit/session_{session_id}.json', 'r') as f:
                persisted_state = json.load(f)
                for key, value in persisted_state.items():
                    if key not in st.session_state:
                        st.session_state[key] = value
                return  # If we successfully loaded state, return early
    except Exception as e:
        print(f"Error loading session state: {str(e)}")
    
    # Only set these if we couldn't load from file
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        if 'user' not in st.session_state:
            st.session_state.user = None
        if 'connector' not in st.session_state:
            st.session_state.connector = None
    if 'workbooks' not in st.session_state:
        st.session_state.workbooks = None
    if 'views' not in st.session_state:
        st.session_state.views = None
    if 'selected_workbook' not in st.session_state:
        st.session_state.selected_workbook = None
    if 'downloaded_data' not in st.session_state:
        st.session_state.downloaded_data = None
    if 'show_dashboard_page' not in st.session_state:
        st.session_state.show_dashboard_page = False
    if 'show_qa_page' not in st.session_state:
        st.session_state.show_qa_page = False
    if 'show_schedule_page' not in st.session_state:
        st.session_state.show_schedule_page = False
    if 'show_modify_schedule' not in st.session_state:
        st.session_state.show_modify_schedule = False
    if 'modifying_schedule' not in st.session_state:
        st.session_state.modifying_schedule = None
    if 'current_dataset' not in st.session_state:
        st.session_state.current_dataset = None
    if 'current_dashboard_id' not in st.session_state:
        st.session_state.current_dashboard_id = None 

def clear_session():
    """Clear all session state variables"""
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.authenticated = False
    st.session_state.user = None

def get_saved_datasets():
    """Get list of saved datasets from the database, excluding internal tables"""
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            cursor = conn.cursor()
            # Get list of all tables except system and internal tables
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name NOT IN ('users', 'organizations', 'schedules', 'sqlite_sequence')
                AND name NOT LIKE 'sqlite_%'
                AND name NOT LIKE '_internal_%'
            """)
            datasets = [row[0] for row in cursor.fetchall()]
            print(f"Found user datasets: {datasets}")  # Debug print
            return datasets
    except Exception as e:
        print(f"Error getting datasets: {str(e)}")
        return []

def load_dataset(dataset_name):
    """Load dataset from database"""
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            # Read the entire table into a DataFrame
            df = pd.read_sql_query(f"SELECT * FROM '{dataset_name}'", conn)
            print(f"Loaded dataset {dataset_name} with {len(df)} rows")  # Debug print
            return df
    except Exception as e:
        print(f"Error loading dataset: {str(e)}")
        return None

def delete_dataset(dataset_name):
    """Delete dataset from database"""
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            cursor = conn.cursor()
            # Drop the table
            cursor.execute(f"DROP TABLE IF EXISTS '{dataset_name}'")
            conn.commit()
            print(f"Deleted dataset: {dataset_name}")  # Debug print
            return True
    except Exception as e:
        print(f"Error deleting dataset: {str(e)}")
        return False

def show_login_page():
    """Show login page"""
    st.title("Welcome to Tableau Data Reporter")
    
    # Initialize user manager
    user_manager = UserManagement()
    
    tab1, tab2 = st.tabs(["Login", "Register"])
    
    with tab1:
        st.subheader("Login")
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        
        if st.button("Login"):
            user = user_manager.verify_user(username, password)
            if user:
                # Store user data in session state
                st.session_state.user = {
                    'id': user[0],
                    'username': user[1],
                    'role': user[2],
                    'permission_type': user[3],
                    'organization_id': user[4],
                    'organization_name': user[5]
                }
                st.session_state.authenticated = True
                st.success("Login successful!")
                time.sleep(1)  # Give time for success message to show
                st.rerun()
            else:
                st.error("Invalid credentials")

    with tab2:
        st.subheader("Register")
        new_username = st.text_input("Username", key="reg_username")
        new_email = st.text_input("Email", key="reg_email")
        new_password = st.text_input("Password", type="password", key="reg_password")
        confirm_password = st.text_input("Confirm Password", type="password")
        
        if st.button("Register"):
            if new_password != confirm_password:
                st.error("Passwords don't match")
            elif not all([new_username, new_password, new_email]):
                st.error("All fields are required")
            else:
                try:
                    # Create user with normal permissions
                    if user_manager.add_user_to_org(
                        username=new_username,
                        password=new_password,
                        org_id=None,
                        permission_type='normal',
                        email=new_email
                    ):
                        st.success("Registration successful! Please login.")
                except ValueError as e:
                    st.error(str(e))

def show_normal_user_page():
    """Show interface for normal users"""
    st.title("Tableau Data Reporter")
    
    # Show user info in sidebar
    with st.sidebar:
        st.title("👤 User Profile")
        st.write(f"**Username:** {st.session_state.user['username']}")
        st.write(f"**Role:** {st.session_state.user['role']}")
        st.write(f"**Organization:** {st.session_state.user['organization_name'] or 'Not assigned'}")
        
        st.markdown("---")
        
        # Navigation buttons
        if st.button("🔌 Connect to Tableau", key="normal_connect_tableau", use_container_width=True):
            st.session_state.show_tableau_page = True
            st.session_state.show_schedule_page = False
            st.rerun()
            
        if st.button("📅 Schedule Reports", key="normal_schedule_reports", use_container_width=True):
            st.session_state.show_schedule_page = True
            st.session_state.show_tableau_page = False
            st.rerun()
        
        st.markdown("---")
        if st.button("🚪 Logout", key="normal_user_logout", use_container_width=True):
            clear_session()
            st.rerun()
    
    # Main content area
    if st.session_state.get('show_schedule_page'):
        show_schedule_page()
    elif st.session_state.get('show_tableau_page'):
        show_tableau_page()
    else:
        # Default view - show available datasets
        show_saved_datasets('normal')

def show_power_user_page():
    """Show interface for power users with additional features"""
    st.title("Tableau Data Reporter")
    
    # Show user info in sidebar
    with st.sidebar:
        st.title("👤 User Profile")
        st.write(f"**Username:** {st.session_state.user['username']}")
        st.write(f"**Role:** {st.session_state.user['role']}")
        st.write(f"**Organization:** {st.session_state.user['organization_name'] or 'Not assigned'}")
        
        st.markdown("---")
        
        # Navigation buttons
        if st.button("🔌 Connect to Tableau", key="power_connect_tableau", use_container_width=True):
            st.session_state.show_tableau_page = True
            st.session_state.show_qa_page = False
            st.session_state.show_schedule_page = False
            st.rerun()
            
        if st.button("💬 Chat with Data", key="power_chat_data", use_container_width=True):
            st.session_state.show_qa_page = True
            st.session_state.show_tableau_page = False
            st.session_state.show_schedule_page = False
            st.rerun()
            
        if st.button("📅 Schedule Reports", key="power_schedule_reports", use_container_width=True):
            st.session_state.show_schedule_page = True
            st.session_state.show_tableau_page = False
            st.session_state.show_qa_page = False
            st.rerun()
        
        st.markdown("---")
        if st.button("🚪 Logout", key="power_user_logout", use_container_width=True):
            clear_session()
            st.rerun()
    
    # Main content area
    if st.session_state.get('show_qa_page'):
        show_qa_page()
    elif st.session_state.get('show_schedule_page'):
        show_schedule_page()
    elif st.session_state.get('show_tableau_page'):
        show_tableau_page()
    else:
        # Default view - show available datasets
        show_saved_datasets('power')

def show_user_dashboard():
    """Show user dashboard (only for superadmin)"""
    st.title("User Dashboard")
    
    if st.session_state.user['role'] != 'superadmin':
        st.error("Access denied")
        return
    
    # Show user info in sidebar
    with st.sidebar:
        st.title("👤 Admin Profile")
        st.write(f"**Username:** {st.session_state.user['username']}")
        st.write(f"**Role:** {st.session_state.user['role']}")
        
        st.markdown("---")
        
        if st.button("🚪 Logout", key="admin_dashboard_logout", use_container_width=True):
            clear_session()
            st.rerun()
    
    # Main content area
    tabs = st.tabs(["Users", "Organizations", "System"])
    
    user_manager = UserManagement()
    
    with tabs[0]:
        st.header("👥 User Management")
        users = user_manager.get_all_users()
        if users:
            st.subheader(f"Total Users: {len(users)}")
            for user in users:
                username, role, permission_type, email, org_name = user
                with st.expander(f"👤 {username}", expanded=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Email:** {email or 'Not set'}")
                        st.write(f"**Organization:** {org_name or 'Not assigned'}")
                    with col2:
                        st.write(f"**Role:** {role}")
                        new_permission = st.selectbox(
                            "Permission Type",
                            options=['normal', 'power', 'superadmin'],
                            index=['normal', 'power', 'superadmin'].index(permission_type),
                            key=f"perm_{username}"
                        )
                        if new_permission != permission_type:
                            if st.button("Update Permission", key=f"update_{username}"):
                                if user_manager.update_user_permission(username, new_permission):
                                    st.success(f"Updated {username}'s permission to {new_permission}")
                                    st.rerun()
        else:
            st.info("No users found")
    
    with tabs[1]:
        st.header("🏢 Organization Management")
        
        # Add organization
        with st.expander("➕ Add New Organization", expanded=False):
            org_name = st.text_input("Organization Name", key="new_org_name")
            org_description = st.text_area("Description", key="new_org_desc")
            if st.button("Create Organization", key="create_org_btn"):
                try:
                    with sqlite3.connect('data/tableau_data.db') as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "INSERT INTO organizations (name, description) VALUES (?, ?)",
                            (org_name, org_description)
                        )
                        conn.commit()
                        st.success(f"Organization '{org_name}' created successfully!")
                        time.sleep(1)
                        st.rerun()
                except sqlite3.IntegrityError:
                    st.error(f"Organization '{org_name}' already exists")
                except Exception as e:
                    st.error(f"Failed to create organization: {str(e)}")
        
        # List existing organizations
        st.subheader("Existing Organizations")
        try:
            with sqlite3.connect('data/tableau_data.db') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT rowid, name, description FROM organizations ORDER BY name")
                organizations = cursor.fetchall()
                
                if organizations:
                    for org in organizations:
                        org_id, name, description = org
                        with st.expander(f"🏢 {name}", expanded=False):
                            st.write(f"**ID:** {org_id}")
                            st.write(f"**Description:** {description or 'No description'}")
                            
                            # Show users in this organization
                            cursor.execute("""
                                SELECT username, role, permission_type, email 
                                FROM users 
                                WHERE organization_id = ?
                            """, (org_id,))
                            org_users = cursor.fetchall()
                            
                            if org_users:
                                st.write("**Users in this organization:**")
                                for user in org_users:
                                    st.write(f"- {user[0]} ({user[1]})")
                            
                            # Add delete button
                            if st.button("🗑️ Delete Organization", key=f"delete_org_{org_id}"):
                                try:
                                    # First update users to remove organization association
                                    cursor.execute(
                                        "UPDATE users SET organization_id = NULL WHERE organization_id = ?",
                                        (org_id,)
                                    )
                                    # Then delete the organization
                                    cursor.execute("DELETE FROM organizations WHERE rowid = ?", (org_id,))
                                    conn.commit()
                                    st.success(f"Organization '{name}' deleted successfully!")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to delete organization: {str(e)}")
                else:
                    st.info("No organizations found. Create your first organization above.")
        except Exception as e:
            st.error(f"Error loading organizations: {str(e)}")
            print(f"Error details: {str(e)}")
    
    with tabs[2]:
        st.header("⚙️ System Settings")
        
        # Organization Assignment Section
        st.subheader("🔄 Organization Assignment")
        col1, col2 = st.columns(2)
        
        with col1:
            try:
                with sqlite3.connect('data/tableau_data.db') as conn:
                    cursor = conn.cursor()
                    
                    # Get all users except superadmin
                    cursor.execute("""
                        SELECT u.username, u.organization_id, o.name as org_name
                        FROM users u
                        LEFT JOIN organizations o ON u.organization_id = o.rowid
                        WHERE u.username != 'superadmin'
                        ORDER BY u.username
                    """)
                    users = cursor.fetchall()
                    
                    if users:
                        selected_user = st.selectbox(
                            "Select User",
                            options=[user[0] for user in users],
                            format_func=lambda x: f"{x} (Current Org: {dict(zip([u[0] for u in users], [u[2] or 'None' for u in users]))[x]})"
                        )
                        
                        # Get current org for selected user
                        current_org = next((user[1] for user in users if user[0] == selected_user), None)
                        
                        # Get all organizations
                        cursor.execute("SELECT rowid, name FROM organizations ORDER BY name")
                        organizations = cursor.fetchall()
                        
                        if organizations:
                            org_options = [("", "No Organization")] + [(str(org[0]), org[1]) for org in organizations]
                            current_index = 0
                            for i, (org_id, _) in enumerate(org_options):
                                if org_id == str(current_org):
                                    current_index = i
                                    break
                            
                            new_org = st.selectbox(
                                "Assign to Organization",
                                options=[org[0] for org in org_options],
                                format_func=lambda x: dict(org_options)[x],
                                index=current_index
                            )
                            
                            if new_org != str(current_org or ""):
                                if st.button("Update Organization", type="primary"):
                                    try:
                                        cursor.execute("""
                                            UPDATE users 
                                            SET organization_id = ? 
                                            WHERE username = ?
                                        """, (new_org if new_org else None, selected_user))
                                        conn.commit()
                                        st.success(f"Updated organization for {selected_user}")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Failed to update organization: {str(e)}")
                        else:
                            st.warning("No organizations available. Please create organizations first.")
                    else:
                        st.info("No users found to assign")
            except Exception as e:
                st.error(f"Error in organization assignment: {str(e)}")
        
        with col2:
            st.write("### Organization Overview")
            try:
                with sqlite3.connect('data/tableau_data.db') as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT o.name, COUNT(u.username) as user_count
                        FROM organizations o
                        LEFT JOIN users u ON o.rowid = u.organization_id
                        GROUP BY o.rowid, o.name
                        ORDER BY o.name
                    """)
                    org_stats = cursor.fetchall()
                    
                    if org_stats:
                        for org_name, user_count in org_stats:
                            st.write(f"**{org_name}**: {user_count} users")
                    else:
                        st.info("No organizations found")
            except Exception as e:
                st.error(f"Error loading organization statistics: {str(e)}")
        
        st.markdown("---")
        
        # Email Configuration
        with st.expander("📧 Email Settings", expanded=False):
            smtp_server = st.text_input("SMTP Server", value=os.getenv('SMTP_SERVER', ''))
            smtp_port = st.number_input("SMTP Port", value=int(os.getenv('SMTP_PORT', '587')))
            sender_email = st.text_input("Sender Email", value=os.getenv('SENDER_EMAIL', ''))
            
            if st.button("Save Email Settings", key="save_email_settings"):
                # Add settings save logic here
                st.success("Email settings saved successfully!")
        
        # WhatsApp Configuration
        with st.expander("📱 WhatsApp Settings", expanded=False):
            twilio_sid = st.text_input("Twilio Account SID", value=os.getenv('TWILIO_ACCOUNT_SID', ''))
            twilio_token = st.text_input("Twilio Auth Token", type="password")
            whatsapp_number = st.text_input("WhatsApp Number", value=os.getenv('TWILIO_WHATSAPP_NUMBER', ''))
            
            if st.button("Save WhatsApp Settings", key="save_whatsapp_settings"):
                # Add settings save logic here
                st.success("WhatsApp settings saved successfully!")
        
        # Backup & Restore
        with st.expander("💾 Backup & Restore", expanded=False):
            st.write("Database Backup")
            if st.button("Create Backup", key="create_backup"):
                # Add backup logic here
                st.success("Backup created successfully!")
            
            st.write("Restore Database")
            backup_file = st.file_uploader("Select backup file", type=['db', 'sqlite'])
            if backup_file is not None and st.button("Restore", key="restore_backup"):
                # Add restore logic here
                st.success("Database restored successfully!")

def authenticate(server_url: str, auth_method: str, credentials: dict, site_name: str = None) -> TSC.Server:
    """Authenticate with Tableau server"""
    tableau_auth = None
    if auth_method == "Personal Access Token":
        tableau_auth = TSC.PersonalAccessTokenAuth(
            token_name=credentials['token_name'],
            personal_access_token=credentials['token_value'],
            site_id=site_name or ""
        )
    else:
        tableau_auth = TSC.TableauAuth(
            username=credentials['username'],
            password=credentials['password'],
            site_id=site_name or ""
        )
    
    server = TSC.Server(server_url, use_server_version=True)
    
    try:
        if auth_method == "Personal Access Token":
            server.auth.sign_in_with_personal_access_token(tableau_auth)
        else:
            server.auth.sign_in(tableau_auth)
        return server
    except Exception as e:
        print(f"Authentication error: {str(e)}")
        raise e

def get_workbooks(server: TSC.Server) -> list:
    """Get list of workbooks from Tableau server"""
    try:
        all_workbooks = []
        pagination_item = server.workbooks.get()[0]
        for workbook in pagination_item:
            project = server.projects.get_by_id(workbook.project_id)
            all_workbooks.append({
                'id': workbook.id,
                'name': workbook.name,
                'project_name': project.name,
                'project_id': project.id
            })
        return all_workbooks
    except Exception as e:
        print(f"Error getting workbooks: {str(e)}")
        return []

def generate_table_name(workbook_name: str, view_names: list) -> str:
    """Generate a valid SQLite table name from workbook and view names"""
    # Combine names and clean special characters
    combined = f"{workbook_name}_{'_'.join(view_names)}"
    clean_name = ''.join(c if c.isalnum() else '_' for c in combined)
    # Ensure name starts with letter and is not too long
    return f"{'t' if clean_name[0].isdigit() else ''}{clean_name[:50]}"

def download_and_save_data(server: TSC.Server, view_ids: list, workbook_name: str, view_names: list, table_name: str) -> bool:
    """Download data from Tableau views and save to SQLite database"""
    try:
        all_data = []
        for view_id in view_ids:
            view = server.views.get_by_id(view_id)
            csv_data = server.views.get_data(view)
            df = pd.read_csv(io.StringIO(csv_data))
            all_data.append(df)
        
        # Combine all dataframes
        final_df = pd.concat(all_data, axis=0, ignore_index=True)
        
        # Save to SQLite
        with sqlite3.connect('data/tableau_data.db') as conn:
            final_df.to_sql(table_name, conn, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"Error downloading data: {str(e)}")
        return False

def load_views(server: TSC.Server, workbook: dict) -> list:
    """Get list of views from a workbook"""
    try:
        workbook_obj = server.workbooks.get_by_id(workbook['id'])
        server.workbooks.populate_views(workbook_obj)
        return [{'id': view.id, 'name': view.name} for view in workbook_obj.views]
    except Exception as e:
        print(f"Error loading views: {str(e)}")
        return []

def show_tableau_page():
    """Show Tableau connection interface"""
    st.header("🔌 Connect to Tableau")
    
    # Connection settings
    with st.form("tableau_connection"):
        st.subheader("Tableau Server Connection")
        
        server_url = st.text_input("Server URL", placeholder="https://your-server.tableau.com")
        
        site_name = st.text_input(
            "Site Name",
            placeholder="Leave blank for default site",
            help="Enter your Tableau site name (not required for default site)"
        )
        
        auth_method = st.radio(
            "Authentication Method",
            options=["Personal Access Token", "Username/Password"],
            horizontal=True
        )
        
        if auth_method == "Personal Access Token":
            col1, col2 = st.columns(2)
            with col1:
                token_name = st.text_input("Token Name")
            with col2:
                token_value = st.text_input("Token Value", type="password")
            credentials = {"token_name": token_name, "token_value": token_value}
        else:
            col1, col2 = st.columns(2)
            with col1:
                username = st.text_input("Username")
            with col2:
                password = st.text_input("Password", type="password")
            credentials = {"username": username, "password": password}
        
        if st.form_submit_button("Connect"):
            if not server_url:
                st.error("Please enter server URL")
                return
                
            if auth_method == "Personal Access Token" and not (token_name and token_value):
                st.error("Please enter both token name and value")
                return
                
            if auth_method == "Username/Password" and not (username and password):
                st.error("Please enter both username and password")
                return
            
            try:
                # Attempt to authenticate
                with st.spinner("Connecting to Tableau server..."):
                    server = authenticate(server_url, auth_method, credentials, site_name)
                    if server:
                        st.session_state.server = server
                        st.success("Successfully connected to Tableau!")
                        
                        # Immediately try to fetch workbooks
                        with st.spinner("Fetching workbooks..."):
                            workbooks = get_workbooks(server)
                            if workbooks:
                                st.session_state.workbooks = workbooks
                                st.success(f"Found {len(workbooks)} workbooks!")
                            else:
                                st.warning("No workbooks found in this site")
                        
                        st.rerun()
            except Exception as e:
                st.error(f"Failed to connect: {str(e)}")
                print(f"Detailed connection error: {str(e)}")
                if hasattr(e, 'args') and len(e.args) > 0:
                    print(f"Error args: {e.args}")
    
    # Show workbook selection if connected
    if hasattr(st.session_state, 'server'):
        st.markdown("---")
        st.subheader("📚 Select Workbook and Views")
        
        try:
            # Get available workbooks if not already loaded
            if 'workbooks' not in st.session_state:
                with st.spinner("Fetching workbooks..."):
                    st.session_state.workbooks = get_workbooks(st.session_state.server)
            
            if not st.session_state.workbooks:
                st.warning("No workbooks found. Please check your permissions for this site.")
                if st.button("Retry Loading Workbooks"):
                    del st.session_state.workbooks
                    st.rerun()
                return
            
            # Workbook selection
            selected_workbook = st.selectbox(
                "Select Workbook",
                options=st.session_state.workbooks,
                format_func=lambda x: f"{x['name']} ({x['project_name']})"
            )
            
            if selected_workbook:
                st.session_state.selected_workbook = selected_workbook
                
                # Get views for selected workbook
                with st.spinner("Loading views..."):
                    views = load_views(st.session_state.server, selected_workbook)
                if not views:
                    st.warning("No views found in this workbook")
                    return
                
                # View selection
                selected_views = st.multiselect(
                    "Select Views to Download",
                    options=views,
                    format_func=lambda x: x['name']
                )
                
                if selected_views:
                    if st.button("Download Selected Views"):
                        with st.spinner("Downloading data..."):
                            # Generate table name from workbook and view names
                            view_names = [view['name'] for view in selected_views]
                            table_name = generate_table_name(selected_workbook['name'], view_names)
                            
                            # Download and save data
                            success = download_and_save_data(
                                st.session_state.server,
                                [view['id'] for view in selected_views],
                                selected_workbook['name'],
                                view_names,
                                table_name
                            )
                            
                            if success:
                                st.success("Data downloaded successfully!")
                                st.rerun()
                            else:
                                st.error("Failed to download data")
                                
        except Exception as e:
            st.error(f"Error loading workbooks: {str(e)}")
            print(f"Detailed workbook loading error: {str(e)}")
            if hasattr(e, 'args') and len(e.args) > 0:
                print(f"Error args: {e.args}")
            if st.button("Reconnect"):
                del st.session_state.server
                if 'workbooks' in st.session_state:
                    del st.session_state.workbooks
                st.rerun()

def show_saved_datasets(permission_type):
    """Show list of saved datasets"""
    st.title("💾 Saved Datasets")
    
    datasets = get_saved_datasets()
    if not datasets:
        st.info("No datasets available. Connect to Tableau to import data.")
        return
    
    # Create a grid layout for datasets
    for dataset in datasets:
        with st.container():
            st.markdown(f"### 📊 {dataset}")
            
            # Load and show dataset preview
            df = load_dataset(dataset)
            if df is not None:
                st.dataframe(df.head(), use_container_width=True)
                st.caption(f"Total rows: {len(df)}")
                
                # Action buttons
                if permission_type == 'power':
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("📅 Schedule", key=f"schedule_{dataset}"):
                            st.session_state.current_dataset = dataset
                            st.session_state.current_page = "schedule"
                            st.rerun()
                    
                    with col2:
                        if st.button("💬 Ask Questions", key=f"qa_{dataset}"):
                            st.session_state.current_dataset = dataset
                            st.session_state.current_page = "qa"
                            st.rerun()
                    
                    with col3:
                        if st.button("🗑️ Delete", key=f"delete_{dataset}", type="secondary"):
                            if delete_dataset(dataset):
                                st.success(f"Dataset {dataset} deleted successfully!")
                                time.sleep(1)
                                st.rerun()
                else:  # normal user
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("📅 Schedule", key=f"schedule_{dataset}"):
                            st.session_state.current_dataset = dataset
                            st.session_state.current_page = "schedule"
                            st.rerun()
                    
                    with col2:
                        if st.button("🗑️ Delete", key=f"delete_{dataset}", type="secondary"):
                            if delete_dataset(dataset):
                                st.success(f"Dataset {dataset} deleted successfully!")
                                time.sleep(1)
                                st.rerun()
            
            st.markdown("---")

def get_row_count(dataset_name):
    """Get the number of rows in a dataset"""
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM '{dataset_name}'")
            return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error getting row count: {str(e)}")
        return 0

def create_schedules_table():
    """Create schedules table if it doesn't exist"""
    db_manager = DatabaseManager()
    try:
        with sqlite3.connect(db_manager.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schedules (
                    schedule_id TEXT PRIMARY KEY,
                    dataset_name TEXT,
                    frequency TEXT,
                    config TEXT,
                    email_config TEXT,
                    next_run TEXT,
                    recipients TEXT,
                    created_at TEXT,
                    format_config TEXT
                )
            """)
            conn.commit()
            return True
    except Exception as e:
        print(f"Error creating schedules table: {str(e)}")
        return False

class DatabaseManager:
    def __init__(self):
        # Create data directory if it doesn't exist
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # SQLite database path
        self.db_path = self.data_dir / "tableau_data.db"
        self.db_url = f"sqlite:///{self.db_path}"
        
        # Create tables
        self.ensure_database_running()
        self._create_schedules_table()
    
    def _create_schedules_table(self):
        """Create schedules table if it doesn't exist"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS schedules (
                        schedule_id TEXT PRIMARY KEY,
                        dataset_name TEXT,
                        frequency TEXT,
                        config TEXT,
                        email_config TEXT,
                        next_run TEXT,
                        recipients TEXT,
                        created_at TEXT,
                        format_config TEXT
                    )
                """)
                conn.commit()
        except Exception as e:
            print(f"Error creating schedules table: {str(e)}")
    
    def ensure_database_running(self):
        """Ensure database is running and create tables if they don't exist"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create organizations table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS organizations (
                        org_id INTEGER PRIMARY KEY,
                        name TEXT UNIQUE NOT NULL,
                        description TEXT
                    )
                """)
                
                # Create schedules table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS schedules (
                        schedule_id TEXT PRIMARY KEY,
                        dataset_name TEXT,
                        frequency TEXT,
                        config TEXT,
                        email_config TEXT,
                        next_run TEXT,
                        recipients TEXT,
                        created_at TEXT,
                        format_config TEXT
                    )
                """)
                
                # Create users table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        username TEXT UNIQUE NOT NULL,
                        password TEXT NOT NULL,
                        role TEXT DEFAULT 'normal',
                        permission_type TEXT DEFAULT 'normal',
                        organization_id INTEGER,
                        email TEXT,
                        FOREIGN KEY (organization_id) REFERENCES organizations(org_id)
                    )
                """)
                
                conn.commit()
        except Exception as e:
            print(f"Error ensuring database is running: {str(e)}")
    
    def create_organization(self, name: str, description: str = None) -> bool:
        """Create a new organization"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO organizations (name, description) VALUES (?, ?)",
                    (name, description)
                )
                conn.commit()
                return True
        except Exception as e:
            print(f"Error creating organization: {str(e)}")
            return False
    
    def list_tables(self, include_internal=True):
        """List only dataset tables with View_Names column"""
        INTERNAL_TABLES = {
            'users', 
            'user_groups', 
            'user_group_members', 
            'dataset_permissions', 
            'app_info', 
            'sqlite_sequence',
            'sqlite_stat1',
            'sqlite_stat4',
            'schedules'
        }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                if not include_internal:
                    return [table[0] for table in tables if table[0] not in INTERNAL_TABLES]
                return [table[0] for table in tables]
                
        except Exception as e:
            print(f"Error listing tables: {str(e)}")
            return []

    def modify_schedule(self, schedule_id):
        """Handle schedule modification"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT dataset_name, frequency, config, email_config, recipients
                    FROM schedules 
                    WHERE schedule_id = ?
                """, (schedule_id,))
                result = cursor.fetchone()
                
                if not result:
                    st.error("Schedule not found")
                    if st.button("← Back to Schedules", use_container_width=True):
                        st.session_state.current_page = "schedule"
                        st.rerun()
                    return
                
                dataset_name, frequency, config, email_config, recipients = result
                config = json.loads(config)
                email_config = json.loads(email_config)
                
                # Display and handle schedule modification UI
                self._display_schedule_form(
                    conn, cursor, schedule_id, dataset_name, 
                    frequency, config, email_config, recipients
                )
                
        except Exception as e:
            st.error(f"Error loading schedule: {str(e)}")
            print(f"Error loading schedule details: {str(e)}")
        
        # Back button
        st.markdown("---")
        if st.button("← Back to Schedules", use_container_width=True):
            st.session_state.current_page = "schedule"
            st.rerun()
    
    def _display_schedule_form(self, conn, cursor, schedule_id, dataset_name, frequency, config, email_config, recipients):
        """Display and handle the schedule modification form"""
        st.subheader(f"📊 Dataset: {dataset_name}")
        
        # Recipients
        st.write("👥 Recipients")
        email_recipients = [r for r in email_config.get('recipients', [])]
        whatsapp_recipients = [r for r in email_config.get('whatsapp_recipients', [])]
        
        email_list = st.text_area(
            "Email Addresses",
            value='\n'.join(email_recipients),
            placeholder="Enter email addresses (one per line)",
            help="These addresses will receive the scheduled reports"
        )
        
        # WhatsApp recipients
        st.write("📱 WhatsApp Recipients")
        whatsapp_list = st.text_area(
            "WhatsApp Numbers",
            value='\n'.join(whatsapp_recipients),
            placeholder="Enter WhatsApp numbers with country code (one per line)\nExample: +1234567890",
            help="These numbers will receive WhatsApp notifications"
        )
        
        # Message content
        st.write("📝 Message Content")
        message_body = st.text_area(
            "Message Body",
            value=email_config.get('body', ''),
            placeholder="Enter the message to include with the report",
            help="This message will be included in both email and WhatsApp notifications"
        )
        
        # Schedule settings
        st.write("🕒 Schedule Settings")
        schedule_type = st.selectbox(
            "Frequency",
            ["One-time", "Daily", "Weekly", "Monthly"],
            index=["one-time", "daily", "weekly", "monthly"].index(frequency.lower()),
            help="How often to send the report"
        ).lower()
        
        schedule_config = self._handle_schedule_type_settings(schedule_type, config)
        
        # Update schedule button
        if st.button("Update Schedule", type="primary", use_container_width=True):
            self._update_schedule(
                conn, cursor, schedule_id, schedule_type, schedule_config,
                email_list, whatsapp_list, message_body, email_config
            )
    
    def _handle_schedule_type_settings(self, schedule_type, config):
        """Handle settings for different schedule types"""
        col1, col2 = st.columns(2)
        with col1:
            if schedule_type == "one-time":
                date = st.date_input(
                    "Select Date",
                    value=datetime.strptime(config['date'], "%Y-%m-%d").date() if 'date' in config else datetime.now().date()
                )
                hour = st.number_input("Hour (24-hour format)", 0, 23, value=config.get('hour', 8))
                minute = st.number_input("Minute", 0, 59, value=config.get('minute', 0))
                schedule_config = {
                    'type': 'one-time',
                    'date': date.strftime("%Y-%m-%d"),
                    'hour': hour,
                    'minute': minute
                }
            elif schedule_type == "daily":
                hour = st.number_input("Hour (24-hour format)", 0, 23, value=config.get('hour', 8))
                minute = st.number_input("Minute", 0, 59, value=config.get('minute', 0))
                schedule_config = {
                    'type': 'daily',
                    'hour': hour,
                    'minute': minute
                }
            elif schedule_type == "weekly":
                current_day = config.get('day', 0)
                day = st.selectbox(
                    "Day of Week", 
                    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                    index=current_day
                )
                hour = st.number_input("Hour (24-hour format)", 0, 23, value=config.get('hour', 8))
                minute = st.number_input("Minute", 0, 59, value=config.get('minute', 0))
                schedule_config = {
                    'type': 'weekly',
                    'day': ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"].index(day.lower()),
                    'hour': hour,
                    'minute': minute
                }
            else:  # monthly
                day = st.number_input("Day of Month", 1, 31, value=config.get('day', 1))
                col1, col2 = st.columns(2)
                with col1:
                    hour = st.number_input("Hour (24-hour format)", 0, 23, value=config.get('hour', 8))
                with col2:
                    minute = st.number_input("Minute", 0, 59, value=config.get('minute', 0))
                schedule_config = {
                    'type': 'monthly',
                    'day': day,
                    'hour': hour,
                    'minute': minute
                }
        
        with col2:
            self._display_schedule_summary(schedule_type, schedule_config)
        
        return schedule_config
    
    def _display_schedule_summary(self, schedule_type, config):
        """Display schedule summary"""
        st.write("Schedule Summary")
        if schedule_type == "one-time":
            st.info(f"""
            Report will be sent once on:
            {config['date']} at {config['hour']:02d}:{config['minute']:02d}
            """)
        elif schedule_type == "daily":
            st.info(f"""
            Report will be sent daily at {config['hour']:02d}:{config['minute']:02d}
            """)
        elif schedule_type == "weekly":
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            st.info(f"""
            Report will be sent every {day_names[config['day']]} at {config['hour']:02d}:{config['minute']:02d}
            """)
        else:
            st.info(f"""
            Report will be sent on day {config['day']} of each month at {config['hour']:02d}:{config['minute']:02d}
            """)
    
    def _update_schedule(self, conn, cursor, schedule_id, schedule_type, schedule_config,
                        email_list, whatsapp_list, message_body, email_config):
        """Update the schedule in the database"""
        if not email_list.strip() and not whatsapp_list.strip():
            st.error("Please enter at least one recipient (email or WhatsApp)")
            return
        
        try:
            # Update email configuration
            email_config.update({
                'recipients': [e.strip() for e in email_list.split('\n') if e.strip()],
                'whatsapp_recipients': [w.strip() for w in whatsapp_list.split('\n') if w.strip()],
                'body': message_body
            })
            
            # Validate WhatsApp numbers if provided
            if email_config['whatsapp_recipients']:
                invalid_numbers = []
                for number in email_config['whatsapp_recipients']:
                    if not number.startswith('+') or not number[1:].isdigit():
                        invalid_numbers.append(number)
                if invalid_numbers:
                    st.error(f"Invalid WhatsApp number(s): {', '.join(invalid_numbers)}. Numbers must start with + and contain only digits.")
                    return
            
            # Update schedule in database
            cursor.execute("""
                UPDATE schedules 
                SET frequency = ?, config = ?, email_config = ?, 
                    recipients = ?, next_run = ?
                WHERE schedule_id = ?
            """, (
                schedule_type,
                json.dumps(schedule_config),
                json.dumps(email_config),
                ', '.join(email_config['recipients'] + email_config.get('whatsapp_recipients', [])),
                datetime.now().isoformat(),
                schedule_id
            ))
            conn.commit()
            
            st.success("Schedule updated successfully! 🎉")
            time.sleep(1)
            st.session_state.current_page = "schedule"
            st.rerun()
            
        except Exception as e:
            st.error(f"Failed to update schedule: {str(e)}")
            print(f"Schedule update error details: {str(e)}")

def show_schedule_page():
    """Show schedule management interface"""
    st.title("📅 Schedule Management")
    
    if st.session_state.get('show_modify_schedule'):
        db_manager = DatabaseManager()
        db_manager.modify_schedule(st.session_state.modifying_schedule)
        return
    
    # Initialize state variables
    if 'create_new_schedule' not in st.session_state:
        st.session_state.create_new_schedule = False
    if 'report_formatter' not in st.session_state:
        st.session_state.report_formatter = ReportFormatter()
    
    # Create New Schedule button
    if not st.session_state.create_new_schedule:
        if st.button("➕ Create New Schedule", type="primary", use_container_width=True):
            st.session_state.create_new_schedule = True
            st.rerun()
    else:
        # Get formatter from session state
        formatter = st.session_state.report_formatter
        
        # Get current dataset
        if not st.session_state.get('current_dataset'):
            st.error("Please select a dataset first")
            if st.button("← Back to Datasets"):
                st.session_state.create_new_schedule = False
                st.rerun()
            return
        
        dataset_name = st.session_state.current_dataset
        df = load_dataset(dataset_name)
        
        if df is None:
            st.error("Failed to load dataset")
            return
        
        st.subheader(f"Schedule Report for: {dataset_name}")
        
        # Create tabs for different sections
        format_tab, preview_tab, schedule_tab = st.tabs(["Report Format", "Preview", "Schedule Settings"])
        
        with format_tab:
            formatter.show_formatting_interface(df)
        
        with preview_tab:
            if df is not None:
                st.dataframe(df.head(), use_container_width=True)
                st.caption(f"Total rows: {len(df)}")
                
                # Show preview if available
                if 'preview_buffer' in st.session_state:
                    # Download button (outside form)
                    st.download_button(
                        "⬇️ Download Preview",
                        data=st.session_state.preview_buffer.getvalue(),
                        file_name=f"report_preview_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                    
                    # Show preview in iframe
                    base64_pdf = base64.b64encode(st.session_state.preview_buffer.getvalue()).decode('utf-8')
                    pdf_display = f'''
                        <iframe 
                            src="data:application/pdf;base64,{base64_pdf}" 
                            width="100%" 
                            height="600" 
                            type="application/pdf"
                        ></iframe>
                    '''
                    st.markdown(pdf_display, unsafe_allow_html=True)
        
        # Schedule settings in a form
        with schedule_tab:
            with st.form("schedule_settings_form"):
                st.subheader("📅 Schedule Settings")
                
                # Schedule type
                schedule_type = st.selectbox(
                    "Frequency",
                    ["One-time", "Daily", "Weekly", "Monthly"],
                    help="How often to send the report"
                )
                
                # Schedule details based on type
                if schedule_type == "One-time":
                    date = st.date_input("Select Date")
                    col1, col2 = st.columns(2)
                    with col1:
                        hour = st.number_input("Hour (24-hour format)", min_value=0, max_value=23, value=8)
                    with col2:
                        minute = st.number_input("Minute", min_value=0, max_value=59, value=0)
                    schedule_config = {
                        'type': 'one-time',
                        'date': date.strftime("%Y-%m-%d"),
                        'hour': hour,
                        'minute': minute
                    }
                elif schedule_type == "Daily":
                    col1, col2 = st.columns(2)
                    with col1:
                        hour = st.number_input("Hour (24-hour format)", min_value=0, max_value=23, value=8)
                    with col2:
                        minute = st.number_input("Minute", min_value=0, max_value=59, value=0)
                    schedule_config = {
                        'type': 'daily',
                        'hour': hour,
                        'minute': minute
                    }
                elif schedule_type == "Weekly":
                    day = st.selectbox("Day of Week", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
                    col1, col2 = st.columns(2)
                    with col1:
                        hour = st.number_input("Hour (24-hour format)", min_value=0, max_value=23, value=8)
                    with col2:
                        minute = st.number_input("Minute", min_value=0, max_value=59, value=0)
                    schedule_config = {
                        'type': 'weekly',
                        'day': ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"].index(day.lower()),
                        'hour': hour,
                        'minute': minute
                    }
                else:  # Monthly
                    day = st.number_input("Day of Month", min_value=1, max_value=31, value=1)
                    col1, col2 = st.columns(2)
                    with col1:
                        hour = st.number_input("Hour (24-hour format)", min_value=0, max_value=23, value=8)
                    with col2:
                        minute = st.number_input("Minute", min_value=0, max_value=59, value=0)
                    schedule_config = {
                        'type': 'monthly',
                        'day': day,
                        'hour': hour,
                        'minute': minute
                    }
                
                # Recipients
                st.subheader("📧 Recipients")
                email_list = st.text_area(
                    "Email Addresses",
                    placeholder="Enter email addresses (one per line)",
                    help="These addresses will receive the scheduled reports"
                )
                
                # WhatsApp recipients
                whatsapp_list = st.text_area(
                    "WhatsApp Numbers",
                    placeholder="Enter WhatsApp numbers with country code (one per line)\nExample: +1234567890",
                    help="These numbers will receive WhatsApp notifications"
                )
                
                # Message
                message = st.text_area(
                    "Message",
                    placeholder="Enter a message to include with the report",
                    help="This message will be included in both email and WhatsApp notifications"
                )
                
                # Get formatting settings from the formatter
                if 'preview_buffer' in st.session_state:
                    format_config = {
                        'page_size': formatter.page_size,
                        'orientation': formatter.orientation,
                        'margins': formatter.margins,
                        'title_style': formatter.title_style,
                        'table_style': formatter.table_style,
                        'chart_size': formatter.chart_size,
                        'report_content': st.session_state.get('report_content', {})
                    }
                else:
                    format_config = None
                
                # Submit button
                submitted = st.form_submit_button("Create Schedule", type="primary", use_container_width=True)
                
                if submitted:
                    try:
                        # Validate inputs
                        if not email_list.strip() and not whatsapp_list.strip():
                            st.error("Please enter at least one recipient (email or WhatsApp)")
                            return
                        
                        if not format_config:
                            st.error("Please preview the report format first before creating the schedule")
                            return
                        
                        # Prepare email configuration
                        email_config = {
                            'recipients': [e.strip() for e in email_list.split('\n') if e.strip()],
                            'whatsapp_recipients': [w.strip() for w in whatsapp_list.split('\n') if w.strip()],
                            'body': message,
                            'format': 'PDF',
                            'smtp_server': os.getenv('SMTP_SERVER'),
                            'smtp_port': int(os.getenv('SMTP_PORT', 587)),
                            'sender_email': os.getenv('SENDER_EMAIL'),
                            'sender_password': os.getenv('SENDER_PASSWORD'),
                            'base_url': os.getenv('BASE_URL', 'https://your-domain.com')  # Add your actual domain here
                        }
                        
                        # Create schedule
                        report_manager = ReportManager()
                        job_id = report_manager.schedule_report(
                            dataset_name, 
                            email_config, 
                            schedule_config,
                            format_config=format_config
                        )
                        
                        if job_id:
                            st.success("Schedule created successfully! 🎉")
                            st.session_state.create_new_schedule = False
                            st.rerun()
                        else:
                            # Check if schedule exists
                            existing_schedules = report_manager.get_active_schedules()
                            schedule_exists = False
                            for existing_job_id, schedule in existing_schedules.items():
                                if (schedule['dataset_name'] == dataset_name and 
                                    schedule['schedule_config']['type'] == schedule_config['type']):
                                    if schedule_config['type'] == 'one-time':
                                        if (schedule['schedule_config']['date'] == schedule_config['date'] and
                                            schedule['schedule_config']['hour'] == schedule_config['hour'] and
                                            schedule['schedule_config']['minute'] == schedule_config['minute']):
                                            schedule_exists = True
                                            break
                                    else:
                                        if (schedule['schedule_config'].get('hour') == schedule_config.get('hour') and
                                            schedule['schedule_config'].get('minute') == schedule_config.get('minute') and
                                            schedule['schedule_config'].get('day') == schedule_config.get('day')):
                                            schedule_exists = True
                                            break
                            
                            if schedule_exists:
                                st.error(f"A schedule already exists for {dataset_name} at the specified time. Please choose a different time or modify the existing schedule.")
                            else:
                                st.error("Failed to create schedule. Please check your settings and try again.")
                        
                    except Exception as e:
                        st.error(f"Failed to create schedule: {str(e)}")
                        print(f"Schedule creation error details: {str(e)}")
        
        # Cancel button (outside form)
        if st.button("❌ Cancel"):
            st.session_state.create_new_schedule = False
            st.rerun()
    
    st.markdown("---")
    
    # Show active schedules
    st.subheader("Active Schedules")
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT schedule_id, dataset_name, frequency, config, email_config, next_run
                FROM schedules
                ORDER BY created_at DESC
            """)
            schedules = cursor.fetchall()
            
            if schedules:
                for schedule in schedules:
                    schedule_id, dataset_name, frequency, config, email_config, next_run = schedule
                    with st.expander(f"📊 {dataset_name} - {frequency.title()}", expanded=True):
                        config = json.loads(config)
                        email_config = json.loads(email_config)
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**Schedule Details:**")
                            st.write(f"- Frequency: {frequency.title()}")
                            st.write(f"- Next Run: {next_run}")
                            st.write(f"- Recipients: {', '.join(email_config.get('recipients', []))}")
                            if email_config.get('whatsapp_recipients'):
                                st.write(f"- WhatsApp: {', '.join(email_config['whatsapp_recipients'])}")
                        
                        with col2:
                            if st.button("✏️ Modify", key=f"modify_{schedule_id}"):
                                st.session_state.show_modify_schedule = True
                                st.session_state.modifying_schedule = schedule_id
                                st.rerun()
                            
                            if st.button("🗑️ Delete", key=f"delete_{schedule_id}"):
                                cursor.execute("DELETE FROM schedules WHERE schedule_id = ?", (schedule_id,))
                                conn.commit()
                                st.success("Schedule deleted successfully!")
                                time.sleep(1)
                                st.rerun()
            else:
                st.info("No active schedules found.")
                
    except Exception as e:
        st.error(f"Error loading schedules: {str(e)}")
        print(f"Error details: {str(e)}")

def show_qa_page():
    """Show Q&A interface for dataset analysis"""
    st.title("💬 Chat with Data")
    
    if not st.session_state.get('current_dataset'):
        st.info("Please select a dataset from the home page to start asking questions.")
        return
    
    dataset_name = st.session_state.current_dataset
    df = load_dataset(dataset_name)
    
    if df is None:
        st.error("Failed to load dataset.")
        return
    
    try:
        # Show dataset preview
        st.subheader(f"📊 Dataset: {dataset_name}")
        st.dataframe(df.head(), use_container_width=True)
        st.caption(f"Total rows: {len(df)}")
        
        # Dataset summary
        st.subheader("📈 Dataset Summary")
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Columns:**")
            for col in df.columns:
                st.write(f"- {col}")
        
        with col2:
            st.write("**Quick Stats:**")
            numeric_cols = df.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                st.write(f"**{col}:**")
                st.write(f"- Mean: {df[col].mean():.2f}")
                st.write(f"- Min: {df[col].min():.2f}")
                st.write(f"- Max: {df[col].max():.2f}")
        
        # Q&A Interface
        st.markdown("---")
        st.subheader("🤔 Ask Questions")
        
        # Example questions based on actual columns
        examples = []
        if len(numeric_cols) > 0:
            examples.extend([
                f"What is the total sum of {numeric_cols[0]}?",
                f"Show me the average {numeric_cols[0]}",
                f"What are the top 5 values in {numeric_cols[0]}?"
            ])
        
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) > 0 and len(numeric_cols) > 0:
            examples.append(f"Show me the trend of {numeric_cols[0]} over time")
        
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            examples.append(f"What are the unique values in {categorical_cols[0]}?")
        
        st.write("Try asking questions like:")
        for example in examples:
            st.write(f"- {example}")
        
        # Question input
        user_question = st.text_area(
            "Ask a question about your data",
            placeholder="Example: What is the total sales? Show me the trend over time.",
            key="qa_input"
        )
        
        if user_question:
            st.write("**Your Question:**", user_question)
            
            # Process basic questions
            try:
                # Look for keywords in the question
                question_lower = user_question.lower()
                
                if 'total' in question_lower or 'sum' in question_lower:
                    # Find numeric columns mentioned in the question
                    for col in numeric_cols:
                        if col.lower() in question_lower:
                            total = df[col].sum()
                            st.success(f"The total {col} is: {total:,.2f}")
                            
                            # Show visualization
                            fig = go.Figure(data=[
                                go.Bar(name=col, x=[col], y=[total])
                            ])
                            fig.update_layout(title=f"Total {col}")
                            st.plotly_chart(fig)
                
                elif 'average' in question_lower or 'mean' in question_lower:
                    # Find numeric columns mentioned in the question
                    for col in numeric_cols:
                        if col.lower() in question_lower:
                            mean = df[col].mean()
                            st.success(f"The average {col} is: {mean:,.2f}")
                            
                            # Show visualization
                            fig = go.Figure(data=[
                                go.Box(name=col, y=df[col])
                            ])
                            fig.update_layout(title=f"Distribution of {col}")
                            st.plotly_chart(fig)
                
                elif 'top' in question_lower:
                    # Extract number (default to 5 if not found)
                    import re
                    numbers = re.findall(r'\d+', question_lower)
                    n = int(numbers[0]) if numbers else 5
                    
                    # Find columns mentioned in the question
                    for col in df.columns:
                        if col.lower() in question_lower:
                            top_values = df[col].value_counts().head(n)
                            st.success(f"Top {n} values in {col}:")
                            
                            # Show results in a table
                            st.write(pd.DataFrame({
                                col: top_values.index,
                                'Count': top_values.values
                            }))
                            
                            # Show visualization
                            fig = go.Figure(data=[
                                go.Bar(x=top_values.index, y=top_values.values)
                            ])
                            fig.update_layout(title=f"Top {n} values in {col}")
                            st.plotly_chart(fig)
                
                elif 'trend' in question_lower or 'over time' in question_lower:
                    # Look for date columns
                    if len(date_cols) > 0:
                        date_col = date_cols[0]  # Use the first date column
                        
                        # Find numeric columns mentioned in the question
                        for col in numeric_cols:
                            if col.lower() in question_lower:
                                # Group by date and calculate mean
                                trend_data = df.groupby(date_col)[col].mean()
                                
                                st.success(f"Showing trend of {col} over time")
                                
                                # Show visualization
                                fig = go.Figure(data=[
                                    go.Scatter(x=trend_data.index, y=trend_data.values, mode='lines+markers')
                                ])
                                fig.update_layout(title=f"Trend of {col} over time")
                                st.plotly_chart(fig)
                    else:
                        st.warning("No date/time columns found in the dataset")
                
                elif 'unique' in question_lower or 'distinct' in question_lower:
                    # Find columns mentioned in the question
                    for col in df.columns:
                        if col.lower() in question_lower:
                            unique_values = df[col].nunique()
                            st.success(f"There are {unique_values:,} unique values in {col}")
                            
                            # Show top 10 most common values
                            top_values = df[col].value_counts().head(10)
                            st.write("Top 10 most common values:")
                            
                            # Show visualization
                            fig = go.Figure(data=[
                                go.Bar(x=top_values.index, y=top_values.values)
                            ])
                            fig.update_layout(title=f"Most common values in {col}")
                            st.plotly_chart(fig)
                
                else:
                    st.info("I'm not sure how to answer that question. Try asking about totals, averages, top values, or trends.")
                    st.write("You can also try one of the example questions above.")
                    
            except Exception as e:
                st.error(f"Error processing question: {str(e)}")
                st.info("Try rephrasing your question or use one of the example questions above.")
    
    except Exception as e:
        st.error(f"Error in Q&A interface: {str(e)}")
        st.info("Please try refreshing the page or selecting the dataset again.")

def modify_schedule_page():
    """Show schedule modification interface"""
    if not st.session_state.get('modifying_schedule'):
        st.error("No schedule selected for modification")
        return
    
    db_manager = DatabaseManager()
    db_manager.modify_schedule(st.session_state.modifying_schedule)

def show_help():
    """Show help and documentation"""
    st.title("ℹ️ Help & Documentation")
    
    st.header("Getting Started")
    st.write("""
    1. Connect to Tableau using your server credentials
    2. Select workbooks and views to download
    3. Schedule reports or analyze data using the Q&A feature
    """)

    st.header("Features")
    st.write("""
    - 🔌 **Tableau Connection**: Connect to your Tableau server
    - 📊 **Data Download**: Download data from selected views
    - 📅 **Scheduling**: Set up automated report delivery
    - 💬 **Q&A**: Ask questions about your data
    - 📧 **Notifications**: Receive reports via email or WhatsApp
    """)

def handle_report_request():
    """Handle shared report link requests"""
    report_id = st.query_params.get('report')
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM reports WHERE report_id = ?", (report_id,))
            report = cursor.fetchone()
            if report:
                st.title("📊 Shared Report")
                st.write(f"Report ID: {report_id}")
                # Display report content
                df = pd.read_sql_query(f"SELECT * FROM '{report[1]}'", conn)
                st.dataframe(df)
            else:
                st.error("Report not found or has expired")
    except Exception as e:
        st.error(f"Error loading report: {str(e)}")

def main():
    """Main function to run the Streamlit app"""
    st.set_page_config(page_title="Tableau Data Reporter", layout="wide")
    
    # Load environment variables from .env file
    from dotenv import load_dotenv
    load_dotenv()
    
    # Initialize session state if not already done
    init_session_state()
    
    # Check for shared report link
    if st.query_params.get('report'):
        handle_report_request()
        return
    
    # Show login page if not authenticated
    if not st.session_state.get('authenticated', False):
        show_login_page()
        return
    
    # Show navigation bar if user is authenticated
    with st.sidebar:
        st.title("Navigation")
        
        # Show user info
        if st.session_state.get('user'):
            st.write(f"**User:** {st.session_state.user['username']}")
            st.write(f"**Role:** {st.session_state.user['role']}")
            st.write(f"**Organization:** {st.session_state.user['organization_name']}")
            st.markdown("---")
        
        # Navigation buttons
        if st.session_state.user['role'] != 'superadmin':
            if st.button("🏠 Home", key="nav_home", use_container_width=True):
                st.session_state.current_page = "home"
                st.rerun()
            
            if st.button("🔌 Connect to Tableau", key="nav_tableau", use_container_width=True):
                st.session_state.current_page = "tableau"
                st.rerun()
            
            if st.button("💾 Saved Data", key="nav_saved_data", use_container_width=True):
                st.session_state.current_page = "saved_data"
                st.rerun()
            
            if st.button("📅 Schedule Reports", key="nav_schedule", use_container_width=True):
                st.session_state.current_page = "schedule"
                st.rerun()
            
            if st.button("❓ Q&A", key="nav_qa", use_container_width=True):
                st.session_state.current_page = "qa"
                st.rerun()
        
        st.markdown("---")
        
        # Help and Logout buttons
        if st.button("ℹ️ Help", key="nav_help", use_container_width=True):
            show_help()
        
        if st.button("🚪 Logout", key="nav_logout", use_container_width=True):
            clear_session()
            st.rerun()
    
    # Show appropriate page based on user role and current page
    if st.session_state.user['role'] == 'superadmin':
        show_user_dashboard()
    else:
        current_page = st.session_state.get('current_page', 'home')
        if current_page == "home":
            show_saved_datasets(st.session_state.user['permission_type'])
        elif current_page == "tableau":
            show_tableau_page()
        elif current_page == "saved_data":
            show_saved_datasets(st.session_state.user['permission_type'])
        elif current_page == "schedule":
            show_schedule_page()
        elif current_page == "qa":
            show_qa_page()
        elif current_page == "modify_schedule":
            modify_schedule_page()

if __name__ == "__main__":
    main() 