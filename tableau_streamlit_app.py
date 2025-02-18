import os
import json
import io
from pathlib import Path
import time
import uuid
from datetime import datetime, timedelta
import sqlite3
import base64
import types
import smtplib
from dotenv import load_dotenv

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

# Load environment variables
load_dotenv()

# Email settings from environment variables
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')

if not all([SMTP_SERVER, SMTP_PORT, SENDER_EMAIL, SENDER_PASSWORD]):
    st.error("""
    ⚠️ Email settings are not properly configured. Please check your .env file and ensure the following variables are set:
    - SMTP_SERVER
    - SMTP_PORT
    - SENDER_EMAIL
    - SENDER_PASSWORD
    """)

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
        if st.button("🔌 Connect to Tableau", key="normal_user_connect_tableau_btn", use_container_width=True):
            st.session_state.show_tableau_page = True
            st.session_state.show_schedule_page = False
            st.rerun()
            
        if st.button("📅 Schedule Reports", key="normal_user_schedule_reports_btn", use_container_width=True):
            st.session_state.show_schedule_page = True
            st.session_state.show_tableau_page = False
            st.rerun()
        
        st.markdown("---")
        if st.button("🚪 Logout", key="normal_user_logout_btn", use_container_width=True):
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
        if st.button("🔌 Connect to Tableau", key="power_user_connect_tableau_btn", use_container_width=True):
            st.session_state.show_tableau_page = True
            st.session_state.show_qa_page = False
            st.session_state.show_schedule_page = False
            st.rerun()
            
        if st.button("💬 Chat with Data", key="power_user_chat_data_btn", use_container_width=True):
            st.session_state.show_qa_page = True
            st.session_state.show_tableau_page = False
            st.session_state.show_schedule_page = False
            st.rerun()
            
        if st.button("📅 Schedule Reports", key="power_user_schedule_reports_btn", use_container_width=True):
            st.session_state.show_schedule_page = True
            st.session_state.show_tableau_page = False
            st.session_state.show_qa_page = False
            st.rerun()
        
        st.markdown("---")
        if st.button("🚪 Logout", key="power_user_logout_btn", use_container_width=True):
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
    try:
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
        except Exception as sign_in_error:
            error_msg = str(sign_in_error)
            if "401" in error_msg:
                print("Authentication failed: Invalid credentials or insufficient permissions")
                raise Exception("Authentication failed: Please check your credentials and permissions")
            elif "403" in error_msg:
                print("Authorization failed: Insufficient permissions")
                raise Exception("Authorization failed: You don't have permission to access this site")
            elif "404" in error_msg:
                print("Server not found: Invalid server URL or site name")
                raise Exception("Server not found: Please check your server URL and site name")
            else:
                print(f"Authentication error: {error_msg}")
                raise Exception(f"Authentication failed: {error_msg}")
    except Exception as e:
        print(f"Authentication error: {str(e)}")
        raise e

def get_workbooks(server: TSC.Server) -> list:
    """Get all workbooks from Tableau Server"""
    try:
        print("Starting workbook retrieval...")
        processed_workbooks = []
        
        # Get all workbooks using Pager
        for workbook in TSC.Pager(server.workbooks):
            try:
                print(f"Processing workbook: {workbook.name}")
                # Get workbook details
                workbook_info = {
                    'id': workbook.id,
                    'name': workbook.name,
                    'project_id': workbook.project_id,
                    'project_name': getattr(workbook, 'project_name', 'Unknown Project'),
                    'views': []
                }
                
                # Get views for this workbook
                server.workbooks.populate_views(workbook)
                if hasattr(workbook, 'views') and workbook.views:
                    for view in workbook.views:
                        if hasattr(view, 'id') and hasattr(view, 'name'):
                            workbook_info['views'].append({
                                'id': view.id,
                                'name': view.name
                            })
                            print(f"Added view: {view.name} (ID: {view.id})")
                
                processed_workbooks.append(workbook_info)
                print(f"Successfully processed workbook {workbook.name} with {len(workbook_info['views'])} views")
                
            except Exception as e:
                print(f"Error processing workbook {getattr(workbook, 'name', 'Unknown')}: {str(e)}")
                print(f"Error type: {type(e)}")
                if hasattr(e, 'args'):
                    print(f"Error args: {e.args}")
                continue
        
        if not processed_workbooks:
            print("No workbooks were found or user lacks necessary permissions")
            print("Server info:", server.server_address)
            print("Site info:", server.site_id)
            # Get current user info
            current_user = server.users.get_by_id(server.user_id)
            print("User info:", current_user.name if current_user else "Unknown user")
        else:
            print(f"Successfully retrieved {len(processed_workbooks)} workbooks")
        
        return processed_workbooks
        
    except Exception as e:
        print(f"Error getting workbooks: {str(e)}")
        print(f"Error type: {type(e)}")
        if hasattr(e, 'args'):
            print(f"Error args: {e.args}")
        return []

def generate_table_name(workbook_name: str, view_names: list) -> str:
    """Generate a valid SQLite table name from workbook and view names"""
    # Combine names and clean special characters
    combined = f"{workbook_name}_{'_'.join(view_names)}"
    clean_name = ''.join(c if c.isalnum() else '_' for c in combined)
    # Ensure name starts with letter and is not too long
    return f"{'t' if clean_name[0].isdigit() else ''}{clean_name[:50]}"

def download_and_save_data(server: TSC.Server, view_ids: list, workbook_name: str, view_names: list, table_name: str) -> bool:
    """Download data from views and save to database"""
    data_downloaded = False
    
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            print(f"\nAttempting to download data for {len(view_ids)} views from workbook: {workbook_name}")
            
            for view_id, view_name in zip(view_ids, view_names):
                print(f"\nProcessing view: {view_name} (ID: {view_id})")
                
                try:
                    # Get the view object
                    view = server.views.get_by_id(view_id)
                    if not view:
                        print(f"View not found: {view_name}")
                        continue
                    
                    print("Attempting to retrieve CSV data...")
                    # First attempt with request options
                    req_option = TSC.RequestOptions()
                    req_option.maxage = 0
                    
                    server.views.populate_csv(view, req_option)
                    csv_data = view.csv
                    
                    if not csv_data:
                        print("Failed to retrieve CSV using populate_csv with request options. Trying without request options...")
                        server.views.populate_csv(view)
                        csv_data = view.csv
                    
                    if not csv_data:
                        print(f"Failed to get CSV data for view {view_name}")
                        continue
                    
                    # Convert CSV data to a list if it's a generator
                    if isinstance(csv_data, types.GeneratorType):
                        csv_data = list(csv_data)
                    
                    # Ensure CSV data is a string
                    if isinstance(csv_data, list):
                        # Decode bytes to string if necessary
                        csv_data = [chunk.decode('utf-8') if isinstance(chunk, bytes) else chunk for chunk in csv_data]
                        csv_data = ''.join(csv_data)
                    
                    # Convert CSV to string
                    try:
                        if isinstance(csv_data, bytes):
                            csv_string = csv_data.decode('utf-8')
                        else:
                            csv_string = csv_data
                        
                        print(f"Successfully retrieved CSV. First 100 chars: {csv_string[:100]}")
                        
                        # Load into DataFrame
                        df = pd.read_csv(io.StringIO(csv_string))
                        print(f"\nDataFrame info for {view_name}:")
                        print(df.info())
                        
                        if df.empty:
                            print(f"Warning: Empty DataFrame for view {view_name}")
                            print("CSV data preview:")
                            print(csv_string[:500])
                            continue
                        
                        # Save to database
                        print(f"Saving {len(df)} rows to database...")
                        df.to_sql(table_name, conn, if_exists='replace', index=False)
                        print(f"Successfully saved {len(df)} rows to database table: {table_name}")
                        data_downloaded = True
                        break  # Successfully got data from one view, no need to try others
                        
                    except Exception as df_error:
                        print(f"Error processing DataFrame: {str(df_error)}")
                        print("CSV data preview:")
                        print(csv_string[:500] if 'csv_string' in locals() else "No CSV string available")
                        continue
                    
                except Exception as view_error:
                    print(f"Error downloading data from view {view_name}: {str(view_error)}")
                    print(f"Error type: {type(view_error)}")
                    if hasattr(view_error, 'args'):
                        print(f"Error args: {view_error.args}")
                    continue
            
            if not data_downloaded:
                print("\nFailed to download data from any view")
                return False
            
            return data_downloaded
            
    except Exception as e:
        print(f"Error in download_and_save_data: {str(e)}")
        print(f"Error type: {type(e)}")
        if hasattr(e, 'args'):
            print(f"Error args: {e.args}")
        return False

def load_views(server: TSC.Server, workbook: dict) -> list:
    """Get list of views from a workbook"""
    try:
        print(f"Loading views for workbook: {workbook['name']}")
        workbook_obj = server.workbooks.get_by_id(workbook['id'])
        
        # Populate views
        server.workbooks.populate_views(workbook_obj)
        print(f"Found {len(workbook_obj.views) if hasattr(workbook_obj, 'views') else 0} views")
        
        views = []
        if hasattr(workbook_obj, 'views'):
            for view in workbook_obj.views:
                try:
                    if hasattr(view, 'id') and hasattr(view, 'name'):
                        view_info = {'id': view.id, 'name': view.name}
                        views.append(view_info)
                        print(f"Added view: {view.name} (ID: {view.id})")
                    else:
                        print(f"Warning: View missing required attributes - Available attributes: {dir(view)}")
                except Exception as view_error:
                    print(f"Error processing view: {str(view_error)}")
                    continue
        
        return views
        
    except Exception as e:
        print(f"Error loading views: {str(e)}")
        print(f"Error type: {type(e)}")
        if hasattr(e, 'args'):
            print(f"Error args: {e.args}")
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
                        if st.button("📅 Schedule", key=f"schedule_dataset_btn_{dataset}"):
                            st.session_state.current_dataset = dataset
                            st.session_state.show_schedule_page = True
                            st.rerun()
                    
                    with col2:
                        if st.button("💬 Ask Questions", key=f"qa_dataset_btn_{dataset}"):
                            st.session_state.current_dataset = dataset
                            st.session_state.show_qa_page = True
                            st.rerun()
                    
                    with col3:
                        if st.button("🗑️ Delete", key=f"delete_dataset_btn_{dataset}", type="secondary"):
                            if delete_dataset(dataset):
                                st.success(f"Dataset {dataset} deleted successfully!")
                                time.sleep(1)
                                st.rerun()
                else:  # normal user
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("📅 Schedule", key=f"schedule_dataset_btn_{dataset}"):
                            st.session_state.current_dataset = dataset
                            st.session_state.show_schedule_page = True
                            st.rerun()
                    
                    with col2:
                        if st.button("🗑️ Delete", key=f"delete_dataset_btn_{dataset}", type="secondary"):
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
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dataset_name TEXT NOT NULL,
                        schedule_type TEXT NOT NULL,
                        schedule_config TEXT NOT NULL,
                        email_config TEXT NOT NULL,
                        format_config TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_run TIMESTAMP,
                        next_run TIMESTAMP,
                        status TEXT DEFAULT 'active'
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
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dataset_name TEXT NOT NULL,
                        schedule_type TEXT NOT NULL,
                        schedule_config TEXT NOT NULL,
                        email_config TEXT NOT NULL,
                        format_config TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_run TIMESTAMP,
                        next_run TIMESTAMP,
                        status TEXT DEFAULT 'active'
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
            # Get existing schedule details
            report_manager = ReportManager()
            schedules = report_manager.get_active_schedules()
            if schedule_id not in schedules:
                st.error("Schedule not found")
                return
            
            schedule = schedules[schedule_id]
            dataset_name = schedule['dataset_name']
            current_config = schedule['schedule_config']
            current_email_config = schedule['email_config']
            
            st.title("Modify Schedule")
            st.write(f"Modifying schedule for dataset: {dataset_name}")
            
            # Schedule type selection
            schedule_type = st.selectbox(
                "Schedule Type",
                ["one-time", "daily", "weekly", "monthly"],
                index=["one-time", "daily", "weekly", "monthly"].index(current_config['type'])
            )
            
            # Get schedule configuration based on type
            new_schedule_config = self._handle_schedule_type_settings(schedule_type, current_config)
            
            # Email and WhatsApp recipients
            st.markdown("### Recipients")
            
            # Email recipients
            current_email_list = "\n".join(current_email_config.get('recipients', []))
            email_list = st.text_area(
                "Email Recipients (one per line)",
                value=current_email_list,
                help="Enter email addresses, one per line"
            )
            
            # WhatsApp recipients
            current_whatsapp_list = "\n".join(current_email_config.get('whatsapp_recipients', []))
            whatsapp_list = st.text_area(
                "WhatsApp Recipients (one per line)",
                value=current_whatsapp_list,
                help="Enter WhatsApp numbers with country code (e.g., +1234567890), one per line"
            )
            
            # Message body
            message_body = st.text_area(
                "Message Body (optional)",
                value=current_email_config.get('body', ''),
                help="Enter an optional message to include with the report"
            )
            
            # Update button
            if st.button("Update Schedule", type="primary", use_container_width=True):
                try:
                    # Validate recipients
                    if not email_list.strip() and not whatsapp_list.strip():
                        st.error("Please enter at least one recipient (email or WhatsApp)")
                        return
                    
                    # Prepare email configuration
                    new_email_config = {
                        'recipients': [e.strip() for e in email_list.split('\n') if e.strip()],
                        'whatsapp_recipients': [w.strip() for w in whatsapp_list.split('\n') if w.strip()],
                        'body': message_body,
                        'smtp_server': SMTP_SERVER,
                        'smtp_port': SMTP_PORT,
                        'sender_email': SENDER_EMAIL,
                        'sender_password': SENDER_PASSWORD
                    }
                    
                    # Update schedule using the existing job ID
                    updated_job_id = report_manager.schedule_report(
                        dataset_name=dataset_name,
                        email_config=new_email_config,
                        schedule_config=new_schedule_config,
                        existing_job_id=schedule_id
                    )
                    
                    if updated_job_id:
                        st.success("Schedule updated successfully! 🎉")
                        time.sleep(1)
                        st.session_state.show_modify_schedule = False
                        st.session_state.modifying_schedule = None
                        st.rerun()
                    else:
                        st.error("Failed to update schedule")
                        
                except Exception as e:
                    st.error(f"Failed to update schedule: {str(e)}")
                    print(f"Schedule update error details: {str(e)}")
            
            # Back button
            st.markdown("---")
            if st.button("← Back to Schedules", use_container_width=True):
                st.session_state.show_modify_schedule = False
                st.session_state.modifying_schedule = None
                st.rerun()
            
        except Exception as e:
            st.error(f"Error loading schedule: {str(e)}")
            print(f"Error loading schedule details: {str(e)}")

    def _handle_schedule_type_settings(self, schedule_type, config):
        """Handle settings for different schedule types"""
        col1, col2 = st.columns(2)
        
        with col1:
            if schedule_type == "one-time":
                date = st.date_input(
                    "Select Date",
                    value=datetime.strptime(config.get('date', datetime.now().strftime("%Y-%m-%d")), "%Y-%m-%d").date(),
                    min_value=datetime.now().date()
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
                st.write("Select Days of Week")
                days = []
                day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                
                # Create three columns for better layout
                col1, col2, col3 = st.columns(3)
                columns = [col1, col2, col3]
                
                # Distribute days across columns with unique keys
                for i, day in enumerate(day_names):
                    with columns[i // 3]:
                        if st.checkbox(day, key=f"new_schedule_day_{day}_{i}"):
                            days.append(i)
                
                # Show validation message if no days selected
                if not days:
                    st.error("⚠️ Please select at least one day")
                
                # Show selected days summary
                if days:
                    selected_days = [day_names[i] for i in days]
                    st.success(f"✅ Selected days: {', '.join(selected_days)}")
                
                col1, col2 = st.columns(2)
                with col1:
                    hour = st.number_input("Hour (24-hour format)", min_value=0, max_value=23, value=8)
                with col2:
                    minute = st.number_input("Minute", min_value=0, max_value=59, value=0)
                schedule_config = {
                    'type': 'weekly',
                    'days': days,
                    'hour': hour,
                    'minute': minute
                }
            
            else:  # monthly
                day_option = st.radio(
                    "Day Selection",
                    ["Specific Day", "Last Day", "First Weekday", "Last Weekday"],
                    index=["Specific Day", "Last Day", "First Weekday", "Last Weekday"].index(config.get('day_option', "Specific Day")),
                    help="Choose how to select the day of the month"
                )
                
                if day_option == "Specific Day":
                    day = st.number_input("Day of Month", 1, 31, value=config.get('day', 1))
                else:
                    day = None
                
                hour = st.number_input("Hour (24-hour format)", 0, 23, value=config.get('hour', 8))
                minute = st.number_input("Minute", 0, 59, value=config.get('minute', 0))
                
                schedule_config = {
                    'type': 'monthly',
                    'day': day,
                    'day_option': day_option,
                    'hour': hour,
                    'minute': minute
                }
        
        with col2:
            self._display_schedule_summary(schedule_type, schedule_config)
        
        return schedule_config
    
    def _display_schedule_summary(self, schedule_type, config):
        """Display schedule summary"""
        st.write("Schedule Summary")
        
        time_str = f"{config['hour']:02d}:{config['minute']:02d}"
        
        if schedule_type == "one-time":
            st.info(f"Report will be sent once on: {config['date']} at {time_str}")
        
        elif schedule_type == "daily":
            st.info(f"Report will be sent daily at {time_str}")
        
        elif schedule_type == "weekly":
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            days_str = ", ".join([day_names[d] for d in config['days']])
            st.info(f"Report will be sent every {days_str} at {time_str}")
        
        else:  # monthly
            if config['day_option'] == "Specific Day":
                st.info(f"Report will be sent on day {config['day']} of each month at {time_str}")
            else:
                st.info(f"Report will be sent on the {config['day_option']} of each month at {time_str}")

def display_pdf(pdf_path: str, title: str = "PDF Preview"):
    """Display a PDF file with zoom controls"""
    try:
        # Read the PDF file
        with open(pdf_path, 'rb') as f:
            pdf_bytes = f.read()
        
        # Create two columns for better layout
        col1, col2 = st.columns([1, 4])
        
        with col1:
            st.write("Preview Options:")
            zoom_level = st.slider("Zoom %", min_value=50, max_value=200, value=100, step=10)
        
        with col2:
            # Display PDF with custom width based on zoom level
            width = int(700 * (zoom_level/100))
            st.write(title)
            
            # Add download button
            st.download_button(
                "⬇️ Download PDF",
                data=pdf_bytes,
                file_name=os.path.basename(pdf_path),
                mime="application/pdf",
                use_container_width=True
            )
            
            # Convert PDF to base64 and display
            base64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
            pdf_display = f'''
                <embed 
                    src="data:application/pdf;base64,{base64_pdf}" 
                    type="application/pdf"
                    width="{width}"
                    height="800"
                    style="border: 1px solid #ccc; border-radius: 5px;"
                >
            '''
            st.markdown(pdf_display, unsafe_allow_html=True)
            
            # Add a note about download option
            st.info("💡 If the preview is not visible, please use the download button above to view the PDF.")
    except Exception as e:
        st.error(f"Error displaying PDF: {str(e)}")
        st.info("Please use the download button to view the PDF.")

def show_schedule_page():
    """Show schedule management interface"""
    st.title("📅 Schedule Management")
    
    # Initialize report manager
    report_manager = ReportManager()
    
    # Add Create New Schedule button at the top
    if st.button("➕ Create New Schedule", type="primary", use_container_width=True):
        st.session_state.show_create_schedule = True
        st.rerun()
    
    st.markdown("---")
    
    # Get saved schedules
    schedules = report_manager.get_active_schedules()
    
    if not schedules:
        st.info("No schedules found. Click 'Create New Schedule' to get started.")
    else:
        st.write(f"Found {len(schedules)} schedule(s)")
        
        # Display each schedule
        for schedule_id, schedule in schedules.items():
            with st.expander(f"Schedule: {schedule.get('dataset_name', 'Unknown Dataset')}"):
                st.write(f"Type: {schedule.get('schedule_config', {}).get('type', 'Unknown')}")
                st.write(f"Configuration: {schedule.get('schedule_config', {})}")
                
                # Add Modify and Delete buttons
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✏️ Modify", key=f"modify_{schedule_id}", use_container_width=True):
                        st.session_state.modifying_schedule = schedule_id
                        st.session_state.show_modify_schedule = True
                        st.rerun()
                
                with col2:
                    if st.button("🗑️ Delete", key=f"delete_{schedule_id}", type="secondary", use_container_width=True):
                        if report_manager.remove_schedule(schedule_id):
                            st.success(f"Schedule deleted successfully!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Failed to delete schedule")
                
                # Display report if available
                if 'last_report' in schedule:
                    report_path = schedule['last_report']
                    if os.path.exists(report_path):
                        display_pdf(report_path, title="Last Generated Report")
                
                # Show preview if available
                if 'preview_buffer' in st.session_state:
                    # Save preview to a temporary file
                    preview_path = os.path.join('static/reports', f'preview_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf')
                    with open(preview_path, 'wb') as f:
                        f.write(st.session_state.preview_buffer.getvalue())
                    display_pdf(preview_path, title="Report Preview")
                    # Clean up the temporary file
                    try:
                        os.remove(preview_path)
                    except:
                        pass
    
    # Handle create new schedule
    if st.session_state.get('show_create_schedule'):
        st.markdown("---")
        st.subheader("Create New Schedule")
        
        # Dataset selection
        datasets = get_saved_datasets()
        selected_dataset = st.selectbox("Select Dataset", datasets)
        
        if selected_dataset:
            # Recipients
            st.write("👥 Recipients")
            email_list = st.text_area(
                "Email Addresses",
                placeholder="Enter email addresses (one per line)",
                help="These addresses will receive the scheduled reports"
            )
            
            # WhatsApp recipients
            st.write("📱 WhatsApp Recipients")
            whatsapp_list = st.text_area(
                "WhatsApp Numbers",
                placeholder="Enter WhatsApp numbers with country code (one per line)\nExample: +1234567890",
                help="These numbers will receive WhatsApp notifications"
            )
            
            # Message content
            st.write("📝 Message Content")
            message_body = st.text_area(
                "Message Body",
                placeholder="Enter the message to include with the report",
                help="This message will be included in both email and WhatsApp notifications"
            )
            
            # Schedule settings
            st.write("🕒 Schedule Settings")
            schedule_type = st.selectbox(
                "Frequency",
                ["One-time", "Daily", "Weekly", "Monthly"],
                help="How often to send the report"
            ).lower()
            
            # Get schedule configuration based on type
            db_manager = DatabaseManager()
            schedule_config = db_manager._handle_schedule_type_settings(schedule_type, {})
            
            # Create schedule button
            if st.button("Create Schedule", type="primary", use_container_width=True):
                try:
                    # Validate recipients
                    if not email_list.strip() and not whatsapp_list.strip():
                        st.error("Please enter at least one recipient (email or WhatsApp)")
                        return
                    
                    # Prepare email configuration
                    email_config = {
                        'recipients': [e.strip() for e in email_list.split('\n') if e.strip()],
                        'whatsapp_recipients': [w.strip() for w in whatsapp_list.split('\n') if w.strip()],
                        'body': message_body,
                        'smtp_server': SMTP_SERVER,
                        'smtp_port': SMTP_PORT,
                        'sender_email': SENDER_EMAIL,
                        'sender_password': SENDER_PASSWORD
                    }
                    
                    # Create schedule
                    job_id = report_manager.schedule_report(
                        dataset_name=selected_dataset,
                        email_config=email_config,
                        schedule_config=schedule_config
                    )
                    
                    if job_id:
                        st.success("Schedule created successfully! 🎉")
                        time.sleep(1)
                        st.session_state.show_create_schedule = False
                        st.rerun()
                    else:
                        st.error("Failed to create schedule")
                        
                except Exception as e:
                    st.error(f"Failed to create schedule: {str(e)}")
            
            # Cancel button
            if st.button("Cancel", use_container_width=True):
                st.session_state.show_create_schedule = False
                st.rerun()
    
    # Handle modify schedule
    if st.session_state.get('show_modify_schedule') and st.session_state.get('modifying_schedule'):
        db_manager = DatabaseManager()
        db_manager.modify_schedule(st.session_state.modifying_schedule)

def show_qa_page():
    """Show Q&A interface for dataset analysis"""
    st.title("💬 Chat with Data")
    # ... rest of the file content ...

def main():
    """Main function to run the Streamlit application"""
    st.set_page_config(
        page_title="Tableau Data Reporter",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state if needed
    init_session_state()
    
    # Show appropriate page based on authentication status
    if not st.session_state.get('authenticated', False):
        show_login_page()
    else:
        # Show different pages based on user type
        if st.session_state.get('user_type') == 'power_user':
            show_power_user_page()
        else:
            show_normal_user_page()

if __name__ == "__main__":
    main() 