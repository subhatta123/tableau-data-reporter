import streamlit as st
import sqlite3
import hashlib
from datetime import datetime
import pandas as pd
from database_manager import DatabaseManager
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class UserManager:
    def __init__(self):
        """Initialize user manager"""
        self.db_path = 'data/tableau_data.db'
        self.setup_database()
    
    def hash_password(self, password: str) -> str:
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def setup_database(self):
        """Set up the database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create organizations table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS organizations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL
                )
            ''')
            
            # Create users table with permission field and email if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    role TEXT NOT NULL,
                    permission_type TEXT DEFAULT 'normal',
                    organization_id INTEGER,
                    email TEXT,
                    FOREIGN KEY (organization_id) REFERENCES organizations(id)
                )
            ''')
            
            # Create default superadmin if not exists
            cursor.execute("SELECT * FROM users WHERE username = 'superadmin'")
            if not cursor.fetchone():
                hashed_password = self.hash_password('superadmin')
                cursor.execute('''
                    INSERT INTO users (username, password, role, permission_type, organization_id, email)
                    VALUES ('superadmin', ?, 'superadmin', 'power', NULL, 'admin@example.com')
                ''', (hashed_password,))
                print("Created superadmin user")
            
            conn.commit()
    
    def verify_user(self, username: str, password: str):
        """Verify user credentials and return user data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                hashed_password = self.hash_password(password)
                cursor.execute('''
                    SELECT 
                        u.id, 
                        u.username, 
                        u.role, 
                        COALESCE(u.permission_type, 'normal') as permission_type,
                        u.organization_id, 
                        o.name as org_name
                    FROM users u
                    LEFT JOIN organizations o ON u.organization_id = o.id
                    WHERE u.username = ? AND u.password = ?
                ''', (username, hashed_password))
                
                user = cursor.fetchone()
                if user:
                    print(f"Found user: {user}")  # Debug print
                    return user
                return None
        except Exception as e:
            print(f"Error verifying user: {str(e)}")
            return None
    
    def send_welcome_email(self, email, username):
        """Send welcome email to new user"""
        try:
            sender_email = "tableautoexcel@gmail.com"  # Gmail address
            sender_password = "ptvy yerb ymbj fngu"   # Gmail app password
            
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = email
            msg['Subject'] = "Welcome to Tableau Data Reporter!"
            
            body = f"""
            Welcome to Tableau Data Reporter, {username}!
            
            Your account has been successfully created. You can now log in using your credentials.
            
            Best regards,
            The Tableau Data Reporter Team
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            
            return True
        except Exception as e:
            print(f"Error sending email: {str(e)}")
            return False
    
    def create_organization(self, org_name):
        """Create a new organization"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO organizations (name) VALUES (?)', (org_name,))
                org_id = cursor.lastrowid
                conn.commit()
                return org_id
        except Exception as e:
            print(f"Error creating organization: {str(e)}")
            return None

    def add_user_to_org(self, username: str, password: str, org_id: int = None, permission_type: str = 'normal', email: str = None):
        """Add a new user to an organization"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # If no org_id provided, create a new organization for the user
                if org_id is None and username != 'superadmin':
                    org_name = f"{username}'s Organization"
                    org_id = self.create_organization(org_name)
                    if not org_id:
                        raise ValueError("Failed to create organization")
                
                # Add user with specified permission type and hashed password
                hashed_password = self.hash_password(password)
                cursor.execute('''
                    INSERT INTO users (username, password, role, permission_type, organization_id, email)
                    VALUES (?, ?, 'user', ?, ?, ?)
                ''', (username, hashed_password, permission_type, org_id, email))
                
                conn.commit()
                print(f"Added user {username} to organization {org_id} with permission {permission_type}")
                
                # Send welcome email
                if email and username != 'superadmin':
                    self.send_welcome_email(email, username)
                
                return True
        except sqlite3.IntegrityError:
            raise ValueError("Username already exists")
        except Exception as e:
            print(f"Error adding user: {str(e)}")
            return False
    
    def update_user_permission(self, username: str, permission_type: str):
        """Update user's permission type"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE users 
                    SET permission_type = ?
                    WHERE username = ? AND role != 'superadmin'
                ''', (permission_type, username))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating permission: {str(e)}")
            return False
    
    def get_users_by_org(self, org_id: int):
        """Get all users in an organization"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT u.username, u.role, u.permission_type, u.email
                    FROM users u
                    WHERE u.organization_id = ?
                    ORDER BY u.username
                ''', (org_id,))
                return cursor.fetchall()
        except Exception as e:
            print(f"Error getting users: {str(e)}")
            return []

    def get_organizations(self):
        """Get all organizations"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT id, name FROM organizations')
                return cursor.fetchall()
        except Exception as e:
            print(f"Error getting organizations: {str(e)}")
            return []

    def get_organization_name(self, org_id):
        """Get organization name by ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT name FROM organizations WHERE id = ?', (org_id,))
                result = cursor.fetchone()
                return result[0] if result else "Unknown"
        except Exception as e:
            print(f"Error getting organization name: {str(e)}")
            return "Unknown"

    def get_all_users(self):
        """Get all users with their organization details"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT u.username, u.role, u.permission_type, u.email, o.name as org_name
                    FROM users u
                    LEFT JOIN organizations o ON u.organization_id = o.id
                    ORDER BY u.username
                ''')
                return cursor.fetchall()
        except Exception as e:
            print(f"Error getting users: {str(e)}")
            return []

    def delete_organization(self, org_id: int):
        """Delete organization and its users"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # First delete all users in the organization
                cursor.execute('DELETE FROM users WHERE organization_id = ?', (org_id,))
                # Then delete the organization
                cursor.execute('DELETE FROM organizations WHERE id = ?', (org_id,))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error deleting organization: {str(e)}")
            return False

def get_saved_datasets():
    """Get list of saved datasets from the database"""
    try:
        conn = sqlite3.connect('data/tableau_data.db')
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT dataset_name FROM datasets")
        datasets = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return datasets
    except Exception as e:
        print(f"Error getting datasets: {str(e)}")
        return []

def load_dataset(dataset_name):
    """Load dataset from database"""
    try:
        conn = sqlite3.connect('data/tableau_data.db')
        query = f"SELECT * FROM datasets WHERE dataset_name = ?"
        df = pd.read_sql_query(query, conn, params=(dataset_name,))
        conn.close()
        return df
    except Exception as e:
        print(f"Error loading dataset: {str(e)}")
        return None

def show_login_page():
    """Show login page"""
    st.title("Welcome to Tableau Data Reporter")
    
    # Initialize user manager
    user_manager = UserManager()
    
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
                print(f"Logged in user permission type: {user[3]}")  # Debug print
                st.success("Login successful!")
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
                        st.success("Registration successful! Please wait for admin to assign you to an organization.")
                except ValueError as e:
                    st.error(str(e))

def show_admin_page():
    """Show admin interface"""
    st.title("Admin Dashboard")
    
    # Only show organization management for superadmin
    if st.session_state.user['role'] == 'superadmin':
        user_manager = UserManager()
        tabs = st.tabs(["All Users", "Organizations", "User Permissions"])
        
        with tabs[0]:
            st.subheader("👥 All Registered Users")
            users = user_manager.get_all_users()
            
            if users:
                # Create a DataFrame for better display
                df = pd.DataFrame(users, columns=['Username', 'Role', 'Permission Type', 'Email', 'Organization'])
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No registered users found.")
        
        with tabs[1]:
            st.subheader("🏢 Manage Organizations")
            
            # Show existing organizations
            organizations = user_manager.get_organizations()
            if organizations:
                st.write("### Existing Organizations")
                for org_id, org_name in organizations:
                    with st.expander(f"📍 {org_name}"):
                        col1, col2 = st.columns([5, 1])
                        with col1:
                            st.write(f"**Organization ID:** {org_id}")
                            users = user_manager.get_users_by_org(org_id)
                            if users:
                                st.write("**Members:**")
                                for username, role, perm_type, email in users:
                                    st.write(f"- {username} ({role}, {perm_type}, {email})")
                            else:
                                st.info("No users in this organization")
                        with col2:
                            if st.button("🗑️", key=f"delete_org_{org_id}"):
                                if user_manager.delete_organization(org_id):
                                    st.success(f"Organization '{org_name}' deleted successfully!")
                                    st.rerun()
                                else:
                                    st.error("Failed to delete organization")
            
            # Add new organization form
            st.write("### Add New Organization")
            with st.form("add_org_form"):
                new_org_name = st.text_input("Organization Name")
                new_username = st.text_input("Admin Username")
                new_password = st.text_input("Admin Password", type="password")
                new_email = st.text_input("Admin Email")
                
                if st.form_submit_button("Create Organization"):
                    if all([new_org_name, new_username, new_password, new_email]):
                        # First create the organization
                        org_id = user_manager.create_organization(new_org_name)
                        if org_id:
                            try:
                                # Then create the user with normal permissions
                                if user_manager.add_user_to_org(
                                    username=new_username,
                                    password=new_password,
                                    org_id=org_id,
                                    permission_type='normal',
                                    email=new_email
                                ):
                                    st.success(f"Organization '{new_org_name}' and user created successfully!")
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    # If user creation fails, delete the organization
                                    user_manager.delete_organization(org_id)
                                    st.error("Failed to create user")
                            except Exception as e:
                                # If anything goes wrong, clean up the organization
                                user_manager.delete_organization(org_id)
                                st.error(f"Error creating user: {str(e)}")
                        else:
                            st.error("Failed to create organization")
                    else:
                        st.error("All fields are required")
        
        with tabs[2]:
            st.subheader("🔑 User Permissions")
            
            # Organization selection for permissions
            org_id = st.selectbox(
                "Select Organization",
                options=[org[0] for org in user_manager.get_organizations()],
                format_func=lambda x: user_manager.get_organization_name(x),
                key="perm_org_select"
            )
            
            if org_id:
                users = user_manager.get_users_by_org(org_id)
                if users:
                    for username, role, current_perm, email in users:
                        if role != 'superadmin':
                            col1, col2, col3 = st.columns([2, 2, 1])
                            with col1:
                                st.write(f"**{username}** ({email})")
                            with col2:
                                new_perm = st.selectbox(
                                    "Permission Type",
                                    options=["normal", "power"],
                                    index=0 if current_perm == "normal" else 1,
                                    key=f"perm_{username}"
                                )
                            with col3:
                                if new_perm != current_perm:
                                    if st.button("Update", key=f"update_{username}"):
                                        if user_manager.update_user_permission(username, new_perm):
                                            st.success(f"Updated {username}'s permission to {new_perm}")
                                            st.rerun()
                                        else:
                                            st.error("Failed to update permission")

def show_help():
    """Show help information in a popup"""
    st.info("""
    ### 📚 Help Guide
    - **Normal Users**: Can schedule reports and delete datasets
    - **Power Users**: Additional access to dashboards and data analysis
    - **Schedule Reports**: Set up automated report delivery
    - **Delete Dataset**: Remove datasets you no longer need
    """)

def show_user_page():
    """Show regular user interface based on permissions"""
    # Initialize database
    db_manager = DatabaseManager()
    db_url = db_manager.ensure_database_running()
    
    # Get user's permission type from session state
    permission_type = st.session_state.user.get('permission_type', 'normal')
    print(f"Current user permission type: {permission_type}")  # Debug print
    
    # Sidebar setup
    with st.sidebar:
        st.title("Navigation")
        if st.button("Show Help"):
            show_help()
        st.markdown("---")
        
        # Show saved datasets with permission-based buttons
        show_saved_datasets(permission_type)  # Pass the permission type directly
        
        # Add Schedule Reports button (available to all)
        st.markdown("---")
        if st.button("📅 Schedule Reports"):
            st.session_state.show_schedule_page = True
            st.rerun()
        
        st.markdown("---")
        show_logout_button()

def show_logout_button():
    """Show logout button in sidebar"""
    if st.button("🚪 Logout", type="primary"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

def show_saved_datasets(permission_type='normal'):
    """Show list of saved datasets with permission-based options"""
    st.subheader("📊 Saved Datasets")
    
    datasets = get_saved_datasets()
    if not datasets:
        st.info("No datasets found. Please download some data first.")
        return
    
    for dataset in datasets:
        with st.expander(f"📊 {dataset}", expanded=True):
            st.write(f"**{dataset}**")
            df_preview = load_dataset(dataset)
            if df_preview is not None:
                st.dataframe(df_preview.head(), use_container_width=True)
                st.caption(f"Total rows: {len(df_preview)}")
            
            st.markdown("---")
            
            # Create button columns based on permission type
            if permission_type == 'power':
                # Power users see all buttons
                button_cols = st.columns([1, 1, 1, 1])
                
                with button_cols[0]:
                    if st.button("📊 Dashboard", key=f"dashboard_{dataset}", use_container_width=True):
                        show_dashboard(dataset)
                
                with button_cols[1]:
                    if st.button("❓ Ask Questions", key=f"ask_{dataset}", use_container_width=True):
                        show_qa(dataset)
                
                with button_cols[2]:
                    if st.button("📅 Schedule", key=f"schedule_{dataset}", use_container_width=True):
                        show_schedule(dataset)
                
                with button_cols[3]:
                    if st.button("🗑️ Delete", key=f"delete_{dataset}", type="secondary", use_container_width=True):
                        delete_dataset(dataset)
            else:
                # Normal users only see schedule and delete
                button_cols = st.columns([1, 1])
                
                with button_cols[0]:
                    if st.button("📅 Schedule", key=f"schedule_{dataset}", use_container_width=True):
                        show_schedule(dataset)
                
                with button_cols[1]:
                    if st.button("🗑️ Delete", key=f"delete_{dataset}", type="secondary", use_container_width=True):
                        delete_dataset(dataset)
        
        st.markdown("---")

def delete_dataset(dataset_name):
    """Delete dataset from database"""
    try:
        conn = sqlite3.connect('data/tableau_data.db')
        cursor = conn.cursor()
        cursor.execute("DELETE FROM datasets WHERE dataset_name = ?", (dataset_name,))
        conn.commit()
        cursor.close()
        conn.close()
        st.success(f"Dataset {dataset_name} deleted successfully!")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Error deleting dataset: {str(e)}")

def show_dashboard(dataset_name):
    """Show dashboard page for dataset"""
    st.session_state.selected_dataset = dataset_name
    st.session_state.show_dashboard_page = True
    st.rerun()

def show_qa(dataset_name):
    """Show Q&A page for dataset"""
    st.session_state.selected_dataset = dataset_name
    st.session_state.show_qa_page = True
    st.rerun()

def show_schedule(dataset_name):
    """Show schedule page for dataset"""
    st.session_state.selected_dataset = dataset_name
    st.session_state.show_schedule_page = True
    
    # Add email body input
    st.session_state.email_body = st.text_area(
        "Email Body",
        placeholder="Enter the message you want to include in the email...",
        height=150,
        key=f"email_body_{dataset_name}"
    )
    
    st.rerun()

def show_profile_page():
    """Show user profile page"""
    st.title("User Profile")
    
    if not st.session_state.get('user'):
        st.error("Access denied")
        return
    
    user = st.session_state.user
    st.write(f"Username: {user['username']}")
    st.write(f"Organization: {user['organization_name']}")
    st.write(f"Role: {user['role']}")
    
    # Add change password functionality
    st.subheader("Change Password")
    current_password = st.text_input("Current Password", type="password")
    new_password = st.text_input("New Password", type="password")
    confirm_password = st.text_input("Confirm New Password", type="password")
    
    if st.button("Change Password"):
        if not all([current_password, new_password, confirm_password]):
            st.error("All fields are required")
        elif new_password != confirm_password:
            st.error("New passwords don't match")
        else:
            user_manager = UserManager()
            # Verify current password
            if user_manager.verify_user(user['username'], current_password):
                # Update password in database
                try:
                    conn = sqlite3.connect('data/tableau_data.db', timeout=20)
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            "UPDATE users SET password = ? WHERE id = ?",
                            (user_manager.hash_password(new_password), user['id'])
                        )
                        conn.commit()
                        st.success("Password updated successfully!")
                    finally:
                        cursor.close()
                        conn.close()
                except Exception as e:
                    st.error(f"Failed to update password: {str(e)}")
            else:
                st.error("Current password is incorrect")

def show_logout_page():
    """Show logout page"""
    if st.button("Logout"):
        st.session_state.user = None
        st.rerun() 