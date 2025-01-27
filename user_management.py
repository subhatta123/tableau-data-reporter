import streamlit as st
import sqlite3
import hashlib
from datetime import datetime
import pandas as pd
from database_manager import DatabaseManager
import time

class UserManager:
    def __init__(self):
        self.init_user_db()
        
    def init_user_db(self):
        """Initialize user database with organization support"""
        try:
            conn = sqlite3.connect('data/tableau_data.db', timeout=20)
            cursor = conn.cursor()
            
            try:
                # Only create tables if they don't exist, don't drop existing ones
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS organizations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        description TEXT,
                        created_at TIMESTAMP,
                        is_active INTEGER DEFAULT 1
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        password TEXT NOT NULL,
                        email TEXT NOT NULL,
                        role TEXT NOT NULL CHECK(role IN ('user', 'admin', 'superadmin')),
                        organization_id INTEGER,
                        created_at TIMESTAMP,
                        last_login TIMESTAMP,
                        is_active INTEGER DEFAULT 1,
                        FOREIGN KEY (organization_id) REFERENCES organizations(id),
                        UNIQUE(username, organization_id),
                        UNIQUE(email, organization_id)
                    )
                ''')
                
                # Create superadmin user if not exists
                cursor.execute("SELECT * FROM users WHERE username = 'superadmin'")
                if not cursor.fetchone():
                    hashed_password = self.hash_password('superadmin')
                    cursor.execute('''
                        INSERT INTO users (username, password, email, role, created_at, is_active)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', ('superadmin', hashed_password, 'superadmin@example.com', 'superadmin', datetime.now(), 1))
                    print("Created superadmin user")
                
                conn.commit()
                print("Database initialized successfully")
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            print(f"Database initialization error: {str(e)}")
            st.error(f"Failed to initialize database: {str(e)}")

    def hash_password(self, password):
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()

    def create_organization(self, name: str, description: str = None) -> int:
        """Create a new organization"""
        try:
            conn = sqlite3.connect('data/tableau_data.db', timeout=20)
            cursor = conn.cursor()
            try:
                print(f"Creating organization: {name}")
                cursor.execute('''
                    INSERT INTO organizations (name, description, created_at, is_active)
                    VALUES (?, ?, ?, 1)
                ''', (name, description, datetime.now()))
                conn.commit()
                org_id = cursor.lastrowid
                print(f"Created organization with ID: {org_id}")
                return org_id
            except Exception as e:
                print(f"Error creating organization: {str(e)}")
                return None
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            print(f"Database connection error: {str(e)}")
            return None

    def create_user(self, username: str, password: str, email: str, organization_id: int, role: str = 'user') -> bool:
        """Create a new user within an organization"""
        try:
            conn = sqlite3.connect('data/tableau_data.db', timeout=20)
            cursor = conn.cursor()
            try:
                print(f"Creating user: {username} with role: {role}")
                hashed_password = self.hash_password(password)
                cursor.execute('''
                    INSERT INTO users (username, password, email, organization_id, role, created_at, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (username, hashed_password, email, organization_id, role, datetime.now(), 1))
                conn.commit()
                print(f"Created user successfully")
                return True
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            print(f"Error creating user: {str(e)}")
            st.error(f"Failed to create user: {str(e)}")
            return False

    def verify_user(self, username: str, password: str):
        """Verify user credentials"""
        try:
            conn = sqlite3.connect('data/tableau_data.db', timeout=20)
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    SELECT u.id, u.username, u.role, u.is_active, u.organization_id, o.name as org_name
                    FROM users u
                    LEFT JOIN organizations o ON u.organization_id = o.id
                    WHERE u.username = ? AND u.password = ? AND u.is_active = 1
                ''', (username, self.hash_password(password)))
                user = cursor.fetchone()
                
                if user:
                    # Update last login
                    cursor.execute(
                        "UPDATE users SET last_login = ? WHERE id = ?",
                        (datetime.now(), user[0])
                    )
                    conn.commit()
                    return user
                return None
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            st.error(f"Login failed: {str(e)}")
            return None

    def delete_user(self, user_id: int, organization_id: int = None) -> bool:
        """Delete a user (optionally within an organization)"""
        try:
            conn = sqlite3.connect('data/tableau_data.db', timeout=20)
            cursor = conn.cursor()
            try:
                if organization_id:
                    cursor.execute('DELETE FROM users WHERE id = ? AND organization_id = ?', (user_id, organization_id))
                else:
                    cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
                conn.commit()
                return True
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            st.error(f"Failed to delete user: {str(e)}")
            return False

    def get_organizations(self):
        """Get all organizations"""
        try:
            conn = sqlite3.connect('data/tableau_data.db', timeout=20)
            return pd.read_sql('''
                SELECT id, name, description, created_at, is_active
                FROM organizations
                ORDER BY name
            ''', conn)
        except Exception as e:
            st.error(f"Failed to get organizations: {str(e)}")
            return pd.DataFrame()

    def get_organization_users(self, organization_id: int):
        """Get all users in an organization"""
        try:
            conn = sqlite3.connect('data/tableau_data.db', timeout=20)
            return pd.read_sql('''
                SELECT id, username, email, role, is_active
                FROM users
                WHERE organization_id = ? AND role != 'superadmin'
                ORDER BY username
            ''', conn, params=[organization_id])
        except Exception as e:
            st.error(f"Failed to get organization users: {str(e)}")
            return pd.DataFrame()

    def get_pending_users(self):
        """Get all pending users without organization"""
        try:
            conn = sqlite3.connect('data/tableau_data.db', timeout=20)
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    SELECT id, username, email, created_at 
                    FROM users 
                    WHERE organization_id IS NULL 
                    AND role = 'user'
                    AND is_active = 1
                    ORDER BY created_at DESC
                ''')
                users = cursor.fetchall()
                return users
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            print(f"Error getting pending users: {str(e)}")
            return []

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
                st.session_state.user = {
                    'id': user[0],
                    'username': user[1],
                    'role': user[2],
                    'organization_id': user[4],
                    'organization_name': user[5]
                }
                st.success("Login successful!")
                st.rerun()
            else:
                st.error("Invalid credentials")
    
    with tab2:
        st.subheader("Register")
        new_username = st.text_input("Username", key="reg_username")
        new_password = st.text_input("Password", type="password", key="reg_password")
        confirm_password = st.text_input("Confirm Password", type="password")
        email = st.text_input("Email")
        
        if st.button("Register"):
            if new_password != confirm_password:
                st.error("Passwords don't match")
            elif not all([new_username, new_password, email]):
                st.error("All fields are required")
            else:
                # Create user without organization (will be assigned by superadmin)
                if user_manager.create_user(new_username, new_password, email, None, 'user'):
                    st.success("Registration successful! Please wait for admin to assign you to an organization.")

def show_admin_page():
    """Show admin dashboard"""
    if not st.session_state.get('user'):
        st.error("Access denied")
        return
    
    user_manager = UserManager()
    user = st.session_state.user
    
    if user['role'] == 'superadmin':
        st.title("Superadmin Dashboard")
        
        # Create tabs for different management sections
        org_tab, users_tab = st.tabs(["Organizations", "Pending Users"])
        
        with org_tab:
            st.subheader("Organization Management")
            
            # Create new organization section
            with st.expander("Create New Organization", expanded=True):
                org_name = st.text_input("Organization Name")
                org_description = st.text_area("Description")
                if st.button("Create Organization", type="primary"):
                    if org_name:
                        org_id = user_manager.create_organization(org_name, org_description)
                        if org_id:
                            st.success(f"Organization '{org_name}' created successfully!")
                            time.sleep(1)  # Give time for the success message
                            st.rerun()
                        else:
                            st.error("Failed to create organization")
                    else:
                        st.error("Organization name is required")
            
            # List and manage organizations
            organizations = user_manager.get_organizations()
            if not organizations.empty:
                for _, org in organizations.iterrows():
                    with st.expander(f"📊 {org['name']}", expanded=True):
                        st.write(f"**Description:** {org['description'] or 'No description'}")
                        st.write(f"**Created:** {org['created_at']}")
                        
                        # Show users in organization
                        st.subheader("Organization Users")
                        users = user_manager.get_organization_users(org['id'])
                        if not users.empty:
                            for _, user in users.iterrows():
                                cols = st.columns([3, 1, 1])
                                with cols[0]:
                                    st.write(f"**{user['username']}** ({user['email']})")
                                with cols[1]:
                                    st.write(f"Role: {user['role']}")
                                with cols[2]:
                                    if st.button("🗑️", key=f"delete_{org['id']}_{user['id']}", help="Delete user"):
                                        if user_manager.delete_user(user['id'], org['id']):
                                            st.success(f"Deleted user {user['username']}")
                                            time.sleep(1)
                                            st.rerun()
            else:
                st.info("No organizations created yet.")
        
        with users_tab:
            st.subheader("Pending Users")
            pending_users = user_manager.get_pending_users()
            
            if pending_users:
                organizations = user_manager.get_organizations()
                if organizations.empty:
                    st.warning("Please create an organization first before assigning users.")
                else:
                    for user_data in pending_users:
                        cols = st.columns([2, 2, 2, 1])
                        with cols[0]:
                            st.write(f"**{user_data[1]}**")  # username
                        with cols[1]:
                            st.write(user_data[2])  # email
                        with cols[2]:
                            org_names = organizations['name'].tolist()
                            selected_org = st.selectbox(
                                "Assign to Organization",
                                org_names,
                                key=f"assign_org_{user_data[0]}"
                            )
                            org_id = organizations[organizations['name'] == selected_org]['id'].iloc[0]
                        with cols[3]:
                            if st.button("Assign", key=f"assign_{user_data[0]}"):
                                try:
                                    with sqlite3.connect('data/tableau_data.db') as conn:
                                        conn.execute(
                                            "UPDATE users SET organization_id = ? WHERE id = ?",
                                            (org_id, user_data[0])
                                        )
                                        conn.commit()
                                    st.success(f"Assigned {user_data[1]} to {selected_org}")
                                    time.sleep(1)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to assign user: {str(e)}")
            else:
                st.info("No pending users to assign")
    
    elif user['role'] == 'admin':
        st.title("Admin Dashboard")
        st.subheader(f"Organization: {user['organization_name']}")
        
        # Show users in organization
        users = user_manager.get_organization_users(user['organization_id'])
        if not users.empty:
            st.write("### Users")
            for _, org_user in users.iterrows():
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.write(f"**{org_user['username']}**")
                with col2:
                    st.write(org_user['email'])
                with col3:
                    if st.button("Delete", key=f"delete_{org_user['id']}"):
                        if user_manager.delete_user(org_user['id'], user['organization_id']):
                            st.success(f"Deleted user {org_user['username']}")
                            st.rerun()
        
        # Add new user to organization
        with st.expander("Add New User", expanded=True):
            cols = st.columns([2, 2, 2, 1])
            with cols[0]:
                new_username = st.text_input("Username")
            with cols[1]:
                new_password = st.text_input("Password", type="password")
            with cols[2]:
                new_email = st.text_input("Email")
            with cols[3]:
                if st.button("Add User", use_container_width=True):
                    if all([new_username, new_password, new_email]):
                        if user_manager.create_user(new_username, new_password, new_email, user['organization_id'], 'user'):
                            st.success(f"Added user {new_username}")
                            st.rerun()
                    else:
                        st.error("All fields are required")
    
    else:
        st.error("Access denied")

def show_user_page():
    """Show user dashboard"""
    st.title("User Dashboard")
    
    if not st.session_state.get('user'):
        st.error("Access denied")
        return
    
    user = st.session_state.user
    st.write(f"Welcome, {user['username']}!")
    st.write(f"Organization: {user['organization_name']}")
    st.write(f"Role: {user['role']}")
    
    # Show available datasets for the organization
    st.subheader("Available Datasets")
    datasets = DatabaseManager().list_tables(include_internal=False)
    for dataset in datasets:
        st.write(f"- {dataset}")

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