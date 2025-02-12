import sqlite3
import hashlib
import os
from pathlib import Path

class UserManagement:
    def __init__(self):
        """Initialize user manager"""
        # Create data directory if it doesn't exist
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Set database path
        self.db_path = str(self.data_dir / "tableau_data.db")
        print(f"Database path: {self.db_path}")  # Debug print
        
        # Initialize database
        self.setup_database()
    
    def hash_password(self, password: str) -> str:
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def setup_database(self):
        """Set up the database tables"""
        try:
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
                
                # Drop existing superadmin to ensure clean state
                cursor.execute("DELETE FROM users WHERE username = 'superadmin'")
                
                # Create superadmin user
                hashed_password = self.hash_password('superadmin')
                cursor.execute('''
                    INSERT INTO users (username, password, role, permission_type, organization_id, email)
                    VALUES ('superadmin', ?, 'superadmin', 'superadmin', NULL, 'admin@example.com')
                ''', (hashed_password,))
                print("Created/Updated superadmin user with password: superadmin")
                
                conn.commit()
                print("Database setup completed successfully")
        except Exception as e:
            print(f"Error setting up database: {str(e)}")
            raise
    
    def verify_user(self, username: str, password: str):
        """Verify user credentials and return user data"""
        try:
            print(f"Verifying user: {username}")  # Debug print
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                hashed_password = self.hash_password(password)
                
                # Special handling for superadmin
                if username == 'superadmin':
                    # Ensure superadmin has correct permissions
                    cursor.execute("""
                        UPDATE users 
                        SET permission_type = 'superadmin', 
                            role = 'superadmin'
                        WHERE username = 'superadmin'
                    """)
                    conn.commit()
                
                # Get user data with consistent role and permission_type
                cursor.execute('''
                    SELECT 
                        u.id, 
                        u.username, 
                        CASE 
                            WHEN u.username = 'superadmin' THEN 'superadmin'
                            ELSE COALESCE(u.permission_type, u.role)
                        END as role,
                        CASE 
                            WHEN u.username = 'superadmin' THEN 'superadmin'
                            ELSE COALESCE(u.permission_type, 'normal')
                        END as permission_type,
                        u.organization_id, 
                        o.name as org_name
                    FROM users u
                    LEFT JOIN organizations o ON u.organization_id = o.id
                    WHERE u.username = ? AND u.password = ?
                ''', (username, hashed_password))
                
                user = cursor.fetchone()
                if user:
                    print(f"Found user: {user}")  # Debug print
                    
                    # Ensure role and permission_type are in sync for non-superadmin users
                    if username != 'superadmin':
                        cursor.execute('''
                            UPDATE users 
                            SET role = permission_type
                            WHERE username = ? AND role != permission_type
                        ''', (username,))
                        conn.commit()
                    
                    return user
                print("No user found with provided credentials")  # Debug print
                return None
        except Exception as e:
            print(f"Error verifying user: {str(e)}")
            return None
    
    def update_user_permission(self, username: str, permission_type: str) -> bool:
        """Update user's permission type and role"""
        try:
            print(f"Updating permission for {username} to {permission_type}")  # Debug print
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Don't allow updating superadmin
                cursor.execute("SELECT role FROM users WHERE username = ?", (username,))
                current_role = cursor.fetchone()
                if current_role and current_role[0] == 'superadmin':
                    print("Cannot update superadmin permissions")
                    return False
                
                # Update both permission_type and role to maintain consistency
                cursor.execute('''
                    UPDATE users 
                    SET permission_type = ?,
                        role = ?
                    WHERE username = ?
                ''', (permission_type, permission_type, username))
                
                conn.commit()
                success = cursor.rowcount > 0
                print(f"Update {'successful' if success else 'failed'}")  # Debug print
                return success
                
        except Exception as e:
            print(f"Error updating permission: {str(e)}")
            return False
    
    def add_user_to_org(self, username: str, password: str, org_id: int = None, permission_type: str = 'normal', email: str = None):
        """Add a new user to an organization"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # If no org_id provided, create a new organization for the user
                if org_id is None and username != 'superadmin':
                    org_name = f"{username}'s Organization"
                    cursor.execute('INSERT INTO organizations (name) VALUES (?)', (org_name,))
                    org_id = cursor.lastrowid
                    print(f"Created new organization: {org_name} with ID: {org_id}")
                
                # Add user with specified permission type and hashed password
                hashed_password = self.hash_password(password)
                cursor.execute('''
                    INSERT INTO users (username, password, role, permission_type, organization_id, email)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (username, hashed_password, permission_type, permission_type, org_id, email))
                
                conn.commit()
                print(f"Added user {username} to organization {org_id} with permission {permission_type}")
                return True
                
        except sqlite3.IntegrityError as e:
            print(f"Database integrity error: {str(e)}")
            if "UNIQUE constraint failed" in str(e):
                raise ValueError("Username already exists")
            raise
        except Exception as e:
            print(f"Error adding user: {str(e)}")
            return False 
    
    def get_all_users(self):
        """Get all users with their organization details"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT 
                        u.username,
                        u.role,
                        u.permission_type,
                        u.email,
                        o.name as org_name
                    FROM users u
                    LEFT JOIN organizations o ON u.organization_id = o.id
                    ORDER BY u.username
                ''')
                users = cursor.fetchall()
                print(f"Found {len(users)} users")  # Debug print
                return users
        except Exception as e:
            print(f"Error getting users: {str(e)}")
            return [] 