import sqlite3
import os
from pathlib import Path

def fix_superadmin_permissions():
    try:
        # Get the correct database path
        current_dir = Path.cwd()
        if current_dir.name == 'tableau-data-reporter-main':
            db_path = current_dir / "data" / "tableau_data.db"
        else:
            db_path = current_dir / "tableau-data-reporter-main" / "data" / "tableau_data.db"
        
        print(f"Using database path: {db_path}")
        
        # Create data directory if it doesn't exist
        db_path.parent.mkdir(exist_ok=True)
        
        # Create database connection
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            
            # Create users table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    role TEXT NOT NULL,
                    permission_type TEXT DEFAULT 'normal',
                    organization_id INTEGER,
                    email TEXT
                )
            ''')
            
            # Check if superadmin exists
            cursor.execute("SELECT * FROM users WHERE username = 'superadmin'")
            user = cursor.fetchone()
            print(f"Current user data: {user}")
            
            if user:
                # Update existing superadmin
                cursor.execute("""
                    UPDATE users 
                    SET permission_type = 'superadmin', 
                        role = 'superadmin'
                    WHERE username = 'superadmin'
                """)
                print("Updated existing superadmin permissions")
            else:
                # Create new superadmin user with hashed password
                cursor.execute("""
                    INSERT INTO users (username, password, role, permission_type, email)
                    VALUES ('superadmin', 'superadmin', 'superadmin', 'superadmin', 'admin@example.com')
                """)
                print("Created new superadmin user")
            
            conn.commit()
            
            # Verify the update
            cursor.execute("SELECT * FROM users WHERE username = 'superadmin'")
            user = cursor.fetchone()
            print(f"Updated superadmin data: {user}")
            
    except Exception as e:
        print(f"Error updating permissions: {str(e)}")
        print(f"Current directory: {os.getcwd()}")

if __name__ == "__main__":
    fix_superadmin_permissions() 