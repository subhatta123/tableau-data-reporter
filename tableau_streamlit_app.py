import streamlit as st
import pandas as pd
import sqlite3
import os
from pathlib import Path
from tableau_data_app import TableauConnector
from typing import List
from datetime import datetime
from data_analyzer import show_analysis_tab
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import io
import time
from report_manager import ReportManager
from user_management import UserManager, show_login_page, show_profile_page, show_admin_page
from dashboard_manager import DashboardManager, show_dashboard_page
import json
import uuid
import numpy as np
import plotly.graph_objects as go
from streamlit.runtime.scriptrunner import get_script_run_ctx
from streamlit.runtime.state import SessionState
from scipy import stats

# Load environment variables at the very start
load_dotenv()

# Verify API key is loaded
api_key = os.getenv('OPENAI_API_KEY')
if not api_key or api_key == 'your-api-key-here':
    st.error("OpenAI API key not found or invalid. Please check your .env file.")
    st.stop()

class DatabaseManager:
    def __init__(self):
        # Create data directory if it doesn't exist
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # SQLite database path
        self.db_path = self.data_dir / "tableau_data.db"
        self.db_url = f"sqlite:///{self.db_path}"
        
    def ensure_database_running(self):
        """Initialize SQLite database"""
        try:
            # Test database connection
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Create a test table to verify connection
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS app_info (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                """)
                conn.commit()
            
            st.success("✅ Database is ready!")
            return self.db_url
            
        except Exception as e:
            st.error(f"""
            Failed to initialize database. Error: {str(e)}
            
            Please ensure:
            1. The application has write permissions to the data directory
            2. Sufficient disk space is available
            3. SQLite is working properly
            """)
            return None
    
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
            'sqlite_stat4'
        }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Only get tables that are not in INTERNAL_TABLES
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' 
                    AND name NOT IN (""" + ','.join(['?']*len(INTERNAL_TABLES)) + ")",
                    tuple(INTERNAL_TABLES)
                )
                tables = cursor.fetchall()
                
                dataset_tables = []
                for (table_name,) in tables:
                    try:
                        cursor.execute(f"SELECT * FROM '{table_name}' LIMIT 0")
                        columns = [description[0] for description in cursor.description]
                        if 'View_Names' in columns:
                            dataset_tables.append(table_name)
                    except:
                        continue
                
                return dataset_tables
                
        except Exception as e:
            st.error(f"Failed to list tables: {str(e)}")
            return []
    
    def get_table_preview(self, table_name):
        """Get preview of table data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql(f"SELECT * FROM '{table_name}' LIMIT 5", conn)
        except Exception as e:
            st.error(f"Failed to preview table: {str(e)}")
            return pd.DataFrame()

    def get_table_row_count(self, table_name):
        """Get the total number of rows in a table"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM '{table_name}'")
                return cursor.fetchone()[0]
        except Exception as e:
            st.error(f"Failed to get row count: {str(e)}")
            return 0

def generate_table_name(workbook_name: str, view_names: List[str]) -> str:
    """Generate a unique table name based on workbook and views"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Clean workbook name to be SQL friendly
    clean_wb_name = "".join(c if c.isalnum() else "_" for c in workbook_name)
    return f"{clean_wb_name}_{timestamp}"

def show_saved_data(db_manager):
    """Show saved data in the sidebar with enhanced display"""
    st.sidebar.markdown("---")
    st.sidebar.subheader("Saved Data")
    
    # Always pass include_internal=False
    tables = db_manager.list_tables(include_internal=False)
    
    if tables:
        # Group tables by workbook
        table_groups = {}
        for table in tables:
            workbook_name = table.split('_')[0]  # Get workbook name from table name
            if workbook_name not in table_groups:
                table_groups[workbook_name] = []
            table_groups[workbook_name].append(table)
        
        # Display tables grouped by workbook
        for workbook, workbook_tables in table_groups.items():
            with st.sidebar.expander(f"📊 {workbook}"):
                for table in workbook_tables:
                    st.write(f"📑 {table}")
                    preview = db_manager.get_table_preview(table)
                    if not preview.empty:
                        st.dataframe(preview, use_container_width=True)
                        row_count = db_manager.get_table_row_count(table)
                        st.caption(f"Total rows: {row_count}")
    else:
        st.sidebar.info("No saved data yet")

def save_to_database(df: pd.DataFrame, table_name: str, db_path: str):
    """Save DataFrame to SQLite database"""
    try:
        # Create database connection
        with sqlite3.connect(db_path) as conn:
            # Save data
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            st.success(f"Data successfully saved to table: {table_name}")
    except Exception as e:
        st.error(f"Failed to save to database: {str(e)}")

def show_help():
    st.markdown("""
    # Tableau Data Downloader Help
    
    This application helps you download data from Tableau Server/Online and save it to a database.
    
    ## Getting Started
    
    1. **Server URL**
       - For Tableau Online (US): https://10ay.online.tableau.com
       - For Tableau Online (EU): https://10az.online.tableau.com
       - For Tableau Server: Your server's URL
    
    2. **Authentication**
       - Personal Access Token (Recommended):
         - Works with 2FA
         - More secure
         - Generate from your Tableau account settings
       - Username/Password:
         - Basic authentication
         - Not compatible with 2FA
    
    3. **Site Name**
       - For Tableau Online: Found in your URL after #/site/
       - For Tableau Server: Usually blank for default
    
    ## Using the App
    
    1. Enter your server details and authenticate
    2. Select a workbook from the list
    3. Choose one or more views to download
    4. Preview the data before saving
    5. Save to database if desired
    
    ## Troubleshooting
    
    - **Connection Issues**: Verify your server URL and credentials
    - **Empty Data**: Ensure the views contain data
    - **Database Errors**: Check if database is running (status shown at top)
    
    ## Need Help?
    
    Contact your Tableau administrator or refer to [Tableau's REST API documentation](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)
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

# Call init_session_state at startup
init_session_state()

def save_session_state():
    """Save session state to file"""
    session_id = get_session()
    if session_id:
        # Create .streamlit directory if it doesn't exist
        os.makedirs('.streamlit', exist_ok=True)
        
        # Convert session state to dict, excluding non-serializable objects
        state_dict = {}
        for key, value in dict(st.session_state).items():
            try:
                # Only save primitive types and simple objects
                if isinstance(value, (str, int, float, bool, list, dict)) or value is None:
                    state_dict[key] = value
            except:
                continue
        
        # Save to file
        try:
            with open(f'.streamlit/session_{session_id}.json', 'w') as f:
                json.dump(state_dict, f)
        except Exception as e:
            print(f"Error saving session state: {str(e)}")

def clear_session():
    """Clear session state and remove persisted file"""
    session_id = get_session()
    if session_id:
        try:
            if os.path.exists(f'.streamlit/session_{session_id}.json'):
                os.remove(f'.streamlit/session_{session_id}.json')
        except Exception as e:
            print(f"Error removing session file: {str(e)}")
    
    # Clear all session state
    for key in list(st.session_state.keys()):
        del st.session_state[key]

def authenticate(server_url, auth_method, credentials):
    """Handle Tableau authentication"""
    try:
        connector = TableauConnector(server_url)
        if auth_method == "Personal Access Token (PAT)":
            success = connector.authenticate_with_pat(
                credentials['pat_name'],
                credentials['pat_secret'],
                credentials.get('site_name', '')
            )
        else:
            success = connector.authenticate(
                credentials['username'],
                credentials['password'],
                credentials.get('site_name', '')
            )
        
        if success:
            st.session_state.authenticated = True
            st.session_state.connector = connector
            st.session_state.workbooks = None
            st.session_state.views = None
            st.session_state.selected_workbook = None
            return True
        return False
    except Exception as e:
        st.error(f"Authentication error: {str(e)}")
        return False

def load_views(workbook):
    """Load views for selected workbook"""
    try:
        return st.session_state.connector.get_views(workbook)
    except Exception as e:
        print(f"Error loading views: {str(e)}")
        return None

def download_and_save_data(view_ids, workbook_name, view_names, db_manager):
    """Download data and automatically save to database"""
    if st.session_state.connector:
        df = st.session_state.connector.download_view_data(view_ids, workbook_name)
        if not df.empty:
            # Add metadata columns
            df['Workbook'] = workbook_name
            df['Download_Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df['View_Names'] = ', '.join(view_names)
            df['Organization_ID'] = st.session_state.user['organization_id']
            
            # Generate table name and check if it exists
            table_name = generate_table_name(workbook_name, view_names)
            
            # Check if dataset already exists
            with sqlite3.connect(db_manager.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                if cursor.fetchone():
                    st.warning(f"Dataset '{table_name}' already exists. Please delete the existing dataset first if you want to replace it.")
                    return False
            
            # Save to database if dataset doesn't exist
            save_to_database(df, table_name, db_manager.db_path)
            st.session_state.downloaded_data = df
            st.session_state.last_saved_table = table_name
            return True
    return False

def delete_dataset(dataset_name):
    """Delete a dataset from the database"""
    try:
        with sqlite3.connect(DatabaseManager().db_path) as conn:
            cursor = conn.cursor()
            
            # Check if Organization_ID column exists and verify ownership
            cursor.execute(f"PRAGMA table_info('{dataset_name}')")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'Organization_ID' in columns:
                # Verify the dataset belongs to the user's organization
                cursor.execute(f"""
                    SELECT COUNT(*) FROM '{dataset_name}'
                    WHERE Organization_ID = ?
                """, (st.session_state.user['organization_id'],))
                count = cursor.fetchone()[0]
                if count == 0:
                    raise ValueError("You don't have permission to delete this dataset")
            
            # Delete the table
            cursor.execute(f"DROP TABLE IF EXISTS '{dataset_name}'")
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Failed to delete dataset: {str(e)}")
        return False

def show_schedule_page(datasets=None):
    """Show schedule report page"""
    st.title("Schedule Report")

    # Use the current dataset if one is selected
    selected_dataset = st.session_state.get('current_dataset')
    if not selected_dataset:
        available_datasets = get_saved_datasets()
        selected_dataset = st.selectbox(
            "Choose a dataset to schedule",
            available_datasets,
            format_func=lambda x: f"{x} ({get_row_count(x)} rows)"
        )

    if selected_dataset:
        df = load_dataset(selected_dataset)
        if df is not None:
            # Create tabs for different sections
            tabs = st.tabs(["Active Schedules", "Create Schedule"])
            
            # Email configuration is now hidden and set from environment variables
            email_config = {
                'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
                'smtp_port': int(os.getenv('SMTP_PORT', '587')),
                'sender_email': os.getenv('SENDER_EMAIL', 'tableautoexcel@gmail.com'),
                'sender_password': os.getenv('SENDER_PASSWORD', 'ptvy yerb ymbj fngu')
            }
            
            with tabs[0]:
                st.subheader("📅 Active Schedules")
                report_manager = ReportManager()
                active_schedules = report_manager.get_active_schedules()
                
                if active_schedules:
                    schedules_for_dataset = {
                        job_id: schedule for job_id, schedule in active_schedules.items()
                        if schedule['dataset_name'] == selected_dataset
                    }
                    
                    if schedules_for_dataset:
                        for job_id, schedule in schedules_for_dataset.items():
                            with st.expander(f"Schedule: {schedule['schedule_config']['type'].title()}", expanded=True):
                                col1, col2, col3 = st.columns([2, 1, 1])
                                
                                with col1:
                                    schedule_info = ""
                                    if schedule['schedule_config']['type'] == 'one-time':
                                        schedule_info = f"""
                                        One-time on: {schedule['schedule_config']['date']} at {schedule['schedule_config']['hour']:02d}:{schedule['schedule_config']['minute']:02d}
                                        """
                                    elif schedule['schedule_config']['type'] == 'daily':
                                        schedule_info = f"""
                                        Daily at {schedule['schedule_config']['hour']:02d}:{schedule['schedule_config']['minute']:02d}
                                        """
                                    elif schedule['schedule_config']['type'] == 'weekly':
                                        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                                        day = days[schedule['schedule_config']['day']]
                                        schedule_info = f"""
                                        Weekly on {day} at {schedule['schedule_config']['hour']:02d}:{schedule['schedule_config']['minute']:02d}
                                        """
                                    else:
                                        schedule_info = f"""
                                        Monthly on day {schedule['schedule_config']['day']} at {schedule['schedule_config']['hour']:02d}:{schedule['schedule_config']['minute']:02d}
                                        """
                                    st.info(schedule_info)
                                    st.write("**Recipients:**")
                                    for recipient in schedule['email_config']['recipients']:
                                        st.write(f"- {recipient}")
                                
                                with col2:
                                    modify_key = f"modify_{job_id}"
                                    if st.button("🔄 Modify", key=modify_key):
                                        st.session_state.modifying_schedule = job_id
                                        st.session_state.show_modify_schedule = True
                                        st.session_state.show_schedule_page = False
                                        st.rerun()
                                
                                with col3:
                                    delete_key = f"delete_{job_id}"
                                    if st.button("🗑️ Delete", key=delete_key):
                                        delete_manager = ReportManager()
                                        if delete_manager.remove_schedule(job_id):
                                            st.success("Schedule removed successfully!")
                                            if job_id in schedules_for_dataset:
                                                del schedules_for_dataset[job_id]
                                            time.sleep(1)
                                            st.rerun()
                                        else:
                                            st.error("Failed to remove schedule. Please try again.")
                    else:
                        st.info("No active schedules found for this dataset.")
                else:
                    st.info("No active schedules found.")

            with tabs[1]:
                st.subheader("📝 Create New Schedule")
                st.subheader("👥 Recipients")
                email_list = st.text_area(
                    "Email Addresses",
                    placeholder="Enter email addresses (one per line)",
                    help="These addresses will receive the scheduled reports"
                )
                
                # Email body section
                st.subheader("📧 Email Message")
                email_body = st.text_area(
                    "Email Body",
                    placeholder="Enter the message you want to include in the email...",
                    help="This message will be included in the email body",
                    height=150
                )
                
                report_format = st.radio(
                    "Report Format",
                    options=["CSV", "PDF"],
                    horizontal=True
                )
            
                # Schedule settings section
                st.subheader("🕒 Schedule Settings")
                schedule_type = st.selectbox(
                    "Frequency",
                    ["One-time", "Daily", "Weekly", "Monthly"],
                    help="How often to send the report"
                ).lower()

                col1, col2 = st.columns(2)
                with col1:
                    schedule_config = {}
                    if schedule_type == "one-time":
                        date = st.date_input("Select Date")
                        hour = st.number_input("Hour (24-hour format)", 0, 23, 8)
                        minute = st.number_input("Minute", 0, 59, 0)
                        schedule_config = {
                            'type': 'one-time',
                            'date': date.strftime("%Y-%m-%d"),
                            'hour': int(hour),
                            'minute': int(minute)
                        }
                    elif schedule_type == "daily":
                        hour = st.number_input("Hour (24-hour format)", 0, 23, 8)
                        minute = st.number_input("Minute", 0, 59, 0)
                        schedule_config = {
                            'type': 'daily',
                            'hour': int(hour),
                            'minute': int(minute)
                        }
                    elif schedule_type == "weekly":
                        weekday = st.selectbox("Day of Week", 
                            ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
                        hour = st.number_input("Hour (24-hour format)", 0, 23, 8)
                        minute = st.number_input("Minute", 0, 59, 0)
                        schedule_config = {
                            'type': 'weekly',
                            'day': ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"].index(weekday.lower()),
                            'hour': int(hour),
                            'minute': int(minute)
                        }
                    elif schedule_type == "monthly":
                        day = st.number_input("Day of Month", 1, 31, 1)
                        hour = st.number_input("Hour (24-hour format)", 0, 23, 8)
                        minute = st.number_input("Minute", 0, 59, 0)
                        schedule_config = {
                            'type': 'monthly',
                            'day': int(day),
                            'hour': int(hour),
                            'minute': int(minute)
                        }

                with col2:
                    if schedule_type == "one-time":
                        st.info(f"""
                        Report will be sent once on:
                        {date.strftime('%Y-%m-%d')} at {hour:02d}:{minute:02d}
                        """)
                    else:
                        st.info(f"""
                        Report will be sent:
                        {'Daily' if schedule_type == 'daily' else ''}
                        {'Every ' + weekday if schedule_type == 'weekly' else ''}
                        {'On day ' + str(day) + ' of each month' if schedule_type == 'monthly' else ''}
                        at {hour:02d}:{minute:02d}
                        """)
                        
                # Create schedule button
                if st.button("Create Schedule", type="primary", use_container_width=True):
                    if not email_list.strip():
                        st.error("Please enter at least one recipient email")
                        return
                    
                    try:
                        # Add recipients and email body to email config
                        email_config = {
                            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
                            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
                            'sender_email': os.getenv('SENDER_EMAIL', 'tableautoexcel@gmail.com'),
                            'sender_password': os.getenv('SENDER_PASSWORD', 'ptvy yerb ymbj fngu'),
                            'recipients': [e.strip() for e in email_list.split('\n') if e.strip()],
                            'format': report_format,
                            'body': email_body  # Add the email body to the config
                        }
                        
                        report_manager = ReportManager()
                        job_id = report_manager.schedule_report(
                            selected_dataset,
                            email_config,
                            schedule_config
                        )
                        
                        if job_id:
                            st.success(f"""
                            Report scheduled successfully! 🎉
                            Schedule: {schedule_type}
                            Next run: {hour:02d}:{minute:02d}
                            """)
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("Failed to create schedule. Please check your configuration.")
                        
                    except Exception as e:
                        st.error(f"Failed to schedule report: {str(e)}")
                        print(f"Schedule error details: {str(e)}")
                        print(f"Schedule config: {schedule_config}")
            
            # Back button at the bottom
            st.markdown("---")
            if st.button("← Back to Datasets", use_container_width=True):
                st.session_state.show_schedule_page = False
                st.rerun()

def get_row_count(dataset_name):
    """Get row count for a dataset"""
    try:
        with sqlite3.connect(DatabaseManager().db_path) as conn:
            count = pd.read_sql(f"SELECT COUNT(*) FROM '{dataset_name}'", conn).iloc[0, 0]
            return count
    except Exception:
        return 0

def main():
    """Main function to run the Streamlit app"""
    st.set_page_config(
        page_title="Tableau Data App",
        page_icon="📊",
        layout="wide"
    )
    
    # Initialize session state at the start
    if 'initialized' not in st.session_state:
        init_session_state()
        st.session_state.initialized = True
    
    # Check for existing session
    if 'user' in st.session_state and st.session_state.user:
        with st.sidebar:
            # User Profile section
            st.title("👤 User Profile")
            st.write(f"Welcome, {st.session_state.user['username']}")
            st.write(f"Role: {st.session_state.user['permission_type']}")
            st.markdown("---")
            
            # Navigation section
            st.title("📍 Navigation")
            if st.button("❓ Show Help"):
                show_help()
            st.markdown("---")
            
            # Saved Datasets section
            st.title("📊 Saved Datasets")
            show_saved_datasets(st.session_state.user['permission_type'])
            
            # Logout button at the bottom
            st.markdown("---")
            if st.button("🚪 Logout", key="sidebar_logout_button"):
                # Clear session state
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
        
        # Main content area
        if st.session_state.user['permission_type'] == 'superadmin':
            show_admin_page()
        else:
            if st.session_state.show_dashboard_page:
                show_dashboard_page()
            elif st.session_state.show_qa_page:
                show_qa_page()
            elif st.session_state.show_schedule_page:
                show_schedule_page()
            elif st.session_state.show_modify_schedule:
                show_modify_schedule_page(st.session_state.modifying_schedule)
            else:
                show_user_page()
    else:
        show_login_page()

def show_login_page():
    """Show login page and handle authentication"""
    st.title("Login")
    
    # Create tabs for Login and Register
    tab1, tab2 = st.tabs(["Login", "Register"])
    
    # Login Tab
    with tab1:
        # Show login form
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Login")
            
            if submit:
                user_manager = UserManager()
                user = user_manager.verify_user(username, password)
                if user:
                    # Store user info in session state
                    st.session_state.user = {
                        'id': user[0],
                        'username': user[1],
                        'role': user[2],
                        'permission_type': user[3],
                        'organization_id': user[4],
                        'organization_name': user[5]
                    }
                    st.success("Login successful!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Invalid username or password")
    
    # Register Tab
    with tab2:
        with st.form("register_form"):
            st.subheader("Create New Account")
            new_username = st.text_input("Username", key="reg_username")
            new_password = st.text_input("Password", type="password", key="reg_password")
            confirm_password = st.text_input("Confirm Password", type="password")
            email = st.text_input("Email")
            
            register = st.form_submit_button("Register")
            
            if register:
                if not new_username or not new_password:
                    st.error("Username and password are required")
                elif new_password != confirm_password:
                    st.error("Passwords do not match")
                else:
                    try:
                        user_manager = UserManager()
                        
                        # Add new user without organization
                        success = user_manager.add_user_to_org(
                            new_username,
                            new_password,
                            org_id=None,  # No organization
                            permission_type='normal',  # Default permission type
                            email=email
                        )
                        
                        if success:
                            st.success("Registration successful! Please login.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Failed to register user. Please try again.")
                    except Exception as e:
                        st.error(f"Registration error: {str(e)}")

def generate_suggested_questions(df):
    """Generate relevant questions based on data analysis"""
    questions = []
    
    # Get clean columns (excluding system columns)
    system_columns = [
        'Sheet Name', 'sheet name', 'Workbook', 'workbook',
        'Download_Timestamp', 'download_timestamp',
        'View_Names', 'view names',
        'Organization_ID', 'organization_id'
    ]
    df_clean = df.copy()
    df_clean = df_clean.drop(columns=[col for col in df.columns if col in system_columns or any(sys_col.lower() == col.lower() for sys_col in system_columns)])
    
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = df_clean.select_dtypes(include=['object', 'category']).columns.tolist()
    date_cols = df_clean.select_dtypes(include=['datetime64']).columns.tolist()
    
    # 1. Correlation-based questions
    if len(numeric_cols) >= 2:
        corr_matrix = df_clean[numeric_cols].corr()
        # Find highest correlation
        max_corr = 0
        max_corr_pair = None
        for i in range(len(numeric_cols)):
            for j in range(i+1, len(numeric_cols)):
                if abs(corr_matrix.iloc[i,j]) > max_corr:
                    max_corr = abs(corr_matrix.iloc[i,j])
                    max_corr_pair = (numeric_cols[i], numeric_cols[j])
        if max_corr_pair:
            questions.append({
                'category': 'Correlation',
                'question': f"How are {max_corr_pair[0]} and {max_corr_pair[1]} related?",
                'explanation': f"These metrics show a strong {max_corr:.2f} correlation"
            })
    
    # 2. Anomaly-based questions
    for col in numeric_cols:
        z_scores = np.abs(stats.zscore(df_clean[col]))
        anomalies = (z_scores > 3).sum()
        if anomalies > 0:
            questions.append({
                'category': 'Anomaly',
                'question': f"Show me unusual patterns in {col}",
                'explanation': f"Found {anomalies} potential anomalies"
            })
    
    # 3. Distribution questions
    for cat_col in categorical_cols:
        value_counts = df_clean[cat_col].value_counts()
        if not value_counts.empty:
            questions.append({
                'category': 'Frequency',
                'question': f"What's the distribution of {cat_col}?",
                'explanation': f"Analyze {len(value_counts)} unique categories"
            })
    
    # 4. Time-based questions
    if date_cols:
        for date_col in date_cols:
            for num_col in numeric_cols:
                questions.append({
                    'category': 'Trend',
                    'question': f"Show me the trend of {num_col} over time",
                    'explanation': "Analyze temporal patterns"
                })
    
    # 5. Performance questions
    for num_col in numeric_cols:
        questions.append({
            'category': 'Performance',
            'question': f"What's our highest {num_col}?",
            'explanation': "Find top performers"
        })
        if categorical_cols:
            cat_col = categorical_cols[0]
            questions.append({
                'category': 'Comparison',
                'question': f"Compare {num_col} across different {cat_col}",
                'explanation': "Analyze category-wise performance"
            })
    
    return questions

def analyze_data(question, df):
    """Analyze data based on the question"""
    question = question.lower().strip()
    
    try:
        # Get clean columns (excluding system columns)
        system_columns = [
            'Sheet Name', 'sheet name', 'Workbook', 'workbook',
            'Download_Timestamp', 'download_timestamp',
            'View_Names', 'view names',
            'Organization_ID', 'organization_id'
        ]
        df_clean = df.copy()
        df_clean = df_clean.drop(columns=[col for col in df.columns if col in system_columns or any(sys_col.lower() == col.lower() for sys_col in system_columns)])
        
        # Total/Sum Analysis
        if any(word in question for word in ['total', 'sum']):
            numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                return "No numeric columns found for total/sum analysis."
                
            response = "💰 **Total Analysis:**\n\n"
            for col in numeric_cols:
                total = df_clean[col].sum()
                avg = df_clean[col].mean()
                response += f"- {col}:\n"
                response += f"  • Total: ${total:,.2f}\n"
                response += f"  • Average: ${avg:,.2f}\n"
            return response
        
        # Correlation Analysis
        elif any(word in question for word in ['related', 'correlation', 'relationship']):
            numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) < 2:
                return "Need at least 2 numeric columns for correlation analysis."
                
            corr_matrix = df_clean[numeric_cols].corr()
            response = "📊 **Correlation Analysis:**\n\n"
            correlations_found = False
            
            for i in range(len(numeric_cols)):
                for j in range(i+1, len(numeric_cols)):
                    corr = corr_matrix.iloc[i,j]
                    if abs(corr) > 0.3:  # Show more correlations by lowering threshold
                        correlations_found = True
                        strength = "strong" if abs(corr) > 0.7 else "moderate" if abs(corr) > 0.5 else "weak"
                        direction = "positive" if corr > 0 else "negative"
                        response += f"- {numeric_cols[i]} and {numeric_cols[j]} have a {strength} {direction} correlation ({corr:.2f})\n"
            
            return response if correlations_found else "No significant correlations found between numeric columns."
        
        # Distribution Analysis
        elif any(word in question for word in ['distribution', 'breakdown', 'frequency']):
            categorical_cols = df_clean.select_dtypes(include=['object', 'category']).columns
            if len(categorical_cols) == 0:
                return "No categorical columns found for distribution analysis."
                
            response = "📊 **Distribution Analysis:**\n\n"
            for col in categorical_cols:
                value_counts = df_clean[col].value_counts()
                total = len(df_clean)
                response += f"{col} breakdown:\n"
                for category, count in value_counts.head(5).items():
                    percentage = (count/total) * 100
                    response += f"- {category}: {count:,} ({percentage:.1f}%)\n"
                response += "\n"
            return response
        
        # General Analysis (default)
        else:
            response = "📊 **General Analysis:**\n\n"
            
            # Basic dataset info
            response += f"Dataset Overview:\n"
            response += f"- Total records: {len(df_clean):,}\n"
            response += f"- Number of columns: {len(df_clean.columns):,}\n\n"
            
            # Numeric summaries
            numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
            if not numeric_cols.empty:
                response += "Numeric Metrics:\n"
                for col in numeric_cols:
                    total = df_clean[col].sum()
                    avg = df_clean[col].mean()
                    max_val = df_clean[col].max()
                    min_val = df_clean[col].min()
                    response += f"- {col}:\n"
                    response += f"  • Total: ${total:,.2f}\n"
                    response += f"  • Average: ${avg:,.2f}\n"
                    response += f"  • Range: ${min_val:,.2f} to ${max_val:,.2f}\n"
                response += "\n"
            
            # Categorical summaries
            categorical_cols = df_clean.select_dtypes(include=['object', 'category']).columns
            if not categorical_cols.empty:
                response += "Category Breakdowns:\n"
                for col in categorical_cols:
                    value_counts = df_clean[col].value_counts()
                    top_category = value_counts.index[0]
                    count = value_counts.iloc[0]
                    total = len(df_clean)
                    percentage = (count/total) * 100
                    unique_count = len(value_counts)
                    response += f"- {col}:\n"
                    response += f"  • Most common: {top_category} ({count:,} occurrences, {percentage:.1f}%)\n"
                    response += f"  • Unique values: {unique_count:,}\n"
            
            return response
    
    except Exception as e:
        print(f"Analysis error: {str(e)}")  # Debug print
        return f"Error analyzing data: {str(e)}"

def show_qa_page():
    """Show enhanced Q&A interface for the selected dataset"""
    st.title("💡 Ask Your Data Analyst")
    
    # Get and clean the current dataset
    if isinstance(st.session_state.current_dataset, str):
        df = load_dataset(st.session_state.current_dataset)
    else:
        df = st.session_state.current_dataset
    
    if df is None or df.empty:
        st.error("No data available to analyze")
        return
    
    # Clean the data first (remove system columns)
    system_columns = [
        'Sheet Name', 'sheet name', 'Workbook', 'workbook',
        'Download_Timestamp', 'download_timestamp',
        'View_Names', 'view names',
        'Organization_ID', 'organization_id'
    ]
    df_clean = df.copy()
    df_clean = df_clean.drop(columns=[col for col in df.columns if col in system_columns or any(sys_col.lower() == col.lower() for sys_col in system_columns)])
    
    # Show data preview
    with st.expander("📊 Data Preview"):
        st.dataframe(df_clean.head(), use_container_width=True)
        st.caption(f"Showing first 5 rows of {len(df_clean):,} total rows")
        
        # Show column information
        st.write("\n**Available Columns:**")
        col1, col2 = st.columns(2)
        with col1:
            st.write("Numeric Columns:")
            numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                st.write(f"- {col}")
        with col2:
            st.write("Categorical Columns:")
            cat_cols = df_clean.select_dtypes(include=['object', 'category']).columns
            for col in cat_cols:
                st.write(f"- {col}")
    
    # Custom question input first
    st.write("### 🤔 Ask Your Own Question")
    question_col1, question_col2 = st.columns([4, 1])
    
    with question_col1:
        if 'current_question' not in st.session_state:
            st.session_state.current_question = ""
            
        user_question = st.text_input(
            "Your question:",
            value=st.session_state.current_question,
            placeholder="Ask anything about your data...",
            key="question_input"
        )
    
    with question_col2:
        analyze_clicked = st.button("Analyze", type="primary", key="analyze_btn")
    
    # Generate and show suggested questions
    st.write("### 💡 Suggested Questions")
    suggested_questions = generate_suggested_questions(df_clean)
    
    # Group questions by category
    questions_by_category = {}
    for q in suggested_questions:
        if q['category'] not in questions_by_category:
            questions_by_category[q['category']] = []
        questions_by_category[q['category']].append(q)
    
    # Display suggested questions in tabs
    if questions_by_category:
        tabs = st.tabs(list(questions_by_category.keys()))
        for tab, category in zip(tabs, questions_by_category.keys()):
            with tab:
                for q in questions_by_category[category]:
                    if st.button(
                        f"{q['question']} ℹ️", 
                        help=q['explanation'],
                        use_container_width=True,
                        key=f"btn_{category}_{q['question']}"
                    ):
                        st.session_state.current_question = q['question']
                        st.rerun()
    else:
        st.info("No suggested questions available for this dataset")
    
    # Show analysis
    if analyze_clicked and user_question:
        st.write("---")
        with st.spinner("Analyzing your data..."):
            try:
                # Get the analysis response
                response = analyze_data(user_question, df_clean)
                
                if response:
                    st.success("Analysis complete!")
                    st.markdown("### 📊 Analysis Results")
                    st.markdown(response)
                    
                    # If it's a total/sum question, show summary cards
                    if any(word in user_question.lower() for word in ['total', 'sum']):
                        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
                        metrics_container = st.container()
                        with metrics_container:
                            cols = st.columns(len(numeric_cols))
                            for i, col in enumerate(numeric_cols):
                                with cols[i]:
                                    total = df_clean[col].sum()
                                    st.metric(
                                        label=f"Total {col}",
                                        value=f"${total:,.2f}",
                                        help=f"Total sum of {col} across all records"
                                    )
                else:
                    st.warning("I couldn't find relevant insights for your question. Try rephrasing or ask something else.")
            except Exception as e:
                st.error(f"Error analyzing data: {str(e)}")
                st.info("Please try rephrasing your question or ask something else.")
    
    # Add a back button at the bottom
    st.markdown("---")
    if st.button("← Back to Datasets", use_container_width=True):
        st.session_state.show_qa_page = False
        st.rerun()

def show_saved_datasets(permission_type):
    """Show list of saved datasets with permission-based buttons"""
    datasets = get_saved_datasets()
    
    if not datasets:
        st.info("No datasets found. Please download some data first.")
        return
    
    for dataset in datasets:
        with st.container():
            # Dataset name and row count
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"📊 **{dataset}**")
            with col2:
                row_count = get_row_count(dataset)
                st.caption(f"Rows: {row_count}")
            
            # Action buttons based on user permission
            if permission_type == 'power':
                # Power users see all buttons in two rows
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("📊 Dashboard", key=f"dashboard_{dataset}", use_container_width=True):
                        try:
                            df = load_dataset(dataset)
                            if df is not None:
                                dashboard_manager = DashboardManager()
                                dashboard_id = dashboard_manager.create_dashboard(df)
                                st.session_state.current_dataset = df
                                st.session_state.current_dashboard_id = dashboard_id
                                st.session_state.show_dashboard_page = True
                                # Reset other page states
                                st.session_state.show_qa_page = False
                                st.session_state.show_schedule_page = False
                                st.session_state.show_modify_schedule = False
                                st.rerun()
                            else:
                                st.error(f"Failed to load dataset: {dataset}")
                        except Exception as e:
                            st.error(f"Error loading dataset: {str(e)}")
                
                with col2:
                    if st.button("❓ Ask", key=f"ask_{dataset}", use_container_width=True):
                        try:
                            df = load_dataset(dataset)
                            if df is not None:
                                st.session_state.current_dataset = df
                                st.session_state.show_qa_page = True
                                # Reset other page states
                                st.session_state.show_dashboard_page = False
                                st.session_state.show_schedule_page = False
                                st.session_state.show_modify_schedule = False
                                st.rerun()
                            else:
                                st.error(f"Failed to load dataset: {dataset}")
                        except Exception as e:
                            st.error(f"Error loading dataset: {str(e)}")
                
                col3, col4 = st.columns(2)
                with col3:
                    if st.button("📅 Schedule", key=f"schedule_{dataset}", use_container_width=True):
                        st.session_state.show_schedule_page = True
                        st.session_state.current_dataset = dataset
                        # Reset other page states
                        st.session_state.show_dashboard_page = False
                        st.session_state.show_qa_page = False
                        st.session_state.show_modify_schedule = False
                        st.rerun()
                
                with col4:
                    if st.button("🗑️ Delete", key=f"delete_{dataset}", type="secondary", use_container_width=True):
                        if delete_dataset(dataset):
                            st.success(f"Dataset '{dataset}' deleted successfully!")
                            # Reset all page states
                            st.session_state.show_dashboard_page = False
                            st.session_state.show_qa_page = False
                            st.session_state.show_schedule_page = False
                            st.session_state.show_modify_schedule = False
                            st.session_state.current_dataset = None
                            st.session_state.current_dashboard_id = None
                            st.rerun()
            else:
                # Normal users see only Schedule and Delete buttons
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("📅 Schedule", key=f"schedule_{dataset}", use_container_width=True):
                        st.session_state.show_schedule_page = True
                        st.session_state.current_dataset = dataset
                        # Reset other page states
                        st.session_state.show_dashboard_page = False
                        st.session_state.show_qa_page = False
                        st.session_state.show_modify_schedule = False
                        st.rerun()
                
                with col2:
                    if st.button("🗑️ Delete", key=f"delete_{dataset}", type="secondary", use_container_width=True):
                        if delete_dataset(dataset):
                            st.success(f"Dataset '{dataset}' deleted successfully!")
                            # Reset all page states
                            st.session_state.show_dashboard_page = False
                            st.session_state.show_qa_page = False
                            st.session_state.show_schedule_page = False
                            st.session_state.show_modify_schedule = False
                            st.session_state.current_dataset = None
                            st.session_state.current_dashboard_id = None
                            st.rerun()
            
            st.markdown("---")

def load_dataset(table_name):
    """Load dataset from SQLite database filtered by organization"""
    try:
        with sqlite3.connect(DatabaseManager().db_path) as conn:
            cursor = conn.cursor()
            
            # Check if Organization_ID column exists
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'Organization_ID' in columns:
                # Only load data for the user's organization
                query = f"""
                SELECT * FROM '{table_name}'
                WHERE Organization_ID = ?
                """
                df = pd.read_sql_query(query, conn, params=[st.session_state.user['organization_id']])
            else:
                # For legacy tables without Organization_ID, load all data
                query = f"SELECT * FROM '{table_name}'"
                df = pd.read_sql_query(query, conn)
            
            if df.empty:
                st.error(f"No data found in dataset '{table_name}'")
                return None
            print(f"Loaded dataset shape: {df.shape}")  # Debug print
            return df
    except Exception as e:
        st.error(f"Failed to load dataset: {str(e)}")
        return None

def get_saved_datasets():
    """Get list of saved datasets filtered by organization"""
    try:
        with sqlite3.connect(DatabaseManager().db_path) as conn:
            cursor = conn.cursor()
            # Get all tables
            tables = DatabaseManager().list_tables(include_internal=False)
            filtered_tables = []
            
            for table in tables:
                try:
                    # Check if Organization_ID column exists
                    cursor.execute(f"PRAGMA table_info('{table}')")
                    columns = [col[1] for col in cursor.fetchall()]
                    
                    if 'Organization_ID' in columns:
                        # Check if table has data for this organization
                        query = f"""
                        SELECT COUNT(*) FROM '{table}' 
                        WHERE Organization_ID = ?
                        """
                        count = pd.read_sql_query(query, conn, params=[st.session_state.user['organization_id']])
                        if count.iloc[0, 0] > 0:
                            filtered_tables.append(table)
                    else:
                        # For legacy tables without Organization_ID, show to all users
                        filtered_tables.append(table)
                except Exception as table_error:
                    print(f"Error checking table {table}: {str(table_error)}")
                    continue
            
            return filtered_tables
    except Exception as e:
        print(f"Error getting datasets: {str(e)}")
        return []

def show_logout_button():
    """Show logout button in sidebar"""
    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 Logout", key="sidebar_logout_button"):
        clear_session()
        st.rerun()

def show_user_page():
    """Show regular user interface"""
    # Initialize database
    db_manager = DatabaseManager()
    db_url = db_manager.ensure_database_running()
    
    # Main content area
    if not st.session_state.authenticated:
        st.title("Connect to Tableau")
        # Show Tableau authentication form
        server_url = st.text_input(
            "Tableau Server URL",
            help="Example: https://10ay.online.tableau.com for Tableau Online"
        )
        
        auth_method = st.radio(
            "Authentication Method",
            ["Personal Access Token (PAT)", "Username/Password"],
            help="PAT is recommended and works with 2FA"
        )
        
        with st.form("auth_form"):
            credentials = {}
            if auth_method == "Personal Access Token (PAT)":
                credentials['pat_name'] = st.text_input("Personal Access Token Name")
                credentials['pat_secret'] = st.text_input("Personal Access Token Secret", type="password")
            else:
                credentials['username'] = st.text_input("Username")
                credentials['password'] = st.text_input("Password", type="password")
            
            credentials['site_name'] = st.text_input("Site Name (optional)")
            submit_auth = st.form_submit_button("Connect")
            
            if submit_auth and server_url:
                if authenticate(server_url, auth_method, credentials):
                    st.success("Successfully connected to Tableau!")
                    st.rerun()
                else:
                    st.error("Authentication failed. Please check your credentials and try again.")
    
    # Show workbook selection if authenticated
    elif st.session_state.authenticated:
        st.title("Download Tableau Data")
        try:
            if not st.session_state.workbooks:
                st.session_state.workbooks = st.session_state.connector.get_workbooks()
            
            if st.session_state.workbooks:
                # Workbook selection
                workbook_names = [wb.get('@name') or wb.get('name') for wb in st.session_state.workbooks]
                selected_wb_name = st.selectbox(
                    "Select Workbook",
                    workbook_names,
                    key='workbook_selector'
                )
                
                # Find selected workbook
                selected_workbook = next(
                    wb for wb in st.session_state.workbooks 
                    if (wb.get('@name') or wb.get('name')) == selected_wb_name
                )
                
                # Load views if workbook changed
                if (not st.session_state.selected_workbook or 
                    selected_workbook != st.session_state.selected_workbook):
                    load_views(selected_workbook)
                
                # View selection
                if st.session_state.views:
                    view_names = [view.get('@name') or view.get('name') for view in st.session_state.views]
                    selected_views = st.multiselect("Select Views", view_names)
                    
                    if selected_views:
                        if st.button("Download Data"):
                            try:
                                view_ids = [
                                    view.get('@id') or view.get('id')
                                    for view in st.session_state.views
                                    if (view.get('@name') or view.get('name')) in selected_views
                                ]
                                
                                with st.spinner('Downloading and saving data...'):
                                    if download_and_save_data(view_ids, selected_wb_name, selected_views, db_manager):
                                        st.success(f"Data downloaded and saved successfully!")
                                        st.rerun()
                            except Exception as e:
                                st.error(f"Failed to download data: {str(e)}")
                                st.session_state.views = None
                    else:
                        st.warning("No views found in this workbook. Please select another workbook.")
                else:
                    st.error("No workbooks found. Please check your permissions.")
        except Exception as e:
            st.error(f"Error loading workbooks/views: {str(e)}")
            if st.button("Retry Connection"):
                st.session_state.authenticated = False
                st.rerun()
            
def show_modify_schedule_page(job_id):
    """Show modify schedule page"""
    st.title("Modify Schedule")
    
    report_manager = ReportManager()
    schedules = report_manager.get_active_schedules()
    
    if job_id not in schedules:
        st.error("Schedule not found!")
        return
    
    schedule = schedules[job_id]
    
    # Show current schedule info
    st.subheader("Current Schedule Details")
    st.info(f"Dataset: {schedule['dataset_name']}")
    
    # Recipients section
    st.subheader("👥 Recipients")
    current_recipients = '\n'.join(schedule['email_config']['recipients'])
    email_list = st.text_area(
        "Email Addresses",
        value=current_recipients,
        placeholder="Enter email addresses (one per line)",
        help="These addresses will receive the scheduled reports"
    )
    
    report_format = st.radio(
        "Report Format",
        options=["CSV", "PDF"],
        index=0 if schedule['email_config']['format'].upper() == 'CSV' else 1,
        horizontal=True
    )
    
    # Schedule settings section
    st.subheader("🕒 Schedule Settings")
    current_type = schedule['schedule_config']['type']
    schedule_type = st.selectbox(
        "Frequency",
        ["One-time", "Daily", "Weekly", "Monthly"],
        index=["one-time", "daily", "weekly", "monthly"].index(current_type),
        help="How often to send the report"
    ).lower()
    
    col1, col2 = st.columns(2)
    with col1:
        schedule_config = {}
        if schedule_type == "one-time":
            current_date = datetime.strptime(schedule['schedule_config']['date'], "%Y-%m-%d").date()
            date = st.date_input("Select Date", value=current_date)
            hour = st.number_input("Hour (24-hour format)", 0, 23, value=schedule['schedule_config']['hour'])
            minute = st.number_input("Minute", 0, 59, value=schedule['schedule_config']['minute'])
            schedule_config = {
                'type': 'one-time',
                'date': date.strftime("%Y-%m-%d"),
                'hour': int(hour),
                'minute': int(minute)
            }
        elif schedule_type == "daily":
            hour = st.number_input("Hour (24-hour format)", 0, 23, value=schedule['schedule_config']['hour'])
            minute = st.number_input("Minute", 0, 59, value=schedule['schedule_config']['minute'])
            schedule_config = {
                'type': 'daily',
                'hour': int(hour),
                'minute': int(minute)
            }
        elif schedule_type == "weekly":
            current_day = schedule['schedule_config'].get('day', 0)
            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            weekday = st.selectbox("Day of Week", days, index=current_day)
            hour = st.number_input("Hour (24-hour format)", 0, 23, value=schedule['schedule_config']['hour'])
            minute = st.number_input("Minute", 0, 59, value=schedule['schedule_config']['minute'])
            schedule_config = {
                'type': 'weekly',
                'day': days.index(weekday),
                'hour': int(hour),
                'minute': int(minute)
            }
        elif schedule_type == "monthly":
            day = st.number_input("Day of Month", 1, 31, value=schedule['schedule_config'].get('day', 1))
            hour = st.number_input("Hour (24-hour format)", 0, 23, value=schedule['schedule_config']['hour'])
            minute = st.number_input("Minute", 0, 59, value=schedule['schedule_config']['minute'])
            schedule_config = {
                'type': 'monthly',
                'day': int(day),
                'hour': int(hour),
                'minute': int(minute)
            }
        
        with col2:
            if schedule_type == "one-time":
                st.info(f"""
                Report will be sent once on:
                {date.strftime('%Y-%m-%d')} at {hour:02d}:{minute:02d}
                """)
            else:
                st.info(f"""
                Report will be sent:
                {'Daily' if schedule_type == 'daily' else ''}
                {'Every ' + weekday if schedule_type == 'weekly' else ''}
                {'On day ' + str(day) + ' of each month' if schedule_type == 'monthly' else ''}
                at {hour:02d}:{minute:02d}
                """)
        
        # Action buttons
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("← Back", use_container_width=True):
                st.session_state.show_modify_schedule = False
                st.session_state.modifying_schedule = None
                st.rerun()

        with col2:
            if st.button("Save Changes", type="primary", use_container_width=True):
                if not email_list.strip():
                    st.error("Please enter at least one recipient email")
                    return
                
                try:
                    # Remove old schedule
                    report_manager.remove_schedule(job_id)
                    
                    # Create new schedule with updated configuration
                    email_config = {
                        'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
                        'smtp_port': int(os.getenv('SMTP_PORT', '587')),
                        'sender_email': os.getenv('SENDER_EMAIL', 'tableautoexcel@gmail.com'),
                        'sender_password': os.getenv('SENDER_PASSWORD', 'ptvy yerb ymbj fngu'),
                        'recipients': [e.strip() for e in email_list.split('\n') if e.strip()],
                        'format': report_format
                    }
                    
                    new_job_id = report_manager.schedule_report(
                        schedule['dataset_name'],
                        email_config,
                        schedule_config
                    )
                    
                    if new_job_id:
                        st.success("Schedule updated successfully! 🎉")
                        time.sleep(2)
                        st.session_state.show_modify_schedule = False
                        st.session_state.modifying_schedule = None
                        st.rerun()
                except Exception as e:
                    st.error(f"Failed to update schedule: {str(e)}")

def show_dashboard_page():
    """Show dashboard page in Streamlit"""
    # Add back button at the top
    col1, col2 = st.columns([1, 11])
    with col1:
        if st.button("← Back"):
            st.session_state.show_dashboard_page = False
            st.rerun()
    with col2:
        st.title("📊 Interactive Dashboard")
    
    # Get the current dataset
    if isinstance(st.session_state.current_dataset, str):
        # If it's a string (dataset name), load the dataset
        df = load_dataset(st.session_state.current_dataset)
    else:
        # If it's already a dataframe, use it directly
        df = st.session_state.current_dataset
        
    if df is None or df.empty:
        st.error("No data available to display")
        return
    
    # Filter out system columns
    system_columns = [
        'Sheet Name', 'sheet name', 'Workbook', 'workbook',
        'Download_Timestamp', 'download_timestamp',
        'View_Names', 'view names',
        'Organization_ID', 'organization_id'
    ]
    
    # Remove system columns if they exist
    df_filtered = df.copy()
    columns_to_remove = [col for col in df.columns if col in system_columns or any(sys_col.lower() == col.lower() for sys_col in system_columns)]
    if columns_to_remove:
        df_filtered = df_filtered.drop(columns=columns_to_remove)
        
    # Create dashboard manager and generate dashboard
    dashboard_manager = DashboardManager()
    dashboard_id = dashboard_manager.create_dashboard(df_filtered)
    dashboard = dashboard_manager.get_dashboard(dashboard_id)
    
    if not dashboard:
        st.error("Failed to create dashboard")
        return
    
    # Show refresh button
    col1, col2, col3 = st.columns([1, 8, 1])
    with col1:
        if st.button("🔄", help="Refresh Dashboard"):
            st.rerun()
    
    # Display KPIs
    st.subheader("📈 Key Performance Indicators")
    kpi_cols = st.columns(4)
    insights = dashboard.get('insights', [])
    
    for i, insight in enumerate(insights[:4]):
        with kpi_cols[i]:
            st.metric(
                label=insight['title'],
                value=insight['value'],
                help=insight['description']
            )
    
    # Display visualizations
    if dashboard.get('visualizations'):
        st.markdown("---")
        st.subheader("📊 Visualizations")
        
        # Create two rows of two columns each for visualizations
        row1_col1, row1_col2 = st.columns(2)
        row2_col1, row2_col2 = st.columns(2)
        viz_columns = [row1_col1, row1_col2, row2_col1, row2_col2]
        
        for i, viz in enumerate(dashboard['visualizations'][:4]):
            with viz_columns[i]:
                st.write(f"**{viz['question']}**")
                try:
                    fig = go.Figure(viz['figure'])
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Failed to display visualization: {str(e)}")

if __name__ == "__main__":
    main() 
