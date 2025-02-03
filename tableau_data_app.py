import requests
import json
from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine
from getpass import getpass
import xmltodict  # Add this import at the top
import tableauserverclient as TSC
import io
import os
from datetime import datetime

class TableauConnector:
    def __init__(self, server_url: str):
        self.server_url = server_url.rstrip('/')
        self.api_version = "3.19"  # Current API version
        self.token = None
        self.site_id = None
        self.headers = None
        self.token_name = None
        self.token_value = None
        self.tableau_auth = None
        self.server = None

    def sign_in(self, username: str, password: str, site_name: str = "") -> bool:
        """Sign in to Tableau Server/Online and get authentication token"""
        url = f"{self.server_url}/api/{self.api_version}/auth/signin"
        
        payload = {
            "credentials": {
                "name": username,
                "password": password,
                "site": {"contentUrl": site_name}
            }
        }
        
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            
            credentials = response.json()['credentials']
            self.token = credentials['token']
            self.site_id = credentials['site']['id']
            self.headers = {
                'X-Tableau-Auth': self.token,
                'Content-Type': 'application/json'
            }
            return True
        except Exception as e:
            print(f"Sign in failed: {str(e)}")
            return False

    def sign_in_with_pat(self, personal_access_token_name: str, personal_access_token: str, site_name: str = "") -> bool:
        """Sign in to Tableau Server/Online using Personal Access Token"""
        try:
            # Store token details for TSC
            self.token_name = personal_access_token_name
            self.token_value = personal_access_token
            
            # Initialize TSC authentication
            self.tableau_auth = TSC.PersonalAccessTokenAuth(
                personal_access_token_name,
                personal_access_token,
                site_name
            )
            self.server = TSC.Server(self.server_url, use_server_version=True)
            
            # Sign in with TSC
            with self.server.auth.sign_in(self.tableau_auth):
                self.site_id = self.server.site_id
                
            # Also authenticate with REST API for backward compatibility
            url = f"{self.server_url}/api/{self.api_version}/auth/signin"
            payload = {
                "credentials": {
                    "personalAccessTokenName": personal_access_token_name,
                    "personalAccessTokenSecret": personal_access_token,
                    "site": {"contentUrl": site_name}
                }
            }
            
            response = requests.post(url, json=payload)
            response.raise_for_status()
            
            credentials = xmltodict.parse(response.text)['tsResponse']['credentials']
            self.token = credentials['@token']
            self.headers = {
                'X-Tableau-Auth': self.token,
                'Content-Type': 'application/json'
            }
            
            print("Successfully authenticated!")
            return True
            
        except Exception as e:
            print(f"Sign in failed: {str(e)}")
            return False

    def get_workbooks(self) -> List[Dict]:
        """Get list of available workbooks"""
        if not self.token:
            raise Exception("Not authenticated. Please sign in first.")
        
        url = f"{self.server_url}/api/{self.api_version}/sites/{self.site_id}/workbooks"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            # Try to parse response as XML first
            try:
                response_dict = xmltodict.parse(response.text)
                workbooks = response_dict['tsResponse']['workbooks']
                # Handle case where no workbooks exist
                if workbooks is None:
                    return []
                # Handle case where only one workbook exists
                if isinstance(workbooks['workbook'], dict):
                    return [workbooks['workbook']]
                # Handle case where multiple workbooks exist
                return workbooks['workbook']
            except Exception as xml_error:
                # If XML parsing fails, try JSON
                try:
                    return response.json()['workbooks']['workbook']
                except Exception as json_error:
                    print(f"Failed to parse response as XML or JSON")
                    print(f"XML error: {str(xml_error)}")
                    print(f"JSON error: {str(json_error)}")
                    print(f"Response text: {response.text}")
                    return []
                
        except Exception as e:
            print(f"Failed to get workbooks: {str(e)}")
            return []

    def get_views(self, workbook_id: str) -> List[Dict]:
        """Get list of views in a workbook"""
        if not self.token:
            raise Exception("Not authenticated. Please sign in first.")
        
        # Clean up the workbook ID
        workbook_id = str(workbook_id).strip()
        if workbook_id.startswith('{'):
            try:
                import ast
                workbook_dict = ast.literal_eval(workbook_id)
                workbook_id = workbook_dict.get('@id') or workbook_dict.get('id')
            except:
                pass
        
        url = f"{self.server_url}/api/{self.api_version}/sites/{self.site_id}/workbooks/{workbook_id}/views"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            # Try to parse response as XML first
            try:
                response_dict = xmltodict.parse(response.text)
                views = response_dict['tsResponse']['views']
                # Handle case where no views exist
                if views is None:
                    return []
                # Handle case where only one view exists
                if isinstance(views['view'], dict):
                    return [views['view']]
                # Handle case where multiple views exist
                return views['view']
            except Exception as xml_error:
                # If XML parsing fails, try JSON
                try:
                    return response.json()['views']['view']
                except Exception as json_error:
                    print(f"Failed to parse response as XML or JSON")
                    print(f"XML error: {str(xml_error)}")
                    print(f"JSON error: {str(json_error)}")
                    print(f"Response text: {response.text}")
                    return []
                
        except Exception as e:
            print(f"Failed to get views: {str(e)}")
            return []

    def download_view_data(self, view_ids: List[str], workbook_name: str = None) -> pd.DataFrame:
        """Download data from specific views using TSC"""
        if not self.tableau_auth or not self.server:
            raise Exception("Not authenticated with TSC. Please sign in first.")
        
        try:
            combined_data = pd.DataFrame()
            downloaded_views = []
            
            with self.server.auth.sign_in(self.tableau_auth):
                for view_id in view_ids:
                    try:
                        view = self.server.views.get_by_id(view_id)
                        self.server.views.populate_csv(view)
                        csv_data = b''.join(view.csv)
                        
                        if csv_data.strip():
                            df = pd.read_csv(io.StringIO(csv_data.decode('utf-8')))
                            df['Sheet Name'] = view.name
                            combined_data = pd.concat([combined_data, df], ignore_index=True)
                            downloaded_views.append(view.name)
                            print(f"Successfully downloaded data from view: {view.name}")
                        else:
                            print(f"No data found in view: {view.name}")
                    
                    except Exception as e:
                        print(f"Failed to download view {view_id}: {str(e)}")
            
            if not combined_data.empty:
                print(f"Successfully downloaded data from views: {', '.join(downloaded_views)}")
                return combined_data
            else:
                print("No data found in any of the selected views")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Failed to download view data: {str(e)}")
            return pd.DataFrame()

    def test_connection(self) -> bool:
        """Test the connection to Tableau Server"""
        try:
            response = requests.get(f"{self.server_url}/api/{self.api_version}/auth/signin")
            print(f"Server responded with status code: {response.status_code}")
            return True
        except Exception as e:
            print(f"Connection test failed: {str(e)}")
            return False

def save_to_database(df: pd.DataFrame, table_name: str, connection_string: str):
    """Save DataFrame to database using SQLAlchemy"""
    try:
        # Test connection first
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            # Try a simple query to verify connection
            conn.execute("SELECT 1")
        
        # If connection successful, save data
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data successfully saved to table: {table_name}")
        
    except Exception as e:
        error_message = str(e)
        if "connection refused" in error_message.lower():
            print("""
            Database connection failed. Please ensure:
            1. PostgreSQL is running
            2. The database exists
            3. User credentials are correct
            4. Port 5432 is not blocked
            """)
        else:
            print(f"Failed to save to database: {error_message}")

def get_server_url() -> str:
    """Prompt user for server URL with examples"""
    print("\nEnter your Tableau server URL.")
    print("Examples:")
    print("- Tableau Online (US): https://10ay.online.tableau.com")
    print("- Tableau Online (EU): https://10az.online.tableau.com")
    print("- Tableau Server: http://your-server-name")
    
    server_url = input("\nServer URL: ").strip()
    return server_url

def get_auth_method() -> str:
    """Prompt user for authentication method"""
    print("\nChoose authentication method:")
    print("1. Personal Access Token (Recommended, works with 2FA)")
    print("2. Username and Password (Not compatible with 2FA)")
    
    while True:
        choice = input("\nEnter choice (1 or 2): ").strip()
        if choice in ['1', '2']:
            return choice
        print("Invalid choice. Please enter 1 or 2.")

def main():
    # Get server URL from user
    server_url = get_server_url()
    
    # Initialize database connection
    database_connection = "postgresql://username:password@localhost:5432/dbname"
    # Example formats:
    # PostgreSQL: "postgresql://username:password@localhost:5432/dbname"
    # MySQL: "mysql://username:password@localhost:3306/dbname"
    # SQLite: "sqlite:///path/to/database.db"
    
    # Initialize Tableau connector
    connector = TableauConnector(server_url)
    
    # Test connection first
    print("\nTesting connection to server...")
    if not connector.test_connection():
        print("Unable to connect to server. Please check the URL and try again.")
        return
    
    # Get authentication method
    auth_method = get_auth_method()
    
    authenticated = False
    if auth_method == '1':
        # PAT authentication
        pat_name = input("Enter your Personal Access Token name: ")
        pat_secret = getpass("Enter your Personal Access Token secret: ")
        site_name = input("Enter site name (press Enter for default): ")
        authenticated = connector.sign_in_with_pat(pat_name, pat_secret, site_name)
    else:
        # Username/password authentication
        username = input("Enter your Tableau username: ")
        password = getpass("Enter your password: ")
        site_name = input("Enter site name (press Enter for default): ")
        authenticated = connector.sign_in(username, password, site_name)
    
    if not authenticated:
        print("Failed to sign in. Exiting...")
        return
    
    # Get available workbooks
    workbooks = connector.get_workbooks()
    print("\nAvailable workbooks:")
    for idx, wb in enumerate(workbooks, 1):
        # Handle both XML (@name) and JSON (name) formats
        workbook_name = wb.get('@name') or wb.get('name')
        print(f"{idx}. {workbook_name}")
    
    # Let user select a workbook
    wb_idx = int(input("\nSelect a workbook (enter number): ")) - 1
    selected_workbook = workbooks[wb_idx]
    
    # Get views in selected workbook
    # Use @id for XML format
    workbook_id = selected_workbook.get('@id') or selected_workbook.get('id')
    views = connector.get_views(workbook_id)
    print("\nAvailable views:")
    for idx, view in enumerate(views, 1):
        # Handle both XML (@name) and JSON (name) formats
        view_name = view.get('@name') or view.get('name')
        print(f"{idx}. {view_name}")
    
    # Let user select multiple views
    print("\nSelect views to download (enter numbers separated by commas):")
    view_indices = [int(idx.strip()) - 1 for idx in input().split(',')]
    selected_views = [views[idx] for idx in view_indices]
    
    # Download view data
    print("\nDownloading data...")
    view_ids = [view.get('@id') or view.get('id') for view in selected_views]
    df = connector.download_view_data(view_ids, workbook_name=selected_workbook.get('@name'))
    
    # Save to database
    if not df.empty:
        print("\nPreview of downloaded data:")
        print(df.head())
        print(f"\nTotal rows: {len(df)}")
        
        save_choice = input("\nDo you want to save this data to the database? (y/n): ")
        if save_choice.lower() == 'y':
            table_name = input("Enter table name to save data: ")
            save_to_database(df, table_name, database_connection)

if __name__ == "__main__":
    main() 