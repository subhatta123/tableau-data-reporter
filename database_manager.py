import streamlit as st
import sqlite3
from pathlib import Path

class DatabaseManager:
    def __init__(self):
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / "tableau_data.db"
        self.db_url = f"sqlite:///{self.db_path}"

    def list_tables(self, include_internal=False):
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