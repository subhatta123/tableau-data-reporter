import sqlite3
import json
from pathlib import Path

def check_database():
    db_path = Path("data/tableau_data.db")
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Check tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            print("\nTables in database:")
            for table in tables:
                print(f"- {table[0]}")
            
            # Check schedules table
            print("\nSchedules table contents:")
            cursor.execute("SELECT * FROM schedules")
            rows = cursor.fetchall()
            
            if not rows:
                print("No schedules found in database")
                return
            
            # Get column names
            cursor.execute("PRAGMA table_info(schedules)")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Print each schedule
            for row in rows:
                print("\n" + "="*50)
                for col, value in zip(columns, row):
                    if col in ['schedule_config', 'email_config', 'format_config'] and value:
                        try:
                            parsed = json.loads(value)
                            print(f"{col}:")
                            print(json.dumps(parsed, indent=2))
                        except:
                            print(f"{col}: {value}")
                    else:
                        print(f"{col}: {value}")
    
    except Exception as e:
        print(f"Error checking database: {str(e)}")

if __name__ == "__main__":
    check_database() 