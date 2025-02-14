import sqlite3

# Path to the SQLite database file
db_path = 'data/tableau_data.db'

# Connect to the database
with sqlite3.connect(db_path) as conn:
    cursor = conn.cursor()
    
    # Execute PRAGMA table_info to get the schema of the schedule_runs table
    cursor.execute("PRAGMA table_info(schedule_runs);")
    schema_info = cursor.fetchall()
    
    # Print the schema information
    print("Schema of schedule_runs table:")
    for column in schema_info:
        print(column) 