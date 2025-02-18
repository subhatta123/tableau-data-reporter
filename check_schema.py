import sqlite3

def check_schema():
    try:
        with sqlite3.connect('data/tableau_data.db') as conn:
            cursor = conn.cursor()
            
            # Check schedule_runs table
            print("\nSchema of schedule_runs table:")
            cursor.execute("PRAGMA table_info(schedule_runs)")
            for row in cursor.fetchall():
                print(row)
                
            # Check schedules table
            print("\nSchema of schedules table:")
            cursor.execute("PRAGMA table_info(schedules)")
            for row in cursor.fetchall():
                print(row)
    except Exception as e:
        print(f"Error checking schema: {str(e)}")

if __name__ == "__main__":
    check_schema() 