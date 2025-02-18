from report_manager_new import ReportManager
from datetime import datetime, timedelta
import os
import sqlite3

def test_schedules():
    # Initialize ReportManager
    report_manager = ReportManager()
    
    # Email configuration with hardcoded values
    email_config = {
        'sender_email': 'tableautoexcel@gmail.com',
        'sender_password': 'ptvy yerb ymbj fngu',
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'recipients': ['tableautoexcel@gmail.com'],
        'format': 'PDF'
    }
    
    try:
        # Create test data table if it doesn't exist
        with sqlite3.connect('data/tableau_data.db') as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS test_data")
            cursor.execute("""
                CREATE TABLE test_data (
                    id INTEGER PRIMARY KEY,
                    value REAL
                )
            """)
            # Add some test data
            cursor.execute("INSERT INTO test_data (id, value) VALUES (1, 100)")
            conn.commit()
            print("Created test data table")
    except Exception as e:
        print(f"Error creating test data: {str(e)}")
        return
    
    # Test weekly schedule
    weekly_config = {
        'type': 'weekly',
        'days': [0, 2, 4],  # Monday, Wednesday, Friday
        'hour': (datetime.now() + timedelta(minutes=5)).hour,
        'minute': (datetime.now() + timedelta(minutes=5)).minute
    }
    
    print("\nCreating weekly schedule...")
    weekly_job_id = report_manager.schedule_report(
        dataset_name='test_data',
        email_config=email_config,
        schedule_config=weekly_config
    )
    
    if weekly_job_id:
        print("\nWeekly schedule created successfully:")
        print(f"Job ID: {weekly_job_id}")
        print("Days: Monday, Wednesday, Friday")
        print(f"Time: {weekly_config['hour']:02d}:{weekly_config['minute']:02d}")
    else:
        print("Failed to create weekly schedule")
    
    # Test monthly schedule with last day
    monthly_config = {
        'type': 'monthly',
        'day_option': 'Last Day',
        'hour': (datetime.now() + timedelta(minutes=10)).hour,
        'minute': (datetime.now() + timedelta(minutes=10)).minute
    }
    
    print("\nCreating monthly schedule...")
    monthly_job_id = report_manager.schedule_report(
        dataset_name='test_data',
        email_config=email_config,
        schedule_config=monthly_config
    )
    
    if monthly_job_id:
        print("\nMonthly schedule created successfully:")
        print(f"Job ID: {monthly_job_id}")
        print("Day: Last day of month")
        print(f"Time: {monthly_config['hour']:02d}:{monthly_config['minute']:02d}")
    else:
        print("Failed to create monthly schedule")
    
    # Verify schedules
    print("\nVerifying active schedules...")
    active_schedules = report_manager.get_active_schedules()
    print(f"\nFound {len(active_schedules)} active schedules:")
    
    for job_id, schedule in active_schedules.items():
        print(f"\nSchedule ID: {job_id}")
        print(f"Type: {schedule['schedule_config']['type']}")
        print(f"Description: {report_manager.get_schedule_description(schedule['schedule_config'])}")
        print(f"Next run: {schedule.get('next_run', 'Not set')}")
        print("-" * 50)

if __name__ == "__main__":
    test_schedules() 