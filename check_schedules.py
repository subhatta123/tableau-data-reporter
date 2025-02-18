from report_manager_new import ReportManager
import json

def check_schedules():
    report_manager = ReportManager()
    schedules = report_manager.get_active_schedules()
    
    print("\nActive Schedules in Database:")
    print("=" * 50)
    
    for job_id, schedule in schedules.items():
        print(f"\nSchedule ID: {job_id}")
        print(f"Dataset: {schedule['dataset_name']}")
        print(f"Type: {schedule['schedule_config']['type']}")
        print(f"Configuration: {json.dumps(schedule['schedule_config'], indent=2)}")
        print(f"Description: {report_manager.get_schedule_description(schedule['schedule_config'])}")
        print(f"Next run: {schedule.get('next_run', 'Not set')}")
        print("-" * 50)

if __name__ == "__main__":
    check_schedules() 