from report_manager_new import ReportManager
from datetime import datetime

def check_scheduler():
    report_manager = ReportManager()
    
    print("\nActive Jobs in Scheduler:")
    print("=" * 50)
    
    # Get all jobs from the scheduler
    jobs = report_manager.scheduler.get_jobs()
    
    if not jobs:
        print("No active jobs found in scheduler")
        return
    
    for job in jobs:
        print(f"\nJob ID: {job.id}")
        print(f"Name: {job.name}")
        print(f"Trigger: {job.trigger}")
        print(f"Next run time: {job.next_run_time}")
        
        # Get schedule details from database
        schedules = report_manager.get_active_schedules()
        if job.id in schedules:
            schedule = schedules[job.id]
            print(f"Schedule type: {schedule['schedule_config']['type']}")
            print(f"Description: {report_manager.get_schedule_description(schedule['schedule_config'])}")
        
        print("-" * 50)

if __name__ == "__main__":
    check_scheduler() 