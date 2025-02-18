from report_manager_new import ReportManager

def main():
    """Reinitialize the database with the correct schema"""
    try:
        report_manager = ReportManager()
        report_manager._init_database()
        print("Database reinitialized successfully!")
    except Exception as e:
        print(f"Error reinitializing database: {str(e)}")

if __name__ == "__main__":
    main() 