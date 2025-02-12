# Tableau Data Reporter

A Python application for automated generation and scheduling of Tableau reports with customizable formatting options.

## Features

- Generate formatted reports from Tableau workbooks
- Schedule reports on a one-time, daily, weekly, or monthly basis
- Customize report formatting including titles, table styles, and grid lines
- Email and WhatsApp notifications for scheduled reports
- Public link generation for easy report sharing

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Configure your Tableau server credentials in the environment variables
2. Run the application:
```bash
streamlit run report_manager_new.py
```

3. Use the web interface to:
   - Generate reports
   - Schedule automated report generation
   - Customize report formatting
   - View and manage scheduled reports

## Configuration

The application requires the following environment variables:
- TABLEAU_SERVER_URL
- TABLEAU_USERNAME
- TABLEAU_PASSWORD
- TABLEAU_SITE

## File Structure

- `report_manager_new.py`: Main application file with Streamlit interface
- `report_formatter_new.py`: Report generation and formatting logic
- `fix_superadmin.py`: Utility for admin configurations
- `requirements.txt`: Python package dependencies

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 