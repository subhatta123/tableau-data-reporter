# Tableau Data Reporter

A Streamlit-based application that allows users to connect to Tableau Server, download data from workbooks and views, schedule automated reports, and send them via email and WhatsApp.

## Features

- 🔌 Connect to Tableau Server using Personal Access Token or Username/Password
- 📊 Download data from Tableau workbooks and views
- 📅 Schedule automated reports (one-time, daily, weekly, monthly)
- 📧 Send reports via email with PDF attachments
- 📱 WhatsApp integration for report notifications
- 👥 User management with different permission levels (normal, power, superadmin)
- 🏢 Organization management for enterprise use
- 💬 Chat with data feature for power users
- 🎨 Customizable report formatting

## Prerequisites

- Python 3.8 or higher
- Tableau Server access
- SMTP server for email notifications
- Twilio account for WhatsApp notifications (optional)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/tableau-data-reporter.git
cd tableau-data-reporter
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file based on `.env.example`:
```bash
cp .env.example .env
```

5. Update the `.env` file with your configuration:
- SMTP settings for email notifications
- Twilio credentials for WhatsApp notifications (optional)
- Base URL for your application

## Directory Structure

```
tableau-data-reporter/
├── data/                  # Database and data storage
├── static/               # Static files and report storage
│   └── reports/         # Generated PDF reports
├── .env                 # Environment variables
├── .gitignore          # Git ignore file
├── README.md           # This file
├── requirements.txt    # Python dependencies
├── tableau_streamlit_app.py  # Main application
├── report_manager_new.py     # Report management
├── user_management.py        # User management
└── report_formatter_new.py   # Report formatting
```

## Usage

1. Start the application:
```bash
streamlit run tableau_streamlit_app.py
```

2. Access the application at `http://localhost:8501`

3. Login with default superadmin credentials:
   - Username: superadmin
   - Password: superadmin

4. Configure your organization and users

5. Connect to Tableau Server and start managing reports

## Environment Variables

| Variable | Description | Required |
|----------|-------------|-----------|
| SMTP_SERVER | SMTP server address | Yes |
| SMTP_PORT | SMTP server port | Yes |
| SENDER_EMAIL | Sender email address | Yes |
| SENDER_PASSWORD | Sender email password/token | Yes |
| TWILIO_ACCOUNT_SID | Twilio Account SID | No |
| TWILIO_AUTH_TOKEN | Twilio Auth Token | No |
| TWILIO_WHATSAPP_NUMBER | Twilio WhatsApp number | No |
| BASE_URL | Application base URL | Yes |

## Security

- All passwords are hashed using SHA-256
- Environment variables are used for sensitive credentials
- User permissions are role-based
- PDF reports are generated with unique hashes
- Report links expire after 24 hours

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please open an issue in the GitHub repository. 