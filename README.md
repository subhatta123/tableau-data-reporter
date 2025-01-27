# Tableau Data Reporter

A Streamlit application that allows users to connect to Tableau, download data, and create automatic dashboards with data analysis capabilities.

## Features

- Multi-organization user management with role-based access control
- Tableau server connection with PAT or username/password authentication
- Automatic dashboard generation with insights
- Interactive Q&A interface for data analysis
- Data isolation between organizations
- Secure user authentication and session management

## Prerequisites

- Python 3.8 or higher
- Tableau Server access or Tableau Online account
- OpenAI API key for Q&A functionality

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd tableau-data-reporter
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the project root:
```
OPENAI_API_KEY=your-api-key-here
```

## Initial Setup

1. Run the application for the first time:
```bash
streamlit run tableau_streamlit_app.py
```

2. Log in as superadmin:
   - Username: superadmin
   - Password: superadmin

3. Create organizations and manage users through the admin interface.

## Deployment Options

### 1. Streamlit Cloud (Recommended for small teams)

1. Push your code to GitHub
2. Visit [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub repository
4. Add your environment variables (OPENAI_API_KEY)
5. Deploy

### 2. Docker Deployment

1. Build the Docker image:
```bash
docker build -t tableau-reporter .
```

2. Run the container:
```bash
docker run -p 8501:8501 -e OPENAI_API_KEY=your-key-here tableau-reporter
```

### 3. Server Deployment (e.g., AWS, GCP, Azure)

1. Set up a virtual machine
2. Install required packages:
```bash
sudo apt-get update
sudo apt-get install python3-pip python3-venv
```

3. Clone repository and follow installation steps
4. Set up a service (e.g., systemd) to run the application
5. Configure nginx as a reverse proxy

## Security Considerations

- Store sensitive credentials in environment variables
- Use HTTPS in production
- Regularly backup the SQLite database
- Monitor application logs
- Keep dependencies updated

## Maintenance

1. Regular backups:
```bash
# Backup database
cp data/tableau_data.db data/tableau_data.db.backup
```

2. Update dependencies:
```bash
pip install -r requirements.txt --upgrade
```

3. Monitor logs:
```bash
tail -f streamlit.log
```

## Support

For issues or questions:
1. Check the documentation
2. Submit an issue on GitHub
3. Contact system administrator

## License

This project is licensed under the MIT License - see the LICENSE file for details. 