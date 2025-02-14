import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import io
import json
import os
from datetime import datetime, timedelta
import sqlite3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from pathlib import Path
import uuid
from twilio.rest import Client
import hashlib
import shutil

class ReportManager:
    def __init__(self):
        """Initialize report manager"""
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Create reports directory for storing generated reports
        self.reports_dir = self.data_dir / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        # Create a directory for public access to reports
        self.public_reports_dir = Path("static/reports")
        self.public_reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self.db_path = self.data_dir / "tableau_data.db"
        self._init_database()
        
        self.schedules_file = self.data_dir / "schedules.json"
        if not self.schedules_file.exists():
            self.save_schedules({})
        
        # Initialize scheduler
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.load_saved_schedules()
        
        # Initialize Twilio client for WhatsApp
        self.twilio_client = None
        self.twilio_whatsapp_number = os.getenv('TWILIO_WHATSAPP_NUMBER')
        self.twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID')
        self.twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN')
        
        # Base URL for report access
        self.base_url = os.getenv('BASE_URL', 'http://localhost:8501')
        
        if all([self.twilio_account_sid, self.twilio_auth_token, self.twilio_whatsapp_number]):
            try:
                self.twilio_client = Client(self.twilio_account_sid, self.twilio_auth_token)
                print("Twilio client initialized successfully")
            except Exception as e:
                print(f"Failed to initialize Twilio client: {str(e)}")
        else:
            print("Twilio configuration incomplete. Please check your .env file")
    
    def _init_database(self):
        """Initialize SQLite database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Drop existing tables if they exist
            cursor.execute("DROP TABLE IF EXISTS schedule_runs")
            cursor.execute("DROP TABLE IF EXISTS schedules")
            
            # Create schedules table
            cursor.execute("""
                CREATE TABLE schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_name TEXT NOT NULL,
                    schedule_type TEXT NOT NULL,
                    schedule_config TEXT NOT NULL,
                    email_config TEXT NOT NULL,
                    format_config TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_run TIMESTAMP,
                    next_run TIMESTAMP,
                    status TEXT DEFAULT 'active'
                )
            """)
            
            # Create schedule_runs table
            cursor.execute("""
                CREATE TABLE schedule_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    schedule_id INTEGER NOT NULL,
                    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    FOREIGN KEY (schedule_id) REFERENCES schedules (id)
                )
            """)
            
            conn.commit()
            print("Database initialized successfully")

    def generate_pdf(self, df, title):
        """Generate PDF report from DataFrame"""
        buffer = io.BytesIO()
        # Use landscape orientation for wide tables with wider margins
        doc = SimpleDocTemplate(
            buffer, 
            pagesize=landscape(letter),
            rightMargin=30,
            leftMargin=30,
            topMargin=50,
            bottomMargin=50
        )
        elements = []
        
        # Add title
        styles = getSampleStyleSheet()
        title_style = styles['Title']
        title_style.fontSize = 24
        title_style.spaceAfter = 30
        elements.append(Paragraph(title, title_style))
        
        # Add timestamp with better styling
        timestamp_style = styles['Normal']
        timestamp_style.fontSize = 10
        timestamp_style.textColor = colors.gray
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elements.append(Paragraph(f"Generated on: {timestamp}", timestamp_style))
        elements.append(Spacer(1, 30))
        
        # Add basic summary statistics with better styling
        summary_title_style = styles['Heading1']
        summary_title_style.fontSize = 18
        summary_title_style.spaceAfter = 20
        elements.append(Paragraph("Summary Statistics", summary_title_style))
        
        summary_data = [
            ["Metric", "Value"],  # Header row
            ["Total Rows", f"{len(df):,}"],
            ["Total Columns", str(len(df.columns))]
        ]
        
        # Create summary table with better styling
        summary_table = Table(
            summary_data,
            colWidths=[doc.width/4, doc.width/4],
            style=[
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2d5d7b')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f5')),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 10),
                ('GRID', (0, 0), (-1, -1), 1, colors.gray),
                ('ROWHEIGHT', (0, 0), (-1, -1), 25),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]
        )
        elements.append(summary_table)
        elements.append(Spacer(1, 30))
        
        # Add data table title
        data_title_style = styles['Heading1']
        data_title_style.fontSize = 18
        data_title_style.spaceAfter = 20
        elements.append(Paragraph("Data Preview", data_title_style))
        
        # Prepare data for main table with better formatting
        formatted_df = df.copy()
        
        # Format numeric values with commas and proper decimal places
        for col in df.select_dtypes(include=['number']).columns:
            try:
                # Check if the column contains large numbers (like sales figures)
                if formatted_df[col].max() > 1000:
                    # Format with commas and 2 decimal places
                    formatted_df[col] = formatted_df[col].apply(lambda x: f"{x:,.2f}")
                else:
                    # For smaller numbers, just use 2 decimal places
                    formatted_df[col] = formatted_df[col].apply(lambda x: f"{x:.2f}")
            except:
                continue
        
        # Prepare table data
        data = [formatted_df.columns.tolist()]  # Header row
        max_rows = 50  # Limit rows for better readability
        if len(formatted_df) > max_rows:
            data.extend(formatted_df.head(max_rows).values.tolist())
            note_style = styles['Italic']
            note_style.textColor = colors.gray
            elements.append(Paragraph(
                f"* Showing first {max_rows:,} rows of {len(df):,} total rows",
                note_style
            ))
        else:
            data.extend(formatted_df.values.tolist())
        
        # Calculate column widths based on content
        num_cols = len(formatted_df.columns)
        available_width = doc.width - 60  # Account for margins
        
        # Define minimum and maximum column widths
        min_col_width = 60  # Minimum width in points
        max_col_width = 150  # Maximum width in points
        
        # Calculate column widths based on column names and content
        col_widths = []
        for col_idx in range(num_cols):
            # Get maximum content width in this column
            col_content = [str(row[col_idx]) for row in data]
            max_content_len = max(len(str(content)) for content in col_content)
            
            # Calculate width based on content (approximate 6 points per character)
            calculated_width = max_content_len * 6
            
            # Constrain width between min and max
            col_width = max(min_col_width, min(calculated_width, max_col_width))
            col_widths.append(col_width)
        
        # Adjust widths to fit available space
        total_width = sum(col_widths)
        if total_width > available_width:
            # Scale down proportionally if too wide
            scale_factor = available_width / total_width
            col_widths = [width * scale_factor for width in col_widths]
        
        # Create main table with improved styling
        main_table = Table(
            data,
            colWidths=col_widths,
            repeatRows=1,
            style=[
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2d5d7b')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),  # Reduced font size
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f5')),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 8),  # Reduced font size
                ('GRID', (0, 0), (-1, -1), 1, colors.gray),
                ('ROWHEIGHT', (0, 0), (-1, -1), 20),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),  # Added padding
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),  # Added padding
                ('WORDBREAK', (0, 0), (-1, -1), True),  # Enable word wrapping
            ]
        )
        elements.append(main_table)
        
        # Build PDF
        doc.build(elements)
        buffer.seek(0)
        return buffer

    def generate_report_link(self, report_path: Path, expiry_hours: int = 24) -> str:
        """Generate a secure, time-limited link for report access"""
        try:
            # Create reports directory if it doesn't exist
            self.public_reports_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate a unique token
            token = str(uuid.uuid4())
            
            # Create a secure hash of the token
            hash_obj = hashlib.sha256(token.encode())
            secure_hash = hash_obj.hexdigest()
            
            # Create the public file path
            public_path = self.public_reports_dir / f"{secure_hash}{report_path.suffix}"
            if public_path.exists():
                public_path.unlink()
            
            # Copy the file to public directory
            shutil.copy2(report_path, public_path)
            print(f"Copied report to public directory: {public_path}")
            
            # Store metadata about the link
            metadata = {
                'original_path': str(report_path),
                'created_at': datetime.now().isoformat(),
                'expires_at': (datetime.now() + timedelta(hours=expiry_hours)).isoformat()
            }
            
            # Save metadata
            metadata_path = self.public_reports_dir / f"{secure_hash}.json"
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Generate the full URL using the actual file path
            url = f"{self.base_url}/static/reports/{secure_hash}{report_path.suffix}"
            print(f"Generated report URL: {url}")
            return url
            
        except Exception as e:
            print(f"Failed to generate report link: {str(e)}")
            print(f"Error details: {e.__dict__ if hasattr(e, '__dict__') else 'No details'}")
            return None

    def save_report(self, df: pd.DataFrame, dataset_name: str, format: str) -> tuple:
        """Save report to file and return file path and link"""
        try:
            # Generate unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{dataset_name}_{timestamp}"
            
            if format.upper() == 'CSV':
                # Save as CSV
                file_path = self.reports_dir / f"{filename}.csv"
                df.to_csv(file_path, index=False)
            else:
                # Save as PDF
                file_path = self.reports_dir / f"{filename}.pdf"
                pdf_buffer = self.generate_pdf(df, f"Report: {dataset_name}")
                with open(file_path, 'wb') as f:
                    f.write(pdf_buffer.getvalue())
            
            # Generate shareable link
            share_link = self.generate_report_link(file_path)
            
            return file_path, share_link
            
        except Exception as e:
            print(f"Failed to save report: {str(e)}")
            return None, None

    def _serialize_format_config(self, format_config):
        """Serialize format config for JSON storage"""
        if not format_config:
            return None
            
        # Create a serializable copy of the format config
        serializable_config = {
            'page_size': format_config.get('page_size', None),
            'orientation': format_config.get('orientation', 'portrait'),
            'margins': format_config.get('margins', None),
            'chart_size': format_config.get('chart_size', None),
            'report_content': format_config.get('report_content', {}),
        }
        
        # Handle title style
        if 'title_style' in format_config:
            title_style = format_config['title_style']
            # Convert color to hex string if it exists
            text_color = getattr(title_style, 'textColor', None)
            if text_color:
                if hasattr(text_color, 'rgb'):
                    # Convert RGB values to integers (0-255)
                    rgb = [int(x * 255) if isinstance(x, float) else x for x in text_color.rgb()]
                    text_color = '#{:02x}{:02x}{:02x}'.format(*rgb)
                elif hasattr(text_color, 'hexval'):
                    text_color = '#{:06x}'.format(text_color.hexval())
                else:
                    text_color = '#000000'
            else:
                text_color = '#000000'
                
            serializable_config['title_style'] = {
                'fontName': getattr(title_style, 'fontName', 'Helvetica'),
                'fontSize': getattr(title_style, 'fontSize', 24),
                'alignment': getattr(title_style, 'alignment', 1),  # 0=left, 1=center, 2=right
                'textColor': text_color,
                'spaceAfter': getattr(title_style, 'spaceAfter', 30)
            }
        
        # Handle table style
        if 'table_style' in format_config:
            table_style = format_config['table_style']
            # Convert TableStyle commands to serializable format
            serializable_config['table_style'] = []
            
            # Get the commands from the TableStyle object
            if hasattr(table_style, 'commands'):
                commands = table_style.commands
            elif hasattr(table_style, '_cmds'):
                commands = table_style._cmds
            else:
                commands = []
                
            for cmd in commands:
                # Convert command to serializable format
                try:
                    if len(cmd) != 4:
                        print(f"Skipping invalid command: {cmd}")
                        continue
                        
                    cmd_name, start_pos, end_pos, value = cmd
                    
                    # Convert color objects to hex strings
                    if hasattr(value, 'rgb'):
                        # Convert RGB values to integers (0-255)
                        rgb = [int(x * 255) if isinstance(x, float) else x for x in value.rgb()]
                        value = '#{:02x}{:02x}{:02x}'.format(*rgb)
                    elif hasattr(value, 'hexval'):
                        value = '#{:06x}'.format(value.hexval())
                    elif isinstance(value, (int, float)):
                        # Keep numeric values as is
                        value = float(value)
                    
                    # Convert tuples to lists for JSON serialization
                    serialized_cmd = [
                        cmd_name,
                        list(start_pos) if isinstance(start_pos, tuple) else start_pos,
                        list(end_pos) if isinstance(end_pos, tuple) else end_pos,
                        value
                    ]
                    serializable_config['table_style'].append(serialized_cmd)
                except Exception as e:
                    print(f"Error serializing table style command {cmd}: {str(e)}")
                    continue
        
        return serializable_config
    
    def _deserialize_format_config(self, serialized_config):
        """Deserialize format config from JSON storage"""
        if not serialized_config:
            return None
            
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.colors import HexColor, Color
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
        
        # Create a new format config
        format_config = {
            'page_size': serialized_config.get('page_size', None),
            'orientation': serialized_config.get('orientation', 'portrait'),
            'margins': serialized_config.get('margins', None),
            'chart_size': serialized_config.get('chart_size', None),
            'report_content': serialized_config.get('report_content', {})
        }
        
        # Reconstruct title style
        if 'title_style' in serialized_config:
            title_style_data = serialized_config['title_style']
            alignment_map = {0: TA_LEFT, 1: TA_CENTER, 2: TA_RIGHT}
            
            # Convert hex color string to Color object
            text_color = title_style_data.get('textColor', '#000000')
            if isinstance(text_color, str) and text_color.startswith('#'):
                text_color = HexColor(text_color)
            
            format_config['title_style'] = ParagraphStyle(
                'CustomTitle',
                fontName=title_style_data.get('fontName', 'Helvetica'),
                fontSize=title_style_data.get('fontSize', 24),
                alignment=alignment_map.get(title_style_data.get('alignment', 1), TA_CENTER),
                textColor=text_color,
                spaceAfter=title_style_data.get('spaceAfter', 30)
            )
        
        # Reconstruct table style
        if 'table_style' in serialized_config:
            from reportlab.platypus import TableStyle
            try:
                # Convert serialized commands back to TableStyle format
                commands = []
                for cmd in serialized_config['table_style']:
                    # Convert command back to proper format
                    command_name = cmd[0]
                    start_pos = tuple(cmd[1]) if isinstance(cmd[1], list) else cmd[1]
                    end_pos = tuple(cmd[2]) if isinstance(cmd[2], list) else cmd[2]
                    
                    # Handle color values
                    value = cmd[3]
                    if isinstance(value, str) and value.startswith('#'):
                        value = HexColor(value)
                    elif isinstance(value, (int, float)):
                        value = float(value)  # Convert to float for consistency
                    
                    commands.append((command_name, start_pos, end_pos, value))
                
                format_config['table_style'] = TableStyle(commands)
            except Exception as e:
                print(f"Error reconstructing table style: {str(e)}")
                # Use a default table style if reconstruction fails
                format_config['table_style'] = TableStyle([
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('GRID', (0, 0), (-1, -1), 1, colors.gray)
                ])
        
        return format_config

    def schedule_report(self, dataset_name: str, email_config: dict, schedule_config: dict, format_config: dict = None) -> str:
        """Schedule a new report"""
        try:
            print(f"\nAttempting to schedule report for dataset: {dataset_name}")
            print(f"Schedule config: {schedule_config}")
            print(f"Email config: {email_config}")
            
            # Input validation
            if not dataset_name:
                raise ValueError("Dataset name is required")
            if not email_config or not email_config.get('recipients'):
                raise ValueError("Email configuration with recipients is required")
            if not schedule_config or 'type' not in schedule_config:
                raise ValueError("Schedule configuration with type is required")

            # Generate job_id first
            job_id = str(uuid.uuid4())
            print(f"Generated job ID: {job_id}")
            
            # Prepare format config
            if format_config is None:
                format_config = {}
            
            # Ensure report content is included
            if 'report_content' not in format_config:
                format_config['report_content'] = {
                    'report_title': f"Report: {dataset_name}",
                    'selected_columns': None,
                    'include_row_count': True,
                    'include_totals': True,
                    'include_averages': True
                }
            
            # Create the job based on schedule type
            try:
                if schedule_config['type'] == 'one-time':
                    if 'date' not in schedule_config:
                        raise ValueError("Date not specified for one-time schedule")
                    
                    schedule_date = datetime.strptime(
                        f"{schedule_config['date']} {schedule_config['hour']:02d}:{schedule_config['minute']:02d}:00",
                        "%Y-%m-%d %H:%M:%S"
                    )
                    
                    if schedule_date <= datetime.now():
                        raise ValueError("Schedule date must be in the future")
                    
                    print(f"Scheduling one-time job for: {schedule_date}")
                    self.scheduler.add_job(
                        func=self.send_report,
                        trigger='date',
                        run_date=schedule_date,
                        args=[dataset_name, email_config, format_config],
                        id=job_id,
                        name=f"Report_{dataset_name}"
                    )
                
                elif schedule_config['type'] in ['daily', 'weekly', 'monthly']:
                    trigger_args = {
                        'hour': schedule_config['hour'],
                        'minute': schedule_config['minute']
                    }
                    
                    if schedule_config['type'] == 'weekly':
                        trigger_args['day_of_week'] = schedule_config['day']
                    elif schedule_config['type'] == 'monthly':
                        trigger_args['day'] = schedule_config['day']
                    
                    print(f"Scheduling {schedule_config['type']} job with args: {trigger_args}")
                    self.scheduler.add_job(
                        func=self.send_report,
                        trigger='cron',
                        args=[dataset_name, email_config, format_config],
                        id=job_id,
                        name=f"Report_{dataset_name}",
                        **trigger_args
                    )
                else:
                    raise ValueError(f"Invalid schedule type: {schedule_config['type']}")
                
                # Verify the job was added
                job = self.scheduler.get_job(job_id)
                if not job:
                    raise ValueError("Failed to add job to scheduler")
                
                print(f"Job successfully added to scheduler. Next run time: {job.next_run_time}")
                
                # Save schedule to file
                schedules = self.load_schedules()
                schedules[job_id] = {
                    'dataset_name': dataset_name,
                    'email_config': email_config,
                    'schedule_config': schedule_config,
                    'format_config': self._serialize_format_config(format_config),
                    'created_at': datetime.now().isoformat()
                }
                self.save_schedules(schedules)
                
                print(f"Successfully scheduled report with ID: {job_id}")
                return job_id
                
            except Exception as e:
                print(f"Failed to create schedule: {str(e)}")
                # Clean up if job was partially created
                if self.scheduler.get_job(job_id):
                    self.scheduler.remove_job(job_id)
                raise
                
        except Exception as e:
            print(f"Failed to schedule report: {str(e)}")
            return None

    def verify_whatsapp_number(self, to_number: str) -> bool:
        """Verify if a WhatsApp number is valid and opted-in"""
        try:
            # Clean up the phone number
            to_number = ''.join(filter(str.isdigit, to_number))
            
            # Add country code if missing
            if not to_number.startswith('1'):
                to_number = '1' + to_number
            
            # Check if the number exists in Twilio
            numbers = self.twilio_client.incoming_phone_numbers.list(
                phone_number=f"+{to_number}"
            )
            
            return len(numbers) > 0
        except Exception as e:
            print(f"Failed to verify WhatsApp number: {str(e)}")
            return False

    def send_whatsapp_message(self, to_number: str, message: str) -> bool:
        """Send WhatsApp message with improved error handling"""
        try:
            if not self.twilio_client:
                print("Twilio client not initialized. Check your environment variables.")
                return False

            # Clean up the phone numbers
            from_number = self.twilio_whatsapp_number.strip()
            to_number = to_number.strip()
            
            # Add whatsapp: prefix if not present
            if not from_number.startswith('whatsapp:'):
                from_number = f'whatsapp:{from_number}'
            if not to_number.startswith('whatsapp:'):
                to_number = f'whatsapp:{to_number}'
            
            print(f"Attempting to send WhatsApp message from {from_number} to {to_number}")
            
            try:
                # First, try to send the actual message
                message = self.twilio_client.messages.create(
                    from_=from_number,
                    body=message,
                    to=to_number
                )
                print(f"WhatsApp message sent successfully with SID: {message.sid}")
                return True
            except Exception as e:
                error_msg = str(e)
                print(f"WhatsApp error: {error_msg}")
                
                if "not currently opted in" in error_msg.lower():
                    # Send sandbox join instructions
                    sandbox_message = f"""
                    *Welcome to Tableau Data Reporter!*
                    
                    To receive notifications, please:
                    1. Save {self.twilio_whatsapp_number} in your contacts
                    2. Send 'join' to this number on WhatsApp
                    3. Wait for confirmation before we can send you reports
                    
                    This is a one-time setup process.
                    """
                    
                    try:
                        message = self.twilio_client.messages.create(
                            from_=from_number,
                            body=sandbox_message,
                            to=to_number
                        )
                        print(f"Sent sandbox join instructions to {to_number}")
                        return False
                    except Exception as sandbox_error:
                        print(f"Failed to send sandbox instructions: {str(sandbox_error)}")
                        return False
                else:
                    raise e

        except Exception as e:
            print(f"Failed to send WhatsApp message: {str(e)}")
            if "not a valid WhatsApp" in str(e):
                print("Please make sure you're using a valid WhatsApp number with country code")
            elif "not currently opted in" in str(e):
                print("Recipient needs to opt in to receive messages")
            return False

    def send_report(self, dataset_name: str, email_config: dict, format_config: dict = None):
        """Send scheduled report"""
        try:
            print(f"\nStarting to send report for dataset: {dataset_name}")
            print(f"Email config: {email_config}")
            
            # Load dataset
            with sqlite3.connect('data/tableau_data.db') as conn:
                print("Loading dataset from database...")
                df = pd.read_sql_query(f"SELECT * FROM '{dataset_name}'", conn)
                print(f"Loaded {len(df)} rows from dataset")
            
            if df.empty:
                print(f"No data found in dataset: {dataset_name}")
                return
            
            # Get the message body or use default
            message_body = email_config.get('body', '').strip()
            if not message_body:
                message_body = f"Please find attached the scheduled report for dataset: {dataset_name}"
            
            print("Generating report...")
            # Generate report with formatting settings
            if format_config:
                from report_formatter_new import ReportFormatter
                formatter = ReportFormatter()
                
                # Apply saved formatting settings
                formatter.page_size = format_config.get('page_size', formatter.page_size)
                formatter.orientation = format_config.get('orientation', formatter.orientation)
                formatter.margins = format_config.get('margins', formatter.margins)
                formatter.title_style = format_config.get('title_style', formatter.title_style)
                formatter.table_style = format_config.get('table_style', formatter.table_style)
                formatter.chart_size = format_config.get('chart_size', formatter.chart_size)
                
                # Set report content directly instead of using session state
                report_content = format_config.get('report_content', {})
                selected_columns = report_content.get('selected_columns', df.columns.tolist())
                
                # Generate formatted report
                pdf_buffer = formatter.generate_report(
                    df[selected_columns] if selected_columns else df,
                    include_row_count=report_content.get('include_row_count', True),
                    include_totals=report_content.get('include_totals', True),
                    include_averages=report_content.get('include_averages', True),
                    report_title=report_content.get('report_title', f"Report: {dataset_name}")
                )
                
                # Save report and get shareable link
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{dataset_name}_{timestamp}.pdf"
                file_path = self.reports_dir / filename
                
                with open(file_path, 'wb') as f:
                    f.write(pdf_buffer.getvalue())
                print(f"Report saved to: {file_path}")
            else:
                # Use default formatting if no format_config provided
                print("Using default formatting...")
                file_path, _ = self.save_report(df, dataset_name, email_config.get('format', 'PDF'))
            
            if not file_path:
                raise Exception("Failed to generate report file")
            
            # Generate shareable link with proper base URL
            base_url = email_config.get('base_url', 'http://localhost:8501')
            share_link = self.generate_report_link(file_path)  # Use the generate_report_link method
            if not share_link:
                share_link = f"{base_url.rstrip('/')}/reports/{file_path.name}"
            print(f"Generated share link: {share_link}")
            
            print("Preparing email...")
            # Create email
            msg = MIMEMultipart()
            msg['From'] = email_config['sender_email']
            msg['To'] = ', '.join(email_config['recipients'])
            msg['Subject'] = f"Scheduled Report: {dataset_name}"
            
            # Format email body with custom message, report details, and link
            email_body = f"""{message_body}

Report Details:
- Dataset: {dataset_name}
- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Format: {email_config.get('format', 'PDF')}

View and download your report here: {share_link}
(Link expires in 24 hours)

This is an automated report. Please do not reply to this email."""

            msg.attach(MIMEText(email_body, 'plain'))
            
            # Attach report file
            print("Attaching report file...")
            with open(file_path, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype=file_path.suffix[1:])
                attachment.add_header('Content-Disposition', 'attachment', filename=file_path.name)
                msg.attach(attachment)
            
            # Send email
            print(f"Sending email to: {email_config['recipients']}")
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                print("Logging in to SMTP server...")
                server.login(email_config['sender_email'], email_config['sender_password'])
                print("Sending email...")
                server.send_message(msg)
                print("Email sent successfully!")
            
            # Send WhatsApp message if configured
            if self.twilio_client and email_config.get('whatsapp_recipients'):
                print("Sending WhatsApp notifications...")
                # Format WhatsApp message with custom message, report details, and link
                whatsapp_body = f"""📊 *Scheduled Report: {dataset_name}*

{message_body}

*Report Details:*
- Dataset: {dataset_name}
- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Format: {email_config.get('format', 'PDF')}

🔗 *View and Download Report:*
{share_link}
_(Link expires in 24 hours)_"""
                
                for recipient in email_config['whatsapp_recipients']:
                    print(f"Sending WhatsApp message to: {recipient}")
                    if self.send_whatsapp_message(recipient, whatsapp_body):
                        print(f"WhatsApp notification sent to {recipient}")
                    else:
                        print(f"WhatsApp notification failed for {recipient}. Please check if the number is opted in.")
            
            print(f"Report sent successfully for dataset: {dataset_name}")
            
        except Exception as e:
            print(f"Failed to send report: {str(e)}")
            print(f"Error type: {type(e)}")
            print(f"Error details: {e.__dict__ if hasattr(e, '__dict__') else 'No details'}")
            if hasattr(e, 'args'):
                print(f"Error arguments: {e.args}")
    
    def remove_schedule(self, job_id: str) -> bool:
        """Remove a scheduled report"""
        try:
            # Remove from scheduler if job exists
            try:
                if self.scheduler.get_job(job_id):
                    self.scheduler.remove_job(job_id)
            except Exception as scheduler_error:
                print(f"Warning: Job not found in scheduler: {str(scheduler_error)}")
            
            # Remove from saved schedules
            schedules = self.load_schedules()
            if job_id in schedules:
                del schedules[job_id]
                self.save_schedules(schedules)
                print(f"Successfully removed schedule: {job_id}")
                return True
            else:
                print(f"Schedule {job_id} not found in saved schedules")
                return False
                
        except Exception as e:
            print(f"Failed to remove schedule {job_id}: {str(e)}")
            return False
    
    def load_schedules(self) -> dict:
        """Load saved schedules"""
        try:
            schedules = {}
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                try:
                    # Get column names from the table
                    cursor.execute("PRAGMA table_info(schedules)")
                    columns = [col[1] for col in cursor.fetchall()]
                    print(f"Available columns: {columns}")
                    
                    cursor.execute("""
                        SELECT id, dataset_name, schedule_type, schedule_config, 
                               email_config, format_config, created_at, last_run, 
                               next_run, status 
                        FROM schedules 
                        WHERE status = 'active'
                    """)
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        schedule_id = row[0]  # id column
                        schedules[schedule_id] = {
                            'dataset_name': row[1],
                            'schedule_type': row[2],
                            'schedule_config': json.loads(row[3]),
                            'email_config': json.loads(row[4]),
                            'format_config': json.loads(row[5]) if row[5] else None,
                            'created_at': row[6],
                            'last_run': row[7],
                            'next_run': row[8],
                            'status': row[9]
                        }
                    
                except sqlite3.OperationalError as e:
                    if "no such table" in str(e) or "no such column" in str(e):
                        print("Database schema issue detected, reinitializing database...")
                        self._init_database()
                        return {}
                    else:
                        raise
                
            print(f"Loaded {len(schedules)} schedules from database")
            return schedules
            
        except Exception as e:
            print(f"Error loading schedules: {str(e)}")
            return {}
    
    def save_schedules(self, schedules: dict):
        """Save schedules to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Begin transaction
                cursor.execute("BEGIN TRANSACTION")
                
                try:
                    for schedule_id, schedule in schedules.items():
                        # Convert schedule data to JSON strings
                        schedule_config = json.dumps(schedule['schedule_config'])
                        email_config = json.dumps(schedule['email_config'])
                        format_config = json.dumps(schedule.get('format_config')) if schedule.get('format_config') else None
                        
                        # Insert or update schedule
                        cursor.execute("""
                        INSERT OR REPLACE INTO schedules (
                            id, dataset_name, schedule_type, schedule_config, 
                            email_config, format_config, created_at, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            schedule_id,
                            schedule['dataset_name'],
                            schedule['schedule_config']['type'],
                            schedule_config,
                            email_config,
                            format_config,
                            schedule.get('created_at', datetime.now().isoformat()),
                            'active'
                        ))
                    
                    # Commit transaction
                    conn.commit()
                    print(f"Saved {len(schedules)} schedules to database")
                    
                except Exception as e:
                    # Rollback on error
                    conn.rollback()
                    print(f"Error saving schedules, rolling back: {str(e)}")
                    raise
                    
        except Exception as e:
            print(f"Error saving schedules to database: {str(e)}")
            raise
    
    def load_saved_schedules(self):
        """Load saved schedules into scheduler"""
        try:
            schedules = self.load_schedules()
            print(f"Loading {len(schedules)} saved schedules...")
            
            for job_id, schedule in schedules.items():
                try:
                    # Skip if job already exists
                    if self.scheduler.get_job(job_id):
                        print(f"Job {job_id} already exists in scheduler")
                        continue
                        
                    # Deserialize format config if it exists
                    format_config = self._deserialize_format_config(schedule.get('format_config'))
                    
                    # Get schedule configuration
                    schedule_config = schedule['schedule_config']
                    
                    # Create the job based on schedule type
                    if schedule_config['type'] == 'one-time':
                        # Parse date and time
                        schedule_date = datetime.strptime(
                            f"{schedule_config['date']} {schedule_config['hour']:02d}:{schedule_config['minute']:02d}:00",
                            "%Y-%m-%d %H:%M:%S"
                        )
                        
                        # Only add if the schedule date is in the future
                        if schedule_date > datetime.now():
                            self.scheduler.add_job(
                                func=self.send_report,
                                trigger='date',
                                run_date=schedule_date,
                                args=[schedule['dataset_name'], schedule['email_config'], format_config],
                                id=job_id,
                                name=f"Report_{schedule['dataset_name']}"
                            )
                            print(f"Loaded one-time schedule for {schedule_date}")
                    
                    elif schedule_config['type'] in ['daily', 'weekly', 'monthly']:
                        trigger_args = {
                            'hour': schedule_config['hour'],
                            'minute': schedule_config['minute']
                        }
                        
                        if schedule_config['type'] == 'weekly':
                            trigger_args['day_of_week'] = schedule_config['day']
                        elif schedule_config['type'] == 'monthly':
                            trigger_args['day'] = schedule_config['day']
                        
                        self.scheduler.add_job(
                            func=self.send_report,
                            trigger='cron',
                            args=[schedule['dataset_name'], schedule['email_config'], format_config],
                            id=job_id,
                            name=f"Report_{schedule['dataset_name']}",
                            **trigger_args
                        )
                        print(f"Loaded {schedule_config['type']} schedule")
                    
                except Exception as e:
                    print(f"Failed to load schedule {job_id}: {str(e)}")
                    continue
                    
            print("Finished loading saved schedules")
            
        except Exception as e:
            print(f"Error loading saved schedules: {str(e)}")
            
    def get_active_schedules(self) -> dict:
        """Get all active schedules"""
        try:
            schedules = self.load_schedules()
            active_schedules = {}
            
            for job_id, schedule in schedules.items():
                # Get the job from the scheduler
                job = self.scheduler.get_job(job_id)
                if job:
                    # Add next run time to schedule info
                    schedule['next_run'] = job.next_run_time.isoformat() if job.next_run_time else None
                    active_schedules[job_id] = schedule
                    
            print(f"Found {len(active_schedules)} active schedules")
            return active_schedules
            
        except Exception as e:
            print(f"Error getting active schedules: {str(e)}")
            return {}

    def cleanup_expired_reports(self):
        """Clean up expired report links and files"""
        try:
            current_time = datetime.now()
            
            for metadata_file in self.public_reports_dir.glob('*.json'):
                try:
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    
                    expires_at = datetime.fromisoformat(metadata['expires_at'])
                    if current_time > expires_at:
                        # Remove the report file
                        report_path = self.public_reports_dir / metadata_file.stem
                        if report_path.exists():
                            report_path.unlink()
                        
                        # Remove the metadata file
                        metadata_file.unlink()
                except Exception as e:
                    print(f"Error cleaning up report {metadata_file}: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"Error in cleanup process: {str(e)}") 