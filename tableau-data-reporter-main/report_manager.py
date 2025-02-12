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
        
        # Base URL for report access (update this with your domain)
        self.base_url = os.getenv('BASE_URL', 'http://localhost:8501')
        
        if all([self.twilio_account_sid, self.twilio_auth_token, self.twilio_whatsapp_number]):
            try:
                self.twilio_client = Client(self.twilio_account_sid, self.twilio_auth_token)
                print("Twilio client initialized successfully")
            except Exception as e:
                print(f"Failed to initialize Twilio client: {str(e)}")
        else:
            print("Twilio configuration incomplete. Please check your .env file")
    
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
            # Generate a unique token
            token = str(uuid.uuid4())
            
            # Create a secure hash of the token
            hash_obj = hashlib.sha256(token.encode())
            secure_hash = hash_obj.hexdigest()
            
            # Create a symbolic link with the secure hash
            public_path = self.public_reports_dir / f"{secure_hash}{report_path.suffix}"
            if public_path.exists():
                public_path.unlink()
            
            # Copy the file to public directory
            shutil.copy2(report_path, public_path)
            
            # Store metadata about the link
            metadata = {
                'original_path': str(report_path),
                'created_at': datetime.now().isoformat(),
                'expires_at': (datetime.now() + timedelta(hours=expiry_hours)).isoformat()
            }
            
            # Save metadata
            metadata_path = self.public_reports_dir / f"{secure_hash}.json"
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)
            
            # Generate the full URL
            return f"{self.base_url}/reports/{secure_hash}{report_path.suffix}"
            
        except Exception as e:
            print(f"Failed to generate report link: {str(e)}")
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

    def schedule_report(self, dataset_name: str, email_config: dict, schedule_config: dict) -> str:
        """Schedule a new report"""
        try:
            # Input validation
            if not dataset_name or not email_config or not schedule_config:
                raise ValueError("Missing required parameters for scheduling")
            
            # Validate schedule configuration
            if 'type' not in schedule_config:
                raise ValueError("Schedule type not specified")
            
            # Validate email configuration
            if not email_config.get('recipients'):
                raise ValueError("No email recipients specified")

            # Check for existing schedules for this dataset
            existing_schedules = self.get_active_schedules()
            for existing_job_id, schedule in existing_schedules.items():
                if (schedule['dataset_name'] == dataset_name and 
                    schedule['schedule_config']['type'] == schedule_config['type']):
                    # For one-time schedules, check date and time
                    if schedule_config['type'] == 'one-time':
                        if (schedule['schedule_config']['date'] == schedule_config['date'] and
                            schedule['schedule_config']['hour'] == schedule_config['hour'] and
                            schedule['schedule_config']['minute'] == schedule_config['minute']):
                            # Instead of raising an error, return None with a message
                            print("A schedule already exists for this dataset at the specified time")
                            return None
                    else:
                        # For recurring schedules, check timing
                        if (schedule['schedule_config'].get('hour') == schedule_config.get('hour') and
                            schedule['schedule_config'].get('minute') == schedule_config.get('minute') and
                            schedule['schedule_config'].get('day') == schedule_config.get('day')):
                            # Instead of raising an error, return None with a message
                            print("A schedule already exists for this dataset with the same configuration")
                            return None

            # Generate job_id
            job_id = str(uuid.uuid4())
            
            # Create the job based on schedule type
            try:
                if schedule_config['type'] == 'one-time':
                    if 'date' not in schedule_config:
                        raise ValueError("Date not specified for one-time schedule")
                    
                    # Parse date and time
                    schedule_date = datetime.strptime(
                        f"{schedule_config['date']} {schedule_config['hour']:02d}:{schedule_config['minute']:02d}:00",
                        "%Y-%m-%d %H:%M:%S"
                    )
                    
                    # Add job to scheduler
                    self.scheduler.add_job(
                        func=self.send_report,
                        trigger='date',
                        run_date=schedule_date,
                        args=[dataset_name, email_config],
                        id=job_id,
                        name=f"Report_{dataset_name}"
                    )
                
                elif schedule_config['type'] == 'daily':
                    self.scheduler.add_job(
                        func=self.send_report,
                        trigger='cron',
                        hour=schedule_config['hour'],
                        minute=schedule_config['minute'],
                        args=[dataset_name, email_config],
                        id=job_id,
                        name=f"Report_{dataset_name}"
                    )
                
                elif schedule_config['type'] == 'weekly':
                    self.scheduler.add_job(
                        func=self.send_report,
                        trigger='cron',
                        day_of_week=schedule_config['day'],
                        hour=schedule_config['hour'],
                        minute=schedule_config['minute'],
                        args=[dataset_name, email_config],
                        id=job_id,
                        name=f"Report_{dataset_name}"
                    )
                
                elif schedule_config['type'] == 'monthly':
                    self.scheduler.add_job(
                        func=self.send_report,
                        trigger='cron',
                        day=schedule_config['day'],
                        hour=schedule_config['hour'],
                        minute=schedule_config['minute'],
                        args=[dataset_name, email_config],
                        id=job_id,
                        name=f"Report_{dataset_name}"
                    )
                
                else:
                    raise ValueError(f"Invalid schedule type: {schedule_config['type']}")
                
                # Save schedule to file
                schedules = self.load_schedules()
                schedules[job_id] = {
                    'dataset_name': dataset_name,
                    'email_config': email_config,
                    'schedule_config': schedule_config,
                    'created_at': datetime.now().isoformat()
                }
                self.save_schedules(schedules)
                
                print(f"Successfully scheduled report with ID: {job_id}")
                return job_id
                
            except Exception as scheduler_error:
                print(f"Failed to create schedule: {str(scheduler_error)}")
                # Clean up if job was partially created
                if self.scheduler.get_job(job_id):
                    self.scheduler.remove_job(job_id)
                return None
                
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
                print("Twilio client not initialized")
                return False

            # Clean up the phone numbers
            from_number = self.twilio_whatsapp_number.strip()
            to_number = to_number.strip()
            
            # Add whatsapp: prefix if not present
            if not from_number.startswith('whatsapp:'):
                from_number = f'whatsapp:{from_number}'
            if not to_number.startswith('whatsapp:'):
                to_number = f'whatsapp:{to_number}'
            
            # Add sandbox join message for first-time users
            sandbox_message = """
            *Welcome to our WhatsApp notification service!*
            
            To receive notifications, please join our sandbox by sending this message to your Twilio WhatsApp number:
            
            join <your-sandbox-code>
            
            You can find your sandbox code in your Twilio Console.
            """
            
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
                if "is not currently opted in" in str(e):
                    # If user is not opted in, send sandbox join instructions
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

    def send_report(self, dataset_name: str, email_config: dict):
        """Send scheduled report"""
        try:
            # Load dataset
            with sqlite3.connect('data/tableau_data.db') as conn:
                df = pd.read_sql_query(f"SELECT * FROM '{dataset_name}'", conn)
            
            if df.empty:
                print(f"No data found in dataset: {dataset_name}")
                return
            
            # Get the message body or use default
            message_body = email_config.get('body', '').strip()
            if not message_body:
                message_body = f"Please find attached the scheduled report for dataset: {dataset_name}"
            
            # Save report and get shareable link
            file_path, share_link = self.save_report(df, dataset_name, email_config['format'])
            if not file_path or not share_link:
                raise Exception("Failed to generate report file or link")
            
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
- Format: {email_config['format']}

View and download your report here: {share_link}
(Link expires in 24 hours)

This is an automated report. Please do not reply to this email."""

            msg.attach(MIMEText(email_body, 'plain'))
            
            # Attach report file
            with open(file_path, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype=file_path.suffix[1:])
                attachment.add_header('Content-Disposition', 'attachment', filename=file_path.name)
                msg.attach(attachment)
            
            # Send email
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                server.login(email_config['sender_email'], email_config['sender_password'])
                server.send_message(msg)
            
            # Send WhatsApp message if configured
            if self.twilio_client and email_config.get('whatsapp_recipients'):
                # Format WhatsApp message with custom message, report details, and link
                whatsapp_body = f"""📊 *Scheduled Report: {dataset_name}*

{message_body}

*Report Details:*
- Dataset: {dataset_name}
- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Format: {email_config['format']}

🔗 *View and Download Report:*
{share_link}
_(Link expires in 24 hours)_"""
                
                for recipient in email_config['whatsapp_recipients']:
                    if self.send_whatsapp_message(recipient, whatsapp_body):
                        print(f"WhatsApp notification sent to {recipient}")
                    else:
                        print(f"WhatsApp notification failed for {recipient}. Please check if the number is opted in.")
            
            print(f"Report sent successfully for dataset: {dataset_name}")
            
        except Exception as e:
            print(f"Failed to send report: {str(e)}")
    
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
            with open(self.schedules_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    
    def save_schedules(self, schedules: dict):
        """Save schedules to file"""
        with open(self.schedules_file, 'w') as f:
            json.dump(schedules, f)
    
    def load_saved_schedules(self):
        """Load saved schedules into scheduler"""
        schedules = self.load_schedules()
        for job_id, schedule in schedules.items():
            try:
                self.schedule_report(
                    schedule['dataset_name'],
                    schedule['email_config'],
                    schedule['schedule_config']
                )
            except Exception as e:
                print(f"Failed to load schedule {job_id}: {str(e)}")
    
    def get_active_schedules(self) -> dict:
        """Get all active schedules"""
        return self.load_schedules()

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