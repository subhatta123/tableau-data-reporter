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
from datetime import datetime
import sqlite3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from pathlib import Path
import uuid

class ReportManager:
    def __init__(self):
        """Initialize report manager"""
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.schedules_file = self.data_dir / "schedules.json"
        if not self.schedules_file.exists():
            self.save_schedules({})
        
        # Initialize scheduler
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.load_saved_schedules()
    
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

    def schedule_report(self, dataset_name: str, email_config: dict, schedule_config: dict) -> str:
        """Schedule a new report"""
        try:
            # Check for existing schedules for this dataset
            existing_schedules = self.get_active_schedules()
            for job_id, schedule in existing_schedules.items():
                if (schedule['dataset_name'] == dataset_name and 
                    schedule['schedule_config']['type'] == schedule_config['type']):
                    # For one-time schedules, check date and time
                    if schedule_config['type'] == 'one-time':
                        if (schedule['schedule_config']['date'] == schedule_config['date'] and
                            schedule['schedule_config']['hour'] == schedule_config['hour'] and
                            schedule['schedule_config']['minute'] == schedule_config['minute']):
                            raise ValueError("A schedule already exists for this dataset at the specified time")
                    else:
                        # For recurring schedules, check timing
                        if (schedule['schedule_config'].get('hour') == schedule_config.get('hour') and
                            schedule['schedule_config'].get('minute') == schedule_config.get('minute') and
                            schedule['schedule_config'].get('day') == schedule_config.get('day')):
                            raise ValueError("A schedule already exists for this dataset with the same configuration")
            
            job_id = str(uuid.uuid4())
            
            # Validate schedule configuration
            if 'type' not in schedule_config:
                raise ValueError("Schedule type not specified")
            
            # Create the job based on schedule type
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
            
        except Exception as e:
            print(f"Failed to schedule report: {str(e)}")
            return None
    
    def send_report(self, dataset_name: str, email_config: dict):
        """Send scheduled report"""
        try:
            # Load dataset
            with sqlite3.connect('data/tableau_data.db') as conn:
                df = pd.read_sql_query(f"SELECT * FROM '{dataset_name}'", conn)
            
            if df.empty:
                print(f"No data found in dataset: {dataset_name}")
                return
            
            # Create email
            msg = MIMEMultipart()
            msg['From'] = email_config['sender_email']
            msg['To'] = ', '.join(email_config['recipients'])
            msg['Subject'] = f"Scheduled Report: {dataset_name}"
            
            # Add email body with custom message if provided, fixing the formatting
            email_body = email_config.get('body', '').strip()
            body = f"{email_body}\n\nReport Details:\n- Dataset: {dataset_name}\n- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach report file
            if email_config['format'].upper() == 'CSV':
                # Save DataFrame to CSV
                csv_data = df.to_csv(index=False).encode('utf-8')
                attachment = MIMEApplication(csv_data, _subtype='csv')
                attachment.add_header('Content-Disposition', 'attachment', filename=f"{dataset_name}.csv")
                msg.attach(attachment)
            else:
                # Generate PDF report
                pdf_buffer = self.generate_pdf(df, f"Report: {dataset_name}")
                attachment = MIMEApplication(pdf_buffer.getvalue(), _subtype='pdf')
                attachment.add_header('Content-Disposition', 'attachment', filename=f"{dataset_name}.pdf")
                msg.attach(attachment)
            
            # Send email
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                server.login(email_config['sender_email'], email_config['sender_password'])
                server.send_message(msg)
            
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