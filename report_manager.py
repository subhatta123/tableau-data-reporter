import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
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
        doc = SimpleDocTemplate(buffer, pagesize=letter)
        elements = []
        
        # Add title
        styles = getSampleStyleSheet()
        elements.append(Paragraph(title, styles['Title']))
        elements.append(Spacer(1, 20))
        
        # Add timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elements.append(Paragraph(f"Generated on: {timestamp}", styles['Normal']))
        elements.append(Spacer(1, 20))
        
        # Add summary statistics
        summary_data = [
            ["Total Rows", str(len(df))],
            ["Total Columns", str(len(df.columns))],
        ]
        
        # Add numerical column statistics
        num_cols = df.select_dtypes(include=['number']).columns
        if len(num_cols) > 0:
            elements.append(Paragraph("Numerical Statistics:", styles['Heading2']))
            for col in num_cols:
                stats = df[col].describe()
                summary_data.extend([
                    [f"{col} (Mean)", f"{stats['mean']:.2f}"],
                    [f"{col} (Min)", f"{stats['min']:.2f}"],
                    [f"{col} (Max)", f"{stats['max']:.2f}"]
                ])
        
        # Create summary table
        summary_table = Table(summary_data)
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(summary_table)
        elements.append(Spacer(1, 20))
        
        # Add data table
        data = [df.columns.tolist()] + df.values.tolist()
        table = Table(data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(table)
        
        # Build PDF
        doc.build(elements)
        buffer.seek(0)
        return buffer

    def schedule_report(self, dataset_name: str, email_config: dict, schedule_config: dict) -> str:
        """Schedule a new report"""
        try:
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
            
            # Add email body
            body = f"""
            Please find attached the scheduled report for {dataset_name}.
            Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach report file
            if email_config['format'].upper() == 'CSV':
                # Save DataFrame to CSV
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                attachment = MIMEText(csv_buffer.getvalue())
                attachment.add_header('Content-Disposition', 'attachment', filename=f"{dataset_name}.csv")
                msg.attach(attachment)
            else:
                # PDF format (placeholder for future implementation)
                pass
            
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
            # Remove from scheduler
            self.scheduler.remove_job(job_id)
            
            # Remove from saved schedules
            schedules = self.load_schedules()
            if job_id in schedules:
                del schedules[job_id]
                self.save_schedules(schedules)
            
            return True
        except Exception as e:
            print(f"Failed to remove schedule: {str(e)}")
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