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

class ReportManager:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.load_schedules()
    
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

    def schedule_report(self, dataset_name, email_config, schedule_config):
        """Schedule a report for recurring delivery"""
        # Validate email configuration
        required_email_fields = ['smtp_server', 'smtp_port', 'sender_email', 
                               'sender_password', 'recipients', 'format']
        if not all(field in email_config for field in required_email_fields):
            raise ValueError("Missing required email configuration fields")
        
        if not email_config['recipients']:
            raise ValueError("No recipients specified")
        
        # Validate schedule configuration
        if 'type' not in schedule_config:
            raise ValueError("Schedule type not specified")
        
        if schedule_config['type'] not in ['daily', 'weekly', 'monthly']:
            raise ValueError("Invalid schedule type")
        
        job_id = f"report_{dataset_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Create trigger based on schedule type
        try:
            if schedule_config['type'] == 'daily':
                trigger = CronTrigger(
                    hour=schedule_config.get('hour', 0),
                    minute=schedule_config.get('minute', 0)
                )
            elif schedule_config['type'] == 'weekly':
                trigger = CronTrigger(
                    day_of_week=schedule_config.get('day', 0),
                    hour=schedule_config.get('hour', 0),
                    minute=schedule_config.get('minute', 0)
                )
            elif schedule_config['type'] == 'monthly':
                trigger = CronTrigger(
                    day=schedule_config.get('day', 1),
                    hour=schedule_config.get('hour', 0),
                    minute=schedule_config.get('minute', 0)
                )
            
            # Add job to scheduler
            self.scheduler.add_job(
                self.send_scheduled_report,
                trigger=trigger,
                args=[dataset_name, email_config],
                id=job_id,
                replace_existing=True
            )
            
            # Save schedule configuration
            self.save_schedule(job_id, dataset_name, email_config, schedule_config)
            
            return job_id
            
        except Exception as e:
            print(f"Failed to schedule report: {str(e)}")
            raise

    def send_scheduled_report(self, dataset_name, email_config):
        """Send scheduled report"""
        try:
            # Load dataset
            with sqlite3.connect("data/tableau_data.db") as conn:
                df = pd.read_sql(f"SELECT * FROM '{dataset_name}'", conn)
            
            # Generate report
            if email_config['format'] == 'PDF':
                buffer = self.generate_pdf(df, f"Report: {dataset_name}")
                report_data = buffer.getvalue()
                mime_type = 'application/pdf'
                file_ext = 'pdf'
            else:
                buffer = io.StringIO()
                df.to_csv(buffer, index=False)
                report_data = buffer.getvalue()
                mime_type = 'text/csv'
                file_ext = 'csv'
            
            # Send email
            for recipient in email_config['recipients']:
                msg = MIMEMultipart()
                msg['Subject'] = f'Scheduled Report: {dataset_name}'
                msg['From'] = email_config['sender_email']
                msg['To'] = recipient
                
                # Add body
                body = f"""
                Automated Report: {dataset_name}
                Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                
                Please find the attached report.
                """
                msg.attach(MIMEText(body, 'plain'))
                
                # Add attachment
                attachment = MIMEApplication(report_data)
                attachment['Content-Disposition'] = f'attachment; filename="{dataset_name}_{datetime.now().strftime("%Y%m%d")}_{email_config["format"].lower()}"'
                msg.attach(attachment)
                
                # Send email
                with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                    server.starttls()
                    server.login(email_config['sender_email'], email_config['sender_password'])
                    server.send_message(msg)
            
            print(f"Successfully sent scheduled report for {dataset_name}")
            
        except Exception as e:
            print(f"Failed to send scheduled report: {str(e)}")
            # Log error or handle it appropriately

    def save_schedule(self, job_id, dataset_name, email_config, schedule_config):
        """Save schedule configuration to file"""
        schedule_data = {
            'job_id': job_id,
            'dataset_name': dataset_name,
            'email_config': email_config,
            'schedule_config': schedule_config
        }
        
        schedules = self.load_schedules()
        schedules[job_id] = schedule_data
        
        with open('schedules.json', 'w') as f:
            json.dump(schedules, f)

    def load_schedules(self):
        """Load saved schedules"""
        try:
            with open('schedules.json', 'r') as f:
                return json.load(f)
        except:
            return {}

    def get_active_schedules(self):
        """Get list of active schedules"""
        return self.load_schedules()

    def remove_schedule(self, job_id):
        """Remove a scheduled report"""
        try:
            self.scheduler.remove_job(job_id)
            schedules = self.load_schedules()
            if job_id in schedules:
                del schedules[job_id]
                with open('schedules.json', 'w') as f:
                    json.dump(schedules, f)
            return True
        except:
            return False 