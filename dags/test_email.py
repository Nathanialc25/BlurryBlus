from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_brevo_email():
    # Your Brevo credentials - HARDCODED for now to bypass connection issues
    smtp_host = 'smtp-relay.brevo.com'
    smtp_port = 587
    login = '96663f001@smtp-brevo.com'
    password = 'WS8jLYZsnA71Mc9D'  # Your actual Brevo password
    
    # Email content
    from_email = 'nathanialc17@gmail.com'
    to_email = 'jovgarcia49@gmail.com'
    subject = 'Test from Airflow via Python'
    html_content = '<h1>This is a test from Airflow via Python!</h1><p>It bypasses the connection system.</p>'
    
    # Create message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    msg.attach(MIMEText(html_content, 'html'))
    
    # Send email - using the proven working code
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()  # Upgrade connection to TLS
            server.login(login, password)
            server.sendmail(from_email, to_email, msg.as_string())
        print("Email sent successfully!")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 5),
}

with DAG(
    'test_brevo_email',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    send_email = PythonOperator(
        task_id='send_test_email',
        python_callable=send_brevo_email
    )