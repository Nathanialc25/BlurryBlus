from datetime import datetime, timedelta
import random
import logging
import jinja2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Constants
TABLE_NAME = "apple_music_album_releases"
VIEW_NAME = "v_weekly_new_releases"
SCHEMA = 'public'
BREVO_LOGIN = os.environ.get("BREVO_LOGIN") 
BREVO_PASSWORD = os.environ.get("BREVO_PASSWORD")

def generate_album_blurb(artist: str, album: str) -> str:
    # Temporary: Remove OpenAI until you set it up properly
    # Replace with simple blurbs or implement your OpenAI connection later
    blurbs = [
        f"Fresh sounds from {artist} that you won't want to miss",
        f"{artist} returns with a compelling new collection",
        f"A standout release from {artist} worth checking out",
        f"New music from {artist} that's making waves"
    ]
    return random.choice(blurbs)

def fetch_this_weeks_albums():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    return hook.get_records(f"""
        SELECT 
            artist, 
            album_name, 
            release_date, 
            cover_art_url, 
            genre,
            COALESCE(editorial_notes, '') as notes,
            url
        FROM {SCHEMA}.{VIEW_NAME}
        ORDER BY release_date DESC
    """)

def generate_email_content(**kwargs):
    """Create the HTML email content"""
    ti = kwargs['ti']
    
    try:
        all_albums = fetch_this_weeks_albums()
        
        featured = random.sample(all_albums, min(3, len(all_albums))) if all_albums else []
        others = [a for a in all_albums if a not in featured]
        
        featured_with_blurbs = []
        for album in featured:
            blurb = generate_album_blurb(artist=album[0], album=album[1])
            featured_with_blurbs.append((*album, blurb))
        
        html = jinja2.Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; color: #333; }
                .header { text-align: center; padding: 20px 0; border-bottom: 1px solid #eee; }
                .featured-section { margin: 30px 0; }
                .featured-title { font-size: 18px; margin-bottom: 15px; }
                .featured-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; }
                .featured-album { text-align: center; }
                .featured-cover { width: 180px; height: 180px; object-fit: cover; border-radius: 8px; }
                .featured-name { font-weight: bold; margin: 8px 0 0; }
                .featured-artist { color: #666; font-size: 14px; }
                .featured-date { color: #888; font-size: 12px; margin: 3px 0; }
                .featured-blurb { font-style: italic; font-size: 13px; margin: 8px 0; }
                .other-section { margin-top: 40px; }
                .other-title { font-size: 16px; margin-bottom: 15px; }
                .other-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(120px, 1fr)); gap: 15px; }
                .other-album { text-align: center; }
                .other-cover { width: 100px; height: 100px; object-fit: cover; border-radius: 4px; }
                .other-name { font-size: 13px; margin: 5px 0 0; }
                .other-artist { color: #666; font-size: 12px; }
                .other-date { color: #888; font-size: 11px; margin: 2px 0; }
                .other-notes { font-size: 11px; color: #666; margin-top: 3px; }
                .footer { margin-top: 30px; text-align: center; color: #999; font-size: 12px; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>New Music Friday</h1>
                <p>{{ date }}</p>
            </div>
            
            {% if featured %}
            <div class="featured-section">
                <div class="featured-title">Featured Releases</div>
                <div class="featured-grid">
                    {% for album in featured %}
                    <div class="featured-album">
                        <img src="{{ album[3] }}" class="featured-cover" alt="{{ album[1] }}">
                        <div class="featured-name">{{ album[1] }}</div>
                        <div class="featured-artist">{{ album[0] }}</div>
                        <div class="featured-date">{{ album[2] }}</div>
                        <div class="featured-blurb">{{ album[6] }}</div>
                    </div>
                    {% endfor %}
                </div>
            </div>
            {% endif %}
            
            {% if others %}
            <div class="other-section">
                <div class="other-title">Also Released This Week</div>
                <div class="other-grid">
                    {% for album in others %}
                    <div class="other-album">
                        <img src="{{ album[3] }}" class="other-cover" alt="{{ album[1] }}">
                        <div class="other-name">{{ album[1] }}</div>
                        <div class="other-artist">{{ album[0] }}</div>
                        <div class="other-date">{{ album[2] }}</div>
                        {% if album[4] %}
                        <div class="other-notes">{{ album[4]|truncate(60) }}</div>
                        {% endif %}
                    </div>
                    {% endfor %}
                </div>
            </div>
            {% endif %}
            
            <div class="footer">
                <p>Delivered by your music discovery pipeline, brought to you by Nate C</p>
            </div>
        </body>
        </html>
        """).render(
            featured=featured_with_blurbs,
            others=others,
            date=kwargs['ds']
        )
        
        ti.xcom_push(key='email_html', value=html)
        return html
        
    except Exception as e:
        logging.error(f"Failed to generate email: {str(e)}")
        raise

def send_email_python(**kwargs):
    """Send email using pure Python smtplib (the proven method)"""
    ti = kwargs['ti']
    html_content = ti.xcom_pull(task_ids='generate_email_content', key='email_html')
    
    # Your Brevo SMTP credentials
    smtp_host = 'smtp-relay.brevo.com'
    smtp_port = 587
    login = BREVO_LOGIN 
    password = BREVO_PASSWORD 
    
    from_email = 'nathanialc17@gmail.com'
    to_email = 'jovgarcia49@gmail.com'
    subject = f'New Music Friday - {kwargs["ds"]}'
    
    # Create the email message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    msg.attach(MIMEText(html_content, 'html'))
    
    # Send the email using the proven method
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()  # Upgrade to secure connection
            server.login(login, password)
            server.sendmail(from_email, to_email, msg.as_string())
        logging.info("Email sent successfully via Python smtplib!")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        raise

with DAG(
    'Music_newsletter_dag',
    default_args=default_args,
    description='Weekly music newsletter with featured albums',
    schedule='0 12 * * 5',  # Fridays at 12pm
    catchup=False,
    tags=['music'],
) as dag:

    generate_email = PythonOperator(
        task_id='generate_email_content',
        python_callable=generate_email_content,
    )

    send_email = PythonOperator(
        task_id='send_weekly_newsletter',
        python_callable=send_email_python,
    )

    generate_email >> send_email