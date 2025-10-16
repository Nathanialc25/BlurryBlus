'''
9/9
TODO 
-secrets are all over the place in here,  dont commit until thats resolved
-env variables need to be brought in for those logins
- understand the html
'''

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
from datetime import date
from airflow import Dataset
from utils.recommendation_weights import calculate_album_score, get_known_artists


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
VIEW_DATASET = Dataset("view://apple_music/v_weekly_new_releases")
BREVO_LOGIN = os.environ.get("BREVO_LOGIN")
BREVO_PASSWORD = os.environ.get("BREVO_PASSWORD")
TEST_MODE = True  

def get_active_subscribers():
    """Fetches all active subscribers with their preferences."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    if TEST_MODE:
        # Only get user with user_id = 1
        query = """
            SELECT user_id, first_name, email, genres, favorite_artist, album_length
            FROM user_preferences 
            WHERE is_active = TRUE AND user_id = 2
        """
        logging.info("TEST MODE: Only sending to user_id = 2")
    else:
        # Get all active subscribers
        query = """
            SELECT user_id, first_name, email, genres, favorite_artist, album_length
            FROM user_preferences 
            WHERE is_active = TRUE
        """
        logging.info("PRODUCTION MODE: Sending to all active subscribers")
    
    records = hook.get_records(query)
    
    # Convert to list of dictionaries
    subscribers = []
    for user_id, first_name, email, genres, favorite_artist, album_length in records:
        subscribers.append({
            'user_id': user_id,
            'first_name': first_name,
            'email': email,
            'genres': genres,
            'favorite_artist': favorite_artist,
            'album_length': album_length
        })
    
    return subscribers

# Need to find a more dynamic way to write blurbs, would be cool to hit openAI for this
def generate_album_blurb(artist: str, album: str) -> str:
    blurbs = [
        f"Fresh sounds from {artist} that you won't want to miss",
        f"{artist} returns with a compelling new collection",
        f"A standout release from {artist} worth checking out", 
        f"New music from {artist} that's making waves",
        f"{artist} delivers with this captivating new album",
        f"Don't miss this latest offering from {artist}",
        f"{artist} continues to innovate with this release",
        f"A must-listen addition from {artist}",
        f"This new album showcases {artist}'s evolving sound",
        f"{artist} brings the heat with this fresh collection"
    ]
    return random.choice(blurbs)

def fetch_this_weeks_albums():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    records = hook.get_records(f"""
        SELECT 
            artist, 
            album_name, 
            release_date, 
            cover_art_url, 
            genre,
            track_count, 
            COALESCE(editorial_notes, '') as notes,
            url
        FROM {SCHEMA}.{VIEW_NAME}
        ORDER BY release_date DESC
    """)
    
    # Convert to list of dictionaries with proper field names
    albums = []
    for record in records:
        albums.append({
            'artist': record[0],
            'album_name': record[1],
            'release_date': record[2],
            'cover_art_url': record[3],
            'genre': record[4],
            'track_count': record[5],  # ADD THIS
            'notes': record[6],
            'url': record[7]
        })
    
    return albums

def generate_email_content(**kwargs):
    run_date = kwargs.get('ds') or kwargs.get('logical_date') or date.today().strftime('%Y-%m-%d')
    logging.info(f"Generating personalized emails for run_date={run_date}")
    
    try:
        # Fetch all albums and subscribers
        all_albums = fetch_this_weeks_albums()
        subscribers = get_active_subscribers()
        
        # Fetch full known artist list once per run
        known_artists = get_known_artists()
        
        personalized_emails = {}
        
        for subscriber in subscribers:
            try:
                scored_albums = []
                for album in all_albums:
                    score = calculate_album_score(album, subscriber, known_artists=known_artists)
                    scored_albums.append({**album, 'score': score})
                
                scored_albums.sort(key=lambda x: x['score'], reverse=True)
                top_albums = scored_albums[:20]
                
                featured = top_albums[:3]
                others = top_albums[3:22]
                
                html = create_personalized_email_html(subscriber, featured, others, run_date)
                personalized_emails[subscriber['email']] = html
                logging.info(f"Generated recommendations for {subscriber['email']}")
            except Exception as e:
                logging.error(f"Failed for {subscriber['email']}: {e}")
                continue
        
        ti = kwargs['ti']
        ti.xcom_push(key='personalized_emails', value=personalized_emails)
        return personalized_emails
        
    except Exception as e:
        logging.error(f"Failed to generate emails: {e}")
        raise

def create_personalized_email_html(subscriber, featured, others, run_date):
    """Professional HTML email with personalization details and genre info"""
    
    featured_with_blurbs = []
    for album in featured:
        blurb = generate_album_blurb(artist=album['artist'], album=album['album_name'])
        
        # Determine what the subscriber has in common with this album
        common_elements = []
        
        # Check genre match
        if subscriber.get('genres') and album.get('genre'):
            subscriber_genres = subscriber['genres']
            if isinstance(subscriber_genres, str):
                subscriber_genres = [g.strip() for g in subscriber_genres.split(',')]
            if album['genre'] in subscriber_genres:
                common_elements.append(f"Genre: {album['genre']}")
        
        # Check artist match (if subscriber has favorite artists)
        if subscriber.get('favorite_artist') and album.get('artist'):
            if subscriber['favorite_artist'].lower() in album['artist'].lower():
                common_elements.append("Favorite Artist")
        
        # Check album length preference
        if subscriber.get('album_length') and album.get('track_count'):
            subscriber_pref = subscriber['album_length']
            track_count = album['track_count']
            
            if subscriber_pref == "short" and track_count <= 8:
                common_elements.append("Short Album")
            elif subscriber_pref == "medium" and 9 <= track_count <= 15:
                common_elements.append("Medium Length")
            elif subscriber_pref == "long" and track_count >= 16:
                common_elements.append("Long Album")
        
        featured_with_blurbs.append({
            **album, 
            'blurb': blurb,
            'common_elements': common_elements
        })

    # Split others into 4 rows of 4 albums each
    others_rows = [others[i:i+4] for i in range(0, min(len(others), 16), 4)]
    
    html = jinja2.Template("""
    <!DOCTYPE html>
    <html>
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            /* Reset for email clients */
            body, table, td, div, p { margin: 0; padding: 0; }
            body { font-family: Arial, sans-serif; background: #f8f9fa; color: #212529; }
            .container { max-width: 650px; margin: 0 auto; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
            
            /* Header */
            .header { text-align: center; padding: 30px 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
            .header h1 { margin: 0 0 10px 0; font-size: 32px; font-weight: 700; }
            .header p { margin: 5px 0; font-size: 16px; opacity: 0.9; }
            .subtitle { font-size: 18px; margin-top: 15px; }
            
            /* Content */
            .content { padding: 30px; }
            .section-title { font-size: 22px; font-weight: 600; margin: 0 0 20px 0; padding-bottom: 10px; border-bottom: 2px solid #e9ecef; color: #495057; }
            
            /* Featured section */
            .featured-section { margin-bottom: 30px; }
            .featured-table { width: 100%; border-spacing: 15px; border-collapse: separate; }
            .featured-cell { width: 33%; vertical-align: top; }
            .featured-album { background: #f8f9fa; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.04); }
            .featured-cover { width: 100%; height: auto; display: block; }
            .featured-details { padding: 15px; }
            .featured-name { font-weight: 700; font-size: 16px; margin: 0 0 5px 0; color: #212529; line-height: 1.3; }
            .featured-artist { color: #6c757d; font-size: 14px; margin: 0 0 10px 0; font-weight: 500; }
            .featured-blurb { font-size: 13px; color: #495057; font-style: italic; margin: 0; line-height: 1.4; }
            
            /* Album info section */
            .album-info-section { margin-top: 12px; padding-top: 12px; border-top: 1px solid #e9ecef; }
            .album-info-item { font-size: 12px; color: #6c757d; margin-bottom: 4px; }
            
            /* Personalization badges */
            .personalization-badges { margin-top: 10px; }
            .personalization-badge { 
                display: inline-block; 
                background: #e9ecef; 
                color: #495057; 
                padding: 4px 8px; 
                border-radius: 100px; 
                font-size: 11px; 
                margin-right: 6px; 
                margin-bottom: 6px; 
            }
            
            /* Recommendations section */
            .recommendations-section { background: #f8f9fa; border-radius: 12px; padding: 25px; margin-top: 30px; }
            .albums-table { width: 100%; border-spacing: 10px; border-collapse: separate; }
            .album-cell { width: 25%; vertical-align: top; }
            .album-card { background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.04); }
            .album-cover { width: 100%; height: auto; display: block; }
            .album-info { padding: 10px 8px; }
            .album-name { font-weight: 600; font-size: 13px; margin: 0 0 4px 0; line-height: 1.3; color: #212529; }
            .album-artist { color: #6c757d; font-size: 12px; margin: 0; }
            
            /* Footer */
            .footer { text-align: center; padding: 25px; background: #f1f3f5; color: #6c757d; font-size: 14px; }
            .footer p { margin: 5px 0; }
            
            /* Mobile styles */
            @media only screen and (max-width: 650px) {
                .container { border-radius: 0; box-shadow: none; }
                .featured-cell, .album-cell { display: block; width: 100% !important; margin-bottom: 15px; }
                .featured-table, .albums-table { border-spacing: 0 !important; }
                .content { padding: 20px !important; }
                .header { padding: 20px 15px !important; }
                .header h1 { font-size: 26px !important; }
                .recommendations-section { padding: 20px !important; }
                .section-title { font-size: 20px !important; }
            }
        </style>
    </head>
    <body>
        <center class="container">
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                    <td class="header">
                        <h1>Hi {{ first_name }}, Here's Your Weekly Music Discovery</h1>
                        <p>{{ date }}</p>
                        <p class="subtitle">Curated just for you based on your music taste</p>
                    </td>
                </tr>
                <tr>
                    <td class="content">
                        <!-- Featured albums -->
                        <div class="featured-section">
                            <h2 class="section-title">Featured Picks</h2>
                            <table class="featured-table" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    {% for album in featured %}
                                    <td class="featured-cell">
                                        <div class="featured-album">
                                            <a href="{{ album.url }}"><img src="{{ album.cover_art_url }}" class="featured-cover" alt="{{ album.album_name }}" width="100%"></a>
                                            <div class="featured-details">
                                                <h3 class="featured-name">{{ album.album_name }}</h3>
                                                <p class="featured-artist">{{ album.artist }}</p>
                                                <p class="featured-blurb">{{ album.blurb }}</p>
                                                
                                                <!-- Album info section with genre -->
                                                <div class="album-info-section">
                                                    <div class="album-info-item"><strong>Genre:</strong> {{ album.genre }}</div>
                                                    <div class="album-info-item"><strong>Tracks:</strong> {{ album.track_count }}</div>
                                                    {% if album.notes %}
                                                    <div class="album-info-item"><strong>Notes:</strong> {{ album.notes|truncate(60) }}</div>
                                                    {% endif %}
                                                </div>
                                                
                                                <!-- Personalization badges -->
                                                {% if album.common_elements %}
                                                <div class="personalization-badges">
                                                    {% for element in album.common_elements %}
                                                    <span class="personalization-badge">{{ element }}</span>
                                                    {% endfor %}
                                                </div>
                                                {% endif %}
                                            </div>
                                        </div>
                                    </td>
                                    {% endfor %}
                                </tr>
                            </table>
                        </div>
                        
                        <!-- Recommendations -->
                        <div class="recommendations-section">
                            <h2 class="section-title">More Recommendations For You</h2>
                            {% for row in others_rows %}
                            <table class="albums-table" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    {% for album in row %}
                                    <td class="album-cell">
                                        <div class="album-card">
                                            <a href="{{ album.url }}"><img src="{{ album.cover_art_url }}" class="album-cover" alt="{{ album.album_name }}" width="100%"></a>
                                            <div class="album-info">
                                                <h3 class="album-name">{{ album.album_name }}</h3>
                                                <p class="album-artist">{{ album.artist }}</p>
                                                <div class="album-info-item" style="font-size: 11px; color: #6c757d; margin-top: 4px;">
                                                    {{ album.genre }} • {{ album.track_count }} tracks
                                                </div>
                                            </div>
                                        </div>
                                    </td>
                                    {% endfor %}
                                </tr>
                            </table>
                            {% endfor %}
                        </div>
                    </td>
                </tr>
                <tr>
                    <td class="footer">
                        <p>Delivered by BlurryBlu • Brought to you by Nate C</p>
                        <p><small>Want to change your preferences? <a href="#" style="color: #6c757d;">Update your settings</a></small></p>
                    </td>
                </tr>
            </table>
        </center>
    </body>
    </html>
    """).render(
        first_name=subscriber.get('first_name', 'Music Lover'),
        featured=featured_with_blurbs,
        others_rows=others_rows,
        date=run_date
    )

    return html

def send_email_python(**kwargs):
    """Send personalized emails to all subscribers"""
    run_date = kwargs.get('ds') or kwargs.get('logical_date') or date.today().strftime('%Y-%m-%d')
    
    ti = kwargs['ti']
    personalized_emails = ti.xcom_pull(task_ids='generate_email_content', key='personalized_emails')
    
    smtp_host = 'smtp-relay.brevo.com'
    smtp_port = 587
    login = BREVO_LOGIN 
    password = BREVO_PASSWORD 
    from_email = 'nathanialc17@gmail.com'
    
    success_count = 0
    failure_count = 0
    
    for to_email, html_content in personalized_emails.items():
        subject = f'Your Personalized Music Recommendations - {run_date}'
        
        # Create the email message
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = to_email
        msg.attach(MIMEText(html_content, 'html'))
        
        # Send the email
        try:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(login, password)
                server.sendmail(from_email, to_email, msg.as_string())
            logging.info(f"Email sent successfully to {to_email}!")
            success_count += 1
        except Exception as e:
            logging.error(f"Failed to send email to {to_email}: {e}")
            failure_count += 1
    
    logging.info(f"Email sending complete. Success: {success_count}, Failures: {failure_count}")
    
    if failure_count > 0:
        raise Exception(f"Failed to send {failure_count} emails")

with DAG(
    'Personalized_email_generation',
    default_args=default_args,
    description='Weekly music newsletter with featured albums',
    schedule=[VIEW_DATASET],  
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
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    generate_email >> send_email