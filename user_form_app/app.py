from flask import Flask, render_template, request, redirect, url_for
import psycopg2
from psycopg2 import IntegrityError  # Add this import
import os
from datetime import date, timedelta
import json
from utils.related_artist import process_user_artists

'''
9/8
TODO: fix the env variables
Good to commit

'''

app = Flask(__name__)

# env variables arent working, its just coalescing to second value- which is defined in the docker compose yaml
DB_HOST = os.getenv('DB_HOST', 'localhost')  
DB_NAME = os.getenv('DB_NAME', 'your_database_name')
DB_USER = os.getenv('DB_USER', 'your_username')
DB_PASS = os.getenv('DB_PASS', 'your_password')

#Genres found in the music so far, used in dynamic list in HTML
COMMON_GENRES = [
    'Alternative',
    'Pop',
    'Rap',
    'Country',
    'R&B/Soul',
    'Rock',
    'Dance',
    'Folk',
    'K-Pop',
    'Soundtrack',
    'Christian',
    'Electronic',
    'Hip-Hop/Rap',
    'House',
    'Indie Pop',
    'Indie Rock',
    'Latin',
    'Metal',
    'Singer/Songwriter',
    'TV Soundtrack',
    'Urbano latino'
]

@app.route('/')
def index():
    """Displays the signup form."""
    return render_template('index.html', genres=COMMON_GENRES)

# Add an error route to display the error page
@app.route('/error')
def error():
    """Displays the error page."""
    error_message = request.args.get('message', 'An unexpected error occurred.')
    return render_template('error.html', error_message=error_message)

@app.route('/submit', methods=['POST'])
def submit_form():
    # Get form data
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')
    email = request.form.get('email')
    selected_genres = request.form.getlist('genres') 
    favorite_artist_input = request.form.get('favorite_artist')  # ← Renamed for clarity
    album_length = request.form.get('album_length')       

    # Process the artists
    favorite_artists, related_artists = process_user_artists(favorite_artist_input)

    # Basic validation 
    if not first_name or not last_name or not email or not selected_genres or not album_length: 
        error_msg = "Please fill out all required fields."
        return redirect(url_for('error', message=error_msg))

    # Connect to the database and insert the data
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()

        # Updated SQL INSERT statement to include related_artists
        insert_query = """
            INSERT INTO user_preferences (first_name, last_name, email, genres, favorite_artist, related_artists, album_length, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        genres_json = json.dumps(selected_genres)
        favorite_artists_str = ','.join(favorite_artists) 
        related_artists_str = ','.join(related_artists) 

        cursor.execute(insert_query, (first_name, last_name, email, genres_json, favorite_artists_str, related_artists_str, album_length, True))
        conn.commit()

        cursor.close()
        conn.close()

    except IntegrityError as e:
        error_msg = "This email address is already registered for this product. Please use a different email."
        return redirect(url_for('error', message=error_msg))
        
    except Exception as e:
        error_msg = "A system error occurred. Please try again later."
        return redirect(url_for('error', message=error_msg))

    return redirect(url_for('success'))


@app.route('/success')
def success():
    """Success page."""
    today = date.today()
    days_until_friday = (4 - today.weekday() + 7) % 7
    if days_until_friday == 0:
        days_until_friday = 7
    next_friday = today + timedelta(days=days_until_friday)
    formatted_date = next_friday.strftime('%Y-%m-%d')
    
    return render_template('success.html', formatted_date=formatted_date)