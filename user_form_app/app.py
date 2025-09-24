from flask import Flask, render_template, request, redirect, url_for
import psycopg2
from psycopg2 import IntegrityError  # Add this import
import os
from datetime import date, timedelta
import json

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
    """Handles the form submission and inserts data into Postgres."""

    #gathering info from the from the person just filled out
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')
    email = request.form.get('email')
    selected_genres = request.form.getlist('genres') 
    favorite_artist = request.form.get('favorite_artist') 
    album_length = request.form.get('album_length')       

    # Basic validation 
    if not first_name or not last_name or not email or not selected_genres or not album_length: 
        error_msg = "Please fill out all required fields."
        return redirect(url_for('error', message=error_msg))

    # Connect to the database and insert the data
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()

        # SQL INSERT statement with first_name and last_name
        insert_query = """
            INSERT INTO user_preferences (first_name, last_name, email, genres, favorite_artist, album_length, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        genres_json = json.dumps(selected_genres)

        cursor.execute(insert_query, (first_name, last_name, email, genres_json, favorite_artist, album_length, True))
        conn.commit()  # Save the changes to the database

        #close the connection
        cursor.close()
        conn.close()

    except IntegrityError as e:
        # Handle database constraint violations
        error_msg = "This email address is already registered for this product. Please use a different email."
        return redirect(url_for('error', message=error_msg))
        
    except Exception as e:
        # Handle any other errors
        error_msg = "A system error occurred. Please try again later."
        return redirect(url_for('error', message=error_msg))

    return redirect(url_for('success'))

@app.route('/success')
def success():
    """A simple success page."""
    today = date.today()
    days_until_friday = (4 - today.weekday() + 7) % 7
    if days_until_friday == 0:
        days_until_friday = 7
    next_friday = today + timedelta(days=days_until_friday)

    # Format the date first, then use it in the f-string
    formatted_date = next_friday.strftime('%Y-%m-%d')
    return f"Thank you for signing up! You'll start receiving recommendations this upcoming Friday on {formatted_date}!"