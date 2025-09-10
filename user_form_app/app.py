from flask import Flask, render_template, request, redirect, url_for
import psycopg2
import os
from datetime import date, timedelta
import json

'''
9/8
TODO: fix the env variables
Good to commit

'''

#meaning everything to run this app is in this directory right here
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

#render the html template that we made, this will be on the root URL
@app.route('/')

#at this root page, run this function, use the html template, and supply the genres as a variable to be used in the template
def index():
    """Displays the signup form."""
    return render_template('index.html', genres=COMMON_GENRES)

# at the submit page, we are recieving the POST data parcel, processing, and putting it into db, 
# closing connection, and then redirecting to succes page

# this matches the HTML saying - I only accept POST deliveries
@app.route('/submit', methods=['POST'])
def submit_form():
    """Handles the form submission and inserts data into Postgres."""

    #gathering info from the from the person just filled out
    email = request.form.get('email')
    selected_genres = request.form.getlist('genres') 
    favorite_artist = request.form.get('favorite_artist') 
    album_length = request.form.get('album_length')       

    # Basic validation 
    if not email or not selected_genres or not album_length: 
        return "Error: Email, at least one genre, and album length are required.", 400

    # Connect to the database and insert the data
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()

        # SQL INSERT statement. `%s` is a placeholder for safe parameterization.
        # s for strings, f for floats, remember that
        insert_query = """
            INSERT INTO user_preferences (email, genres, favorite_artist, album_length, is_active)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        genres_json = json.dumps(selected_genres)

        cursor.execute(insert_query, (email, genres_json, favorite_artist, album_length, True))
        conn.commit()  # Save the changes to the database

        #close the connection
        cursor.close()
        conn.close()

    except Exception as e:
        # Handle any errors (e.g., database not available, duplicate email)
        return f"An error occurred: {str(e)}", 500

    # If successful,  send to the /success route thats defined below
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