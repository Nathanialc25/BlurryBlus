from datetime import datetime, timedelta
import requests
import logging
import hashlib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import Dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

JWT_DATASET = Dataset("dataset://apple/jwt")

# VIEW_DATASET = Dataset("postgresql://mydb/public/v_weekly_new_releases")
VIEW_DATASET = Dataset("view://apple_music/v_weekly_new_releases")

STORE_FRONT = 'US'
PLAYLIST_ID = "pl.2b0e6e332fdf4b7a91164da3162127b5"  # New Music Daily
TABLE_NAME = "apple_music_album_releases"
VIEW_NAME = "v_weekly_new_releases"
JWT_PATH = '/opt/airflow/secrets/apple_jwt.txt'
SCHEMA = 'public'

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    complex_id VARCHAR(100) PRIMARY KEY,
    album_name TEXT NOT NULL,
    artist TEXT NOT NULL,
    release_date DATE NOT NULL,
    track_count INTEGER,
    genre TEXT ,
    url TEXT UNIQUE NOT NULL,
    editorial_notes TEXT,
    cover_art_url TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_album UNIQUE (complex_id)
);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    complex_id, 
    album_name,
    artist, 
    release_date, 
    track_count, 
    genre, 
    url, 
    editorial_notes, 
    cover_art_url
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (complex_id) DO NOTHING;
"""

VIEW_SQL = f"""
CREATE OR REPLACE VIEW {VIEW_NAME} AS
SELECT * 
FROM {SCHEMA}.{TABLE_NAME}
WHERE 
    DATE(release_date) BETWEEN (CURRENT_DATE - INTERVAL '7 days') AND CURRENT_DATE
    AND 
    DATE(load_date) BETWEEN (CURRENT_DATE - INTERVAL '7 days') AND CURRENT_DATE;
"""

def generate_album_id(album_data: dict) -> str:
    unique_string = f"{album_data.get('release_date','')} | {album_data.get('artist','')} | {album_data.get('url','')}"
    return hashlib.sha256(unique_string.encode()).hexdigest()

def get_album_artwork(artwork_data):
    if not artwork_data or not artwork_data.get('url'):
        return None
    return artwork_data['url'].replace('{w}x{h}', '600x600')

def get_jwt_token():
    with open(JWT_PATH, 'r') as file:
        return file.readline().strip()

def get_headers():
    return {"Authorization": f"Bearer {get_jwt_token()}"}

def fetch_playlist_data(**kwargs):
    url = f"https://api.music.apple.com/v1/catalog/{STORE_FRONT}/playlists/{PLAYLIST_ID}"
    response = requests.get(url, headers=get_headers())
    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

    playlist_data = response.json()
    songs = playlist_data.get('data', [])[0].get('relationships', {}).get('tracks', {}).get('data', [])

    trending_songs = [
        {
            "song_name": song.get('attributes', {}).get('name', 'Unknown Song'),
            "album_name": song.get('attributes', {}).get('albumName', 'Unknown Album'),
            "artist": song.get('attributes', {}).get('artistName', 'Unknown Artist')
        }
        for song in songs
        if '- Single' not in song.get('attributes', {}).get('albumName', '')
    ]
    
    kwargs['ti'].xcom_push(key='reduced_songs', value=trending_songs)
    return trending_songs

def fetch_album_details(**kwargs):
    ti = kwargs['ti']
    reduced_songs = ti.xcom_pull(task_ids='fetch_playlist_data', key='reduced_songs')
    base_url = f"https://api.music.apple.com/v1/catalog/{STORE_FRONT}/search"
    
    album_details = []
    for song in reduced_songs:
        params = {"term": f"{song['album_name']} {song['artist']}", "types": "albums", "limit": 1}
        response = requests.get(base_url, headers=get_headers(), params=params)
        if response.status_code == 200:
            data = response.json()
            if albums := data.get('results', {}).get('albums', {}).get('data', []):
                album = albums[0]['attributes']
                album_details.append({
                    'album_name': album.get('name'),
                    'artist': album.get('artistName'),
                    'release_date': album.get('releaseDate'),
                    'track_count': album.get('trackCount'),
                    'genre': ', '.join(album.get('genreNames', [])),
                    'url': album.get('url'),
                    'editorial_notes': album.get('editorialNotes', {}).get('short', ''),
                    'cover_art': get_album_artwork(album.get('artwork'))
                })
    
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    recent_albums = [album for album in album_details if album['release_date'] and album['release_date'] >= seven_days_ago]
    
    kwargs['ti'].xcom_push(key='recent_albums', value=recent_albums)
    return recent_albums

def store_album_data(**kwargs):
    ti = kwargs['ti']
    recent_albums = ti.xcom_pull(task_ids='fetch_album_details', key='recent_albums')
    if not recent_albums:
        logging.info("No recent albums found to store")
        return

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        for album in recent_albums:
            release_date = None
            if album.get('release_date'):
                try:
                    release_date = datetime.strptime(album['release_date'], '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    pass

            track_count = int(album.get('track_count', 0)) if str(album.get('track_count', '0')).isdigit() else 0
            album_id = generate_album_id(album)
            
            cursor.execute(UPSERT_SQL, (
                album_id,
                str(album.get('album_name', '')),
                str(album.get('artist', '')),
                release_date,
                track_count,
                str(album.get('genre', '')),
                str(album.get('url', '')),
                str(album.get('editorial_notes', '')),
                album.get('cover_art')
            ))
        
        conn.commit()
        logging.info(f"Processed {len(recent_albums)} albums (duplicates skipped)")
    
    except Exception as e:
        conn.rollback()
        logging.error(f"Error storing album data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

with DAG(
    'apple_music_weekly_pipeline',
    default_args=default_args,
    description='Fetches recent trending audio, and pull it in the album info to postgres if release date was recent ',
    schedule=[JWT_DATASET],
    catchup=False,
    tags=['music', 'etl'],
) as dag:

    def within_24h(execution_date):
        return execution_date - timedelta(hours=1) 

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql=CREATE_TABLE_SQL
    )

    create_view = SQLExecuteQueryOperator(
        task_id='create_view',
        conn_id='postgres_default',
        sql=VIEW_SQL,
        outlets=[VIEW_DATASET]
    )

    fetch_playlist = PythonOperator(
        task_id='fetch_playlist_data',
        python_callable=fetch_playlist_data,
    )

    fetch_albums = PythonOperator(
        task_id='fetch_album_details',
        python_callable=fetch_album_details,
    )

    store_data = PythonOperator(
        task_id='store_album_data',
        python_callable=store_album_data,
    )

    create_table >> fetch_playlist >> fetch_albums >> store_data >> create_view