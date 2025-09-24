# music_utils.py
import requests
import re

def get_apple_music_token():
    """Get Apple Music JWT token from file"""
    token_path = 'airflow-docker/secrets/apple_jwt.txt'
    with open(token_path, 'r') as f:
        token_content = f.read().strip()
        token_match = re.search(r'([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)', token_content)
        return token_match.group(1) if token_match else token_content.split('\n')[0].strip()

def get_related_artists(artist_name):
    """Get related artists for a given artist"""
    headers = {'Authorization': f'Bearer {get_apple_music_token()}'}
    
    # Get artist ID
    search_response = requests.get(
        'https://api.music.apple.com/v1/catalog/us/search',
        params={'term': artist_name, 'types': 'artists', 'limit': 1},
        headers=headers
    )
    
    if search_response.status_code != 200:
        return []
    
    artist_data = search_response.json()
    artists_list = artist_data['results']['artists'].get('data', [])
    
    if not artists_list:
        return []
    
    artist_id = artists_list[0]['id']
    
    # Get artist genre
    artist_response = requests.get(
        f'https://api.music.apple.com/v1/catalog/us/artists/{artist_id}',
        headers=headers
    )
    
    if artist_response.status_code != 200:
        return []
    
    artist_info = artist_response.json()
    genres = artist_info['data'][0]['attributes'].get('genreNames', [])
    
    if not genres:
        return []
    
    # Search for artists in the same genre
    genre_response = requests.get(
        'https://api.music.apple.com/v1/catalog/us/search',
        params={'term': genres[0], 'types': 'artists', 'limit': 15},
        headers=headers
    )
    
    if genre_response.status_code != 200:
        return []
    
    genre_artists = genre_response.json()['results']['artists']['data']
    
    # Return artist names (excluding the original), max 10
    return [
        artist['attributes']['name'] 
        for artist in genre_artists 
        if artist['attributes']['name'].lower() != artist_name.lower()
    ][:10]

def process_user_artists(artist_input):
    """Process comma-separated artist input and return related artists"""
    favorite_artists = [artist.strip() for artist in artist_input.split(',') if artist.strip()]
    
    all_related_artists = []
    for artist in favorite_artists:
        related = get_related_artists(artist)
        all_related_artists.extend(related)
    
    # Remove duplicates while preserving order
    unique_related_artists = []
    for artist in all_related_artists:
        if artist not in unique_related_artists:
            unique_related_artists.append(artist)
    
    return favorite_artists, unique_related_artists[:15]