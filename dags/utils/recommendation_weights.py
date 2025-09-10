
# import json
# from datetime import dat#etime

'''
9/9 definitely needs work. this recomendation system isnt too strong, 
There is a huge issue with the single favorite artist input, hwo to have that clean, limit to one person, how do i clean that, should I just match to similar artist? that would take some mapping
'''

def calculate_album_score(album, user_prefs):
    """
    Calculates a personalized score for an album based on user preferences.
    album: dict (row from your album view)
    user_prefs: dict (row from user_preferences table for a specific user)
    """
    score = 0

    # Genre Matching (important factor)
    album_genres = [g.strip() for g in album['genre'].split(',')] if album['genre'] else []
    user_genres = user_prefs['genres']  
    
    # O(N^2?) - lol maybe adjust this
    for album_genre in album_genres:
        if album_genre in user_genres:
            score += 15  

    #Favorite Artist Match (most important)  
    # this will need to  be cleaned, we have way to much variability for input
    user_fav_artist = user_prefs.get('favorite_artist', '').lower()
    album_artist = album['artist'].lower() if album['artist'] else ''
    
    if user_fav_artist and user_fav_artist in album_artist:
        score += 25  

    # Album Length Preference (mid importance)
    user_length_pref = user_prefs.get('album_length', 'standard')
    track_count = album.get('track_count', 0)
    
    if user_length_pref == 'short' and 1 <= track_count <= 6:
        score += 10
    elif user_length_pref == 'standard' and 7 <= track_count <= 18:
        score += 10
    elif user_length_pref == 'long' and track_count >= 19:
        score += 10

    return score