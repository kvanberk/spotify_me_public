import tekore as tk
import json
import os
import pandas as pd

# class to handle spotify login and authorisation
class spotify_login:
    def __init__ (self, client_id, client_secret, redirect_uri):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
    
    def authenticate(self, scope=tk.scope.every, save=True):
        if os.path.exists("config/login.cfg"):
            conf = tk.config_from_file("config/login.cfg", return_refresh=True)
            user_token = tk.refresh_user_token(*conf[:2], conf[3])
        else:
            user_token = tk.prompt_for_user_token(self.client_id, self.client_secret, self.redirect_uri, scope)

        app_token = tk.request_client_token(self.client_id, self.client_secret)
        instance = tk.Spotify(app_token, max_limits_on=False, chunked_on=True)
        instance.token = user_token

        if save:
            config = (self.client_id, self.client_secret, self.redirect_uri, user_token.refresh_token)
            tk.config_to_file("config/login.cfg", config)

        return instance

# class to handle api requests
class spotify_fetch:
    def __init__(self, artists, albums, tracks):
        self.artists = artists
        self.albums = albums
        self.tracks = tracks
    
    def artist_fetch():
        return pd.DataFrame(instance.current_user_top_artists().items)

    def album_fetch(artists):
        spotify_albums = list()
        for artist in artists.id.to_list():
            albums = instance.artist_albums(artist, limit=50)
            for album in albums.items:
                record = album.__dict__
                record['artist_id'] = artist
                spotify_albums.append(record)
        return pd.DataFrame(spotify_albums)

    def track_fetch(albums):
        spotify_tracks = list()
        for album in albums.id.to_list():
            tracks = instance.album_tracks(album, limit=50)
            for track in tracks.items:
                record = track.__dict__
                record['album_id'] = album
                spotify_tracks.append(record)  
        return pd.DataFrame(spotify_tracks)

# class to handle credential and data read/write
class spotify_file:
    def credentials():
        with open("restricted/credentials.txt") as f:
            try:
                return f.read().split("\n")
            except:
                raise Exception('Credential file is invalid')

    def store(data):
        for name, value in data.__dict__.items():
            output = value.to_json(orient="records")
            with open(f"datahub/raw/{name}.json", "w") as f:
                json.dump(output, f)

spotify = spotify_login(*spotify_file.credentials()) # instance will open url and require user to C&P into terminal
instance = spotify.authenticate()

artists = spotify_fetch.artist_fetch()
albums = spotify_fetch.album_fetch(artists)
tracks = spotify_fetch.track_fetch(albums)

dataframes = spotify_fetch(artists, albums, tracks)
spotify_file.store(dataframes)