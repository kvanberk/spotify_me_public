import json
import pandas as pd
from dataclasses import dataclass
import glob
import os.path

# class to handle data collation
@dataclass
class spotify_store:
    artists: pd.DataFrame
    artists_external_urls: pd.DataFrame
    artists_genres: pd.DataFrame
    albums: pd.DataFrame
    albums_external_urls: pd.DataFrame
    albums_markets: pd.DataFrame
    tracks: pd.DataFrame
    tracks_external_urls: pd.DataFrame
    tracks_markets: pd.DataFrame

# class to handle data reading and writing
class spotify_file:
    def read_directory():
        files = glob.glob("datahub/raw/*")
        names = [os.path.basename(file).split(".")[0] for file in files]
        return dict(zip(names, files))

    def read_json(directory):
        with open(directory) as f:
            file = json.load(f)
            return pd.read_json(file)

    def store(data):
        for name, value in data.__dict__.items():
            output = value.drop_duplicates().to_json(orient="records")
            with open(f"datahub/access/{name}.json", "w") as f:
                json.dump(output, f)

# class to handle column unpackaging
class spotify_handle:
    def handle_external_urls(externals):
        return externals.apply(lambda x: list(x.values()))
        
    def handle_image_urls(image):
        return image.apply(pd.Series)[0].apply(pd.Series)['url']

    def handle_followers(follower):
        return follower.apply(pd.Series)['total']
    
    def handle_explode(dataframe):
        return dataframe.explode(dataframe.columns[1])

    def handle_drop(dataframe, columns):
        return dataframe.drop(columns = columns)


# class to handle data unpackaging
class spotify_process:
    def spotify_artists(artists):
        artists.external_urls = spotify_handle.handle_external_urls(artists.external_urls)
        artists.followers = spotify_handle.handle_followers(artists.followers)
        artists.images = spotify_handle.handle_image_urls(artists.images)
        artists_genres = spotify_handle.handle_explode(artists[['id','genres']])
        artists_external_urls = spotify_handle.handle_explode(artists[['id','external_urls']])
        artists = spotify_handle.handle_drop(artists, ['external_urls','genres'])

        return artists, artists_external_urls, artists_genres
            
    def spotify_albums(albums):
        albums.external_urls = spotify_handle.handle_external_urls(albums.external_urls)
        albums.images = spotify_handle.handle_image_urls(albums.images)
        albums_markets = spotify_handle.handle_explode(albums[['id','available_markets']])
        albums_external_urls = spotify_handle.handle_explode(albums[['id','external_urls']])
        albums = spotify_handle.handle_drop(albums, ['artists','available_markets','external_urls'])

        return albums, albums_external_urls, albums_markets

    def spotify_tracks(tracks):
        tracks.external_urls = spotify_handle.handle_external_urls(tracks.external_urls)
        tracks_external_urls = spotify_handle.handle_explode(tracks[['id','external_urls']])
        tracks_markets = spotify_handle.handle_explode(tracks[['id','available_markets']])
        tracks = spotify_handle.handle_drop(tracks, ['artists','available_markets','external_urls'])

        return tracks, tracks_external_urls, tracks_markets 

files = spotify_file.read_directory()

raw_artists = spotify_file.read_json(files['artists'])
raw_albums = spotify_file.read_json(files['albums'])
raw_tracks = spotify_file.read_json(files['tracks'])

artists, artists_external_urls, artists_genres = spotify_process.spotify_artists(raw_artists)
albums, albums_external_urls, albums_markets = spotify_process.spotify_albums(raw_albums)
tracks, tracks_external_urls, tracks_markets = spotify_process.spotify_tracks(raw_tracks)

dataframes = spotify_store(artists, artists_external_urls, artists_genres,albums, albums_external_urls, albums_markets,tracks, tracks_external_urls, tracks_markets)

spotify_file.store(dataframes)