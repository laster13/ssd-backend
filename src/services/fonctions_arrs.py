import json
import requests
from pathlib import Path
from typing import Optional

RADARR_PORT = 7878
SONARR_PORT = 8989
CONFIG_PATH = Path("data/config.json")

def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError("Fichier config.json introuvable.")
    with open(CONFIG_PATH) as f:
        return json.load(f)


# ---------------------- RADARR SERVICE ----------------------
class RadarrService:
    def __init__(self):
        config = load_config()
        self.api_key = config.get("radarr_api_key")
        self.host = config.get("radarr_host", "localhost")  # tu peux l'ajouter si besoin
        if not self.api_key:
            raise ValueError("Clé API Radarr manquante dans config.json")

        self.base_url = f"http://{self.host}:{RADARR_PORT}/api/v3"
        self.headers = {"X-Api-Key": self.api_key}

    def get_movies(self):
        res = requests.get(f"{self.base_url}/movie", headers=self.headers)
        res.raise_for_status()
        return res.json()

    def get_movie_id_by_path(self, file_path: str) -> Optional[int]:
        movies = self.get_movies()
        for movie in movies:
            if movie.get("path") and file_path.startswith(movie["path"]):
                return movie["id"]
        return None

    def rescan_movie(self, movie_id: int):
        payload = {"name": "RescanMovie", "movieIds": [movie_id]}
        res = requests.post(f"{self.base_url}/command", json=payload, headers=self.headers)
        res.raise_for_status()
        return res.json()

    def search_missing_movie(self, movie_id: int):
        payload = {"name": "MoviesSearch", "movieIds": [movie_id]}
        res = requests.post(f"{self.base_url}/command", json=payload, headers=self.headers)
        res.raise_for_status()
        return res.json()


# ---------------------- SONARR SERVICE ----------------------
class SonarrService:
    def __init__(self):
        config = load_config()
        self.api_key = config.get("sonarr_api_key")
        self.host = config.get("sonarr_host", "localhost")  # tu peux l'ajouter si besoin
        if not self.api_key:
            raise ValueError("Clé API Sonarr manquante dans config.json")

        self.base_url = f"http://{self.host}:{SONARR_PORT}/api/v3"
        self.headers = {"X-Api-Key": self.api_key}

    def get_series(self):
        res = requests.get(f"{self.base_url}/series", headers=self.headers)
        res.raise_for_status()
        return res.json()

    def get_series_id_by_path(self, file_path: str) -> Optional[int]:
        series_list = self.get_series()
        for series in series_list:
            if series.get("path") and file_path.startswith(series["path"]):
                return series["id"]
        return None

    def rescan_series(self, series_id: int):
        payload = {"name": "RescanSeries", "seriesId": series_id}
        res = requests.post(f"{self.base_url}/command", json=payload, headers=self.headers)
        res.raise_for_status()
        return res.json()

    def search_missing_episodes(self, series_id: int):
        payload = {"name": "SeriesSearch", "seriesId": series_id}
        res = requests.post(f"{self.base_url}/command", json=payload, headers=self.headers)
        res.raise_for_status()
        return res.json()
