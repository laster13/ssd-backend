import requests
from pathlib import Path
import json
from typing import Optional
import re
from fastapi import HTTPException
import logging

RADARR_PORT = 7878
SONARR_PORT = 8989
CONFIG_PATH = Path("data/config.json")
logger = logging.getLogger(__name__)


def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError("Fichier config.json introuvable.")
    with open(CONFIG_PATH) as f:
        return json.load(f)


class RadarrService:
    def __init__(self):
        config = load_config()
        self.api_key = config.get("radarr_api_key")
        self.host = config.get("radarr_host", "localhost")
        self.base_url = f"http://{self.host}:{RADARR_PORT}/api/v3"
        self.headers = {"X-Api-Key": self.api_key}

    def get_movie_by_clean_title(self, raw_name: str):
        """
        Recherche un film dans Radarr à partir d'un nom brut (ex: 'The Green Hornet (2011) {imdb-tt1234567}').
        Nettoie le nom, extrait le titre + année, et cherche un match exact dans Radarr.
        """
        logger.debug(f"🔍 Titre brut reçu : {raw_name}")

        # Retirer la partie {imdb-ttxxxxxxx}
        cleaned_name = re.sub(r"\{imdb-tt\d{7}\}", "", raw_name).strip()
        logger.debug(f"🧹 Titre nettoyé : {cleaned_name}")

        # Extraire l'année entre parenthèses
        title_match = re.match(r"^(.*?)(?:\s+\((\d{4})\))?$", cleaned_name)
        if not title_match:
            logger.warning(f"❗ Nom de film mal formaté : {cleaned_name}")
            return None

        base_title = title_match.group(1).strip()
        year = int(title_match.group(2)) if title_match.group(2) else None

        logger.debug(f"🔠 Titre sans année : {base_title}")
        if year:
            logger.debug(f"📅 Année extraite : {year}")

        try:
            response = requests.get(
                f"{self.base_url}/movie",
                headers=self.headers,
                params={"searchTerm": base_title}
            )
            response.raise_for_status()
            movies = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur API Radarr : {e}")
            raise HTTPException(status_code=500, detail="Erreur lors de la requête Radarr")

        # Recherche exacte
        for movie in movies:
            if movie["title"].lower() == base_title.lower():
                if year is None or movie.get("year") == year:
                    logger.debug(f"✅ Film trouvé : {movie['title']} ({movie.get('year')}) — ID Radarr : {movie['id']}")
                    return movie

        logger.warning(f"⚠️ Aucun film correspondant à '{base_title}' ({year}) trouvé dans Radarr.")
        return None

    def refresh_movie(self, movie_id: int):
        """Rafraîchit les métadonnées d'un film dans Radarr."""
        try:
            payload = {"name": "RefreshMovie", "movieIds": [movie_id]}
            logger.debug(f"Envoi du refresh pour movie_id={movie_id} avec payload: {payload}")
            res = requests.post(f"{self.base_url}/command", json=payload, headers=self.headers)
            res.raise_for_status()
            logger.info(f"✅ Rafraîchissement demandé pour le film ID {movie_id}")
            return res.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors du rafraîchissement du film : {e}")
            raise HTTPException(status_code=500, detail=f"Erreur lors du rafraîchissement du film : {e}")

    def search_missing_movie(self, movie_id: int):
        """Recherche les téléchargements manquants pour un film dans Radarr."""
        try:
            payload = {"name": "MoviesSearch", "movieIds": [movie_id]}
            logger.debug(f"📡 Envoi de la recherche manquante pour movie_id={movie_id} avec payload: {payload}")
            
            res = requests.post(f"{self.base_url}/command", json=payload, headers=self.headers)
            res.raise_for_status()
            
            response_data = res.json()
            logger.info(f"📦 Réponse complète Radarr (MoviesSearch) : {response_data}")
            
            command_id = response_data.get("id")
            command_state = response_data.get("state")
            logger.debug(f"📄 Commande Radarr : ID={command_id}, état={command_state}")
            
            return response_data

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur lors de la recherche des films manquants : {e}")
            raise HTTPException(status_code=500, detail=f"Erreur lors de la recherche des films manquants : {e}")

class SonarrService:
    def __init__(self):
        config = load_config()
        self.api_key = config.get("sonarr_api_key")
        self.host = config.get("sonarr_host", "localhost")  # Peut être en dur si localhost toujours
        self.base_url = f"http://{self.host}:8989/api/v3"
        self.headers = {"X-Api-Key": self.api_key}

    def get_series_by_clean_title(self, raw_name: str):
        logger.debug(f"📥 Titre brut reçu : {raw_name}")

        # 🔧 Nettoyage brut
        cleaned_name = re.sub(r"\s*\{imdb-tt\d{7,8}\}", "", raw_name).strip()
        logger.debug(f"🧼 Titre nettoyé transmis à Sonarr : {cleaned_name}")

        title_match = re.match(r"^(.*?)(?:\s+\((\d{4})\))?$", cleaned_name)
        if not title_match:
            logger.warning("❌ Échec du parsing du titre avec l’année")
            return None

        base_title = title_match.group(1).strip()
        year = int(title_match.group(2)) if title_match.group(2) else None
        logger.debug(f"🔎 Recherche dans Sonarr : base_title='{base_title}' | year={year}")

        try:
            res = requests.get(f"{self.base_url}/series", headers=self.headers)
            res.raise_for_status()
            all_series = res.json()
            logger.debug(f"📚 {len(all_series)} séries récupérées depuis Sonarr")
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erreur requête GET /series : {e}")
            raise HTTPException(status_code=500, detail=str(e))

        for series in all_series:
            sonarr_title = series["title"].strip().lower()

            # Nettoie les titres Sonarr pour comparaison
            sonarr_base_match = re.match(r"^(.*?)(?:\s+\((\d{4})\))?$", sonarr_title)
            sonarr_base_title = sonarr_base_match.group(1).strip() if sonarr_base_match else sonarr_title
            sonarr_year = series.get("year")

            # Match flexible
            if sonarr_base_title == base_title.lower():
                if year is None or sonarr_year == year:
                    logger.debug(f"✅ Série trouvée : {series['title']} ({sonarr_year})")
                    return series

        logger.warning(f"❗ Aucune correspondance trouvée pour : {base_title} (année={year})")
        return None

    def refresh_series(self, series_id: int):
        logger.debug(f"🔄 Rafraîchissement de la série ID={series_id}")
        try:
            return requests.post(
                f"{self.base_url}/command",
                json={"name": "RefreshSeries", "seriesId": series_id},
                headers=self.headers
            ).json()
        except Exception as e:
            logger.error(f"❌ Erreur lors du POST RefreshSeries : {e}")
            raise

    def search_missing_episodes(self, series_id: int):
        logger.debug(f"🔍 Lancement de la recherche manuelle pour ID={series_id}")
        try:
            return requests.post(
                f"{self.base_url}/command",
                json={"name": "SeriesSearch", "seriesId": series_id},
                headers=self.headers
            ).json()
        except Exception as e:
            logger.error(f"❌ Erreur lors du POST SeriesSearch : {e}")
            raise



    def get_missing_seasons(self, series_id: int) -> list[int]:
        """
        Retourne la liste des numéros de saisons où il manque au moins un épisode pour une série donnée.
        """
        try:
            response = requests.get(
                f"{self.base_url}/episode?seriesId={series_id}",
                headers=self.headers
            )
            response.raise_for_status()
            episodes = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur lors du GET /episode : {e}")
            raise HTTPException(status_code=500, detail=str(e))

        # Filtrer les épisodes manquants
        missing_episodes = [ep for ep in episodes if not ep.get("hasFile", True)]

        # Extraire les numéros de saison uniques
        missing_seasons = sorted(set(ep["seasonNumber"] for ep in missing_episodes))

        logger.debug(f"📌 Saisons manquantes pour série {series_id} : {missing_seasons}")
        return missing_seasons

    def get_all_series_with_missing_seasons(self) -> list[dict]:
        """
        Retourne une liste de séries qui ont au moins une saison avec des épisodes manquants.
        Chaque entrée contient : id, title, missing_seasons
        """
        try:
            response = requests.get(f"{self.base_url}/series", headers=self.headers)
            response.raise_for_status()
            all_series = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur lors du GET /series : {e}")
            raise HTTPException(status_code=500, detail=str(e))

        result = []
        for series in all_series:
            series_id = series.get("id")
            title = series.get("title")

            try:
                missing_seasons = self.get_missing_seasons(series_id)
            except Exception as e:
                logger.warning(f"⚠️ Impossible d'analyser la série {title} (ID={series_id}) : {e}")
                continue

            if missing_seasons:
                result.append({
                    "id": series_id,
                    "title": title,
                    "missing_seasons": missing_seasons
                })

        logger.info(f"📊 Séries avec saisons manquantes : {len(result)} trouvées")
        return result
