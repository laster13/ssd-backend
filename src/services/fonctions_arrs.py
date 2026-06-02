import requests
import json
import aiohttp
import re
import asyncio
from typing import Optional , Dict, List
from pathlib import Path
from fastapi import HTTPException
from loguru import logger
import logging
from program.utils.text_utils import normalize_name, clean_movie_name, clean_series_name
import os


RADARR_PORT = 7878
SONARR_PORT = 8989

RADARR_HOST = os.getenv("RADARR_HOST", "radarr")
SONARR_HOST = os.getenv("SONARR_HOST", "sonarr")

CONFIG_PATH = Path("data/config.json")
logger = logging.getLogger(__name__)


def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError("Fichier config.json introuvable.")
    with open(CONFIG_PATH) as f:
        return json.load(f)

class TMDbService:
    """
    🎬 Service pour interroger TMDb et récupérer IMDb IDs.
    - Supporte les modes synchrone et asynchrone
    - Utilise HTTP/2 pour l’asynchrone (via aiohttp)
    - Gère automatiquement les erreurs et timeouts
    """

    def __init__(self):
        config = load_config()
        self.api_key = config.get("tmdb_api_key")
        if not self.api_key:
            raise ValueError("❌ Clé TMDb introuvable dans config.json")
        self.base_url = "https://api.themoviedb.org/3"

    # ----------------------------------------------------------------------
    # 🌍 VERSION SYNCHRONE (compatible avec l’existant)
    # ----------------------------------------------------------------------
    def search_movie(self, query: str, year: int | None = None):
        """Recherche un film sur TMDb par nom + année (version synchrone)."""
        params = {"api_key": self.api_key, "query": query}
        if year:
            params["year"] = year

        try:
            res = requests.get(f"{self.base_url}/search/movie", params=params, timeout=10)
            res.raise_for_status()

            results = res.json().get("results", [])
            if not results:
                logger.debug(f"⚠️ Aucun résultat TMDb pour '{query}'")
                return None

            tmdb_id = results[0].get("id")
            return self.get_movie_details(tmdb_id)

        except requests.RequestException as e:
            logger.error(f"❌ Erreur TMDb search_movie({query}): {e}")
            return None

    def get_movie_details(self, tmdb_id: int):
        """Récupère les détails d’un film TMDb (inclut IMDb ID si dispo)."""
        url = f"{self.base_url}/movie/{tmdb_id}"
        params = {"api_key": self.api_key, "append_to_response": "external_ids"}
        try:
            res = requests.get(url, params=params, timeout=10)
            res.raise_for_status()

            data = res.json()
            imdb_id = data.get("external_ids", {}).get("imdb_id")
            return {
                "id": data.get("id"),
                "title": data.get("title"),
                "year": data.get("release_date", "")[:4],
                "imdb_id": imdb_id,
            }

        except requests.RequestException as e:
            logger.error(f"💥 Erreur TMDb get_movie_details({tmdb_id}): {e}")
            return None

    # ----------------------------------------------------------------------
    # ⚡ VERSION ASYNCHRONE (utilisée par /fix/imdb ultra-rapide)
    # ----------------------------------------------------------------------
    async def async_search_movie(self, session: aiohttp.ClientSession, query: str, year: int | None = None):
        """🔍 Recherche un film en mode asynchrone (rapide, HTTP/2)."""
        params = {"api_key": self.api_key, "query": query}
        if year:
            params["year"] = year

        url = f"{self.base_url}/search/movie"

        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as res:
                if res.status != 200:
                    text = await res.text()
                    logger.error(f"❌ Erreur TMDb async_search_movie({query}): {res.status} {text}")
                    return None

                data = await res.json()
                results = data.get("results", [])
                if not results:
                    logger.debug(f"⚠️ Aucun résultat TMDb pour '{query}'")
                    return None

                tmdb_id = results[0].get("id")
                return await self.async_get_movie_details(session, tmdb_id)

        except asyncio.TimeoutError:
            logger.warning(f"⏱ Timeout TMDb async_search_movie({query})")
            return None
        except Exception as e:
            logger.warning(f"💥 Erreur TMDb async_search_movie({query}): {e}")
            return None

    async def async_get_movie_details(self, session: aiohttp.ClientSession, tmdb_id: int):
        """📄 Récupère les détails TMDb d’un film en mode asynchrone."""
        url = f"{self.base_url}/movie/{tmdb_id}"
        params = {"api_key": self.api_key, "append_to_response": "external_ids"}

        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as res:
                if res.status != 200:
                    text = await res.text()
                    logger.error(f"❌ Erreur TMDb async_get_movie_details({tmdb_id}): {res.status} {text}")
                    return None

                data = await res.json()
                imdb_id = data.get("external_ids", {}).get("imdb_id")
                return {
                    "id": data.get("id"),
                    "title": data.get("title"),
                    "year": data.get("release_date", "")[:4],
                    "imdb_id": imdb_id,
                }

        except asyncio.TimeoutError:
            logger.warning(f"⏱ Timeout TMDb async_get_movie_details({tmdb_id})")
            return None
        except Exception as e:
            logger.warning(f"💥 Erreur TMDb async_get_movie_details({tmdb_id}): {e}")
            return None

class RadarrService:
    def __init__(self):
        config = load_config()
        self.api_key = config.get("radarr_api_key")
        self.host = config.get("radarr_host") or RADARR_HOST
        self.base_url = f"http://{self.host}:{RADARR_PORT}/api/v3"
        self.headers = {"X-Api-Key": self.api_key}
        self.profile_cache = {}

    def cache_profile(self, imdb_id: str, profile_id: int):
        """🧠 Sauvegarde le profil qualité en mémoire."""
        self.profile_cache[imdb_id] = profile_id

    def get_cached_profile(self, imdb_id: str) -> Optional[int]:
        """🔎 Récupère le profil qualité sauvegardé (si dispo)."""
        return self.profile_cache.get(imdb_id)


    def get_movie_by_clean_title(self, raw_name: str):
        logger.debug(f"📥 Titre brut reçu : {raw_name}")

        # 🔧 Nettoyage brut
        cleaned_name = re.sub(r"\s*\{imdb-tt\d{7,8}\}", "", raw_name).strip()
        logger.debug(f"🧼 Titre nettoyé transmis à Radarr : {cleaned_name}")

        title_match = re.match(r"^(.*?)(?:\s+\((\d{4})\))?$", cleaned_name)
        if not title_match:
            logger.warning("❌ Échec du parsing du titre avec l’année")
            return None

        base_title = title_match.group(1).strip()
        year = int(title_match.group(2)) if title_match.group(2) else None
        logger.debug(f"🔎 Recherche dans Radarr : base_title='{base_title}' | year={year}")

        try:
            res = requests.get(f"{self.base_url}/movie", headers=self.headers)
            res.raise_for_status()
            all_movies = res.json()
            logger.debug(f"🎬 {len(all_movies)} films récupérés depuis Radarr")
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erreur requête GET /movie : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

        for movie in all_movies:
            radarr_title = movie["title"].strip().lower()
            radarr_base_match = re.match(r"^(.*?)(?:\s+\((\d{4})\))?$", radarr_title)
            radarr_base_title = radarr_base_match.group(1).strip() if radarr_base_match else radarr_title
            radarr_year = movie.get("year")

            if normalize_name(radarr_base_title) == normalize_name(base_title):
                if year is None or radarr_year == year:
                    logger.debug(f"✅ Film trouvé par nom : {movie['title']} ({radarr_year})")
                    return movie

        # 🔄 Fallback IMDb ID
        imdb_match = re.search(r"imdb-(tt\d{7,8})", raw_name)
        if imdb_match:
            imdb_id = imdb_match.group(1)
            logger.debug(f"🆘 Fallback IMDb — Recherche via ID : {imdb_id}")
            try:
                lookup_url = f"{self.base_url}/movie/lookup/imdb?imdbId={imdb_id}"
                res = requests.get(lookup_url, headers=self.headers)
                res.raise_for_status()
                movie = res.json()
                if movie and "title" in movie:
                    logger.debug(f"✅ Film trouvé via IMDb : {movie['title']} ({movie.get('year')})")
                    return movie
            except requests.exceptions.RequestException as e:
                logger.error(f"🌐 Erreur lookup IMDb : {e}", exc_info=True)

        logger.warning(f"❗ Aucune correspondance trouvée pour : {base_title} (année={year})")
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

    def get_all_movies(self):
        """Récupère la liste complète des films depuis Radarr."""
        try:
            res = requests.get(f"{self.base_url}/movie", headers=self.headers)
            res.raise_for_status()
            movies = res.json()
            logger.info(f"📚 {len(movies)} films récupérés depuis Radarr (get_all_movies)")
            return movies
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erreur lors de la récupération des films : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Erreur récupération films Radarr : {e}")

    def get_movie_by_imdb(self, imdb_id: str):
        """
        Recherche un film dans Radarr via IMDb ID.
        Renvoie un objet complet (avec 'id') si le film est déjà importé,
        sinon renvoie l'objet du lookup brut.
        """
        try:
            # 1️⃣ Lookup direct par IMDb
            lookup_url = f"{self.base_url}/movie/lookup/imdb?imdbId={imdb_id}"
            res = requests.get(lookup_url, headers=self.headers)
            res.raise_for_status()
            lookup_movie = res.json()

            if not lookup_movie or "tmdbId" not in lookup_movie:
                logger.warning(f"❌ Aucun film trouvé via IMDb {imdb_id}")
                return None

            tmdb_id = lookup_movie["tmdbId"]

            # 2️⃣ Vérifier dans la liste des films existants
            all_movies = self.get_all_movies()
            for movie in all_movies:
                if movie.get("tmdbId") == tmdb_id:
                    logger.debug(f"✅ Film trouvé dans Radarr par IMDb {imdb_id} → {movie['title']}")
                    return movie

            # 3️⃣ Si pas trouvé dans la liste → renvoyer quand même le lookup (incomplet)
            logger.debug(f"⚠️ Film IMDb {imdb_id} trouvé en lookup mais pas importé dans Radarr")
            return lookup_movie

        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erreur Radarr lookup IMDb {imdb_id} : {e}")
            return None

    def get_movie_by_tmdb(self, tmdb_id: int):
        """
        Recherche un film dans Radarr via TMDb ID.
        Renvoie un objet complet (avec 'id') si le film est déjà importé,
        sinon renvoie l'objet lookup brut.
        """
        try:
            lookup_url = f"{self.base_url}/movie/lookup/tmdb?tmdbId={tmdb_id}"
            res = requests.get(lookup_url, headers=self.headers)
            res.raise_for_status()
            lookup_movie = res.json()

            if not lookup_movie or "title" not in lookup_movie:
                logger.warning(f"❌ Aucun film trouvé via TMDb {tmdb_id}")
                return None

            all_movies = self.get_all_movies()
            for movie in all_movies:
                if movie.get("tmdbId") == tmdb_id:
                    logger.debug(f"✅ Film trouvé dans Radarr par TMDb {tmdb_id} → {movie['title']}")
                    return movie

            logger.debug(f"⚠️ Film TMDb {tmdb_id} trouvé en lookup mais pas importé")
            return lookup_movie

        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erreur Radarr lookup TMDb {tmdb_id} : {e}", exc_info=True)
            return None


class SonarrService:
    def __init__(self):
        config = load_config()
        self.api_key = config.get("sonarr_api_key")
        self.host = config.get("sonarr_host") or SONARR_HOST
        self.base_url = f"http://{self.host}:{SONARR_PORT}/api/v3"
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
            if normalize_name(sonarr_base_title) == normalize_name(base_title):
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

        # logger.debug(f"📌 Saisons manquantes pour série {series_id} : {missing_seasons}")
        return missing_seasons

    def get_all_series_with_missing_seasons(self) -> list[dict]:
        """
        Retourne une liste de séries qui ont au moins une saison avec des épisodes manquants
        (hors saison 0). Chaque entrée contient : id, title, missing_seasons
        """
        try:
            response = requests.get(f"{self.base_url}/series", headers=self.headers)
            response.raise_for_status()
            all_series = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur lors du GET /series : {e}")
            raise HTTPException(status_code=500, detail=str(e))

        result = []
        total_seasons = 0

        for series in all_series:
            series_id = series.get("id")
            title = series.get("title")

            try:
                missing_seasons = self.get_missing_seasons(series_id)
            except Exception as e:
                logger.warning(f"⚠️ Impossible d'analyser la série {title} (ID={series_id}) : {e}")
                continue

            # ❌ Exclure la saison 0
            valid_missing_seasons = [s for s in missing_seasons if s != 0]

            if valid_missing_seasons:
                result.append({
                    "id": series_id,
                    "title": title,
                    "missing_seasons": valid_missing_seasons
                })
                total_seasons += len(valid_missing_seasons)

        logger.info(f"📊 Séries avec saisons manquantes (hors saison 0) : {len(result)}")
        logger.info(f"📊 Total de saisons manquantes (hors saison 0) : {total_seasons}")

        return result

    def search_season(self, series_id: int, season_number: int):
        """
        Lance une recherche pour tous les épisodes de la saison spécifiée (sans toucher aux autres saisons).
        """
        # logger.debug(f"🔎 Recherche de la saison {season_number} pour la série ID={series_id}")
        try:
            response = requests.post(
                f"{self.base_url}/command",
                headers=self.headers,
                json={
                    "name": "SeasonSearch",
                    "seriesId": series_id,
                    "seasonNumber": season_number
                }
            )
            response.raise_for_status()
            logger.info(f"✅ Recherche envoyée pour saison {season_number} (série ID={series_id})")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur lors du SeasonSearch : {e}")
            raise

    def get_all_episodes(self, series_id: int) -> list[dict]:
        """Récupère tous les épisodes d'une série depuis l'API Sonarr"""
        try:
            response = requests.get(
                f"{self.base_url}/episode",
                params={"seriesId": series_id},
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                logger.warning(f"⚠️ Format inattendu reçu pour les épisodes (ID={series_id}) : {data}")
                return []
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur récupération épisodes pour la série ID={series_id} : {e}", exc_info=True)
            return []

    def get_all_series(self) -> list[dict]:
        """Récupère toutes les séries configurées dans Sonarr"""
        try:
            response = requests.get(
                f"{self.base_url}/series",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            series_list = response.json()
            if not isinstance(series_list, list):
                logger.warning(f"⚠️ Format inattendu reçu pour les séries : {series_list}")
                return []
            return series_list
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur lors du GET /series : {e}", exc_info=True)
            return []

    def get_episode(self, series_id: int, season: int, episode: int) -> dict | None:
        """
        Récupère un épisode précis (saison + numéro) pour une série donnée.
        """
        episodes = self.get_all_episodes(series_id)
        for ep in episodes:
            if ep.get("seasonNumber") == season and ep.get("episodeNumber") == episode:
                return ep
        return None

    def resolve_series(self, raw_name: str) -> Optional[dict]:
        """
        Résout une série à partir d’un nom brut de dossier/fichier :
        1. Essaye par titre nettoyé
        2. Fallback via IMDb si présent dans le nom {imdb-tt...}
        3. Fallback via TMDb si présent {tmdb-...}
        """
        cleaned = clean_series_name(raw_name)

        # 1️⃣ Essai direct via titre
        match = self.get_series_by_clean_title(cleaned)
        if match:
            return match

        # 2️⃣ Fallback IMDb
        imdb_match = re.search(r"imdb-(tt\d+)", raw_name)
        if imdb_match:
            imdb_id = imdb_match.group(1)
            try:
                return self.get_series_by_imdb(imdb_id)
            except Exception as e:
                logger.warning(f"⚠️ IMDb fallback échoué pour {imdb_id}: {e}")

        # 3️⃣ (Optionnel) Fallback TMDb
        tmdb_match = re.search(r"tmdb-(\d+)", raw_name)
        if tmdb_match:
            tmdb_id = int(tmdb_match.group(1))
            try:
                return self.get_series_by_tmdb(tmdb_id)
            except Exception as e:
                logger.warning(f"⚠️ TMDb fallback échoué pour {tmdb_id}: {e}")

        logger.warning(f"❌ Impossible de résoudre la série : {raw_name}")
        return None

    def get_series_by_imdb(self, imdb_id: str) -> Optional[dict]:
        """Recherche une série directement via IMDb ID."""
        try:
            lookup_url = f"{self.base_url}/series/lookup?term=imdb:{imdb_id}"
            res = requests.get(lookup_url, headers=self.headers, timeout=10)
            res.raise_for_status()
            data = res.json()
            if isinstance(data, list) and data:
                return data[0]  # retourne la première correspondance
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erreur lookup IMDb {imdb_id} : {e}", exc_info=True)
            return None

    def get_series_by_tmdb(self, tmdb_id: int) -> Optional[dict]:
        """Recherche une série directement via TMDb ID."""
        try:
            lookup_url = f"{self.base_url}/series/lookup?term=tmdb:{tmdb_id}"
            res = requests.get(lookup_url, headers=self.headers, timeout=10)
            res.raise_for_status()
            data = res.json()
            if isinstance(data, list) and data:
                return data[0]
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Erreur lookup TMDb {tmdb_id} : {e}", exc_info=True)
            return None