import time
import json
import threading
from pathlib import Path
from loguru import logger
import re

from program.utils.text_utils import normalize_name, clean_movie_name
from src.services.fonctions_arrs import RadarrService


# --- Cache partage ---
_radarr_index: dict[str, int] = {}
_radarr_catalog: dict[str, dict] = {}
_radarr_host: str | None = None
_radarr_idx_lock = threading.Lock()

# --- Parametres cache disque ---
_CACHE_FILE = Path.home() / ".cache" / "radarr_cache.json"
_INDEX_TTL_SEC = 86400  # 24h
_last_index_build = 0.0


def _add_index_alias(index: dict[str, int], title: str | None, year, tmdb_id: int):
    if not title:
        return

    cleaned = clean_movie_name(title)
    norm = normalize_name(cleaned)

    if not norm:
        return

    index[norm] = tmdb_id

    if year:
        index[f"{norm}{year}"] = tmdb_id


async def _build_radarr_index(force: bool = False) -> None:
    """
    Construit ou recharge l'index Radarr partage.

    Version sans appel TMDb massif.
    Radarr fournit deja:
    - title
    - originalTitle
    - tmdbId
    - imdbId
    - year
    - overview
    - genres
    - images
    - ratings

    On evite donc les milliers d'appels TMDb qui provoquent HTTP 429.
    """
    global _radarr_index, _radarr_catalog, _radarr_host, _last_index_build

    now = time.time()

    def _sanitize(obj):
        if isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        if isinstance(obj, dict):
            return {str(k): _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v) for v in obj]
        return str(obj)

    # --- 1. Lecture cache disque ---
    if not force and _CACHE_FILE.exists():
        try:
            mtime = _CACHE_FILE.stat().st_mtime

            if (now - mtime) < _INDEX_TTL_SEC:
                with open(_CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)

                with _radarr_idx_lock:
                    _radarr_index = data.get("index", {})
                    _radarr_catalog = data.get("catalog", {})
                    _radarr_host = data.get("host")
                    _last_index_build = mtime

                logger.info(
                    f"Cache Radarr charge ({len(_radarr_index)} cles, {len(_radarr_catalog)} films)"
                )
                return

        except Exception as e:
            logger.warning(f"Lecture cache disque echouee: {e}")

    # --- 2. Rebuild complet depuis Radarr ---
    try:
        radarr = RadarrService()
        movies = radarr.get_all_movies()

        logger.info(
            f"{len(movies)} films recuperes depuis Radarr pour indexation"
        )

        index: dict[str, int] = {}
        catalog: dict[str, dict] = {}

        for movie in movies:
            tmdb_id = movie.get("tmdbId")

            if not tmdb_id:
                continue

            year = movie.get("year")
            title = movie.get("title")
            original_title = movie.get("originalTitle")
            imdb_id = movie.get("imdbId")

            # Index base sur les titres Radarr.
            _add_index_alias(index, title, year, tmdb_id)
            _add_index_alias(index, original_title, year, tmdb_id)

            # Index ultra-fiable par IMDb.
            # Important pour les dossiers deja nommes avec {imdb-tt...}.
            if imdb_id:
                index[f"imdb:{str(imdb_id).lower()}"] = tmdb_id

            images = movie.get("images") or []
            poster = next(
                (
                    img.get("url")
                    for img in images
                    if img.get("coverType") == "poster"
                ),
                None,
            )

            catalog[str(tmdb_id)] = {
                "id": movie.get("id"),
                "title": title,
                "originalTitle": original_title,
                "tmdbId": tmdb_id,
                "imdbId": imdb_id,
                "year": year,
                "overview": movie.get("overview"),
                "genres": movie.get("genres") or [],
                "poster": poster,
                "images": movie.get("images"),
                "ratings": movie.get("ratings"),
            }

        # --- Correction poster host ---
        from routers.secure.symlinks import get_traefik_host

        host = get_traefik_host("radarr")

        for meta in catalog.values():
            poster = meta.get("poster")

            if poster and poster.startswith("/"):
                meta["poster"] = f"https://{host}{poster}" if host else poster

        # --- Mise a jour globale ---
        with _radarr_idx_lock:
            _radarr_index = index
            _radarr_catalog = catalog
            _radarr_host = host
            _last_index_build = now

        # --- Sauvegarde ---
        try:
            _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)

            with open(_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "index": _sanitize(_radarr_index),
                        "catalog": _sanitize(_radarr_catalog),
                        "host": _sanitize(_radarr_host),
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            logger.success(
                f"Cache Radarr sauvegarde | {len(_radarr_index)} cles, {len(_radarr_catalog)} films"
            )

        except Exception as e:
            logger.warning(f"Sauvegarde cache disque echouee: {e}")

        logger.success(
            f"Index Radarr pret: {len(_radarr_index)} cles | {len(_radarr_catalog)} films | host={host}"
        )

    except Exception as e:
        logger.exception(f"Echec construction index Radarr: {e}")


def enrich_from_radarr_index(
    symlink_path: Path,
    allow_fallback: bool = True,
) -> dict:
    """
    Enrichit un symlink avec les infos Radarr.

    Priorite de matching :
    1. IMDb tag dans le dossier : {imdb-tt...}
    2. variantes de titre en cache memoire
    3. fallback Radarr direct seulement si allow_fallback=True

    allow_fallback=False:
        utilise uniquement le cache memoire.
        Important pendant le scan initial des symlinks.
    """
    raw_name = symlink_path.parent.name

    def extract_imdb_id(value: str) -> str | None:
        match = re.search(r"(tt\d+)", value or "", flags=re.IGNORECASE)
        return match.group(1).lower() if match else None

    def candidate_keys(name: str) -> list[str]:
        keys = []

        def add_key(value: str | None):
            if not value:
                return

            norm_value = normalize_name(value)

            if norm_value and norm_value not in keys:
                keys.append(norm_value)

        imdb_id = extract_imdb_id(name)

        # 1. IMDb exact : le plus fiable.
        if imdb_id:
            keys.append(f"imdb:{imdb_id}")

        # 2. Nom nettoye standard.
        cleaned = clean_movie_name(name)
        add_key(cleaned)

        # 3. Nom brut normalise.
        add_key(name)

        # 4. Nom sans annee.
        no_year = re.sub(
            r"\(?\b(19|20)\d{2}\b\)?",
            "",
            cleaned,
            flags=re.IGNORECASE,
        ).strip()
        add_key(no_year)

        # 5. Nom sans tokens release courants.
        no_release_tokens = re.sub(
            r"\b("
            r"multi|french|truefrench|vostfr|subfrench|vo|vf|"
            r"1080p|720p|2160p|4k|uhd|hdr|dv|"
            r"bluray|blu-ray|web-dl|webdl|webrip|bdrip|hdrip|remux|"
            r"x264|x265|h264|h265|hevc|aac|ddp|dts|atmos|"
            r"proper|repack|extended|unrated|theatrical"
            r")\b",
            " ",
            cleaned,
            flags=re.IGNORECASE,
        )
        no_release_tokens = re.sub(r"\s+", " ", no_release_tokens).strip()
        add_key(no_release_tokens)

        # 6. Nom sans annee + sans tokens release.
        no_year_no_release = re.sub(
            r"\(?\b(19|20)\d{2}\b\)?",
            "",
            no_release_tokens,
            flags=re.IGNORECASE,
        ).strip()
        add_key(no_year_no_release)

        # 7. Si une annee existe, essaie aussi title+year.
        year_match = re.search(r"\b(19|20)\d{2}\b", name)

        if year_match:
            year = year_match.group(0)

            for base in (no_year, no_year_no_release):
                base_norm = normalize_name(base)

                if base_norm:
                    key_with_year = f"{base_norm}{year}"

                    if key_with_year not in keys:
                        keys.append(key_with_year)

        return keys

    keys = candidate_keys(raw_name)

    with _radarr_idx_lock:
        tmdb_id = None

        for key in keys:
            tmdb_id = _radarr_index.get(key)

            if tmdb_id:
                break

        movie = _radarr_catalog.get(str(tmdb_id)) if tmdb_id else None

    if movie:
        return _format_movie_metadata(movie)

    if not allow_fallback:
        return {}

    try:
        radarr = RadarrService()
        movie = radarr.get_movie_by_clean_title(raw_name)

        if not movie:
            return {}

        tmdb_id = movie.get("tmdbId")

        if not tmdb_id:
            return {}

        imdb_id = movie.get("imdbId")
        cleaned = clean_movie_name(raw_name)
        norm = normalize_name(cleaned)

        with _radarr_idx_lock:
            _radarr_index[norm] = tmdb_id

            if movie.get("year"):
                _radarr_index[f"{norm}{movie['year']}"] = tmdb_id

            if imdb_id:
                _radarr_index[f"imdb:{str(imdb_id).lower()}"] = tmdb_id

            _radarr_catalog[str(tmdb_id)] = movie

        return _format_movie_metadata(movie)

    except Exception as e:
        logger.warning(f"Radarr fallback failed for {raw_name}: {e}")
        return {}


def _format_movie_metadata(movie: dict) -> dict:
    """Normalise un dict movie Radarr en metadata exploitable."""
    poster_url = movie.get("poster")

    if poster_url and poster_url.startswith("/"):
        poster_url = f"https://{_radarr_host}{poster_url}" if _radarr_host else poster_url

    rating = None
    ratings = movie.get("ratings") or {}

    if isinstance(ratings, dict):
        tmdb_rating = ratings.get("tmdb")

        if isinstance(tmdb_rating, dict):
            rating = tmdb_rating.get("value")

    return {
        "id": movie.get("id"),
        "title": movie.get("title"),
        "tmdbId": movie.get("tmdbId"),
        "imdbId": movie.get("imdbId"),
        "poster": poster_url,
        "year": movie.get("year"),
        "rating": rating,
        "overview": movie.get("overview"),
        "genres": movie.get("genres") or [],
    }