import time
import json
import asyncio
import aiohttp
import threading
from pathlib import Path
from loguru import logger
from pathlib import Path

from program.utils.text_utils import normalize_name, clean_movie_name
from program.settings.manager import config_manager
from src.services.fonctions_arrs import RadarrService

# --- Cache partagé ---
_radarr_index: dict[str, int] = {}      # titre normalisé → tmdbId
_radarr_catalog: dict[str, dict] = {}   # tmdbId → movie complet
_radarr_host: str | None = None
_radarr_idx_lock = threading.Lock()

# --- Paramètres cache disque ---
_CACHE_FILE = Path.home() / ".cache" / "radarr_cache.json"
_INDEX_TTL_SEC = 86400  # 24h
_last_index_build = 0.0


async def _fetch_tmdb_details(session, tmdb_id: int, lang: str, api_key: str, sem: asyncio.Semaphore):
    """Récupère les détails TMDb pour un film donné (une langue), avec limite de parallélisme."""
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={api_key}&language={lang}&append_to_response=external_ids"
    async with sem:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} {await resp.text()}")
            return await resp.json()


# ✅ VERSION CORRIGÉE _build_radarr_index() AVEC IMDb DE RADARR

async def _build_radarr_index(force: bool = False) -> None:
    """
    Construit (ou recharge) l’index Radarr partagé.
    ⚡ Version optimisée + cache disque
    ⚡ Ajout IMDb via TMDb external_ids + ✅ via Radarr DIRECT
    """
    global _radarr_index, _radarr_catalog, _radarr_host, _last_index_build
    now = time.time()

    def _sanitize(obj):
        """Convertit récursivement tout objet en structure JSON-safe."""
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
                _radarr_index = data.get("index", {})
                _radarr_catalog = data.get("catalog", {})
                _radarr_host = data.get("host")
                _last_index_build = mtime
                logger.info(
                    f"✅ Cache Radarr chargé ({len(_radarr_index)} clés, {len(_radarr_catalog)} films)"
                )
                return
        except Exception as e:
            logger.warning(f"⚠️ Lecture cache disque échouée : {e}")

    # --- 2. Rebuild complet depuis Radarr ---
    try:
        radarr = RadarrService()
        movies = radarr.get_all_movies()
        logger.info(f"📚 {len(movies)} films récupérés depuis Radarr pour indexation")

        index: dict[str, int] = {}
        catalog: dict[str, dict] = {}

        api_key = config_manager.config.tmdb_api_key
        if not api_key:
            logger.error("❌ Pas de clé TMDb")
            return

        async with aiohttp.ClientSession() as session:
            sem = asyncio.Semaphore(10)
            tasks = []

            for m in movies:
                tmdb_id = m.get("tmdbId")
                if not tmdb_id:
                    continue

                # --- Index basé sur nom radarr ---
                title = m.get("title") or ""
                year = m.get("year")
                cleaned = clean_movie_name(title)
                norm = normalize_name(cleaned)
                if norm:
                    index[norm] = tmdb_id
                    if year:
                        index[f"{norm}{year}"] = tmdb_id

                # --- Ajout au catalogue ---
                if str(tmdb_id) not in catalog:
                    images = m.get("images") or []
                    poster = next(
                        (img.get("url") for img in images if img.get("coverType") == "poster"),
                        None
                    )
                    catalog[str(tmdb_id)] = {
                        "id": m.get("id"),
                        "title": m.get("title"),
                        "originalTitle": m.get("originalTitle"),
                        "tmdbId": tmdb_id,
                        "year": m.get("year"),
                        "overview": m.get("overview"),
                        "genres": m.get("genres") or [],
                        "poster": poster,
                        "images": m.get("images"),
                        "ratings": m.get("ratings"),
                    }

                # ✅✅✅ AJOUT CRUCIAL ICI ✅✅✅
                # ---------------------------------------------------------
                # Récupère IMDb directement depuis Radarr (fiable à 100 %)
                # ---------------------------------------------------------
                imdb_direct = m.get("imdbId")
                if imdb_direct:
                    catalog[str(tmdb_id)]["imdbId"] = imdb_direct
                # ---------------------------------------------------------

                # --- Tâches TMDb FR/EN ---
                for lang in ["en-US", "fr-FR"]:
                    tasks.append(_fetch_tmdb_details(session, tmdb_id, lang, api_key, sem))

            # --- Résultats TMDb ---
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for details in results:
                if isinstance(details, Exception):
                    logger.error(f"💥 Erreur TMDb fetch : {details}")
                    continue

                tmdb_id = details.get("id")
                year = details.get("release_date", "").split("-")[0] if details.get("release_date") else None
                alt_title = details.get("title") or details.get("original_title")
                if not tmdb_id or not alt_title:
                    continue

                # --- Enrichissement IMDb via TMDb external_ids ---
                imdb_id = details.get("external_ids", {}).get("imdb_id")
                if imdb_id and str(tmdb_id) in catalog:
                    catalog[str(tmdb_id)]["imdbId"] = imdb_id

                # --- Aliases normalisés ---
                cleaned_alt = clean_movie_name(alt_title)
                norm_alt = normalize_name(cleaned_alt)
                if norm_alt:
                    index[norm_alt] = tmdb_id
                    if year:
                        index[f"{norm_alt}{year}"] = tmdb_id

        # --- Correction poster host ---
        from routers.secure.symlinks import get_traefik_host
        host = get_traefik_host("radarr")
        for meta in catalog.values():
            poster = meta.get("poster")
            if poster and poster.startswith("/"):
                meta["poster"] = f"https://{host}{poster}" if host else poster

        # --- Mise à jour globale ---
        with _radarr_idx_lock:
            _radarr_index = index
            _radarr_catalog = catalog
            _radarr_host = host
            _last_index_build = now

        # --- Sauvegarde ---
        try:
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
                f"💾 Cache Radarr sauvegardé | {len(_radarr_index)} clés, {len(_radarr_catalog)} films"
            )
        except Exception as e:
            logger.warning(f"⚠️ Sauvegarde cache disque échouée : {e}")

        logger.success(
            f"🗂️ Index Radarr prêt : {len(_radarr_index)} clés | {len(_radarr_catalog)} films | host={host}"
        )

    except Exception as e:
        logger.exception(f"💥 Échec construction index Radarr: {e}")

def enrich_from_radarr_index(symlink_path: Path) -> dict:
    """Enrichit un symlink avec les infos Radarr.
    ⚡ Si le film n'est pas déjà dans l'index mémoire, on le cherche via Radarr et on le patch en mémoire.
    ❌ Pas d'écriture disque ici → la sauvegarde est gérée par _build_radarr_index.
    """
    raw_name = symlink_path.parent.name
    cleaned = clean_movie_name(raw_name)
    norm = normalize_name(cleaned)

    with _radarr_idx_lock:
        tmdb_id = _radarr_index.get(norm) or _radarr_index.get(f"{norm}")

    # 🎯 Déjà en mémoire → enrichissement direct
    if tmdb_id:
        movie = _radarr_catalog.get(str(tmdb_id))
        if movie:
            return _format_movie_metadata(movie)

    # 🚨 Pas trouvé → on va interroger Radarr (fallback direct)
    try:
        radarr = RadarrService()
        movie = radarr.get_movie_by_clean_title(raw_name)
        if not movie:
            return {}

        tmdb_id = movie.get("tmdbId")
        if not tmdb_id:
            return {}

        # ✅ Patch mémoire (index + catalog)
        with _radarr_idx_lock:
            _radarr_index[norm] = tmdb_id
            if movie.get("year"):
                _radarr_index[f"{norm}{movie['year']}"] = tmdb_id
            _radarr_catalog[str(tmdb_id)] = movie

        return _format_movie_metadata(movie)

    except Exception as e:
        logger.warning(f"⚠️ Radarr fallback échoué pour {raw_name}: {e}")
        return {}


def _format_movie_metadata(movie: dict) -> dict:
    """Normalise un dict movie Radarr en metadata exploitable."""
    # 🎨 Poster corrigé
    poster_url = movie.get("poster")
    if poster_url and poster_url.startswith("/"):
        poster_url = f"https://{_radarr_host}{poster_url}" if _radarr_host else poster_url

    # ⭐ Note TMDb
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
        "poster": poster_url,
        "year": movie.get("year"),
        "rating": rating,
        "overview": movie.get("overview"),
        "genres": movie.get("genres") or [],
    }
