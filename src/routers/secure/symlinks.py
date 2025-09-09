from typing import Optional
from pathlib import Path
from collections import Counter
from fastapi import APIRouter, HTTPException, Query, Request, Depends
from fastapi.responses import StreamingResponse
from fastapi.concurrency import run_in_threadpool
import json
import urllib.parse
import asyncio
import re
import os
import httpx
import docker
from datetime import datetime
from loguru import logger
from urllib.parse import unquote
from program.managers.sse_manager import sse_manager
from src.services.fonctions_arrs import RadarrService, SonarrService
from program.settings.manager import config_manager
from program.settings.models import SymlinkConfig
from program.utils.text_utils import normalize_name, clean_movie_name, clean_series_name
from program.utils.discord_notifier import send_discord_message

router = APIRouter(
    prefix="/symlinks",
    tags=["Symlinks"],
)

# ⚠️ Ne JAMAIS réassigner cette liste : toujours modifier en place (clear/extend, slices, etc.)
symlink_store = []
VALID_MEDIA_EXTS = {".mkv", ".mp4", ".m4v"}

# ---------------------------
# Utilitaires chemins & roots
# ---------------------------

# -----------------
# Utilise nom container sonarr pour recuperer sous domaine
# -----------------

client = docker.from_env()

def get_traefik_host(container_name: str) -> str | None:
    try:
        container = client.containers.get(container_name)
        labels = container.attrs["Config"]["Labels"]
        for k, v in labels.items():
            if k.startswith("traefik.http.routers.") and ".rule" in k:
                if v.startswith("Host("):
                    return v.replace("Host(`", "").replace("`)", "")
        return None
    except Exception as e:
        print(f"Erreur: {e}")
        return None

def is_relative_to(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False

def get_roots() -> list[Path]:
    return [Path(ld.path).resolve() for ld in config_manager.config.links_dirs]

def get_root_map() -> dict[str, Path]:
    return {Path(ld.path).name.lower(): Path(ld.path).resolve() for ld in config_manager.config.links_dirs}

def roots_for_manager(manager_name: str) -> list[Path]:
    """Racines filtrées par manager (ex: 'sonarr' => /Medias/shows)."""
    return [Path(ld.path).resolve() for ld in config_manager.config.links_dirs if ld.manager == manager_name]

def filter_items_by_folder(items, folder: Optional[str]):
    if not folder:
        return items
    roots = get_roots()
    root_map = get_root_map()
    key = folder.lower()
    if key in root_map:
        folder_paths = [root_map[key]]
    else:
        folder_paths = [(p / folder).resolve() for p in roots]
    return [i for i in items if any(is_relative_to(Path(i["symlink"]), fp) for fp in folder_paths)]

# -------------
# Settings manager
# -------------

@router.get("/config", response_model=SymlinkConfig)
async def get_symlinks_config():
    """Récupérer la config symlinks depuis config.json"""
    return config_manager.config

@router.post("/config", response_model=dict)
async def set_symlinks_config(new_config: SymlinkConfig):
    """Sauvegarder une nouvelle config symlinks dans config.json"""
    try:
        config_manager.config = SymlinkConfig.model_validate(new_config.model_dump())
        config_manager.save()
        return {"message": "✅ Config mise à jour avec succès !"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# -------------
# Scan symlinks
# -------------
def scan_symlinks(radarr: RadarrService | None = None):
    """
    Scan des symlinks + enrichissement Radarr optimisé :
    - Un seul appel API pour récupérer tous les films
    - Dictionnaire {normalized_title: movie, normalized_title+year: movie} en RAM
    - Recherche instantanée pour chaque symlink
    - Posters Radarr corrigés avec URL publique (Traefik)
    - Ajout de year, rating, overview, genres
    """
    global radarr_index
    config = config_manager.config
    links_dirs = [(Path(ld.path).resolve(), ld.manager) for ld in config.links_dirs]
    mount_dirs = [Path(d).resolve() for d in config.mount_dirs]

    for links_dir, _ in links_dirs:
        if not links_dir.exists():
            raise RuntimeError(f"Dossier introuvable : {links_dir}")
    for mount_dir in mount_dirs:
        if not mount_dir.exists():
            raise RuntimeError(f"Dossier introuvable : {mount_dir}")

    # --- Construire index Radarr (si dispo)
    radarr_index = {}
    radarr_host = None
    if radarr:
        try:
            all_movies = radarr.get_all_movies()
            logger.info(f"📚 {len(all_movies)} films récupérés depuis Radarr")
            for movie in all_movies:
                cleaned = clean_movie_name(movie.get("title", ""))
                norm = normalize_name(cleaned)
                radarr_index[norm] = movie
                year = movie.get("year")
                if year:
                    radarr_index[f"{norm}{year}"] = movie
            radarr_host = get_traefik_host("radarr")
        except Exception as e:
            logger.error(f"❌ Impossible de récupérer la liste complète des films Radarr : {e}")

    symlinks_list = []
    for links_dir, manager in links_dirs:
        for symlink_path in links_dir.rglob("*"):
            if symlink_path.is_symlink():
                try:
                    target_path = symlink_path.resolve(strict=True)
                except FileNotFoundError:
                    target_path = symlink_path.resolve(strict=False)

                matched_mount = None
                relative_target = None
                for mount_dir in mount_dirs:
                    try:
                        relative_target = target_path.relative_to(mount_dir)
                        matched_mount = mount_dir
                        break
                    except ValueError:
                        continue
                full_target = str(matched_mount / relative_target) if matched_mount else str(target_path)

                try:
                    relative_path = str(symlink_path.resolve().relative_to(links_dir))
                except Exception:
                    relative_path = str(symlink_path).replace(str(links_dir) + "/", "")

                stat = symlink_path.lstat()
                created_at = datetime.fromtimestamp(stat.st_mtime).isoformat()

                item = {
                    "symlink": str(symlink_path),
                    "relative_path": relative_path,
                    "target": full_target,
                    "target_exists": target_path.exists(),
                    "manager": manager,
                    "type": manager,
                    "created_at": created_at
                }

                # --- Enrichissement Radarr via index
                if manager == "radarr" and radarr_index:
                    raw_name = symlink_path.parent.name
                    cleaned = clean_movie_name(raw_name)
                    norm_cleaned = normalize_name(cleaned)
                    movie = radarr_index.get(norm_cleaned)
                    if movie:
                        # Poster
                        poster_url = None
                        if "images" in movie:
                            poster = next(
                                (img.get("url") for img in movie["images"] if img.get("coverType") == "poster"),
                                None
                            )
                            if poster:
                                if poster.startswith("/"):
                                    if radarr_host:
                                        poster_url = f"https://{radarr_host}{poster}"
                                    else:
                                        poster_url = f"{radarr.base_url.rstrip('/')}{poster}"
                                else:
                                    poster_url = poster

                        # Rating (TMDB si dispo)
                        rating = None
                        ratings = movie.get("ratings") or {}
                        if isinstance(ratings, dict):
                            tmdb_rating = ratings.get("tmdb")
                            if isinstance(tmdb_rating, dict):
                                rating = tmdb_rating.get("value")

                        item.update({
                            "id": movie.get("id"),
                            "title": movie.get("title"),
                            "tmdbId": movie.get("tmdbId"),
                            "poster": poster_url,
                            "year": movie.get("year"),
                            "rating": rating,
                            "overview": movie.get("overview"),
                            "genres": movie.get("genres") or []
                        })

                symlinks_list.append(item)

    target_counts = Counter(item["target"] for item in symlinks_list if item["target_exists"])
    results = [{
        **item,
        "ref_count": target_counts.get(item["target"], 0) if item["target_exists"] else 0
    } for item in symlinks_list]

    logger.success(f"{len(results)} symlinks scannés (Radarr index {len(radarr_index)})")
    return results

# ---------------
# Liste symlinks
# ---------------
@router.get("")
def list_symlinks(
    page: int = Query(1, gt=0),
    limit: int = Query(50, gt=0, le=1000),
    search: Optional[str] = None,
    sort: Optional[str] = "symlink",
    order: Optional[str] = "asc",
    orphans: Optional[bool] = False,
    folder: Optional[str] = None,
    all: bool = False
):
    """
    Liste des symlinks filtrés / paginés.
    - folder = nom de racine (ex: "movies" ou "shows")
    - sort = symlink | target | ref_count | created_at
    - order = asc | desc
    - orphans = liens brisés (cible absente) → basé UNIQUEMENT sur target_exists=False
    """
    try:
        items = list(symlink_store or [])
    except Exception:
        logger.exception("💥 Impossible de lire symlink_store")
        return {
            "total": 0,
            "page": 1,
            "limit": limit,
            "data": [],
            "orphaned": 0,
            "unique_targets": 0
        }

    try:
        # 📂 Filtre par dossier racine
        if folder:
            config = config_manager.config
            base_paths = [Path(ld.path) for ld in config.links_dirs if Path(ld.path).name == folder]
            if not base_paths:
                logger.warning(f"⚠️ Racine inconnue: {folder}")
                items = []
            else:
                folder_strs = [str(bp.resolve()) for bp in base_paths]
                items = [i for i in items if any(i["symlink"].startswith(fs) for fs in folder_strs)]

        # 🔍 Filtre recherche
        if search:
            s_low = search.lower()
            items = [
                i for i in items
                if s_low in i.get("symlink", "").lower()
                or s_low in i.get("target", "").lower()
                or s_low in str(i.get("title", "")).lower()
                or s_low == str(i.get("id", ""))
                or s_low == str(i.get("tmdbId", ""))
            ]

        # ⚠️ Filtre orphelins (cible absente)
        if orphans:
            items = [i for i in items if not i.get("target_exists", True)]

        # ↕️ Tri
        reverse = order.lower() == "desc"
        if sort in {"symlink", "target", "ref_count", "created_at"}:
            try:
                if sort == "created_at":
                    items.sort(
                        key=lambda x: datetime.fromisoformat(x.get("created_at"))
                        if x.get("created_at") else datetime.min,
                        reverse=reverse
                    )
                else:
                    items.sort(key=lambda x: x.get(sort), reverse=reverse)
            except Exception as e:
                logger.warning(f"⚠️ Tri impossible sur {sort} : {e}")

        total = len(items)

        # 📑 Pagination
        if not all:
            start = (page - 1) * limit
            end = start + limit
            paginated = items[start:end]
        else:
            paginated = items

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": paginated,
            "orphaned": sum(1 for i in items if not i.get("target_exists", True)),
            "unique_targets": len(set(i["target"] for i in items if i.get("target_exists", True))),
        }

    except Exception:
        logger.exception("💥 Erreur interne dans /symlinks")
        return {
            "total": 0,
            "page": 1,
            "limit": limit,
            "data": [],
            "orphaned": 0,
            "unique_targets": 0
        }

# -----
# Scan
# -----
@router.post("/scan")
async def trigger_scan(radarr: RadarrService = Depends(RadarrService)):
    global symlink_store
    try:
        logger.info("🚀 [SCAN] Début du scan symlinks")
        scanned = await run_in_threadpool(scan_symlinks, radarr)

        # ✅ IMPORTANT : modifier la liste en place pour conserver la référence partagée
        symlink_store.clear()
        symlink_store.extend(scanned)

        payload = {
            "event": "scan_completed",
            "count": len(symlink_store),
        }
        sse_manager.publish_event("symlink_update", payload)

        return {
            "message": "Scan terminé",
            "count": len(symlink_store),
            "data": symlink_store,
        }
    except Exception as e:
        logger.error(f"💥 Erreur scan: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ---
# SSE
# ---
@router.get("/events")
async def get_events():
    async def event_generator():
        async for message in sse_manager.subscribe():
            yield message

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Transfer-Encoding": "chunked",
        },
    )

# -----------------------
# Racines (pour le front)
# -----------------------
@router.get("/folders")
def list_root_folders():
    """
    Renvoie uniquement les noms des dossiers racines définis dans config.links_dirs
    Exemple: ["movies", "shows"]
    """
    try:
        config = config_manager.config
        roots = []
        for ld in config.links_dirs:
            path = Path(ld.path)
            if path.exists():
                roots.append(path.name)
        return roots
    except Exception:
        logger.exception("💥 Erreur récupération dossiers racines")
        return []

# --------------------------
# Lien Radarr (UI publique)
# --------------------------
@router.get("/get-radarr-url/{symlink_path:path}")
async def get_radarr_movie_url(
    symlink_path: str,
    radarr: RadarrService = Depends(RadarrService)
):
    """
    Renvoie l'URL publique du film dans Radarr (interface web), via Traefik.
    """
    raw_name = Path(symlink_path).stem
    cleaned = clean_movie_name(raw_name)

    movie = radarr.get_movie_by_clean_title(cleaned)
    if not movie:
        raise HTTPException(status_code=404, detail="Film introuvable dans Radarr")

    title_slug = movie.get("titleSlug")
    if not title_slug:
        raise HTTPException(status_code=500, detail="Champ titleSlug manquant dans la réponse Radarr")

    host = get_traefik_host("radarr")
    if not host:
        raise HTTPException(status_code=500, detail="Impossible de déterminer l'URL publique Radarr")

    url = f"https://{host}/movie/{title_slug}"
    logger.debug(f"🔗 Radarr URL: {url}")
    return {"url": url, "title": movie["title"]}

# --------------------------
# ID + Poster Radarr (proxy)
# --------------------------
@router.get("/get-radarr-id/{movie_folder}")
async def get_radarr_id(
    movie_folder: str,
    radarr: RadarrService = Depends(RadarrService)
):
    try:
        logger.debug("🎬 get-radarr-id appelé avec: {}", movie_folder)
        movie = radarr.get_movie_by_clean_title(movie_folder)

        if not movie:
            logger.warning("❌ Aucun film trouvé pour {}", movie_folder)
            raise HTTPException(status_code=404, detail=f"Film non trouvé: {movie_folder}")

        host = get_traefik_host("radarr")
        if not host:
            raise HTTPException(status_code=500, detail="Impossible de déterminer l'URL publique Radarr")

        poster_url = None
        if "images" in movie:
            poster = next((img.get("url") for img in movie["images"] if img.get("coverType") == "poster"), None)
            if poster:
                poster_url = f"https://{host}{poster}"

        return {
            "id": movie.get("id"),
            "title": movie.get("title"),
            "poster": poster_url,
        }

    except Exception as e:
        logger.error("💥 Erreur get-radarr-id pour {}: {}", movie_folder, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# Suppression (Radarr)
# -------------------------
@router.post("/delete_broken")
async def delete_broken_symlinks(
    folder: Optional[str] = None,
    radarr: RadarrService = Depends(RadarrService)
):
    logger.info("🚀 Suppression en masse des symlinks cassés demandée (Radarr)")

    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    try:
        # Récupère uniquement les racines Radarr
        roots = [
            Path(ld.path).resolve()
            for ld in config_manager.config.links_dirs
            if ld.manager == "radarr"
        ]
        root_map = {
            Path(ld.path).name.lower(): Path(ld.path).resolve()
            for ld in config_manager.config.links_dirs
            if ld.manager == "radarr"
        }
        logger.debug(f"📁 Racines Radarr: {roots}")
    except Exception as e:
        logger.error(f"❌ Impossible de lire links_dirs : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Configuration invalide")

    if not roots:
        logger.warning("⚠️ Aucune racine Radarr trouvée")
        return {"message": "Aucune racine Radarr trouvée", "deleted": 0}

    def is_relative_to(child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False

    items = list(symlink_store)
    logger.debug(f"📦 Cache actuel: {len(items)} symlinks en mémoire")

    # 📂 Filtre par dossier si précisé
    if folder:
        key = folder.lower()
        if key in root_map:
            folder_paths = [root_map[key]]
        else:
            folder_paths = [(r / folder) for r in roots]

        before = len(items)
        items = [
            i for i in items
            if any(is_relative_to(Path(i["symlink"]), fp) for fp in folder_paths)
        ]
        logger.debug(f"📁 Filtrage sur '{folder}' — {before} → {len(items)} éléments restants")

    # 🔎 Ne garder que les symlinks cassés (cible absente)
    broken_symlinks = [
        i for i in items
        if not i.get("target_exists", True)
        and any(is_relative_to(Path(i["symlink"]), r) for r in roots)
    ]

    logger.info(f"🧹 {len(broken_symlinks)} symlinks détectés comme cassés (avant suppression)")

    if not broken_symlinks:
        return {"message": "Aucun symlink cassé à supprimer", "deleted": 0}

    deleted_count = 0
    errors: list[str] = []

    for item in broken_symlinks:
        try:
            symlink_path = Path(item["symlink"])
            logger.debug(f"➡️ Traitement du symlink cassé: {symlink_path} | cible={item['target']}")

            # Vérification stricte : c'est bien un symlink sous une racine Radarr
            if not any(is_relative_to(symlink_path, r) for r in roots):
                logger.warning(f"⛔ Chemin interdit (hors racines Radarr) : {symlink_path}")
                continue

            if not symlink_path.is_symlink():
                logger.warning(f"⚠️ Pas un symlink valide (fichier disparu ou transformé) : {symlink_path}")
                continue

            # 🗑️ Suppression physique
            symlink_path.unlink(missing_ok=True)
            logger.info(f"🗑️ Supprimé physiquement : {symlink_path}")
            deleted_count += 1

            # 🎬 Identifier le film associé
            raw_name = symlink_path.parent.name
            cleaned = clean_movie_name(raw_name)
            logger.debug(f"🎬 Nettoyage nom film: brut='{raw_name}' → clean='{cleaned}'")

            match = radarr.get_movie_by_clean_title(cleaned)
            if not match:
                logger.warning(f"❗ Aucun film trouvé dans Radarr pour : {cleaned}")
                continue

            movie_id = match["id"]
            logger.debug(f"🎬 Film associé trouvé: {match.get('title')} (ID={movie_id})")

            try:
                radarr.refresh_movie(movie_id)
                await asyncio.sleep(2)
                radarr.search_missing_movie(movie_id)
                logger.info(f"📥 Recherche relancée dans Radarr pour : {match.get('title')}")
            except Exception as e:
                err_msg = f"{symlink_path}: action Radarr échouée — {e}"
                logger.error(err_msg)
                errors.append(err_msg)

            # 📡 Event SSE incrémental
            payload = {
                "event": "symlink_removed",
                "path": str(symlink_path),
                "movie": {
                    "id": movie_id,
                    "title": match.get("title", "Inconnu")
                }
            }
            logger.debug(f"📡 Envoi event SSE symlink_removed: {payload}")
            sse_manager.publish_event("symlink_update", payload)

        except Exception as e:
            err_msg = f"💥 Erreur {item['symlink']}: {str(e)}"
            logger.error(err_msg, exc_info=True)
            errors.append(err_msg)

    return {
        "message": f"{deleted_count} symlinks cassés supprimés",
        "deleted": deleted_count,
        "errors": errors
    }

@router.delete("/delete/{symlink_path:path}")
async def delete_symlink(
    symlink_path: str,
    root: Optional[str] = Query(None, description="Nom de la racine (ex: movies)"),
    radarr: RadarrService = Depends(RadarrService)
):
    """
    Supprime un symlink (Radarr) :
    - Supprime physiquement le lien symbolique si présent (même si cible orpheline)
    - Essaie d’identifier le film Radarr associé (par titre ou imdb/tmdb)
    - Relance refresh + search Radarr si trouvé
    - Publie un event SSE informatif
    """
    try:
        # 🔎 Identifier racines Radarr
        if root:
            root_paths = {
                Path(ld.path).name: Path(ld.path).resolve()
                for ld in config_manager.config.links_dirs if ld.manager == "radarr"
            }
            if root not in root_paths:
                raise HTTPException(status_code=400, detail="Racine Radarr inconnue")
            roots = [root_paths[root]]
        else:
            roots = [Path(ld.path).resolve() for ld in config_manager.config.links_dirs if ld.manager == "radarr"]

        candidate_abs = None
        for r in roots:
            test_path = (r / symlink_path)
            try:
                test_path.relative_to(r)
                if test_path.is_symlink():
                    candidate_abs = test_path
                    break
            except ValueError:
                continue

        if not candidate_abs:
            raise HTTPException(status_code=404, detail="Symlink introuvable dans Radarr")

        # 🗑️ Suppression du symlink même si la cible est orpheline
        if candidate_abs.is_symlink():
            try:
                candidate_abs.unlink(missing_ok=True)  # ⚡ Python 3.8+ → évite crash si déjà manquant
                logger.info(f"🗑️ Symlink supprimé : {candidate_abs}")
            except Exception as e:
                logger.warning(f"⚠️ Impossible de supprimer le symlink {candidate_abs} : {e}")

        # 🎬 Essayer d’identifier le film
        movie = None
        raw_name = candidate_abs.parent.name
        cleaned = clean_movie_name(raw_name)

        try:
            movie = radarr.get_movie_by_clean_title(cleaned)
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer le film '{cleaned}' via Radarr : {e}")

        # Fallback : tenter par imdbId si présent dans le nom
        if not movie:
            imdb_match = re.search(r"\{imdb-(tt\d+)\}", symlink_path)
            if imdb_match:
                imdb_id = imdb_match.group(1)
                try:
                    movie = radarr.get_movie_by_imdb(imdb_id)
                    if movie:
                        logger.info(f"🎯 Film trouvé via IMDb {imdb_id} : {movie.get('title')}")
                except Exception:
                    pass

        if movie:
            movie_id = movie["id"]
            radarr.refresh_movie(movie_id)
            await asyncio.sleep(2)
            radarr.search_missing_movie(movie_id)
            logger.info(f"📥 Recherche relancée pour {movie.get('title')}")

            payload = {
                "event": "symlink_removed",
                "path": str(candidate_abs),
                "movie": {
                    "id": movie_id,
                    "title": movie.get("title")
                }
            }
            sse_manager.publish_event("symlink_update", payload)

            return {"message": f"✅ Symlink supprimé et recherche relancée pour {movie.get('title')}"}
        else:
            logger.warning(f"⚠️ Aucun film associé trouvé pour {candidate_abs}")
            return {"message": "✅ Symlink supprimé (film non retrouvé dans Radarr, aucune recherche lancée)"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"💥 Erreur suppression symlink : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erreur interne : {e}")

# --------------------------
# (Sonarr) –> seasonarr
# --------------------------

@router.get("/get-sonarr-id/{symlink_path:path}")
async def get_sonarr_series_id_only(
    symlink_path: str,
    sonarr: SonarrService = Depends(SonarrService)
):
    """
    Récupère les infos principales d'une série + saison/épisode à partir d’un symlink.
    """
    logger.debug(f"📥 Chemin reçu : {symlink_path}")

    try:
        parts = Path(symlink_path).parts
        if not parts:
            raise HTTPException(status_code=400, detail="Chemin invalide")

        # --- Identifier le bon dossier "série"
        raw_series = None
        if "Medias" in parts:
            try:
                show_idx = parts.index("Medias") + 2
                raw_series = parts[show_idx]
            except Exception:
                raw_series = parts[0]
        else:
            raw_series = parts[0]

        cleaned = clean_series_name(raw_series)
        logger.debug(f"🔍 Série nettoyée : {cleaned}")

        # --- Trouver la série dans Sonarr
        series = sonarr.get_series_by_clean_title(cleaned)
        if not series:
            logger.warning(f"❌ Série '{cleaned}' introuvable dans Sonarr")
            raise HTTPException(status_code=404, detail="Série introuvable dans Sonarr")

        # --- Poster (proxy interne)
        poster_url = None
        poster = next(
            (img for img in series.get("images", []) if img.get("coverType") == "poster"),
            None
        )
        if poster:
            poster_url = f"/api/v1/proxy-image?url={poster['url']}&instance_id=1"

        # --- Extraire Saison / Épisode depuis le nom du fichier
        filename = Path(symlink_path).name
        match = re.search(r"(?:S(\d{1,2})E(\d{1,2})|(\d{1,2})x(\d{1,2}))", filename, re.IGNORECASE)

        season_num, episode_num, episode_title, downloaded = None, None, None, None

        if match:
            if match.group(1) and match.group(2):
                season_num = int(match.group(1))
                episode_num = int(match.group(2))
            else:
                season_num = int(match.group(3))
                episode_num = int(match.group(4))

            logger.debug(f"🎯 Détecté S{season_num:02d}E{episode_num:02d}")

            # --- Chercher l’épisode exact dans Sonarr
            episodes = sonarr.get_all_episodes(series["id"])
            ep = next(
                (e for e in episodes if e["seasonNumber"] == season_num and e["episodeNumber"] == episode_num),
                None
            )
            if ep:
                episode_title = ep.get("title")
                downloaded = ep.get("hasFile", False)

        logger.info(f"🔑 Série trouvée : {series['title']} (ID: {series['id']})")

        return {
            "id": series["id"],
            "title": series["title"],
            "poster": poster_url,
            "year": series.get("year"),
            "status": series.get("status"),
            "network": series.get("network"),
            "genres": series.get("genres", []),
            "season": season_num,
            "episode": episode_num,
            "episodeTitle": episode_title,
            "downloaded": downloaded,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"💥 Erreur lors de la récupération Sonarr : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erreur interne : {e}")

# --------------------------
# Lien Sonarr (UI publique)
# --------------------------
@router.get("/get-sonarr-url/{symlink_path:path}")
async def get_sonarr_series_url(
    symlink_path: str,
    sonarr: SonarrService = Depends(SonarrService)
):
    """
    Renvoie l'URL publique de la série dans Sonarr (interface web).
    """
    cleaned = clean_series_name(Path(symlink_path).parts[0])
    series = sonarr.get_series_by_clean_title(cleaned)

    if not series:
        raise HTTPException(status_code=404, detail="Série introuvable dans Sonarr")

    title_slug = series.get("titleSlug")
    if not title_slug:
        raise HTTPException(status_code=500, detail="Champ titleSlug manquant dans la réponse Sonarr")

    host = get_traefik_host("sonarr")  # ou "tv" selon ton conteneur
    if not host:
        raise HTTPException(status_code=500, detail="Impossible de déterminer l'URL publique Sonarr")

    url = f"https://{host}/series/{title_slug}"
    return {"url": url, "title": series["title"]}

# --------------------------
# Suppression (Sonarr) – unitaire
# --------------------------
@router.delete("/delete-sonarr/{symlink_path:path}")
async def delete_symlink_sonarr(
    symlink_path: str,
    root: Optional[str] = Query(None, description="Nom de la racine (ex: shows)"),
    sonarr: SonarrService = Depends(SonarrService)
):
    """
    Supprime un symlink Sonarr et relance la recherche :
    - si SxxEyy détecté → recherche épisode précis
    - si Sxx seulement → recherche saison
    - sinon → recherche globale série
    """
    logger.debug("🔧 Début suppression symlink (Sonarr)")
    logger.debug(f"📥 Chemin relatif reçu : {symlink_path}")

    try:
        # 1️⃣ Récupérer racines Sonarr
        if root:
            root_paths = {
                Path(ld.path).name.lower(): Path(ld.path)
                for ld in config_manager.config.links_dirs
                if ld.manager == "sonarr"
            }
            if root.lower() not in root_paths:
                logger.warning(f"❌ Racine '{root}' introuvable dans config Sonarr")
                raise HTTPException(status_code=400, detail="Racine Sonarr inconnue")
            roots = [root_paths[root.lower()]]
        else:
            roots = [Path(ld.path) for ld in config_manager.config.links_dirs if ld.manager == "sonarr"]

        # 2️⃣ Vérifier symlink
        candidate_abs = None
        for r in roots:
            test_path = r / symlink_path
            try:
                test_path.relative_to(r)
                candidate_abs = test_path
                break
            except ValueError:
                continue

        if not candidate_abs:
            logger.warning(f"❌ Chemin invalide ou introuvable : {symlink_path}")
            raise HTTPException(status_code=404, detail="Symlink introuvable")

        # 3️⃣ Suppression physique
        try:
            if candidate_abs.exists() or candidate_abs.is_symlink():
                if candidate_abs.is_symlink():
                    candidate_abs.unlink()
                    logger.info(f"🗑️ Symlink supprimé : {candidate_abs}")
                else:
                    logger.warning(f"⚠️ Pas un symlink : {candidate_abs}")
            else:
                logger.warning(f"⚠️ Déjà inexistant : {candidate_abs}")
        except Exception as e:
            logger.error(f"💥 Erreur suppression symlink : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Erreur suppression symlink : {e}")

        # 4️⃣ Identifier la série
        cleaned = clean_series_name(Path(symlink_path).parts[0])
        series = sonarr.get_series_by_clean_title(cleaned)
        if not series:
            logger.warning(f"❗ Série '{cleaned}' introuvable dans Sonarr")
            return {"message": f"✅ Symlink supprimé (aucune série trouvée : {cleaned})"}

        series_id = series["id"]

        # 5️⃣ Déterminer saison / épisode
        filename = Path(symlink_path).name
        episode_match = re.search(r"[Ss](\d{1,2})[Ee](\d{1,2})", filename)
        season_match = re.search(r"[Ss](\d{1,2})", filename)

        try:
            if episode_match:
                season = int(episode_match.group(1))
                episode = int(episode_match.group(2))
                sonarr.refresh_series(series_id)
                await asyncio.sleep(1)

                episodes = sonarr.get_all_episodes(series_id)
                ep = next(
                    (e for e in episodes if e["seasonNumber"] == season and e["episodeNumber"] == episode),
                    None
                )
                if ep:
                    sonarr.search_season(series_id, season)  # Sonarr ne supporte pas recherche épisode direct
                    logger.info(f"📥 Recherche relancée pour {series['title']} S{season:02d}E{episode:02d}")
            elif season_match:
                season = int(season_match.group(1))
                sonarr.refresh_series(series_id)
                await asyncio.sleep(1)
                sonarr.search_season(series_id, season)
                logger.info(f"📥 Recherche relancée pour saison {season} de {series['title']}")
            else:
                sonarr.refresh_series(series_id)
                await asyncio.sleep(1)
                sonarr.search_missing_episodes(series_id)
                logger.info(f"📥 Recherche relancée pour la série entière {series['title']}")
        except Exception as e:
            logger.error(f"💥 Erreur recherche Sonarr : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Erreur recherche Sonarr : {e}")

        return {"message": f"✅ Symlink supprimé et recherche relancée pour '{series['title']}'"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"💥 Erreur inattendue delete_symlink_sonarr : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erreur interne : {e}")

# ------------------------------------
# Suppression en masse (Sonarr, séries)
# ------------------------------------
@router.post("/delete_broken_sonarr")
async def delete_broken_sonarr_symlinks(
    folder: Optional[str] = None,
    sonarr: SonarrService = Depends(SonarrService)
):
    logger.info("🚀 Suppression en masse des symlinks Sonarr cassés demandée")

    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    # 📁 Racines Sonarr uniquement (même logique que Radarr)
    try:
        roots = [
            Path(ld.path).resolve()
            for ld in config_manager.config.links_dirs
            if getattr(ld, "manager", "") == "sonarr"
        ]
        root_map = {
            Path(ld.path).name.lower(): Path(ld.path).resolve()
            for ld in config_manager.config.links_dirs
            if getattr(ld, "manager", "") == "sonarr"
        }
        logger.debug(f"📁 Racines Sonarr détectées : {roots}")
    except Exception as e:
        logger.error(f"❌ Impossible de lire links_dirs : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Configuration invalide")

    if not roots:
        logger.warning("⚠️ Aucun dossier racine Sonarr trouvé")
        return {"message": "Aucune racine Sonarr trouvée", "deleted": 0}

    def is_relative_to(child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
            return True
        except Exception:
            return False

    items = list(symlink_store)
    logger.debug(f"📦 Total symlinks en cache : {len(items)}")

    # 🔍 Filtrage par dossier (identique à Radarr)
    if folder:
        key = folder.lower()
        if key in root_map:
            folder_paths = [root_map[key]]
        else:
            folder_paths = [(r / folder) for r in roots]

        # Debug ciblé
        logger.debug(f"🧭 folder='{folder}' | folder_paths={folder_paths}")
        _sample = items if len(items) <= 200 else items[:200]
        for i in _sample:
            child = Path(i["symlink"])
            for fp in folder_paths:
                try:
                    _ = is_relative_to(child, fp)
                except Exception as e:
                    logger.debug(f"TEST_ERROR child={child} parent={fp} err={e}")

        before_count = len(items)
        items = [
            i for i in items
            if any(is_relative_to(Path(i["symlink"]), fp) for fp in folder_paths)
        ]
        logger.debug(f"📁 Filtrage sur '{folder}' — {before_count} → {len(items)} éléments restants")

    # 🎯 Ne garder que les symlinks cassés ET sous les racines Sonarr
    before_filter = len(items)
    broken_symlinks = [
        i for i in items
        if i.get("ref_count", 0) == 0 and any(is_relative_to(Path(i["symlink"]), r) for r in roots)
    ]
    logger.debug(f"🧹 Filtre symlinks cassés : {before_filter} → {len(broken_symlinks)}")

    if not broken_symlinks:
        return {"message": "Aucun symlink cassé Sonarr à supprimer", "deleted": 0}

    logger.info(f"🔍 {len(broken_symlinks)} symlinks Sonarr cassés à traiter")

    deleted_count = 0
    errors: list[str] = []

    # 📚 Récup liste séries Sonarr une seule fois + index normalisé
    try:
        all_series = sonarr.get_all_series()
        logger.debug(f"📚 {len(all_series)} séries récupérées depuis Sonarr")
    except Exception as e:
        logger.error(f"❌ Erreur récupération séries Sonarr : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erreur récupération séries Sonarr")

    def _norm(s: str) -> str:
        return normalize_name(clean_series_name(s or ""))

    series_index = {_norm(s.get("title", "")): s for s in all_series}

    for item in broken_symlinks:
        try:
            symlink_path = Path(item["symlink"])

            if not any(is_relative_to(symlink_path, r) for r in roots):
                logger.warning(f"⛔ Chemin interdit (hors racines Sonarr) : {symlink_path}")
                continue

            if not symlink_path.is_symlink():
                continue

            # 🧹 Suppression physique
            logger.debug(f"🧹 Suppression du symlink : {symlink_path}")
            symlink_path.unlink()
            logger.info(f"🗑️ Supprimé : {symlink_path}")
            deleted_count += 1

            # 📂 Identifier la série
            series_dir = symlink_path.parent.parent
            raw_series_name = series_dir.name
            norm_cleaned = _norm(raw_series_name)

            match = series_index.get(norm_cleaned)
            if not match:
                match = next(
                    (s for k, s in series_index.items()
                     if k == norm_cleaned or k in norm_cleaned or norm_cleaned in k),
                    None
                )

            if not match:
                logger.warning(f"❗ Aucune série trouvée pour : {raw_series_name}")
                continue

            series_id = match.get("id")
            logger.info(f"📺 Série trouvée : {match.get('title', raw_series_name)} (ID={series_id})")

            try:
                sonarr.refresh_series(series_id)
                await asyncio.sleep(2)
            except Exception as e:
                err_msg = f"{symlink_path}: refresh Sonarr échoué — {e}"
                logger.error(err_msg)
                errors.append(err_msg)

            # 📂 Vérifie si la saison est vide
            season_dir = symlink_path.parent
            valid_exts = {".mkv", ".mp4", ".m4v"}
            try:
                if season_dir.exists() and season_dir.is_dir():
                    remaining = [
                        f for f in season_dir.iterdir()
                        if f.suffix.lower() in valid_exts and f.exists()
                    ]
                    logger.debug(f"📂 Fichiers restants dans {season_dir} : {[f.name for f in remaining]}")
                else:
                    logger.warning(f"⚠️ Saison introuvable ou inaccessible : {season_dir}")
                    remaining = None
            except Exception as e:
                logger.warning(f"⚠️ Erreur lors du scan du dossier de saison : {e}")
                remaining = None

            if remaining is not None and not remaining:
                match_season = re.search(r"(\d{1,2})", season_dir.name)
                if match_season:
                    season_number = int(match_season.group(1))
                    logger.debug(f"🔢 Numéro de saison extrait : {season_number}")

                    try:
                        async with httpx.AsyncClient(timeout=20.0) as client:
                            response = await client.post(
                                "http://localhost:8080/api/v1/symlinks/delete-sonarr-season",
                                params={"series_name": clean_series_name(raw_series_name), "season_number": season_number}
                            )
                            if response.status_code != 200:
                                logger.error(f"❌ Appel API delete-sonarr-season échoué : {response.text}")
                            else:
                                logger.info(f"✅ Suppression de la saison {season_number} pour {raw_series_name}")
                    except Exception as e:
                        logger.error(f"❌ Erreur appel API : {e}")

        except Exception as e:
            msg = f"Erreur {item['symlink']}: {str(e)}"
            logger.error(msg, exc_info=True)
            errors.append(msg)

    try:
        sse_manager.publish_event("symlink_update", json.dumps({"event": "refreshed"}))
    except Exception as e:
        logger.warning(f"⚠️ Impossible d'envoyer l'événement SSE : {e}")

    return {
        "message": f"{deleted_count} symlinks Sonarr cassés supprimés",
        "deleted": deleted_count,
        "errors": errors
    }

# ---------------------------------------
# Réinitialisation d'une saison Sonarr
# ---------------------------------------
@router.post("/delete-sonarr-season")
async def delete_sonarr_season(
    series_name: str = Query(..., description="Nom complet de la série"),
    season_number: int = Query(..., description="Numéro de la saison"),
    sonarr: SonarrService = Depends(SonarrService)
):
    logger.info(f"🔁 [delete-sonarr-season] Traitement saison {season_number} pour : {series_name}")

    cleaned_series = clean_series_name(series_name)
    normalized_cleaned = normalize_name(cleaned_series)

    try:
        all_series = sonarr.get_all_series()
        logger.debug(f"📚 {len(all_series)} séries récupérées depuis Sonarr")
    except Exception as e:
        logger.error(f"❌ Erreur récupération séries Sonarr : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erreur récupération séries Sonarr")

    def match_title(s):
        norm = normalize_name(s.get("title", ""))
        return norm == normalized_cleaned or norm in normalized_cleaned or normalized_cleaned in norm

    match = next((s for s in all_series if match_title(s)), None)
    if not match:
        logger.warning(f"❗ Série introuvable dans Sonarr pour : {cleaned_series}")
        raise HTTPException(status_code=404, detail="Série introuvable")

    series_id = match["id"]
    logger.info(f"📺 Série trouvée : {match['title']} (ID={series_id})")

    try:
        sonarr.refresh_series(series_id)
        await asyncio.sleep(2)
        sonarr.search_missing_episodes(series_id)
        logger.info(f"📥 Recherche manuelle lancée pour : {match['title']}")
        await asyncio.sleep(3)

        try:
            sonarr_roots = [
                Path(ld.path) for ld in config_manager.config.links_dirs
                if getattr(ld, "manager", "") == "sonarr"
            ]
        except Exception as e:
            logger.error(f"❌ Impossible de lire links_dirs : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Configuration invalide")

        if not sonarr_roots:
            raise HTTPException(status_code=404, detail="Aucune racine Sonarr trouvée")

        def is_under(child: Path, parent: Path) -> bool:
            try:
                child.relative_to(parent)
                return True
            except ValueError:
                return False

        series_dir = None
        for root in sonarr_roots:
            if not root.exists():
                continue
            for d in root.iterdir():
                if d.is_dir() and match_title({"title": d.name}) and is_under(d, root):
                    series_dir = d
                    break
            if series_dir:
                break

        if not series_dir:
            raise HTTPException(status_code=404, detail="Répertoire série introuvable")

        logger.debug(f"📁 Répertoire série trouvé : {series_dir}")

        season_dir = next((d for d in series_dir.glob(f"*{season_number:02d}*") if d.is_dir()), None)
        if not season_dir:
            raise HTTPException(status_code=404, detail="Répertoire saison introuvable")

        logger.debug(f"📁 Répertoire saison trouvé : {season_dir}")

        valid_exts = {".mkv", ".mp4", ".m4v"}
        remaining_files = [f for f in season_dir.iterdir() if f.is_file() and f.suffix.lower() in valid_media_exts]

        if remaining_files:
            return {
                "message": f"Recherche relancée pour la saison {season_number} de {match['title']}. Fichiers présents."
            }

        logger.warning(f"🚫 Aucun fichier vidéo trouvé dans la saison {season_number} — suppression dossiers/fichiers résiduels")
        for f in season_dir.iterdir():
            try:
                if f.is_file() or f.is_symlink():
                    f.unlink()
                elif f.is_dir():
                    shutil.rmtree(f, ignore_errors=True)
                logger.info(f"🗑️ Supprimé : {f}")
            except Exception as e:
                logger.warning(f"⚠️ Échec suppression {f} : {e}")

        sonarr.refresh_series(series_id)
        await asyncio.sleep(2)
        sonarr.search_missing_episodes(series_id)

        return {
            "message": f"✅ Saison {season_number} réinitialisée pour {match['title']} — recherche complète relancée"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"⚠️ Erreur traitement saison {number} de {series_name} : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erreur traitement saison Sonarr")

# ---------------------------------------
# Réparation des saisons manquantes (SSE)
# ---------------------------------------
@router.post("/repair-missing-seasons")
async def repair_missing_seasons(
    folder: Optional[str] = None,
    sonarr: SonarrService = Depends(SonarrService)
):
    logger.info("🛠️ Réparation des saisons manquantes demandée")

    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    try:
        sonarr_roots = [
            Path(ld.path) for ld in config_manager.config.links_dirs
            if getattr(ld, "manager", "") == "sonarr"
        ]
    except Exception as e:
        logger.error(f"❌ Impossible de lire links_dirs : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Configuration invalide")

    if not sonarr_roots:
        return {"message": "Aucune racine Sonarr trouvée", "symlinks_deleted": 0}

    def is_under(child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False

    items = list(symlink_store)
    if folder:
        folder_paths = [(root / folder) for root in sonarr_roots]
        items = [i for i in items if any(is_under(Path(i["symlink"]), fp) for fp in folder_paths)]
        logger.debug(f"📁 Filtrage sur dossier '{folder}' — {len(items)} éléments restants")

    deleted_count = 0
    errors = []

    try:
        missing_list = sonarr.get_all_series_with_missing_seasons()
    except Exception as e:
        logger.error(f"❌ Erreur récupération séries : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erreur récupération des séries avec saisons manquantes")

    for entry in missing_list:
        series_id = entry["id"]
        series_title = entry["title"]
        raw_missing_seasons = [s for s in entry.get("missing_seasons", []) if s != 0]

        if not raw_missing_seasons:
            continue

        logger.info(f"   '{series_title}' - Saisons manquantes : {raw_missing_seasons}")

        try:
            all_episodes = sonarr.get_all_episodes(series_id)
        except Exception as e:
            logger.error(f"❌ Erreur récupération épisodes pour '{series_title}': {e}", exc_info=True)
            errors.append(f"{series_title} - episodes")
            continue

        confirmed_missing = []
        for season_num in raw_missing_seasons:
            season_eps = [ep for ep in all_episodes if ep.get("seasonNumber") == season_num]
            if not season_eps:
                confirmed_missing.append(season_num)
                continue
            future_eps = [
                ep for ep in season_eps
                if ep.get("airDateUtc") and ep["airDateUtc"] > datetime.utcnow().isoformat()
            ]
            if not future_eps:
                confirmed_missing.append(season_num)

        if not confirmed_missing:
            continue

        norm_title = normalize_name(series_title)
        def match_path(path_str: str) -> bool:
            p_norm = normalize_name(path_str)
            return p_norm == norm_title or p_norm in norm_title or norm_title in p_norm

        matching_items = [i for i in items if match_path(i["symlink"])]
        if not matching_items:
            continue

        for season_num in confirmed_missing:
            logger.debug(f"🔍 Saison {season_num} pour '{series_title}' (ID={series_id})")
            pattern = f"S{season_num:02}"
            filtered_symlinks = [
                i for i in matching_items if pattern.lower() in i["symlink"].lower()
            ]

            for item in filtered_symlinks:
                symlink_path = Path(item["symlink"])

                if not any(is_under(symlink_path, root) for root in sonarr_roots):
                    continue

                try:
                    if symlink_path.exists() and symlink_path.is_symlink():
                        symlink_path.unlink()
                        logger.info(f"🗑️ Symlink supprimé : {symlink_path}")
                        deleted_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Erreur suppression symlink {symlink_path}: {e}")
                    errors.append(str(symlink_path))

            try:
                sonarr.refresh_series(series_id)
                await asyncio.sleep(2)
                sonarr.search_season(series_id=series_id, season_number=season_num)
                logger.info(f"📥 Recherche relancée pour S{season_num:02} de '{series_title}'")
            except Exception as e:
                logger.error(f"❌ Échec recherche saison {season_num} de '{series_title}' : {e}", exc_info=True)
                errors.append(f"{series_title} - S{season_num:02}")

    try:
        sse_manager.publish_event("symlink_update", json.dumps({"event": "refreshed"}))
    except Exception as e:
        logger.warning(f"⚠️ Impossible d'envoyer l'événement SSE : {e}")

    return {
        "message": "Saisons manquantes traitées",
        "symlinks_deleted": deleted_count,
        "errors": errors
    }

# -----------------
# Doublons (cibles)
# -----------------
@router.get("/duplicates")
def list_duplicates():
    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    target_map = {}
    for item in symlink_store:
        target = item["target"]
        if item.get("ref_count", 0) > 1:
            target_map.setdefault(target, []).append(item)

    duplicates = []
    for items in target_map.values():
        if len(items) > 1:
            duplicates.extend(items)

    return {"total": len(duplicates), "data": duplicates}
