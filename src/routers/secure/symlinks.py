from typing import Optional
from pathlib import Path
from collections import Counter
from fastapi import APIRouter, HTTPException, Query, Request, Depends, BackgroundTasks, Header
from fastapi.responses import StreamingResponse
from fastapi.concurrency import run_in_threadpool
from functools import partial
import json
import urllib.parse
import asyncio
import re
import os
import httpx
import docker
import threading
import uuid
from sqlalchemy.orm import Session
from integrations.seasonarr.db.database import get_db
from integrations.seasonarr.core.auth import get_current_user
from integrations.seasonarr.db.models import User
from integrations.seasonarr.services.season_it_service import SeasonItService
from integrations.seasonarr.clients.sonarr_client import SonarrClient
from integrations.seasonarr.db.models import SonarrInstance
from integrations.seasonarr.db.models import UserSettings
from integrations.seasonarr.db.database import SessionLocal
from integrations.seasonarr.db.models import SystemActivity
from datetime import datetime
from loguru import logger
from urllib.parse import unquote
from fastapi import BackgroundTasks
from program.managers.sse_manager import sse_manager
from src.services.fonctions_arrs import RadarrService, SonarrService
from program.settings.manager import config_manager
from program.settings.models import SymlinkConfig
from program.utils.text_utils import normalize_name, clean_movie_name
from program.utils.discord_notifier import send_discord_message
from concurrent.futures import ThreadPoolExecutor, as_completed
from program.file_watcher import start_symlink_watcher
from program.utils.imdb import is_missing_imdb 
from program.radarr_cache import (
    _radarr_index,
    _radarr_catalog,
    _radarr_host,
    _radarr_idx_lock,
    _build_radarr_index,
    enrich_from_radarr_index,
)
from pathlib import Path
from collections import Counter

router = APIRouter(
    prefix="/symlinks",
    tags=["Symlinks"],
)


# ⚠️ Ne JAMAIS réassigner cette liste : toujours modifier en place (clear/extend, slices, etc.)
symlink_store = []
VALID_MEDIA_EXTS = {".mkv", ".mp4", ".m4v"}

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

watcher_thread = None
broken_scan_thread = None
broken_scan_lock = threading.Lock()
broken_scan_running = False

@router.post("/config", response_model=dict)
async def set_symlinks_config(new_config: SymlinkConfig, background_tasks: BackgroundTasks):
    """Sauvegarder une nouvelle config symlinks et démarrer le watcher si nécessaire"""
    global watcher_thread
    try:
        # 1️⃣ Sauvegarder la config
        config_manager.config = SymlinkConfig.model_validate(new_config.model_dump())
        config_manager.save()

        # 2️⃣ Démarrer le watcher si pas encore actif
        if not watcher_thread or not watcher_thread.is_alive():
            def start_watcher():
                logger.info("🚀 Démarrage du symlink watcher après config")
                start_symlink_watcher()

            watcher_thread = threading.Thread(target=start_watcher, daemon=True)
            watcher_thread.start()

        return {"message": "✅ Config mise à jour avec succès ! Watcher actif 🚀"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def scan_symlinks_parallel(workers: int = 32, fast: bool = True, ultra_fast: bool = True):
    """
    ⚡ Scan des symlinks ultra-rapide.
    - `fast=True`  → désactive Path.exists()
    - `ultra_fast=True` → saute aussi lstat() (scan instantané)
    - `workers` → nombre de threads (par défaut 32)
    """
    config = config_manager.config
    links_dirs = [(Path(ld.path).resolve(), ld.manager) for ld in config.links_dirs]
    mount_dirs = [Path(d).resolve() for d in config.mount_dirs]

    for d, _ in links_dirs + [(m, "") for m in mount_dirs]:
        if not d.exists():
            raise RuntimeError(f"Dossier introuvable : {d}")

    symlinks_list = []
    tasks = []

    def process_symlink(symlink_path: str, root: Path, manager: str):
        try:
            # Lecture rapide du lien symbolique
            try:
                target_raw = os.readlink(symlink_path)
                target_path = Path(target_raw)
                if not target_path.is_absolute():
                    target_path = (Path(symlink_path).parent / target_path).resolve()
            except Exception:
                target_path = Path(symlink_path).resolve(strict=False)

            # 🔗 Adaptation mount_dir
            full_target = str(target_path)
            for mount_dir in mount_dirs:
                try:
                    relative_target = target_path.relative_to(mount_dir)
                    full_target = str(mount_dir / relative_target)
                    break
                except ValueError:
                    continue

            # Lecture stat (désactivée si ultra_fast)
            if not ultra_fast:
                stat = os.lstat(symlink_path)
                created_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
            else:
                created_at = None

            # Vérification existence cible (très lente)
            if fast or ultra_fast:
                target_exists = True
            else:
                target_exists = Path(full_target).exists()

            item = {
                "symlink": symlink_path,
                "relative_path": os.path.relpath(symlink_path, root),
                "target": full_target,
                "target_exists": target_exists,
                "manager": manager,
                "type": manager,
                "created_at": created_at,
            }

            # 🎬 Enrichissement Radarr uniquement si manager = radarr
            if manager == "radarr":
                extra = enrich_from_radarr_index(Path(symlink_path))
                if extra:
                    item.update(extra)

            return item

        except Exception as e:
            logger.debug(f"⚠️ Erreur symlink {symlink_path}: {e!r}")
            return None

    def walk_dir(root: Path, manager: str):
        stack = [root]
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for entry in it:
                        if entry.is_symlink():
                            yield entry.path, root, manager
                        elif entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
            except (PermissionError, FileNotFoundError):
                continue

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for root, manager in links_dirs:
            for symlink_path, r, m in walk_dir(root, manager):
                tasks.append(executor.submit(process_symlink, symlink_path, r, m))

        for future in as_completed(tasks):
            result = future.result()
            if result:
                symlinks_list.append(result)

    # ✅ Calcul des ref_count (rapide)
    target_counts = Counter(item["target"] for item in symlinks_list if item["target_exists"])
    for item in symlinks_list:
        item["ref_count"] = target_counts.get(item["target"], 0) if item["target_exists"] else 0

    logger.success(
        f"{len(symlinks_list)} symlinks scannés "
        f"(workers={workers}, fast={fast}, ultra_fast={ultra_fast})"
    )
    return symlinks_list

# ⚡ Remplace l’ancien alias
scan_symlinks = scan_symlinks_parallel


def run_broken_detection_job():
    """
    Lance une détection complète des symlinks brisés en arrière-plan.

    Version optimisée :
    - snapshot local du store pour éviter les itérations concurrentes
    - vérification parallèle des cibles
    - très peu d'accès DB
    - un seul commit DB
    - recalcul ref_count en mémoire uniquement
    - SSE de progression limité
    - conserve la logique watcher / monitor léger
    """
    global symlink_store, broken_scan_running

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from collections import Counter
    from pathlib import Path
    import os
    import uuid
    import time

    started_at = time.perf_counter()

    try:
        items = list(symlink_store or [])
        total_items = len(items)

        logger.info(f"🔍 [BROKEN-JOB] Début détection complète des symlinks brisés ({total_items} items)")

        sse_manager.publish_event("symlink_update", {
            "event": "broken_scan_started",
            "action": "scan",
            "message": f"Détection des symlinks brisés démarrée ({total_items} symlinks)",
            "count": total_items,
            "id": str(uuid.uuid4()),
        })

        if total_items == 0:
            sse_manager.publish_event("symlink_update", {
                "event": "broken_scan_completed",
                "action": "scan",
                "count": 0,
                "broken_count": 0,
                "newly_broken": 0,
                "newly_repaired": 0,
                "message": "Détection terminée : aucun symlink à analyser",
                "id": str(uuid.uuid4()),
            })
            logger.info("✅ [BROKEN-JOB] Aucun symlink à analyser")
            return

        def check_one(item: dict):
            symlink_str = item.get("symlink")
            if not symlink_str:
                return None

            symlink_path = Path(symlink_str)

            try:
                if not symlink_path.exists() and not symlink_path.is_symlink():
                    return {
                        "item": item,
                        "skip": True,
                        "exists": False,
                        "currently_broken": item.get("broken", False) or not item.get("target_exists", True),
                    }
            except Exception:
                return {
                    "item": item,
                    "skip": True,
                    "exists": False,
                    "currently_broken": item.get("broken", False) or not item.get("target_exists", True),
                }

            exists = False

            try:
                if symlink_path.is_symlink():
                    raw_target = os.readlink(symlink_path)
                    if not os.path.isabs(raw_target):
                        raw_target = os.path.join(str(symlink_path.parent), raw_target)
                    exists = os.path.exists(raw_target)
                else:
                    exists = symlink_path.exists()
            except Exception:
                exists = False

            currently_broken = item.get("broken", False) or not item.get("target_exists", True)

            return {
                "item": item,
                "skip": False,
                "exists": exists,
                "currently_broken": currently_broken,
            }

        max_workers = min(64, max(8, (os.cpu_count() or 8) * 4))
        logger.info(f"🔎 [BROKEN-JOB] Vérification parallèle de {total_items} symlink(s) avec {max_workers} worker(s)")

        checked = 0
        progress_step = 2000

        broken_paths = set()
        repaired_paths = set()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(check_one, item) for item in items]

            for future in as_completed(futures):
                result = future.result()
                if not result:
                    continue

                item = result["item"]
                path_str = item["symlink"]

                if not result["skip"]:
                    exists = result["exists"]
                    currently_broken = result["currently_broken"]

                    if not exists and not currently_broken:
                        broken_paths.add(path_str)
                    elif exists and currently_broken:
                        repaired_paths.add(path_str)

                checked += 1

                if checked % progress_step == 0 or checked == total_items:
                    sse_manager.publish_event("symlink_update", {
                        "event": "broken_scan_progress",
                        "action": "scan",
                        "checked": checked,
                        "total": total_items,
                        "message": f"Détection en cours : {checked}/{total_items}",
                        "id": str(uuid.uuid4()),
                    })

        db = SessionLocal()
        newly_broken = 0
        newly_repaired = 0

        try:
            broken_db_rows = db.query(SystemActivity).filter(
                SystemActivity.action == "broken"
            ).all()

            broken_db_by_path = {row.path: row for row in broken_db_rows}
            broken_db_paths = set(broken_db_by_path.keys())

            for path_str in broken_paths:
                item = next((x for x in items if x.get("symlink") == path_str), None)
                if not item:
                    continue

                if path_str not in broken_db_paths:
                    db.add(SystemActivity(
                        event="symlink_broken_manual",
                        action="broken",
                        path=path_str,
                        manager=item.get("manager", "unknown"),
                        message=f"Symlink brisé détecté manuellement : {path_str}",
                        extra={"target": item.get("target")},
                    ))

            if repaired_paths:
                db.query(SystemActivity).filter(
                    SystemActivity.action == "broken",
                    SystemActivity.path.in_(list(repaired_paths))
                ).delete(synchronize_session=False)

            db.commit()

        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

        store_index = {item["symlink"]: item for item in symlink_store if item.get("symlink")}

        for path_str in broken_paths:
            x = store_index.get(path_str)
            if x:
                if not x.get("broken", False) or x.get("target_exists", True):
                    newly_broken += 1
                x["broken"] = True
                x["target_exists"] = False
                x["ref_count"] = 0

        for path_str in repaired_paths:
            x = store_index.get(path_str)
            if x:
                if x.get("broken", False) or not x.get("target_exists", True):
                    newly_repaired += 1
                x["broken"] = False
                x["target_exists"] = True

        target_counts = Counter(
            item["target"]
            for item in symlink_store
            if item.get("target_exists", True) and item.get("target")
        )

        for item in symlink_store:
            if item.get("target_exists", True):
                item["ref_count"] = target_counts.get(item.get("target"), 0)
            else:
                item["ref_count"] = 0

        total_broken = sum(
            1 for item in symlink_store
            if item.get("broken", False) or not item.get("target_exists", True)
        )

        duration = round(time.perf_counter() - started_at, 1)

        sse_manager.publish_event("symlink_update", {
            "event": "broken_scan_completed",
            "action": "scan",
            "count": len(symlink_store),
            "broken_count": total_broken,
            "newly_broken": newly_broken,
            "newly_repaired": newly_repaired,
            "duration": duration,
            "message": (
                f"Détection terminée : {newly_broken} nouveaux brisés, "
                f"{newly_repaired} réparés, {total_broken} brisés au total "
                f"({duration}s)"
            ),
            "id": str(uuid.uuid4()),
        })

        logger.success(
            f"✅ [BROKEN-JOB] Terminé en {duration}s — "
            f"{newly_broken} nouveaux brisés, "
            f"{newly_repaired} réparés, "
            f"{total_broken} brisés au total"
        )

    except Exception as e:
        logger.error(f"💥 Erreur run_broken_detection_job: {e}", exc_info=True)

        sse_manager.publish_event("symlink_update", {
            "event": "broken_scan_failed",
            "action": "scan",
            "message": f"Échec détection symlinks brisés : {str(e)}",
            "id": str(uuid.uuid4()),
        })

    finally:
        with broken_scan_lock:
            broken_scan_running = False

# ---------------
# Liste symlinks
# --------------

@router.get("")
def list_symlinks(
    page: int = Query(1, gt=0),
    limit: int = Query(50, gt=0, le=1000),
    search: Optional[str] = None,
    sort: Optional[str] = "symlink",
    order: Optional[str] = "asc",
    orphans: Optional[bool] = False,
    folder: Optional[str] = None,
    all: bool = False,
    rename: bool = False,   # ✅ flag "rename"
):
    """
    Liste des symlinks filtrés / paginés.
    - folder  = nom de racine (ex: "movies" ou "shows")
    - sort    = symlink | target | ref_count | created_at
    - order   = asc | desc
    - orphans = liens brisés (cible absente)
    - rename  = dossiers parents sans identifiant IMDb valide
                (filtrage par dossier film/série, pas les saisons)
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
            "unique_targets": 0,
            "duplicates": [],
            "imdb_missing": 0,
        }

    try:
        # 📊 Calcul global du nombre de dossiers à renommer (avant filtres)
        imdb_missing = 0
        seen_parents = set()
        for i in items:
            symlink_path = Path(i["symlink"])
            parent = symlink_path.parent
            if "shows" in symlink_path.parts:
                parent = parent.parent
            if parent not in seen_parents:
                seen_parents.add(parent)
                if parent.exists() and is_missing_imdb(parent.name):
                    imdb_missing += 1

        all_broken = sum(
            1 for i in symlink_store or []
            if i.get("broken", False) or not i.get("target_exists", True)
        )

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
            items = [
                i for i in items
                if i.get("broken", False) or not i.get("target_exists", True)
            ]

        # 🎬 Mode rename (dossiers sans IMDb valide)
        if rename:
            seen = set()
            filtered = []
            for i in items:
                symlink_path = Path(i["symlink"])
                parent = symlink_path.parent
                if "shows" in symlink_path.parts:
                    parent = parent.parent
                if parent not in seen:
                    seen.add(parent)
                    if parent.exists() and is_missing_imdb(parent.name):
                        filtered.append(i)
            items = filtered

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
            "orphaned": sum(
                1 for i in items
                if i.get("broken", False) or not i.get("target_exists", True)
            ),
            "unique_targets": len(set(i["target"] for i in items if i.get("target_exists", True))),
            "imdb_missing": imdb_missing,  # ✅ cohérent en snake_case
            "all_broken": all_broken,
        }

    except Exception:
        logger.exception("💥 Erreur interne dans /symlinks")
        return {
            "total": 0,
            "page": 1,
            "limit": limit,
            "data": [],
            "orphaned": 0,
            "unique_targets": 0,
            "duplicates": [],
            "imdb_missing": 0,
        }


# -----
# Scan
# -----
@router.post("/scan")
async def trigger_scan():
    global symlink_store
    try:
        logger.info("🚀 [SCAN] Début du scan symlinks rapide")

        # Snapshot de l'état courant pour préserver les symlinks déjà marqués brisés
        previous_state = {
            item["symlink"]: {
                "broken": item.get("broken", False),
                "target_exists": item.get("target_exists", True),
                "ref_count": item.get("ref_count", 0),
            }
            for item in (symlink_store or [])
        }

        # Scan rapide identique à la logique du watcher au démarrage
        scanned = await run_in_threadpool(
            partial(scan_symlinks_parallel, workers=8, fast=True, ultra_fast=True)
        )

        # Réinjecte l'état "broken" déjà connu pour ne pas le perdre après un scan rapide
        restored_broken = 0
        for item in scanned:
            prev = previous_state.get(item["symlink"])
            if prev and (prev.get("broken", False) or not prev.get("target_exists", True)):
                item["broken"] = True
                item["target_exists"] = False
                item["ref_count"] = 0
                restored_broken += 1

        symlink_store.clear()
        symlink_store.extend(scanned)

        payload = {
            "event": "scan_completed",
            "action": "scan",
            "count": len(symlink_store),
            "restored_broken": restored_broken,
        }
        sse_manager.publish_event("symlink_update", payload)

        return {
            "message": "Scan rapide terminé",
            "count": len(symlink_store),
            "restored_broken": restored_broken,
            "data": symlink_store,
        }
    except Exception as e:
        logger.error(f"💥 Erreur scan: {e!r}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scan-broken")
async def trigger_broken_scan():
    global broken_scan_thread, broken_scan_running

    try:
        with broken_scan_lock:
            if broken_scan_running:
                return {
                    "message": "Un scan des symlinks brisés est déjà en cours",
                    "running": True,
                }

            broken_scan_running = True

        broken_scan_thread = threading.Thread(
            target=run_broken_detection_job,
            daemon=True
        )
        broken_scan_thread.start()

        return {
            "message": "Détection des symlinks brisés lancée",
            "running": True,
        }

    except Exception as e:
        with broken_scan_lock:
            broken_scan_running = False

        logger.error(f"💥 Erreur trigger_broken_scan: {e!r}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/scan-broken/status")
async def get_broken_scan_status():
    return {
        "running": broken_scan_running,
    }

# ---
# SSE
# ---

@router.get("/events")
async def get_events(request: Request, last_event_id: str = Header(None)):
    async def event_generator():
        subscriber = sse_manager.subscribe()

        # ✅ Si le client a un Last-Event-ID, rejoue les events manqués
        if last_event_id:
            missed = sse_manager.replay_events_since(last_event_id)
            if missed:
                logger.info(f"🔁 Rejoue {len(missed)} événements manqués (Last-Event-ID={last_event_id})")
                for event in missed:
                    yield event

        # ⚡ Envoi immédiat d’un ping pour ouvrir le flux
        yield "event: ping\ndata: {}\n\n"

        try:
            while True:
                try:
                    message = await asyncio.wait_for(subscriber.__anext__(), timeout=20)
                    yield message
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: {}\n\n"
        except asyncio.CancelledError:
            return

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
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

    movie = radarr.get_movie_by_clean_title(raw_name)
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
    logger.debug(f"   Cache actuel: {len(items)} symlinks en mémoire")

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
            logger.info(f"   Recherche relancée pour {movie.get('title')}")

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

        logger.debug(f"🔍 Série nettoyée : {raw_series}")

        # --- Trouver la série dans Sonarr
        series = sonarr.resolve_series(raw_series)
        if not series:
            logger.warning(f"❌ Série '{raw_series}' introuvable dans Sonarr")
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
            "tmdbId": series.get("tmdbId"),
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
    raw_series = Path(symlink_path).parts[0]  # ou extraire comme dans get-sonarr-id
    series = sonarr.resolve_series(raw_series)

    if not series:
        raise HTTPException(status_code=404, detail=f"Série introuvable dans Sonarr : {raw_series}")

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

        # 4️⃣ Identifier la série (corrigé)
        series_dir = candidate_abs.parent.parent
        raw_series_name = series_dir.name

        series = sonarr.resolve_series(raw_series_name)
        if not series:
            logger.warning(f"❗ Série '{raw_series_name}' introuvable dans Sonarr")
            return {"message": f"✅ Symlink supprimé (aucune série trouvée : {raw_series_name})"}

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


@router.post("/delete_broken_sonarr")
async def delete_broken_sonarr_symlinks(
    folder: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    logger.info("🚀 Suppression des symlinks Sonarr cassés + réparation SeasonIt")

    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide : lancez un scan d'abord.")

    # ----------------------------------------------------------------------
    # 1) Racines Sonarr
    # ----------------------------------------------------------------------
    try:
        roots = [
            Path(ld.path).resolve()
            for ld in config_manager.config.links_dirs
            if ld.manager == "sonarr"
        ]
        root_map = {
            Path(ld.path).name.lower(): Path(ld.path).resolve()
            for ld in config_manager.config.links_dirs
            if ld.manager == "sonarr"
        }
    except Exception:
        raise HTTPException(status_code=500, detail="Configuration invalide")

    if not roots:
        return {"message": "Aucune racine Sonarr trouvée", "deleted": 0}

    def is_relative(child: Path, parent: Path):
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False

    # ----------------------------------------------------------------------
    # 2) Extraction depuis le STORE
    # ----------------------------------------------------------------------
    items = list(symlink_store)

    # Filtre folder
    if folder:
        key = folder.lower()
        if key in root_map:
            folder_paths = [root_map[key]]
        else:
            folder_paths = [(r / folder) for r in roots]

        before = len(items)
        items = [
            i for i in items
            if any(is_relative(Path(i["symlink"]), fp) for fp in folder_paths)
        ]
        logger.debug(f"📁 Filtre '{folder}' : {before} → {len(items)} éléments")

    # ----------------------------------------------------------------------
    # 3) Symlinks cassés
    # ----------------------------------------------------------------------
    broken_symlinks = [
        i for i in items
        if not i.get("target_exists", True)
        and any(is_relative(Path(i["symlink"]), r) for r in roots)
    ]

    logger.info(f"🧹 {len(broken_symlinks)} symlinks cassés détectés")

    if not broken_symlinks:
        return {"message": "Aucun symlink cassé Sonarr", "deleted": 0}

    # ----------------------------------------------------------------------
    # 4) Préparation services
    # ----------------------------------------------------------------------
    service = SeasonItService(db, current_user.id)
    FIXED_INSTANCE_ID = 1

    instance: SonarrInstance = db.query(SonarrInstance).get(FIXED_INSTANCE_ID)
    if not instance:
        raise HTTPException(status_code=500, detail="Instance Sonarr 1 introuvable")

    sonarr_client = SonarrClient(
        base_url=instance.url,
        api_key=instance.api_key,
        instance_id=FIXED_INSTANCE_ID,
    )

    local_resolver = SonarrService()
    season_regex = re.compile(r"(?:saison|season)\s*0?(\d{1,2})", re.IGNORECASE)

    deleted_count = 0
    errors = []
    tasks = []  # (series_id, title, season_number)

    # ----------------------------------------------------------------------
    # 5) PHASE 1 : suppression + collecte des tâches SeasonIt
    # ----------------------------------------------------------------------
    for item in broken_symlinks:
        symlink_path = Path(item["symlink"])

        try:
            # Suppression physique
            if symlink_path.is_symlink():
                symlink_path.unlink(missing_ok=True)
                logger.info(f"🗑️ Suppression symlink : {symlink_path}")
                deleted_count += 1
            else:
                continue

            # Résolution série
            season_dir = symlink_path.parent
            series_dir = season_dir.parent
            raw_series = series_dir.name

            resolved = local_resolver.resolve_series(raw_series)
            if not resolved:
                errors.append(f"{symlink_path}: série '{raw_series}' non trouvée")
                continue

            series_id = resolved["id"]

            # Détecter saison
            m = season_regex.search(season_dir.name)
            if not m:
                errors.append(f"{symlink_path}: saison introuvable")
                continue

            season_number = int(m.group(1))

            # Stocker la tâche à exécuter plus tard
            tasks.append((series_id, resolved["title"], season_number))

            # Event SSE immédiat (symlink supprimé)
            payload = {
                "event": "sonarr_symlink_removed",
                "path": str(symlink_path),
                "series": {"id": series_id, "title": resolved["title"]},
                "season": season_number,
            }
            sse_manager.publish_event("symlink_update", payload)

        except Exception as e:
            errors.append(f"{symlink_path}: {e}")

    # ----------------------------------------------------------------------
    # 6) PHASE 2 : Refresh Sonarr pour toutes les séries
    # ----------------------------------------------------------------------
    unique_series = {series_id for (series_id, _, _) in tasks}

    for sid in unique_series:
        try:
            logger.info(f"🔄 Refresh Sonarr pour série ID={sid}")
            local_resolver.refresh_series(sid)
            await asyncio.sleep(3)
        except Exception as e:
            logger.warning(f"⚠️ Refresh failed for {sid}: {e}")

    # ----------------------------------------------------------------------
    # 7) PHASE 3 : SeasonIt
    # ----------------------------------------------------------------------

    # Forcer le mode SAFE
    settings = db.query(UserSettings).filter(UserSettings.user_id == current_user.id).first()
    if settings:
        settings.skip_episode_deletion = True
        settings.disable_season_pack_check = True
        db.commit()
        db.refresh(settings)
        logger.info(f"🛡️ Mode SAFE activé : skip_episode_deletion=True pour user={current_user.id}")

    for (series_id, title, season_number) in tasks:
        try:
            logger.info(f"🎬 SeasonIt: {title} S{season_number}")
            await service.process_season_it(series_id, season_number, FIXED_INSTANCE_ID)

        except Exception as e:
            errors.append(f"{title} S{season_number} : SeasonIt failed — {e}")

    # ----------------------------------------------------------------------
    # FIN
    # ----------------------------------------------------------------------
    return {
        "message": f"{deleted_count} symlinks supprimés (Sonarr) + SeasonIt exécuté",
        "deleted": deleted_count,
        "errors": errors,
    }

# -----------------
# Doublons (cibles)
# -----------------

@router.get("/duplicates")
def list_duplicates(folder: str = Query(None)):
    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    # ⚡ On applique d’abord le filtrage par dossier si fourni
    results = symlink_store
    if folder:
        # tu peux améliorer en utilisant filter_items_by_folder si tu veux :
        # results = filter_items_by_folder(results, folder)
        results = [s for s in results if folder in s["symlink"]]

    # ⚡ Ensuite on construit la map des doublons
    target_map: dict[str, list[dict]] = {}
    for item in results:
        target = item["target"]
        if item.get("ref_count", 0) > 1:
            target_map.setdefault(target, []).append(item)

    duplicates: list[dict] = []
    for items in target_map.values():
        if len(items) > 1:
            duplicates.extend(items)

    # 🕒 Enrichir les doublons avec created_at (sans toucher au scan global)
    from datetime import datetime
    import os

    for item in duplicates:
        if not item.get("created_at"):
            try:
                stat = os.lstat(item["symlink"])
                item["created_at"] = datetime.fromtimestamp(stat.st_mtime).isoformat()
            except Exception:
                # en cas d'erreur (symlink disparu, permission, etc.)
                item["created_at"] = None

    return {
        "total": len(duplicates),
        "data": duplicates
    }

# --------------------------
# Proxy TMDB (films/séries)
# --------------------------
@router.get("/tmdb/{media_type}/{tmdb_id}")
async def get_tmdb_data(
    media_type: str,
    tmdb_id: int,
    lang: str = Query("fr-FR")
):
    api_key = config_manager.config.tmdb_api_key
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
    params = {
        "api_key": api_key,
        "language": lang,
        "append_to_response": "videos,credits,recommendations,release_dates"
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()

    # Trailer (YouTube uniquement)
    trailer = None
    videos = data.get("videos", {}).get("results", [])
    for v in videos:
        if v.get("site") == "YouTube" and v.get("type") == "Trailer":
            trailer = f"https://www.youtube.com/watch?v={v['key']}"
            break

    # Casting (top 10)
    cast = [
        {
            "name": c["name"],
            "character": c.get("character"),
            "profile": f"https://image.tmdb.org/t/p/w185{c['profile_path']}" if c.get("profile_path") else None
        }
        for c in data.get("credits", {}).get("cast", [])[:10]
    ]

    # Recommendations
    recos = [
        {
            "id": r["id"],
            "title": r.get("title") or r.get("name"),
            "poster": f"https://image.tmdb.org/t/p/w342{r['poster_path']}" if r.get("poster_path") else None
        }
        for r in data.get("recommendations", {}).get("results", [])[:10]
    ]

    # Certification
    certification = None
    rel_dates = data.get("release_dates", {}).get("results", [])
    for entry in rel_dates:
        if entry["iso_3166_1"] == "FR":
            certs = entry.get("release_dates", [])
            if certs:
                certification = certs[0].get("certification")
                break

    # Gestion runtime sécurisé (films ou séries)
    runtime = None
    if data.get("runtime"):
        runtime = data["runtime"]
    else:
        episode_run_time = data.get("episode_run_time")
        if isinstance(episode_run_time, list) and episode_run_time:
            runtime = episode_run_time[0]

    return {
        "id": data.get("id"),
        "title": data.get("title") or data.get("name"),
        "overview": data.get("overview"),
        "genres": [g["name"] for g in data.get("genres", [])],
        "runtime": runtime,
        "year": (data.get("release_date") or data.get("first_air_date") or "")[:4],
        "poster": f"https://image.tmdb.org/t/p/w342{data['poster_path']}" if data.get("poster_path") else None,
        "backdrop": f"https://image.tmdb.org/t/p/w780{data['backdrop_path']}" if data.get("backdrop_path") else None,
        "rating": data.get("vote_average"),
        "trailer": trailer,
        "cast": cast,
        "recommendations": recos,
        "certification": certification
    }

# -------------------
# Suppression Symlink local doublons
# -------------------
@router.delete("/delete_local/{symlink_path:path}")
async def delete_local_symlink(
    symlink_path: str,
    root: Optional[str] = Query(None, description="Nom de la racine (ex: movies ou shows)")
):
    """
    Supprime un symlink :
    - Supprime physiquement le lien symbolique si présent (même si cible orpheline)
    - Publie un event SSE informatif
    """
    try:
        # 🔎 Identifier les répertoires racines autorisés
        if root:
            root_paths = {
                Path(ld.path).name: Path(ld.path).resolve()
                for ld in config_manager.config.links_dirs
            }
            if root not in root_paths:
                raise HTTPException(status_code=400, detail="Racine inconnue")
            roots = [root_paths[root]]
        else:
            roots = [Path(ld.path).resolve() for ld in config_manager.config.links_dirs]

        candidate_abs = None
        for r in roots:
            # Chemin absolu basé sur la racine
            test_path = r / symlink_path
            try:
                # Vérifie bien que le chemin est dans la racine
                test_path.relative_to(r)
            except ValueError:
                continue

            if test_path.exists() or test_path.is_symlink():
                candidate_abs = test_path
                break

        if not candidate_abs:
            raise HTTPException(status_code=404, detail="Symlink introuvable")

        if not candidate_abs.is_symlink():
            raise HTTPException(status_code=400, detail="Le chemin trouvé n'est pas un symlink")

        # 🗑️ Suppression du symlink
        try:
            candidate_abs.unlink(missing_ok=True)
            logger.info(f"🗑️ Symlink supprimé : {candidate_abs}")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de supprimer le symlink {candidate_abs} : {e}")
            raise HTTPException(status_code=500, detail=f"Impossible de supprimer le symlink : {e}")

        # 📢 Publier un event SSE
        payload = {
            "event": "symlink_removed",
            "path": str(candidate_abs),
        }
        sse_manager.publish_event("symlink_update", payload)

        return {"message": f"✅ Symlink supprimé : {candidate_abs}"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"💥 Erreur suppression symlink : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erreur interne : {e}")


#-------------------------------------------
#explorateur pour configuration dossiers symlinks
#-------------------------------------------

@router.get("/fs")
async def list_media_folders_and_files(
    path: str = Query("", description="Sous-chemin relatif à $HOME/Medias"),
    limit: int = Query(1000, gt=1, le=10000),
    show_hidden: bool = Query(False, description="Inclure les dossiers cachés"),
    include_files: bool = Query(True, description="Inclure les fichiers vidéos (.mkv, .mp4, etc.)"),
):
    """
    📂 Explore $HOME/Medias : dossiers + fichiers vidéos (.mkv, .mp4…)
    """
    try:
        home_dir = Path(os.getenv("HOME", "/home/ubuntu"))
        root_dir = home_dir / "Medias"

        if not root_dir.exists():
            raise HTTPException(status_code=404, detail=f"Le dossier {root_dir} n'existe pas")

        # Sécurisation du chemin
        target = (root_dir / path).resolve()
        try:
            target.relative_to(root_dir)
        except ValueError:
            raise HTTPException(status_code=403, detail="Accès en dehors du répertoire autorisé")

        if not target.exists() or not target.is_dir():
            raise HTTPException(status_code=404, detail=f"{target} n'est pas un dossier valide")

        # --- Récupération des dossiers et fichiers ---
        folders, files = [], []
        video_exts = {".mkv", ".mp4", ".avi", ".mov", ".m4v"}

        with os.scandir(target) as it:
            for entry in it:
                if entry.name.startswith(".") and not show_hidden:
                    continue
                if entry.is_dir(follow_symlinks=False):
                    folders.append({
                        "name": entry.name,
                        "path": str(Path(entry.path).relative_to(root_dir)),
                        "is_dir": True,
                        "mtime": entry.stat().st_mtime
                    })
                elif include_files and entry.is_file():
                    ext = Path(entry.name).suffix.lower()
                    if ext in video_exts:
                        files.append({
                            "name": entry.name,
                            "path": str(Path(entry.path).relative_to(root_dir)),
                            "is_dir": False,
                            "size": entry.stat().st_size,
                            "mtime": entry.stat().st_mtime,
                            "ext": ext
                        })

        folders.sort(key=lambda e: e["name"].lower())
        files.sort(key=lambda e: e["name"].lower())

        return {
            "root": str(root_dir),
            "current": str(target.relative_to(root_dir)),
            "count": len(folders) + len(files),
            "folders": folders,
            "files": files,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
