import os
import time
import threading
import subprocess
import json
import asyncio
from datetime import datetime
from pathlib import Path
from loguru import logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from src.services.fonctions_arrs import RadarrService
from program.utils.text_utils import normalize_name, clean_movie_name
from program.settings.manager import config_manager
from program.managers.sse_manager import sse_manager
from .json_manager import update_json_files
from routers.secure.symlinks import scan_symlinks, symlink_store
from program.utils.discord_notifier import send_discord_summary

USER = os.getenv("USER") or os.getlogin()
YAML_PATH = f"/home/{USER}/.ansible/inventories/group_vars/all.yml"
VAULT_PASSWORD_FILE = f"/home/{USER}/.vault_pass"

# --- Buffer Discord ---
symlink_events_buffer = []
last_sent_time = datetime.utcnow()
SUMMARY_INTERVAL = 60  # en secondes
MAX_EVENTS_BEFORE_FLUSH = 20
buffer_lock = threading.Lock()

# --- Cache d'enrichissement Radarr partagé ---
_radarr_index: dict[str, dict] = {}
_radarr_host: str | None = None
_radarr_idx_lock = threading.Lock()
_last_index_build = 0.0
_INDEX_TTL_SEC = 3600


def _build_radarr_index(force: bool = False) -> None:
    """Construit (ou reconstruit) l’index Radarr partagé"""
    global _radarr_index, _radarr_host, _last_index_build
    now = time.time()
    if not force and (now - _last_index_build) < _INDEX_TTL_SEC and _radarr_index:
        return

    try:
        radarr = RadarrService()
        movies = radarr.get_all_movies()
        logger.info(f"📚 Index Radarr: {len(movies)} films chargés")

        idx: dict[str, dict] = {}
        for m in movies:
            cleaned = clean_movie_name(m.get("title", "") or "")
            norm = normalize_name(cleaned)
            if not norm:
                continue
            idx[norm] = m
            y = m.get("year")
            if y:
                idx[f"{norm}{y}"] = m

        from routers.secure.symlinks import get_traefik_host
        host = get_traefik_host("radarr")

        with _radarr_idx_lock:
            _radarr_index = idx
            _radarr_host = host
            _last_index_build = now

        logger.success(f"🗂️ Index Radarr prêt: {len(idx)} clés | host={host}")

    except Exception as e:
        logger.warning(f"⚠️ Échec construction index Radarr: {e}", exc_info=True)


def _enrich_from_radarr_index(symlink_path: Path) -> dict:
    """Enrichit un item symlink avec les infos Radarr (cache + fallback direct)"""
    raw_name = symlink_path.parent.name
    cleaned = clean_movie_name(raw_name)
    norm = normalize_name(cleaned)

    with _radarr_idx_lock:
        movie = _radarr_index.get(norm) or _radarr_index.get(f"{norm}")

    if not movie:
        try:
            radarr = RadarrService()
            movie = radarr.get_movie_by_clean_title(raw_name)
            if movie:
                with _radarr_idx_lock:
                    _radarr_index[norm] = movie
                    if movie.get("year"):
                        _radarr_index[f"{norm}{movie['year']}"] = movie
        except Exception as e:
            logger.warning(f"⚠️ Lookup direct Radarr échoué pour '{raw_name}' : {e}")

    if not movie:
        return {}

    poster_url = None
    images = movie.get("images") or []
    poster = next((img.get("url") for img in images if img.get("coverType") == "poster"), None)
    if poster:
        if poster.startswith("/"):
            host = _radarr_host
            poster_url = f"https://{host}{poster}" if host else poster
        else:
            poster_url = poster

    return {
        "id": movie.get("id"),
        "title": movie.get("title"),
        "tmdbId": movie.get("tmdbId"),
        "poster": poster_url,
        "year": movie.get("year"),
        "overview": movie.get("overview"),
        "genres": movie.get("genres") or []
    }


# --- 1. YAML watcher ---
class YAMLFileEventHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if os.path.abspath(event.src_path) == os.path.abspath(YAML_PATH):
            try:
                command = f"ansible-vault view {YAML_PATH} --vault-password-file {VAULT_PASSWORD_FILE}"
                result = subprocess.run(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    shell=True
                )

                if result.returncode != 0:
                    if "input is not vault encrypted data" in result.stderr:
                        return
                    logger.error(f"🔐 Erreur ansible-vault : {result.stderr}")
                    return

                decrypted_yaml_content = result.stdout
                update_json_files(decrypted_yaml_content)

            except Exception as e:
                logger.exception(f"💥 Exception YAML: {e}")


def start_yaml_watcher():
    logger.info("🛰️ YAML watcher démarré")
    observer = Observer()
    observer.schedule(YAMLFileEventHandler(), path=os.path.dirname(YAML_PATH), recursive=False)
    logger.info(f"📍 Surveillance active sur : {YAML_PATH}")
    observer.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# --- 2. Symlink watcher ---
class SymlinkEventHandler(FileSystemEventHandler):
    def __init__(self):
        self._lock = threading.Lock()

    def on_any_event(self, event):
        if event.is_directory:
            return

        path = Path(event.src_path)
        logger.debug(f"📂 Événement détecté : {event.event_type} -> {path}")

        if event.event_type == "created" and path.is_symlink():
            self._handle_created(path)
        elif event.event_type == "deleted":
            self._handle_deleted(path)

    def _handle_created(self, symlink_path: Path):
        try:
            config = config_manager.config
            links_dirs = [(Path(ld.path).resolve(), ld.manager) for ld in config.links_dirs]
            mount_dirs = [Path(d).resolve() for d in config.mount_dirs]

            root, manager = None, "unknown"
            for ld, mgr in links_dirs:
                if str(symlink_path).startswith(str(ld)):
                    root, manager = ld, mgr
                    break
            if not root:
                return

            try:
                target_path = symlink_path.resolve(strict=True)
            except FileNotFoundError:
                target_path = symlink_path.resolve(strict=False)

            matched_mount, relative_target = None, None
            for mount_dir in mount_dirs:
                try:
                    relative_target = target_path.relative_to(mount_dir)
                    matched_mount = mount_dir
                    break
                except ValueError:
                    continue
            full_target = str(matched_mount / relative_target) if matched_mount else str(target_path)

            try:
                relative_path = str(symlink_path.resolve().relative_to(root))
            except Exception:
                relative_path = str(symlink_path).replace(str(root) + "/", "")

            stat = symlink_path.lstat()
            created_at = datetime.fromtimestamp(stat.st_mtime).isoformat()

            item = {
                "symlink": str(symlink_path),
                "relative_path": relative_path,
                "target": full_target,
                "target_exists": True,
                "manager": manager,
                "type": manager,
                "created_at": created_at,
                "ref_count": 1,
            }

            if manager == "radarr":
                extra = _enrich_from_radarr_index(symlink_path)
                if extra:
                    item.update(extra)

            with self._lock:
                symlink_store.append(item)

            sse_manager.publish_event("symlink_update", {
                "event": "symlink_added",
                "item": item,
                "count": len(symlink_store),
            })

            logger.success(f"➕ Symlink enrichi ajouté au cache : {symlink_path}")

        except Exception as e:
            logger.error(f"💥 Erreur ajout symlink {symlink_path}: {e}", exc_info=True)

    def _handle_deleted(self, symlink_path: Path):
        removed = False
        with self._lock:
            for idx in range(len(symlink_store) - 1, -1, -1):
                if symlink_store[idx].get("symlink") == str(symlink_path):
                    del symlink_store[idx]
                    removed = True

        if removed:
            sse_manager.publish_event("symlink_update", {
                "event": "symlink_removed",
                "path": str(symlink_path),
                "count": len(symlink_store)
            })
            logger.success(f"➖ Symlink supprimé du cache : {symlink_path}")


# --- 3. Flush Discord ---
def start_discord_flusher():
    def loop():
        global last_sent_time, symlink_events_buffer
        while True:
            try:
                now = datetime.utcnow()
                webhook = config_manager.config.discord_webhook_url

                if webhook and symlink_events_buffer and (now - last_sent_time).total_seconds() >= SUMMARY_INTERVAL:
                    try:
                        asyncio.run(send_discord_summary(webhook, symlink_events_buffer))
                        logger.info(f"📊 Rapport Discord envoyé ({len(symlink_events_buffer)} événements)")
                        symlink_events_buffer.clear()
                        last_sent_time = now
                    except Exception as e:
                        logger.error(f"💥 Erreur envoi résumé Discord : {e}")
                time.sleep(10)
            except Exception as e:
                logger.error(f"💥 Erreur flusher Discord : {e}")
                time.sleep(30)

    threading.Thread(target=loop, daemon=True).start()


# --- 4. Lancement watchers ---
def start_symlink_watcher():
    logger.info("🛰️ Symlink watcher démarré")
    observers = []
    try:
        config = config_manager.config
        links_dirs = [str(ld.path) for ld in config.links_dirs]

        if not links_dirs:
            logger.warning("⚠️ Aucun répertoire dans 'links_dirs'")
            return

        _build_radarr_index(force=True)

        try:
            radarr = RadarrService()
        except Exception:
            radarr = None
        symlinks_data = scan_symlinks(radarr)
        symlink_store.clear()
        symlink_store.extend(symlinks_data)
        logger.success(f"✔️ Scan initial terminé — {len(symlinks_data)} symlinks chargés")

        sse_manager.publish_event("symlink_update", {
            "event": "initial_scan",
            "message": "Scan initial terminé",
            "count": len(symlinks_data)
        })

        for dir_path in links_dirs:
            path = Path(dir_path)
            if not path.exists():
                logger.warning(f"⚠️ Dossier symlink introuvable : {path}")
                continue

            observer = Observer()
            observer.schedule(SymlinkEventHandler(), path=str(path), recursive=True)
            observer.start()
            observers.append(observer)
            logger.info(f"📍 Symlink watcher actif sur {path.resolve()}")

        # === Boucle de fond ===
        scan_interval = 3600  # 1h
        last_scan = time.time()

        while True:
            logger.debug("📡 Symlink thread actif...")
            _build_radarr_index(force=False)

            if time.time() - last_scan >= scan_interval:
                try:
                    radarr = RadarrService()
                except Exception:
                    radarr = None

                symlinks_data = scan_symlinks(radarr)
                with threading.Lock():
                    symlink_store.clear()
                    symlink_store.extend(symlinks_data)

                sse_manager.publish_event("symlink_update", {
                    "event": "periodic_scan",
                    "message": "Rescan automatique",
                    "count": len(symlinks_data)
                })

                logger.success(f"🔄 Scan périodique exécuté — {len(symlinks_data)} symlinks")
                last_scan = time.time()

            time.sleep(30)

    except KeyboardInterrupt:
        logger.info("⏹️ Arrêt du Symlink watcher")
        for obs in observers:
            obs.stop()
    except Exception as e:
        logger.exception(f"💥 Erreur watcher symlink : {e}")

    for obs in observers:
        obs.join()


def start_all_watchers():
    logger.info("🚀 Lancement des watchers YAML + Symlink...")
    threading.Thread(target=start_yaml_watcher, daemon=True).start()
    threading.Thread(target=start_symlink_watcher, daemon=True).start()
    start_discord_flusher()
