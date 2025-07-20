import os
import time
import threading
import subprocess
import json
import asyncio
from pathlib import Path
from loguru import logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from program.managers.sse_manager import sse_manager
from .json_manager import update_json_files
from routers.secure.symlinks import scan_symlinks, symlink_store, load_config

USER = os.getenv("USER") or os.getlogin()
YAML_PATH = f"/home/{USER}/.ansible/inventories/group_vars/all.yml"
VAULT_PASSWORD_FILE = f"/home/{USER}/.vault_pass"

# --- 1. YAML watcher ---

class YAMLFileEventHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if os.path.abspath(event.src_path) == os.path.abspath(YAML_PATH):
            try:
                command = f"ansible-vault view {YAML_PATH} --vault-password-file {VAULT_PASSWORD_FILE}"
                result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

                if result.returncode != 0:
                    if "input is not vault encrypted data" in result.stderr:
                        return
                    else:
                        logger.error(f"🔐 Erreur ansible-vault : {result.stderr}")
                        return

                decrypted_yaml_content = result.stdout
                update_json_files(decrypted_yaml_content)
                logger.success("📘 YAML mis à jour avec succès")

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
        self._timer = None

    def on_any_event(self, event):
        if event.is_directory:
            return

        logger.debug(f"📂 Événement détecté : {event.event_type} -> {event.src_path}")
        self._debounce_refresh()

    def _debounce_refresh(self, delay=2):
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(delay, self._refresh_symlinks)
            self._timer.start()

    def _refresh_symlinks(self):
        try:
            symlinks_data = scan_symlinks()
            symlink_store.clear()
            symlink_store.extend(symlinks_data)

            logger.success(f"🔗 symlink_store mis à jour ({len(symlinks_data)} symlinks)")
            asyncio.run(asyncio.sleep(0.1))
            sse_manager.publish_event("symlink_update", json.dumps({"count": len(symlinks_data)}))

        except Exception as e:
            logger.exception(f"💥 Erreur dans le watcher symlinks : {e}")


def start_symlink_watcher():
    logger.info("🛰️ Symlink watcher démarré")
    try:
        config = load_config()
        links_dirs = config.get("links_dirs", [])

        if not links_dirs:
            logger.warning("⚠️ Aucun répertoire dans 'links_dirs'")
            return

        observers = []

        # ✅ Scan initial AVANT d'attendre un événement
        symlinks_data = scan_symlinks()
        symlink_store.clear()
        symlink_store.extend(symlinks_data)

        logger.success(f"✔️ Scan initial terminé — {len(symlinks_data)} symlinks chargés")

        # ✅ Envoi d’un événement SSE pour notifier le frontend
        sse_manager.publish_event("symlink_update", json.dumps({
            "event": "initial_scan",
            "message": "Scan initial terminé",
            "count": len(symlinks_data)
        }))

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

        while True:
            logger.debug("📡 Symlink thread actif...")
            time.sleep(30)

    except KeyboardInterrupt:
        for obs in observers:
            obs.stop()
    except Exception as e:
        logger.exception(f"💥 Erreur lors du démarrage du watcher symlink : {e}")

    for obs in observers:
        obs.join()

def start_all_watchers():
    logger.info("🚀 Lancement des watchers YAML + Symlink...")
    threading.Thread(target=start_yaml_watcher, daemon=True).start()
    threading.Thread(target=start_symlink_watcher, daemon=True).start()
