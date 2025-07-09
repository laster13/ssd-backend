import os
import time
import threading
import subprocess
import json
import asyncio
from pathlib import Path
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
                        print(f"[Watcher] Erreur ansible-vault : {result.stderr}")
                        return

                decrypted_yaml_content = result.stdout
                update_json_files(decrypted_yaml_content)
                print("[Watcher] YAML mis à jour avec succès")

            except Exception as e:
                print(f"[Watcher] Exception YAML: {e}")


def start_yaml_watcher():
    print("[Watcher] YAML watcher started")
    observer = Observer()
    observer.schedule(YAMLFileEventHandler(), path=os.path.dirname(YAML_PATH), recursive=False)
    print(f"[Watcher] Surveillance active sur : {YAML_PATH}")
    observer.start()
    print(f"[Watcher] YAML watcher démarré sur {YAML_PATH}")

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

        print(f"[Watcher] Événement détecté : {event.event_type} -> {event.src_path}")
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

            print(f"[Watcher] symlink_store mis à jour ({len(symlinks_data)} symlinks)")

            sse_manager.publish_event("symlink_update", json.dumps({"count": len(symlinks_data)}))

        except Exception as e:
            print(f"[Watcher] Erreur dans le watcher symlinks : {e}")


def start_symlink_watcher():
    print("[Watcher] Symlink watcher started")
    try:
        config = load_config()
        links_dir = Path(config["links_dir"])
        if not links_dir.exists():
            print(f"[Watcher] Dossier symlink introuvable : {links_dir}")
            return

        observer = Observer()
        observer.schedule(SymlinkEventHandler(), path=str(links_dir), recursive=False)
        observer.start()
        print(f"[Watcher] Symlink watcher actif sur {links_dir.resolve()}")

        while True:
            print("[Watcher] Symlink thread actif...")
            time.sleep(30)

    except KeyboardInterrupt:
        observer.stop()
    except Exception as e:
        print(f"[Watcher] Erreur lors du démarrage du watcher symlink : {e}")
    observer.join()

# --- Lancer les deux watchers dans des threads ---

def start_all_watchers():
    print("[Watcher] Lancement des watchers...")
    threading.Thread(target=start_yaml_watcher, daemon=True).start()
    threading.Thread(target=start_symlink_watcher, daemon=True).start()
