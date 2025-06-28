import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from .json_manager import update_json_files

USER = os.getenv("USER") or os.getlogin()
YAML_PATH = f"/home/{USER}/.ansible/inventories/group_vars/all.yml"
VAULT_PASSWORD_FILE = f"/home/{USER}/.vault_pass"

class YAMLFileEventHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path == YAML_PATH:
            # Déchiffrer le fichier YAML avec Ansible Vault directement
            try:
                command = f"ansible-vault view {YAML_PATH} --vault-password-file {VAULT_PASSWORD_FILE}"
                result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

                # Ignorer les erreurs sans afficher de messages
                if result.returncode != 0:
                    if "input is not vault encrypted data" in result.stderr:
                        return
                    else:
                        return

                decrypted_yaml_content = result.stdout

                # Mettre à jour les fichiers JSON
                update_json_files(decrypted_yaml_content)

            except Exception:
                return

def start_watching_yaml_file():
    event_handler = YAMLFileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(YAML_PATH), recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(5)  # Attendre 5 secondes entre chaque vérification
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
