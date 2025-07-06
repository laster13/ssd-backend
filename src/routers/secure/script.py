
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
import os
import subprocess
import yaml
import json
from program.json_manager import update_json_files, save_json_to_file, load_json_from_file
from program.settings.manager import settings_manager
from sse_starlette.sse import EventSourceResponse
import time
import asyncio
import docker


USER = os.getenv("USER") or os.getlogin()

SCRIPTS_DIR = f"/home/{USER}/projet-ssd/ssd-frontend/scripts"
YAML_PATH = f"/home/{USER}/.ansible/inventories/group_vars/all.yml"
VAULT_PASSWORD_FILE = f"/home/{USER}/.vault_pass"
BACKEND_JSON_PATH = f"/home/{USER}/projet-ssd/ssd-backend/data/settings.json"
FRONTEND_JSON_PATH = f"/home/{USER}/projet-ssd/ssd-frontend/static/settings.json"

# Chargement initial des données JSON
json_data = load_json_from_file(BACKEND_JSON_PATH)

# Création du router pour les scripts et les configurations YAML
router = APIRouter(
    prefix="/scripts",
    tags=["scripts"],
    responses={404: {"description": "Not found"}},
)

class ScriptModel(BaseModel):
    name: str
    params: list[str] = []

# Route pour vérifier l'existence d'un fichier
@router.get("/check-file")
async def check_file():
    file_path = f'/home/{USER}/seedbox-compose/ssddb'
    
    try:
        if os.path.exists(file_path):
            return {"exists": True}
        else:
            return {"exists": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")

# Route pour exécuter un script Bash
@router.get("/run/{script_name}")
async def run_script(script_name: str, label: str = Query(None, description="Label du conteneur")):
    if not script_name.isalnum():
        raise HTTPException(status_code=400, detail="Nom de script invalide.")

    script_path = os.path.join(SCRIPTS_DIR, f"{script_name}.sh")
    
    if not os.path.isfile(script_path):
        raise HTTPException(status_code=404, detail=f"Script non trouvé: {script_name}.sh")

    def stream_logs():
        try:
            if label:
                process = subprocess.Popen(['bash', script_path, label], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            else:
                process = subprocess.Popen(['bash', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            for line in process.stdout:
                yield f"data: {line}\n\n"
            for err in process.stderr:
                yield f"data: Erreur: {err}\n\n"

            process.wait()
            yield "event: end\ndata: Fin du script\n\n"
        except Exception as e:
            yield f"data: Erreur lors de l'exécution: {str(e)}\n\n"

    return StreamingResponse(stream_logs(), media_type="text/event-stream")

@router.post("/update-config")
async def update_config():
    try:
        # Tenter de déchiffrer le fichier YAML avec Ansible Vault
        command = f"ansible-vault view {YAML_PATH} --vault-password-file {VAULT_PASSWORD_FILE}"
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

        # Log du résultat pour analyse
        print(f"Résultat de la commande ansible-vault: {result.stdout}, Erreurs: {result.stderr}")

        if result.returncode != 0:
            raise Exception(f"Erreur lors du déchiffrement : {result.stderr}")

        decrypted_yaml_content = result.stdout

        # Charger les données YAML dans un dictionnaire
        yaml_data = yaml.safe_load(decrypted_yaml_content)

        # Vérifie si yaml_data est vide ou incorrect
        if not yaml_data:
            raise Exception("Le fichier YAML déchiffré est vide ou mal formaté.")

        # Mettre à jour les fichiers JSON avec les données 'sub' déchiffrées du YAML
        update_json_files(decrypted_yaml_content)

        # Mettre à jour les informations Cloudflare
        if 'cloudflare' in yaml_data:
            settings_manager.settings.cloudflare.cloudflare_login = yaml_data['cloudflare']['login']
            settings_manager.settings.cloudflare.cloudflare_api_key = yaml_data['cloudflare']['api']

        # Mettre à jour les informations utilisateur
        if 'user' in yaml_data:
            settings_manager.settings.utilisateur.username = yaml_data['user']['name']
            settings_manager.settings.utilisateur.email = yaml_data['user']['mail']
            settings_manager.settings.utilisateur.domain = yaml_data['user']['domain']
            settings_manager.settings.utilisateur.password = yaml_data['user']['pass']

        # Sauvegarder les paramètres mis à jour
        settings_manager.save()

        return {"message": "Configuration mise à jour avec succès."}

    except Exception as e:
        # Log de l'erreur pour analyse
        print(f"Erreur lors de la mise à jour : {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors de la mise à jour : {str(e)}")

@router.get("/domains")
async def get_domains():
    try:
        data = load_json_from_file(BACKEND_JSON_PATH)

        if "utilisateur" not in data or "domain" not in data["utilisateur"]:
            raise HTTPException(status_code=500, detail="'utilisateur.domain' manquant")
        if "dossiers" not in data or "domaine" not in data["dossiers"]:
            raise HTTPException(status_code=500, detail="'dossiers.domaine' manquant")

        base_domain = data["utilisateur"]["domain"]
        app_domains = data["dossiers"]["domaine"]

        return {app: f"{sub}.{base_domain}" for app, sub in app_domains.items()}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lecture JSON : {str(e)}")

@router.get("/rdtclient-status", tags=["Status"])
async def rdtclient_status():
    try:
        client = docker.from_env()
        container = client.containers.get("rdtclient")  # nom du conteneur Docker
        status = container.status  # "running", "exited", etc.
    except docker.errors.NotFound:
        status = "not_found"
    except Exception as e:
        status = f"error: {str(e)}"

    return {"service": "rdtclient", "status": status}


