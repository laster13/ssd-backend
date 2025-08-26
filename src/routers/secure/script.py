from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse

import os
import subprocess
import yaml
import json
import time
import docker
import asyncio

from loguru import logger
from program.json_manager import update_json_files, save_json_to_file, load_json_from_file
from program.settings.manager import settings_manager

USER = os.getenv("USER") or os.getlogin()

SCRIPTS_DIR = f"/home/{USER}/seedbox/docker/{USER}/projet-ssd/ssd-frontend/scripts"
YAML_PATH = f"/home/{USER}/.ansible/inventories/group_vars/all.yml"
VAULT_PASSWORD_FILE = f"/home/{USER}/.vault_pass"
BACKEND_JSON_PATH = f"/home/{USER}/seedbox/docker/{USER}/projet-ssd/ssd-backend/data/settings.json"
FRONTEND_JSON_PATH = f"/home/{USER}/seedbox/docker/{USER}/projet-ssd/ssd-frontend/static/settings.json"

json_data = load_json_from_file(BACKEND_JSON_PATH)

router = APIRouter(
    prefix="/scripts",
    tags=["scripts"],
)

class ScriptModel(BaseModel):
    name: str
    params: list[str] = []

@router.get("/check-file")
async def check_file():
    file_path = f'/home/{USER}/seedbox-compose/ssddb'
    try:
        exists = os.path.exists(file_path)
        logger.info(f"Vérification du fichier : {file_path} → {'existe' if exists else 'inexistant'}")
        return {"exists": exists}
    except Exception as e:
        logger.error(f"Erreur check_file: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")

@router.get("/run/{script_name}")
async def run_script(script_name: str, label: str = Query(None, description="Label du conteneur")):
    if not script_name.isalnum():
        raise HTTPException(status_code=400, detail="Nom de script invalide.")

    script_path = os.path.join(SCRIPTS_DIR, f"{script_name}.sh")
    if not os.path.isfile(script_path):
        logger.warning(f"Script non trouvé: {script_path}")
        raise HTTPException(status_code=404, detail=f"Script non trouvé: {script_name}.sh")

    logger.info(f"Exécution du script : {script_path} avec label: {label}")

    def stream_logs():
        try:
            args = ['bash', script_path] + ([label] if label else [])
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            for line in process.stdout:
                yield f"data: {line}\n\n"

            for err in process.stderr:
                logger.error(f"[stderr] {err.strip()}")
                yield f"data: Erreur: {err}\n\n"

            process.wait()
            yield "event: end\ndata: Fin du script\n\n"
            logger.success(f"Script terminé : {script_name}")

        except Exception as e:
            logger.exception(f"Erreur pendant le script {script_name}")
            yield f"data: Erreur lors de l'exécution: {str(e)}\n\n"

    return StreamingResponse(stream_logs(), media_type="text/event-stream")

@router.post("/update-config")
async def update_config():
    try:
        logger.info("Déchiffrement du fichier YAML via ansible-vault...")
        command = f"ansible-vault view {YAML_PATH} --vault-password-file {VAULT_PASSWORD_FILE}"
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

        if result.returncode != 0:
            logger.error(f"Erreur ansible-vault : {result.stderr.strip()}")
            raise Exception(f"Erreur lors du déchiffrement : {result.stderr}")

        decrypted_yaml_content = result.stdout
        yaml_data = yaml.safe_load(decrypted_yaml_content)

        if not yaml_data:
            logger.warning("Le fichier YAML est vide ou mal formé")
            raise Exception("Le fichier YAML déchiffré est vide ou mal formaté.")

        update_json_files(decrypted_yaml_content)

        if 'cloudflare' in yaml_data:
            cf = yaml_data['cloudflare']
            settings_manager.settings.cloudflare.cloudflare_login = cf.get('login')
            settings_manager.settings.cloudflare.cloudflare_api_key = cf.get('api')

        if 'user' in yaml_data:
            user = yaml_data['user']
            settings_manager.settings.utilisateur.username = user.get('name')
            settings_manager.settings.utilisateur.email = user.get('mail')
            settings_manager.settings.utilisateur.domain = user.get('domain')
            settings_manager.settings.utilisateur.password = user.get('pass')

        settings_manager.save()

        logger.success("Configuration mise à jour avec succès.")

    except Exception as e:
        logger.exception("Erreur dans update-config")
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
        result = {app: f"{sub}.{base_domain}" for app, sub in app_domains.items()}

        logger.info(f"Domaines générés : {result}")
        return result

    except Exception as e:
        logger.exception("Erreur lors de la récupération des domaines")
        raise HTTPException(status_code=500, detail=f"Erreur lecture JSON : {str(e)}")
