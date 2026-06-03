from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from fastapi.responses import StreamingResponse

import os
import subprocess
import yaml

from loguru import logger
from program.json_manager import update_json_files, load_json_from_file
from program.settings.manager import settings_manager


USER = os.getenv("SSD_USER") or os.getenv("USER") or os.getlogin()

FRONTEND_CONTAINER = os.getenv("FRONTEND_CONTAINER", "ssd-frontend")
SCRIPTS_DIR = os.getenv("SCRIPTS_DIR", "/app/scripts")

YAML_PATH = os.getenv(
    "ANSIBLE_VARS",
    f"/home/{USER}/.ansible/inventories/group_vars/all.yml"
)

VAULT_PASSWORD_FILE = os.getenv(
    "ANSIBLE_VAULT_PASSWORD_FILE",
    f"/home/{USER}/.vault_pass"
)

BACKEND_JSON_PATH = os.getenv(
    "BACKEND_SETTINGS_PATH",
    "/app/data/settings.json"
)

FRONTEND_JSON_PATH = os.getenv(
    "FRONTEND_SETTINGS_PATH",
    "/shared/ssd-frontend/static/settings.json"
)

router = APIRouter(
    prefix="/scripts",
    tags=["scripts"],
)


class ScriptModel(BaseModel):
    name: str
    params: list[str] = []


@router.get("/check-file")
async def check_file():
    file_path = f"/home/{USER}/seedbox-compose/ssddb"

    try:
        exists = os.path.exists(file_path)
        logger.info(
            f"Vérification du fichier : {file_path} → {'existe' if exists else 'inexistant'}"
        )
        return {"exists": exists}

    except Exception as e:
        logger.exception("Erreur check_file")
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")


@router.get("/run/{script_name}")
async def run_script(
    script_name: str,
    label: str = Query(None, description="Label du conteneur")
):
    if not script_name.isalnum():
        raise HTTPException(status_code=400, detail="Nom de script invalide.")

    script_path = os.path.join(SCRIPTS_DIR, f"{script_name}.sh")

    logger.info(
        f"Exécution du script dans {FRONTEND_CONTAINER}: {script_path} avec label: {label}"
    )

    def stream_logs():
        try:
            check_args = [
                "docker",
                "exec",
                FRONTEND_CONTAINER,
                "test",
                "-f",
                script_path
            ]

            check = subprocess.run(
                check_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            if check.returncode != 0:
                logger.warning(
                    f"Script non trouvé dans {FRONTEND_CONTAINER}: {script_path}"
                )
                yield f"data: Script non trouvé: {script_name}.sh\n\n"
                yield "event: end\ndata: Fin du script\n\n"
                return

            args = [
                "docker",
                "exec",
                FRONTEND_CONTAINER,
                "bash",
                script_path
            ]

            if label:
                args.append(label)

            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            if process.stdout:
                for line in process.stdout:
                    yield f"data: {line}\n\n"

            if process.stderr:
                for err in process.stderr:
                    logger.error(f"[stderr] {err.strip()}")
                    yield f"data: Erreur: {err}\n\n"

            process.wait()

            if process.returncode != 0:
                logger.error(
                    f"Script {script_name} terminé avec code {process.returncode}"
                )
                yield f"data: Erreur: script terminé avec code {process.returncode}\n\n"
            else:
                logger.success(f"Opération terminée avec succès : {script_name}")

            yield "event: end\ndata: Fin du script\n\n"

        except Exception as e:
            logger.exception(f"Erreur pendant le script {script_name}")
            yield f"data: Erreur lors de l'exécution: {str(e)}\n\n"
            yield "event: end\ndata: Fin du script\n\n"

    return StreamingResponse(stream_logs(), media_type="text/event-stream")


@router.post("/update-config")
async def update_config():
    try:
        logger.info("Déchiffrement du fichier YAML via ansible-vault...")

        command = [
            "ansible-vault",
            "view",
            YAML_PATH,
            "--vault-password-file",
            VAULT_PASSWORD_FILE
        ]

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.returncode != 0:
            logger.error(f"Erreur ansible-vault : {result.stderr.strip()}")
            raise Exception(f"Erreur lors du déchiffrement : {result.stderr}")

        decrypted_yaml_content = result.stdout
        yaml_data = yaml.safe_load(decrypted_yaml_content)

        if not yaml_data:
            logger.warning("Le fichier YAML est vide ou mal formé")
            raise Exception("Le fichier YAML déchiffré est vide ou mal formaté.")

        update_json_files(decrypted_yaml_content)

        try:
            if "cloudflare" in yaml_data:
                cf = yaml_data["cloudflare"]
                settings_manager.settings.cloudflare.cloudflare_login = cf.get("login")
                settings_manager.settings.cloudflare.cloudflare_api_key = cf.get("api")

            if "user" in yaml_data:
                user = yaml_data["user"]
                settings_manager.settings.utilisateur.username = user.get("name")
                settings_manager.settings.utilisateur.email = user.get("mail")
                settings_manager.settings.utilisateur.domain = user.get("domain")
                settings_manager.settings.utilisateur.password = user.get("pass")

            settings_manager.save()

        except Exception as settings_error:
            logger.warning(
                f"Settings manager non mis à jour, JSON déjà synchronisés : {settings_error}"
            )

        logger.success("Configuration mise à jour avec succès.")

        return {
            "message": "Configuration mise à jour avec succès.",
            "frontend_settings_path": FRONTEND_JSON_PATH,
            "backend_settings_path": BACKEND_JSON_PATH
        }

    except Exception as e:
        logger.exception("Erreur dans update-config")
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lors de la mise à jour : {str(e)}"
        )

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

        result = {
            app: f"{sub}.{base_domain}"
            for app, sub in app_domains.items()
        }

        return result

    except Exception as e:
        logger.exception("Erreur lors de la récupération des domaines")
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lecture JSON : {str(e)}"
        )