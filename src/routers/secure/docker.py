from fastapi import APIRouter, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path
from datetime import datetime
import shutil
import json
import getpass
from zoneinfo import ZoneInfo
from fastapi.responses import JSONResponse
from loguru import logger

USER = getpass.getuser()

router = APIRouter(prefix="/docker", tags=["DockerBackups"])
scheduler = AsyncIOScheduler()

DOCKER_CONFIGS_PATH = Path(f"/home/{USER}/seedbox/docker/{USER}")
SETTINGS_FILE = Path("data/backup.json")
BACKUP_ROOT = Path(f"/home/{USER}/Backups")


# 🔍 Scanner les répertoires Docker (containers)
def scan_folders():
    if not DOCKER_CONFIGS_PATH.exists():
        logger.warning(f"Dossier introuvable : {DOCKER_CONFIGS_PATH}")
        return []
    return [f.name for f in DOCKER_CONFIGS_PATH.iterdir() if f.is_dir()]


# 🔄 Charger les planifications Docker
def load_config():
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE) as f:
                settings = json.load(f)
                return settings.get("docker_schedules", {})
        except json.JSONDecodeError:
            logger.error("Fichier JSON invalide : data/backup.json")
    return {}


# 💾 Sauvegarder les planifications Docker
def save_config(docker_schedules):
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    settings = {}

    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE) as f:
                settings = json.load(f)
        except json.JSONDecodeError:
            logger.warning("Fichier JSON corrompu, réinitialisation.")

    settings["docker_schedules"] = docker_schedules

    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)
    logger.success("Planification Docker sauvegardée.")


# ⚙️ Exécuter une sauvegarde immédiate
def run_backup(name):
    source = DOCKER_CONFIGS_PATH / name
    if not source.exists():
        logger.warning(f"Dossier Docker introuvable : {source}")
        return

    timestamp = datetime.now(ZoneInfo("Europe/Paris")).strftime("%d%m%Y_%H%M%S")
    dest_dir = BACKUP_ROOT / "docker" / name
    dest_dir.mkdir(parents=True, exist_ok=True)
    archive_path = dest_dir / f"{name}_{timestamp}.tar.gz"

    shutil.make_archive(str(archive_path).replace(".tar.gz", ""), 'gztar', str(source))
    logger.success(f"Sauvegarde créée pour {name} → {archive_path}")


# ⏰ Programmer toutes les sauvegardes planifiées
def schedule_all():
    scheduler.remove_all_jobs()
    config = load_config()

    for name, sched in config.items():
        if isinstance(sched, dict):
            day = sched.get("day")
            hour = sched.get("hour")
            if day is not None and hour is not None:
                def job(name=name):
                    return lambda: run_backup(name)

                scheduler.add_job(
                    job(),
                    CronTrigger(day_of_week=day, hour=hour),
                    id=f"docker_{name}"
                )
                logger.debug(f"Tâche Docker planifiée : {name} ({day=}, {hour=})")


@router.on_event("startup")
def on_startup():
    schedule_all()
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler Docker démarré.")


# 🔍 GET /docker/scan : liste des containers
@router.get("/scan")
def get_folders():
    return scan_folders()


# 📄 GET /docker : lire les horaires planifiés
@router.get("")
def get_schedules():
    return load_config()


# 💾 POST /docker/schedule : enregistrer une planification
@router.post("/schedule")
def save_schedule(data: dict):
    name = data.get("name")
    day = data.get("day")
    hour = data.get("hour")

    if name not in scan_folders():
        raise HTTPException(status_code=404, detail="Dossier Docker introuvable")

    config = load_config()
    config[name] = {"day": day, "hour": hour}
    save_config(config)
    schedule_all()
    return {"message": f"Sauvegarde planifiée pour {name}"}


# ▶️ POST /docker/run : lancer une sauvegarde immédiate
@router.post("/run")
def run_now(data: dict):
    name = data.get("name")
    if name not in scan_folders():
        raise HTTPException(status_code=404, detail="Dossier Docker introuvable")
    run_backup(name)
    return {"message": f"Sauvegarde lancée pour {name}"}


# ❌ DELETE /docker/schedule/{name} : supprimer une planification
@router.delete("/schedule/{name}")
def delete_schedule(name: str):
    config = load_config()
    if name not in config:
        raise HTTPException(status_code=404, detail="Planification introuvable")

    del config[name]
    save_config(config)
    schedule_all()
    return {"message": f"Planification supprimée pour {name}"}

@router.get("/backups/{name}/")
def list_backups(name: str):
    backup_dir = BACKUP_ROOT / "docker" / name
    if not backup_dir.exists():
        return []

    backups = sorted([f.name for f in backup_dir.glob("*.tar.gz")])
    return JSONResponse(content=backups)

import time  # à mettre en haut du fichier avec les autres imports

@router.post("/restore/")
def restore_backup(data: dict):
    import subprocess

    name = data.get("name")
    filename = data.get("file")

    if not name or not filename:
        raise HTTPException(status_code=400, detail="Paramètres manquants.")

    archive_path = BACKUP_ROOT / "docker" / name / filename
    target_path = DOCKER_CONFIGS_PATH / name

    logger.debug(f"Nom du conteneur à restaurer: {name}")
    logger.debug(f"Fichier à restaurer: {filename}")
    logger.debug(f"Chemin archive: {archive_path}")
    logger.debug(f"Chemin destination: {target_path}")

    if not archive_path.exists():
        raise HTTPException(status_code=404, detail="Fichier de sauvegarde introuvable.")

    if target_path.exists():
        logger.debug(f"✅ Suppression du dossier cible {target_path}")
        shutil.rmtree(target_path)
    else:
        logger.warning(f"❌ Dossier à supprimer introuvable : {target_path}")

    shutil.unpack_archive(str(archive_path), str(target_path))
    logger.success(f"Sauvegarde {filename} restaurée dans {target_path}")

    extracted_files = list(target_path.rglob("*"))
    logger.info(f"{len(extracted_files)} fichiers extraits pour {name}")

    # 💤 Petite pause pour éviter de redémarrer trop vite
    logger.debug("⏳ Pause 5 secondes pour garantir la stabilité du répertoire restauré...")
    time.sleep(5)

    try:
        result = subprocess.run(
            ["docker", "restart", name],
            check=True,
            capture_output=True,
            text=True
        )
        logger.success(f"🔁 Conteneur {name} redémarré avec succès")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Échec du redémarrage de {name}")
        logger.error(f"Stderr: {e.stderr.strip()}")
        raise HTTPException(status_code=500, detail=f"Échec du redémarrage de {name}")

    return {"message": f"Sauvegarde {filename} restaurée et conteneur {name} redémarré."}


