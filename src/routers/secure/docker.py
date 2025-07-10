from fastapi import APIRouter, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path
from datetime import datetime
import shutil
import json
import getpass

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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
