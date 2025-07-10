from fastapi import APIRouter, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path
from datetime import datetime
import shutil
import json
import getpass

from loguru import logger

router = APIRouter(prefix="/media-backups", tags=["MediaBackups"])
scheduler = AsyncIOScheduler()

USER = getpass.getuser()
MEDIAS_PATH = Path(f"/home/{USER}/Medias")
SETTINGS_FILE = Path("data/backup.json")
BACKUP_ROOT = Path(f"/home/{USER}/Backups")


def scan_folders():
    if not MEDIAS_PATH.exists():
        logger.warning(f"Le dossier {MEDIAS_PATH} est introuvable.")
        return []
    return [f.name for f in MEDIAS_PATH.iterdir() if f.is_dir()]


def load_config():
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE) as f:
                settings = json.load(f)
                return settings.get("schedules", {})
        except json.JSONDecodeError:
            logger.error("Fichier JSON invalide : data/backup.json")
    return {}


def save_config(schedules):
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    settings = {}
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE) as f:
                settings = json.load(f)
        except json.JSONDecodeError:
            logger.warning("Fichier JSON corrompu, il sera réinitialisé.")

    settings["schedules"] = schedules

    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)
    logger.success("Configuration des sauvegardes enregistrée.")


def run_backup(subfolder):
    source = MEDIAS_PATH / subfolder
    if not source.exists():
        logger.warning(f"Dossier introuvable : {source}")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_dir = BACKUP_ROOT / subfolder
    dest_dir.mkdir(parents=True, exist_ok=True)
    archive_path = dest_dir / f"{subfolder}_{timestamp}.tar.gz"

    shutil.make_archive(str(archive_path).replace(".tar.gz", ""), 'gztar', str(source))
    logger.success(f"Backup de {subfolder} créée → {archive_path}")


def schedule_all():
    scheduler.remove_all_jobs()
    config = load_config()

    for name, sched in config.items():
        if isinstance(sched, dict):
            day = sched.get("day")
            hour = sched.get("hour")

            if day is not None and hour is not None:
                def create_job(folder=name):
                    return lambda: run_backup(folder)

                scheduler.add_job(
                    create_job(),
                    CronTrigger(day_of_week=day, hour=hour),
                    id=name
                )
                logger.debug(f"Tâche planifiée : {name} ({day=}, {hour=})")


@router.on_event("startup")
def startup_event():
    schedule_all()
    if not scheduler.running:
        scheduler.start()
        logger.info("Planificateur (scheduler) démarré")


@router.get("/scan")
async def get_media_folders():
    return scan_folders()


@router.get("")
@router.get("/")
async def get_schedules():
    return load_config()


@router.post("/schedule")
async def schedule_folder(data: dict):
    name = data.get("name")
    day = data.get("day")
    hour = data.get("hour")

    if name not in scan_folders():
        raise HTTPException(status_code=404, detail="Dossier inexistant")

    config = load_config()
    config[name] = {"day": day, "hour": hour}
    save_config(config)
    schedule_all()
    return {"message": f"Sauvegarde planifiée pour {name}"}


@router.post("/run")
async def run_now(data: dict):
    name = data.get("name")
    if name not in scan_folders():
        raise HTTPException(status_code=404, detail="Dossier inexistant")
    run_backup(name)
    return {"message": f"Backup lancée pour {name}"}
