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
import time
import subprocess
import psutil
import subprocess
import os


USER = getpass.getuser()

router = APIRouter(
    prefix="/docker",
    tags=["DockerBackups"],
)

scheduler = AsyncIOScheduler()


# 🗄️ Cache local pour /stats
_last_stats = None
_last_stats_time = 0
_STATS_TTL = 0.5  # secondes

# 🗄️ Cache local pour /containers
_last_containers = None
_last_containers_time = 0
_CONTAINERS_TTL = 2  # secondes (docker ps est plus lourd)

DOCKER_CONFIGS_PATH = Path(f"/home/{USER}/seedbox/docker/{USER}")
SETTINGS_FILE = Path("data/backup.json")
BACKUP_ROOT = Path(f"/home/{USER}/Backups")


# ===== Décorateur pour gérer les slashes =====
def route_with_slash(router: APIRouter, path: str, **kwargs):
    """Enregistre la route avec et sans slash final."""
    def decorator(func):
        router.add_api_route(path, func, **kwargs)
        if not path.endswith("/"):
            router.add_api_route(path + "/", func, **kwargs)
        else:
            router.add_api_route(path.rstrip("/"), func, **kwargs)
        return func
    return decorator


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


# ===== Routes API avec gestion auto du slash =====
@route_with_slash(router, "/scan", methods=["GET"])
def get_folders():
    return scan_folders()


@route_with_slash(router, "", methods=["GET"])
def get_schedules():
    return load_config()


@route_with_slash(router, "/schedule", methods=["POST"])
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


@route_with_slash(router, "/run", methods=["POST"])
def run_now(data: dict):
    name = data.get("name")
    if name not in scan_folders():
        raise HTTPException(status_code=404, detail="Dossier Docker introuvable")
    run_backup(name)
    return {"message": f"Sauvegarde lancée pour {name}"}


@route_with_slash(router, "/schedule/{name}", methods=["DELETE"])
def delete_schedule(name: str):
    config = load_config()
    if name not in config:
        raise HTTPException(status_code=404, detail="Planification introuvable")

    del config[name]
    save_config(config)
    schedule_all()
    return {"message": f"Planification supprimée pour {name}"}


@route_with_slash(router, "/backups/{name}", methods=["GET"])
def list_backups(name: str):
    backup_dir = BACKUP_ROOT / "docker" / name
    if not backup_dir.exists():
        return []
    backups = sorted([f.name for f in backup_dir.glob("*.tar.gz")])
    return JSONResponse(content=backups)


@route_with_slash(router, "/restore", methods=["POST"])
def restore_backup(data: dict):
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
        subprocess.run(
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

# utilitaire pour / et /containers/
def route_with_slash(router: APIRouter, path: str, **kwargs):
    """Enregistre la route avec et sans slash final."""
    def decorator(func):
        router.add_api_route(path, func, **kwargs)
        if not path.endswith("/"):
            router.add_api_route(path + "/", func, **kwargs)
        else:
            router.add_api_route(path.rstrip("/"), func, **kwargs)
        return func
    return decorator

# 📦 Liste des conteneurs avec CPU et RAM
@route_with_slash(router, "/containers", methods=["GET"])
def list_containers():
    """Liste les conteneurs Docker avec CPU/MEM si dispo"""
    global _last_containers, _last_containers_time
    try:
        now = time.time()
        if _last_containers and (now - _last_containers_time) < _CONTAINERS_TTL:
            return _last_containers

        # 1. Lister tous les conteneurs
        ps_result = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.ID}}|{{.Names}}|{{.Status}}"],
            capture_output=True, text=True, check=True
        )

        containers = []
        for line in ps_result.stdout.strip().splitlines():
            parts = line.split("|", 2)
            if len(parts) < 3:
                continue
            cid, name, status = parts
            containers.append({
                "id": cid,
                "name": name,
                "status": status,
                "cpu": "N/A",
                "mem": "N/A"
            })

        # 2. Récup stats CPU/MEM (uniquement pour conteneurs en cours)
        stats_result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.ID}}|{{.CPUPerc}}|{{.MemPerc}}"],
            capture_output=True, text=True, check=True
        )

        stats_map = {}
        for line in stats_result.stdout.strip().splitlines():
            parts = line.split("|", 2)
            if len(parts) < 3:
                continue
            cid, cpu, mem = parts
            stats_map[cid] = {"cpu": cpu, "mem": mem}

        # 3. Fusionner
        for c in containers:
            if c["id"] in stats_map:
                c.update(stats_map[c["id"]])

        _last_containers = containers
        _last_containers_time = now
        return containers

    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Erreur docker: {e.stderr.strip()}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur inattendue: {str(e)}")

# 🖥️ Stats système globales (CPU + RAM + DISK)
@route_with_slash(router, "/stats", methods=["GET"])
def get_system_stats():
    """Stats système globales CPU + RAM + Disque + Uptime + Load + Réseau + IO"""
    global _last_stats, _last_stats_time
    try:
        now = time.time()
        if _last_stats and (now - _last_stats_time) < _STATS_TTL:
            return _last_stats

        # CPU & RAM
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()

        # Disque
        disk = psutil.disk_usage("/")
        
        # Uptime
        boot_time = psutil.boot_time()
        uptime_seconds = time.time() - boot_time
        days, remainder = divmod(uptime_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, _ = divmod(remainder, 60)
        uptime_str = f"{int(days)}j {int(hours)}h {int(minutes)}m"

        # Load avg (Linux only)
        try:
            load1, load5, load15 = os.getloadavg()
        except OSError:
            load1 = load5 = load15 = 0.0

        # Réseau
        net = psutil.net_io_counters()
        net_sent = round(net.bytes_sent / (1024**2), 2)
        net_recv = round(net.bytes_recv / (1024**2), 2)

        # Disque I/O
        disk_io = psutil.disk_io_counters()
        read_mb = round(disk_io.read_bytes / (1024**2), 2)
        write_mb = round(disk_io.write_bytes / (1024**2), 2)

        stats = {
            "cpu": cpu,
            "memory": {
                "percent": mem.percent,
                "used": round(mem.used / (1024**3), 2),
                "total": round(mem.total / (1024**3), 2)
            },
            "disk": {
                "percent": disk.percent,
                "used": round(disk.used / (1024**3), 2),
                "total": round(disk.total / (1024**3), 2)
            },
            "uptime": uptime_str,
            "load": {
                "1m": round(load1, 2),
                "5m": round(load5, 2),
                "15m": round(load15, 2),
            },
            "network": {
                "sent_mb": net_sent,
                "recv_mb": net_recv
            },
            "disk_io": {
                "read_mb": read_mb,
                "write_mb": write_mb
            }
        }

        _last_stats = stats
        _last_stats_time = now
        return stats

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ▶️⏹️🔄 Actions sur les conteneurs
@route_with_slash(router, "", methods=["POST"])
def docker_action(data: dict):
    container_id = data.get("id")
    action = data.get("action")

    if not container_id or not action:
        raise HTTPException(status_code=400, detail="Paramètres manquants")

    if action not in ["start", "stop", "restart"]:
        raise HTTPException(status_code=400, detail="Action invalide")

    try:
        subprocess.run(
            ["docker", action, container_id],
            check=True,
            capture_output=True,
            text=True
        )
        return {"message": f"Container {action}ed"}
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Erreur {action}: {e.stderr.strip()}")

@route_with_slash(router, "/sensors", methods=["GET"])
def get_sensors():
    try:
        temps_data = []
        fans_data = []

        # Récup températures
        temps = psutil.sensors_temperatures(fahrenheit=False)
        if temps:
            for name, entries in temps.items():
                for e in entries:
                    temps_data.append({
                        "chip": name,
                        "label": e.label or "N/A",
                        "temp": e.current
                    })

        # Récup ventilateurs
        fans = psutil.sensors_fans()
        if fans:
            for name, entries in fans.items():
                for e in entries:
                    fans_data.append({
                        "chip": name,
                        "label": e.label or "fan",
                        "rpm": e.current
                    })

        return {
            "temperatures": temps_data,
            "fans": fans_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))