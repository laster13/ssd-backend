from fastapi import APIRouter, Depends
from loguru import logger
import asyncio
import json
import urllib.request
from updater.auto_update import main as run_auto_update
from program.managers.sse_manager import sse_manager
from src.version import get_version
from sqlalchemy.orm import Session
from src.integrations.seasonarr.db.models import Notification
from src.integrations.seasonarr.db.database import get_db
from packaging import version  # ✅ pour compare_versions
from src.integrations.seasonarr.db.database import SessionLocal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo


router = APIRouter(prefix="/update", tags=["update"])
scheduler = AsyncIOScheduler(timezone=ZoneInfo("Europe/Paris"))


FRONTEND_VERSION_URL = "https://raw.githubusercontent.com/laster13/ssd-frontend/main/version.json"
BACKEND_VERSION_URL = "https://raw.githubusercontent.com/laster13/ssd-backend/main/version.json"
SAISON_FRONTEND_VERSION_URL = "https://raw.githubusercontent.com/laster13/saison-frontend/main/version.json"


# ==========================================================
# 🧩 1. Lancer la mise à jour manuelle (backend + frontend)
# ==========================================================
@router.post("/run")
async def run_update():
    """
    Déclenche manuellement la mise à jour backend + frontend,
    puis notifie les clients SSE quand elle est terminée.
    """
    logger.info("🔧 Mise à jour manuelle déclenchée via le frontend")

    async def update_task():
        try:
            # 🧩 Lancement complet (backend + frontend)
            run_auto_update()
            logger.success("✅ Mise à jour terminée avec succès")

            # 🔔 Notifie les clients SSE
            sse_manager.publish_event(
                "update_finished",
                {"message": "✅ Mise à jour terminée, rechargez la page."}
            )

        except Exception as e:
            logger.error(f"❌ Erreur pendant la mise à jour manuelle : {e}")
            sse_manager.publish_event(
                "update_error",
                {"message": f"❌ Erreur pendant la mise à jour : {e}"}
            )

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, asyncio.run, update_task())

    return {"status": "update started"}


# ==========================================================
# 🚀 Lancer uniquement la mise à jour BACKEND
# ==========================================================
@router.post("/run/backend")
async def run_update_backend(db: Session = Depends(get_db)):
    """
    Met à jour uniquement le backend. Le nettoyage SQLite est géré
    directement dans auto_update.py avant le redémarrage.
    """
    logger.info("🔧 Mise à jour BACKEND déclenchée")

    try:
        run_auto_update(target="backend")

        # 🔔 Notifie tous les clients via SSE
        sse_manager.publish_event(
            "update_finished",
            {"message": "✅ Mise à jour BACKEND terminée."}
        )

        logger.success("✅ Mise à jour BACKEND terminée avec succès")
        return {"status": "ok", "message": "Mise à jour BACKEND terminée"}

    except Exception as e:
        logger.error(f"❌ Erreur MAJ backend : {e}")
        sse_manager.publish_event("update_error", {"message": str(e)})
        return {"status": "error", "message": f"Erreur MAJ backend : {e}"}


# ==========================================================
# 🎨 Lancer uniquement la mise à jour FRONTEND 
# ==========================================================
@router.post("/run/frontend")
async def run_update_frontend(db: Session = Depends(get_db)):
    """
    Met à jour uniquement le frontend. Le nettoyage SQLite est géré
    directement dans auto_update.py avant le redémarrage.
    """
    logger.info("🎨 Mise à jour FRONTEND déclenchée")

    try:
        run_auto_update(target="frontend")

        # 🔔 Notifie tous les clients via SSE
        sse_manager.publish_event(
            "update_finished",
            {"message": "✅ Mise à jour FRONTEND terminée."}
        )

        logger.success("✅ Mise à jour FRONTEND terminée avec succès")
        return {"status": "ok", "message": "Mise à jour FRONTEND terminée"}

    except Exception as e:
        logger.error(f"❌ Erreur MAJ frontend : {e}")
        sse_manager.publish_event("update_error", {"message": str(e)})
        return {"status": "error", "message": f"Erreur MAJ frontend : {e}"}

# ==========================================================
# 🎬 Lancer uniquement la mise à jour SAISON FRONTEND
# ==========================================================
@router.post("/run/saison_frontend")
async def run_update_saison_frontend(db: Session = Depends(get_db)):
    """
    Met à jour uniquement le frontend Saison/Seasonarr.
    """
    logger.info("🎬 Mise à jour SAISON FRONTEND déclenchée")

    try:
        run_auto_update(target="saison_frontend")

        # 🔔 Notifie tous les clients via SSE
        sse_manager.publish_event(
            "update_finished",
            {"message": "✅ Mise à jour SAISON FRONTEND terminée."}
        )

        logger.success("✅ Mise à jour SAISON FRONTEND terminée avec succès")
        return {"status": "ok", "message": "Mise à jour SAISON FRONTEND terminée"}

    except Exception as e:
        logger.error(f"❌ Erreur MAJ saison frontend : {e}")
        sse_manager.publish_event("update_error", {"message": str(e)})
        return {"status": "error", "message": f"Erreur MAJ saison frontend : {e}"}


# ==========================================================
# 🔍 3. Obtenir la version backend + frontend (pour /admin/update)
# ==========================================================
@router.get("/version")
async def get_versions():
    """
    Retourne la version locale du backend et la version distante du frontend.
    """
    try:
        versions = get_version()
        backend_version = versions.get("backend", "—")
        frontend_version = versions.get("frontend", "—")
        saison_frontend_version = versions.get("saison_frontend", "—")

        # Vérifie aussi la version distante du frontend
        try:
            with urllib.request.urlopen(FRONTEND_VERSION_URL, timeout=5) as response:
                data = json.load(response)
                remote_frontend = data.get("version", "—")
                frontend_version = remote_frontend or frontend_version
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer la version distante du frontend : {e}")

        return {
            "backend": backend_version,
            "frontend": frontend_version,
            "saison_frontend": saison_frontend_version,
        }

    except Exception as e:
        logger.error(f"💥 Erreur lors de la récupération des versions : {e}")
        return {
            "backend": "0.0.0",
            "frontend": "0.0.0",
            "saison_frontend": "0.0.0",
        }

def perform_update_check(db: Session):
    """
    Vérifie s’il existe une nouvelle version du backend, du frontend principal
    et du frontend Saison.
    Compare les fichiers version.json locaux et distants,
    et enregistre une notification persistante si une mise à jour est disponible.
    Supprime les notifications obsolètes si tout est à jour.
    Retourne un dict avec le résultat.
    """
    try:
        # =====================================================
        # 🧩 1. Versions locales (fichiers version.json)
        # =====================================================
        local = get_version()
        local_backend = local.get("backend", "—")
        local_frontend = local.get("frontend", "—")
        local_saison_frontend = local.get("saison_frontend", "—")

        # =====================================================
        # 🧩 2. Versions distantes (GitHub)
        # =====================================================
        remote_backend = "—"
        remote_frontend = "—"
        remote_saison_frontend = "—"

        try:
            with urllib.request.urlopen(BACKEND_VERSION_URL, timeout=5) as response:
                data = json.load(response)
                remote_backend = data.get("version", "—")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer la version BACKEND distante : {e}")

        try:
            with urllib.request.urlopen(FRONTEND_VERSION_URL, timeout=5) as response:
                data = json.load(response)
                remote_frontend = data.get("version", "—")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer la version FRONTEND distante : {e}")

        try:
            with urllib.request.urlopen(SAISON_FRONTEND_VERSION_URL, timeout=5) as response:
                data = json.load(response)
                remote_saison_frontend = data.get("version", "—")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer la version SAISON FRONTEND distante : {e}")

        # =====================================================
        # 🧮 3. Comparaison intelligente
        # =====================================================
        def compare_versions(local_v, remote_v):
            try:
                if local_v in ("—", None) or remote_v in ("—", None):
                    return False

                return version.parse(remote_v) > version.parse(local_v)
            except Exception:
                return remote_v != local_v

        backend_has_update = compare_versions(local_backend, remote_backend)
        frontend_has_update = compare_versions(local_frontend, remote_frontend)
        saison_frontend_has_update = compare_versions(
            local_saison_frontend,
            remote_saison_frontend,
        )

        update_available = (
            backend_has_update
            or frontend_has_update
            or saison_frontend_has_update
        )

        # =====================================================
        # 🧠 4. Si tout est à jour → nettoyage complet
        # =====================================================
        if not update_available:
            db.query(Notification).filter(
                Notification.message_type == "system_update"
            ).delete()
            db.commit()

            return {
                "update_available": False,
                "message": "✅ Toutes les versions sont à jour.",
                "backend": {
                    "current": local_backend,
                    "remote": remote_backend,
                    "has_update": False,
                },
                "frontend": {
                    "current": local_frontend,
                    "remote": remote_frontend,
                    "has_update": False,
                },
                "saison_frontend": {
                    "current": local_saison_frontend,
                    "remote": remote_saison_frontend,
                    "has_update": False,
                },
            }

        # =====================================================
        # 💬 5. Message dynamique
        # =====================================================
        updates = []

        if backend_has_update:
            updates.append(f"BACKEND {remote_backend}")

        if frontend_has_update:
            updates.append(f"FRONTEND {remote_frontend}")

        if saison_frontend_has_update:
            updates.append(f"SAISON FRONTEND {remote_saison_frontend}")

        message = "🚀 Nouvelle version disponible : " + ", ".join(updates)

        # =====================================================
        # 🧱 6. Persistance en base (Notification)
        # =====================================================
        if backend_has_update:
            save_update_notification(db, "backend", remote_backend, message)

        if frontend_has_update:
            save_update_notification(db, "frontend", remote_frontend, message)

        if saison_frontend_has_update:
            save_update_notification(
                db,
                "saison_frontend",
                remote_saison_frontend,
                message,
            )

        # =====================================================
        # 🧾 7. Log + retour
        # =====================================================
        return {
            "update_available": update_available,
            "backend": {
                "current": local_backend,
                "remote": remote_backend,
                "has_update": backend_has_update,
            },
            "frontend": {
                "current": local_frontend,
                "remote": remote_frontend,
                "has_update": frontend_has_update,
            },
            "saison_frontend": {
                "current": local_saison_frontend,
                "remote": remote_saison_frontend,
                "has_update": saison_frontend_has_update,
            },
            "message": message,
        }

    except Exception as e:
        logger.error(f"💥 Erreur pendant la vérification de mise à jour : {e}")

        return {
            "update_available": False,
            "message": "❌ Erreur pendant la vérification des mises à jour.",
            "backend": {
                "current": "—",
                "remote": "—",
                "has_update": False,
            },
            "frontend": {
                "current": "—",
                "remote": "—",
                "has_update": False,
            },
            "saison_frontend": {
                "current": "—",
                "remote": "—",
                "has_update": False,
            },
        }

def scheduled_check_updates():
    """
    Job périodique appelé par APScheduler.
    Il ouvre une session DB, appelle perform_update_check,
    et peut envoyer des events SSE si une mise à jour est dispo.
    """
    db = SessionLocal()
    try:
        result = perform_update_check(db)

        # Si une mise à jour est dispo, on pousse des SSE
        if result.get("update_available"):
            backend = result.get("backend", {}) or {}
            frontend = result.get("frontend", {}) or {}
            saison_frontend = result.get("saison_frontend", {}) or {}

            if backend.get("has_update"):
                sse_manager.publish_event(
                    "update_available_backend",
                    {
                        "message": result.get("message", ""),
                        "version": backend.get("remote"),
                    },
                )

            if frontend.get("has_update"):
                sse_manager.publish_event(
                    "update_available_frontend",
                    {
                        "message": result.get("message", ""),
                        "version": frontend.get("remote"),
                    },
                )

            if saison_frontend.get("has_update"):
                sse_manager.publish_event(
                    "update_available_saison_frontend",
                    {
                        "message": result.get("message", ""),
                        "version": saison_frontend.get("remote"),
                    },
                )

    except Exception as e:
        logger.error(f"💥 Erreur dans scheduled_check_updates : {e}")
    finally:
        db.close()


# ==========================================================
#    4. Vérifier si une mise à jour backend ou frontend est disponible
# ==========================================================

@router.get("/check")
async def check_updates(db: Session = Depends(get_db)):
    """
    Endpoint HTTP qui utilise la logique de perform_update_check.
    Toujours utilisable par ton frontend comme avant.
    """
    return perform_update_check(db)

# ==========================================================
# 🧱 5. Persistance des notifications de mise à jour
# ==========================================================

def save_update_notification(db: Session, target: str, version: str, message: str):
    """
    Crée ou met à jour une notification persistante uniquement
    si la version est nouvelle. Ne duplique pas les notifications identiques.
    """
    existing = (
        db.query(Notification)
        .filter(
            Notification.message_type == "system_update",
            Notification.notification_type == target,
            Notification.extra_data["version"].as_string() == version  # version déjà notifiée
        )
        .first()
    )

    if existing:
        # 🔁 Déjà notifiée → rien à faire
        return

    # 🧹 Marque les anciennes comme lues
    db.query(Notification).filter(
        Notification.message_type == "system_update",
        Notification.notification_type == target,
        Notification.read == False
    ).update({"read": True})

    # 🆕 Crée la nouvelle notification
    notif = Notification(
        user_id=1,
        title=f"Mise à jour {target.upper()} disponible",
        message=message,
        notification_type=target,
        message_type="system_update",
        persistent=True,
        read=False,
        extra_data={"version": version},
    )
    db.add(notif)
    db.commit()
    logger.success(f"🆕 Nouvelle Mise à Jour {target.upper()} enregistrée (v{version})")

def mark_update_as_finished(_, target: str):
    """
    Marque comme lue la notification de mise à jour correspondante,
    en rouvrant une session locale indépendante.
    """
    from sqlalchemy import and_

    target = target.strip().lower()
    db = SessionLocal()

    try:
        notif = (
            db.query(Notification)
            .filter(
                and_(
                    Notification.message_type == "system_update",
                    Notification.notification_type.ilike(f"%{target}%"),
                    Notification.read == False,
                )
            )
            .order_by(Notification.created_at.desc())
            .first()
        )

        if notif:
            notif.read = True
            db.commit()
            logger.success(f"✅ Notification {target.upper()} marquée comme lue (id={notif.id}).")
        else:
            logger.warning(f"⚠️ Aucune notification non lue trouvée pour {target}.")
    except Exception as e:
        logger.error(f"💥 Erreur mark_update_as_finished({target}) : {e}")
    finally:
        db.close()

@router.get("/persistent")
def get_persistent_update_notification(db: Session = Depends(get_db)):
    """
    Retourne la notification persistante de mise à jour (si existante).
    """
    notif = (
        db.query(Notification)
        .filter(Notification.message_type == "system_update",
                Notification.read == False)
        .order_by(Notification.created_at.desc())
        .first()
    )
    if not notif:
        return {"has_update": False}

    return {
        "has_update": True,
        "type": notif.notification_type,
        "message": notif.message,
        "version": notif.extra_data.get("version") if notif.extra_data else None,
    }

@router.on_event("startup")
def start_update_scheduler():
    """
    Démarre le scheduler qui vérifie les mises à jour régulièrement.
    Ici : toutes les 6 heures à minute 0.
    """
    scheduler.add_job(
        scheduled_check_updates,
        CronTrigger(minute="*/10"),
        id="check_updates",
        replace_existing=True,
    )

    if not scheduler.running:
        scheduler.start()
        logger.info("⏰ Scheduler de vérification de mises à jour démarré")
