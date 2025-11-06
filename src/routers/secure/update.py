from fastapi import APIRouter, Request, Depends
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


router = APIRouter(prefix="/update", tags=["update"])

FRONTEND_VERSION_URL = "https://raw.githubusercontent.com/laster13/ssd-frontend/main/version.json"
BACKEND_VERSION_URL = "https://raw.githubusercontent.com/laster13/ssd-backend/main/version.json"



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
            # 🧩 Lance la mise à jour complète
            run_auto_update()
            logger.success("✅ Mise à jour terminée avec succès")

            # 🔔 Notifie tous les clients via SSE
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

    # Lancement en arrière-plan
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, asyncio.run, update_task())

    return {"status": "update started"}

# ==========================================================
# 🚀 Lancer uniquement la mise à jour BACKEND
# ==========================================================
@router.post("/run/backend")
async def run_update_backend(db: Session = Depends(get_db)):
    logger.info("🔧 Mise à jour BACKEND déclenchée")
    try:
        # 🧩 Lance la mise à jour du backend
        run_auto_update(target="backend")

        # ✅ Marque la notification persistante comme terminée (si existante)
        try:
            mark_update_as_finished(db, target="backend")
            logger.info("🧹 Notification BACKEND marquée comme terminée.")
        except Exception as notif_err:
            logger.warning(f"⚠️ Impossible de marquer la notification backend comme terminée : {notif_err}")

        # 🔔 Notifie tous les clients connectés via SSE
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
    logger.info("🎨 Mise à jour FRONTEND déclenchée")
    try:
        # 🧩 Lance la mise à jour du frontend
        run_auto_update(target="frontend")

        # ✅ Marque la notification persistante comme terminée (si existante)
        try:
            mark_update_as_finished(db, target="frontend")
            logger.info("🧹 Notification FRONTEND marquée comme terminée.")
        except Exception as notif_err:
            logger.warning(f"⚠️ Impossible de marquer la notification frontend comme terminée : {notif_err}")

        # 🔔 Notifie tous les clients connectés via SSE
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
# 🧠 2. Notification SSE “update_finished”
# ==========================================================
@router.post("/sse/update_finished")
async def notify_update_finished(request: Request):
    """
    Permet à un script externe (auto_update.py) d’envoyer la notification SSE
    après la fin d’une mise à jour.
    """
    payload = await request.json()
    event_type = payload.get("event", "update_finished")  # 👈 ajoute cette ligne
    sse_manager.publish_event(event_type, payload)         # 👈 utilise event_type
    return {"status": "ok"}


# ==========================================================
# 🔍 3. Obtenir la version backend + frontend (pour /admin/update)
# ==========================================================
@router.get("/version")
async def get_versions():
    """
    Retourne la version locale du backend et la version distante du frontend.
    """
    try:
        versions = get_version()  # 🔥 on récupère ton dict propre {'backend': '1.0.1', 'frontend': '1.0.0'}
        backend_version = versions["backend"]
        frontend_version = versions["frontend"]

        # Vérifie aussi la version distante du frontend
        try:
            with urllib.request.urlopen(FRONTEND_VERSION_URL, timeout=5) as response:
                data = json.load(response)
                remote_frontend = data.get("version", "—")
                frontend_version = remote_frontend or frontend_version
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer la version distante du frontend : {e}")

        return {"backend": backend_version, "frontend": frontend_version}

    except Exception as e:
        logger.error(f"💥 Erreur lors de la récupération des versions : {e}")
        return {"backend": "0.0.0", "frontend": "0.0.0"}

# ==========================================================
# 🔎 4. Vérifier si une mise à jour backend ou frontend est disponible
# ==========================================================

@router.get("/check")
async def check_updates(db: Session = Depends(get_db)):
    """
    Vérifie s’il existe une nouvelle version du backend et du frontend.
    Compare les fichiers version.json locaux et distants,
    et enregistre une notification persistante si une mise à jour est disponible.
    """
    try:
        # =====================================================
        # 🧩 1. Versions locales (fichiers version.json)
        # =====================================================
        local = get_version()
        local_backend = local.get("backend", "—")
        local_frontend = local.get("frontend", "—")

        # =====================================================
        # 🧩 2. Versions distantes (GitHub)
        # =====================================================
        remote_backend = "—"
        remote_frontend = "—"

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

        # =====================================================
        # 🧮 3. Comparaison intelligente
        # =====================================================
        def compare_versions(local_v, remote_v):
            try:
                return version.parse(remote_v) > version.parse(local_v)
            except Exception:
                return remote_v != local_v

        backend_has_update = compare_versions(local_backend, remote_backend)
        frontend_has_update = compare_versions(local_frontend, remote_frontend)
        update_available = backend_has_update or frontend_has_update

        # =====================================================
        # 💬 4. Message dynamique
        # =====================================================
        if backend_has_update and frontend_has_update:
            message = f"🚀 Nouvelle version BACKEND {remote_backend} et FRONTEND {remote_frontend} disponibles"
        elif backend_has_update:
            message = f"🚀 Nouvelle version BACKEND {remote_backend} disponible"
        elif frontend_has_update:
            message = f"🎨 Nouvelle version FRONTEND {remote_frontend} disponible"
        else:
            message = "✅ Toutes les versions sont à jour."

        # =====================================================
        # 🧱 5. Persistance en base (Notification)
        # =====================================================
        # Utilise la fonction globale (corrigée) définie plus bas
        if backend_has_update:
            save_update_notification(db, "backend", remote_backend, message)
        if frontend_has_update:
            save_update_notification(db, "frontend", remote_frontend, message)

        # =====================================================
        # 🧾 6. Log + retour
        # =====================================================
        result = {
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
            "message": message,
        }

        logger.info(f"🔍 Vérification de mise à jour : {result}")
        return result

    except Exception as e:
        logger.error(f"💥 Erreur pendant la vérification de mise à jour : {e}")
        return {
            "update_available": False,
            "message": "❌ Erreur pendant la vérification des mises à jour.",
        }

# ==========================================================
# 🧱 5. Persistance des notifications de mise à jour
# ==========================================================

def save_update_notification(db: Session, target: str, version: str, message: str):
    """
    Crée ou met à jour une notification persistante en base
    pour signaler une mise à jour disponible.
    Nettoie d'abord toutes les anciennes notifications non lues.
    """
    # 🧹 Nettoie les anciennes notifications du même type encore non lues
    db.query(Notification).filter(
        Notification.message_type == "system_update",
        Notification.notification_type == target,
        Notification.read == False
    ).update({"read": True})

    # 🆕 Crée une nouvelle notification propre
    notif = Notification(
        user_id=1,  # ou un utilisateur système si applicable
        title=f"Mise à jour {target.upper()} disponible",
        message=message,
        notification_type=target,  # backend ou frontend
        message_type="system_update",
        persistent=True,
        read=False,
        extra_data={"version": version},
    )
    db.add(notif)
    db.commit()

    logger.info(f"🆕 Nouvelle notification persistante {target.upper()} enregistrée (v{version})")

def mark_update_as_finished(db: Session, target: str):
    """
    Marque comme lue la notification de mise à jour correspondante.
    """
    notif = (
        db.query(Notification)
        .filter(
            Notification.message_type == "system_update",
            Notification.notification_type.ilike(target),  # insensible à la casse
            Notification.read == False
        )
        .order_by(Notification.created_at.desc())
        .first()
    )

    if notif:
        notif.read = True
        db.commit()
        logger.info(f"✅ Notification {target.upper()} marquée comme lue en base (id={notif.id}).")
    else:
        logger.warning(f"⚠️ Aucune notification non lue trouvée pour {target}.")

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
