from fastapi import APIRouter, Request
from loguru import logger
import asyncio
import json
import urllib.request
from updater.auto_update import main as run_auto_update
from program.managers.sse_manager import sse_manager
from src.version import get_version

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
async def run_update_backend():
    logger.info("🔧 Mise à jour BACKEND déclenchée")
    try:
        # Exemple : ton script actuel fait déjà tout, tu peux passer un paramètre
        run_auto_update(target="backend")

        sse_manager.publish_event(
            "update_finished",
            {"message": "✅ Mise à jour BACKEND terminée."}
        )
        return {"status": "ok", "message": "Mise à jour BACKEND terminée"}
    except Exception as e:
        logger.error(f"❌ Erreur MAJ backend : {e}")
        sse_manager.publish_event("update_error", {"message": str(e)})
        return {"status": "error", "message": str(e)}

# ==========================================================
# 🎨 Lancer uniquement la mise à jour FRONTEND
# ==========================================================
@router.post("/run/frontend")
async def run_update_frontend():
    logger.info("🎨 Mise à jour FRONTEND déclenchée")
    try:
        run_auto_update(target="frontend")

        sse_manager.publish_event(
            "update_finished",
            {"message": "✅ Mise à jour FRONTEND terminée."}
        )
        return {"status": "ok", "message": "Mise à jour FRONTEND terminée"}
    except Exception as e:
        logger.error(f"❌ Erreur MAJ frontend : {e}")
        sse_manager.publish_event("update_error", {"message": str(e)})
        return {"status": "error", "message": str(e)}


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
async def check_updates():
    """
    Vérifie s’il existe une nouvelle version du backend et du frontend.
    Compare les fichiers version.json locaux et distants.
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

        # --- Backend distant
        try:
            with urllib.request.urlopen(BACKEND_VERSION_URL, timeout=5) as response:
                data = json.load(response)
                remote_backend = data.get("version", "—")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer la version BACKEND distante : {e}")

        # --- Frontend distant
        try:
            with urllib.request.urlopen(FRONTEND_VERSION_URL, timeout=5) as response:
                data = json.load(response)
                remote_frontend = data.get("version", "—")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de récupérer la version FRONTEND distante : {e}")

        # =====================================================
        # 🧮 3. Comparaison intelligente (version.parse)
        # =====================================================
        def compare_versions(local_v, remote_v):
            try:
                return version.parse(remote_v) > version.parse(local_v)
            except Exception:
                return remote_v != local_v  # fallback simple

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
        # 🧾 5. Log + retour
        # =====================================================
        result = {
            "update_available": update_available,
            "backend": {
                "current": local_backend,
                "remote": remote_backend,
                "has_update": backend_has_update
            },
            "frontend": {
                "current": local_frontend,
                "remote": remote_frontend,
                "has_update": frontend_has_update
            },
            "message": message
        }

        logger.info(f"🔍 Vérification de mise à jour : {result}")
        return result

    except Exception as e:
        logger.error(f"💥 Erreur pendant la vérification de mise à jour : {e}")
        return {
            "update_available": False,
            "message": "❌ Erreur pendant la vérification des mises à jour."
        }