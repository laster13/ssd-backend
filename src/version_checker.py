# backend/src/program/version_checker.py

import asyncio
import json
import urllib.request
from loguru import logger
from pathlib import Path
from program.managers.sse_manager import sse_manager
from src.version import get_version

# URLs GitHub des fichiers version.json
BACKEND_VERSION_URL = "https://raw.githubusercontent.com/laster13/ssd-backend/main/version.json"
FRONTEND_VERSION_URL = "https://raw.githubusercontent.com/laster13/ssd-frontend/main/version.json"

# Intervalle de vérification (minutes)
CHECK_INTERVAL_MINUTES = 10


# ==========================================================
# 🔹 Utilitaires
# ==========================================================

def get_remote_version(url: str) -> str:
    """Récupère la version distante (backend ou frontend) depuis GitHub."""
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.load(response)
            return data.get("version", "0.0.0")
    except Exception as e:
        logger.error(f"❌ Impossible de lire la version distante depuis {url} : {e}")
        return "0.0.0"


# ==========================================================
# 🔄 Boucle de surveillance
# ==========================================================

async def check_updates_loop():
    """Surveille les mises à jour toutes les X minutes et notifie via SSE."""
    await asyncio.sleep(10)  # laisse le backend démarrer

    current_local = get_version()
    current_backend = current_local.get("backend", "0.0.0")
    current_frontend = current_local.get("frontend", "0.0.0")

    logger.info(f"🔍 Surveillance des mises à jour activée — Backend : {current_backend} | Frontend : {current_frontend}")

    while True:
        try:
            remote_backend = get_remote_version(BACKEND_VERSION_URL)
            remote_frontend = get_remote_version(FRONTEND_VERSION_URL)

            # --- Vérifie le backend ---
            if remote_backend != "0.0.0" and remote_backend != current_backend:
                logger.success(f"🚀 Nouvelle version BACKEND détectée : {remote_backend}")
                current_backend = remote_backend
                sse_manager.publish_event(
                    "update_available_backend",
                    {
                        "version": remote_backend,
                        "message": f"🚀 Nouvelle version BACKEND {remote_backend} disponible 🎉",
                    },
                )

            # --- Vérifie le frontend ---
            if remote_frontend != "0.0.0" and remote_frontend != current_frontend:
                logger.success(f"🎨 Nouvelle version FRONTEND détectée : {remote_frontend}")
                current_frontend = remote_frontend
                sse_manager.publish_event(
                    "update_available_frontend",
                    {
                        "version": remote_frontend,
                        "message": f"🎨 Nouvelle version FRONTEND {remote_frontend} disponible 🎉",
                    },
                )

            # --- Rien de nouveau ---
            if remote_backend == current_backend and remote_frontend == current_frontend:
                logger.debug(f"Aucune mise à jour — Backend={current_backend}, Frontend={current_frontend}")

        except Exception as e:
            logger.error(f"💥 Erreur dans la boucle de vérification de mise à jour : {e}")

        await asyncio.sleep(CHECK_INTERVAL_MINUTES * 60)
