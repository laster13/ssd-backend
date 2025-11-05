# backend/src/updater/auto_update.py

import subprocess
import json
import urllib.request
from pathlib import Path
import requests
from loguru import logger
from src.version import get_version

# ====== CONFIG ======
PROJECT_ROOT = Path(__file__).resolve().parents[1].parent
BACKEND_PATH = PROJECT_ROOT / "ssd-backend"
FRONTEND_PATH = PROJECT_ROOT / "ssd-frontend"
BACKEND_VERSION_FILE = BACKEND_PATH / "version.json"
BACKEND_NOTIFY_URL = "http://localhost:8080/api/v1/sse/update_finished"

REMOTE_BACKEND_URL = "https://raw.githubusercontent.com/laster13/ssd-backend/main/version.json"
REMOTE_FRONTEND_URL = "https://raw.githubusercontent.com/laster13/ssd-frontend/main/version.json"


# ==========================================================
# ⚙️ OUTILS
# ==========================================================

def run(cmd: str, cwd=None) -> bool:
    """Exécute une commande shell et affiche la sortie."""
    logger.info(f"⚙️ Exécution : {cmd}")
    result = subprocess.run(cmd, cwd=cwd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(result.stderr.strip() or "Erreur inconnue")
        return False

    if result.stdout.strip():
        logger.debug(result.stdout.strip())
    return True


def get_remote_version(url: str) -> str:
    """Lit la version distante depuis GitHub."""
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.load(response)
            return data.get("version", "0.0.0")
    except Exception as e:
        logger.error(f"❌ Impossible de lire la version distante ({url}) : {e}")
        return "0.0.0"


# ==========================================================
# 🔧 MISE À JOUR BACKEND
# ==========================================================

def update_backend():
    logger.info("🚀 Mise à jour du backend en cours...")
    run("git fetch --all", cwd=BACKEND_PATH)
    run("git reset --hard origin/main", cwd=BACKEND_PATH)
    run("poetry install --no-interaction --no-root", cwd=BACKEND_PATH)
    run("pm2 restart backend || true")
    logger.success("✅ Backend mis à jour et redémarré avec succès.")


# ==========================================================
# 🎨 MISE À JOUR FRONTEND
# ==========================================================

def update_frontend():
    if not FRONTEND_PATH.exists():
        logger.warning("⚠️ Aucun dossier frontend trouvé — mise à jour ignorée.")
        return

    logger.info("🎨 Mise à jour du frontend en cours...")
    run("git fetch --all", cwd=FRONTEND_PATH)
    run("git reset --hard origin/main", cwd=FRONTEND_PATH)

    # Utilise pnpm si disponible, sinon npm
    has_pnpm = run("pnpm --version")
    if has_pnpm:
        logger.info("📦 Installation via PNPM détectée.")
        run("pnpm install", cwd=FRONTEND_PATH)
        run("pnpm run build", cwd=FRONTEND_PATH)
    else:
        logger.warning("⚠️ PNPM non trouvé — utilisation de NPM.")
        if (FRONTEND_PATH / "package-lock.json").exists():
            run("npm ci", cwd=FRONTEND_PATH)
        else:
            run("npm install", cwd=FRONTEND_PATH)
        run("npm run build", cwd=FRONTEND_PATH)

    run("pm2 restart frontend || true")
    logger.success("✅ Frontend mis à jour et reconstruit avec succès.")


# ==========================================================
# 📡 NOTIFICATION SSE
# ==========================================================

def notify_backend_update_done(success=True, message="✅ Mise à jour terminée avec succès."):
    """Notifie le backend (SSE) que la mise à jour est finie."""
    try:
        payload = {
            "type": "update_finished",
            "message": message if success else "❌ Erreur pendant la mise à jour.",
            "success": success
        }
        requests.post(BACKEND_NOTIFY_URL, json=payload, timeout=3)
        logger.info("📡 Notification SSE envoyée au backend (update_finished).")
    except Exception as e:
        logger.warning(f"⚠️ Impossible d’envoyer la notification SSE : {e}")


# ==========================================================
# 🚀 MAIN — Logique globale
# ==========================================================

def main():
    try:
        local_versions = get_version()
        local_backend = local_versions.get("backend", "0.0.0")
        local_frontend = local_versions.get("frontend", "0.0.0")

        remote_backend = get_remote_version(REMOTE_BACKEND_URL)
        remote_frontend = get_remote_version(REMOTE_FRONTEND_URL)

        logger.info(f"📦 Backend local : {local_backend} | distant : {remote_backend}")
        logger.info(f"💅 Frontend local : {local_frontend} | distant : {remote_frontend}")

        backend_needs_update = local_backend != remote_backend
        frontend_needs_update = local_frontend != remote_frontend

        if backend_needs_update:
            update_backend()
        else:
            logger.info("🟢 Le backend est déjà à jour.")

        if frontend_needs_update:
            update_frontend()
        else:
            logger.info("🟢 Le frontend est déjà à jour.")

        if not backend_needs_update and not frontend_needs_update:
            logger.info("🟢 Aucun composant n’avait besoin d’une mise à jour.")

        notify_backend_update_done(success=True)

    except Exception as e:
        logger.error(f"💥 Erreur durant la mise à jour : {e}")
        notify_backend_update_done(success=False, message=str(e))


if __name__ == "__main__":
    main()
