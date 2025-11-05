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
    """Met à jour, reconstruit et redémarre le frontend (pnpm ou npm), sans bruit de console."""
    if not FRONTEND_PATH.exists():
        logger.warning(⚠️ Aucun dossier frontend trouvé — mise à jour ignorée.")
        return

    logger.info("🎨 Mise à jour du frontend en cours...")
    run("git fetch --all > /dev/null 2>&1", cwd=FRONTEND_PATH)
    run("git reset --hard origin/main > /dev/null 2>&1", cwd=FRONTEND_PATH)

    # ======================================================
    # 🧹 Nettoyage avant installation
    # ======================================================
    logger.info("🧹 Nettoyage du frontend avant installation...")
    import shutil

    node_modules = FRONTEND_PATH / "node_modules"
    if node_modules.exists():
        try:
            shutil.rmtree(node_modules)
            logger.debug("🗑️ node_modules supprimé.")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de supprimer node_modules : {e}")

    lockfile = FRONTEND_PATH / "package-lock.json"
    if lockfile.exists():
        try:
            lockfile.unlink()
            logger.debug("🗑️ package-lock.json supprimé.")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de supprimer package-lock.json : {e}")

    # ======================================================
    # 📦 Installation des dépendances (PNPM ou NPM)
    # ======================================================
    logger.info("📦 Vérification de PNPM...")
    has_pnpm = run("command -v pnpm >/dev/null 2>&1")

    if has_pnpm:
        logger.info("📦 PNPM détecté — installation avec pnpm.")
        if not run("pnpm install --frozen-lockfile > /dev/null 2>&1", cwd=FRONTEND_PATH):
            logger.warning("⚠️ Erreur pendant l'installation PNPM — tentative avec NPM.")
            run("npm install --silent > /dev/null 2>&1", cwd=FRONTEND_PATH)
    else:
        logger.warning("⚠️ PNPM non trouvé — utilisation de NPM.")
        run("npm install --silent > /dev/null 2>&1", cwd=FRONTEND_PATH)

    # ======================================================
    # 🏗️ Build du frontend
    # ======================================================
    logger.info("🏗️ Construction du frontend...")
    if not run("npm run build --silent > /dev/null 2>&1", cwd=FRONTEND_PATH):
        logger.error("❌ Échec de la construction du frontend.")
        return

    # ======================================================
    # 🔁 Redémarrage du frontend
    # ======================================================
    logger.info("🔁 Redémarrage du frontend (pm2)...")
    run("pm2 restart frontend > /dev/null 2>&1", cwd=FRONTEND_PATH)
    logger.success("✅ Frontend mis à jour, reconstruit et redémarré avec succès.")


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
        requests.post(BACKEND_NOTIFY_URL, json=payload, timeout=10)
        logger.info("📡 Notification SSE envoyée au backend (update_finished).")
    except requests.exceptions.ReadTimeout:
        logger.warning(⚠️ Notification SSE : le frontend redémarre probablement (timeout ignoré).")
    except requests.exceptions.ConnectionError:
        logger.warning("⚠️ Notification SSE : le frontend est injoignable (en redémarrage ?)")
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
