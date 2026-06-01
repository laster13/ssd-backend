# backend/src/updater/auto_update.py

import json
import re
import shutil
import subprocess
import urllib.request
from pathlib import Path

import requests
from loguru import logger

from src.version import get_version


# ====== CONFIG ======

PROJECT_ROOT = Path(__file__).resolve().parents[1].parent

BACKEND_PATH = PROJECT_ROOT / "ssd-backend"
FRONTEND_PATH = PROJECT_ROOT / "ssd-frontend"
SAISON_FRONTEND_PATH = PROJECT_ROOT / "saison-frontend"

BACKEND_VERSION_FILE = BACKEND_PATH / "version.json"

BACKEND_NOTIFY_URL = "http://localhost:8080/api/v1/sse/update_finished"

REMOTE_BACKEND_URL = (
    "https://raw.githubusercontent.com/laster13/ssd-backend/main/version.json"
)
REMOTE_FRONTEND_URL = (
    "https://raw.githubusercontent.com/laster13/ssd-frontend/main/version.json"
)
REMOTE_SAISON_FRONTEND_URL = (
    "https://raw.githubusercontent.com/laster13/saison-frontend/main/version.json"
)


# ==========================================================
# ⚙️ OUTILS
# ==========================================================

def run(cmd: str, cwd=None) -> bool:
    """
    Exécute une commande shell et retourne True si elle réussit.
    """
    logger.info(f"⚙️ Exécution : {cmd}")

    result = subprocess.run(
        cmd,
        cwd=cwd,
        shell=True,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        if result.stderr.strip():
            logger.error(result.stderr.strip())
        return False

    if result.stdout.strip():
        logger.debug(result.stdout.strip())

    return True


def normalize_version(version: str) -> str:
    """
    Nettoie une version avant comparaison.

    Exemples acceptés :
    - 1.0.9
    - v1.0.9
    - " 1.0.9 "
    - "—" devient "0.0.0"
    """
    if version is None:
        return "0.0.0"

    version = str(version).strip()

    if not version or version == "—":
        return "0.0.0"

    if version.startswith("v") or version.startswith("V"):
        version = version[1:]

    return version.strip()


def parse_version(version: str) -> tuple:
    """
    Transforme une version en tuple comparable.

    Exemple :
    - 1.0.9  -> (1, 0, 9)
    - 1.10.0 -> (1, 10, 0)

    Si la version est invalide, retourne (0, 0, 0).
    """
    version = normalize_version(version)

    match = re.match(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?", version)
    if not match:
        return (0, 0, 0)

    major = int(match.group(1) or 0)
    minor = int(match.group(2) or 0)
    patch = int(match.group(3) or 0)

    return (major, minor, patch)


def is_remote_newer(local: str, remote: str, label: str) -> bool:
    """
    Retourne True uniquement si la version distante est supérieure
    à la version locale.

    Important :
    - local 1.0.9 / remote 1.0.9 -> False
    - local 1.0.8 / remote 1.0.9 -> True
    - local 1.1.0 / remote 1.0.9 -> False
    """
    local_clean = normalize_version(local)
    remote_clean = normalize_version(remote)

    local_parsed = parse_version(local_clean)
    remote_parsed = parse_version(remote_clean)

    logger.info(
        f"🔎 Comparaison {label} : local={local_clean} ({local_parsed}) | "
        f"distant={remote_clean} ({remote_parsed})"
    )

    return remote_parsed > local_parsed


def get_remote_version(url: str) -> str:
    """
    Lit la version distante depuis GitHub.
    """
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.load(response)

        version = data.get("version", "0.0.0")

        if not isinstance(version, str):
            logger.warning(f"⚠️ Version distante invalide ({url}) : {version}")
            return "0.0.0"

        return normalize_version(version)

    except Exception as e:
        logger.error(f"❌ Impossible de lire la version distante ({url}) : {e}")
        return "0.0.0"


# ==========================================================
# MISE À JOUR BACKEND
# ==========================================================

def update_backend():
    """
    Met à jour le backend.
    """
    logger.info("♻️ Mise à jour du backend en cours...")

    run("git fetch --all", cwd=BACKEND_PATH)
    run("git reset --hard origin/main", cwd=BACKEND_PATH)
    run("poetry install --no-interaction --no-root", cwd=BACKEND_PATH)
    run("pm2 restart backend || true")

    logger.success("✅ Backend mis à jour et redémarré avec succès.")


# ==========================================================
# MISE À JOUR FRONTEND
# ==========================================================

def update_node_frontend(path: Path, pm2_name: str, label: str):
    """
    Met à jour, reconstruit et redémarre un frontend Node/Svelte.
    """
    if not path.exists():
        logger.warning(f"⚠️ Dossier {label} introuvable — mise à jour ignorée : {path}")
        return

    logger.info(f"♻️ Mise à jour de {label} en cours...")

    run("git fetch --all > /dev/null 2>&1", cwd=path)
    run("git reset --hard origin/main > /dev/null 2>&1", cwd=path)

    logger.info(f"🧹 Nettoyage de {label} avant installation...")

    node_modules = path / "node_modules"
    if node_modules.exists():
        try:
            shutil.rmtree(node_modules)
            logger.debug(f"node_modules supprimé pour {label}.")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de supprimer node_modules pour {label} : {e}")

    lockfile = path / "package-lock.json"
    if lockfile.exists():
        try:
            lockfile.unlink()
            logger.debug(f"package-lock.json supprimé pour {label}.")
        except Exception as e:
            logger.warning(
                f"⚠️ Impossible de supprimer package-lock.json pour {label} : {e}"
            )

    logger.info(f"📦 Installation des dépendances pour {label}...")

    has_pnpm = run("command -v pnpm >/dev/null 2>&1")

    if has_pnpm:
        logger.info(f"PNPM détecté — installation avec pnpm pour {label}.")
        if not run("pnpm install --frozen-lockfile > /dev/null 2>&1", cwd=path):
            logger.warning(f"⚠️ Erreur PNPM pour {label} — tentative avec NPM.")
            run("npm install --silent > /dev/null 2>&1", cwd=path)
    else:
        logger.warning(f"⚠️ PNPM non trouvé — utilisation de NPM pour {label}.")
        run("npm install --silent > /dev/null 2>&1", cwd=path)

    logger.info(f"🏗️ Construction de {label}...")

    if not run("npm run build --silent > /dev/null 2>&1", cwd=path):
        logger.error(f"❌ Échec de la construction de {label}.")
        return

    logger.info(f"♻️ Redémarrage PM2 de {label} ({pm2_name})...")

    if not run(f"pm2 restart {pm2_name} > /dev/null 2>&1", cwd=path):
        logger.warning(
            f"⚠️ Process PM2 {pm2_name} introuvable — tentative de démarrage..."
        )

        if not run(
            f"pm2 start npm --name {pm2_name} -- run preview > /dev/null 2>&1",
            cwd=path,
        ):
            logger.error(f"❌ Impossible de démarrer le process PM2 {pm2_name}.")
            return

    logger.success(f"✅ {label} mis à jour, reconstruit et redémarré avec succès.")


def update_frontend():
    """
    Met à jour le frontend principal.
    """
    update_node_frontend(
        path=FRONTEND_PATH,
        pm2_name="frontend",
        label="frontend",
    )


def update_saison_frontend():
    """
    Met à jour le frontend Saison.
    """
    update_node_frontend(
        path=SAISON_FRONTEND_PATH,
        pm2_name="saison-frontend",
        label="saison-frontend",
    )


# ==========================================================
# NOTIFICATION SSE
# ==========================================================

def notify_backend_update_done(
    success=True,
    message="✅ Mise à jour terminée avec succès.",
):
    """
    Notifie le backend via SSE que la mise à jour est finie.
    """
    try:
        payload = {
            "type": "update_finished",
            "message": message if success else "❌ Erreur pendant la mise à jour.",
            "success": success,
        }

        requests.post(BACKEND_NOTIFY_URL, json=payload, timeout=10)
        logger.info("📡 Notification SSE envoyée au backend (update_finished).")

    except requests.exceptions.ReadTimeout:
        logger.warning(
            "⚠️ Notification SSE : le frontend redémarre probablement "
            "(timeout ignoré)."
        )

    except requests.exceptions.ConnectionError:
        logger.warning(
            "⚠️ Notification SSE : le frontend est injoignable "
            "(en redémarrage ?)"
        )

    except Exception as e:
        logger.warning(f"⚠️ Impossible d’envoyer la notification SSE : {e}")


# ==========================================================
# MAIN — Logique globale
# ==========================================================

def main(target: str | None = None):
    """
    Met à jour backend, frontend, saison_frontend ou tous les composants.

    Si target est fourni :
    - backend
    - frontend
    - saison_frontend

    Alors l'update est forcée pour ce composant.

    Si target est absent :
    - lecture des versions locales
    - lecture des versions distantes
    - update uniquement si la version distante est supérieure
    """
    try:
        logger.info(f"🚀 Lancement de la mise à jour (target={target})")

        # --------------------------------------------------
        # Updates forcées
        # --------------------------------------------------

        if target == "backend":
            update_backend()
            notify_backend_update_done(
                success=True,
                message="✅ Backend mis à jour avec succès.",
            )
            return

        if target == "frontend":
            update_frontend()
            notify_backend_update_done(
                success=True,
                message="✅ Frontend mis à jour avec succès.",
            )
            return

        if target == "saison_frontend":
            update_saison_frontend()
            notify_backend_update_done(
                success=True,
                message="✅ Saison Frontend mis à jour avec succès.",
            )
            return

        # --------------------------------------------------
        # Mise à jour automatique par version.json
        # --------------------------------------------------

        local_versions = get_version()

        local_backend = local_versions.get("backend", "0.0.0")
        local_frontend = local_versions.get("frontend", "0.0.0")
        local_saison_frontend = local_versions.get("saison_frontend", "0.0.0")

        remote_backend = get_remote_version(REMOTE_BACKEND_URL)
        remote_frontend = get_remote_version(REMOTE_FRONTEND_URL)
        remote_saison_frontend = get_remote_version(REMOTE_SAISON_FRONTEND_URL)

        logger.info(f"📦 Backend local : {local_backend} | distant : {remote_backend}")
        logger.info(
            f"📦 Frontend local : {local_frontend} | distant : {remote_frontend}"
        )
        logger.info(
            f"📦 Saison Frontend local : {local_saison_frontend} | "
            f"distant : {remote_saison_frontend}"
        )

        backend_needs_update = is_remote_newer(
            local=local_backend,
            remote=remote_backend,
            label="backend",
        )

        frontend_needs_update = is_remote_newer(
            local=local_frontend,
            remote=remote_frontend,
            label="frontend",
        )

        saison_frontend_needs_update = is_remote_newer(
            local=local_saison_frontend,
            remote=remote_saison_frontend,
            label="saison-frontend",
        )

        if backend_needs_update:
            update_backend()
        else:
            logger.info("✅ Le backend est déjà à jour.")

        if frontend_needs_update:
            update_frontend()
        else:
            logger.info("✅ Le frontend est déjà à jour.")

        if saison_frontend_needs_update:
            update_saison_frontend()
        else:
            logger.info("✅ Le Saison Frontend est déjà à jour.")

        if (
            not backend_needs_update
            and not frontend_needs_update
            and not saison_frontend_needs_update
        ):
            logger.info("✅ Aucun composant n’avait besoin d’une mise à jour.")

        notify_backend_update_done(success=True)

    except Exception as e:
        logger.error(f"❌ Erreur durant la mise à jour : {e}")
        notify_backend_update_done(success=False, message=str(e))


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else None)