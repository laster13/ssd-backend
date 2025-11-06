import json
from pathlib import Path
from loguru import logger

def get_version() -> dict:
    """
    Retourne les versions locales du backend et du frontend.
    Lecture directe des fichiers :
      - ssd-backend/version.json
      - ssd-frontend/version.json
    """

    backend_version = "—"
    frontend_version = "—"

    try:
        # 📂 Chemins principaux
        backend_root = Path(__file__).resolve().parent.parent
        project_root = backend_root.parent

        # 🔹 Fichier version du backend
        backend_version_file = project_root / "ssd-backend" / "version.json"
        if backend_version_file.exists():
            with open(backend_version_file, "r") as f:
                data = json.load(f)
                backend_version = data.get("version", "—")
        else:
            logger.warning(f"⚠️ Fichier backend/version.json introuvable à {backend_version_file}")

        # 🔹 Fichier version du frontend
        frontend_version_file = project_root / "ssd-frontend" / "version.json"
        if frontend_version_file.exists():
            with open(frontend_version_file, "r") as f:
                data = json.load(f)
                frontend_version = data.get("version", "—")
        else:
            logger.warning(f"⚠️ Fichier frontend/version.json introuvable à {frontend_version_file}")

    except Exception as e:
        logger.error(f"💥 Erreur lors de la lecture des fichiers version.json : {e}")

    return {
        "backend": backend_version,
        "frontend": frontend_version
    }
