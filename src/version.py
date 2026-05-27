import json
from pathlib import Path

from loguru import logger


def _read_version_file(path: Path, label: str) -> str:
    if not path.exists():
        logger.warning(f"⚠️ Fichier {label}/version.json introuvable à {path}")
        return "—"

    try:
        with open(path, "r") as f:
            data = json.load(f)

        return data.get("version", "—")

    except Exception as e:
        logger.error(f"❌ Erreur lecture version {label} depuis {path} : {e}")
        return "—"


def get_version() -> dict:
    """
    Retourne les versions locales :
    - ssd-backend/version.json
    - ssd-frontend/version.json
    - saison-frontend/version.json
    """
    try:
        backend_root = Path(__file__).resolve().parent.parent
        project_root = backend_root.parent

        backend_version = _read_version_file(
            project_root / "ssd-backend" / "version.json",
            "backend",
        )

        frontend_version = _read_version_file(
            project_root / "ssd-frontend" / "version.json",
            "frontend",
        )

        saison_frontend_version = _read_version_file(
            project_root / "saison-frontend" / "version.json",
            "saison-frontend",
        )

        return {
            "backend": backend_version,
            "frontend": frontend_version,
            "saison_frontend": saison_frontend_version,
        }

    except Exception as e:
        logger.error(f"❌ Erreur lors de la lecture des fichiers version.json : {e}")

        return {
            "backend": "—",
            "frontend": "—",
            "saison_frontend": "—",
        }