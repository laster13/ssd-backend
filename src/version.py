import json
from pathlib import Path

from loguru import logger


def _read_version_file(path: Path, label: str) -> str:
    """
    Lit un fichier version.json au format :
    {
      "version": "1.0.9"
    }

    Retourne toujours une version exploitable par la logique de mise à jour.
    Si le fichier est absent ou invalide, retourne "0.0.0".
    """
    if not path.exists():
        logger.warning(f"⚠️ Fichier {label}/version.json introuvable à {path}")
        return "0.0.0"

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        version = data.get("version", "0.0.0")

        if not isinstance(version, str):
            logger.warning(
                f"⚠️ Version invalide dans {label}/version.json : {version}"
            )
            return "0.0.0"

        version = version.strip()

        if not version:
            logger.warning(f"⚠️ Version vide dans {label}/version.json")
            return "0.0.0"

        return version

    except Exception as e:
        logger.error(f"❌ Erreur lecture version {label} depuis {path} : {e}")
        return "0.0.0"


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
            "backend": "0.0.0",
            "frontend": "0.0.0",
            "saison_frontend": "0.0.0",
        }