import json
import os
import subprocess
from pathlib import Path

from loguru import logger


def _read_version_file(path: Path, label: str) -> str:
    """
    Lit un fichier version.json local au format :
    {
      "version": "1.0.9"
    }

    Retourne toujours une version exploitable.
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


def _read_version_from_container(container_name: str, path: str, label: str) -> str:
    """
    Lit /app/version.json dans un conteneur Docker déjà lancé.

    Exemple :
      docker exec ssd-frontend cat /app/version.json
    """
    try:
        result = subprocess.run(
            ["docker", "exec", container_name, "cat", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
        )

        if result.returncode != 0:
            logger.warning(
                f"⚠️ Impossible de lire {label}/version.json dans {container_name}:{path} : "
                f"{result.stderr.strip()}"
            )
            return "0.0.0"

        data = json.loads(result.stdout)
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
        logger.error(
            f"❌ Erreur lecture version {label} depuis le conteneur {container_name}:{path} : {e}"
        )
        return "0.0.0"


def get_version() -> dict:
    """
    Retourne les versions locales réellement déployées :
    - backend depuis /app/version.json
    - frontend depuis le conteneur ssd-frontend:/app/version.json
    - saison frontend depuis le conteneur saison-frontend:/app/version.json
    """
    try:
        backend_version_path = Path(
            os.getenv("BACKEND_VERSION_PATH", "/app/version.json")
        )

        frontend_container = os.getenv("FRONTEND_CONTAINER", "ssd-frontend")
        frontend_version_path = os.getenv("FRONTEND_VERSION_PATH", "/app/version.json")

        saison_container = os.getenv("SAISON_FRONTEND_CONTAINER", "saison-frontend")
        saison_version_path = os.getenv(
            "SAISON_FRONTEND_VERSION_PATH",
            "/app/version.json",
        )

        backend_version = _read_version_file(
            backend_version_path,
            "backend",
        )

        frontend_version = _read_version_from_container(
            frontend_container,
            frontend_version_path,
            "frontend",
        )

        saison_frontend_version = _read_version_from_container(
            saison_container,
            saison_version_path,
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