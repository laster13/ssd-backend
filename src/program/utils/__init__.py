# -*- coding: utf-8 -*-

import os
import secrets
import string
from pathlib import Path

data_dir_path = Path(__file__).resolve().parents[3] / "data"

def get_version() -> str:
    """Retourne une version statique ou lue depuis un fichier VERSION"""
    version_file = Path(__file__).resolve().parents[3] / "VERSION"
    if version_file.exists():
        return version_file.read_text().strip()
    return "0.1.0"


def generate_api_key(length: int = 32) -> str:
    """Génère une clé API sécurisée"""
    env_key = os.getenv("API_KEY", "")
    if len(env_key) == length:
        return env_key

    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))
