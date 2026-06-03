import json
import os
from pathlib import Path

import yaml


USER = os.getenv("SSD_USER") or os.getenv("USER") or os.getlogin()

OUTPUT_JSON_PATH = os.getenv(
    "FRONTEND_SETTINGS_PATH",
    f"/home/{USER}/seedbox/docker/{USER}/projet-ssd/ssd-frontend/static/settings.json"
)

BACKEND_JSON_PATH = os.getenv(
    "BACKEND_SETTINGS_PATH",
    f"/home/{USER}/seedbox/docker/{USER}/projet-ssd/ssd-backend/data/settings.json"
)


def update_json_files(decrypted_yaml_content):
    yaml_data = parse_yaml(decrypted_yaml_content)

    if not yaml_data:
        raise ValueError("YAML vide ou invalide")

    if "sub" not in yaml_data or not isinstance(yaml_data["sub"], dict):
        raise ValueError("Clé 'sub' absente ou invalide dans le YAML")

    output_json_data = load_json_from_file(OUTPUT_JSON_PATH)
    backend_json_data = load_json_from_file(BACKEND_JSON_PATH)

    new_data = {
        "applications": [],
        "dossiers": {
            "on_item_type": [],
            "authentification": {
                "authappli": "basique"
            },
            "domaine": {}
        }
    }

    for key, value in yaml_data["sub"].items():
        if not isinstance(value, dict):
            value = {}

        new_data["applications"].append({
            "id": str(len(new_data["applications"]) + 1),
            "label": key
        })

        new_data["dossiers"]["on_item_type"].append(key)
        new_data["dossiers"]["authentification"][key] = value.get("auth", "basique")
        new_data["dossiers"]["domaine"][key] = value.get(key, "")

    updated_output_json = merge_dicts(output_json_data, new_data)
    updated_backend_json = merge_dicts(backend_json_data, new_data)

    save_json_to_file(updated_output_json, OUTPUT_JSON_PATH)
    save_json_to_file(updated_backend_json, BACKEND_JSON_PATH)


def load_json_from_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}

    return {}


def save_json_to_file(data, file_path):
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def merge_dicts(original, new_data):
    if not isinstance(original, dict):
        original = {}

    if "dossiers" in original and isinstance(original["dossiers"], dict):
        if "authentification" in original["dossiers"]:
            for key in list(original["dossiers"]["authentification"].keys()):
                if key != "authappli" and key not in new_data["dossiers"]["authentification"]:
                    del original["dossiers"]["authentification"][key]

        if "domaine" in original["dossiers"]:
            for key in list(original["dossiers"]["domaine"].keys()):
                if key not in new_data["dossiers"]["domaine"]:
                    del original["dossiers"]["domaine"][key]

    for key, value in new_data.items():
        if isinstance(value, dict):
            original[key] = merge_dicts(original.get(key, {}), value)
        else:
            original[key] = value

    return original


def parse_yaml(yaml_content):
    return yaml.safe_load(yaml_content)