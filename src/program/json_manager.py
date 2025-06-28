import json
import os

USER = os.getenv("USER") or os.getlogin()
OUTPUT_JSON_PATH = f"/home/{USER}/projet-riven/riven-frontend/static/settings.json"
BACKEND_JSON_PATH = f"/home/{USER}/projet-riven/riven/data/settings.json"

# Fonction pour mettre à jour les fichiers JSON avec le contenu décrypté du YAML
def update_json_files(decrypted_yaml_content):
    yaml_data = parse_yaml(decrypted_yaml_content)

    # Charger les données JSON existantes
    output_json_data = load_json_from_file(OUTPUT_JSON_PATH)
    backend_json_data = load_json_from_file(BACKEND_JSON_PATH)

    # Créer un dictionnaire temporaire pour les nouvelles données
    new_data = {
        "applications": [],
        "dossiers": {
            "on_item_type": [],
            "authentification": {
                "authappli": "basique"  # Garder "basique" comme valeur par défaut
            },
            "domaine": {}
        }
    }

    # Mettre à jour le nouveau dictionnaire avec les données YAML
    for key, value in yaml_data['sub'].items():
        new_data["applications"].append({"id": str(len(new_data["applications"]) + 1), "label": key})
        new_data["dossiers"]["on_item_type"].append(key)
        new_data["dossiers"]["authentification"][key] = value.get("auth", "basique")
        new_data["dossiers"]["domaine"][key] = value.get(key, "")

    # Supprimer les anciennes clés de "authentification" et "domaine" si elles ne sont plus présentes dans new_data
    keys_to_remove_auth = [
        key for key in output_json_data.get("dossiers", {}).get("authentification", {}) 
        if key != "authappli" and key not in new_data["dossiers"]["authentification"]
    ]
    keys_to_remove_domain = [
        key for key in output_json_data.get("dossiers", {}).get("domaine", {}) 
        if key not in new_data["dossiers"]["domaine"]
    ]

    for key in keys_to_remove_auth:
        del output_json_data["dossiers"]["authentification"][key]

    for key in keys_to_remove_domain:
        del output_json_data["dossiers"]["domaine"][key]

    # Fusionner les nouvelles données avec les données existantes
    updated_output_json = merge_dicts(output_json_data, new_data)
    updated_backend_json = merge_dicts(backend_json_data, new_data)

    # Sauvegarder les fichiers JSON mis à jour
    save_json_to_file(updated_output_json, OUTPUT_JSON_PATH)
    save_json_to_file(updated_backend_json, BACKEND_JSON_PATH)

# Fonction pour charger un fichier JSON
def load_json_from_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# Fonction pour sauvegarder un fichier JSON
def save_json_to_file(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

# Fonction pour fusionner deux dictionnaires, en supprimant les clés obsolètes
def merge_dicts(original, new_data):
    """
    Fusionne les nouvelles données dans les données existantes.
    Supprime les clés obsolètes qui ne sont plus présentes dans les nouvelles données.
    """
    # Supprimer les clés dans 'authentification' et 'domaine' si elles ne sont plus dans new_data
    if 'dossiers' in original and 'authentification' in original['dossiers']:
        # Supprimer les anciennes clés d'authentification
        for key in list(original['dossiers']['authentification'].keys()):
            if key != 'authappli' and key not in new_data['dossiers']['authentification']:
                del original['dossiers']['authentification'][key]

    if 'dossiers' in original and 'domaine' in original['dossiers']:
        # Supprimer les anciennes clés de domaine
        for key in list(original['dossiers']['domaine'].keys()):
            if key not in new_data['dossiers']['domaine']:
                del original['dossiers']['domaine'][key]

    # Fusion des nouvelles données
    for key, value in new_data.items():
        if isinstance(value, dict) and key in original:
            original[key] = merge_dicts(original.get(key, {}), value)
        else:
            original[key] = value

    return original

# Fonction pour parser le YAML déchiffré
def parse_yaml(yaml_content):
    import yaml
    return yaml.safe_load(yaml_content)
