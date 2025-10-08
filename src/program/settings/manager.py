import json
import os
from pathlib import Path
from loguru import logger
from pydantic import ValidationError, Field
from program.settings.models import AppModel, Observable, SymlinkConfig, LinkDir
from program.utils import data_dir_path


class SettingsManager:
    """Class that handles settings, ensuring they are validated against a Pydantic schema."""

    def __init__(self):
        self.observers = []
        self.filename = "settings.json"
        self.settings_file = data_dir_path / self.filename

        # ✅ Crée le dossier parent au démarrage si nécessaire
        self.settings_file.parent.mkdir(parents=True, exist_ok=True)

        Observable.set_notify_observers(self.notify_observers)

        if not self.settings_file.exists():
            self.settings = AppModel()
            self.settings = AppModel.model_validate(
                self.check_environment(json.loads(self.settings.model_dump_json()), "RIVEN")
            )
            self.notify_observers()
        else:
            self.load()

    def register_observer(self, observer):
        self.observers.append(observer)

    def notify_observers(self):
        for observer in self.observers:
            observer()

    def check_environment(self, settings, prefix="", seperator="_"):
        checked_settings = {}
        for key, value in settings.items():
            if isinstance(value, dict):
                sub_checked_settings = self.check_environment(value, f"{prefix}{seperator}{key}")
                checked_settings[key] = sub_checked_settings
            else:
                environment_variable = f"{prefix}_{key}".upper()
                if os.getenv(environment_variable, None):
                    new_value = os.getenv(environment_variable)
                    if isinstance(value, bool):
                        checked_settings[key] = new_value.lower() == "true" or new_value == "1"
                    elif isinstance(value, int):
                        checked_settings[key] = int(new_value)
                    elif isinstance(value, float):
                        checked_settings[key] = float(new_value)
                    elif isinstance(value, list):
                        checked_settings[key] = json.loads(new_value)
                    else:
                        checked_settings[key] = new_value
                else:
                    checked_settings[key] = value
        return checked_settings

    def load(self, settings_dict: dict | None = None):
        """Load settings from file, validating against the AppModel schema."""
        try:
            if not settings_dict:
                with open(self.settings_file, "r", encoding="utf-8") as file:
                    settings_dict = json.loads(file.read())
                    if os.environ.get("RIVEN_FORCE_ENV", "false").lower() == "true":
                        settings_dict = self.check_environment(settings_dict, "RIVEN")
            self.settings = AppModel.model_validate(settings_dict)
            self.save()
        except ValidationError as e:
            formatted_error = format_validation_error(e)
            logger.error(f"Settings validation failed:\n{formatted_error}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing settings file: {e}")
            raise
        except FileNotFoundError:
            logger.warning(f"Error loading settings: {self.settings_file} does not exist")
            raise
        self.notify_observers()

    def save(self):
        """Save settings to file, using Pydantic model for JSON serialization."""
        # ✅ Assure la création du dossier parent
        self.settings_file.parent.mkdir(parents=True, exist_ok=True)

        with open(self.settings_file, "w", encoding="utf-8") as file:
            file.write(self.settings.model_dump_json(indent=4))


def format_validation_error(e: ValidationError) -> str:
    """Format validation errors in a user-friendly way"""
    messages = []
    for error in e.errors():
        field = ".".join(str(x) for x in error["loc"])
        message = error.get("msg")
        messages.append(f"• {field}: {message}")
    return "\n".join(messages)


settings_manager = SettingsManager()


class ConfigManager:
    def __init__(self):
        self.config_file = data_dir_path / "config.json"
        self.config: SymlinkConfig | None = None

        # ✅ Crée le dossier parent au démarrage si nécessaire
        self.config_file.parent.mkdir(parents=True, exist_ok=True)

        if not self.config_file.exists():
            # Création d'une config par défaut
            self.config = SymlinkConfig.model_validate(
                {
                    "links_dirs": [],
                    "mount_dirs": [],
                    "alldebrid_instances": [],
                    "orphan_manager": {"auto_delete": False},
                    "radarr_api_key": None,
                    "sonarr_api_key": None,
                    "discord_webhook_url": None,
                    "tmdb_api_key": None,
                }
            )
            self.save()
        else:
            self.load()

    def check_environment(self, config_dict, prefix="", seperator="_"):
        """Remplace les valeurs par celles des variables d'environnement si présentes."""
        checked_config = {}
        for key, value in config_dict.items():
            if isinstance(value, dict):
                checked_config[key] = self.check_environment(value, f"{prefix}{seperator}{key}")
            else:
                env_key = f"{prefix}_{key}".upper()
                if os.getenv(env_key):
                    env_val = os.getenv(env_key)
                    if isinstance(value, bool):
                        checked_config[key] = env_val.lower() in ["1", "true"]
                    elif isinstance(value, int):
                        checked_config[key] = int(env_val)
                    elif isinstance(value, float):
                        checked_config[key] = float(env_val)
                    elif isinstance(value, list):
                        try:
                            checked_config[key] = json.loads(env_val)
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️ Impossible de parser {env_key} comme JSON")
                            checked_config[key] = value
                    else:
                        checked_config[key] = env_val
                else:
                    checked_config[key] = value
        return checked_config

    def load(self):
        """Charge la config depuis config.json et valide avec Pydantic."""
        try:
            with self.config_file.open("r", encoding="utf-8") as f:
                config_dict = json.load(f)
                self.config = SymlinkConfig.model_validate(config_dict)
                self.save()  # On re-sauvegarde pour garder un format propre
        except ValidationError as e:
            logger.error(f"❌ Validation échouée pour config.json :\n{self.format_validation_error(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ Erreur JSON dans config.json : {e}")
            raise
        except FileNotFoundError:
            logger.warning(f"⚠️ {self.config_file} introuvable")
            raise

    def save(self):
        """Sauvegarde la config dans config.json."""
        try:
            # ✅ Assure la création du dossier parent
            self.config_file.parent.mkdir(parents=True, exist_ok=True)

            with self.config_file.open("w", encoding="utf-8") as f:
                f.write(self.config.model_dump_json(indent=4))
            logger.success(f"💾 Configuration sauvegardée dans {self.config_file}")
        except Exception as e:
            logger.error(f"❌ Impossible de sauvegarder config.json : {e}")
            raise

    def format_validation_error(self, e: ValidationError) -> str:
        return "\n".join(
            f"• {'.'.join(str(x) for x in err['loc'])}: {err['msg']}"
            for err in e.errors()
        )


# Initialisation globale
config_manager = ConfigManager()
