from typing import Any, Dict, List, Callable
from pydantic import BaseModel, Field
from program.utils import generate_api_key, get_version


class Observable(BaseModel):
    _notify_observers: Callable = None

    @classmethod
    def set_notify_observers(cls, notify_observers_callable):
        cls._notify_observers = notify_observers_callable

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if self.__class__._notify_observers:
            self.__class__._notify_observers()

# Modèle Plex
class PlexModel(Observable):
    token: str = ""
    login: str = ""
    password: str = ""
    enabled: bool = False

class JellyfinModel(Observable):
    enabled: bool = False
    api_key: str = ""

class EmbyModel(Observable):
    enabled: bool = False
    api_key: str = ""

# Modèle utilisateur
class UtilisateurModel(Observable):
    username: str = ""
    email: str = ""
    domain: str = ""
    password: str = ""
    oauth_enabled: bool = False
    oauth_client: str = ""
    oauth_secret: str = ""
    oauth_mail: str = ""
    zurg_enabled: bool = False
    zurg_token: str = ""


class CloudflareModel(Observable):
    cloudflare_login: str = ""
    cloudflare_api_key: str = ""


class DossierModel(BaseModel):
    on_item_type: List[str] = []
    authentification: Dict[str, str] = Field(default_factory=dict)
    domaine: Dict[str, Any] = Field(default_factory=dict)

class ApplicationModel(Observable):
    id: int = 1
    label: str = ""


class AppModel(Observable):
    version: str = get_version()
    api_key: str = ""
    debug: bool = True
    log: bool = True
    utilisateur: UtilisateurModel = UtilisateurModel()
    cloudflare: CloudflareModel = CloudflareModel()
    dossiers: DossierModel = DossierModel()
    applications: List[ApplicationModel] = []
    plex: PlexModel = PlexModel()
    jellyfin: JellyfinModel = JellyfinModel()
    emby: EmbyModel = EmbyModel()

    def __init__(self, **data: Any):
        super().__init__(**data)
        if self.api_key == "":
            self.api_key = generate_api_key()
