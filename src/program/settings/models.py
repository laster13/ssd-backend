from typing import Any, Dict, List, Callable, Optional
from pydantic import BaseModel, Field
from datetime import datetime
from pathlib import Path

from program.utils import generate_api_key, get_version


# ═══════════════════════════════════════════════════════════
# 🔁 Observable base
# ═══════════════════════════════════════════════════════════

class Observable(BaseModel):
    """Base pour notifier les observateurs (UI ou autres) lors d’un changement."""
    _notify_observers: Callable = None

    @classmethod
    def set_notify_observers(cls, notify_observers_callable):
        cls._notify_observers = notify_observers_callable

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if self.__class__._notify_observers:
            self.__class__._notify_observers()


# ═══════════════════════════════════════════════════════════
# 🎬 Services et utilisateurs
# ═══════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════
# ⚙️ Configuration générale de l’application
# ═══════════════════════════════════════════════════════════

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
    firstRun: bool = True
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


# ═══════════════════════════════════════════════════════════
# 📁 Gestion des symlinks
# ═══════════════════════════════════════════════════════════

class LinkDir(BaseModel):
    path: str
    manager: str  # "sonarr" ou "radarr"


# ═══════════════════════════════════════════════════════════
# 🧩 Configuration des instances AllDebrid
# ═══════════════════════════════════════════════════════════

class AllDebridInstance(BaseModel):
    """
    Représente une instance AllDebrid complète.
    Chaque instance peut avoir son propre chemin de cache Decypharr,
    défini manuellement par l'utilisateur via le frontend.
    """
    name: str
    api_key: str
    mount_path: str
    cache_path: Optional[str] = None
    rate_limit: float = 0.2
    priority: int = 1
    enabled: bool = True

class OrphanManagerConfig(BaseModel):
    auto_delete: bool = False

# ═══════════════════════════════════════════════════════════
# ⚙️ Configuration principale (config.json)
# ═══════════════════════════════════════════════════════════

class SymlinkConfig(BaseModel):
    links_dirs: List[LinkDir] = Field(default_factory=list)
    mount_dirs: List[str] = Field(default_factory=list)
    alldebrid_instances: List[AllDebridInstance] = Field(default_factory=list)
    orphan_manager: OrphanManagerConfig = OrphanManagerConfig()
    radarr_api_key: Optional[str] = None
    sonarr_api_key: Optional[str] = None
    discord_webhook_url: Optional[str] = None
    tmdb_api_key: Optional[str] = None

# ═══════════════════════════════════════════════════════════
# 🧹 Résultat du scan des orphelins
# ═══════════════════════════════════════════════════════════

class OrphanScanStats(BaseModel):
    sources: int
    symlinks: int
    orphans: int


class OrphanActions(BaseModel):
    auto_delete: bool
    deletable: int


class OrphanScanResult(BaseModel):
    scan_date: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    instance: str
    mount_path: str
    stats: OrphanScanStats
    orphans: List[str] = Field(default_factory=list)
    actions: OrphanActions
