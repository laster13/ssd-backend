from typing import Any, Dict, List, Callable, Optional
from pydantic import BaseModel, Field
from datetime import datetime

from program.utils import generate_api_key, get_version


# ═══════════════════════════════════════════════════════════
# Observable base
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
# Services et utilisateurs
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
#⚙️ Configuration générale de l’application
# ═══════════════════════════════════════════════════════════
class DossierModel(BaseModel):
    on_item_type: List[str] = Field(default_factory=list)
    authentification: Dict[str, str] = Field(default_factory=dict)
    domaine: Dict[str, Any] = Field(default_factory=dict)


class ApplicationModel(Observable):
    id: int = 1
    label: str = ""


class AppModel(Observable):
    version: str = get_version()
    api_key: str = ""
    auth_enabled: bool = True
    firstRun: bool = True
    debug: bool = True
    log: bool = True
    utilisateur: UtilisateurModel = UtilisateurModel()
    cloudflare: CloudflareModel = CloudflareModel()
    dossiers: DossierModel = DossierModel()
    applications: List[ApplicationModel] = Field(default_factory=list)
    plex: PlexModel = PlexModel()
    jellyfin: JellyfinModel = JellyfinModel()
    emby: EmbyModel = EmbyModel()

    def __init__(self, **data: Any):
        super().__init__(**data)
        if self.api_key == "":
            self.api_key = generate_api_key()


# ═══════════════════════════════════════════════════════════
# Gestion des symlinks
# ═══════════════════════════════════════════════════════════
class LinkDir(BaseModel):
    path: str
    manager: str  # "sonarr" ou "radarr"


# ═══════════════════════════════════════════════════════════
# Configuration des instances AllDebrid
# ═══════════════════════════════════════════════════════════
class AllDebridInstance(BaseModel):
    """
    Représente une instance AllDebrid.

    Une instance correspond à un compte AllDebrid utilisable
    pour :
    - le scan des torrents présents dans le mount WebDAV
    - la suppression via l’API AllDebrid
    - le nettoyage éventuel du cache Decypharr associé
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

    # Toute la logique AllDebrid / WebDAV / suppression passe désormais par ces instances
    alldebrid_instances: List[AllDebridInstance] = Field(default_factory=list)

    orphan_manager: OrphanManagerConfig = OrphanManagerConfig()
    auto_repair_broken_symlinks: bool = False

    # Options Seasonarr intégrées
    disable_season_pack_check: bool = False
    skip_episode_deletion: bool = False
    require_cached_pack_before_deletion: bool = True
    auto_seasonarr_missing_enabled: bool = False
    auto_seasonarr_missing_run_interval_minutes: int = 180
    auto_seasonarr_missing_max_shows_per_run: int = 50

    # API externes
    radarr_api_key: Optional[str] = None
    sonarr_api_key: Optional[str] = None
    discord_webhook_url: Optional[str] = None
    tmdb_api_key: Optional[str] = None


# ═══════════════════════════════════════════════════════════
# Résultat du scan des orphelins
# ═══════════════════════════════════════════════════════════
class OrphanScanStats(BaseModel):
    """Statistiques du scan."""
    sources: int = Field(..., description="Nombre total de dossiers torrents trouvés sur le mount/WebDAV")
    symlinks: int = Field(..., description="Nombre total de cibles de symlinks trouvées")
    orphans: int = Field(..., description="Nombre total d’orphelins détectés")


class OrphanActions(BaseModel):
    """Actions possibles selon la configuration."""
    auto_delete: bool = Field(..., description="True si la suppression automatique est activée")
    deletable: int = Field(..., description="Nombre d’éléments supprimables automatiquement")


class OrphanScanResult(BaseModel):
    """Résultat principal d’un scan orphelins."""
    scan_date: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    instance: str = Field(..., description="Nom de l’instance AllDebrid traitée")
    mount_path: str = Field(..., description="Chemin du mount/WebDAV AllDebrid scanné")
    duration_seconds: float = Field(..., description="Durée du scan en secondes")
    stats: OrphanScanStats
    orphans: List[str] = Field(
        default_factory=list,
        description="Liste complète des dossiers ou fichiers orphelins détectés"
    )
    actions: OrphanActions