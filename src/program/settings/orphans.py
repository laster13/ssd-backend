from typing import List
from datetime import datetime
from pydantic import BaseModel, Field


class OrphanScanStats(BaseModel):
    sources: int = Field(..., description="Nombre total de dossiers torrents présents sur le WebDAV")
    symlinks: int = Field(..., description="Nombre total de cibles de symlinks détectées")
    orphans: int = Field(..., description="Nombre total de dossiers torrents orphelins")


class OrphanActions(BaseModel):
    auto_delete: bool = Field(..., description="Suppression automatique activée ou non")
    deletable: int = Field(..., description="Nombre de dossiers torrents supprimables")


class OrphanScanResult(BaseModel):
    scan_date: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    instance: str = Field(..., description="Nom de l'instance AllDebrid")
    mount_path: str = Field(..., description="Chemin du WebDAV/mount scanné")
    duration_seconds: float = Field(..., description="Durée du scan en secondes")
    stats: OrphanScanStats
    orphans: List[str] = Field(default_factory=list, description="Liste des chemins orphelins détectés")
    actions: OrphanActions