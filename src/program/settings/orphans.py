from typing import List
from datetime import datetime
from pydantic import BaseModel, Field


class OrphanScanStats(BaseModel):
    """Sous-structure : statistiques du scan."""
    sources: int = Field(..., description="Nombre total de fichiers sources trouvés sur le mount")
    symlinks: int = Field(..., description="Nombre total de symlinks trouvés")
    orphans: int = Field(..., description="Nombre total d'orphelins détectés")


class OrphanActions(BaseModel):
    """Sous-structure : actions disponibles selon la config."""
    auto_delete: bool = Field(..., description="True si la suppression automatique est activée dans la config")
    deletable: int = Field(..., description="Nombre d’éléments supprimables automatiquement")


class OrphanScanResult(BaseModel):
    """Structure principale retournée par /api/v1/orphans/scan"""
    scan_date: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    instance: str = Field(..., description="Nom de l’instance AllDebrid traitée")
    mount_path: str = Field(..., description="Chemin du mount AllDebrid scanné")
    stats: OrphanScanStats
    orphans: List[str] = Field(default_factory=list, description="Liste complète des fichiers orphelins détectés")
    actions: OrphanActions = Field(..., description="Actions effectuées ou possibles selon la config")


# Exemple d’utilisation :
# result = OrphanScanResult(
#     instance="AllDebrid-Main",
#     mount_path="/mnt/alldebrid/torrents",
#     stats=OrphanScanStats(sources=1200, symlinks=1180, orphans=20),
#     orphans=[
#         "/mnt/alldebrid/torrents/MovieX/file1.mkv",
#         "/mnt/alldebrid/torrents/ShowY/S01E01.mp4",
#     ],
#     actions=OrphanActions(auto_delete=False, deletable=20)
# )
