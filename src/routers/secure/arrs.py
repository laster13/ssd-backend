from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from pathlib import Path

from src.services.fonctions_arrs import RadarrService, SonarrService

router = APIRouter(prefix="/arrs", tags=["Arrs"])


class SymlinkPayload(BaseModel):
    symlink: str


@router.post("/rescan-by-symlink", status_code=200)
async def rescan_by_symlink(
    payload: SymlinkPayload,
    radarr: RadarrService = Depends(RadarrService),
    sonarr: SonarrService = Depends(SonarrService)
):
    symlink_path = Path(payload.symlink)

    if not symlink_path.exists():
        raise HTTPException(status_code=404, detail="Symlink non trouvé.")

    if not symlink_path.is_symlink():
        raise HTTPException(status_code=400, detail="Ce n'est pas un symlink valide.")

    target_path = symlink_path.resolve()
    try:
        symlink_path.unlink()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur suppression symlink : {e}")

    # --- Radarr ---
    try:
        movie_id = radarr.get_movie_id_by_path(str(target_path))
        if movie_id:
            radarr.rescan_movie(movie_id)
            radarr.search_missing_movie(movie_id)
            return {"detail": f"🎬 Film détecté (ID {movie_id}) — rescan lancé."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur Radarr : {e}")

    # --- Sonarr ---
    try:
        series_id = sonarr.get_series_id_by_path(str(target_path))
        if series_id:
            sonarr.rescan_series(series_id)
            sonarr.search_missing_episodes(series_id)
            return {"detail": f"📺 Série détectée (ID {series_id}) — rescan lancé."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur Sonarr : {e}")

    return {"detail": "❌ Aucun média trouvé correspondant au fichier supprimé."}
