import re
import json
import urllib.parse
from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Request, Depends
from typing import List, Optional
from pathlib import Path
from pydantic import BaseModel
from collections import Counter
from fastapi.responses import StreamingResponse
from program.managers.sse_manager import sse_manager
from loguru import logger
from pathlib import Path
from src.services.fonctions_arrs import RadarrService, SonarrService
from fastapi import status
import asyncio
import httpx
import unicodedata
import re
from datetime import datetime
from program.utils.text_utils import normalize_name, clean_movie_name, clean_series_name

sonarr = SonarrService()

router = APIRouter(prefix="/symlinks", tags=["Symlinks"])

symlink_store = []

class ConfigModel(BaseModel):
    links_dirs: List[str]  # Liste de répertoires pour links_dir
    mount_dirs: List[str]  # Liste de répertoires pour mount_dir
    radarr_api_key: str
    sonarr_api_key: str

CONFIG_PATH = Path("data/config.json")

class SymlinkItem(BaseModel):
    symlink: str
    target: str
    ref_count: int

def load_config():
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except json.JSONDecodeError:
            logger.warning("Fichier de configuration JSON invalide, utilisation des valeurs par défaut")
    return {"links_dirs": ["/links"], "mount_dirs": ["/mnt/rd"]}

def save_config(data):
    CONFIG_PATH.parent.mkdir(exist_ok=True, parents=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=2)
    logger.success("Configuration enregistrée")

def scan_symlinks():
    config = load_config()

    links_dirs = [Path(d) for d in config["links_dirs"]]
    mount_dirs = [Path(d).resolve() for d in config["mount_dirs"]]

    for links_dir in links_dirs:
        if not links_dir.exists():
            raise RuntimeError(f"Dossier introuvable : {links_dir}")
    for mount_dir in mount_dirs:
        if not mount_dir.exists():
            raise RuntimeError(f"Dossier introuvable : {mount_dir}")

    symlinks_list = []

    for links_dir in links_dirs:
        for symlink_path in links_dir.rglob("*"):
            if symlink_path.is_symlink():
                try:
                    target_path = symlink_path.resolve(strict=True)
                except FileNotFoundError:
                    target_path = symlink_path.resolve(strict=False)

                # 🔍 Trouver le mount_dir applicable
                matched_mount = None
                for mount_dir in mount_dirs:
                    try:
                        relative_target = target_path.relative_to(mount_dir)
                        matched_mount = mount_dir
                        break
                    except ValueError:
                        continue

                if matched_mount:
                    full_target = str(matched_mount / relative_target)
                else:
                    full_target = str(target_path)

                # 📦 Détection du type (radarr / sonarr)
                lower_symlink = str(symlink_path).lower()
                if "/shows/" in lower_symlink or "/series/" in lower_symlink:
                    symlink_type = "sonarr"
                elif "/movies/" in lower_symlink or "/films/" in lower_symlink:
                    symlink_type = "radarr"
                else:
                    symlink_type = "unknown"

                symlinks_list.append({
                    "symlink": str(symlink_path),
                    "target": full_target,
                    "target_exists": target_path.exists(),
                    "type": symlink_type,
                })

    # 🧮 Calcul des ref_count
    target_counts = Counter(item["target"] for item in symlinks_list if item["target_exists"])

    results = []
    for item in symlinks_list:
        ref_count = target_counts.get(item["target"], 0) if item["target_exists"] else 0
        results.append({
            "symlink": item["symlink"],
            "target": item["target"],
            "ref_count": ref_count,
            "type": item["type"],
        })

    logger.success(f"{len(results)} liens symboliques scannés")
    return results

@router.get("")
def list_symlinks(
    page: int = Query(1, gt=0),
    limit: int = Query(50, gt=0, le=1000),
    search: Optional[str] = None,
    sort: Optional[str] = "symlink",
    order: Optional[str] = "asc",
    orphans: Optional[bool] = False,
    folder: Optional[str] = None,
    all: bool = False  # ⬅️ ajout
):
    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    items = symlink_store.copy()

    # 🔽 Filtre par dossier
    if folder:
        try:
            config = load_config()
            base_dir = Path(config.get("links_dirs", ["/"])[0])
            folder_path = (base_dir / folder).resolve()
            folder_str = str(folder_path)

            items = [i for i in items if i["symlink"].startswith(folder_str)]
        except Exception as e:
            logger.error(f"Erreur filtre folder={folder} : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Erreur dossier {folder}")

    # 🔍 Filtres search et orphelins
    if search:
        search_lower = search.lower()
        items = [i for i in items if search_lower in i["symlink"].lower() or search_lower in i["target"].lower()]

    if orphans:
        items = [i for i in items if i["ref_count"] == 0]

    reverse = order == "desc"
    if sort in {"symlink", "target", "ref_count"}:
        items.sort(key=lambda x: x[sort], reverse=reverse)

    total = len(items)

    if not all:
        start = (page - 1) * limit
        end = start + limit
        paginated = items[start:end]
    else:
        paginated = items  # ⬅️ ignore pagination si all=True

    logger.info(f"✅ Renvoi du cache mémoire — page {page} ({len(paginated)} éléments)")

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "data": paginated,
        "orphaned": sum(1 for i in items if i["ref_count"] == 0),
        "unique_targets": len(set(i["target"] for i in items if i["ref_count"] > 0)),
    }

@router.post("/scan")
def trigger_scan():
    try:
        data = scan_symlinks()
        symlink_store.clear()
        symlink_store.extend(data)
        sse_manager.publish_event("symlink_update", json.dumps({"event": "refreshed"}))
        return {"message": "Scan terminé", "count": len(data), "data": data}
    except Exception as e:
        logger.error(f"Erreur scan symlinks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete/{symlink_path:path}")
async def delete_symlink(
    symlink_path: str,
    radarr: RadarrService = Depends(RadarrService)
):
    logger.debug("🔧 Début de la suppression du symlink")

    try:
        decoded_symlink_path = urllib.parse.unquote(symlink_path)
        logger.debug(f"📥 Chemin symlink brut : {symlink_path}")
        logger.debug(f"📥 Chemin symlink décodé : {decoded_symlink_path}")

        config = load_config()
        links_dirs = [Path(d).resolve() for d in config["links_dirs"]]
        logger.debug(f"📁 Répertoires autorisés (links_dirs) : {links_dirs}")

        full_path = None
        for links_dir in links_dirs:
            candidate = links_dir / decoded_symlink_path
            if candidate.exists() or candidate.is_symlink():
                full_path = candidate
                break

        if not full_path:
            logger.warning(f"❌ Chemin en dehors des links_dirs : {decoded_symlink_path}")
            raise HTTPException(status_code=400, detail="Chemin invalide ou hors des répertoires autorisés")

        symlink_path = full_path
        logger.debug(f"📌 Chemin absolu reconstruit : {symlink_path}")

        if not symlink_path.is_symlink():
            logger.warning(f"⚠️ Le lien symbolique n'existe pas : {symlink_path}")
            raise HTTPException(status_code=404, detail="Lien symbolique introuvable")

        symlink_path.unlink()
        logger.success(f"🗑️ Lien symbolique supprimé avec succès : {symlink_path}")

        target_path = symlink_path.resolve(strict=False)
        logger.debug(f"📍 Chemin de la cible (non strict) : {target_path}")

        # 🆕 Utilisation du nom du dossier parent
        raw_name = symlink_path.parent.name
        logger.debug(f"🎬 Nom brut récupéré : {raw_name}")

        # Assure que la fonction clean_movie_name existe
        if 'clean_movie_name' not in globals():
            raise RuntimeError("La fonction 'clean_movie_name' est manquante ou non importée")

        cleaned_title = clean_movie_name(raw_name)
        logger.debug(f"🧼 Titre nettoyé : {cleaned_title}")

        normalized_cleaned = normalize_name(cleaned_title)
        logger.debug(f"🔠 Titre normalisé : {normalized_cleaned}")

        logger.debug(f"🔍 Recherche du film dans Radarr : {cleaned_title}")
        movie = radarr.get_movie_by_clean_title(cleaned_title)

        if not movie:
            logger.warning(f"❗ Aucun film trouvé correspondant à : {cleaned_title}")
            raise HTTPException(status_code=404, detail=f"Aucun film trouvé avec le nom : {cleaned_title}")

        movie_id = movie["id"]
        logger.info(f"🎯 Film trouvé : {movie.get('title', 'Inconnu')} ({movie.get('year', '?')}) — ID : {movie_id}")

        try:
            logger.debug("🔄 Rafraîchissement du film dans Radarr")
            radarr.refresh_movie(movie_id)
            logger.info(f"✅ Rafraîchissement réussi pour : {movie.get('title', 'Inconnu')}")
        except Exception as e:
            logger.error(f"❌ Erreur lors du rafraîchissement : {type(e).__name__} — {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Erreur rafraîchissement : {e}")

        try:
            logger.debug("⏱️ Pause avant lancement recherche")
            await asyncio.sleep(2)
            logger.debug("📡 Lancement recherche de téléchargement manquant")
            radarr.search_missing_movie(movie_id)
            logger.info(f"📥 Recherche de téléchargement lancée pour : {movie.get('title', 'Inconnu')}")
        except Exception as e:
            logger.error(f"❌ Erreur pendant la recherche : {type(e).__name__} — {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Erreur recherche : {e}")

    except HTTPException as e:
        logger.error(f"🚨 HTTPException capturée : {type(e).__name__} — {e.detail}", exc_info=True)
        raise e
    except Exception as e:
        logger.error(f"💥 Exception inattendue capturée : {type(e).__name__} — {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erreur interne : {type(e).__name__} — {str(e)}")

    logger.info("✅ Suppression et actions Radarr terminées")
    return {"message": "Symlink supprimé et traitement Radarr effectué avec succès."}

@router.get("/config", response_model=ConfigModel)
def get_config():
    return load_config()

@router.post("/config", response_model=ConfigModel)
def save_new_config(config: ConfigModel):
    save_config(config.dict())
    return config

@router.get("/events")
async def symlink_sse(request: Request):
    async def event_generator():
        async for event in sse_manager.subscribe("symlink_update"):
            if await request.is_disconnected():
                break
            yield f"data: {event}\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@router.get("/folders")
def list_subfolders():
    config = load_config()
    links_dirs = config.get("links_dirs", [])
    root_dir = Path(links_dirs[0]) if links_dirs else None

    if not root_dir or not root_dir.exists():
        raise HTTPException(status_code=500, detail="Répertoire links_dirs invalide ou manquant")

    try:
        subdirs = [f.name for f in root_dir.iterdir() if f.is_dir()]
        return subdirs
    except Exception as e:
        logger.error(f"Erreur lecture dossiers : {e}")
        raise HTTPException(status_code=500, detail="Erreur lecture des sous-dossiers")

@router.post("/delete_broken")
async def delete_broken_symlinks(
    folder: Optional[str] = None,
    radarr: RadarrService = Depends(RadarrService)
):

    logger.info("🚀 Suppression en masse des symlinks cassés demandée")

    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    config = load_config()
    base_links_dir = Path(config.get("links_dirs", ["/"])[0]).resolve()

    # Filtrage initial
    items = symlink_store.copy()
    if folder:
        folder_path = (base_links_dir / folder).resolve()
        items = [i for i in items if i["symlink"].startswith(str(folder_path))]

    # Filtrer uniquement les symlinks cassés
    broken_symlinks = [i for i in items if i["ref_count"] == 0]
    if not broken_symlinks:
        return {"message": "Aucun symlink cassé à supprimer", "deleted": 0}

    logger.info(f"🔍 {len(broken_symlinks)} symlinks cassés à traiter")

    deleted_count = 0
    errors = []

    try:
        all_movies = radarr.get_all_movies()
    except Exception as e:
        logger.error(f"❌ Erreur récupération films Radarr : {e}")
        raise HTTPException(status_code=500, detail="Erreur récupération des films")

    for item in broken_symlinks:
        try:
            symlink_path = Path(item["symlink"])
            if not symlink_path.is_symlink():
                logger.warning(f"⛔ Pas un symlink valide : {symlink_path}")
                continue

            symlink_path.unlink()
            logger.success(f"🗑️ Supprimé : {symlink_path}")
            deleted_count += 1

            # Nettoyage + matching
            raw_name = symlink_path.parent.name
            cleaned = clean_movie_name(raw_name)
            normalized_cleaned = normalize_name(cleaned)

            logger.debug(f"🎬 Nom brut = {raw_name} → nettoyé = {cleaned} → normalisé = {normalized_cleaned}")

            match = next(
                (m for m in all_movies if normalize_name(m["title"]) in normalized_cleaned),
                None
            )

            if not match:
                logger.warning(f"❗ Aucun film trouvé pour : {cleaned}")
                continue

            movie_id = match["id"]
            radarr.refresh_movie(movie_id)
            await asyncio.sleep(2)
            radarr.search_missing_movie(movie_id)
            logger.info(f"📥 Recherche relancée pour : {match['title']}")

        except Exception as e:
            error_msg = f"Erreur {item['symlink']}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            errors.append(error_msg)

    # Notification SSE
    sse_manager.publish_event("symlink_update", json.dumps({"event": "refreshed"}))

    return {
        "message": f"{deleted_count} symlinks cassés supprimés",
        "deleted": deleted_count,
        "errors": errors
    }


@router.delete("/delete-sonarr/{symlink_path:path}")
async def delete_symlink_sonarr(
    symlink_path: str,
    sonarr: SonarrService = Depends(SonarrService)
):
    """
    Supprime un lien symbolique côté Sonarr, puis déclenche le rafraîchissement + recherche d’épisodes manquants.
    Si le fichier est toujours manquant après le scan, on supprime tous les symlinks de la saison
    et on appelle l'API delete-sonarr-season.
    """
    logger.debug("🔧 Début de la suppression du symlink (Sonarr)")

    # Décodage de l’URL encodée
    decoded_path = urllib.parse.unquote(symlink_path)
    logger.debug(f"📥 Chemin symlink brut : {symlink_path}")
    logger.debug(f"📥 Chemin symlink décodé : {decoded_path}")

    # Chargement config
    config = load_config()
    links_dirs = [Path(d).resolve() for d in config["links_dirs"]]
    logger.debug(f"📁 Répertoires autorisés (links_dirs) : {links_dirs}")

    # Recherche du chemin absolu basé sur links_dirs
    symlink_path = None
    for base in links_dirs:
        candidate = base / decoded_path
        logger.debug(f"🔍 Test du chemin candidat : {candidate}")
        if candidate.is_symlink() or candidate.exists():
            symlink_path = candidate
            break

    if not symlink_path:
        logger.warning(f"❌ Chemin en dehors des links_dirs : {decoded_path}")
        raise HTTPException(status_code=400, detail="Chemin invalide ou hors des répertoires autorisés")

    logger.debug(f"📌 Chemin absolu du lien symbolique : {symlink_path}")

    try:
        # Vérification existence
        if not symlink_path.is_symlink():
            logger.warning(f"⚠️ Le lien symbolique n'existe pas : {symlink_path}")
            raise HTTPException(status_code=404, detail="Lien symbolique introuvable")

        # Suppression du symlink
        symlink_path.unlink()
        logger.info(f"🗑️ Lien supprimé avec succès : {symlink_path}")

        # Extraction du dossier de la saison et de la série
        season_dir = symlink_path.parent
        series_dir = season_dir.parent
        logger.debug(f"📁 Dossier série : {series_dir}")

        series_folder_name = series_dir.name

        # Nettoyage du nom
        cleaned = clean_series_name(series_folder_name)
        normalized_cleaned = normalize_name(cleaned)

        # Extraction du titre et année éventuelle
        match = re.match(r"^(.*?)(?:\s+\((\d{4})\))?$", cleaned)
        if not match:
            raise HTTPException(status_code=400, detail="Format de nom de série invalide")

        base_title = match.group(1).strip()
        year = int(match.group(2)) if match.group(2) else None

        # Récupération de la série
        series = sonarr.get_series_by_clean_title(cleaned)
        if not series:
            logger.warning(f"❗ Aucune série trouvée pour : {cleaned}")
            raise HTTPException(status_code=404, detail=f"Aucune série trouvée avec le nom : {cleaned}")

        series_id = series["id"]
        logger.info(f"🎯 Série trouvée : {series['title']} — ID : {series_id}")

        # Rafraîchissement + recherche
        logger.debug(f"🔄 Rafraîchissement de la série ID={series_id}")
        sonarr.refresh_series(series_id)

        await asyncio.sleep(5)

        logger.debug(f"🔍 Lancement de la recherche manuelle pour ID={series_id}")
        sonarr.search_missing_episodes(series_id)

        logger.info(f"📥 Recherche d'épisodes lancée pour : {series['title']}")

        # Vérification locale après scan
        await asyncio.sleep(2)
        if symlink_path.exists():
            logger.info("✅ Le fichier est revenu localement après scan. Fin normale.")
            return {"message": "Symlink supprimé et fichier revenu après scan."}

        logger.warning("🚫 Fichier toujours manquant après scan — suppression complète de la saison")

        # Suppression de tous les .mkv de la saison
        deleted_count = 0

        valid_extensions = {".mkv", ".m4v", ".mp4"}

        deleted_count = 0
        for file in season_dir.iterdir():
                if file.suffix.lower() in valid_extensions and (file.is_symlink() or not file.exists()):
                        try:
                                file.unlink()
                                deleted_count += 1
                                logger.info(f"🧹 Supprimé : {file}")
                        except Exception as e:
                                logger.error(f"❌ Erreur suppression fichier : {e}")

        # Extraction robuste du numéro de saison
        match = re.search(r"(\d{1,2})", season_dir.name)
        if not match:
            logger.error(f"❌ Numéro de saison introuvable dans : {season_dir.name}")
            raise HTTPException(status_code=400, detail="Numéro de saison introuvable")
        season_number = int(match.group(1))
        logger.debug(f"🔢 Numéro de saison extrait : {season_number}")

        # Appel de l’API POST /delete-sonarr-season
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.post(
                    "http://localhost:8080/api/v1/symlinks/delete-sonarr-season",
                    params={"series_name": cleaned, "season_number": season_number}
                )
                if response.status_code != 200:
                    logger.error(f"❌ API delete-sonarr-season a échoué : {response.text}")
                    raise HTTPException(status_code=500, detail="Échec suppression saison via delete-sonarr-season")
        except Exception as e:
            logger.error(f"❌ Erreur appel httpx : {str(e)}")
            raise HTTPException(status_code=500, detail="Erreur lors de l'appel à delete-sonarr-season")

        logger.info("✅ Suppression saison effectuée via delete-sonarr-season")
        return {"message": "Symlink supprimé. Saison nettoyée et supprimée côté Sonarr."}

    except HTTPException as e:
        logger.error(f"🚨 HTTPException capturée : {e.detail}")
        raise e
    except Exception as e:
        logger.error("💥 Exception inattendue capturée", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

@router.post("/delete-sonarr-season")
async def delete_sonarr_season(
    series_name: str = Query(..., description="Nom complet de la série"),
    season_number: int = Query(..., description="Numéro de la saison"),
    sonarr: SonarrService = Depends(SonarrService)
):
    logger.info(f"🔁 [delete-sonarr-season] Traitement saison {season_number} pour : {series_name}")

    cleaned_series = clean_series_name(series_name)
    normalized_cleaned = normalize_name(cleaned_series)

    logger.debug(f"🧼 Nom nettoyé : {cleaned_series}")
    logger.debug(f"🔎 Clé de recherche normalisée : {normalized_cleaned}")

    try:
        all_series = sonarr.get_all_series()
        logger.debug(f"📚 {len(all_series)} séries récupérées depuis Sonarr")
    except Exception as e:
        logger.error(f"❌ Erreur récupération séries Sonarr : {e}")
        raise HTTPException(status_code=500, detail="Erreur récupération séries Sonarr")

    match = next(
        (s for s in all_series if normalize_name(s["title"]) in normalized_cleaned),
        None
    )

    if not match:
        logger.warning(f"❗ Série introuvable dans Sonarr pour : {cleaned_series}")
        raise HTTPException(status_code=404, detail="Série introuvable")

    series_id = match["id"]
    logger.info(f"📺 Série trouvée : {match['title']} (ID={series_id})")

    try:
        logger.debug(f"🔄 Rafraîchissement de la série ID={series_id}")
        sonarr.refresh_series(series_id)
        await asyncio.sleep(2)

        logger.debug(f"🔍 Recherche manuelle des épisodes manquants pour ID={series_id}")
        sonarr.search_missing_episodes(series_id)
        logger.info(f"📥 Recherche manuelle lancée pour : {match['title']}")

        await asyncio.sleep(3)

        # 📁 Localisation du dossier série (dans shows/* uniquement)
        config = load_config()
        base_links_dir = Path(config.get("links_dirs", ["/"])[0]).resolve()
        shows_dir = base_links_dir / "shows"

        if not shows_dir.exists():
            logger.warning(f"📁 Répertoire 'shows' introuvable dans : {base_links_dir}")
            raise HTTPException(status_code=404, detail="Répertoire 'shows' introuvable")

        all_dirs = [d for d in shows_dir.iterdir() if d.is_dir()]
        # logger.debug(f"📁 Dossiers trouvés dans /shows : {[d.name for d in all_dirs]}")

        series_dir = next(
            (d for d in all_dirs if normalized_cleaned in normalize_name(d.name)),
            None
        )

        if not series_dir or not series_dir.exists():
            logger.warning("📁 Répertoire série introuvable — abandon")
            raise HTTPException(status_code=404, detail="Répertoire série introuvable")

        logger.debug(f"📁 Répertoire série localisé : {series_dir}")

        # 📂 Localisation du dossier saison
        season_dir = next(
            (d for d in series_dir.glob(f"*{season_number:02d}*") if d.is_dir()),
            None
        )

        if not season_dir:
            logger.warning("📁 Répertoire de la saison introuvable — abandon")
            raise HTTPException(status_code=404, detail="Répertoire saison introuvable")

        logger.debug(f"📁 Répertoire saison localisé : {season_dir}")

        valid_exts = {".mkv", ".mp4", ".m4v"}
        remaining_files = [f for f in season_dir.iterdir() if f.is_file() and f.suffix.lower() in valid_exts]

        logger.debug(f"📂 Fichiers trouvés dans la saison : {[f.name for f in remaining_files]}")

        if remaining_files:
            logger.info(f"✅ Des fichiers sont présents pour la saison {season_number}, aucune suppression nécessaire.")
            return {
                "message": f"Recherche relancée pour la saison {season_number} de {match['title']}. Fichiers présents."
            }

        # 🧹 Aucun fichier → on supprime les éventuels fichiers résiduels
        logger.warning("🚫 Aucun fichier valide trouvé — nettoyage de la saison")

        for f in season_dir.iterdir():
            try:
                f.unlink()
                logger.info(f"🧹 Fichier supprimé : {f}")
            except Exception as e:
                logger.warning(f"⚠️ Échec suppression fichier {f} : {e}")

        # 🔁 Relance complète
        logger.info("📦 Relance recherche complète de la série après nettoyage")
        sonarr.refresh_series(series_id)
        await asyncio.sleep(2)
        sonarr.search_missing_episodes(series_id)

        return {
            "message": f"✅ Saison {season_number} réinitialisée pour {match['title']} — recherche complète relancée"
        }

    except Exception as e:
        logger.warning(f"⚠️ Erreur lors du traitement de la saison : {e}")
        raise HTTPException(status_code=500, detail="Erreur traitement saison Sonarr")

@router.post("/delete_broken_sonarr")
async def delete_broken_sonarr_symlinks(
    folder: Optional[str] = None,
    sonarr: SonarrService = Depends(SonarrService)
):
    logger.info("🚀 Suppression en masse des symlinks Sonarr cassés demandée")

    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    config = load_config()
    base_links_dir = Path(config.get("links_dirs", ["/"])[0]).resolve()

    def is_relative_to(child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False

    items = symlink_store.copy()
    if folder:
        folder_path = (base_links_dir / folder).resolve()
        items = [i for i in items if i["symlink"].startswith(str(folder_path))]
        logger.debug(f"📁 Filtrage sur dossier : {folder_path} — {len(items)} éléments restants")

    broken_symlinks = [
        i for i in items
        if i["ref_count"] == 0 and is_relative_to(Path(i["symlink"]).resolve(), base_links_dir)
    ]

    logger.debug(f"🔎 {len(broken_symlinks)} symlinks cassés trouvés dans {base_links_dir}")

    if not broken_symlinks:
        return {"message": "Aucun symlink cassé Sonarr à supprimer", "deleted": 0}

    logger.info(f"🔍 {len(broken_symlinks)} symlinks Sonarr cassés à traiter")

    deleted_count = 0
    errors = []

    try:
        all_series = sonarr.get_all_series()
        logger.debug(f"📚 {len(all_series)} séries récupérées depuis Sonarr")
    except Exception as e:
        logger.error(f"❌ Erreur récupération séries Sonarr : {e}")
        raise HTTPException(status_code=500, detail="Erreur récupération séries Sonarr")

    for item in broken_symlinks:
        try:
            symlink_path = Path(item["symlink"])
            if not symlink_path.is_symlink():
                logger.warning(f"⛔ Pas un symlink valide : {symlink_path}")
                continue

            logger.debug(f"🧹 Suppression du symlink : {symlink_path}")
            symlink_path.unlink()
            logger.success(f"🗑️ Supprimé : {symlink_path}")
            deleted_count += 1

            series_dir = symlink_path.parent.parent
            raw_name = series_dir.name
            cleaned = clean_series_name(raw_name)
            normalized_cleaned = normalize_name(cleaned)

            match = next(
                (s for s in all_series if normalize_name(s["title"]) in normalized_cleaned),
                None
            )

            if not match:
                logger.warning(f"❗ Aucune série trouvée pour : {cleaned}")
                continue

            series_id = match["id"]
            logger.info(f"📺 Série trouvée : {match['title']} (ID={series_id})")

            # Rafraîchissement uniquement
            logger.debug(f"🔄 Refresh série ID={series_id}")
            sonarr.refresh_series(series_id)
            await asyncio.sleep(2)

            season_dir = symlink_path.parent
            valid_exts = {".mkv", ".mp4", ".m4v"}

            try:
                remaining = [
                    f for f in season_dir.iterdir()
                    if f.suffix.lower() in valid_exts and f.exists()
                ]
                logger.debug(f"📂 Fichiers restants dans {season_dir} : {[f.name for f in remaining]}")
            except Exception as e:
                logger.warning(f"⚠️ Erreur lors du scan du dossier de saison : {e}")
                remaining = []

            if not remaining:
                logger.warning("🚫 Tous les fichiers absents — suppression complète de la saison")

                match_season = re.search(r"(\d{1,2})", season_dir.name)
                if match_season:
                    season_number = int(match_season.group(1))
                    logger.debug(f"🔢 Numéro de saison extrait : {season_number}")

                    try:
                        async with httpx.AsyncClient(timeout=20.0) as client:
                            response = await client.post(
                                "http://localhost:8080/api/v1/symlinks/delete-sonarr-season",
                                params={
                                    "series_name": cleaned,
                                    "season_number": season_number
                                }
                            )
                            if response.status_code != 200:
                                logger.error(f"❌ Appel API delete-sonarr-season échoué : {response.text}")
                            else:
                                logger.info(f"✅ Suppression de la saison {season_number} pour {cleaned}")
                    except Exception as e:
                        logger.error(f"❌ Erreur appel API : {e}")

        except Exception as e:
            msg = f"Erreur {item['symlink']}: {str(e)}"
            logger.error(msg, exc_info=True)
            errors.append(msg)

    sse_manager.publish_event("symlink_update", json.dumps({"event": "refreshed"}))

    return {
        "message": f"{deleted_count} symlinks Sonarr cassés supprimés",
        "deleted": deleted_count,
        "errors": errors
    }

@router.post("/repair-missing-seasons")
async def repair_missing_seasons(
    folder: Optional[str] = None,
    sonarr: SonarrService = Depends(SonarrService)
):

    logger.info("🛠️ Réparation des saisons manquantes demandée")

    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    config = load_config()
    base_links_dir = Path(config.get("links_dirs", ["/"])[0]).resolve()

    # Filtrage par dossier (si fourni)
    items = symlink_store.copy()
    if folder:
        folder_path = (base_links_dir / folder).resolve()
        items = [i for i in items if i["symlink"].startswith(str(folder_path))]

    deleted_count = 0
    errors = []

    try:
        missing_list = sonarr.get_all_series_with_missing_seasons()
    except Exception as e:
        logger.error(f"❌ Erreur récupération séries : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erreur récupération des séries avec saisons manquantes")

    for entry in missing_list:
        series_id = entry["id"]
        series_title = entry["title"]
        raw_missing_seasons = [s for s in entry["missing_seasons"] if s != 0]

        if not raw_missing_seasons:
            logger.info(f"📺 Série '{series_title}' - Aucune saison manquante (hors saison 0)")
            continue

        logger.info(f"📺 Série '{series_title}' - Saisons manquantes détectées : {raw_missing_seasons}")

        # 🔎 Vérifier si les épisodes de la saison sont tous passés
        try:
            all_episodes = sonarr.get_all_episodes(series_id)
        except Exception as e:
            logger.error(f"❌ Erreur récupération épisodes pour '{series_title}': {e}", exc_info=True)
            errors.append(f"{series_title} - episodes")
            continue

        confirmed_missing = []
        for season_num in raw_missing_seasons:
            season_eps = [ep for ep in all_episodes if ep.get("seasonNumber") == season_num]
            if not season_eps:
                confirmed_missing.append(season_num)
                continue

            future_eps = [
                ep for ep in season_eps
                if ep.get("airDateUtc") and ep["airDateUtc"] > datetime.utcnow().isoformat()
            ]
            if future_eps:
                logger.info(f"⏳ Saison {season_num} de '{series_title}' ignorée (épisodes à venir)")
            else:
                confirmed_missing.append(season_num)

        if not confirmed_missing:
            logger.info(f"📺 Série '{series_title}' - Aucune saison à traiter après vérification")
            continue

        logger.info(f"📺 Série '{series_title}' - Saisons confirmées comme manquantes : {confirmed_missing}")

        # 🔗 Match amélioré en nom normalisé
        norm_title = normalize_name(series_title)
        matching_items = [i for i in items if norm_title in normalize_name(i["symlink"])]

        if not matching_items:
            logger.warning(f"⚠️ Aucun symlink trouvé pour : {series_title}")
            continue

        for season_num in confirmed_missing:
            logger.debug(f"🔍 Analyse de la saison {season_num} pour '{series_title}' (ID={series_id})")
            pattern = f"S{season_num:02}"
            filtered_symlinks = [
                i for i in matching_items if pattern.lower() in i["symlink"].lower()
            ]

            if not filtered_symlinks:
                logger.warning(f"⚠️ Aucun symlink trouvé pour la saison {season_num} de '{series_title}'")

            for item in filtered_symlinks:
                symlink_path = Path(item["symlink"])

                if not str(symlink_path).startswith(str(base_links_dir)):
                    logger.warning(f"⛔ Chemin interdit (hors links_dir) : {symlink_path}")
                    continue

                try:
                    if symlink_path.exists() and symlink_path.is_symlink():
                        symlink_path.unlink()
                        logger.success(f"🗑️ Symlink supprimé : {symlink_path}")
                        deleted_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Erreur suppression symlink {symlink_path}: {e}")
                    errors.append(str(symlink_path))

            # 📥 Recherche forcée
            try:
                sonarr.refresh_series(series_id)
                await asyncio.sleep(2)
                sonarr.search_season(series_id=series_id, season_number=season_num)
                logger.info(f"📥 Recherche relancée pour la saison {season_num} de '{series_title}'")
            except Exception as e:
                logger.error(f"❌ Échec recherche saison {season_num} de '{series_title}' : {e}", exc_info=True)
                errors.append(f"{series_title} - S{season_num:02}")

    sse_manager.publish_event("symlink_update", json.dumps({"event": "refreshed"}))

    return {
        "message": "Saisons manquantes traitées",
        "symlinks_deleted": deleted_count,
        "errors": errors
    }

@router.get("/duplicates")
def list_duplicates():
    """
    Retourne les symlinks en doublon (même cible avec ref_count > 1).
    """
    if not symlink_store:
        raise HTTPException(status_code=503, detail="Cache vide, lancez un scan d'abord.")

    # Groupement par cible
    target_map = {}
    for item in symlink_store:
        target = item["target"]
        if item["ref_count"] > 1:
            target_map.setdefault(target, []).append(item)

    # On ne garde que les cibles avec plus d'un symlink
    duplicates = []
    for items in target_map.values():
        if len(items) > 1:
            duplicates.extend(items)

    return {
        "total": len(duplicates),
        "data": duplicates
    }

