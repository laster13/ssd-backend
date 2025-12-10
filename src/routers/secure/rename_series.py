import asyncio
import re
import httpx
from fastapi import APIRouter, Query, HTTPException, Body
from fastapi.concurrency import run_in_threadpool
from pathlib import Path
from src.services.medias_series import MediasSeries
from program.settings.manager import config_manager
from program.managers.sse_manager import sse_manager
import os
import requests
from loguru import logger
from src.services.fonctions_arrs import SonarrService


router = APIRouter(
    prefix="/series",
    tags=["Series"]
)

# -------------------------------------------------------------------
# RENAME
# -------------------------------------------------------------------
@router.post("/rename-series")
async def rename_series(
    apply: bool = Query(False, description="Applique réellement le renommage (sinon dry-run)")
):

    cfg = config_manager.config

    if not cfg.tmdb_api_key:
        raise HTTPException(500, "TMDb API key manquante")

    show_dirs = [
        Path(ld.path).resolve()
        for ld in cfg.links_dirs
        if ld.manager == "sonarr"
    ]

    if not show_dirs:
        raise HTTPException(500, "Aucune racine Sonarr trouvée")

    base_dir = show_dirs[0]

    renamer = MediasSeries(base_dir, cfg.tmdb_api_key, apply=apply)

    async def run_task():
        result = await renamer.run()

        sse_manager.publish_event("series_rename_completed", {
            "total": result["stats"]["total"],
            "tmdb_ok": result["stats"]["tmdb_ok"],
            "imdb_present": result["stats"]["imdb_present"],
            "imdb_fallback_ok": result["stats"]["imdb_fallback_ok"],
            "not_found": result["stats"]["not_found"]
        })

        return result

    result = await run_in_threadpool(lambda: asyncio.run(run_task()))

    return {
        "message": "Renommage séries terminé",
        "root": str(base_dir),
        "stats": result["stats"],
        "results": result["results"]
    }


# -------------------------------------------------------------------
# SCAN
# -------------------------------------------------------------------
@router.post("/scan")
async def scan_series():

    cfg = config_manager.config

    tmdb_key = cfg.tmdb_api_key
    if not tmdb_key:
        raise HTTPException(500, "TMDb API key manquante")

    dirs = [
        Path(ld.path).resolve()
        for ld in cfg.links_dirs
        if ld.manager == "sonarr"
    ]

    if not dirs:
        raise HTTPException(500, "Aucune racine Sonarr trouvée")

    base = dirs[0]

    scanner = MediasSeries(base, tmdb_key)

    result = await scanner.scan()

    return {
        "message": "Scan séries terminé",
        "root": str(base),
        "statistiques": result["statistiques"],
        "details": result["details"],
        "elapsed": 0   # <-- OBLIGATOIRE POUR LA PAGE
    }

# -------------------------------------------------------------------
# GET CONTENT OF A SERIES (Saisons + Episodes)
# -------------------------------------------------------------------
@router.get("/content")
async def get_series_content(path: str):

    cfg = config_manager.config

    # Racine Sonarr
    root_dirs = [
        Path(ld.path).resolve()
        for ld in cfg.links_dirs
        if ld.manager == "sonarr"
    ]

    if not root_dirs:
        raise HTTPException(500, "Aucune racine Sonarr trouvée")

    root = root_dirs[0]

    # Le paramètre 'path' n'est PAS un chemin absolu → c'est juste un dossier
    folder_name = path.strip()

    # Dossier brut
    d1 = root / folder_name

    # Dossier nettoyé
    from src.services.medias_series import MediasSeries
    clean = MediasSeries.clean_name(folder_name)
    d2 = root / clean

    if d1.exists() and d1.is_dir():
        serie_dir = d1
    elif d2.exists() and d2.is_dir():
        serie_dir = d2
    else:
        raise HTTPException(
            404,
            f"Aucun dossier trouvé : {d1} ni {d2}"
        )

    # Liste saisons
    seasons = []
    import re

    for sub in sorted(serie_dir.iterdir()):
        if not sub.is_dir():
            continue

        lname = sub.name.lower()

        if not (
            "season" in lname or
            "saison" in lname or
            re.match(r"s\d{2}", lname) or
            re.match(r"\d{1,2}$", lname)
        ):
            continue

        num = None
        m = re.search(r"(\d{1,2})", sub.name)
        if m:
            num = int(m.group(1))

        episodes = [
            {
                "name": f.name,
                "path": str(f),
                "size": f.stat().st_size
            }
            for f in sorted(sub.glob("*"))
            if f.is_file() and f.suffix.lower() in {".mkv", ".mp4", ".avi"}
        ]

        seasons.append({
            "season": num,
            "path": str(sub),
            "episodes": episodes
        })

    return {
        "title": folder_name,
        "clean_name": clean,
        "root": str(root),
        "dir": str(serie_dir),
        "seasons": seasons
    }

@router.post("/edit")
async def edit_series(payload: dict = Body(...)):
    from src.services.medias_series import MediasSeries

    cfg = config_manager.config

    # Racine Sonarr
    root_dirs = [
        Path(ld.path).resolve()
        for ld in cfg.links_dirs
        if ld.manager == "sonarr"
    ]

    if not root_dirs:
        raise HTTPException(500, "Aucune racine Sonarr trouvée")

    root = root_dirs[0]

    raw_name = (payload.get("old_path") or "").strip()
    new_folder = (payload.get("new_folder") or "").strip()
    dry_run = bool(payload.get("dry_run", False))

    if not raw_name:
        raise HTTPException(400, "old_path manquant")

    # On gère comme /content : brut + nettoyé
    clean = MediasSeries.clean_name(raw_name)

    d1 = root / raw_name
    d2 = root / clean

    if d1.exists() and d1.is_dir():
        serie_dir = d1
    elif d2.exists() and d2.is_dir():
        serie_dir = d2
    else:
        raise HTTPException(
            404,
            f"Aucun dossier trouvé pour '{raw_name}' ({d1} ni {d2})"
        )

    if not new_folder:
        raise HTTPException(400, "new_folder manquant")

    new_dir = serie_dir.parent / new_folder

    # Valeurs de chemins pour la réponse
    path_old = str(serie_dir)
    path_new = str(new_dir)

    # --- DRY RUN ---
    if dry_run:
        return {
            "message": f"[Simulation] La série serait renommée en '{new_folder}'",
            "dry_run": True,

            # Infos "modernes"
            "folder_old": Path(path_old).name,
            "folder_new": Path(path_new).name,
            "path_old": path_old,
            "path_new": path_new,

            # Compat avec l’existant Svelte
            "ancien": path_old,
            "nouveau": path_new,
        }

    # --- DÉJÀ AU BON NOM ---
    if serie_dir == new_dir:
        return {
            "message": "Déjà au bon nom",
            "dry_run": False,

            "folder_old": Path(path_old).name,
            "folder_new": Path(path_new).name,
            "path_old": path_old,
            "path_new": path_new,

            "ancien": path_old,
            "nouveau": path_new,
        }

    # --- RENOMMAGE EFFECTIF ---
    serie_dir.rename(new_dir)

    return {
        "message": f"✅ Renommage dossier série effectué → '{new_folder}'",
        "dry_run": False,

        "folder_old": Path(path_old).name,
        "folder_new": Path(path_new).name,
        "path_old": path_old,
        "path_new": path_new,

        "ancien": path_old,
        "nouveau": path_new,
    }

# -------------------------------------------------------------------
# RECHERCHE IMDb + RENOMMAGE POUR UNE SÉRIE
# -------------------------------------------------------------------
@router.post("/imdb-one")
async def imdb_one(payload: dict = Body(...)):
    from src.services.medias_series import MediasSeries

    cfg = config_manager.config

    # Racine Sonarr
    root_dirs = [
        Path(ld.path).resolve()
        for ld in cfg.links_dirs
        if ld.manager == "sonarr"
    ]

    if not root_dirs:
        raise HTTPException(500, "Aucune racine Sonarr trouvée")

    root = root_dirs[0]

    raw = (payload.get("path") or payload.get("folder") or "").strip()
    dry_run = bool(payload.get("dry_run", False))

    if not raw:
        raise HTTPException(400, "Champ 'path' ou 'folder' manquant")

    try:
        # Comme /edit : brut + nettoyé
        clean = MediasSeries.clean_name(raw)

        d1 = root / raw
        d2 = root / clean

        if d1.exists() and d1.is_dir():
            serie_dir = d1
        elif d2.exists() and d2.is_dir():
            serie_dir = d2
        else:
            raise HTTPException(
                404,
                f"Aucun dossier trouvé pour '{raw}' ({d1} ni {d2})"
            )

        # On part du nom réel du dossier
        base_name = MediasSeries.clean_name(serie_dir.name)

        year_match = re.search(r"\((\d{4})\)", base_name)
        year = year_match.group(1) if year_match else None
        title = re.sub(r"\(\d{4}\)", "", base_name).strip()

        # Service pour réutiliser imdb_fallback + normalize_filename
        svc = MediasSeries(root, cfg.tmdb_api_key or "")

        async with httpx.AsyncClient(timeout=10) as client:
            imdb_fb = await svc.imdb_fallback(client, title, year)

        if not imdb_fb:
            raise HTTPException(
                404,
                f"Aucun résultat IMDb pour '{title}' ({year or 'année inconnue'})"
            )

        imdb_id = imdb_fb["imdb_id"]
        serie_title = imdb_fb["name"]
        imdb_year = imdb_fb["year"]

        # Nouveau nom de dossier Plex-like
        safe_name = svc.normalize_filename(serie_title)
        new_folder = f"{safe_name} ({imdb_year}) {{imdb-{imdb_id}}}"
        new_dir = serie_dir.parent / new_folder

        # Renommage réel si pas dry-run
        if not dry_run and new_dir != serie_dir:
            serie_dir.rename(new_dir)

        path_old = str(serie_dir)
        path_new = str(new_dir)

        # Message uniforme type Radarr
        if dry_run:
            msg = f"[Simulation] Sonarr renommerait la série en '{new_folder}'"
        else:
            msg = f"✅ Série renommée en '{new_folder}'"

        return {
            "message": msg,
            "dry_run": dry_run,

            # Infos "standardisées" comme pour Radarr
            "title": serie_title,
            "imdb_id": imdb_id,
            "year": imdb_year,
            "path_old": path_old,
            "path_new": path_new,

            # Champs historiques utilisés par ton front actuel
            "query": {
                "raw": raw,
                "title": title,
                "year": year,
            },
            "imdb": imdb_fb,
            "ancien": path_old,
            "nouveau": path_new,
        }

    except HTTPException:
        # On relaisse passer les HTTPException définies plus haut
        raise
    except Exception as e:
        # Toute autre erreur → 500 JSON propre
        raise HTTPException(500, f"Erreur interne imdb-one: {e!r}")

# Cache partagé avec sync_sonarr
SONARR_CACHE: dict[str, dict] = {}


@router.post("/sync_sonarr")
async def sync_sonarr(
    payload: dict = Body(
        ...,
        example={
            "path": "Anticosti (2025) {imdb-tt1234567}",
            "dry_run": True
        },
    )
):
    """
    🔄 Synchronise une série renommée avec Sonarr à partir d'un *nom de dossier* :
      - path = nom de dossier (ex: "Anticosti (2025) {imdb-tt1234567}")
      - Résout la série via resolve_series (titre/IMDb/TMDb)
      - Sauvegarde les profils dans un cache global (SONARR_CACHE)
      - Supprime la série dans Sonarr (sans supprimer les fichiers, sans exclusion)
      - Supporte le mode dry-run
    """
    try:
        raw_input = (payload.get("path") or "").strip()
        dry_run = bool(payload.get("dry_run", True))

        if not raw_input:
            raise HTTPException(400, "Le champ 'path' est requis.")

        # Si jamais on reçoit un chemin complet, on ne garde que le nom final
        raw_name = Path(raw_input).name

        sonarr = SonarrService()

        # 🆔 identifiants potentiels dans le nom
        imdb_match = re.search(r"imdb-(tt\d+)", raw_name, re.IGNORECASE)
        tmdb_match = re.search(r"tmdb-(\d+)", raw_name, re.IGNORECASE)

        imdb_id = imdb_match.group(1) if imdb_match else None
        tmdb_id = int(tmdb_match.group(1)) if tmdb_match else None

        serie = None

        # 🔍 1️⃣ IMDb explicite
        if imdb_id:
            serie = sonarr.get_series_by_imdb(imdb_id)

        # 🔍 2️⃣ TMDb explicite
        if not serie and tmdb_id:
            serie = sonarr.get_series_by_tmdb(tmdb_id)

        # 🔍 3️⃣ Résolution intelligente via resolve_series
        if not serie:
            serie = sonarr.resolve_series(raw_name)

        # 🔍 4️⃣ Dernier recours : chemin Sonarr se terminant par le nom de dossier
        if not serie:
            all_series = sonarr.get_all_series()
            serie = next(
                (
                    s
                    for s in all_series
                    if (s.get("path") or "").endswith(raw_name)
                ),
                None,
            )

        if not serie or "id" not in serie:
            raise HTTPException(
                404,
                f"Aucune série trouvée dans Sonarr pour le nom fourni ({raw_name})",
            )

        sonarr_id = serie["id"]
        serie_title = serie.get("title", "Inconnue")
        quality_profile_id = serie.get("qualityProfileId")
        language_profile_id = serie.get("languageProfileId")
        sonarr_path = serie.get("path")

        # 🧠 Cache (utile plus tard pour réimport)
        cache_key = imdb_id or f"tmdb-{tmdb_id}" or serie_title
        SONARR_CACHE[cache_key] = {
            "quality_profile_id": quality_profile_id,
            "language_profile_id": language_profile_id,
            "title": serie_title,
            "path": sonarr_path,
        }
        logger.info(
            f"🧠 Profil Sonarr (qualité {quality_profile_id}, langue {language_profile_id}) "
            f"sauvegardé pour {cache_key} ({serie_title})"
        )

        # --- 🧪 DRY RUN ---
        if dry_run:
            logger.info(
                f"[DRY-RUN] Suppression simulée de la série {serie_title} "
                f"(Profil {quality_profile_id}, Langue {language_profile_id})"
            )
            return {
                "message": f"[Simulation] Sonarr supprimerait la série '{serie_title}'",
                "title": serie_title,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "quality_profile_id": quality_profile_id,
                "language_profile_id": language_profile_id,
                "path": sonarr_path,   # 👈 même clé que Radarr
                "raw_name": raw_name,
                "dry_run": True,
            }

        # --- 🗑️ SUPPRESSION RÉELLE ---
        delete_url = f"{sonarr.base_url}/series/{sonarr_id}?deleteFiles=false&addImportExclusion=false"
        delete_res = requests.delete(delete_url, headers=sonarr.headers)

        if not delete_res.ok:
            raise HTTPException(
                delete_res.status_code,
                f"Erreur suppression Sonarr: {delete_res.text}",
            )

        logger.success(
            f"✅ Série '{serie_title}' supprimée de Sonarr "
            f"(profil {quality_profile_id}, langue {language_profile_id})"
        )

        # 🕐 Petite pause
        await asyncio.sleep(2)

        return {
            "message": f"✅ Série '{serie_title}' supprimée proprement de Sonarr.",
            "title": serie_title,
            "imdb_id": imdb_id,
            "tmdb_id": tmdb_id,
            "quality_profile_id": quality_profile_id,
            "language_profile_id": language_profile_id,
            "path": sonarr_path,   # 👈 comme Radarr
            "raw_name": raw_name,
            "dry_run": False,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"💥 Erreur sync_sonarr : {e}")
        raise HTTPException(500, detail=str(e))


@router.post("/reimport_sonarr")
async def reimport_sonarr(
    payload: dict = Body(
        ...,
        example={
            "path": "Anticosti (2025) {imdb-tt1234567}",
            "dry_run": True,
        },
    )
):
    """
    ♻️ Réimporte une série dans Sonarr à partir du nom de dossier :
      - path = nom de dossier sous la racine Sonarr (ou chemin complet)
      - Résout le chemin réel sur disque via la racine Sonarr
      - Vérifie si la série existe déjà dans Sonarr
      - Réutilise les profils qualité/langue depuis SONARR_CACHE si dispo
      - Fallback : infère les profils depuis d'autres séries du même root
      - Lookup intelligent via IMDb, TMDb ou titre+année
      - Ajoute la série + RescanSeries
      - Supporte le mode dry-run
    """
    try:
        raw_input = (payload.get("path") or "").strip()
        dry_run = bool(payload.get("dry_run", True))

        if not raw_input:
            raise HTTPException(400, "Le champ 'path' est requis.")

        cfg = config_manager.config

        # Récupérer la racine Sonarr (comme dans /scan, /edit, /content)
        root_dirs = [
            Path(ld.path).resolve()
            for ld in cfg.links_dirs
            if ld.manager == "sonarr"
        ]

        if not root_dirs:
            raise HTTPException(500, "Aucune racine Sonarr trouvée")

        root = root_dirs[0]

        # Peut être un chemin absolu OU juste le nom du dossier
        p = Path(raw_input)

        if p.is_absolute():
            series_dir = p
        else:
            # On reproduit la logique de /edit : brut + nettoyé
            clean = MediasSeries.clean_name(raw_input)

            d1 = root / raw_input
            d2 = root / clean

            if d1.exists() and d1.is_dir():
                series_dir = d1
            elif d2.exists() and d2.is_dir():
                series_dir = d2
            else:
                raise HTTPException(
                    404,
                    f"Aucun dossier trouvé pour '{raw_input}' ({d1} ni {d2})"
                )

        path = str(series_dir)
        base_name = series_dir.name

        sonarr = SonarrService()

        # ----------------------------------------
        # 🛑 1️⃣ Vérifier si déjà présent dans Sonarr
        # ----------------------------------------
        existing_series = sonarr.get_all_series()
        already = next((s for s in existing_series if s.get("path") == path), None)

        if already:
            serie_title = already.get("title", "Inconnue")
            serie_id = already.get("id")
            quality_profile_id = already.get("qualityProfileId")
            language_profile_id = already.get("languageProfileId")

            logger.info(f"ℹ️ Déjà présent dans Sonarr : {serie_title} (ID {serie_id})")

            return {
                "message": f"ℹ️ Déjà présente dans Sonarr : {serie_title}",
                "status": "imported",
                "raison": "Série déjà existante dans Sonarr",
                "title": serie_title,
                "path_final": path,
                "id_sonarr": serie_id,
                "quality_profile_id": quality_profile_id,
                "language_profile_id": language_profile_id,
            }

        # ----------------------------------------
        # 2️⃣ Extraction IMDb / TMDb / titre
        # ----------------------------------------
        imdb_match = re.search(r"tt\d{7,8}", base_name)
        tmdb_match = re.search(r"tmdb-(\d+)", base_name, re.IGNORECASE)
        imdb_id = imdb_match.group(0) if imdb_match else None
        tmdb_id = tmdb_match.group(1) if tmdb_match else None

        title_match = re.match(r"(.+?)\s*\((\d{4})\)", base_name)
        if title_match:
            title_term = f"{title_match.group(1).strip()} {title_match.group(2)}"
        else:
            title_term = base_name

        logger.info(f"📺 Données → IMDb: {imdb_id}, TMDb: {tmdb_id}, term='{title_term}'")

        # ----------------------------------------
        # 3️⃣ Profils qualité / langue depuis le cache
        # ----------------------------------------
        cache_key = imdb_id or f"tmdb-{tmdb_id}" or title_term
        cache_entry = SONARR_CACHE.get(cache_key)

        quality_profile_id = cache_entry.get("quality_profile_id") if cache_entry else None
        language_profile_id = cache_entry.get("language_profile_id") if cache_entry else None

        logger.info(
            f"🧠 Profils Sonarr utilisés → qualité={quality_profile_id}, langue={language_profile_id}"
        )

        # 🧠 Fallback : inférer depuis d'autres séries du même root
        if quality_profile_id is None or language_profile_id is None:
            logger.info("🔍 Aucun profil trouvé dans le cache — recherche d’un profil existant...")

            all_series = existing_series or sonarr.get_all_series()
            same_root_quality: list[int] = []
            same_root_language: list[int] = []

            for s in all_series:
                s_path = s.get("path")
                if not s_path:
                    continue

                # même répertoire parent
                if os.path.dirname(s_path) == os.path.dirname(path):
                    qid = s.get("qualityProfileId")
                    lid = s.get("languageProfileId")
                    if qid:
                        same_root_quality.append(qid)
                    if lid:
                        same_root_language.append(lid)

            if same_root_quality:
                from collections import Counter
                quality_profile_id = Counter(same_root_quality).most_common(1)[0][0]

            if same_root_language:
                from collections import Counter
                language_profile_id = Counter(same_root_language).most_common(1)[0][0]

            if quality_profile_id:
                logger.success(
                    f"🎯 Profil qualité détecté automatiquement : {quality_profile_id}"
                )
            else:
                logger.warning(
                    "⚠️ Aucun profil qualité trouvé — Sonarr utilisera son profil par défaut."
                )

            if language_profile_id:
                logger.success(
                    f"🎯 Profil langue détecté automatiquement : {language_profile_id}"
                )
            else:
                logger.warning(
                    "⚠️ Aucun profil langue trouvé — Sonarr utilisera son profil par défaut."
                )

        # ----------------------------------------
        # 4️⃣ Lookup intelligent (IMDb → TMDb → titre)
        # ----------------------------------------
        lookup_data = None

        # IMDb direct
        if imdb_id:
            res = requests.get(
                f"{sonarr.base_url}/series/lookup?term=imdb:{imdb_id}",
                headers=sonarr.headers,
            )
            if res.ok and res.json():
                data = res.json()
                lookup_data = data[0] if isinstance(data, list) else data

        # TMDb direct
        if not lookup_data and tmdb_id:
            res = requests.get(
                f"{sonarr.base_url}/series/lookup?term=tmdb:{tmdb_id}",
                headers=sonarr.headers,
            )
            if res.ok and res.json():
                data = res.json()
                lookup_data = data[0] if isinstance(data, list) else data

        # Fallback par titre
        if not lookup_data:
            res = requests.get(
                f"{sonarr.base_url}/series/lookup?term={title_term}",
                headers=sonarr.headers,
            )
            if res.ok and res.json():
                data = res.json()
                lookup_data = data[0] if isinstance(data, list) else data
            else:
                raise HTTPException(
                    404,
                    f"Aucune série trouvée sur Sonarr pour '{title_term}'",
                )

        tvdb_id = lookup_data.get("tvdbId")
        serie_title = lookup_data.get("title", base_name)

        # ----------------------------------------
        # 5️⃣ Simulation
        # ----------------------------------------
        if dry_run:
            return {
                "message": f"[Simulation] Sonarr réimporterait la série '{serie_title}'",
                "title": serie_title,
                "path_final": path,
                "dossier_serie": path,
                "tvdb_id": tvdb_id,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "quality_profile_id": quality_profile_id,
                "language_profile_id": language_profile_id,
                "dry_run": True,
            }

        # ----------------------------------------
        # 6️⃣ Ajout dans Sonarr
        # ----------------------------------------
        root_folder = str(root)

        payload_add = {
            "tvdbId": tvdb_id,
            "title": serie_title,
            "qualityProfileId": quality_profile_id,
            "languageProfileId": language_profile_id,
            "monitored": True,
            "rootFolderPath": root_folder,
            "path": path,
            "seasonFolder": True,
            "seriesType": lookup_data.get("seriesType", "standard"),
            "addOptions": {
                "searchForMissingEpisodes": False,
                "searchForCutoffUnmetEpisodes": False,
                "monitor": "all",
            },
        }

        add_res = requests.post(
            f"{sonarr.base_url}/series",
            json=payload_add,
            headers=sonarr.headers,
        )
        if not add_res.ok:
            raise HTTPException(add_res.status_code, add_res.text)

        new_id = add_res.json().get("id")

        # Rescan
        requests.post(
            f"{sonarr.base_url}/command",
            json={"name": "RescanSeries", "seriesId": new_id},
            headers=sonarr.headers,
        )

        # Nettoyage cache
        SONARR_CACHE.pop(cache_key, None)

        return {
            "message": f"✅ Série '{serie_title}' réimportée et rescannée",
            "title": serie_title,
            "path_final": path,
            "dossier_serie": path,
            "id_sonarr": new_id,
            "imdb_id": imdb_id,
            "tmdb_id": tmdb_id,
            "quality_profile_id": quality_profile_id,
            "language_profile_id": language_profile_id,
            "dry_run": False,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"💥 Erreur lors du réimport Sonarr : {e}")
        raise HTTPException(500, detail=str(e))
