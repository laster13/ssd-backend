from fastapi import APIRouter, Query, HTTPException, Body, Request
from pathlib import Path
from loguru import logger
import asyncio
import aiohttp
import shutil
import requests
import json
import time
import unicodedata
import re
import os
from src.services.medias_movies import MediasMovies
from src.services.medias_series import MediasSeries
from src.services.fonctions_arrs import TMDbService
from program.settings.manager import config_manager
from src.services.fonctions_arrs import RadarrService


router = APIRouter(prefix="/rename", tags=["Rename"])

# 🧠 Cache global partagé entre les routes
RADARR_CACHE: dict[str, dict] = {}

@router.post("/scan_only")
async def scan_only():
    """
    🔎 Scan rapide : analyse TOUS les dossiers Radarr configurés (links_dirs)
    - AUCUNE correction
    - Pure analyse IMDb/TMDb + conformité Plex
    - Même logique de scan que /scan, mais sans MediasMovies.run()
    """
    cfg = config_manager.config

    start = time.time()
    statistiques_globales = {
        "total": 0,
        "deja_conforme": 0,
        "non_conforme_plex": 0,
        "imdb_manquant_ou_invalide": 0,
        "inconnu_dans_radarr": 0,
        "erreurs": 0,
    }
    details_global = {k: [] for k in statistiques_globales if k != "total"}

    logger.info("🔎 Lancement du scan-only (AUCUNE correction)")

    tasks = []
    paths = []

    # 🔥 exactement comme dans /scan
    for ld in cfg.links_dirs:
        if ld.manager != "radarr":
            continue

        base_dir = Path(ld.path)
        if not base_dir.exists():
            logger.warning(f"⚠️ Dossier introuvable : {base_dir}")
            continue

        logger.info(f"📁 Scan-only → {base_dir}")
        mm = MediasMovies(base_dir)

        tasks.append(mm.analyse())  # 👈 analyse seule (pas de correction)
        paths.append(base_dir)

    # Exécution parallèle
    results = await asyncio.gather(*tasks)

    # Agrégation
    for resultat in results:
        stats = resultat.get("statistiques", {})
        details = resultat.get("details", {})

        for cle, valeur in stats.items():
            if cle in statistiques_globales:
                statistiques_globales[cle] += valeur

        for cle, items in details.items():
            if cle in details_global:
                details_global[cle].extend(items)

    elapsed = round(time.time() - start, 2)

    logger.info("📊 Résultats du scan-only en {} sec :", elapsed)
    cat_order = [
        "deja_conforme",
        "non_conforme_plex",
        "imdb_manquant_ou_invalide",
        "inconnu_dans_radarr",
        "erreurs",
    ]

    for cat in cat_order:
        logger.info("   - {} : {}", cat, statistiques_globales.get(cat, 0))

    logger.info("📁 Total analysé : {}", statistiques_globales["total"])
    logger.info("✔️ Scan-only terminé (aucune modification appliquée)")

    return {
        "message": "🔎 Scan-only terminé (analyse sans correction)",
        "elapsed": elapsed,
        "statistiques": statistiques_globales,
        "details": details_global,
    }

# -------------------------------------------------------------------------
# 🔍 1️⃣  Scan global (analyse IMDb + Plex)
# -------------------------------------------------------------------------
@router.post("/scan")
async def scan_all_movies(
    dry_run: bool = Query(True, description="Simulation (true) ou exécution réelle (false)")
):
    """
    🔍 Analyse tous les dossiers Radarr configurés (basé sur IMDb + TMDb + vérification Plex).
    Exécute D'ABORD ULTRA_FAST (renommage automatique IMDB/TMDB),
    puis lance l’analyse/correction MediasMovies.
    """

    cfg = config_manager.config

    # -----------------------------------------------
    # 🚀 Étape 0 — ULTRA_FAST systématique
    # -----------------------------------------------
    try:
        logger.info("🚀 Prétraitement ULTRA_FAST (Radarr Only) lancé…")

        resultat_ultra = await fix_imdb_ultra_fast_internal(dry_run=dry_run)

        logger.info(
            f"🏁 ULTRA_FAST terminé : {resultat_ultra.get('modifiés', 0)} "
            f"/ {resultat_ultra.get('total', 0)} dossiers modifiés"
        )

    except Exception as e:
        logger.exception(f"⚠️ ULTRA_FAST a échoué : {e}")
        resultat_ultra = {"error": str(e)}

    # -----------------------------------------------
    # 📊 Statistiques MediasMovies
    # -----------------------------------------------
    statistiques_globales = {
        "total": 0,
        "deja_conforme": 0,
        "non_conforme_plex": 0,
        "imdb_manquant_ou_invalide": 0,
        "inconnu_dans_radarr": 0,
        "erreurs": 0,
    }
    details_global = {k: [] for k in statistiques_globales if k != "total"}

    try:
        # -----------------------------------------------
        # 🔧 Étape 1 — Analyse Plex + IMDb/TMDb (MediasMovies)
        # -----------------------------------------------
        tasks = []
        base_dirs = []

        for ld in cfg.links_dirs:
            if ld.manager != "radarr":
                continue

            base_dir = Path(ld.path)
            if not base_dir.exists():
                logger.warning(f"⚠️ Dossier introuvable : {base_dir}")
                continue

            logger.info(f"🎬 Scan IMDb/TMDb + Plex → {base_dir}")
            gestionnaire = MediasMovies(base_dir=base_dir)

            if dry_run:
                tasks.append(gestionnaire.analyse())
            else:
                tasks.append(gestionnaire.run(dry_run=False))

            base_dirs.append(base_dir)

        # Exécution parallèle
        results = await asyncio.gather(*tasks)

        # -----------------------------------------------
        # 📦 Agrégation des résultats
        # -----------------------------------------------
        for resultat in results:
            stats = resultat.get("statistiques", {})
            details = resultat.get("details", {})

            for cle, valeur in stats.items():
                if cle in statistiques_globales:
                    statistiques_globales[cle] += valeur

            for cle, items in details.items():
                if cle in details_global:
                    details_global[cle].extend(items)

        logger.info(
            f"{'🔎 Simulation' if dry_run else '🧩 Correction réelle'} terminée : "
            f"{statistiques_globales['total']} fichiers | "
            f"{statistiques_globales['deja_conforme']} conformes | "
            f"{statistiques_globales['non_conforme_plex']} non conformes Plex | "
            f"{statistiques_globales['imdb_manquant_ou_invalide']} IMDb/TMDb manquants | "
            f"{statistiques_globales['inconnu_dans_radarr']} inconnus Radarr"
        )

        # -----------------------------------------------
        # 📤 Retour JSON complet
        # -----------------------------------------------
        return {
            "message": (
                "✅ Simulation complète terminée"
                if dry_run
                else "🧩 Corrections appliquées"
            ),
            "simulation": dry_run,

            # 👇 intégration ULTRA_FAST dans la réponse
            "ultra_fast": resultat_ultra,

            # 👇 résultats MediasMovies
            "statistiques": statistiques_globales,
            "details": details_global,
        }

    except Exception as e:
        logger.exception(f"💥 Erreur pendant le scan IMDb/TMDb + Plex : {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/edit")
async def edit_symlink(
    payload: dict = Body(
        ...,
        example={
            "old_path": "/home/ubuntu/Medias/movies/It Happens (2024)/It Happens (2024).mkv",
            "new_folder": "It Happens (2024) {imdb-tt1234567}",
            "new_file": "It Happens (2024) {imdb-tt1234567}",
            "dry_run": True,
        },
    )
):
    """
    ✏️ Permet d’éditer manuellement un symlink/fichier :
      - Renomme le dossier parent (si new_folder fourni)
      - Renomme le fichier (si new_file fourni)
      - Supporte le mode dry_run (simulation)

    Exemple JSON attendu :
    {
        "old_path": "/home/ubuntu/Medias/movies/Old Name (2024)/Old Name (2024).mkv",
        "new_folder": "New Name (2024) {imdb-tt1234567}",
        "new_file": "New Name (2024) {imdb-tt1234567}",
        "dry_run": true
    }
    """
    try:
        old_path = Path(payload.get("old_path", ""))
        new_folder = payload.get("new_folder")
        new_file = payload.get("new_file")
        dry_run = bool(payload.get("dry_run", True))

        if not old_path.exists():
            raise HTTPException(status_code=404, detail="⚠️ Fichier introuvable")

        # Dossier parent
        parent_dir = old_path.parent
        base_root = parent_dir.parent

        # --- 📁 Nouveau dossier ---
        if new_folder:
            new_folder_path = base_root / new_folder
        else:
            new_folder_path = parent_dir

        # --- 🎞️ Nouveau fichier ---
        if new_file:
            new_file_name = f"{new_file}{old_path.suffix}"
        else:
            new_file_name = old_path.name

        # --- 🧩 Nouveau chemin complet ---
        new_path = new_folder_path / new_file_name

        if dry_run:
            logger.info(f"[DRY-RUN] Renommerait :\n - Dossier : {parent_dir.name} → {new_folder_path.name}\n - Fichier : {old_path.name} → {new_file_name}")
        else:
            # Créer le dossier s'il n'existe pas
            new_folder_path.mkdir(parents=True, exist_ok=True)

            # Déplacer le fichier
            shutil.move(str(old_path), str(new_path))

            # Supprimer l'ancien dossier s'il est vide
            try:
                if parent_dir.exists() and not any(parent_dir.iterdir()):
                    parent_dir.rmdir()
            except Exception as cleanup_err:
                logger.warning(f"⚠️ Impossible de supprimer {parent_dir}: {cleanup_err}")

            logger.success(f"✅ Renommage effectué : {old_path} → {new_path}")

        return {
            "message": "Simulation effectuée avec succès" if dry_run else "Renommage effectué avec succès",
            "ancien": str(old_path),
            "nouveau": str(new_path),
            "dry_run": dry_run,
        }

    except Exception as e:
        logger.exception(f"💥 Erreur édition symlink : {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# 🚀 src/routers/secure/sync_radarr.py
# ============================================

@router.post("/sync_radarr")
async def sync_radarr(
    payload: dict = Body(
        ...,
        example={
            "path": "/home/ubuntu/Medias/movies/Underworld (2003) {imdb-tt0320691}/Underworld (2003) {imdb-tt0320691}.mkv",
            "dry_run": True
        },
    )
):
    """
    🔄 Synchronise un film renommé avec Radarr :
      - Recherche via IMDb ID, TMDb ID ou chemin complet
      - Sauvegarde le profil qualité dans le cache global
      - Supprime l’enregistrement Radarr (sans exclusion)
      - Nettoie les exclusions "fantômes"
      - Supporte le mode dry-run
      - Supporte toutes les extensions vidéo
    """
    try:
        path = payload.get("path", "")
        dry_run = bool(payload.get("dry_run", True))

        if not path:
            raise HTTPException(status_code=400, detail="Le champ 'path' est requis.")

        # 🎞️ Extensions vidéo reconnues
        video_exts = [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"]

        # 🔧 Corrige si un fichier a été fourni au lieu d’un dossier
        if os.path.isfile(path) and Path(path).suffix.lower() in video_exts:
            path = os.path.dirname(path)

        imdb_match = re.search(r"tt\d{7,8}", path)
        tmdb_match = re.search(r"tmdb-(\d+)", path)
        imdb_id = imdb_match.group(0) if imdb_match else None
        tmdb_id = tmdb_match.group(1) if tmdb_match else None

        radarr = RadarrService()
        movie = None

        # 🔍 1️⃣ Recherche IMDb
        if imdb_id:
            movie = radarr.get_movie_by_imdb(imdb_id)

        # 🔍 2️⃣ Recherche TMDb
        if not movie and tmdb_id:
            movie = radarr.get_movie_by_tmdb(tmdb_id)

        # 🔍 3️⃣ Recherche par chemin exact
        if not movie:
            all_movies = radarr.get_all_movies()
            movie = next((m for m in all_movies if m.get("path") == path), None)

        if not movie or "id" not in movie:
            raise HTTPException(status_code=404, detail=f"Aucun film trouvé dans Radarr pour le chemin fourni ({path})")

        radarr_id = movie["id"]
        movie_title = movie.get("title", "Inconnu")
        quality_profile_id = movie.get("qualityProfileId")

        # 🧠 Sauvegarde du profil dans le cache global
        cache_key = imdb_id or f"tmdb-{tmdb_id}" or movie_title
        RADARR_CACHE[cache_key] = {
            "profile_id": quality_profile_id,
            "title": movie_title,
            "path": path,
        }
        logger.info(f"🧠 Profil {quality_profile_id} sauvegardé pour {cache_key} ({movie_title})")

        # --- 🧪 Mode simulation ---
        if dry_run:
            logger.info(f"[DRY-RUN] Suppression simulée de {movie_title} (Profil {quality_profile_id})")
            return {
                "message": f"[Simulation] Radarr supprimerait '{movie_title}'",
                "title": movie_title,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
                "quality_profile_id": quality_profile_id,
                "dry_run": True,
            }

        # --- 🗑️ Suppression réelle ---
        delete_url = f"{radarr.base_url}/movie/{radarr_id}?deleteFiles=false&addImportExclusion=false"
        delete_res = requests.delete(delete_url, headers=radarr.headers)

        if not delete_res.ok:
            raise HTTPException(status_code=delete_res.status_code, detail=f"Erreur suppression Radarr: {delete_res.text}")

        logger.success(f"✅ Film '{movie_title}' supprimé de Radarr (profil {quality_profile_id})")

        # 🧹 Nettoyage exclusions fantômes
        exclusions_url = f"{radarr.base_url}/importexclusion"
        excl_res = requests.get(exclusions_url, headers=radarr.headers)
        if excl_res.ok:
            for excl in excl_res.json():
                # IMDb
                if imdb_id and imdb_id in (excl.get("movieTitle") or ""):
                    requests.delete(f"{radarr.base_url}/importexclusion/{excl['id']}", headers=radarr.headers)
                    logger.info(f"🧹 Exclusion IMDb {imdb_id} supprimée")

                # Chemin exact
                elif path in (excl.get("moviePath") or ""):
                    requests.delete(f"{radarr.base_url}/importexclusion/{excl['id']}", headers=radarr.headers)
                    logger.info(f"🧹 Exclusion par chemin supprimée ({path})")

        # 🕐 Pause pour éviter "Path already configured"
        await asyncio.sleep(2)

        return {
            "message": f"✅ Film '{movie_title}' supprimé proprement de Radarr.",
            "title": movie_title,
            "imdb_id": imdb_id,
            "tmdb_id": tmdb_id,
            "quality_profile_id": quality_profile_id,
            "path": path,
            "dry_run": False,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"💥 Erreur sync_radarr : {e}")
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/reimport_radarr")
async def reimport_radarr(
    payload: dict = Body(
        ...,
        example={
            "path": "/home/ubuntu/Medias/movies/My Everything (2024)/My Everything (2024).mkv",
            "dry_run": True,
        },
    )
):
    """
    ♻️ Réimporte un film dans Radarr :
      - Recherche via IMDb, TMDb ou nom de dossier (fallback intelligent)
      - Supprime doublons et exclusions fantômes avant ajout
      - Réutilise le profil qualité depuis le cache
      - Force un rescan du dossier
      - Supporte le mode dry-run
    """
    try:
        path = payload.get("path", "")
        dry_run = bool(payload.get("dry_run", True))

        if not path:
            raise HTTPException(status_code=400, detail="Le champ 'path' est requis.")

        # 🔧 Corriger si un fichier complet a été donné
        if os.path.isfile(path):
            path = os.path.dirname(path)

        radarr = RadarrService()

        # ----------------------------------------
        # 🛑 1️⃣ Vérifier si déjà présent dans Radarr
        # ----------------------------------------
        movies_res = requests.get(f"{radarr.base_url}/movie", headers=radarr.headers)
        if movies_res.ok:
            existing_movies = movies_res.json()

            for mv in existing_movies:
                if mv.get("path") == path:
                    title = mv.get("title", "Inconnu")
                    movie_id = mv.get("id")

                    # 🔍 récupérer le vrai fichier vidéo dans le dossier
                    video_exts = [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"]
                    real_video_file = None

                    for f in Path(path).iterdir():
                        if f.is_file() and f.suffix.lower() in video_exts:
                            real_video_file = f
                            break

                    final_path = str(real_video_file) if real_video_file else path

                    logger.info(f"ℹ️ Déjà présent dans Radarr : {title} (ID {movie_id})")

                    return {
                        "message": f"ℹ️ Déjà présent dans Radarr : {title}",
                        "status": "imported",
                        "raison": "Film déjà existant dans Radarr",
                        "path_final": final_path,
                        "fichier_nouveau": final_path,
                        "id_radarr": movie_id,
                    }

        # ----------------------------------------
        # 2️⃣ Extraction ID / titre
        # ----------------------------------------
        imdb_match = re.search(r"tt\d{7,8}", path)
        tmdb_match = re.search(r"tmdb-(\d+)", path)
        imdb_id = imdb_match.group(0) if imdb_match else None
        tmdb_id = tmdb_match.group(1) if tmdb_match else None

        base_name = Path(path).name
        title_match = re.match(r"(.+?)\s*\((\d{4})\)", base_name)
        if title_match:
            title_term = f"{title_match.group(1).strip()} {title_match.group(2)}"
        else:
            title_term = base_name

        logger.info(f"🎬 Données → IMDb: {imdb_id}, TMDb: {tmdb_id}, term='{title_term}'")

        # ----------------------------------------
        # 3️⃣ Profil qualité
        # ----------------------------------------
        cache_key = imdb_id or f"tmdb-{tmdb_id}" or title_term
        cache_entry = RADARR_CACHE.get(cache_key)
        cached_profile = cache_entry.get("profile_id") if cache_entry else None


        logger.info(f"🧠 Profil qualité utilisé : {cached_profile}")

        # 🧠 Fallback : aucun profil trouvé → détecter profil des films du même rootfolder
        if cached_profile is None:
            logger.info("🔍 Aucun profil trouvé dans le cache — recherche d’un profil existant...")

            all_movies = requests.get(
                f"{radarr.base_url}/movie",
                headers=radarr.headers
            ).json()

            # Trouver d’autres films dans le même dossier racine
            same_root_profiles = []
            for mv in all_movies:
                mv_path = mv.get("path")
                if mv_path and mv_path != path and path.startswith(os.path.dirname(mv_path)):
                    pid = mv.get("qualityProfileId")
                    if pid:
                        same_root_profiles.append(pid)

            if same_root_profiles:
                # On prend le profil dominante
                cached_profile = max(set(same_root_profiles), key=same_root_profiles.count)
                logger.success(
                    f"🎯 Profil détecté automatiquement depuis la bibliothèque : {cached_profile}"
                )
            else:
                logger.warning(
                    "⚠️ Aucun autre film trouvé pour inférer un profil. Radarr utilisera son profil interne."
                )


        # ----------------------------------------
        # 4️⃣ Nettoyage doublons / exclusions
        # ----------------------------------------
        if movies_res.ok:
            existing = [m for m in movies_res.json() if m.get("path") == path]
            for e in existing:
                del_url = f"{radarr.base_url}/movie/{e['id']}?deleteFiles=false&addImportExclusion=false"
                requests.delete(del_url, headers=radarr.headers)

        excl_res = requests.get(f"{radarr.base_url}/importexclusion", headers=radarr.headers)
        if excl_res.ok:
            for excl in excl_res.json():
                if path in (excl.get("moviePath") or ""):
                    del_excl_url = f"{radarr.base_url}/importexclusion/{excl['id']}"
                    requests.delete(del_excl_url, headers=radarr.headers)

        await asyncio.sleep(2)

        # ----------------------------------------
        # 5️⃣ Lookup intelligent
        # ----------------------------------------
        lookup_data = None

        if imdb_id:
            res = requests.get(f"{radarr.base_url}/movie/lookup/imdb?imdbId={imdb_id}", headers=radarr.headers)
            if res.ok and res.json():
                lookup_data = res.json()

        if not lookup_data and tmdb_id:
            res = requests.get(f"{radarr.base_url}/movie/lookup/tmdb?tmdbId={tmdb_id}", headers=radarr.headers)
            if res.ok and res.json():
                lookup_data = res.json()

        if not lookup_data:
            res = requests.get(
                f"{radarr.base_url}/movie/lookup?term={title_term}",
                headers=radarr.headers
            )
            if res.ok and res.json():
                lookup_data = res.json()[0]
            else:
                raise HTTPException(status_code=404, detail=f"Aucun film trouvé sur TMDb pour '{title_term}'")

        if isinstance(lookup_data, list):
            lookup_data = lookup_data[0]

        tmdb_id = lookup_data.get("tmdbId")
        movie_title = lookup_data.get("title", "Inconnu")

        # ----------------------------------------
        # 6️⃣ Simulation
        # ----------------------------------------
        if dry_run:
            return {
                "message": f"[Simulation] Radarr réimporterait '{movie_title}'",
                "path_final": path,
                "fichier_nouveau": path,
            }

        # ----------------------------------------
        # 7️⃣ Ajout dans Radarr
        # ----------------------------------------
        payload_add = {
            "tmdbId": tmdb_id,
            "qualityProfileId": cached_profile,
            "monitored": True,
            "path": path,
            "addOptions": {"searchForMovie": False},
        }

        add_res = requests.post(f"{radarr.base_url}/movie", json=payload_add, headers=radarr.headers)
        if not add_res.ok:
            raise HTTPException(status_code=add_res.status_code, detail=add_res.text)

        new_id = add_res.json().get("id")

        # Rescan
        requests.post(
            f"{radarr.base_url}/command",
            json={"name": "RescanMovie", "movieId": new_id},
            headers=radarr.headers
        )

        # Nettoyage cache
        RADARR_CACHE.pop(cache_key, None)

        # ----------------------------------------
        # 8️⃣ Retour fichier complet
        # ----------------------------------------
        video_exts = [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"]
        real_video_file = next(
            (f for f in Path(path).iterdir() if f.is_file() and f.suffix.lower() in video_exts),
            None
        )

        final_path = str(real_video_file) if real_video_file else path

        return {
            "message": f"✅ Film '{movie_title}' réimporté et rescanné",
            "path_final": final_path,
            "fichier_nouveau": final_path,
            "id_radarr": new_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"💥 Erreur lors du réimport Radarr : {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/fix/imdb_one")
async def fix_imdb_single(
    payload: dict = Body(
        ...,
        example={
            "path": "/home/ubuntu/Medias/movies/Cash (2023)/Cash (2023).mkv",
            "dry_run": True,
            "imdb_id": "tt2737050",
        },
    )
):
    """
    🎯 Corrige le nom d’un film individuel (CACHE RADARR UNIQUEMENT) :
      - Utilise uniquement /home/ubuntu/.cache/radarr_cache.json comme source
      - Matche sur :
          * Titre du dossier (Titre (Année))
          * title ET originalTitle du cache
          * année (tolérance, on prend la plus proche)
      - Option : on peut forcer un IMDb via "imdb_id" dans le payload
      - Récupère imdbId / tmdbId depuis le cache Radarr
      - Renomme le dossier ET le fichier principal avec {imdb-ttXXXX} ou {tmdb-XXXXX}
      - Supporte le mode dry_run (simulation)
    """
    import unicodedata

    try:
        path = payload.get("path")
        dry_run = bool(payload.get("dry_run", False))
        imdb_forced = payload.get("imdb_id")  # ex: "tt2737050"

        if not path:
            raise HTTPException(status_code=400, detail="Le champ 'path' est requis.")

        # 🔧 Corrige si un fichier est fourni au lieu d’un dossier
        if os.path.isfile(path) or path.endswith((".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts")):
            path = os.path.dirname(path)
            logger.warning(f"⚠️ Path corrigé automatiquement vers le dossier : {path}")

        movie_dir = Path(path)
        if not movie_dir.exists():
            raise HTTPException(status_code=404, detail=f"⚠️ Fichier ou dossier introuvable : {path}")

        base_name = movie_dir.name  # ex: "Two Days, One Night (2014)"

        # -------- helpers de normalisation --------

        def strip_tag(text: str) -> str:
            """Enlève un éventuel {imdb-...} ou {tmdb-...} en fin de chaîne."""
            return re.sub(r"\s*\{(imdb-tt\d{7,8}|tmdb-\d+)\}\s*$", "", text).strip()

        def normalize_title(title: str) -> str:
            """
            Normalise un titre pour comparaison :
            - enlève les accents
            - minuscule
            - remplace tout sauf lettres/chiffres par des espaces
            - trim
            """
            if not title:
                return ""
            t = unicodedata.normalize("NFD", title)
            t = "".join(ch for ch in t if not unicodedata.combining(ch))
            t = t.lower()
            t = re.sub(r"[^a-z0-9]+", " ", t)
            return t.strip()

        base_clean = strip_tag(base_name)

        # Extraction Titre + Année à partir du dossier
        title_match = re.match(r"(.+?)\s*\((\d{4})\)$", base_clean)
        if not title_match:
            raise HTTPException(status_code=400, detail=f"Format non reconnu pour {base_name}")

        title = title_match.group(1).strip()
        year_str = title_match.group(2)
        year = int(year_str)
        norm_title = normalize_title(title)

        imdb_id = None
        tmdb_id = None
        title_resolved = title
        year_resolved = year
        source = "RadarrCache"

        # -------- 1️⃣ Lecture du cache Radarr JSON --------

        cache_path: Path = Path.home() / ".cache" / "radarr_cache.json"

        if cache_path.exists():
            try:
                with cache_path.open("r", encoding="utf-8") as f:
                    cache_data = json.load(f)
            except Exception as e:
                logger.warning(f"⚠️ Erreur lecture cache Radarr {cache_path}: {e}")
                cache_data = {}
        else:
            logger.warning(f"⚠️ Cache Radarr introuvable : {cache_path}")
            cache_data = {}

        catalog = (
            cache_data.get("catalog")
            or cache_data.get("movies")
            or cache_data
        )

        if not isinstance(catalog, dict) or not catalog:
            raise HTTPException(
                status_code=500,
                detail="Cache Radarr vide ou invalide, impossible de retrouver l'IMDb"
            )

        chosen_movie = None

        # 🆕 1bis) Si un imdb_id est fourni, on force directement ce film
        if imdb_forced:
            for mv in catalog.values():
                if not isinstance(mv, dict):
                    continue
                if str(mv.get("imdbId")) == str(imdb_forced):
                    chosen_movie = mv
                    break

            if not chosen_movie:
                raise HTTPException(
                    status_code=404,
                    detail=f"IMDb {imdb_forced} introuvable dans le cache Radarr"
                )

            imdb_id = imdb_forced
            tmdb_id = chosen_movie.get("tmdbId") or chosen_movie.get("tmdb_id")
            title_resolved = (
                chosen_movie.get("title")
                or chosen_movie.get("originalTitle")
                or title
            )
            year_resolved = chosen_movie.get("year") or year
            source = "RadarrCacheForcedIMDb"

            logger.info(
                f"🎬 Film sélectionné (IMDb forcé) pour '{base_clean}' → "
                f"{title_resolved} ({year_resolved}) | IMDb={imdb_id} | TMDb={tmdb_id}"
            )

        else:
            # -------- 2️⃣ Matching robuste dans le cache --------

            possibles = []

            for mv in catalog.values():
                if not isinstance(mv, dict):
                    continue

                mv_year = mv.get("year")
                if mv_year is None:
                    continue

                mv_year_str = str(mv_year)

                raw_title = (mv.get("title") or "").strip()
                raw_orig = (mv.get("originalTitle") or "").strip()

                norm_t = normalize_title(raw_title)
                norm_o = normalize_title(raw_orig)

                if not norm_t and not norm_o:
                    continue

                # 💡 Matching plus souple : égalité ou inclusion dans un sens OU dans l'autre
                def match_norm(cand: str, target: str) -> bool:
                    if not cand or not target:
                        return False
                    if cand == target:
                        return True
                    if cand in target:
                        return True
                    if target in cand:
                        return True
                    return False

                title_match_norm_cache = (
                    match_norm(norm_t, norm_title) or match_norm(norm_o, norm_title)
                )

                if not title_match_norm_cache:
                    continue

                try:
                    dist = abs(int(mv_year_str) - year)
                except Exception:
                    dist = 9999

                possibles.append((dist, mv))

            if not possibles:
                raise HTTPException(
                    status_code=404,
                    detail=f"Film introuvable dans le cache Radarr pour '{base_clean}'"
                )

            # on prend le film avec l'année la plus proche
            possibles.sort(key=lambda x: x[0])
            _, chosen_movie = possibles[0]

            imdb_id = chosen_movie.get("imdbId") or chosen_movie.get("imdb_id")
            tmdb_id = chosen_movie.get("tmdbId") or chosen_movie.get("tmdb_id")
            title_resolved = (
                chosen_movie.get("title")
                or chosen_movie.get("originalTitle")
                or title
            )
            year_resolved = chosen_movie.get("year") or year
            source = "RadarrCache"

            logger.info(
                f"🎬 Film sélectionné depuis cache Radarr pour '{base_clean}' → "
                f"{title_resolved} ({year_resolved}) | IMDb={imdb_id} | TMDb={tmdb_id}"
            )

        if not imdb_id and not tmdb_id:
            raise HTTPException(
                status_code=404,
                detail=f"Aucun identifiant IMDb/TMDb disponible dans le cache pour '{base_clean}' (source={source})"
            )

        # -------- 3️⃣ Nouveau nom dossier + fichier --------

        if imdb_id:
            tag = f"imdb-{imdb_id}"
        else:
            tag = f"tmdb-{tmdb_id}"

        new_folder_name = f"{base_clean} {{{tag}}}"
        new_folder_path = movie_dir.parent / new_folder_name

        # Trouver le fichier vidéo principal
        video_exts = [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"]
        video_files = [
            f for f in movie_dir.glob("*")
            if f.is_file() and f.suffix.lower() in video_exts
        ]
        main_file = video_files[0] if video_files else None

        new_file_name = None
        new_file_path = None

        if main_file:
            base_file_stem = re.sub(r"\.[^/.]+$", "", main_file.name)
            base_file_clean = strip_tag(base_file_stem)
            new_file_name = f"{base_file_clean} {{{tag}}}{main_file.suffix}"
            new_file_path = new_folder_path / new_file_name
        else:
            logger.warning(f"⚠️ Aucun fichier vidéo trouvé dans {movie_dir}")

        # -------- 4️⃣ Simulation ou renommage réel --------

        if dry_run:
            logger.info(f"[DRY-RUN] {movie_dir.name} → {new_folder_name}")
            if main_file and new_file_name:
                logger.info(f"[DRY-RUN] {main_file.name} → {new_file_name}")
        else:
            try:
                new_folder_path.mkdir(parents=True, exist_ok=True)

                # Déplace le fichier vidéo (avec renommage)
                if main_file and new_file_path:
                    shutil.move(str(main_file), str(new_file_path))
                    logger.success(f"✅ Fichier renommé : {main_file.name} → {new_file_name}")

                # Déplace les autres éléments du dossier dans le nouveau dossier
                for item in list(movie_dir.iterdir()):
                    if item.exists():
                        target = new_folder_path / item.name
                        if not target.exists():
                            shutil.move(str(item), str(target))

                # Supprime l’ancien dossier vide
                if movie_dir.exists() and not any(movie_dir.iterdir()):
                    movie_dir.rmdir()

                logger.success(f"✅ Dossier renommé : {base_name} → {new_folder_name}")
            except Exception as move_err:
                logger.error(f"💥 Erreur déplacement : {move_err}")
                raise HTTPException(status_code=500, detail=f"Erreur déplacement : {move_err}")

        # -------- 5️⃣ Retour frontend --------

        return {
            "message": (
                f"[Simulation] IDs trouvés pour '{title_resolved}' via {source}"
                if dry_run
                else f"✅ Dossier et fichier renommés : {new_folder_name}"
            ),
            "original": str(movie_dir),
            "nouveau": str(new_folder_path),
            "fichier_original": str(main_file) if main_file else None,
            "fichier_nouveau": str(new_file_path) if main_file and new_file_path else None,
            "path_complet": str(new_file_path or new_folder_path),
            "title": title_resolved,
            "year": year_resolved,
            "imdb_id": imdb_id,
            "tmdb_id": tmdb_id,
            "source": source,
            "dry_run": dry_run,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"   Erreur IMDb/TMDb ciblée (cache Radarr) : {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Async move global (garde-le si tu l'utilises ailleurs) ---
async def async_move(src: str, dst: str):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, shutil.move, src, dst)


@router.post("/fix/imdb/ultra_fast")
async def fix_imdb_ultra_fast(
    body: dict = Body(..., example={"dry_run": True}),
):
    """
    🚀 ULTRA FAST (Radarr ONLY) — renomme dossiers ET fichiers vidéo en ajoutant {imdb-...} ou {tmdb-...}

    🔥 Version MULTI-RACINES :
    - aucune option "folder"
    - toutes les racines Radarr définies dans config.links_dirs sont scannées automatiquement
    """

    import unicodedata

    dry_run = bool(body.get("dry_run", False))

    # ---------------------------------------------------------
    # 🔍 1. Récupérer toutes les racines Radarr depuis config.json
    # ---------------------------------------------------------
    try:
        radarr_roots = [
            Path(ld.path).resolve()
            for ld in config_manager.config.links_dirs
            if getattr(ld, "manager", "") == "radarr"
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Impossible de lire config.links_dirs : {e}")

    if not radarr_roots:
        logger.error("❌ Aucune racine Radarr trouvée dans config.links_dirs !")
        raise HTTPException(status_code=400, detail="Aucune racine Radarr déclarée.")

    for r in radarr_roots:
        if not r.exists() or not r.is_dir():
            raise HTTPException(status_code=400, detail=f"Racine Radarr invalide : {r}")

    logger.info(f"📁 Racines Radarr détectées : {[str(r) for r in radarr_roots]}")

    # ---------------------------------------------------------
    # 🔧 2. (Reprise de TON CODE original : helpers, regex, etc.)
    # ---------------------------------------------------------

    sem = asyncio.Semaphore(32)
    VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".wmv"}

    TAG_REGEX = re.compile(r"\{(imdb-tt\d{7,8}|tmdb-\d+)\}")
    END_TAG_REGEX = re.compile(r"\s*\{(imdb-tt\d{7,8}|tmdb-\d+)\}\s*$")

    def strip_tag(text: str) -> str:
        return END_TAG_REGEX.sub("", text).strip()

    def normalize_title(title: str) -> str:
        if not title:
            return ""
        t = unicodedata.normalize("NFD", title)
        t = "".join(ch for ch in t if not unicodedata.combining(ch))
        t = t.lower()
        t = re.sub(r"[^a-z0-9]+", " ", t)
        return t.strip()

    async def async_move(src: str, dst: str):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, shutil.move, src, dst)

    async def move_item_into_dir(src: Path, dest_dir: Path):
        dest_target = dest_dir / src.name
        if dest_target.exists() and dest_target.is_dir():
            for child in src.iterdir():
                target_child = dest_target / child.name
                await async_move(str(child), str(target_child))
            try:
                if src.exists() and not any(src.iterdir()):
                    src.rmdir()
            except Exception:
                pass
        else:
            await async_move(str(src), str(dest_target))

    async def rename_files_with_tag(target_folder: Path, tag: str):
        renamed_files = []
        for f in target_folder.iterdir():
            if f.is_file() and f.suffix.lower() in VIDEO_EXTS:
                if TAG_REGEX.search(f.name):
                    continue
                base_stem = re.sub(r"\.[^/.]+$", "", f.name)
                base_clean = strip_tag(base_stem)
                new_name = f"{base_clean} {{{tag}}}{f.suffix}"
                new_path = target_folder / new_name
                try:
                    if dry_run:
                        renamed_files.append({"old": str(f), "new": str(new_path)})
                    else:
                        await async_move(str(f), str(new_path))
                        renamed_files.append({"old": str(f), "new": str(new_path)})
                except Exception as e:
                    logger.error(f"Erreur renommage fichier {f}: {e}")
        return renamed_files

    # ---------------------------------------------------------
    # 📚 3. Charger Radarr (inchangé)
    # ---------------------------------------------------------

    from src.services.fonctions_arrs import RadarrService
    radarr = RadarrService()

    try:
        all_movies = radarr.get_all_movies()
    except Exception as e:
        logger.error(f"💥 Impossible de récupérer les films Radarr : {e}")
        raise HTTPException(status_code=500, detail="Impossible de contacter Radarr")

    logger.info(f"📚 {len(all_movies)} films récupérés depuis Radarr")

    # ---------------------------------------------------------
    # 🔍 4. Construire index Radarr (inchangé)
    # ---------------------------------------------------------

    index_by_folder_clean = {}
    index_by_title_norm = {}

    for movie in all_movies:
        try:
            m_path = movie.get("path") or ""
            m_title = (movie.get("title") or "").strip()
            m_orig = (movie.get("originalTitle") or "").strip()
            m_year = movie.get("year")

            if not m_path and not m_title and not m_orig:
                continue

            m_folder = Path(m_path).name if m_path else ""
            folder_clean = strip_tag(m_folder)

            norm_folder = normalize_title(folder_clean)
            norm_title = normalize_title(m_title)
            norm_orig = normalize_title(m_orig)

            entry = {
                "movie": movie,
                "folder_clean": folder_clean,
                "norm_folder": norm_folder,
                "norm_title": norm_title,
                "norm_orig": norm_orig,
                "year": m_year,
            }

            if folder_clean:
                index_by_folder_clean.setdefault(folder_clean, []).append(entry)

            if norm_title:
                index_by_title_norm.setdefault(norm_title, []).append(entry)

            if norm_orig and norm_orig != norm_title:
                index_by_title_norm.setdefault(norm_orig, []).append(entry)

        except Exception as e:
            logger.warning(f"⚠️ Erreur indexation film Radarr: {e}")
            continue

    logger.info(
        f"📚 Index Radarr construit : "
        f"{len(index_by_folder_clean)} clés de dossier, "
        f"{len(index_by_title_norm)} clés de titre"
    )

    # ---------------------------------------------------------
    # 🔄 5. Traitement d’un dossier film (inchangé)
    # ---------------------------------------------------------

    async def process_movie_dir(base_dir: Path):
        async with sem:
            base_name = base_dir.name

            if TAG_REGEX.search(base_name):
                logger.info(f"⏭️ Dossier déjà tagué, skip : {base_name}")
                return {"old": str(base_dir), "skipped": True, "reason": "already_tagged"}

            base_clean = strip_tag(base_name)

            m = re.match(r"(.+?)\s*\((\d{4})\)$", base_clean)
            if not m:
                logger.warning(f"⛔ Format non reconnu (Titre (Année)) : {base_name}")
                return {"old": str(base_dir), "skipped": True, "reason": "invalid_name_format"}

            title = m.group(1).strip()
            year = int(m.group(2))
            norm_title = normalize_title(title)

            candidates_folder = index_by_folder_clean.get(base_clean, [])
            candidates_title = index_by_title_norm.get(norm_title, [])

            chosen = None
            source = "none"

            def year_dist(e):
                y = e.get("year")
                try:
                    return abs(int(y) - year) if y is not None else 9999
                except Exception:
                    return 9999

            if candidates_folder:
                chosen = sorted(candidates_folder, key=year_dist)[0]
                source = "RadarrFolder"
            elif candidates_title:
                chosen = sorted(candidates_title, key=year_dist)[0]
                source = "RadarrTitle"
            else:
                logger.warning(f"⛔ Aucun film Radarr correspondant pour : {base_clean}")
                return {"old": str(base_dir), "skipped": True, "reason": "not_found_in_radarr"}

            movie_data = chosen["movie"]
            imdb_id = movie_data.get("imdbId")
            tmdb_id = movie_data.get("tmdbId")
            title_radarr = movie_data.get("title") or movie_data.get("originalTitle") or title
            year_radarr = movie_data.get("year") or year

            logger.info(
                f"🎬 Match Radarr pour '{base_clean}' → "
                f"{title_radarr} ({year_radarr}) | IMDb={imdb_id} | TMDb={tmdb_id} | source={source}"
            )

            if not imdb_id and not tmdb_id:
                logger.warning(f"⛔ Film trouvé mais sans IMDb/TMDb : {base_clean}")
                return {"old": str(base_dir), "skipped": True, "reason": "no_imdb_tmdb_in_radarr"}

            tag = f"imdb-{imdb_id}" if imdb_id else f"tmdb-{tmdb_id}"

            new_folder_name = f"{base_clean} {{{tag}}}"
            new_folder_path = base_dir.parent / new_folder_name

            if dry_run:
                renamed_files = await rename_files_with_tag(base_dir, tag)
                return {
                    "old": str(base_dir),
                    "new": str(new_folder_path),
                    "files_renamed": renamed_files,
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "source": source,
                    "skipped": False,
                }

            try:
                if new_folder_path.exists():
                    logger.info(f"🔁 Fusion : {base_dir} -> {new_folder_path}")
                    for child in list(base_dir.iterdir()):
                        await move_item_into_dir(child, new_folder_path)
                    try:
                        if base_dir.exists() and not any(base_dir.iterdir()):
                            base_dir.rmdir()
                    except Exception:
                        pass
                    final_folder = new_folder_path
                else:
                    logger.info(f"🔁 Renommage dossier : {base_dir} -> {new_folder_path}")
                    await async_move(str(base_dir), str(new_folder_path))
                    final_folder = new_folder_path

                renamed_files = await rename_files_with_tag(final_folder, tag)

                return {
                    "old": str(base_dir),
                    "new": str(final_folder),
                    "files_renamed": renamed_files,
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "source": source,
                    "skipped": False,
                }

            except Exception as e:
                logger.error(f"💥 Erreur traitement {base_dir}: {e}")
                return {"old": str(base_dir), "skipped": True, "reason": f"exception: {e}"}

    # ---------------------------------------------------------
    # 📁 6. SCAN DES MULTIPLES RACINES RADARR
    # ---------------------------------------------------------

    all_dirs = []
    for root in radarr_roots:
        try:
            dirs = [d for d in root.iterdir() if d.is_dir()]
            logger.info(f"📂 {len(dirs)} dossiers détectés dans {root}")
            all_dirs.extend(dirs)
        except Exception as e:
            logger.error(f"Erreur lecture {root}: {e}")

    total_dirs = len(all_dirs)
    logger.info(f"📦 TOTAL : {total_dirs} dossiers Radarr à analyser")

    tasks = [process_movie_dir(d) for d in all_dirs]
    results_raw = await asyncio.gather(*tasks, return_exceptions=True)

    details = []
    for r in results_raw:
        if isinstance(r, dict):
            details.append(r)
        else:
            details.append({"error": str(r), "skipped": True})

    modifiés = sum(1 for d in details if not d.get("skipped"))

    return {
        "total": total_dirs,
        "modifiés": modifiés,
        "details": details,
        "dry_run": dry_run,
    }

# ---------------------------------------------------------------------
# 🔧 Fonction interne pour appel Python (sans HTTP)
# ---------------------------------------------------------------------
async def fix_imdb_ultra_fast_internal(dry_run: bool):
    """
    Version interne utilisable dans le code Python
    sans passer par une requête HTTP.
    """
    body = {"dry_run": dry_run}
    return await fix_imdb_ultra_fast(body)
