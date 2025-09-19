import os
import re
import json
import asyncio
import shutil
from pathlib import Path
from datetime import datetime
from loguru import logger


class MediasMovies:
    imdb_pattern = re.compile(r"\{imdb-(tt\d+)\}")  # détecter {imdb-ttXXXX}

    def __init__(self, base_dir: Path,
                 api_key: str = None,   # accepté pour compat mais inutilisé
                 cache_file="radarr_cache_local.json",
                 not_found_file="not_found_films.txt"):
        self.base_dir = Path(base_dir)
        self.cache_file = cache_file
        self.not_found_file = not_found_file
        self.pattern = re.compile(r"^(?P<title>.+?) \((?P<year>\d{4})\)")
        self.video_exts = [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"]

        # Charger cache local si dispo
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
            except Exception as e:
                logger.warning(f"⚠️ Cache corrompu ignoré : {e}")
                self.cache = {}
        else:
            self.cache = {}

    # ---------------- UTILITAIRES ----------------

    @staticmethod
    def sanitize_filename(name: str) -> str:
        """Supprime caractères interdits pour noms de fichiers/dossiers"""
        return re.sub(r'[\/:*?"<>|]', "-", name).strip()

    @staticmethod
    def clean_title(name: str) -> str:
        """Nettoie le titre avant recherche Radarr"""
        name = re.sub(r"\{.*?\}", "", name)  # Supprime {imdb-...}
        name = re.sub(r"\((\d{4})\)\s*\(\1\)", r"(\1)", name)  # doublons (2014)(2014)
        name = re.sub(r"- \(\d{4}\)", "", name)  # doublons - (2014)
        name = re.sub(r"\s+", " ", name)  # espaces multiples
        return name.strip()

    # ---------------- RÉSOLUTION IMDb ----------------

    async def get_imdb_id(self, title, year):
        """
        Résolution IMDb ID via Radarr (index + catalog).
        """
        from program.radarr_cache import (
            _radarr_index,
            _radarr_catalog,
            _radarr_idx_lock,
        )
        from program.utils.text_utils import normalize_name, clean_movie_name

        imdb_match = self.imdb_pattern.search(title)

        # 1️⃣ IMDb ID déjà dans le titre
        if imdb_match:
            imdb_id = imdb_match.group(1)
            with _radarr_idx_lock:
                for movie in _radarr_catalog.values():
                    if not isinstance(movie, dict):  # 🔒 sécurité
                        logger.debug(f"[Radarr] Valeur ignorée (pas un dict): {type(movie)} {movie}")
                        continue
                    if str(movie.get("imdbId")) == imdb_id:
                        return {
                            "imdb_id": imdb_id,
                            "title_en": movie.get("title") or movie.get("originalTitle"),
                            "year": str(movie.get("year") or datetime.now().year),
                        }
            return None

        # 2️⃣ Sinon → chercher par titre/année
        cleaned = clean_movie_name(title)
        norm = normalize_name(cleaned)

        with _radarr_idx_lock:
            tmdb_id = None
            if year and f"{norm}{year}" in _radarr_index:
                tmdb_id = _radarr_index[f"{norm}{year}"]
            elif norm in _radarr_index:
                tmdb_id = _radarr_index[norm]

            movie = _radarr_catalog.get(str(tmdb_id)) if tmdb_id else None

        # 🔒 sécurité
        if not isinstance(movie, dict):
            if movie is not None:
                logger.debug(f"[Radarr] movie inattendu pour {title} ({year}): {type(movie)} {movie}")
            return None

        if movie.get("imdbId"):
            return {
                "imdb_id": movie["imdbId"],
                "title_en": movie.get("title") or movie.get("originalTitle"),
                "year": str(movie.get("year") or year or datetime.now().year),
            }

        # ❌ Rien trouvé
        logger.error(f"❌ Pas trouvé dans Radarr : {title} ({year})")
        try:
            with open(self.not_found_file, "a", encoding="utf-8") as nf:
                nf.write(f"{title} ({year})\n")
        except Exception as e:
            logger.warning(f"⚠️ Impossible d'écrire dans {self.not_found_file} : {e}")
        return None

    # ---------------- TRAITEMENT D’UN FILM ----------------

    async def process_movie(self, path: Path, dry_run=True):
        raw_name = path.parent.name
        cleaned_name = self.clean_title(raw_name)

        # --- Regex titre + année ---
        m = self.pattern.match(cleaned_name)
        if m:
            orig_title, year = m.group("title"), m.group("year")
        else:
            logger.debug(f"⚠️ Aucun match regex pour : {cleaned_name}")
            orig_title, year = cleaned_name, None

        # --- Résolution IMDb ---
        result = await self.get_imdb_id(orig_title, year)
        if not result or not result.get("imdb_id"):
            return {"original": str(path), "new": None, "status": "not_found"}

        imdb_id = result["imdb_id"]
        title_en = self.sanitize_filename(result["title_en"])
        year = result["year"]

        # --- Nouveau chemin ---
        new_folder = self.base_dir / f"{title_en} ({year}) {{imdb-{imdb_id}}}"
        new_file = new_folder / f"{title_en} ({year}) {{imdb-{imdb_id}}}{path.suffix}"

        os.makedirs(new_folder, exist_ok=True)

        if str(path) == str(new_file):
            return {"original": str(path), "new": str(new_file), "status": "already_conform"}

        if dry_run:
            logger.info(f"🎬 [DRY-RUN] {path} → {new_file}")
            return {"original": str(path), "new": str(new_file), "status": "renamed", "dry_run": True}

        try:
            shutil.move(str(path), str(new_file))
            logger.success(f"✅ Film renommé : {new_file}")

            # Supprimer dossier parent si vide
            old_folder = path.parent
            try:
                if not any(old_folder.iterdir()):
                    old_folder.rmdir()
            except Exception:
                pass

            return {"original": str(path), "new": str(new_file), "status": "renamed", "dry_run": False}
        except Exception as e:
            logger.error(f"❌ Erreur renommage {path}: {e}")
            return {"original": str(path), "new": None, "status": "error", "error": str(e)}

    # ---------------- MAIN ----------------

    async def run(self, dry_run=True):
        results = []
        total_files = 0
        renamed = 0
        already_conform = 0
        not_found = 0
        errors = 0

        tasks = []
        for ext in self.video_exts:
            for path in self.base_dir.rglob(f"*{ext}"):
                total_files += 1
                tasks.append(self.process_movie(path, dry_run=dry_run))

        movies = await asyncio.gather(*tasks)

        for m in movies:
            if not m:
                continue
            results.append(m)
            status = m.get("status")
            if status == "renamed":
                renamed += 1
            elif status == "already_conform":
                already_conform += 1
            elif status == "not_found":
                not_found += 1
            elif status == "error":
                errors += 1

        # 🔥 Sauvegarde cache
        if self.cache:
            try:
                with open(self.cache_file, "w", encoding="utf-8") as f:
                    json.dump(self.cache, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.warning(f"⚠️ Erreur sauvegarde cache local : {e}")

        # 📊 Log résumé
        logger.info(
            f"📊 Scan terminé : {total_files} fichiers vidéo détectés | "
            f"🔄 {renamed} renommés | "
            f"⏭ {already_conform} déjà conformes | "
            f"❌ {not_found} introuvables | "
            f"⚠️ {errors} erreurs"
        )

        return {
            "stats": {
                "total": total_files,
                "renamed": renamed,
                "already_conform": already_conform,
                "not_found": not_found,
                "errors": errors,
            },
            "results": results,
        }
