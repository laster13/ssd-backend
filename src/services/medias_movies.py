import os
import re
import json
import asyncio
from pathlib import Path
from datetime import datetime
import shutil
from loguru import logger


class MediasMovies:
    imdb_pattern = re.compile(r"\{imdb-(tt\d+)\}")  # détecter {imdb-ttXXXX}

    def __init__(self, base_dir: Path,
                 api_key: str = None,   # accepté pour compat, mais ignoré
                 cache_file="radarr_cache_local.json",
                 not_found_file="not_found_films.txt"):
        self.base_dir = Path(base_dir)
        self.cache_file = cache_file
        self.not_found_file = not_found_file
        self.pattern = re.compile(r"^(?P<title>.+?) \((?P<year>\d{4})\)")
        self.video_exts = [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"]

        # Cache local (optionnel, pour conserver les résolutions faites)
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "r", encoding="utf-8") as f:
                self.cache = json.load(f)
        else:
            self.cache = {}

    # ---------------- UTILITAIRES ----------------

    @staticmethod
    def sanitize_filename(name: str) -> str:
        """Supprime caractères interdits pour noms de fichiers/dossiers"""
        return re.sub(r'[\/:*?"<>|]', "-", name).strip()

    @staticmethod
    def clean_title(name: str):
        """Nettoie le titre avant recherche Radarr"""
        # Supprime {imdb-...}
        name = re.sub(r"\{.*?\}", "", name)
        # Supprime doublons du type "(2014) (2014)"
        name = re.sub(r"\((\d{4})\)\s*\(\1\)", r"(\1)", name)
        # Supprime doublons "- (YYYY)"
        name = re.sub(r"- \(\d{4}\)", "", name)
        # Supprime espaces multiples
        name = re.sub(r"\s+", " ", name)
        return name.strip()

    async def get_imdb_id(self, title, year):
        """
        Résolution IMDb ID uniquement via Radarr (index + catalog).
        """
        from program.radarr_cache import (
            _radarr_index,
            _radarr_catalog,
            _radarr_idx_lock,
        )
        from program.utils.text_utils import normalize_name, clean_movie_name

        imdb_match = self.imdb_pattern.search(title)

        # 1️⃣ IMDb ID déjà dans le titre → direct
        if imdb_match:
            imdb_id = imdb_match.group(1)
            with _radarr_idx_lock:
                for movie in _radarr_catalog.values():
                    if str(movie.get("imdbId")) == imdb_id:
                        return {
                            "imdb_id": imdb_id,
                            "title_en": movie.get("title") or movie.get("originalTitle"),
                            "year": str(movie.get("year") or datetime.now().year),
                        }
            return None

        # 2️⃣ Sinon → chercher par titre + année dans Radarr
        cleaned = clean_movie_name(title)
        norm = normalize_name(cleaned)

        with _radarr_idx_lock:
            tmdb_id = None
            if year and f"{norm}{year}" in _radarr_index:
                tmdb_id = _radarr_index[f"{norm}{year}"]
            elif norm in _radarr_index:
                tmdb_id = _radarr_index[norm]

            movie = _radarr_catalog.get(str(tmdb_id)) if tmdb_id else None

        if movie and movie.get("imdbId"):
            return {
                "imdb_id": movie["imdbId"],
                "title_en": movie.get("title") or movie.get("originalTitle"),
                "year": str(movie.get("year") or year or datetime.now().year),
            }

        # ❌ Rien trouvé
        logger.error(f"❌ Pas trouvé dans Radarr : {title} ({year})")
        with open(self.not_found_file, "a", encoding="utf-8") as nf:
            nf.write(f"{title} ({year})\n")
        return None

    # ---------------- FILMS ----------------

    async def process_movie(self, path, dry_run=True):
        raw_name = path.parent.name  # nom du dossier brut
        cleaned_name = self.clean_title(raw_name)

        # --- Essai strict avec regex titre + année ---
        m = self.pattern.match(cleaned_name)
        if m:
            orig_title, year = m.group("title"), m.group("year")
        else:
            # Fallback : pas d’année détectée → juste le titre brut
            logger.warning(f"⚠️ Aucun match regex pour : {cleaned_name} → fallback sans année")
            orig_title, year = cleaned_name, None

        # --- Résolution IMDb via Radarr ---
        result = await self.get_imdb_id(orig_title, year)
        if not result or not result.get("imdb_id"):
            logger.error(f"❌ Impossible de résoudre IMDb ID pour : {orig_title} ({year})")
            return None

        imdb_id = result["imdb_id"]
        title_en = self.sanitize_filename(result["title_en"])
        year = result["year"]

        # --- Construction du nouveau chemin ---
        new_folder = self.base_dir / f"{title_en} ({year}) {{imdb-{imdb_id}}}"
        new_file = new_folder / f"{title_en} ({year}) {{imdb-{imdb_id}}}{path.suffix}"

        os.makedirs(new_folder, exist_ok=True)

        if dry_run:
            logger.info(f"🎬 [FILM]\n   {path}\n→  {new_file}\n{'-'*60}")
        else:
            try:
                shutil.move(str(path), str(new_file))
                logger.success(f"✅ Film renommé : {new_file}")

                # supprime dossier parent si vide
                old_folder = path.parent
                try:
                    if not any(old_folder.iterdir()):
                        old_folder.rmdir()
                        logger.info(f"🗑️ Dossier vide supprimé : {old_folder}")
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"❌ Erreur renommage {path}: {e}")

        return {"original": str(path), "new": str(new_file), "dry_run": dry_run}

    # ---------------- MAIN ----------------

    # ---------------- MAIN ----------------
    async def run(self, dry_run=True):
        results = []
        tasks = []
        total_files = 0
        resolved = 0
        renamed = 0
        skipped = 0
        not_found = 0

        for ext in self.video_exts:
            for path in self.base_dir.rglob(f"*{ext}"):
                total_files += 1
                tasks.append(self.process_movie(path, dry_run=dry_run))

        movies = await asyncio.gather(*tasks)

        for m in movies:
            if not m:
                not_found += 1
                continue

            results.append(m)
            if "skipped" in m and m["skipped"]:
                skipped += 1
            else:
                # dry_run → compte comme renommé prévu
                renamed += 1
            resolved += 1

        # 🔥 Sauvegarder le cache local (résolutions faites)
        if self.cache:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)

        logger.info(
            f"📊 Scan terminé : {total_files} fichiers vidéo détectés | "
            f"✅ {resolved} résolus "
            f"(🔄 {renamed} renommés, ⏭ {skipped} déjà conformes) | "
            f"❌ {not_found} introuvables"
        )

        return results
