import os
import re
import json
import asyncio
import shutil
from pathlib import Path
from datetime import datetime
from loguru import logger


class MediasMovies:
    """
    Vérifie les films selon leur identifiant IMDb/TMDb et leur conformité Plex,
    en se basant sur le cache Radarr local (JSON).
    Fournit aussi les fonctions de correction (Plex, IMDb, Radarr).
    """

    # détecter {imdb-ttXXXXXXX}
    imdb_pattern = re.compile(r"\{imdb-(tt\d+)\}")
    # détecter {tmdb-XXXXX}
    tmdb_pattern = re.compile(r"\{tmdb-(\d+)\}")
    # Format de base : Titre (Année) [avec tag optionnel]
    # group(1) = titre, group(2) = année, group(3) = tag éventuel (imdb-tt... ou tmdb-...)
    plex_pattern = re.compile(
        r"^(.+?) \((\d{4})\)(?: \{(imdb-tt\d+|tmdb-\d+)\})?$"
    )

    def __init__(
        self,
        base_dir: Path,
        cache_file: Path = Path.home() / ".cache" / "radarr_cache.json",
    ):
        self.base_dir = Path(base_dir)
        self.cache_file = cache_file
        self.video_exts = [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"]

        # --- Charger le cache Radarr ---
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
                logger.info(f"📚 Lancement Scan rename - Cache Radarr chargé depuis {self.cache_file}")
            except Exception as e:
                logger.warning(f"⚠️ Erreur de lecture du cache Radarr : {e}")
                self.cache = {}
        else:
            logger.warning(f"⚠️ Cache Radarr introuvable : {self.cache_file}")
            self.cache = {}

    # ---------------- UTILITAIRES ----------------

    @staticmethod
    def sanitize_filename(name: str) -> str:
        """Supprime les caractères illégaux pour un nom de fichier."""
        return re.sub(r'[\/:*?"<>|]', "-", name).strip()

    def is_plex_formatted(self, folder_name: str, file_name: str) -> bool:
        """
        Conformité Plex stricte souhaitée :
        - Dossier :  Titre (Année) {id}
        - Fichier : Titre (Année) {id}.ext
        - id = imdb-ttXXXXXXX ou tmdb-XXXXXX
        - Année identique
        - Même id dans dossier ET fichier
        ⚠️ Le titre (Texte) peut être différent, on ne le vérifie PAS.
        """
        file_stem = Path(file_name).stem
        folder_match = self.plex_pattern.match(folder_name)
        file_match = self.plex_pattern.match(file_stem)

        if not folder_match or not file_match:
            return False

        folder_title, folder_year, folder_tag = folder_match.groups()
        file_title, file_year, file_tag = file_match.groups()

        # 1) Année identique
        if folder_year != file_year:
            return False

        # 2) Exiger une balise dans les DEUX (dossier + fichier)
        if not folder_tag or not file_tag:
            return False

        # 3) Exiger la même balise (même imdb-tt… ou tmdb-…)
        if folder_tag != file_tag:
            return False

        return True

    # ---------------- RÉSOLUTION IMDb/TMDb DANS LE CACHE RADARR ----------------

    async def get_movie_by_imdb(self, imdb_id: str):
        """Recherche un film dans le cache Radarr via son IMDb ID."""
        if not imdb_id or not isinstance(self.cache, dict):
            return None

        catalog = (
            self.cache.get("catalog")
            or self.cache.get("movies")
            or self.cache
        )

        for movie in catalog.values():
            if not isinstance(movie, dict):
                continue
            if str(movie.get("imdbId")) == imdb_id:
                return {
                    "imdb_id": imdb_id,
                    "tmdb_id": movie.get("tmdbId"),
                    "title": movie.get("title") or movie.get("originalTitle"),
                    "year": str(movie.get("year") or datetime.now().year),
                }

        return None

    async def get_movie_by_tmdb(self, tmdb_id: str):
        """Recherche un film dans le cache Radarr via son TMDb ID."""
        if not tmdb_id or not isinstance(self.cache, dict):
            return None

        catalog = (
            self.cache.get("catalog")
            or self.cache.get("movies")
            or self.cache
        )

        for movie in catalog.values():
            if not isinstance(movie, dict):
                continue
            if str(movie.get("tmdbId")) == str(tmdb_id):
                return {
                    "imdb_id": movie.get("imdbId"),
                    "tmdb_id": tmdb_id,
                    "title": movie.get("title") or movie.get("originalTitle"),
                    "year": str(movie.get("year") or datetime.now().year),
                }

        return None

    # ---------------- ANALYSE D’UN FILM ----------------

    async def process_movie(self, path: Path):
        """Analyse un film et retourne son statut Plex/IMDb/TMDb/Radarr."""

        # On cherche les tags dans le nom du fichier, puis dans le dossier parent
        imdb_match = self.imdb_pattern.search(path.name) or self.imdb_pattern.search(path.parent.name)
        tmdb_match = self.tmdb_pattern.search(path.name) or self.tmdb_pattern.search(path.parent.name)

        imdb_id = imdb_match.group(1) if imdb_match else None
        tmdb_id = tmdb_match.group(1) if tmdb_match else None

        plex_ok = self.is_plex_formatted(path.parent.name, path.name)

        # 🟥 Aucun ID trouvé (ni IMDb ni TMDb) -> vraiment "ID manquant"
        if not imdb_id and not tmdb_id:
            return {
                "original": str(path),
                "status": "imdb_manquant_ou_invalide",
                "raison": "Aucun identifiant IMDb/TMDb détecté dans le nom",
                "plex_ok": plex_ok,
                "imdb_id": None,
                "tmdb_id": None,
            }

        # 🟦 ID trouvé -> on essaie de le résoudre dans le cache Radarr
        movie_info = None

        if imdb_id:
            movie_info = await self.get_movie_by_imdb(imdb_id)

        if not movie_info and tmdb_id:
            movie_info = await self.get_movie_by_tmdb(tmdb_id)

        if not movie_info:
            return {
                "original": str(path),
                "status": "inconnu_dans_radarr",
                "raison": "Identifiant IMDb/TMDb présent mais absent du cache Radarr",
                "plex_ok": plex_ok,
                "imdb_id": imdb_id,
                "tmdb_id": tmdb_id,
            }

        # 🟩 ID connu + Plex OK -> conforme
        if plex_ok:
            return {
                "original": str(path),
                "status": "deja_conforme",
                "raison": "Identifiant IMDb/TMDb reconnu et format Plex correct (dossier + fichier tagués)",
                "plex_ok": True,
                "imdb_id": imdb_id or movie_info.get("imdb_id"),
                "tmdb_id": tmdb_id or movie_info.get("tmdb_id"),
            }
        else:
            return {
                "original": str(path),
                "status": "non_conforme_plex",
                "raison": "Format de nom incompatible avec Plex (tag manquant ou différent entre dossier/fichier)",
                "plex_ok": False,
                "imdb_id": imdb_id or movie_info.get("imdb_id"),
                "tmdb_id": tmdb_id or movie_info.get("tmdb_id"),
            }

    # ---------------- FONCTIONS DE CORRECTION ----------------

    async def fix_plex_format(self, path: Path, dry_run: bool = True):
        """Corrige un fichier movie en format Plex strict, SANS créer de doublons.

        Comportement :
        - Le fichier propre (Titre (Année) {imdb-xxx}.ext) est PRIORITAIRE.
        - Si un fichier propre existe déjà ➜ le fichier sale est SUPPRIMÉ (jamais renommé en (2)).
        - Si pas de fichier propre ➜ on renomme simplement le fichier sale → fichier propre.
        - Jamais de fichiers (2), (3)…
        - Jamais d’écrasement du fichier propre.
        """

        imdb_match = self.imdb_pattern.search(path.name) or self.imdb_pattern.search(path.parent.name)
        imdb_id = imdb_match.group(1) if imdb_match else None
        if not imdb_id:
            return f"Impossible de corriger {path} : IMDb manquant"

        movie = await self.get_movie_by_imdb(imdb_id)
        if not movie:
            return f"IMDb {imdb_id} introuvable dans le cache"

        # nouveau nom propre
        clean_name = f"{self.sanitize_filename(movie['title'])} ({movie['year']}) {{imdb-{imdb_id}}}"
        target_dir = path.parent  # on reste dans le même dossier
        final_clean_file = target_dir / f"{clean_name}{path.suffix}"

        # 1️⃣ le fichier propre existe déjà → alors path est une version sale → on la supprime
        if final_clean_file.exists():
            if path == final_clean_file:
                # déjà propre → rien à faire
                return str(final_clean_file)

            # fichier sale à supprimer
            if dry_run:
                logger.info(f"[DRY-RUN] Supprimerait la version sale : {path}")
                return str(final_clean_file)

            try:
                path.unlink()
                logger.success(f"🗑 Supprimé version sale : {path}")
            except Exception as e:
                logger.error(f"Erreur suppression version sale {path} : {e}")
                return f"Erreur suppression version sale : {e}"

            return str(final_clean_file)

        # 2️⃣ sinon → aucun fichier propre → on renomme path vers le fichier propre
        if dry_run:
            logger.info(f"[DRY-RUN] Renommerait {path} → {final_clean_file}")
            return str(final_clean_file)

        try:
            shutil.move(str(path), str(final_clean_file))
            logger.success(f"✅ Corrigé : {path} → {final_clean_file}")
        except Exception as e:
            logger.error(f"💥 Erreur renommage {path} : {e}")
            return f"Erreur renommage : {e}"

        return str(final_clean_file)

    async def add_missing_imdb(self, path: Path, dry_run: bool = True):
        """
        Tente d'ajouter un IMDb manquant à partir du cache Radarr.
        Utilisé pour les fichiers qui ont un format Plex 'Titre (Année)' sans tag.
        Version optimisée et robuste :
          - normalisation des titres (accents, casse, ponctuation)
          - prise en compte de title ET originalTitle
          - index en mémoire (titre_normalisé, année) -> lookup rapide
          - fallback si l'année ne matche pas exactement (année la plus proche)
        """
        file_stem = Path(path).stem
        match = self.plex_pattern.match(file_stem)
        if not match:
            return f"Format de nom non reconnaissable : {path}"

        title, year = match.groups()[:2]
        year = str(year)

        # --- récupérer le catalogue brut du cache ---
        catalog = self.cache.get("catalog") or self.cache.get("movies") or self.cache
        if not isinstance(catalog, dict) or not catalog:
            return "Cache Radarr vide ou invalide, impossible de retrouver l'IMDb"

        # --- helpers locaux ---

        def normalize_title(t: str) -> str:
            """
            Normalise un titre pour comparaison :
            - enlève les accents
            - minuscule
            - remplace tout sauf lettres/chiffres par des espaces
            - trim
            """
            import unicodedata

            if not t:
                return ""
            t = unicodedata.normalize("NFD", t)
            t = "".join(ch for ch in t if not unicodedata.combining(ch))
            t = t.lower()
            t = re.sub(r"[^a-z0-9]+", " ", t)
            return t.strip()

        norm_title = normalize_title(title)

        # --- construire un index (titre_normalisé, année) -> liste de films ---
        # construit une seule fois et réutilisé ensuite
        if not hasattr(self, "_index_title_year"):
            index: dict[tuple[str, str], list[dict]] = {}
            for movie in catalog.values():
                if not isinstance(movie, dict):
                    continue

                movie_year = movie.get("year")
                if movie_year is None:
                    continue
                movie_year_str = str(movie_year)

                # on indexe à partir de title ET originalTitle
                for key in ("title", "originalTitle"):
                    raw_title = movie.get(key)
                    if not raw_title:
                        continue
                    n = normalize_title(raw_title)
                    if not n:
                        continue
                    index.setdefault((n, movie_year_str), []).append(movie)

            self._index_title_year = index
            logger.info(f"📚 Index Radarr (titre+année) construit avec {len(index)} entrées")

        index: dict[tuple[str, str], list[dict]] = getattr(self, "_index_title_year", {})

        # --- 1) tentative exacte : titre_normalisé + année identique ---
        candidates = index.get((norm_title, year), [])

        # --- 2) fallback : même titre_normalisé toutes années confondues ---
        if not candidates:
            possibles: list[dict] = []
            for (t_norm, y_movie), movies in index.items():
                if t_norm == norm_title:
                    possibles.extend(movies)

            if possibles:
                # choisir le film dont l'année est la plus proche
                def year_distance(m: dict) -> int:
                    try:
                        return abs(int(str(m.get("year"))) - int(year))
                    except Exception:
                        return 9999

                best = min(possibles, key=year_distance)
                candidates = [best]

        if not candidates:
            return f"IMDb non trouvé dans le cache Radarr pour {title} ({year})"

        movie = candidates[0]
        imdb_id = movie.get("imdbId")
        if not imdb_id:
            return f"IMDb non trouvé pour {title} ({year}) (film sans imdbId dans le cache)"

        logger.info(
            f"🎬 IMDb trouvé via cache Radarr pour '{title}' ({year}) → {imdb_id} "
            f"(titre cache = '{movie.get('title') or movie.get('originalTitle')}', année={movie.get('year')})"
        )

        new_name = f"{title} ({year}) {{imdb-{imdb_id}}}"
        new_path = path.parent / f"{new_name}{path.suffix}"

        if dry_run:
            logger.info(f"[DRY-RUN] Renommerait {path} → {new_path}")
        else:
            try:
                shutil.move(str(path), str(new_path))
                logger.success(f"✅ IMDb ajouté : {new_path}")
            except Exception as e:
                logger.error(f"💥 Erreur déplacement lors de l'ajout IMDb pour {path} : {e}")
                return f"Erreur déplacement : {e}"

        return str(new_path)

    async def handle_unknown_in_radarr(self, path: Path, dry_run=True):
        """Log uniquement les films introuvables dans Radarr (aucun fichier créé)."""
        msg = f"{datetime.now().isoformat()} | {path}"
        logger.warning(f"[Radarr inconnu] {msg}")
        return msg

    async def correct_movie(self, film: dict, dry_run=True):
        """Applique la correction appropriée selon le statut du film."""
        path = Path(film["original"])
        status = film["status"]

        try:
            if status == "non_conforme_plex":
                return await self.fix_plex_format(path, dry_run)
            elif status == "imdb_manquant_ou_invalide":
                return await self.add_missing_imdb(path, dry_run)
            elif status == "inconnu_dans_radarr":
                return await self.handle_unknown_in_radarr(path, dry_run)
        except Exception as e:
            logger.error(f"Erreur correction {path}: {e}")
            return None

    # ---------------- ANALYSE SANS CORRECTION ----------------

    async def analyse(self):
        """Analyse uniquement les fichiers vidéo sans appliquer de correction."""
        statistiques = {
            "total": 0,
            "deja_conforme": 0,
            "imdb_manquant_ou_invalide": 0,
            "inconnu_dans_radarr": 0,
            "non_conforme_plex": 0,
            "erreurs": 0,
        }

        details = {k: [] for k in statistiques if k != "total"}
        tasks = []

        for ext in self.video_exts:
            for path in self.base_dir.rglob(f"*{ext}"):
                statistiques["total"] += 1
                tasks.append(self.process_movie(path))

        results = await asyncio.gather(*tasks)

        for film in results:
            if not film:
                continue
            statut = film.get("status")
            if statut not in statistiques:
                continue
            statistiques[statut] += 1
            details[statut].append(film)

        return {"statistiques": statistiques, "details": details}

    # ---------------- ANALYSE + CORRECTION GLOBALE ----------------

    async def run(self, dry_run=True):
        """Analyse et corrige tous les fichiers selon leur statut."""
        result = await self.analyse()

        # --- Application des corrections ---
        logger.info(f"{'🔧 Simulation' if dry_run else '🧩 Application'} des corrections...")
        correction_tasks = [
            self.correct_movie(film, dry_run=dry_run)
            for film in (
                result["details"]["non_conforme_plex"]
                + result["details"]["imdb_manquant_ou_invalide"]
                + result["details"]["inconnu_dans_radarr"]
            )
        ]
        await asyncio.gather(*correction_tasks)

        # --- Résumé global ---
        logger.info(
            f"📊 Résumé : total={result['statistiques']['total']} | "
            f"Déjà conformes={result['statistiques']['deja_conforme']} | "
            f"Non conformes Plex={result['statistiques']['non_conforme_plex']} | "
            f"IMDb manquant/invalide={result['statistiques']['imdb_manquant_ou_invalide']} | "
            f"Inconnus Radarr={result['statistiques']['inconnu_dans_radarr']} | "
            f"Erreurs={result['statistiques']['erreurs']}"
        )

        return result
