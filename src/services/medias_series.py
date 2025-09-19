import os
import re
import json
import asyncio
import aiohttp
import shutil
from pathlib import Path
from loguru import logger


class MediasSeries:
    def __init__(self, base_dir: Path, sonarr_url: str, sonarr_key: str,
                 not_found_file="not_found_series.txt", max_concurrent=10, apply=False):
        self.base_dir = Path(base_dir)
        self.sonarr_url = sonarr_url.rstrip("/")
        self.sonarr_key = sonarr_key
        self.not_found_file = not_found_file
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.apply = apply

    # ----------------- UTILITAIRES -----------------

    @staticmethod
    def normalize_filename(name: str) -> str:
        return re.sub(r'[\/:*?"<>|]', "-", name).strip()

    def log_not_found(self, title, year=None):
        try:
            with open(self.not_found_file, "a", encoding="utf-8") as f:
                f.write(f"{title} ({year}) [TV]\n")
        except Exception as e:
            logger.warning(f"⚠️ Impossible d'écrire dans {self.not_found_file}: {e}")

    @staticmethod
    def clean_name(raw: str) -> str:
        """Nettoie le nom du dossier en enlevant imdb, années parasites, etc."""
        name = re.sub(r"\{?imdb-?tt\d+\}?", "", raw, flags=re.IGNORECASE)
        name = re.sub(r"tt\d+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"-\s*\(\d{4}\)", "", name)
        name = re.sub(r"\s+-\s+", " ", name)
        name = re.sub(r"\s{2,}", " ", name)
        return name.strip()

    @staticmethod
    def extract_imdb_id(raw: str) -> str | None:
        match = re.search(r"(tt\d+)", raw)
        return match.group(1) if match else None

    # ----------------- SONARR API -----------------

    async def sonarr_lookup(self, session, query: str):
        """Recherche une série via l’API Sonarr"""
        url = f"{self.sonarr_url}/api/v3/series/lookup?term={query}"
        headers = {"X-Api-Key": self.sonarr_key}
        async with self.semaphore, session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"❌ Erreur API Sonarr ({resp.status}) pour {query}")
                return []
            try:
                data = await resp.json()
            except Exception as e:
                logger.error(f"❌ Réponse JSON invalide Sonarr pour {query}: {e}")
                return []
            if isinstance(data, dict):
                return [data]
            if not isinstance(data, list):
                logger.error(f"❌ Réponse inattendue Sonarr pour {query}: {data}")
                return []
            return data

    async def get_sonarr_info(self, session, title, year=None, imdb_id=None):
        """Récupère les infos fiables via Sonarr (IMDb ID, titre, année)"""
        term = imdb_id or title
        results = await self.sonarr_lookup(session, term)

        if not results or not all(isinstance(r, dict) for r in results):
            self.log_not_found(title, year)
            return None

        best = results[0]
        if year:
            for r in results:
                if r.get("year") and str(r["year"]) == str(year):
                    best = r
                    break

        imdb_id = best.get("imdbId")
        title_en = best.get("title")
        year = best.get("year")

        if not imdb_id or not title_en:
            return None

        return {
            "imdb_id": imdb_id,
            "title_en": self.normalize_filename(title_en),
            "year": str(year)
        }

    # ----------------- RENAME EPISODES -----------------

    def rename_episodes_in_dir(self, series_dir: Path, series_name: str, year: str):
        logger.info(f"🔎 Scan des épisodes dans {series_dir}")
        renamed = []

        base_name = f"{series_name} ({year})"

        for root, _, files in os.walk(series_dir):
            for file in files:
                f = Path(root) / file
                if f.suffix.lower() in [".mkv", ".mp4", ".avi"]:
                    match = re.search(r"[Ss](\d{1,2})[Ee](\d{1,2})", f.name)
                    if match:
                        season = int(match.group(1))
                        episode = int(match.group(2))
                        episode_code = f"S{season:02d}E{episode:02d}"
                        new_name = f"{base_name} - {episode_code}{f.suffix}"
                        new_path = f.parent / new_name

                        if f == new_path:
                            renamed.append({
                                "old": str(f),
                                "new": str(new_path),
                                "status": "already_conform"
                            })
                            continue

                        try:
                            if self.apply:
                                shutil.move(str(f), str(new_path))
                                logger.success(f"📂 Épisode renommé : {f.name} → {new_name}")
                            else:
                                logger.info(f"(Dry-run Episode) {f.name} → {new_name}")

                            renamed.append({
                                "old": str(f),
                                "new": str(new_path),
                                "status": "renamed",
                                "dry_run": not self.apply
                            })
                        except Exception as e:
                            logger.error(f"❌ Erreur renommage fichier {f}: {e}")
                            renamed.append({
                                "old": str(f),
                                "new": None,
                                "status": "error",
                                "error": str(e)
                            })
                    else:
                        logger.debug(f"⏭ Aucun pattern SxxEyy détecté pour {f.name}")
        return renamed

    # ----------------- SERIES -----------------

    async def process_series_dir(self, session, d: Path):
        raw_name = d.name
        imdb_id = self.extract_imdb_id(raw_name)
        clean = self.clean_name(raw_name)

        match = re.match(r"^(.*?)\s*\((\d{4})\)", clean)
        if match:
            title, year = match.groups()
        else:
            title, year = clean, None

        logger.info(f"🔎 Recherche via Sonarr : {title} ({year}) {imdb_id or ''}")
        best = await self.get_sonarr_info(session, title, year, imdb_id)

        results = []
        if best:
            imdb_id, title_en, sn_year = best["imdb_id"], best["title_en"], best["year"]

            folder_name = f"{title_en} ({sn_year}) {{imdb-{imdb_id}}}"
            new_path = d.parent / folder_name

            if d != new_path:
                try:
                    if self.apply:
                        shutil.move(str(d), str(new_path))
                        logger.success(f"✅ Dossier renommé : {d.name} → {folder_name}")
                        d = new_path
                    else:
                        logger.info(f"(Dry-run Folder) {d.name} → {folder_name}")
                except Exception as e:
                    logger.error(f"❌ Erreur renommage dossier {d}: {e}")
                    results.append({
                        "series": d.name,
                        "status": "error",
                        "error": str(e)
                    })
                    return results

            results.extend(self.rename_episodes_in_dir(d, title_en, sn_year))
        else:
            logger.warning(f"⚠️ Série introuvable, renommage local pour {d.name}")
            results.append({
                "series": d.name,
                "status": "not_found"
            })
            results.extend(self.rename_episodes_in_dir(d, clean, year or "0000"))

        return results

    # ----------------- MAIN -----------------

    async def run(self):
        results = []
        total = renamed = already_conform = not_found = errors = 0

        async with aiohttp.ClientSession() as session:
            tasks = [self.process_series_dir(session, d) for d in self.base_dir.iterdir() if d.is_dir()]
            all_series = await asyncio.gather(*tasks)

        for series_results in all_series:
            for r in series_results:
                results.append(r)
                total += 1
                if r["status"] == "renamed":
                    renamed += 1
                elif r["status"] == "already_conform":
                    already_conform += 1
                elif r["status"] == "error":
                    errors += 1
                elif r["status"] == "not_found":
                    not_found += 1

        logger.info(
            f"📊 Scan séries terminé : {total} épisodes détectés | "
            f"🔄 {renamed} renommés | "
            f"⏭ {already_conform} déjà conformes | "
            f"❌ {not_found} introuvables | "
            f"⚠️ {errors} erreurs"
        )

        return {
            "stats": {
                "total": total,
                "renamed": renamed,
                "already_conform": already_conform,
                "not_found": not_found,
                "errors": errors,
            },
            "results": results,
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Renommer séries et épisodes via Sonarr (IMDb ID inclus)")
    parser.add_argument("--base", required=True, help="Chemin du dossier contenant les séries")
    parser.add_argument("--sonarr-url", required=True, help="URL de Sonarr (ex: http://localhost:8989)")
    parser.add_argument("--sonarr-key", required=True, help="API Key de Sonarr")
    parser.add_argument("--apply", action="store_true", help="Appliquer réellement les renommages (sinon dry-run)")
    args = parser.parse_args()

    manager = MediasSeries(
        base_dir=Path(args.base),
        sonarr_url=args.sonarr_url,
        sonarr_key=args.sonarr_key,
        apply=args.apply
    )
    asyncio.run(manager.run())
