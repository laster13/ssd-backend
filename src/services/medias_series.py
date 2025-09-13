import os
import re
import json
import asyncio
import aiohttp
import shutil
from pathlib import Path
from datetime import datetime
from loguru import logger


class MediasSeries:
    def __init__(self, base_dir: Path, api_key: str,
                 not_found_file="not_found_series.txt",
                 max_concurrent=10):
        self.base_dir = Path(base_dir)
        self.api_key = api_key
        self.not_found_file = not_found_file
        self.semaphore = asyncio.Semaphore(max_concurrent)

    # ----------------- UTILITAIRES -----------------

    @staticmethod
    def normalize_filename(name: str) -> str:
        return re.sub(r'[\/:*?"<>|]', "-", name).strip()

    def log_not_found(self, title, year=None):
        with open(self.not_found_file, "a", encoding="utf-8") as f:
            f.write(f"{title} ({year}) [TV]\n")

    @staticmethod
    def clean_name(raw: str) -> str:
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

    # ----------------- TMDb API -----------------

    async def fetch_json(self, session, url):
        async with self.semaphore:
            async with session.get(url) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2))
                    logger.warning(f"⚠️ Rate limit atteint, pause {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    return await self.fetch_json(session, url)
                return await resp.json()

    async def get_tmdb_info_by_imdb(self, session, imdb_id):
        url = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={self.api_key}&external_source=imdb_id&language=en-US"
        data = await self.fetch_json(session, url)
        results = data.get("tv_results", [])
        if not results:
            logger.error(f"❌ IMDb ID introuvable sur TMDb : {imdb_id}")
            return None
        details_url = f"https://api.themoviedb.org/3/tv/{results[0]['id']}?api_key={self.api_key}&append_to_response=external_ids&language=en-US"
        details = await self.fetch_json(session, details_url)
        imdb_id = details.get("external_ids", {}).get("imdb_id")
        title_en = details.get("name") or details.get("original_name")
        tmdb_year = (details.get("first_air_date") or "").split("-")[0]
        if not imdb_id or not title_en:
            return None
        title_en = self.normalize_filename(title_en)
        return {"imdb_id": imdb_id, "title_en": title_en, "year": tmdb_year}

    async def get_tmdb_info_by_search(self, session, title, year=None):
        search_url = f"https://api.themoviedb.org/3/search/tv?api_key={self.api_key}&query={title}&language=en-US"
        if year and year != "0":
            search_url += f"&first_air_date_year={year}"
        search_data = await self.fetch_json(session, search_url)
        results = search_data.get("results", [])
        if not results:
            logger.error(f"❌ Pas trouvé : {title} ({year})")
            self.log_not_found(title, year)
            return None
        best = results[0]
        if year and year != "0":
            for r in results:
                first_air_date = r.get("first_air_date") or ""
                result_year = first_air_date.split("-")[0] if first_air_date else None
                if result_year == year:
                    best = r
                    break
        details_url = f"https://api.themoviedb.org/3/tv/{best['id']}?api_key={self.api_key}&append_to_response=external_ids&language=en-US"
        details = await self.fetch_json(session, details_url)
        imdb_id = details.get("external_ids", {}).get("imdb_id")
        title_en = details.get("name") or details.get("original_name") or title
        tmdb_year = (details.get("first_air_date") or "").split("-")[0]
        if not imdb_id or not title_en:
            return None
        title_en = self.normalize_filename(title_en)
        return {"imdb_id": imdb_id, "title_en": title_en, "year": tmdb_year}

    # ----------------- RENAME EPISODES -----------------

    def rename_episodes_in_dir(self, series_dir: Path, series_name: str, year: str):
        logger.info(f"🔎 Scan des épisodes dans {series_dir}")
        renamed = []

        for root, _, files in os.walk(series_dir):
            for file in files:
                f = Path(root) / file
                if f.suffix.lower() in [".mkv", ".mp4", ".avi"]:
                    match = re.search(r"[Ss](\d{1,2})[Ee](\d{1,2})", f.name)
                    if match:
                        season = int(match.group(1))
                        episode = int(match.group(2))
                        episode_code = f"S{season:02d}E{episode:02d}"
                        new_name = f"{series_name} ({year}) - {episode_code}{f.suffix}"
                        new_path = f.parent / new_name

                        if f != new_path:
                            try:
                                if new_path.exists():
                                    logger.warning(f"⚠️ Fichier existe déjà, ignoré : {new_path}")
                                    continue
                                shutil.move(str(f), str(new_path))
                                logger.success(f"📂 Épisode renommé : {f.name} → {new_name}")
                                renamed.append({"old": str(f), "new": str(new_path)})
                            except Exception as e:
                                logger.error(f"❌ Erreur renommage fichier {f}: {e}")
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

        best = None
        if imdb_id:
            logger.info(f"🔎 Recherche TMDb par IMDb : {imdb_id}")
            best = await self.get_tmdb_info_by_imdb(session, imdb_id)
        else:
            logger.info(f"🔎 Recherche TMDb par titre : {title} ({year})")
            best = await self.get_tmdb_info_by_search(session, title, year)

        renamed_eps = []
        if best:
            imdb_id, title_en, tmdb_year = best["imdb_id"], best["title_en"], best["year"]
            new_name = f"{title_en} ({tmdb_year}) {{imdb-{imdb_id}}}"
            new_path = d.parent / new_name

            if d != new_path:
                try:
                    shutil.move(str(d), str(new_path))
                    logger.success(f"✅ Dossier renommé : {d.name} → {new_name}")
                    d = new_path
                except Exception as e:
                    logger.error(f"❌ Erreur renommage dossier {d}: {e}")

            renamed_eps = self.rename_episodes_in_dir(d, title_en, tmdb_year)
        else:
            logger.warning(f"⚠️ TMDb introuvable, renommage local des épisodes pour {d.name}")
            renamed_eps = self.rename_episodes_in_dir(d, clean, year or "0000")

        return {"series": d.name, "episodes": renamed_eps}

    # ----------------- MAIN -----------------

    async def run(self):
        results = []
        async with aiohttp.ClientSession() as session:
            for d in self.base_dir.iterdir():
                if d.is_dir():
                    res = await self.process_series_dir(session, d)
                    results.append(res)
        return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Renommer séries et épisodes avec IMDb ID via TMDb")
    parser.add_argument("--base", required=True, help="Chemin du dossier contenant les séries")
    parser.add_argument("--apply", action="store_true", help="Appliquer réellement les renommages")
    args = parser.parse_args()

    manager = MediasSeries(base_dir=args.base, api_key=API_KEY)
    asyncio.run(manager.run())
