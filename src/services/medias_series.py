import re
import httpx
import asyncio
import shutil
from pathlib import Path
from loguru import logger


class MediasSeries:

    TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/tv"
    TMDB_EXTERNAL_URL = "https://api.themoviedb.org/3/tv/{id}/external_ids"
    IMDB_SEARCH_URL = "https://v2.sg.media-imdb.com/suggestion/{}/{}.json"

    def __init__(self, base_dir: Path, tmdb_api_key: str, apply: bool = False, max_workers: int = 25):
        self.base_dir = Path(base_dir)
        self.tmdb_api_key = tmdb_api_key
        self.apply = apply

        self.semaphore = asyncio.Semaphore(max_workers)

        self.http_limits = httpx.Limits(
            max_connections=50,
            max_keepalive_connections=25
        )

    # ------------------------------------------------------------
    # CLEAN HELPERS
    # ------------------------------------------------------------

    @staticmethod
    def clean_name(raw: str) -> str:
        name = re.sub(r"\{imdb-.*?\}", "", raw)
        name = re.sub(r"\s{2,}", " ", name)
        return name.strip()

    @staticmethod
    def normalize_filename(name: str) -> str:
        return re.sub(r'[\/:*?"<>|]', "-", name).strip()

    @staticmethod
    def extract_imdb_id(text: str):
        m = re.search(r"(tt\d+)", text)
        return m.group(1) if m else None

    # ------------------------------------------------------------
    # TMDB LOOKUP (Titre + Année → Vrai IMDb)
    # ------------------------------------------------------------

    async def tmdb_lookup(self, client: httpx.AsyncClient, title: str, year: str | None):
        logger.info(f"[TMDB] Recherche : {title} ({year})")

        params = {"api_key": self.tmdb_api_key, "query": title}
        if year:
            params["first_air_date_year"] = year

        try:
            async with self.semaphore:
                r = await client.get(self.TMDB_SEARCH_URL, params=params, timeout=10)
                data = r.json()
        except Exception as e:
            logger.error(f"[TMDB_ERROR] {title} → {e}")
            return None

        results = data.get("results", [])
        if not results:
            logger.error(f"[TMDB_NOT_FOUND] Aucun TMDb pour : {title}")
            return None

        best = results[0]
        tmdb_id = best["id"]

        # ---- Récupérer IMDB réel ----

        try:
            async with self.semaphore:
                r = await client.get(
                    self.TMDB_EXTERNAL_URL.format(id=tmdb_id),
                    params={"api_key": self.tmdb_api_key},
                    timeout=8
                )
                ext = r.json()
        except Exception as e:
            logger.error(f"[TMDB_EXT_ERROR] {title} → {e}")
            return None

        imdb = ext.get("imdb_id")

        if not imdb:
            logger.error(f"[TMDB_EXT_NO_IMDB] Pas d’IMDb pour : {title}")
            return None

        return {
            "tmdb_id": tmdb_id,
            "imdb_id": imdb,
            "name": best["name"],
            "year": (best.get("first_air_date") or "0000")[:4]
        }

    # ------------------------------------------------------------
    # IMDb FALLBACK
    # ------------------------------------------------------------

    async def imdb_fallback(self, client: httpx.AsyncClient, title: str, year: str | None):
        logger.info(f"[IMDB_FALLBACK] Recherche : {title} ({year})")

        key = title.lower().replace(" ", "_")
        url = self.IMDB_SEARCH_URL.format(key[0], key)

        try:
            async with self.semaphore:
                r = await client.get(url, timeout=10)
                data = r.json()
        except Exception as e:
            logger.error(f"[IMDB_ERROR] → {e}")
            return None

        items = data.get("d", [])
        if not items:
            return None

        for it in items:
            imdbid = it.get("id")
            name = it.get("l")
            y = it.get("y")

            if year and y and str(y) != str(year):
                continue

            if title.lower().split(" ")[0] in name.lower():
                return {
                    "imdb_id": imdbid,
                    "name": name,
                    "year": str(y or year or "0000")
                }

        return None

    # ------------------------------------------------------------
    # RENAME
    # ------------------------------------------------------------

    def rename_folder(self, d: Path, title: str, year: str, imdb_id: str):

        safe = self.normalize_filename(title)
        folder_name = f"{safe} ({year}) {{imdb-{imdb_id}}}"
        new_path = d.parent / folder_name

        if new_path == d:
            logger.info(f"📁 Déjà conforme : {d.name}")
            return

        if self.apply:
            shutil.move(str(d), str(new_path))
            logger.success(f"📁 Dossier renommé : {d.name} → {folder_name}")
        else:
            logger.info(f"(DRY-RUN) {d.name} → {folder_name}")

    # ------------------------------------------------------------
    # PROCESS DIR
    # ------------------------------------------------------------

    async def process_dir(self, client: httpx.AsyncClient, d: Path):

        raw = d.name
        clean = self.clean_name(raw)

        # Extraire année
        year = None
        m = re.search(r"\((\d{4})\)", clean)
        if m and m.group(1) != "0000":
            year = m.group(1)

        clean = re.sub(r"\(\d{4}\)", "", clean).strip()

        # IMDb direct
        imdb_id = self.extract_imdb_id(raw)
        if imdb_id:
            logger.info(f"[IMDB_PRESENT] {raw}")
            self.rename_folder(d, clean, year or "0000", imdb_id)
            return {"status": "imdb_present", "folder": raw}

        # TMDB lookup
        tmdb = await self.tmdb_lookup(client, clean, year)
        if tmdb:
            self.rename_folder(d, tmdb["name"], tmdb["year"], tmdb["imdb_id"])
            return {"status": "tmdb_ok", "folder": raw, "imdb": tmdb["imdb_id"]}

        # IMDb fallback
        imdb = await self.imdb_fallback(client, clean, year)
        if imdb:
            self.rename_folder(d, imdb["name"], imdb["year"], imdb["imdb_id"])
            return {"status": "imdb_fallback_ok", "folder": raw, "imdb": imdb["imdb_id"]}

        logger.error(f"[FINAL_FAIL] Rien trouvé pour : {raw}")
        return {"status": "not_found", "folder": raw}

    # ------------------------------------------------------------
    # RUN (RENOMMAGE RÉEL)
    # ------------------------------------------------------------

    async def run(self):
        async with httpx.AsyncClient(limits=self.http_limits, timeout=15) as client:
            tasks = [
                self.process_dir(client, d)
                for d in self.base_dir.iterdir()
                if d.is_dir()
            ]

            results = await asyncio.gather(*tasks)

        stats = {
            "total": len(results),
            "tmdb_ok": sum(r["status"] == "tmdb_ok" for r in results),
            "imdb_present": sum(r["status"] == "imdb_present" for r in results),
            "imdb_fallback_ok": sum(r["status"] == "imdb_fallback_ok" for r in results),
            "not_found": sum(r["status"] == "not_found" for r in results),
        }

        logger.info(f"📊 Résumé : {stats}")
        return {"stats": stats, "results": results}

    # ------------------------------------------------------------
    # SCAN (PAS DE RENOMMAGE, CLASSIFICATION RADARR-LIKE)
    # ------------------------------------------------------------

    async def scan(self):
        stats = {
            "total": 0,
            "deja_conforme": 0,
            "non_conforme_plex": 0,
            "imdb_manquant_ou_invalide": 0,
            "inconnu_dans_sonarr": 0,
            "erreurs": 0,
        }

        details = {k: [] for k in stats if k != "total"}

        async with httpx.AsyncClient(timeout=10) as client:

            tasks = []
            for d in self.base_dir.iterdir():
                if d.is_dir():
                    stats["total"] += 1
                    tasks.append(self._scan_one(client, d))

            results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                stats["erreurs"] += 1
                continue

            status = res["status"]
            stats[status] += 1
            details[status].append(res)

        logger.info(
            f"📊 Fin du scan : total={stats['total']} | "
            f"Déjà conformes={stats['deja_conforme']} | "
            f"Non conformes Plex={stats['non_conforme_plex']} | "
            f"IMDb manquant={stats['imdb_manquant_ou_invalide']} | "
            f"Inconnus Sonarr={stats['inconnu_dans_sonarr']} | "
            f"Erreurs={stats['erreurs']}"
        )

        return {"statistiques": stats, "details": details}

    async def _scan_one(self, client, d: Path):
        raw = d.name

        imdb_id = self.extract_imdb_id(raw)

        # Format Plex strict
        plex_pattern = r"^(.+?) \((\d{4})\) \{imdb-tt\d+\}$"
        if re.match(plex_pattern, raw):
            return {
                "status": "deja_conforme",
                "folder": raw,
                "imdb": imdb_id
            }

        if imdb_id:
            return {
                "status": "non_conforme_plex",
                "folder": raw,
                "imdb": imdb_id
            }

        # IMDb manquant → TMDb lookup
        clean = self.clean_name(raw)
        year_match = re.search(r"\((\d{4})\)", clean)
        year = year_match.group(1) if year_match else None
        title = re.sub(r"\(\d{4}\)", "", clean).strip()

        tmdb = await self.tmdb_lookup(client, title, year)
        if tmdb:
            return {
                "status": "imdb_manquant_ou_invalide",
                "folder": raw,
                "imdb": tmdb["imdb_id"]
            }

        # fallback IMDb
        imdb_fb = await self.imdb_fallback(client, title, year)
        if imdb_fb:
            return {
                "status": "imdb_manquant_ou_invalide",
                "folder": raw,
                "imdb": imdb_fb["imdb_id"]
            }

        return {"status": "inconnu_dans_sonarr", "folder": raw}
