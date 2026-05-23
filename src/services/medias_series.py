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

    def __init__(
        self,
        base_dir: Path,
        tmdb_api_key: str,
        apply: bool = False,
        max_workers: int = 3,
    ):
        self.base_dir = Path(base_dir)
        self.tmdb_api_key = tmdb_api_key
        self.apply = apply

        # Limite les appels simultanes vers TMDb / IMDb.
        self.semaphore = asyncio.Semaphore(max_workers)

        # Pool HTTP volontairement limite pour eviter les rafales.
        self.http_limits = httpx.Limits(
            max_connections=5,
            max_keepalive_connections=2,
        )

        # Cache memoire pendant un run.
        self.tmdb_cache = {}

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
        match = re.search(r"(tt\d+)", text)
        return match.group(1) if match else None

    # ------------------------------------------------------------
    # SAFE TMDB GET
    # ------------------------------------------------------------

    async def _tmdb_get(
        self,
        client: httpx.AsyncClient,
        url: str,
        params: dict,
        timeout: int = 10,
    ):
        """
        Safe TMDb call:
        - limits concurrency
        - adds a small pause between calls
        - handles HTTP 429 with Retry-After
        - returns None instead of crashing the run
        """
        async with self.semaphore:
            await asyncio.sleep(0.35)

            try:
                response = await client.get(
                    url,
                    params=params,
                    timeout=timeout,
                )
            except Exception as e:
                logger.error(f"TMDb network fetch error: {e}")
                return None

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else 10
                )

                logger.warning(
                    f"TMDb rate limit reached, waiting {wait_seconds}s before retry"
                )

                await asyncio.sleep(wait_seconds)

                try:
                    response = await client.get(
                        url,
                        params=params,
                        timeout=timeout,
                    )
                except Exception as e:
                    logger.error(f"TMDb network retry error: {e}")
                    return None

            if response.status_code != 200:
                logger.error(
                    f"TMDb fetch error: HTTP {response.status_code} {response.text}"
                )
                return None

            try:
                return response.json()
            except Exception as e:
                logger.error(f"Invalid TMDb JSON response: {e}")
                return None

    # ------------------------------------------------------------
    # TMDB LOOKUP
    # ------------------------------------------------------------

    async def tmdb_lookup(
        self,
        client: httpx.AsyncClient,
        title: str,
        year: str | None,
    ):
        logger.info(f"[TMDB] Search: {title} ({year})")

        cache_key = f"{title.lower().strip()}::{year or ''}"

        if cache_key in self.tmdb_cache:
            logger.info(f"[TMDB_CACHE] Hit: {title} ({year})")
            return self.tmdb_cache[cache_key]

        params = {
            "api_key": self.tmdb_api_key,
            "query": title,
        }

        if year:
            params["first_air_date_year"] = year

        data = await self._tmdb_get(
            client,
            self.TMDB_SEARCH_URL,
            params=params,
            timeout=10,
        )

        if not data:
            self.tmdb_cache[cache_key] = None
            return None

        results = data.get("results", [])

        if not results:
            logger.error(f"[TMDB_NOT_FOUND] No TMDb result for: {title}")
            self.tmdb_cache[cache_key] = None
            return None

        best = results[0]
        tmdb_id = best.get("id")

        if not tmdb_id:
            logger.error(f"[TMDB_NO_ID] TMDb result without ID for: {title}")
            self.tmdb_cache[cache_key] = None
            return None

        ext = await self._tmdb_get(
            client,
            self.TMDB_EXTERNAL_URL.format(id=tmdb_id),
            params={"api_key": self.tmdb_api_key},
            timeout=8,
        )

        if not ext:
            self.tmdb_cache[cache_key] = None
            return None

        imdb = ext.get("imdb_id")

        if not imdb:
            logger.error(f"[TMDB_EXT_NO_IMDB] No IMDb for: {title}")
            self.tmdb_cache[cache_key] = None
            return None

        result = {
            "tmdb_id": tmdb_id,
            "imdb_id": imdb,
            "name": best.get("name") or title,
            "year": (best.get("first_air_date") or "0000")[:4],
        }

        self.tmdb_cache[cache_key] = result
        return result

    # ------------------------------------------------------------
    # IMDB FALLBACK
    # ------------------------------------------------------------

    async def imdb_fallback(
        self,
        client: httpx.AsyncClient,
        title: str,
        year: str | None,
    ):
        logger.info(f"[IMDB_FALLBACK] Search: {title} ({year})")

        key = title.lower().replace(" ", "_")

        if not key:
            return None

        url = self.IMDB_SEARCH_URL.format(key[0], key)

        try:
            async with self.semaphore:
                await asyncio.sleep(0.20)
                response = await client.get(url, timeout=10)

            if response.status_code != 200:
                logger.error(
                    f"[IMDB_ERROR] HTTP {response.status_code} for: {title}"
                )
                return None

            data = response.json()

        except Exception as e:
            logger.error(f"[IMDB_ERROR] {title} -> {e}")
            return None

        items = data.get("d", [])

        if not items:
            return None

        for item in items:
            imdb_id = item.get("id")
            name = item.get("l")
            item_year = item.get("y")

            if not imdb_id or not name:
                continue

            if year and item_year and str(item_year) != str(year):
                continue

            first_word = title.lower().split(" ")[0]

            if first_word in name.lower():
                return {
                    "imdb_id": imdb_id,
                    "name": name,
                    "year": str(item_year or year or "0000"),
                }

        return None

    # ------------------------------------------------------------
    # RENAME
    # ------------------------------------------------------------

    def rename_folder(
        self,
        d: Path,
        title: str,
        year: str,
        imdb_id: str,
    ):
        safe = self.normalize_filename(title)
        folder_name = f"{safe} ({year}) {{imdb-{imdb_id}}}"
        new_path = d.parent / folder_name

        if new_path == d:
            logger.info(f"Folder already compliant: {d.name}")
            return

        if self.apply:
            shutil.move(str(d), str(new_path))
            logger.success(f"Folder renamed: {d.name} -> {folder_name}")
        else:
            logger.info(f"(DRY-RUN) {d.name} -> {folder_name}")

    # ------------------------------------------------------------
    # PROCESS DIR
    # ------------------------------------------------------------

    async def process_dir(
        self,
        client: httpx.AsyncClient,
        d: Path,
    ):
        """
        Traitement complet pour run()/renommage.
        Cette fonction peut contacter TMDb/IMDb.
        """
        raw = d.name
        clean = self.clean_name(raw)

        year = None
        match = re.search(r"\((\d{4})\)", clean)

        if match and match.group(1) != "0000":
            year = match.group(1)

        clean = re.sub(r"\(\d{4}\)", "", clean).strip()

        if not clean:
            logger.error(f"[FINAL_FAIL] Empty cleaned title for: {raw}")
            return {
                "status": "not_found",
                "folder": raw,
            }

        imdb_id = self.extract_imdb_id(raw)

        if imdb_id:
            logger.info(f"[IMDB_PRESENT] {raw}")
            self.rename_folder(d, clean, year or "0000", imdb_id)

            return {
                "status": "imdb_present",
                "folder": raw,
                "imdb": imdb_id,
            }

        tmdb = await self.tmdb_lookup(client, clean, year)

        if tmdb:
            self.rename_folder(d, tmdb["name"], tmdb["year"], tmdb["imdb_id"])

            return {
                "status": "tmdb_ok",
                "folder": raw,
                "imdb": tmdb["imdb_id"],
            }

        imdb = await self.imdb_fallback(client, clean, year)

        if imdb:
            self.rename_folder(d, imdb["name"], imdb["year"], imdb["imdb_id"])

            return {
                "status": "imdb_fallback_ok",
                "folder": raw,
                "imdb": imdb["imdb_id"],
            }

        logger.error(f"[FINAL_FAIL] Nothing found for: {raw}")

        return {
            "status": "not_found",
            "folder": raw,
        }

    # ------------------------------------------------------------
    # RUN
    # ------------------------------------------------------------

    async def run(self):
        """
        Renommage reel ou dry-run.
        Peut contacter TMDb/IMDb, donc garde le throttle anti-429.
        """
        async with httpx.AsyncClient(
            limits=self.http_limits,
            timeout=15,
        ) as client:
            tasks = [
                self.process_dir(client, d)
                for d in self.base_dir.iterdir()
                if d.is_dir()
            ]

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        clean_results = []

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"[RUN_ERROR] {result}")
                clean_results.append(
                    {
                        "status": "not_found",
                        "folder": "unknown",
                        "error": str(result),
                    }
                )
            else:
                clean_results.append(result)

        stats = {
            "total": len(clean_results),
            "tmdb_ok": sum(r["status"] == "tmdb_ok" for r in clean_results),
            "imdb_present": sum(
                r["status"] == "imdb_present"
                for r in clean_results
            ),
            "imdb_fallback_ok": sum(
                r["status"] == "imdb_fallback_ok"
                for r in clean_results
            ),
            "not_found": sum(r["status"] == "not_found" for r in clean_results),
        }

        logger.info(f"Summary: {stats}")

        return {
            "stats": stats,
            "results": clean_results,
        }

    # ------------------------------------------------------------
    # SCAN
    # ------------------------------------------------------------

    async def scan(self):
        """
        Scan rapide sans appel externe.
        Ne contacte pas TMDb/IMDb.
        """
        stats = {
            "total": 0,
            "deja_conforme": 0,
            "non_conforme_plex": 0,
            "imdb_manquant_ou_invalide": 0,
            "inconnu_dans_sonarr": 0,
            "erreurs": 0,
        }

        details = {
            key: []
            for key in stats
            if key != "total"
        }

        tasks = []

        for d in self.base_dir.iterdir():
            if d.is_dir():
                stats["total"] += 1
                tasks.append(self._scan_one(d))

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"[SCAN_ERROR] {result}")
                stats["erreurs"] += 1
                details["erreurs"].append(
                    {
                        "status": "erreurs",
                        "error": str(result),
                    }
                )
                continue

            status = result.get("status")

            if status not in stats:
                logger.error(f"[SCAN_UNKNOWN_STATUS] {result}")
                stats["erreurs"] += 1
                details["erreurs"].append(result)
                continue

            stats[status] += 1
            details[status].append(result)

        logger.info(
            f"Scan finished: total={stats['total']} | "
            f"already_compliant={stats['deja_conforme']} | "
            f"non_compliant_plex={stats['non_conforme_plex']} | "
            f"missing_imdb={stats['imdb_manquant_ou_invalide']} | "
            f"unknown={stats['inconnu_dans_sonarr']} | "
            f"errors={stats['erreurs']}"
        )

        return {
            "statistiques": stats,
            "details": details,
        }

    async def _scan_one(
        self,
        d: Path,
    ):
        """
        Scan rapide sans appel externe.

        Important:
        - ne contacte pas TMDb
        - ne contacte pas IMDb
        - classe seulement le dossier selon son format actuel
        - garde le scan rapide
        """
        raw = d.name
        imdb_id = self.extract_imdb_id(raw)

        # Format Plex strict:
        # Titre (2024) {imdb-tt1234567}
        plex_pattern = r"^(.+?) \((\d{4})\) \{imdb-tt\d+\}$"

        if re.match(plex_pattern, raw):
            return {
                "status": "deja_conforme",
                "folder": raw,
                "imdb": imdb_id,
            }

        # IMDb present mais format Plex non strict.
        if imdb_id:
            return {
                "status": "non_conforme_plex",
                "folder": raw,
                "imdb": imdb_id,
            }

        # Pas d'IMDb: on ne resout pas via TMDb pendant le scan.
        # La resolution TMDb doit se faire uniquement pendant run()/correction.
        clean = self.clean_name(raw)
        year_match = re.search(r"\((\d{4})\)", clean)
        year = year_match.group(1) if year_match else None
        title = re.sub(r"\(\d{4}\)", "", clean).strip()

        if not title:
            return {
                "status": "inconnu_dans_sonarr",
                "folder": raw,
                "title": None,
                "year": year,
            }

        return {
            "status": "imdb_manquant_ou_invalide",
            "folder": raw,
            "title": title,
            "year": year,
            "imdb": None,
        }