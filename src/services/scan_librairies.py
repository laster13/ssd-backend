import json
import re
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

CONFIG_PATH = Path("data/config.json")
IMDB_PATTERN = re.compile(r"\{imdb-tt\d+\}", re.IGNORECASE)


class LibraryScanner:
    def __init__(self, config_path: Path = CONFIG_PATH):
        self.config = self._load_config(config_path)

        # ✅ Gestion multi-racines
        self.movie_dirs = [
            Path(ld["path"])
            for ld in self.config["links_dirs"]
            if ld.get("manager") == "radarr"
        ]
        self.show_dirs = [
            Path(ld["path"])
            for ld in self.config["links_dirs"]
            if ld.get("manager") == "sonarr"
        ]

        if not self.movie_dirs or not self.show_dirs:
            raise RuntimeError("⚠️ Impossible de trouver movies/shows dans config.json")

    @staticmethod
    def _load_config(config_path: Path):
        with open(config_path, "r") as f:
            return json.load(f)

    @staticmethod
    def _check_movie_folder(folder: Path):
        if not folder.is_dir():
            return "skip"
        return "ok" if IMDB_PATTERN.search(folder.name) else "imdb_missing"

    @staticmethod
    def _check_show_folder(folder: Path):
        if not folder.is_dir():
            return "skip"
        return "ok" if IMDB_PATTERN.search(folder.name) else "imdb_missing"

    def _scan_folder(self, base: Path, mode: str):
        results = {"ok": 0, "imdb_missing": 0}
        tasks = []

        with ThreadPoolExecutor() as executor:
            for item in base.iterdir():
                if not item.is_dir():
                    continue
                if mode == "movies":
                    tasks.append(executor.submit(self._check_movie_folder, item))
                elif mode == "shows":
                    tasks.append(executor.submit(self._check_show_folder, item))

            for future in as_completed(tasks):
                res = future.result()
                if res in results:
                    results[res] += 1

        return results

    def scan_movies(self):
        results = {"ok": 0, "imdb_missing": 0}
        for base in self.movie_dirs:
            res = self._scan_folder(base, "movies")
            results["ok"] += res["ok"]
            results["imdb_missing"] += res["imdb_missing"]
        return results

    def scan_shows(self):
        results = {"ok": 0, "imdb_missing": 0}
        for base in self.show_dirs:
            res = self._scan_folder(base, "shows")
            results["ok"] += res["ok"]
            results["imdb_missing"] += res["imdb_missing"]
        return results

    def scan(self) -> dict:
        start = time.time()

        shows_result = self.scan_shows()
        movies_result = self.scan_movies()

        elapsed = time.time() - start

        return {
            "shows": shows_result,
            "movies": movies_result,
            "elapsed": round(elapsed, 2),
        }
