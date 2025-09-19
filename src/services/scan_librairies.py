import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from program.utils.imdb import is_missing_imdb   # ✅ fonction factorisée

CONFIG_PATH = Path("data/config.json")


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
    def _check_folder(folder: Path):
        """Retourne 'ok' ou 'imdb_missing' pour un dossier."""
        if not folder.is_dir():
            return "skip"
        return "ok" if not is_missing_imdb(folder.name) else "imdb_missing"

    def _scan_folder(self, base: Path):
        results = {"ok": 0, "imdb_missing": 0}
        tasks = []

        with ThreadPoolExecutor() as executor:
            for item in base.iterdir():
                if not item.is_dir():
                    continue
                tasks.append(executor.submit(self._check_folder, item))

            for future in as_completed(tasks):
                res = future.result()
                if res in results:
                    results[res] += 1

        return results

    def scan_movies(self):
        results = {"ok": 0, "imdb_missing": 0}
        for base in self.movie_dirs:
            res = self._scan_folder(base)
            results["ok"] += res["ok"]
            results["imdb_missing"] += res["imdb_missing"]
        return results

    def scan_shows(self):
        results = {"ok": 0, "imdb_missing": 0}
        for base in self.show_dirs:
            res = self._scan_folder(base)
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
