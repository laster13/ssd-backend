import re

GOOD_IMDB_REGEX = re.compile(r"\{imdb-tt\d+\}", re.IGNORECASE)

def is_missing_imdb(path: str) -> bool:
    """Retourne True si un chemin n'a pas de balise IMDb valide."""
    if not path:
        return True
    try:
        return not GOOD_IMDB_REGEX.search(str(path))
    except Exception:
        return True
