import unicodedata
import re

def normalize_name(name: str) -> str:
    name = name.lower()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", name)

def clean_movie_name(raw_name: str) -> str:
    """Nettoie un nom de film brut (IMDB tag, espaces, etc.)."""
    cleaned = re.sub(r'\s*\{imdb-tt\d{7,8}[^\}]*\}?', '', raw_name)
    cleaned = re.sub(r'\(\s*\)', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()

def clean_series_name(raw_name: str) -> str:
    """
    Nettoie le nom de la série :
    - Supprime toute balise IMDb {imdb-...}, même incomplète ou mal formée
    - Conserve l'année entre parenthèses si elle existe
    - Supprime les espaces superflus et multiples
    """
    # Supprime toute balise {imdb-...}
    cleaned = re.sub(r'\s*\{imdb-[^}]*\}?', '', raw_name, flags=re.IGNORECASE)

    # Supprime les parenthèses vides ou résiduelles
    cleaned = re.sub(r'\(\s*\)', '', cleaned)

    # Réduit les espaces multiples
    cleaned = re.sub(r'\s+', ' ', cleaned)

    return cleaned.strip()
