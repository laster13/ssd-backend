import os
import time
import threading
import subprocess
import json
import asyncio
import aiohttp
import uuid
from threading import Event
from sqlalchemy import and_, or_
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.services.fonctions_arrs import RadarrService
from program.utils.text_utils import normalize_name, clean_movie_name
from program.settings.manager import config_manager
from program.managers.sse_manager import sse_manager
from .json_manager import update_json_files
from integrations.seasonarr.db.database import SessionLocal
from integrations.seasonarr.db.models import SystemActivity
from program.utils.discord_notifier import send_discord_summary, send_discord_message
from concurrent.futures import ThreadPoolExecutor, as_completed
import program.radarr_cache as radarr_cache
from program.radarr_cache import enrich_from_radarr_index

initial_scan_done = Event()
broken_monitor_cycle_done = Event()
broken_monitor_running = Event()

symlink_watcher_started = False
symlink_watcher_lock = threading.Lock()

USER = os.getenv("USER") or os.getlogin()
YAML_PATH = f"/home/{USER}/.ansible/inventories/group_vars/all.yml"
VAULT_PASSWORD_FILE = f"/home/{USER}/.vault_pass"

# --- Buffer Discord ---
symlink_events_buffer = []
last_sent_time = datetime.utcnow()
SUMMARY_INTERVAL = 60  # en secondes
MAX_EVENTS_BEFORE_FLUSH = 20
buffer_lock = threading.Lock()

# --- 1. YAML watcher ---
class YAMLFileEventHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if os.path.abspath(event.src_path) == os.path.abspath(YAML_PATH):
            try:
                command = f"ansible-vault view {YAML_PATH} --vault-password-file {VAULT_PASSWORD_FILE}"
                result = subprocess.run(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    shell=True
                )

                if result.returncode != 0:
                    if "input is not vault encrypted data" in result.stderr:
                        return
                    logger.error(f"🔐 Erreur ansible-vault : {result.stderr}")
                    return

                decrypted_yaml_content = result.stdout
                update_json_files(decrypted_yaml_content)

            except Exception as e:
                logger.exception(f"💥 Exception YAML: {e}")


def start_yaml_watcher():
    logger.info("🛰️ YAML watcher démarré")
    observer = Observer()
    observer.schedule(YAMLFileEventHandler(), path=os.path.dirname(YAML_PATH), recursive=False)
    logger.info(f"📍 Surveillance active sur : {YAML_PATH}")
    observer.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# --- 2. Symlink watcher ---
class SymlinkEventHandler(FileSystemEventHandler):
    def __init__(self):
        self._lock = threading.Lock()
        self._recent_deleted_events = {}
        self._recent_created_events = {}
        self._event_dedupe_seconds = 5

    def on_any_event(self, event):
        if event.is_directory:
            return

        path = Path(event.src_path)
        logger.debug(f"📂 Événement détecté : {event.event_type} -> {path}")

        # 🔴 Suppression d’un symlink.
        # Important : sur un event deleted, path.is_symlink() peut être False
        # car le fichier n'existe déjà plus.
        if event.event_type == "deleted":
            self._handle_deleted(path)
            return

        # Les traitements ci-dessous concernent uniquement les symlinks existants.
        if not path.is_symlink():
            return

        # 🔍 Symlink brisé.
        # Important : si le lien est brisé, on return après _handle_broken()
        # pour éviter de traiter ensuite le même event comme "created".
        try:
            target = path.resolve(strict=True)

            if not target.exists():
                self._handle_broken(path)
                return

        except FileNotFoundError:
            self._handle_broken(path)
            return

        except Exception as e:
            logger.debug(f"⚠️ Impossible de résoudre le symlink {path}: {e}")
            return

        # 🟢 Création d’un symlink valide.
        if event.event_type == "created":
            self._handle_created(path)
            return

    def _handle_created(self, symlink_path: Path):
        """
        Gère la création d'un nouveau symlink avec détection robuste :
        - Match par ID média fiable : tmdbId / imdb_id / imdbId
        - Sinon match par dossier parent exact
        - Sinon match par nom normalisé exact
        - Sinon fuzzy strict uniquement si très proche
        - Si ancien chemin == nouveau chemin : action="repaired"
        - Si ancien chemin != nouveau chemin : action="replaced"
        - Évite les doublons dans symlink_store
        - Stocke un item léger, cohérent avec le scan
        """
        db = None

        try:
            import re
            from difflib import SequenceMatcher
            from routers.secure.symlinks import symlink_store

            config = config_manager.config
            links_dirs = [
                (Path(ld.path).resolve(), ld.manager)
                for ld in config.links_dirs
            ]
            mount_dirs = [
                Path(d).resolve()
                for d in config.mount_dirs
            ]

            root, manager = None, "unknown"
            symlink_path_str = str(symlink_path)

            now_ts = time.monotonic()

            with self._lock:
                last_ts = self._recent_created_events.get(symlink_path_str)

                if last_ts is not None and now_ts - last_ts < self._event_dedupe_seconds:
                    logger.info(
                        f"⏭️ Création watcher déjà reçue récemment, ignorée : {symlink_path}"
                    )
                    return

                self._recent_created_events[symlink_path_str] = now_ts

                cutoff = now_ts - self._event_dedupe_seconds
                self._recent_created_events = {
                    path: ts
                    for path, ts in self._recent_created_events.items()
                    if ts >= cutoff
                }

            for ld, mgr in links_dirs:
                if symlink_path_str.startswith(str(ld)):
                    root, manager = ld, mgr
                    break

            if not root:
                return

            try:
                target_path = symlink_path.resolve(strict=True)
            except FileNotFoundError:
                target_path = symlink_path.resolve(strict=False)

            full_target = str(target_path)

            for mount_dir in mount_dirs:
                try:
                    relative_target = target_path.relative_to(mount_dir)
                    full_target = str(mount_dir / relative_target)
                    break
                except ValueError:
                    continue

            item = {
                "symlink": symlink_path_str,
                "target": full_target,
                "target_exists": True,
                "manager": manager,
                "ref_count": 1,
            }

            if manager == "radarr":
                extra = enrich_from_radarr_index(
                    symlink_path,
                    allow_fallback=True,
                )

                if extra:
                    allowed_extra = {}

                    for key in ("title", "tmdbId", "imdbId", "imdb_id", "year"):
                        if key in extra:
                            allowed_extra[key] = extra[key]

                    if "imdbId" in allowed_extra and "imdb_id" not in allowed_extra:
                        allowed_extra["imdb_id"] = allowed_extra["imdbId"]

                    if "imdb_id" in allowed_extra and "imdbId" not in allowed_extra:
                        allowed_extra["imdbId"] = allowed_extra["imdb_id"]

                    item.update(allowed_extra)

            with self._lock:
                existing_index = None

                for idx in range(len(symlink_store) - 1, -1, -1):
                    if symlink_store[idx].get("symlink") == symlink_path_str:
                        existing_index = idx
                        break

                if existing_index is not None:
                    symlink_store[existing_index].update(item)
                else:
                    symlink_store.append(item)

            db = SessionLocal()
            now = datetime.utcnow()
            replaced_from = None

            def normalize(value: str) -> str:
                if not value:
                    return ""

                value = str(value).lower().strip()

                replacements = {
                    "é": "e",
                    "è": "e",
                    "ê": "e",
                    "ë": "e",
                    "à": "a",
                    "â": "a",
                    "ä": "a",
                    "î": "i",
                    "ï": "i",
                    "ô": "o",
                    "ö": "o",
                    "ù": "u",
                    "û": "u",
                    "ü": "u",
                    "ç": "c",
                }

                for src, dst in replacements.items():
                    value = value.replace(src, dst)

                value = re.sub(r"[^\w]+", "", value)
                return value.strip()

            def normalize_fs_path(value: str) -> str:
                return str(value or "").strip().rstrip("/")

            def get_extra_dict(activity) -> dict:
                extra = getattr(activity, "extra", None)
                return extra if isinstance(extra, dict) else {}

            def get_tmdb(extra: dict):
                return extra.get("tmdbId") or extra.get("tmdb_id")

            def get_imdb(extra: dict):
                return extra.get("imdb_id") or extra.get("imdbId")

            def parent_dir(path_value: str) -> str:
                try:
                    return str(Path(path_value).parent)
                except Exception:
                    return ""

            def parent_name(path_value: str) -> str:
                try:
                    return Path(path_value).parent.name
                except Exception:
                    return ""

            def is_episode_path(path_value: str) -> bool:
                try:
                    return bool(re.search(r"S\d{1,2}E\d{1,2}", str(path_value), re.IGNORECASE))
                except Exception:
                    return False

            def is_generic_name(value: str) -> bool:
                raw = (value or "").strip().lower()
                norm = normalize(raw)

                if not norm:
                    return True

                generic_patterns = [
                    r"^saison\d+$",
                    r"^season\d+$",
                    r"^s\d{1,2}$",
                    r"^sample$",
                    r"^samples$",
                    r"^subtitle$",
                    r"^subtitles$",
                    r"^subs$",
                    r"^bonus$",
                    r"^extras$",
                    r"^extra$",
                ]

                for pattern in generic_patterns:
                    if re.match(pattern, norm):
                        return True

                return len(norm) < 8

            def strong_fuzzy_match(a: str, b: str) -> bool:
                if is_generic_name(a) or is_generic_name(b):
                    return False

                na = normalize(a)
                nb = normalize(b)

                if not na or not nb:
                    return False

                shortest = min(len(na), len(nb))

                if shortest < 12:
                    return False

                ratio = SequenceMatcher(None, na, nb).ratio()

                if ratio >= 0.92:
                    return True

                prefix_len = 0

                for ca, cb in zip(na, nb):
                    if ca == cb:
                        prefix_len += 1
                    else:
                        break

                return prefix_len >= max(12, int(shortest * 0.75)) and ratio >= 0.85

            def find_similar_deleted():
                item_tmdb = get_tmdb(item)
                item_imdb = get_imdb(item)
                new_parent_dir = parent_dir(symlink_path_str)
                new_parent_name = symlink_path.parent.name
                new_parent_norm = normalize(new_parent_name)

                candidates = db.query(SystemActivity).filter(
                    SystemActivity.action == "deleted",
                    (
                        (SystemActivity.replaced.is_(None)) |
                        (SystemActivity.replaced == False)
                    ),
                    SystemActivity.created_at >= now - timedelta(hours=48),
                ).order_by(SystemActivity.created_at.desc()).all()

                # 1️⃣ Match par chemin exact.
                for deleted in candidates:
                    if normalize_fs_path(deleted.path) == normalize_fs_path(symlink_path_str):
                        return deleted, "same_path"

                # 2️⃣ Match par tmdbId exact.
                if item_tmdb:
                    for deleted in candidates:
                        extra = get_extra_dict(deleted)
                        if get_tmdb(extra) == item_tmdb:
                            return deleted, "tmdbId"

                # 3️⃣ Match par imdb_id / imdbId exact.
                if item_imdb:
                    for deleted in candidates:
                        extra = get_extra_dict(deleted)
                        if get_imdb(extra) == item_imdb:
                            return deleted, "imdbId"

                # 4️⃣ Match par dossier parent exact.
                # Interdit pour les épisodes Sonarr : le parent est le dossier Season XX,
                # donc ce match peut confondre S02E06 avec S02E02.
                if not is_episode_path(symlink_path_str):
                    for deleted in candidates:
                        if parent_dir(deleted.path) == new_parent_dir:
                            return deleted, "parent_dir"

                # 5️⃣ Match par nom parent normalisé exact.
                if new_parent_norm and not is_generic_name(new_parent_name):
                    for deleted in candidates:
                        old_parent_name = parent_name(deleted.path)
                        if normalize(old_parent_name) == new_parent_norm:
                            return deleted, "parent_name"

                # 6️⃣ Fuzzy strict seulement.
                if new_parent_name and not is_generic_name(new_parent_name):
                    for deleted in candidates:
                        old_parent_name = parent_name(deleted.path)
                        if strong_fuzzy_match(new_parent_name, old_parent_name):
                            return deleted, "fuzzy_strict"

                return None, None

            similar_deleted, match_reason = find_similar_deleted()

            if similar_deleted:
                old_path = str(similar_deleted.path)
                same_path_repair = normalize_fs_path(old_path) == normalize_fs_path(symlink_path_str)

                # Marque toutes les suppressions récentes du même chemin comme traitées.
                # Exemple : suppression manuelle + événement watcher.
                matching_deleted_rows = db.query(SystemActivity).filter(
                    SystemActivity.action == "deleted",
                    SystemActivity.path == old_path,
                    (
                        (SystemActivity.replaced.is_(None)) |
                        (SystemActivity.replaced == False)
                    ),
                    SystemActivity.created_at >= now - timedelta(hours=48),
                ).all()

                if not matching_deleted_rows:
                    matching_deleted_rows = [similar_deleted]

                for deleted_row in matching_deleted_rows:
                    deleted_row.replaced = True
                    deleted_row.replaced_at = now
                    deleted_row.updated_at = now

                replaced_from = old_path

                if same_path_repair:
                    repair_extra = {
                        "old_path": old_path,
                        "new_path": symlink_path_str,
                        "match_reason": match_reason,
                        "same_path": True,
                        "created_extra": item,
                        "deleted_extra": get_extra_dict(similar_deleted),
                    }

                    existing_repair = db.query(SystemActivity).filter(
                        SystemActivity.action == "repaired",
                        SystemActivity.path == symlink_path_str,
                        SystemActivity.created_at >= now - timedelta(seconds=30),
                    ).first()

                    if not existing_repair:
                        db.add(SystemActivity(
                            event="symlink_repaired_same_path",
                            action="repaired",
                            path=symlink_path_str,
                            manager=manager or similar_deleted.manager or "unknown",
                            replaced=None,
                            replaced_at=None,
                            message=f"Symlink réparé/recréé : {symlink_path_str}",
                            extra=repair_extra,
                        ))

                    logger.info(
                        f"🛠️ Réparation détectée même chemin ({match_reason}) "
                        f"({old_path})"
                    )

                    sse_manager.publish_event(
                        "symlink_update",
                        {
                            "event": "symlink_repaired_same_path",
                            "action": "repaired",
                            "old_path": old_path,
                            "new_path": symlink_path_str,
                            "path": symlink_path_str,
                            "manager": manager,
                            "message": f"Symlink réparé/recréé : {symlink_path_str}",
                            "replaced": None,
                            "replaced_at": None,
                            "match_reason": match_reason,
                            "same_path": True,
                        },
                    )

                else:
                    replacement_extra = {
                        "old_path": old_path,
                        "new_path": symlink_path_str,
                        "match_reason": match_reason,
                        "created_extra": item,
                        "deleted_extra": get_extra_dict(similar_deleted),
                    }

                    existing_replacement = db.query(SystemActivity).filter(
                        SystemActivity.action == "replaced",
                        SystemActivity.path == symlink_path_str,
                        SystemActivity.replaced.is_(True),
                        SystemActivity.created_at >= now - timedelta(seconds=30),
                    ).first()

                    if not existing_replacement:
                        db.add(SystemActivity(
                            event="symlink_replacement",
                            action="replaced",
                            path=symlink_path_str,
                            manager=manager or similar_deleted.manager or "unknown",
                            replaced=True,
                            replaced_at=now,
                            message=f"Symlink remplacé : {old_path} → {symlink_path_str}",
                            extra=replacement_extra,
                        ))

                    logger.info(
                        f"♻️ Remplacement détecté ({match_reason}) "
                        f"({old_path} → {symlink_path_str})"
                    )

                    sse_manager.publish_event(
                        "symlink_update",
                        {
                            "event": "symlink_replacement",
                            "action": "replaced",
                            "old_path": old_path,
                            "new_path": symlink_path_str,
                            "path": symlink_path_str,
                            "manager": manager,
                            "replaced": True,
                            "replaced_at": now.isoformat(),
                            "update_deleted": True,
                            "match_reason": match_reason,
                        },
                    )

            broken_deleted = db.query(SystemActivity).filter(
                SystemActivity.path == symlink_path_str,
                SystemActivity.action == "broken"
            ).delete()

            if broken_deleted:
                with self._lock:
                    for x in symlink_store:
                        if x.get("symlink") == symlink_path_str:
                            x["broken"] = False
                            x["target_exists"] = True
                            x["ref_count"] = 1
                            break

                existing_broken_repair = db.query(SystemActivity).filter(
                    SystemActivity.action == "repaired",
                    SystemActivity.path == symlink_path_str,
                    SystemActivity.created_at >= now - timedelta(seconds=30),
                ).first()

                if not existing_broken_repair:
                    db.add(SystemActivity(
                        event="symlink_repaired_live",
                        action="repaired",
                        path=symlink_path_str,
                        manager=manager,
                        message=f"Symlink réparé par recréation : {symlink_path}",
                        extra={
                            "auto_repair": False,
                            "reason": "created_after_broken",
                        },
                    ))

                sse_manager.publish_event(
                    "symlink_update",
                    {
                        "event": "symlink_repaired",
                        "action": "repaired",
                        "path": symlink_path_str,
                        "manager": manager,
                        "message": f"Symlink réparé par recréation : {symlink_path}",
                    },
                )

                logger.info(f"🧩 Symlink brisé réparé et nettoyé en base : {symlink_path}")

            db.add(SystemActivity(
                event="symlink_added",
                action="created",
                path=symlink_path_str,
                manager=manager,
                message=f"Symlink ajouté : {symlink_path}",
                extra=item,
            ))

            db.commit()

            sse_manager.publish_event(
                "symlink_update",
                {
                    "event": "symlink_added",
                    "action": "created",
                    "path": symlink_path_str,
                    "item": item,
                    "id": str(uuid.uuid4()),
                    "count": len(symlink_store),
                },
            )

            with buffer_lock:
                symlink_events_buffer.append({
                    "action": "created",
                    "symlink": symlink_path_str,
                    "path": symlink_path_str,
                    "target": item.get("target"),
                    "manager": item.get("manager"),
                    "title": item.get("title"),
                    "tmdbId": item.get("tmdbId"),
                    "imdb_id": item.get("imdb_id"),
                    "imdbId": item.get("imdbId"),
                    "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "replaced_from": replaced_from,
                })

            logger.success(f"Symlink ajouté au cache : {symlink_path}")

        except Exception as e:
            try:
                if db is not None:
                    db.rollback()
            except Exception:
                pass

            logger.error(
                f"Erreur lors de l'ajout du symlink {symlink_path}: {e}",
                exc_info=True,
            )

        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass

    def _handle_deleted(self, symlink_path: Path):
        """
        Gère la suppression d’un symlink.
        Version sécurisée :
        - récupère metadata depuis symlink_store
        - sinon depuis la dernière entrée "created" de la DB
        - sinon via Radarr index uniquement si manager == radarr
        - évite le doublon si une suppression manuelle/bulk vient déjà d'être enregistrée
        """
        from routers.secure.symlinks import symlink_store
        import uuid
        import re

        def normalize(value: str) -> str:
            if not value:
                return ""

            value = str(value).lower().strip()

            replacements = {
                "é": "e",
                "è": "e",
                "ê": "e",
                "ë": "e",
                "à": "a",
                "â": "a",
                "ä": "a",
                "î": "i",
                "ï": "i",
                "ô": "o",
                "ö": "o",
                "ù": "u",
                "û": "u",
                "ü": "u",
                "ç": "c",
            }

            for src, dst in replacements.items():
                value = value.replace(src, dst)

            value = re.sub(r"[^\w]+", "", value)
            return value.strip()

        symlink_path_str = str(symlink_path)
        removed_item = None

        now_ts = time.monotonic()

        with self._lock:
            last_ts = self._recent_deleted_events.get(symlink_path_str)

            if last_ts is not None and now_ts - last_ts < self._event_dedupe_seconds:
                logger.info(
                    f"⏭️ Suppression watcher déjà reçue récemment, ignorée : {symlink_path}"
                )
                return

            self._recent_deleted_events[symlink_path_str] = now_ts

            cutoff = now_ts - self._event_dedupe_seconds
            self._recent_deleted_events = {
                path: ts
                for path, ts in self._recent_deleted_events.items()
                if ts >= cutoff
            }

        with self._lock:
            for idx in range(len(symlink_store) - 1, -1, -1):
                if symlink_store[idx].get("symlink") == symlink_path_str:
                    removed_item = symlink_store[idx]
                    del symlink_store[idx]
                    break

        manager = removed_item.get("manager") if removed_item else self._detect_manager(symlink_path)

        db = None
        metadata = None

        try:
            db = SessionLocal()

            if removed_item:
                metadata = dict(removed_item)
            else:
                last_created = db.query(SystemActivity).filter(
                    SystemActivity.action == "created",
                    SystemActivity.path == symlink_path_str,
                ).order_by(SystemActivity.created_at.desc()).first()

                if last_created and isinstance(last_created.extra, dict):
                    metadata = dict(last_created.extra)

            # Fallback Radarr uniquement pour les symlinks Radarr.
            if not metadata and manager == "radarr":
                try:
                    parent = symlink_path.parent.name
                    parent_norm = normalize(parent)
                    best = None

                    with radarr_cache._radarr_idx_lock:
                        catalog_items = list(radarr_cache._radarr_catalog.items())

                    for tmdb_id, info in catalog_items:
                        titles = [
                            info.get("title"),
                            info.get("originalTitle"),
                        ]

                        for title in titles:
                            if title and normalize(title) == parent_norm:
                                best = info
                                break

                        if best:
                            break

                    if best:
                        metadata = {
                            "tmdbId": best.get("tmdbId"),
                            "imdbId": best.get("imdbId") or best.get("imdb_id"),
                            "imdb_id": best.get("imdb_id") or best.get("imdbId"),
                            "title": best.get("title"),
                            "originalTitle": best.get("originalTitle"),
                            "year": best.get("year"),
                        }

                except Exception as e:
                    logger.debug(f"⚠️ Fallback metadata Radarr impossible pour {symlink_path}: {e}")

            if not metadata:
                metadata = {
                    "title": symlink_path.stem,
                    "guessed": True,
                }

            recent_manual_deleted = db.query(SystemActivity).filter(
                SystemActivity.action == "deleted",
                SystemActivity.path == symlink_path_str,
                SystemActivity.event.in_([
                    "symlink_removed_manual",
                    "sonarr_symlink_removed_manual",
                    "symlink_removed_broken_bulk",
                    "sonarr_symlink_removed_broken_bulk",
                ]),
                SystemActivity.created_at >= datetime.utcnow() - timedelta(seconds=15),
            ).first()

            if recent_manual_deleted:
                logger.info(
                    f"⏭️ Suppression déjà enregistrée récemment "
                    f"({recent_manual_deleted.event}) : {symlink_path}"
                )

                # On ne crée pas une deuxième ligne deleted en DB.
                # Mais on garde le buffer local cohérent pour les événements live.
                with buffer_lock:
                    symlink_events_buffer.append({
                        "action": "deleted",
                        "symlink": symlink_path_str,
                        "path": symlink_path_str,
                        "manager": manager,
                        "title": metadata.get("title"),
                        "tmdbId": metadata.get("tmdbId"),
                        "imdb_id": metadata.get("imdb_id"),
                        "imdbId": metadata.get("imdbId"),
                        "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                        "skipped_duplicate": True,
                        "existing_event": recent_manual_deleted.event,
                    })

                return

            recent_watcher_deleted = db.query(SystemActivity).filter(
                SystemActivity.action == "deleted",
                SystemActivity.path == symlink_path_str,
                SystemActivity.event == "symlink_removed",
                SystemActivity.created_at >= datetime.utcnow() - timedelta(seconds=5),
            ).first()

            if recent_watcher_deleted:
                logger.info(
                    f"⏭️ Suppression watcher déjà enregistrée récemment : {symlink_path}"
                )
                return

            db.add(SystemActivity(
                event="symlink_removed",
                action="deleted",
                path=symlink_path_str,
                manager=manager,
                replaced=None,
                message=f"Symlink supprimé : {symlink_path}",
                extra=metadata,
            ))

            db.commit()

            sse_manager.publish_event(
                "symlink_update",
                {
                    "id": str(uuid.uuid4()),
                    "event": "symlink_removed",
                    "action": "deleted",
                    "path": symlink_path_str,
                    "manager": manager,
                    "metadata": metadata,
                },
            )

            with buffer_lock:
                symlink_events_buffer.append({
                    "action": "deleted",
                    "symlink": symlink_path_str,
                    "path": symlink_path_str,
                    "manager": manager,
                    "title": metadata.get("title"),
                    "tmdbId": metadata.get("tmdbId"),
                    "imdb_id": metadata.get("imdb_id"),
                    "imdbId": metadata.get("imdbId"),
                    "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                })

            logger.success(
                f"➖ Symlink supprimé du cache et enregistré en base avec metadata : {symlink_path}"
            )

        except Exception as e:
            try:
                if db is not None:
                    db.rollback()
            except Exception:
                pass

            logger.error(
                f"💥 Erreur insertion SystemActivity (deleted): {e}",
                exc_info=True,
            )

        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass

    def _handle_broken(self, symlink_path: Path):
        """Gère un symlink dont la cible est devenue invalide."""
        db = None

        def mark_broken_in_store_and_db(target_path=None):
            """Marque immédiatement le symlink comme brisé dans le store + DB + SSE."""
            db_broken = None

            try:
                with self._lock:
                    from routers.secure.symlinks import symlink_store

                    found = False

                    for x in symlink_store:
                        if x.get("symlink") == symlink_path_str:
                            x["broken"] = True
                            x["target_exists"] = False
                            x["ref_count"] = 0
                            found = True
                            break

                    if not found:
                        symlink_store.append({
                            "symlink": symlink_path_str,
                            "target": str(target_path) if target_path else None,
                            "manager": manager,
                            "broken": True,
                            "target_exists": False,
                            "ref_count": 0,
                        })

                db_broken = SessionLocal()

                exists_db = db_broken.query(SystemActivity).filter(
                    SystemActivity.path == symlink_path_str,
                    SystemActivity.action == "broken"
                ).first()

                if not exists_db:
                    db_broken.add(SystemActivity(
                        event="symlink_broken_live",
                        action="broken",
                        path=symlink_path_str,
                        manager=manager,
                        message=f"Symlink brisé détecté en live : {symlink_path}",
                        extra={
                            "target": str(target_path) if target_path else None,
                            "auto_repair_enabled": auto_repair_enabled,
                        }
                    ))
                    db_broken.commit()
                    logger.debug(f"💾 Enregistré en base (symlink brisé live) : {symlink_path}")

                sse_manager.publish_event("symlink_update", {
                    "event": "symlink_broken",
                    "action": "broken",
                    "path": symlink_path_str,
                    "manager": manager,
                    "message": f"Symlink brisé détecté : {symlink_path}",
                })

                with buffer_lock:
                    symlink_events_buffer.append({
                        "action": "broken",
                        "symlink": symlink_path_str,
                        "path": symlink_path_str,
                        "manager": manager,
                        "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    })

                logger.warning(f"⚠️ Symlink brisé détecté (live) : {symlink_path}")

            except Exception as e:
                try:
                    if db_broken is not None:
                        db_broken.rollback()
                except Exception:
                    pass

                logger.error(
                    f"💥 Erreur marquage broken live pour {symlink_path}: {e}",
                    exc_info=True,
                )

            finally:
                if db_broken is not None:
                    try:
                        db_broken.close()
                    except Exception:
                        pass

        def mark_repaired_in_store_and_db(result: dict | None = None):
            """Marque immédiatement le symlink comme réparé dans le store + DB + SSE."""
            db_repair = None

            try:
                with self._lock:
                    from routers.secure.symlinks import symlink_store

                    found = False

                    for x in symlink_store:
                        if x.get("symlink") == symlink_path_str:
                            x["broken"] = False
                            x["target_exists"] = True
                            x["ref_count"] = 1
                            found = True
                            break

                    if not found:
                        symlink_store.append({
                            "symlink": symlink_path_str,
                            "target": str(symlink_path.resolve(strict=False)),
                            "manager": manager,
                            "broken": False,
                            "target_exists": True,
                            "ref_count": 1,
                        })

                db_repair = SessionLocal()

                db_repair.query(SystemActivity).filter(
                    SystemActivity.path == symlink_path_str,
                    SystemActivity.action == "broken"
                ).delete()

                db_repair.add(SystemActivity(
                    event="symlink_repaired_live",
                    action="repaired",
                    path=symlink_path_str,
                    manager=manager,
                    message=f"Symlink réparé automatiquement : {symlink_path}",
                    extra={
                        "auto_repair": True,
                        "result": result or {},
                    },
                ))

                db_repair.commit()

                sse_manager.publish_event("symlink_update", {
                    "event": "symlink_repaired",
                    "action": "repaired",
                    "path": symlink_path_str,
                    "manager": manager,
                    "auto_repair": True,
                    "message": f"Symlink réparé automatiquement : {symlink_path}",
                })

                with buffer_lock:
                    symlink_events_buffer.append({
                        "action": "repaired",
                        "symlink": symlink_path_str,
                        "path": symlink_path_str,
                        "manager": manager,
                        "auto_repair": True,
                        "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    })

                logger.success(
                    f"✅ Symlink réparé automatiquement et base mise à jour : {symlink_path}"
                )

            except Exception as e:
                try:
                    if db_repair is not None:
                        db_repair.rollback()
                except Exception:
                    pass

                logger.error(
                    f"💥 Erreur marquage repaired live pour {symlink_path}: {e}",
                    exc_info=True,
                )

            finally:
                if db_repair is not None:
                    try:
                        db_repair.close()
                    except Exception:
                        pass

        try:
            target_path = None

            try:
                target_path = symlink_path.resolve(strict=True)
                if target_path.exists():
                    return
            except FileNotFoundError:
                pass

            manager = self._detect_manager(symlink_path)
            symlink_path_str = str(symlink_path)

            auto_repair_enabled = getattr(
                config_manager.config,
                "auto_repair_broken_symlinks",
                False,
            )

            # Important :
            # On marque d'abord le lien comme broken, même si l'auto-repair est activé.
            # Sinon, en cas de succès auto-repair, l'ancien code faisait return
            # avant que la base soit correctement mise à jour.
            mark_broken_in_store_and_db(target_path=target_path)

            if auto_repair_enabled:
                try:
                    if manager == "radarr":
                        from routers.secure.symlinks import auto_repair_radarr_symlink

                        logger.info(f"🔧 Auto-repair Radarr déclenché pour : {symlink_path}")
                        radarr = RadarrService()

                        result = asyncio.run(
                            auto_repair_radarr_symlink(
                                symlink_path=symlink_path,
                                radarr=radarr,
                            )
                        )

                        logger.info(f"✅ Auto-repair Radarr terminé : {result}")

                        if result.get("success") and result.get("repaired") is True:
                            mark_repaired_in_store_and_db(result=result)
                            return

                        if result.get("success") and result.get("relaunch") is True:
                            logger.info(f"🔁 Recherche Radarr relancée, symlink pas encore recréé : {symlink_path}")
                            return

                    elif manager == "sonarr":
                        from routers.secure.symlinks import auto_repair_sonarr_symlink

                        logger.info(f"🔧 Auto-repair Sonarr déclenché pour : {symlink_path}")

                        db = SessionLocal()
                        try:
                            result = asyncio.run(
                                auto_repair_sonarr_symlink(
                                    symlink_path=symlink_path,
                                    db=db,
                                )
                            )
                        finally:
                            try:
                                db.close()
                            except Exception:
                                pass
                            db = None

                        logger.info(f"✅ Auto-repair Sonarr terminé : {result}")

                        if result.get("success") and result.get("repaired") is True:
                            mark_repaired_in_store_and_db(result=result)
                            return

                        if result.get("success") and result.get("relaunch") is True:
                            logger.info(f"🔁 Recherche relancée, symlink pas encore recréé : {symlink_path}")
                            return

                except Exception as e:
                    logger.error(
                        f"❌ Échec auto-repair de {symlink_path}: {e}",
                        exc_info=True
                    )

            webhook = config_manager.config.discord_webhook_url
            if webhook:
                asyncio.run(send_discord_message(
                    webhook_url=webhook,
                    title="⚠️ Symlink brisé détecté (live)",
                    description=f"Le lien `{symlink_path}` pointe vers une cible manquante.",
                    action="broken"
                ))

        except Exception as e:
            logger.error(f"💥 Erreur dans _handle_broken : {e}", exc_info=True)

        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass

    def _detect_manager(self, path: Path) -> str:
        """Détermine le gestionnaire (radarr, sonarr, etc.) à partir du chemin."""
        try:
            for ld in config_manager.config.links_dirs:
                if str(path).startswith(str(Path(ld.path).resolve())):
                    return ld.manager
        except Exception as e:
            logger.error(f"   Erreur détection manager pour {path}: {e}")
        return "unknown"

# --- 3. Flush automatique Discord ---
def start_discord_flusher():
    # 🔒 Verrou de buffer (fallback si non défini ailleurs)
    lock = globals().get("buffer_lock")
    if lock is None:
        lock = threading.Lock()
        globals()["buffer_lock"] = lock

    # ⚙️ Paramètres par défaut si absents
    max_before = globals().get("MAX_EVENTS_BEFORE_FLUSH", 25)
    interval = globals().get("SUMMARY_INTERVAL", 60)

    def _as_datetime(v) -> datetime:
        """Convertit v en datetime (UTC). Accepte datetime, epoch (int/float), ou str ISO (gère 'Z')."""
        if isinstance(v, datetime):
            return v
        if isinstance(v, (int, float)):
            return datetime.utcfromtimestamp(v)
        if isinstance(v, str):
            s = v.strip()
            # Tente ISO 8601 simple
            try:
                if s.endswith("Z"):
                    # fromisoformat ne gère pas 'Z' -> convertir en +00:00
                    s = s[:-1] + "+00:00"
                # Certaines chaînes sans tz passent quand même; on récupère naive
                dt = datetime.fromisoformat(s)
                # Si aware -> convertit en naive UTC
                try:
                    return dt.astimezone(tz=None).replace(tzinfo=None)
                except Exception:
                    return dt.replace(tzinfo=None)
            except Exception:
                pass
            # Dernières chances: quelques formats courants
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt)
                except Exception:
                    continue
        # Fallback: maintenant (UTC)
        return datetime.utcnow()

    def _normalize_batch(batch: list) -> list[dict]:
        """Homogénéise les événements et renvoie une nouvelle liste de dicts propres."""
        normalized: list[dict] = []
        for ev in batch:
            # Si l'event est une simple string, on l’enveloppe
            if not isinstance(ev, dict):
                normalized.append({
                    "action": "log",
                    "path": str(ev),
                    "time": datetime.utcnow(),
                    "manager": "unknown",
                    "type": "unknown",
                })
                continue

            action = ev.get("action") or ev.get("type") or ({
                "symlink_added": "created",
                "symlink_removed": "deleted",
            }.get(ev.get("event", ""), "update"))

            path = ev.get("path") or ev.get("symlink") or ev.get("target") or "unknown"

            #    time -> datetime obligatoire
            time_dt = _as_datetime(
                ev.get("time") or ev.get("when") or ev.get("created_at") or ev.get("timestamp") or ev.get("ts")
            )

            normalized.append({
                **ev,
                "action": action,
                "path": path,
                "time": time_dt,                  # ✅ datetime (pas str)
                "manager": ev.get("manager") or ev.get("type") or "unknown",
                "type": ev.get("type") or ev.get("manager") or "unknown",
            })
        return normalized

    def loop():
        global last_sent_time, symlink_events_buffer
        while True:
            try:
                now = datetime.utcnow()
                webhook = config_manager.config.discord_webhook_url

                if not webhook:
                    time.sleep(10)
                    continue

                send_now = False
                batch = None

                with lock:
                    count = len(symlink_events_buffer)
                    if count >= max_before:
                        batch = list(symlink_events_buffer)
                        symlink_events_buffer.clear()
                        last_sent_time = now
                        send_now = True
                        logger.debug(f"🚀 Flush Discord par taille: {count} événements")
                    elif count > 0 and (now - last_sent_time).total_seconds() >= interval:
                        batch = list(symlink_events_buffer)
                        symlink_events_buffer.clear()
                        last_sent_time = now
                        send_now = True
                        logger.debug(f"⏱️ Flush Discord par intervalle: {count} événements")

                if send_now and batch:
                    # ✅ Normalisation: 'time' devient un datetime, + champs minimaux
                    safe_batch = _normalize_batch(batch)
                    try:
                        asyncio.run(send_discord_summary(webhook, safe_batch))
                        logger.info(f"📊 Rapport Discord envoyé ({len(safe_batch)} événements)")
                    except Exception as e:
                        logger.error(f"💥 Erreur envoi résumé Discord : {e}")
                        # Réinsère pour re-essai plus tard
                        with lock:
                            symlink_events_buffer[:0] = batch
                        time.sleep(15)
                        continue

                time.sleep(10)

            except Exception as e:
                logger.error(f"💥 Erreur flusher Discord : {e}")
                time.sleep(30)

    threading.Thread(target=loop, daemon=True).start()

# --- 4. Lancement watchers ---
_radarr_building = threading.Lock()

def _launch_radarr_index(force: bool):
    """Lance la construction de l’index Radarr en arrière-plan (protégé par un verrou)."""
    if _radarr_building.locked():
        logger.debug("⏩ Rebuild Radarr déjà en cours, on skip")
        return

    def runner():
        with _radarr_building:
            start = time.time()

            try:
                if force:
                    logger.info("♻️ Rebuild Radarr forcé (cache ignoré)...")
                else:
                    logger.info("🗄️ Chargement radarr_cache")

                asyncio.run(radarr_cache._build_radarr_index(force=force))

                duration = round(time.time() - start, 1)

                with radarr_cache._radarr_idx_lock:
                    index_count = len(radarr_cache._radarr_index)
                    catalog_count = len(radarr_cache._radarr_catalog)

                logger.debug(
                    f"📦 Rebuild Radarr terminé en {duration}s | "
                    f"{index_count} clés, {catalog_count} films"
                )

            except Exception as e:
                logger.error(f"💥 Erreur rebuild Radarr: {e}", exc_info=True)

    threading.Thread(target=runner, daemon=True).start()

def _ensure_radarr_index_ready(force: bool = False):
    """
    Charge ou reconstruit le cache Radarr de maniere bloquante.

    Important:
    - au premier lancement, si le cache disque n'existe pas, on attend la construction complete
    - si le cache disque existe et est valide, le chargement est tres rapide
    - le scan initial des symlinks ne doit demarrer qu'apres cette fonction
    """
    if _radarr_building.locked():
        logger.info("⏳ Cache Radarr deja en construction, attente de la fin...")

        while _radarr_building.locked():
            time.sleep(0.5)

        with radarr_cache._radarr_idx_lock:
            index_count = len(radarr_cache._radarr_index)
            catalog_count = len(radarr_cache._radarr_catalog)

        logger.success(
            f"✅ Cache Radarr pret apres attente: "
            f"{index_count} cles, {catalog_count} films"
        )
        return

    with _radarr_building:
        start = time.time()

        try:
            if force:
                logger.info("♻️ Rebuild Radarr force avant scan initial...")
            else:
                logger.info("🗄️ Chargement du cache Radarr avant scan initial...")

            asyncio.run(radarr_cache._build_radarr_index(force=force))

            duration = round(time.time() - start, 2)

            with radarr_cache._radarr_idx_lock:
                index_count = len(radarr_cache._radarr_index)
                catalog_count = len(radarr_cache._radarr_catalog)

            logger.success(
                f"✅ Cache Radarr pret avant scan initial: "
                f"{index_count} cles, {catalog_count} films | duree={duration}s"
            )

        except Exception as e:
            logger.error(
                f"💥 Erreur chargement cache Radarr avant scan initial: {e}",
                exc_info=True,
            )

def start_symlink_watcher():
    """
    🛰️ Watcher principal des symlinks :
    - Charge ou construit le cache Radarr AVANT le scan initial.
    - Fait le scan initial avec les infos Radarr disponibles.
    - Démarre les watchers immédiatement après le scan.
    - Démarre les watchers en parallèle, sans bridage artificiel.
    - Lance le monitor léger seulement après activation des watchers.
    - Lance Decypharr/orphans ensuite en arrière-plan.
    """
    global symlink_watcher_started

    with symlink_watcher_lock:
        if symlink_watcher_started:
            logger.warning("⏭️ Symlink watcher déjà démarré, lancement ignoré")
            return

        symlink_watcher_started = True

    from routers.secure.symlinks import scan_symlinks, symlink_store
    from watchdog.observers import Observer
    from watchdog.observers.polling import PollingObserver
    from concurrent.futures import ThreadPoolExecutor

    logger.info("🛰️ Symlink watcher démarré (cache Radarr prioritaire)")
    observers = []

    try:
        config = config_manager.config
        links_dirs = [str(ld.path) for ld in config.links_dirs]

        if not links_dirs:
            logger.warning("⏸️ Aucun links_dirs configuré")
            return

        # --- 1️⃣ Cache Radarr obligatoire AVANT le scan initial ---
        _ensure_radarr_index_ready(force=False)

        # --- 2️⃣ Scan initial avec cache Radarr prêt ---
        try:
            logger.info("🔍 Scan initial des symlinks avec cache Radarr prêt...")

            start_scan = time.time()
            symlinks_data = scan_symlinks()
            duration_scan = round(time.time() - start_scan, 2)

            count_symlinks = len(symlinks_data)

            symlink_store.clear()
            symlink_store.extend(symlinks_data)

            logger.success(
                f"✔️ Scan initial terminé — {count_symlinks} symlinks chargés | durée={duration_scan}s"
            )

        except Exception as e:
            logger.exception(f"💥 Erreur scan initial symlinks : {e}")
            symlinks_data = []
            count_symlinks = 0

        # --- 3️⃣ Démarrage des watchers immédiatement après le scan ---
        def start_observer(dir_path: str):
            path = Path(dir_path)

            if not path.exists():
                logger.warning(f"⚠️ Dossier symlink introuvable : {path}")
                return None

            try:
                observer = Observer() if not path.is_mount() else PollingObserver(timeout=5)
            except Exception:
                observer = PollingObserver(timeout=10)

            try:
                observer.schedule(
                    SymlinkEventHandler(),
                    path=str(path),
                    recursive=True,
                )

                observer.start()

                logger.info(
                    f"📍 Watcher actif sur {path.resolve()} ({observer.__class__.__name__})"
                )

                return observer

            except Exception as e:
                logger.error(
                    f"💥 Impossible de démarrer le watcher sur {path} : {e}",
                    exc_info=True,
                )

                try:
                    observer.stop()
                    observer.join()
                except Exception:
                    pass

                return None

        logger.info("🚀 Démarrage des watchers Inotify/Polling en parallèle...")

        start_watchers = time.time()

        # Important :
        # On garde la vitesse d'avant : un worker par links_dir.
        max_workers = max(1, len(links_dirs))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(start_observer, links_dirs))

        observers = [obs for obs in results if obs]

        duration_watchers = round(time.time() - start_watchers, 2)

        logger.success(
            f"✅ {len(observers)} watcher(s) actif(s) sur {len(links_dirs)} dossier(s) | durée={duration_watchers}s"
        )

        # --- 4️⃣ Notification frontend après scan + watchers actifs ---
        sse_manager.publish_event(
            "symlink_update",
            {
                "event": "initial_scan",
                "action": "scan",
                "path": "Scan initial des symlinks",
                "message": "Scan initial terminé et watchers actifs",
                "count": count_symlinks,
            },
        )

        # ✅ Signal uniquement quand scan + watchers sont prêts
        initial_scan_done.set()
        logger.info("🔔 Signal envoyé : scan initial terminé + watchers actifs")

        # --- 5️⃣ Monitor léger après watchers actifs ---
        broken_monitor_cycle_done.clear()
        broken_monitor_running.clear()

        threading.Thread(
            target=start_light_broken_symlink_monitor,
            daemon=True,
        ).start()

        logger.info("🧩 Monitor léger démarré après activation complète des watchers.")

        # --- 6️⃣ Decypharr / orphans ensuite en arrière-plan ---
        def start_decypharr_orphans_background():
            try:
                wait_for_decypharr_containers(min_uptime_seconds=120)

                if not is_decypharr_orphans_enabled():
                    logger.info("🧩 Scan orphelins Decypharr désactivé par configuration.")
                    return

                logger.info("⏳ Orphelins : attente fin du premier cycle symlinks brisés...")
                broken_monitor_cycle_done.wait()

                while broken_monitor_running.is_set():
                    logger.info("⏳ Orphelins : monitor brisés encore actif, attente 5s...")
                    time.sleep(5)

                logger.info("⏱️ Orphelins : stabilisation après cycle brisés terminé...")
                time.sleep(2 * 60)

                logger.info("🧹 Orphelins : lancement après cycle brisés terminé.")
                run_orphans_process()

            except Exception as e:
                logger.error(
                    f"💥 Erreur process Decypharr/orphans en arrière-plan : {e}",
                    exc_info=True,
                )

        threading.Thread(
            target=start_decypharr_orphans_background,
            daemon=True,
        ).start()

        logger.info("🧹 Process Decypharr/orphans lancé en arrière-plan.")

        # --- 7️⃣ Boucle passive ---
        logger.info("♻️ Boucle passive active (watchers en veille).")

        while True:
            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("⏹️ Arrêt du Symlink watcher manuel")

    except Exception as e:
        logger.exception(f"💥 Erreur watcher symlink : {e}")

    finally:
        for obs in observers:
            try:
                obs.stop()
                obs.join()
            except Exception:
                pass

        logger.warning("✅ Watchers arrêtés proprement")


def wait_for_decypharr_containers(min_uptime_seconds=120):
    """
    Attend que TOUS les conteneurs dont le nom commence par 'decypharr'
    soient démarrés ET aient un uptime suffisant.
    Ajoute une attente minimale pour éviter les boucles rapides.
    """
    import docker
    from datetime import datetime, timezone
    import time

    client = docker.from_env()

    while True:
        try:
            # Sélectionne tous les conteneurs dont le nom commence par "decypharr"
            containers = [
                c for c in client.containers.list(all=True)
                if any(n.startswith("decypharr") for n in c.name.split("/"))
            ]

            if not containers:
                logger.warning("⚠️ Aucun conteneur dont le nom commence par 'decypharr' trouvé.")
                return

            all_ready = True
            wait_times = []

            now = datetime.now(timezone.utc)

            for c in containers:
                state = c.attrs["State"]
                status = state.get("Status", "").lower()
                started_at = state.get("StartedAt")

                # 1️⃣ Si le conteneur n'est pas running, on attend
                if status != "running":
                    all_ready = False
                    logger.info(f"⏳ {c.name} pas encore running (status={status}) — attente 30s")
                    wait_times.append(30)
                    continue

                # 2️⃣ Vérification du temps de démarrage
                if started_at and started_at not in ("", None):
                    start_time = datetime.strptime(
                        started_at.split(".")[0],
                        "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)

                    uptime = (now - start_time).total_seconds()

                    if uptime < min_uptime_seconds:
                        all_ready = False

                        remaining = min_uptime_seconds - uptime
                        remaining = max(1, int(remaining))  # ⬅️ Fix anti-attente-0s

                        logger.info(
                            f"⏳ {c.name} uptime {int(uptime)}s < {min_uptime_seconds}s "
                            f"— attente {remaining}s"
                        )

                        wait_times.append(remaining)
                        continue

            # 3️⃣ Tous prêts → on sort
            if all_ready:
                return

            # 4️⃣ Attend le max du temps nécessaire
            sleep_time = max(wait_times or [30])
            sleep_time = max(1, int(sleep_time))  # ⬅️ sécurité

            time.sleep(sleep_time)

        except Exception as e:
            logger.warning(f"⚠️ Erreur durant la vérification des conteneurs Decypharr : {e}")
            time.sleep(30)

# ─────────────────────────────────────────────────────────────
# Decypharr orphan helpers
# ─────────────────────────────────────────────────────────────

def get_decypharr_config_value(key: str, default=None):
    return getattr(config_manager.config, key, default)


def get_decypharr_torrents_path() -> str:
    """
    Récupère le chemin torrents depuis la première instance AllDebrid active.
    """
    instances = getattr(config_manager.config, "alldebrid_instances", []) or []

    enabled_instances = [
        inst for inst in instances
        if getattr(inst, "enabled", True)
        and getattr(inst, "api_key", None)
        and getattr(inst, "mount_path", None)
    ]

    if not enabled_instances:
        raise RuntimeError(
            "Configuration manquante : aucune instance AllDebrid active avec mount_path"
        )

    # priorité la plus faible = plus prioritaire
    enabled_instances.sort(key=lambda inst: getattr(inst, "priority", 9999))

    mount_path = str(getattr(enabled_instances[0], "mount_path")).rstrip("/")

    return mount_path

def get_decypharr_rate_limit() -> float:
    """
    Retourne le rate_limit de la première instance active.
    """
    instances = getattr(config_manager.config, "alldebrid_instances", []) or []

    enabled_instances = [
        inst for inst in instances
        if getattr(inst, "enabled", True) and getattr(inst, "api_key", None)
    ]

    if not enabled_instances:
        return 0.2

    enabled_instances.sort(key=lambda inst: getattr(inst, "priority", 9999))

    try:
        return float(getattr(enabled_instances[0], "rate_limit", 0.2) or 0.2)
    except Exception:
        return 0.2

def is_decypharr_orphans_enabled() -> bool:
    """
    Le scan n'est actif que s'il existe au moins une instance AllDebrid
    active avec api_key + mount_path.
    """
    instances = getattr(config_manager.config, "alldebrid_instances", []) or []

    enabled_instances = [
        inst for inst in instances
        if getattr(inst, "enabled", True)
        and getattr(inst, "api_key", None)
        and getattr(inst, "mount_path", None)
    ]

    return len(enabled_instances) > 0

def collect_symlink_targets_from_store_and_disk() -> set[Path]:
    """
    Récupère les cibles des symlinks.

    Priorité :
    1. symlink_store déjà rempli par scan_symlinks()
    2. fallback disque seulement si symlink_store est vide

    Important :
    - ne fait pas de resolve() sur 18k liens, pour éviter les lenteurs WebDAV/mount distant
    """
    targets: set[Path] = set()

    # 1. Depuis symlink_store
    try:
        from routers.secure.symlinks import symlink_store

        for item in list(symlink_store):
            target = item.get("target")
            if not target:
                continue

            targets.add(Path(os.path.normpath(str(target))))

        if targets:
            logger.info(f"🔗 Cibles symlinks récupérées depuis symlink_store : {len(targets)}")
            return targets

    except Exception as e:
        logger.debug(f"⚠️ Impossible de lire symlink_store : {e}")

    # 2. Fallback disque uniquement si store vide
    logger.warning("⚠️ symlink_store vide, fallback scan disque des links_dirs...")

    try:
        links_dirs = getattr(config_manager.config, "links_dirs", [])

        for ld in links_dirs:
            root = Path(ld.path)

            if not root.exists():
                logger.warning(f"⚠️ links_dir introuvable pendant scan orphelins : {root}")
                continue

            for item in root.rglob("*"):
                try:
                    if not item.is_symlink():
                        continue

                    raw_target = os.readlink(item)

                    if os.path.isabs(raw_target):
                        target_path = Path(raw_target)
                    else:
                        target_path = item.parent / raw_target

                    targets.add(Path(os.path.normpath(str(target_path))))

                except FileNotFoundError:
                    continue
                except Exception:
                    continue

    except Exception as e:
        logger.error(f"💥 Erreur collecte cibles symlinks : {e}", exc_info=True)

    return targets

def list_torrent_names_from_webdav() -> set[str]:
    """
    Liste les dossiers réellement présents dans le WebDAV.
    Exemple :
        /mnt/alldebrid/__all__
    """
    root = Path(get_decypharr_torrents_path())

    if not root.exists():
        logger.warning(f"⚠️ Dossier torrents WebDAV introuvable : {root}")
        return set()

    names: set[str] = set()

    try:
        for item in root.iterdir():
            try:
                if item.is_dir():
                    names.add(item.name)
            except Exception:
                continue
    except Exception as e:
        logger.error(f"💥 Impossible de lister le WebDAV {root} : {e}", exc_info=True)

    return names


def scan_decypharr_orphans() -> dict:
    """
    Scan optimisé et sécurisé des dossiers WebDAV non rattachés à un symlink.

    Logique :
    - on lit les dossiers présents dans le mount WebDAV
    - on récupère les cibles de symlinks connues
    - on compare uniquement le 1er dossier sous mount_path
    - on retourne des ORPHELINS AU NIVEAU DOSSIER TORRENT
      (et non pas fichier par fichier)
    """
    torrents_path = get_decypharr_torrents_path().rstrip("/")
    torrents_root = Path(torrents_path)

    torrent_names = list_torrent_names_from_webdav()
    symlink_targets = collect_symlink_targets_from_store_and_disk()

    logger.info(f"📁 Dossiers torrents lus depuis WebDAV : {len(torrent_names)}")
    logger.info(f"🔗 Cibles symlinks collectées : {len(symlink_targets)}")
    logger.info(f"📁 Dossier torrents utilisé : {torrents_path}")

    used_torrent_names: set[str] = set()

    for target in symlink_targets:
        try:
            target_path = Path(os.path.normpath(str(target)))

            try:
                rel = target_path.relative_to(torrents_root)
            except ValueError:
                continue

            parts = rel.parts
            if not parts:
                continue

            torrent_folder_name = parts[0]

            if torrent_folder_name in torrent_names:
                used_torrent_names.add(torrent_folder_name)

        except Exception:
            continue

    orphan_names = sorted(torrent_names - used_torrent_names, key=str.lower)

    orphans = [
        {
            "name": name,
            "path": str(torrents_root / name),
        }
        for name in orphan_names
    ]

    logger.info(
        f"🧩 Résultat scan WebDAV : "
        f"{len(used_torrent_names)} dossier(s) utilisé(s), "
        f"{len(orphans)} orphelin(s)"
    )

    # Sécurité : si on a des symlinks + des torrents mais zéro match,
    # alors mount_path probablement faux → on bloque la suppression.
    if len(torrent_names) > 0 and len(symlink_targets) > 0 and len(used_torrent_names) == 0:
        logger.error("🚨 Aucun symlink ne correspond au mount_path configuré.")
        logger.error("🚨 Le chemin configuré est probablement incorrect.")
        logger.error(f"🚨 mount_path actuel : {torrents_path}")

        sample_targets = list(symlink_targets)[:20]
        logger.error("🚨 Exemples de cibles symlinks trouvées :")
        for sample in sample_targets:
            logger.error(f"   ↳ {sample}")

        logger.error("🚨 Scan annulé par sécurité : aucun orphelin ne sera supprimé.")

        return {
            "orphans": [],
            "count": 0,
            "total_torrents": len(torrent_names),
            "total_symlink_targets": len(symlink_targets),
            "used_torrents": [],
            "torrents_path": torrents_path,
            "symlink_targets": [str(p) for p in symlink_targets],
            "source": "webdav",
            "safety_abort": True,
        }

    return {
        "orphans": orphans,
        "count": len(orphans),
        "total_torrents": len(torrent_names),
        "total_symlink_targets": len(symlink_targets),
        "used_torrents": sorted(used_torrent_names, key=str.lower),
        "torrents_path": torrents_path,
        "symlink_targets": [str(p) for p in symlink_targets],
        "source": "webdav",
        "safety_abort": False,
    }

def populate_orphans_store_from_decypharr_scan(scan_result: dict) -> list[str]:
    """
    Remplit routers.secure.orphans.orphans_store pour delete_all_orphans_job().

    Version correcte pour suppression par torrent :
    - stocke directement les DOSSIERS torrents orphelins
    - crée une entrée par instance AllDebrid active
    - chaque instance garde son api_key / rate_limit / mount_path
    """
    from routers.secure.orphans import orphans_store

    detected_orphans = scan_result.get("orphans", []) or []

    orphans_store.clear()

    instances = getattr(config_manager.config, "alldebrid_instances", []) or []

    enabled_instances = [
        inst for inst in instances
        if getattr(inst, "enabled", True)
        and getattr(inst, "api_key", None)
        and getattr(inst, "mount_path", None)
    ]

    if not enabled_instances:
        raise RuntimeError(
            "Aucune instance AllDebrid active avec api_key. "
            "Ajoute au moins un compte dans 'alldebrid_instances'."
        )

    created_names: list[str] = []

    for index, inst in enumerate(enabled_instances, start=1):
        instance_name = getattr(inst, "name", None) or f"alldebrid_{index}"
        api_key = str(getattr(inst, "api_key"))
        mount_path = str(getattr(inst, "mount_path")).rstrip("/")
        rate_limit = float(getattr(inst, "rate_limit", 0.2) or 0.2)

        instance_orphans = []

        for orphan in detected_orphans:
            orphan_path = orphan.get("path")
            orphan_name = orphan.get("name")

            if not orphan_path or not orphan_name:
                continue

            orphan_path_norm = os.path.normpath(str(orphan_path))
            mount_path_norm = os.path.normpath(mount_path)

            if orphan_path_norm.startswith(mount_path_norm + os.sep) or orphan_path_norm == mount_path_norm:
                instance_orphans.append(orphan_path_norm)

        orphans_store[instance_name] = {
            "orphans": sorted(set(instance_orphans)),
            "symlinks_list": scan_result.get("symlink_targets", []),
            "api_key": api_key,
            "mount_path": mount_path,
            "cache_path": getattr(inst, "cache_path", "") or "",
            "rate_limit": rate_limit,
            "stats": {
                "sources": scan_result.get("total_torrents", 0),
                "symlinks": scan_result.get("total_symlink_targets", 0),
                "orphans": len(instance_orphans),
            },
        }

        created_names.append(instance_name)

    logger.info(
        f"🧩 orphans_store préparé pour {len(created_names)} compte(s) AllDebrid : "
        f"{', '.join(created_names)} — {len(detected_orphans)} torrent(s) orphelin(s)"
    )

    return created_names

def run_orphans_process():
    """
    Cycle complet :
    - scan WebDAV ;
    - détection des dossiers torrents sans symlink ;
    - remplissage de orphans_store ;
    - suppression via delete_all_orphans_job ;
    - comparaison WebDAV avant/après.

    Important :
    - Les suppressions d'orphelins AllDebrid ne sont PAS des suppressions de symlink.
    - Elles doivent donc être enregistrées avec action="orphan_deleted",
      et non action="deleted", sinon elles polluent les compteurs
      Suppressions / Non remplacés.
    """
    from routers.secure.orphans import delete_all_orphans_job

    if not is_decypharr_orphans_enabled():
        logger.info("🧩 Scan orphelins Decypharr désactivé.")
        return

    detected_orphans: list[dict] = []
    orphan_count = 0

    try:
        logger.info("🧹 Lancement du scan WebDAV : torrents non rattachés à un symlink...")

        scan_result = scan_decypharr_orphans()

        if scan_result.get("safety_abort"):
            logger.error("🚨 Scan orphelins interrompu par sécurité. Aucune suppression ne sera lancée.")
            return

        detected_orphans = scan_result.get("orphans", [])
        orphan_count = scan_result.get("count", len(detected_orphans))
        total_torrents = scan_result.get("total_torrents", 0)
        total_targets = scan_result.get("total_symlink_targets", 0)

        logger.info(
            f"🔍 Scan WebDAV terminé : "
            f"{orphan_count} orphelin(s) / "
            f"{total_torrents} dossier(s) / "
            f"{total_targets} cible(s) symlink"
        )

        if orphan_count <= 0:
            logger.info("🧩 Aucun torrent orphelin trouvé — aucune suppression lancée.")
            return

        orphan_names = [
            orphan.get("name")
            for orphan in detected_orphans
            if orphan.get("name")
        ]

        logger.success(f"✅ {orphan_count} torrent(s) orphelin(s) détecté(s)")

        for name in orphan_names[:30]:
            logger.debug(f"🧩 Orphelin : {name}")

        if orphan_count > 30:
            logger.debug(f"… +{orphan_count - 30} autre(s) orphelin(s)")

        populate_orphans_store_from_decypharr_scan(scan_result)

        with buffer_lock:
            symlink_events_buffer[:] = [
                ev for ev in symlink_events_buffer
                if ev.get("action") != "orphan"
            ]

            symlink_events_buffer.append({
                "action": "orphan",
                "path": "Scan orphelins WebDAV",
                "manager": "alldebrid",
                "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "count": orphan_count,
                "orphans": orphan_names,
            })

        try:
            sse_manager.publish_event("symlink_update", {
                "event": "orphan_detected",
                "action": "orphan",
                "path": "Scan orphelins WebDAV",
                "manager": "alldebrid",
                "message": f"{orphan_count} torrent(s) orphelin(s) détecté(s)",
                "count": orphan_count,
                "orphans": orphan_names,
            })
        except Exception as e:
            logger.error(f"💥 Erreur SSE orphan_detected : {e}")

        db = None

        try:
            db = SessionLocal()
            db.add(SystemActivity(
                event="orphan_detected",
                action="orphan",
                path="Scan orphelins WebDAV",
                manager="alldebrid",
                message=f"{orphan_count} torrent(s) orphelin(s) détecté(s)",
                extra={
                    "count": orphan_count,
                    "orphans": orphan_names,
                    "torrents_path": get_decypharr_torrents_path(),
                    "source": "webdav",
                },
            ))
            db.commit()

        except Exception as e:
            try:
                if db is not None:
                    db.rollback()
            except Exception:
                pass

            logger.error(f"💥 Erreur DB orphelins : {e}", exc_info=True)

        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"💥 Erreur durant le scan WebDAV orphelins : {e}", exc_info=True)
        return

    try:
        auto_delete = getattr(config_manager.config.orphan_manager, "auto_delete", False)

        if not auto_delete:
            logger.info("🧪 orphan_manager.auto_delete=False → scan uniquement, aucune suppression lancée.")
            return

        logger.info("🧪 Suppression des orphelins...")

        before_torrents = list_torrent_names_from_webdav()
        logger.info(f"📦 Dossiers WebDAV avant suppression : {len(before_torrents)}")

        _captured_logs: list[str] = []
        _sink_id = logger.add(_captured_logs.append, format="{message}")

        try:
            result_delete = asyncio.run(delete_all_orphans_job(dry_run=False))
        finally:
            try:
                logger.remove(_sink_id)
            except Exception:
                pass

        logger.success("✅ Suppression orphelins terminée")

        after_torrents = list_torrent_names_from_webdav()
        logger.info(f"📦 Dossiers WebDAV après suppression : {len(after_torrents)}")

        deleted_names: list[str] = sorted(before_torrents - after_torrents, key=str.lower)
        deleted_count: int = len(deleted_names)

        if isinstance(result_delete, dict):
            returned_deleted_names = (
                result_delete.get("deleted_torrents")
                or result_delete.get("deleted")
                or result_delete.get("removed")
                or []
            )

            if isinstance(returned_deleted_names, str):
                returned_deleted_names = [returned_deleted_names]

            for name in returned_deleted_names:
                if name and name not in deleted_names:
                    deleted_names.append(name)

            returned_deleted_count = (
                result_delete.get("deleted_count")
                or result_delete.get("count")
                or 0
            )

            if deleted_count == 0 and returned_deleted_count:
                try:
                    deleted_count = int(returned_deleted_count)
                except Exception:
                    pass

        for line in _captured_logs:
            try:
                s = str(line)
            except Exception:
                continue

            if "→ supprimé" in s or " deleted" in s.lower():
                name = s.split("]")[-1].split("→")[0].strip(" -:")
                if name and name not in deleted_names:
                    deleted_names.append(name)

            if deleted_count == 0 and "Fin SUPPRESSION" in s and "supprimé(s)" in s:
                try:
                    import re

                    part = s.split("Fin SUPPRESSION", 1)[-1]
                    left = part.split("supprimé(s)")[0]
                    match = re.search(r"(\d+)\s*$", left.strip(" →,:-"))

                    if match:
                        deleted_count = int(match.group(1))
                    else:
                        match = re.search(r"→\s*(\d+)\s+supprimé", part)
                        if match:
                            deleted_count = int(match.group(1))

                except Exception:
                    pass

        if deleted_names and deleted_count == 0:
            deleted_count = len(deleted_names)

        if not deleted_names and deleted_count > 0:
            deleted_names = [f"{deleted_count} élément(s) supprimé(s)"]

        if deleted_names or deleted_count > 0:
            total = deleted_count or len(deleted_names)

            sse_manager.publish_event("symlink_update", {
                "event": "orphans_deleted",
                "action": "orphan_deleted",
                "path": "Suppression orphelins",
                "manager": "alldebrid",
                "message": f"{total} torrent(s) supprimé(s)",
                "count": total,
                "deleted_torrents": deleted_names,
            })

            db = None

            try:
                db = SessionLocal()
                db.add(SystemActivity(
                    event="orphans_deleted",
                    action="orphan_deleted",
                    path="Suppression orphelins",
                    manager="alldebrid",
                    message=f"{total} torrent(s) supprimé(s)",
                    extra={
                        "deleted_torrents": deleted_names,
                        "count": total,
                        "source": "webdav",
                    },
                ))
                db.commit()

            except Exception as e:
                try:
                    if db is not None:
                        db.rollback()
                except Exception:
                    pass

                logger.error(f"💥 Erreur DB suppression orphelins : {e}", exc_info=True)

            finally:
                if db is not None:
                    try:
                        db.close()
                    except Exception:
                        pass

            webhook = config_manager.config.discord_webhook_url

            if webhook:
                sample = "\n".join(f"- {name}" for name in deleted_names)

                asyncio.run(send_discord_message(
                    webhook_url=webhook,
                    title="🗑️ Suppressions AllDebrid",
                    description=sample,
                    action="orphan_deleted",
                ))

            logger.info(f"📢 Suppression orphelins traitée : {total} suppression(s).")

        else:
            logger.info("🧩 Aucun torrent supprimé — aucune activité créée ni message envoyé.")

    except Exception as e:
        logger.error(f"💥 Erreur suppression orphelins : {e}", exc_info=True)

def start_periodic_orphans_task(interval_hours: float = 24.0):
    """
    Tâche périodique Decypharr orphelins.
    Attend un premier intervalle pour éviter un double run au démarrage.
    """
    if not is_decypharr_orphans_enabled():
        logger.info("🧩 Tâche orphelins ignorée : aucune instance AllDebrid active configurée.")
        return

    def loop():
        logger.info(
            f"🧹 Tâche périodique orphelins Decypharr démarrée "
            f"(premier run dans {interval_hours}h, puis toutes les {interval_hours}h)..."
        )

        time.sleep(interval_hours * 3600)

        while True:
            try:
                logger.info("⏳ Orphelins périodiques : attente fin cycle symlinks brisés...")
                broken_monitor_cycle_done.wait()

                while broken_monitor_running.is_set():
                    logger.info("⏳ Orphelins périodiques : monitor brisés encore actif, attente 5s...")
                    time.sleep(5)

                logger.info("⏱️ Orphelins périodiques : stabilisation après brisés...")
                time.sleep(5 * 60)

                logger.info("🧹 Orphelins périodiques : lancement après cycle brisés terminé.")
                run_orphans_process()

            except Exception as e:
                logger.error(f"💥 Erreur dans la tâche périodique orphelins : {e}", exc_info=True)

            time.sleep(interval_hours * 3600)

    threading.Thread(target=loop, daemon=True).start()

def start_replacement_cleanup_task(interval_hours: float = 1.0, expiry_hours: int = 12):
    """
    🧹 Tâche périodique de correction du statut replaced :
    - Corrige les suppressions qui ont été recréées plus tard.
    - Marque comme "non remplacés" seulement les vrais cas après expiry_hours.
    - Matching robuste :
        1. tmdbId exact
        2. imdb_id / imdbId exact
        3. dossier parent exact
        4. nom normalisé exact
        5. fuzzy strict uniquement si très proche
    - Évite les faux positifs trop larges.
    - Charge les created une seule fois par cycle pour éviter trop de requêtes SQL.
    - Ferme toujours la session DB proprement.
    """

    import re
    from difflib import SequenceMatcher

    def normalize(s: str) -> str:
        """Nettoyage stable : minuscules + accents simples + retire ponctuation/espaces."""
        if not s:
            return ""

        s = str(s).lower().strip()

        replacements = {
            "é": "e",
            "è": "e",
            "ê": "e",
            "ë": "e",
            "à": "a",
            "â": "a",
            "ä": "a",
            "î": "i",
            "ï": "i",
            "ô": "o",
            "ö": "o",
            "ù": "u",
            "û": "u",
            "ü": "u",
            "ç": "c",
        }

        for src, dst in replacements.items():
            s = s.replace(src, dst)

        s = re.sub(r"[^\w]+", "", s)
        return s.strip()

    def get_extra_dict(activity) -> dict:
        extra = getattr(activity, "extra", None)
        return extra if isinstance(extra, dict) else {}

    def get_tmdb(extra: dict):
        return extra.get("tmdbId") or extra.get("tmdb_id")

    def get_imdb(extra: dict):
        return extra.get("imdb_id") or extra.get("imdbId")

    def parent_dir(path_value: str) -> str:
        try:
            return str(Path(path_value).parent)
        except Exception:
            return ""

    def parent_name(path_value: str) -> str:
        try:
            return Path(path_value).parent.name
        except Exception:
            return ""

    def is_episode_path(path_value: str) -> bool:
        try:
            return bool(re.search(r"S\d{1,2}E\d{1,2}", str(path_value), re.IGNORECASE))
        except Exception:
            return False

    def is_generic_name(value: str) -> bool:
        """
        Évite les matches dangereux sur des noms vagues :
        Saison 2, Season 1, S02, sample, subtitles...
        """
        raw = (value or "").strip().lower()
        norm = normalize(raw)

        if not norm:
            return True

        generic_patterns = [
            r"^saison\d+$",
            r"^season\d+$",
            r"^s\d{1,2}$",
            r"^sample$",
            r"^samples$",
            r"^subtitle$",
            r"^subtitles$",
            r"^subs$",
            r"^bonus$",
            r"^extras$",
            r"^extra$",
        ]

        for pattern in generic_patterns:
            if re.match(pattern, norm):
                return True

        return len(norm) < 8

    def strong_fuzzy_match(a: str, b: str) -> bool:
        """
        Fuzzy volontairement strict :
        - refuse les noms génériques
        - exige au moins 12 caractères normalisés
        - exige une similarité élevée
        - évite les simples inclusions trop larges
        """
        if is_generic_name(a) or is_generic_name(b):
            return False

        na = normalize(a)
        nb = normalize(b)

        if not na or not nb:
            return False

        shortest = min(len(na), len(nb))

        if shortest < 12:
            return False

        ratio = SequenceMatcher(None, na, nb).ratio()

        if ratio >= 0.92:
            return True

        # Cas proche : même préfixe long + ratio correct.
        prefix_len = 0
        for ca, cb in zip(na, nb):
            if ca == cb:
                prefix_len += 1
            else:
                break

        return prefix_len >= max(12, int(shortest * 0.75)) and ratio >= 0.85

    def find_replacement_match(deleted, createds: list):
        deleted_path = deleted.path
        deleted_extra = get_extra_dict(deleted)

        deleted_tmdb = get_tmdb(deleted_extra)
        deleted_imdb = get_imdb(deleted_extra)
        deleted_parent_dir = parent_dir(deleted_path)
        deleted_parent_name = parent_name(deleted_path)
        deleted_parent_norm = normalize(deleted_parent_name)

        # 1️⃣ Match par tmdbId exact.
        if deleted_tmdb:
            for created in createds:
                created_extra = get_extra_dict(created)
                if get_tmdb(created_extra) == deleted_tmdb:
                    return created, "tmdbId"

        # 2️⃣ Match par imdb_id / imdbId exact.
        if deleted_imdb:
            for created in createds:
                created_extra = get_extra_dict(created)
                if get_imdb(created_extra) == deleted_imdb:
                    return created, "imdbId"

        # 3️⃣ Match par dossier parent exact.
        # Interdit pour les épisodes Sonarr : le parent est le dossier Season XX,
        # donc ce match peut confondre S02E06 avec S02E02.
        if deleted_parent_dir and not is_episode_path(deleted_path):
            for created in createds:
                if parent_dir(created.path) == deleted_parent_dir:
                    return created, "parent_dir"

        # 4️⃣ Match par nom parent normalisé exact.
        if deleted_parent_norm and not is_generic_name(deleted_parent_name):
            for created in createds:
                created_parent_name = parent_name(created.path)
                if normalize(created_parent_name) == deleted_parent_norm:
                    return created, "parent_name"

        # 5️⃣ Fuzzy strict.
        if deleted_parent_name and not is_generic_name(deleted_parent_name):
            for created in createds:
                created_parent_name = parent_name(created.path)
                if strong_fuzzy_match(deleted_parent_name, created_parent_name):
                    return created, "fuzzy_strict"

        return None, None

    def cleanup_loop():
        logger.info("🧠 Tâche cleanup (replacement) démarrée...")

        while True:
            db = None

            try:
                from integrations.seasonarr.db.database import SessionLocal
                from integrations.seasonarr.db.models import SystemActivity

                db = SessionLocal()

                now = datetime.utcnow()
                cutoff = now - timedelta(hours=expiry_hours)

                deleted_entries = db.query(SystemActivity).filter(
                    SystemActivity.action == "deleted",
                    SystemActivity.replaced.is_(None),
                ).all()

                if not deleted_entries:
                    logger.debug("♻️ Cleanup replacement : aucune suppression en attente.")
                    time.sleep(interval_hours * 3600)
                    continue

                oldest_deleted_time = min(
                    [
                        entry.created_at
                        for entry in deleted_entries
                        if entry.created_at is not None
                    ],
                    default=now - timedelta(hours=expiry_hours),
                )

                # On charge les created une seule fois pour le cycle.
                created_entries = db.query(SystemActivity).filter(
                    SystemActivity.action == "created",
                    SystemActivity.created_at >= oldest_deleted_time,
                ).all()

                updated = 0
                marked_non_replaced = 0
                inspected = len(deleted_entries)

                for deleted in deleted_entries:
                    deleted_time = deleted.created_at or (now - timedelta(days=999))
                    deleted_parent = parent_name(deleted.path)

                    # Ne considère que les créations postérieures à cette suppression.
                    createds_after_delete = [
                        created
                        for created in created_entries
                        if created.created_at and created.created_at > deleted_time
                    ]

                    match, match_reason = find_replacement_match(
                        deleted,
                        createds_after_delete,
                    )

                    if match:
                        replacement_time = match.created_at or now
                        old_path = str(deleted.path)
                        new_path = str(match.path)

                        deleted.replaced = True
                        deleted.replaced_at = replacement_time
                        deleted.updated_at = now

                        # Version compatible SQLite :
                        # on évite les filtres JSON du type extra["old_path"].as_string()
                        existing_replaced = db.query(SystemActivity).filter(
                            SystemActivity.action == "replaced",
                            SystemActivity.path == new_path,
                            SystemActivity.replaced.is_(True),
                        ).first()

                        if not existing_replaced:
                            db.add(SystemActivity(
                                event="symlink_replacement_cleanup",
                                action="replaced",
                                path=new_path,
                                manager=match.manager or deleted.manager or "unknown",
                                replaced=True,
                                replaced_at=replacement_time,
                                message=f"Remplacement rattrapé : {old_path} → {new_path}",
                                extra={
                                    "old_path": old_path,
                                    "new_path": new_path,
                                    "match_reason": match_reason,
                                    "source": "cleanup_task",
                                    "deleted_extra": get_extra_dict(deleted),
                                    "created_extra": get_extra_dict(match),
                                },
                            ))

                        updated += 1

                        logger.info(
                            f"♻️ Remplacement rattrapé ({match_reason}) : "
                            f"{old_path} → {new_path}"
                        )

                        try:
                            sse_manager.publish_event(
                                "symlink_update",
                                {
                                    "event": "symlink_replacement_cleanup",
                                    "action": "replaced",
                                    "path": new_path,
                                    "manager": match.manager or deleted.manager or "unknown",
                                    "replaced": True,
                                    "replaced_at": replacement_time.isoformat(),
                                    "old_path": old_path,
                                    "new_path": new_path,
                                    "match_reason": match_reason,
                                },
                            )
                        except Exception:
                            pass

                        continue

                    # Pas de match : seulement marquer non remplacé après expiry_hours.
                    if deleted.created_at and deleted.created_at < cutoff:
                        deleted.replaced = False
                        deleted.replaced_at = None
                        deleted.updated_at = now
                        marked_non_replaced += 1

                db.commit()

                if updated or marked_non_replaced:
                    logger.info(
                        f"♻️ Cleanup replacement : "
                        f"{updated} remplacé(s) corrigé(s), "
                        f"{marked_non_replaced} marqué(s) non remplacé(s), "
                        f"{inspected} inspecté(s)."
                    )
                else:
                    logger.debug(
                        f"♻️ Cleanup replacement : rien à corriger, "
                        f"{inspected} suppression(s) inspectée(s)."
                    )

            except Exception as e:
                logger.error(
                    f"💥 Erreur tâche nettoyage symlinks : {e}",
                    exc_info=True,
                )

                try:
                    if db is not None:
                        db.rollback()
                except Exception:
                    pass

            finally:
                try:
                    if db is not None:
                        db.close()
                except Exception:
                    pass

            time.sleep(interval_hours * 3600)

    threading.Thread(target=cleanup_loop, daemon=True).start()

def start_light_broken_symlink_monitor():
    """
    🔍 Monitor léger des symlinks brisés.
    Vérifie régulièrement les symlinks déjà connus (symlink_store)
    sans rescanner tout le disque.

    Règles :
    - success=True ne veut PAS dire repaired=True
    - relaunch=True veut dire recherche relancée, pas symlink réparé
    - deleted=True veut dire symlink cassé supprimé, pas broken à republier
    - avant d'écrire un broken, on revérifie l'état réel actuel
      pour éviter les races avec le watcher live
    """
    from routers.secure.symlinks import symlink_store
    import os
    import time
    from pathlib import Path
    from datetime import datetime

    logger.debug("⏳ En attente du signal de fin de scan initial...")
    initial_scan_done.wait()
    logger.success("🚀 Signal reçu : lancement de la surveillance des symlinks brisés.")

    mount_dirs = []

    def add_mount_dir(path_value):
        if not path_value:
            return

        try:
            path = Path(str(path_value)).resolve()

            if path not in mount_dirs:
                mount_dirs.append(path)

        except Exception as e:
            logger.debug(f"⚠️ Mount ignoré ({path_value}) : {e}")

    for d in getattr(config_manager.config, "mount_dirs", []) or []:
        add_mount_dir(d)

    if mount_dirs:
        logger.info(
            f"📁 Symlinks Brisés : mount(s) surveillé(s) = "
            f"{[str(p) for p in mount_dirs]}"
        )
    else:
        logger.error(
            "🚨 Monitor léger : aucun mount_dir configuré. "
            "Aucun symlink ne sera vérifié par le monitor léger."
        )

    def get_target_mount_info(target_str: str | None) -> tuple[Path | None, str | None]:
        if not target_str:
            return None, None

        try:
            target_path = Path(os.path.normpath(str(target_str)))
        except Exception:
            return None, None

        sorted_mounts = sorted(
            mount_dirs,
            key=lambda p: len(str(p)),
            reverse=True,
        )

        for mount_dir in sorted_mounts:
            try:
                rel = target_path.relative_to(mount_dir)
                parts = rel.parts

                if not parts:
                    return mount_dir, None

                return mount_dir, parts[0]

            except ValueError:
                continue

        return None, None

    def list_existing_torrent_names_by_mount() -> dict[str, set[str]]:
        result: dict[str, set[str]] = {}

        for mount_dir in mount_dirs:
            names: set[str] = set()

            try:
                if not mount_dir.exists():
                    logger.warning(f"⚠️ Mount introuvable pendant monitor léger : {mount_dir}")
                    result[str(mount_dir)] = names
                    continue

                for child in mount_dir.iterdir():
                    try:
                        if child.is_dir():
                            names.add(child.name)
                    except Exception:
                        continue

                logger.info(
                    f"📁 Symlinks Brisés : {len(names)} dossier(s) listé(s) dans {mount_dir}"
                )

            except Exception as e:
                logger.error(
                    f"💥 Impossible de lister le mount {mount_dir} pendant monitor léger : {e}",
                    exc_info=True,
                )

            result[str(mount_dir)] = names

        return result

    def target_exists_fast(
        item: dict,
        existing_by_mount: dict[str, set[str]],
    ) -> tuple[bool, bool]:
        target_str = item.get("target")

        if not target_str:
            return False, True

        mount_dir, torrent_name = get_target_mount_info(target_str)

        if not mount_dir:
            return True, False

        if not torrent_name:
            return True, False

        mount_key = str(mount_dir)
        known_names = existing_by_mount.get(mount_key, set())

        if known_names:
            return torrent_name in known_names, True

        return True, False

    def symlink_is_valid_on_disk(symlink_path: str) -> bool:
        try:
            path = Path(symlink_path)

            if not path.is_symlink():
                return False

            target = path.resolve(strict=True)
            return target.exists()

        except Exception:
            return False

    def is_currently_ok_in_store_or_disk(symlink_path: str) -> bool:
        """
        Protection anti-race.
        Le watcher live peut avoir recréé le symlink pendant que le monitor
        travaille encore sur une ancienne copie de symlink_store.

        Important :
        Cette fonction est conservée pour la détection des réparations.
        Elle ne doit plus empêcher un symlink détecté brisé par target_exists_fast()
        d'être ajouté à broken_now, écrit en DB, puis comptabilisé.
        """
        try:
            for current in list(symlink_store):
                if str(current.get("symlink")) != symlink_path:
                    continue

                broken = current.get("broken", False)
                target_exists = current.get("target_exists", True)
                ref_count = current.get("ref_count", 1)

                try:
                    ref_count_int = int(ref_count)
                except Exception:
                    ref_count_int = 1

                if (
                    broken is False
                    and target_exists is True
                    and ref_count_int > 0
                ):
                    return True

                break

        except Exception:
            pass

        return symlink_is_valid_on_disk(symlink_path)

    def mark_deleted_activity_resolved(db, symlink_path: str, resolved_at):
        db.query(SystemActivity).filter(
            SystemActivity.action == "deleted",
            SystemActivity.path == symlink_path,
            SystemActivity.replaced.isnot(True),
        ).update(
            {
                SystemActivity.replaced: True,
                SystemActivity.replaced_at: resolved_at,
                SystemActivity.updated_at: resolved_at,
            },
            synchronize_session=False,
        )

    def mark_repaired_after_auto_repair(item: dict, result: dict | None = None):
        db_repair = None
        symlink_path = str(item["symlink"])
        manager = item.get("manager", "unknown")

        try:
            for x in symlink_store:
                if x.get("symlink") == symlink_path:
                    x["broken"] = False
                    x["target_exists"] = True
                    x["ref_count"] = 1
                    break

            db_repair = SessionLocal()
            now = datetime.utcnow()

            db_repair.query(SystemActivity).filter(
                SystemActivity.path == symlink_path,
                SystemActivity.action == "broken"
            ).delete()

            mark_deleted_activity_resolved(
                db_repair,
                symlink_path,
                now,
            )

            db_repair.add(SystemActivity(
                event="symlink_repaired_light",
                action="repaired",
                path=symlink_path,
                manager=manager,
                message=f"Symlink réparé automatiquement : {symlink_path}",
                extra={
                    "auto_repair": True,
                    "source": "light_monitor",
                    "result": result or {},
                    "target": item.get("target"),
                },
                created_at=now,
                updated_at=now,
            ))

            db_repair.commit()

            sse_manager.publish_event(
                "symlink_update",
                {
                    "event": "broken_symlinks_light",
                    "action": "repaired",
                    "path": "Réparation symlink automatique",
                    "message": f"Symlink réparé automatiquement : {symlink_path}",
                    "count": 1,
                    "repaired_symlinks": [symlink_path],
                    "auto_repair": True,
                },
            )

            with buffer_lock:
                symlink_events_buffer.append({
                    "action": "repaired",
                    "symlink": symlink_path,
                    "path": symlink_path,
                    "manager": manager,
                    "auto_repair": True,
                    "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                })

            logger.success(
                f"✅ Symlink réparé automatiquement et base mise à jour : {symlink_path}"
            )

        except Exception as e:
            try:
                if db_repair is not None:
                    db_repair.rollback()
            except Exception:
                pass

            logger.error(
                f"💥 Erreur DB repaired monitor léger pour {symlink_path}: {e}",
                exc_info=True,
            )

        finally:
            if db_repair is not None:
                try:
                    db_repair.close()
                except Exception:
                    pass

    while True:
        cycle_start = time.time()
        broken_monitor_running.set()
        broken_monitor_cycle_done.clear()

        try:
            wait_for_decypharr_containers(min_uptime_seconds=120)

            items = list(symlink_store)
            broken_now = []
            repaired_now = []
            processed_sonarr_seasons = set()

            already_notified = {
                str(s["symlink"])
                for s in items
                if s.get("broken", False) or not s.get("target_exists", True)
            }

            existing_by_mount = list_existing_torrent_names_by_mount()
            total_existing_dirs = sum(len(v) for v in existing_by_mount.values())

            logger.info(
                f"🔎 Symlinks Brisés: {len(items)} symlink(s), "
                f"{total_existing_dirs} dossier(s) WebDAV connu(s)"
            )

            if mount_dirs and total_existing_dirs == 0:
                logger.error(
                    "🚨 Monitor léger annulé : aucun dossier WebDAV listé sur les mount_dirs configurés. "
                    "Protection anti-faux broken / auto-repair."
                )
                time.sleep(6 * 3600)
                continue

            auto_repair_enabled = getattr(
                config_manager.config,
                "auto_repair_broken_symlinks",
                False,
            )

            checked_count = 0
            ignored_count = 0

            for item in items:
                symlink_path = str(item["symlink"])

                exists, checked = target_exists_fast(
                    item,
                    existing_by_mount,
                )

                if checked:
                    checked_count += 1
                else:
                    ignored_count += 1

                if not exists and symlink_path not in already_notified:
                    repaired_by_auto = False
                    auto_action_handled = False

                    if auto_repair_enabled:
                        try:
                            if item.get("manager") == "radarr":
                                from routers.secure.symlinks import auto_repair_radarr_symlink

                                logger.info(f"🔧 Auto-repair Radarr (Symlinks Brisés) : {symlink_path}")
                                radarr = RadarrService()

                                result = asyncio.run(
                                    auto_repair_radarr_symlink(
                                        symlink_path=Path(symlink_path),
                                        radarr=radarr,
                                    )
                                )

                                logger.info(f"✅ Auto-repair Radarr (Symlinks Brisés) terminé : {result}")

                                if result.get("success") and result.get("repaired") is True:
                                    mark_repaired_after_auto_repair(item, result=result)
                                    repaired_by_auto = True
                                    auto_action_handled = True

                                elif result.get("success") and result.get("relaunch") is True:
                                    logger.info(
                                        f"🔁 Recherche Radarr relancée, symlink pas encore recréé : {symlink_path}"
                                    )
                                    auto_action_handled = True

                                elif result.get("success") and result.get("deleted") is True:
                                    logger.info(
                                        f"🗑️ Symlink Radarr cassé supprimé, aucune réparation immédiate : {symlink_path}"
                                    )
                                    auto_action_handled = True

                            elif item.get("manager") == "sonarr":
                                from routers.secure.symlinks import auto_repair_sonarr_symlink

                                season_key = str(Path(symlink_path).parent)

                                if season_key in processed_sonarr_seasons:
                                    logger.info(
                                        f"⏭️ Saison Sonarr déjà traitée dans ce cycle, épisode ignoré : {symlink_path}"
                                    )
                                    auto_action_handled = True
                                    continue

                                processed_sonarr_seasons.add(season_key)

                                logger.info(f"🔧 Auto-repair Sonarr (Symlinks Brisés) : {symlink_path}")

                                db_auto = SessionLocal()

                                try:
                                    result = asyncio.run(
                                        auto_repair_sonarr_symlink(
                                            symlink_path=Path(symlink_path),
                                            db=db_auto,
                                        )
                                    )
                                finally:
                                    try:
                                        db_auto.close()
                                    except Exception:
                                        pass

                                logger.info(f"✅ Auto-repair Sonarr (Symlinks Brisés) terminé : {result}")

                                if result.get("success") and result.get("repaired") is True:
                                    mark_repaired_after_auto_repair(item, result=result)
                                    repaired_by_auto = True
                                    auto_action_handled = True

                                elif result.get("success") and result.get("relaunch") is True:
                                    logger.info(
                                        f"🔁 Recherche Sonarr relancée, symlink pas encore recréé : {symlink_path}"
                                    )
                                    auto_action_handled = True

                                elif result.get("success") and result.get("deleted") is True:
                                    logger.info(
                                        f"🗑️ Symlink Sonarr cassé supprimé, aucune réparation immédiate : {symlink_path}"
                                    )
                                    auto_action_handled = True

                        except Exception as e:
                            logger.error(
                                f"❌ Échec auto-repair monitor léger pour {symlink_path}: {e}",
                                exc_info=True,
                            )

                    if repaired_by_auto or auto_action_handled:
                        continue

                    broken_now.append(item)

                elif exists and symlink_path in already_notified:
                    if is_currently_ok_in_store_or_disk(symlink_path):
                        repaired_now.append(item)

            logger.info(
                f"⚡ Symlinks Brisés : {checked_count} symlink(s) vérifié(s) via mount_dirs, "
                f"{ignored_count} ignoré(s) hors mount_dirs"
            )

            persisted_broken_now = []

            if broken_now:
                db = None

                try:
                    db = SessionLocal()
                    updated_store = 0

                    for item in broken_now:
                        symlink_path = str(item["symlink"])

                        exists_db = db.query(SystemActivity).filter(
                            SystemActivity.path == symlink_path,
                            SystemActivity.action == "broken"
                        ).first()

                        if not exists_db:
                            db.add(SystemActivity(
                                event="symlink_broken_light",
                                action="broken",
                                path=symlink_path,
                                manager=item.get("manager", "unknown"),
                                message=f"Symlink brisé détecté (monitor léger) : {symlink_path}",
                                extra={
                                    "target": item.get("target"),
                                    "source": "light_monitor",
                                },
                            ))

                        changed = False

                        for x in symlink_store:
                            if x.get("symlink") == symlink_path:
                                x["broken"] = True
                                x["target_exists"] = False
                                x["ref_count"] = 0
                                updated_store += 1
                                changed = True
                                break

                        if changed:
                            persisted_broken_now.append(item)

                    db.commit()

                    if updated_store > 0 and persisted_broken_now:
                        sse_manager.publish_event(
                            "symlink_update",
                            {
                                "event": "broken_symlinks_light",
                                "action": "broken",
                                "path": "Détection symlinks brisés (monitor léger)",
                                "message": f"{updated_store} nouveaux liens brisés détectés",
                                "count": updated_store,
                                "broken_symlinks": [
                                    str(s["symlink"])
                                    for s in persisted_broken_now
                                ],
                            },
                        )

                        logger.warning(
                            f"⚠️ {updated_store} symlinks marqués brisés (store)"
                        )

                except Exception as e:
                    try:
                        if db is not None:
                            db.rollback()
                    except Exception:
                        pass

                    logger.error(
                        f"💥 Erreur DB broken Symlinks Brisés : {e}",
                        exc_info=True,
                    )

                finally:
                    if db is not None:
                        try:
                            db.close()
                        except Exception:
                            pass

            persisted_repaired_now = []

            if repaired_now:
                db = None

                try:
                    db = SessionLocal()
                    fixed = 0

                    for item in repaired_now:
                        symlink_path = str(item["symlink"])

                        if not is_currently_ok_in_store_or_disk(symlink_path):
                            continue

                        now = datetime.utcnow()

                        db.query(SystemActivity).filter(
                            SystemActivity.path == symlink_path,
                            SystemActivity.action == "broken"
                        ).delete()

                        mark_deleted_activity_resolved(
                            db,
                            symlink_path,
                            now,
                        )

                        existing_repaired = db.query(SystemActivity).filter(
                            SystemActivity.path == symlink_path,
                            SystemActivity.action == "repaired",
                            SystemActivity.event == "symlink_repaired_light",
                        ).first()

                        if not existing_repaired:
                            db.add(SystemActivity(
                                event="symlink_repaired_light",
                                action="repaired",
                                path=symlink_path,
                                manager=item.get("manager", "unknown"),
                                message=f"Symlink réparé détecté (monitor léger) : {symlink_path}",
                                extra={
                                    "target": item.get("target"),
                                    "source": "light_monitor",
                                    "auto_repair": False,
                                },
                                created_at=now,
                                updated_at=now,
                            ))

                        for x in symlink_store:
                            if x.get("symlink") == symlink_path:
                                x["broken"] = False
                                x["target_exists"] = True
                                x["ref_count"] = 1
                                fixed += 1
                                persisted_repaired_now.append(item)
                                break

                    db.commit()

                    if fixed > 0 and persisted_repaired_now:
                        sse_manager.publish_event(
                            "symlink_update",
                            {
                                "event": "broken_symlinks_light",
                                "action": "repaired",
                                "path": "Réparation symlinks (monitor léger)",
                                "message": f"{fixed} liens réparés détectés",
                                "count": fixed,
                                "repaired_symlinks": [
                                    str(s["symlink"])
                                    for s in persisted_repaired_now
                                ],
                            },
                        )

                        logger.info(f"🧩 {fixed} symlinks réparés marqués dans le store")

                except Exception as e:
                    try:
                        if db is not None:
                            db.rollback()
                    except Exception:
                        pass

                    logger.error(
                        f"💥 Erreur DB repaired monitor léger : {e}",
                        exc_info=True,
                    )

                finally:
                    if db is not None:
                        try:
                            db.close()
                        except Exception:
                            pass

            if persisted_broken_now:
                total = len(persisted_broken_now)
                shown = persisted_broken_now[:20]

                logger.warning("╭───────────────────────────────────────────────")
                logger.warning(f"│ ⚠️  Symlinks brisés détectés : {total}")

                for item in shown:
                    symlink_path = item.get("symlink", "❌ chemin inconnu")
                    target_path = item.get("target") or "❌ cible inconnue"

                    logger.warning("│")
                    logger.warning(f"│ • Symlink : {symlink_path}")
                    logger.warning(f"│   Cible   : {target_path}")

                if total > len(shown):
                    logger.warning("│")
                    logger.warning(
                        f"│ … +{total - len(shown)} autre(s) symlink(s) brisé(s) non affiché(s)"
                    )

                logger.warning("╰───────────────────────────────────────────────")

            if persisted_repaired_now:
                total = len(persisted_repaired_now)
                shown = persisted_repaired_now[:20]

                logger.info("╭───────────────────────────────────────────────")
                logger.info(f"│ ✅ Symlinks réparés détectés : {total}")

                for item in shown:
                    symlink_path = item.get("symlink", "❌ chemin inconnu")
                    target_path = item.get("target") or "✅ cible retrouvée"

                    logger.info("│")
                    logger.info(f"│ • Symlink : {symlink_path}")
                    logger.info(f"│   Cible   : {target_path}")

                if total > len(shown):
                    logger.info("│")
                    logger.info(
                        f"│ … +{total - len(shown)} autre(s) symlink(s) réparé(s) non affiché(s)"
                    )

                logger.info("╰───────────────────────────────────────────────")

        except Exception as e:
            logger.exception(f"💥 Erreur dans le monitor Symlinks : {e}")

        try:
            logger.debug("🧠 Vérification de cohérence entre la base et le store...")

            db = None

            try:
                db = SessionLocal()
                cleaned_count = 0

                broken_db_entries = db.query(SystemActivity).filter(
                    SystemActivity.action == "broken"
                ).all()

                broken_in_store = {
                    str(s["symlink"])
                    for s in symlink_store
                    if s.get("broken", False) or not s.get("target_exists", True)
                }

                for entry in broken_db_entries:
                    if entry.path not in broken_in_store:
                        logger.info(
                            f"🧹 Nettoyage cohérence base : "
                            f"{entry.path} n'est plus marqué brisé (suppression DB)."
                        )
                        db.delete(entry)
                        cleaned_count += 1

                db.commit()

                if cleaned_count > 0:
                    sse_manager.publish_event(
                        "symlink_update",
                        {
                            "event": "broken_symlinks_cleanup",
                            "action": "cleanup_db",
                            "message": (
                                f"{cleaned_count} entrées 'broken' nettoyées "
                                f"dans la base (réparées côté store)"
                            ),
                            "count": cleaned_count,
                        },
                    )

                    logger.success(
                        f"🧹 Nettoyage cohérence base terminé : "
                        f"{cleaned_count} entrées supprimées."
                    )
                else:
                    logger.debug("✅ Base déjà cohérente avec le store.")

            except Exception as e:
                try:
                    if db is not None:
                        db.rollback()
                except Exception:
                    pass

                logger.error(
                    f"💥 Erreur nettoyage cohérence base : {e}",
                    exc_info=True,
                )

            finally:
                if db is not None:
                    try:
                        db.close()
                    except Exception:
                        pass

            total_broken = sum(
                1
                for s in symlink_store
                if s.get("broken", False) or not s.get("target_exists", True)
            )
            total_ok = len(symlink_store) - total_broken

            sse_manager.publish_event(
                "symlink_update",
                {
                    "event": "symlink_count_refresh",
                    "action": "count_update",
                    "message": f"Recalcul du compteur global : {total_broken} liens brisés / {total_ok} valides",
                    "broken_count": total_broken,
                    "ok_count": total_ok,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
            )

            logger.info(
                f"🔄 Symlinks Brisés : Compteur global mis à jour : {total_broken} brisés / {total_ok} valides."
            )

        except Exception as e:
            logger.error(
                f"💥 Erreur pendant la validation de cohérence (base ↔ store) : {e}"
            )

        cycle_duration = round(time.time() - cycle_start, 1)

        logger.info(
            f"⏱️ Détection symlinks brisés terminée en {cycle_duration}s"
        )

        broken_monitor_running.clear()
        broken_monitor_cycle_done.set()

        time.sleep(5 * 60)

def start_all_watchers():
    from integrations.seasonarr.db.database import init_db

    logger.info("🧠 Initialisation de la base de données Seasonarr...")
    init_db()
    logger.info("✅ Base de données initialisée avec succès.")

    logger.info("🚀 Lancement des watchers YAML + Symlink...")
    threading.Thread(target=start_yaml_watcher, daemon=True).start()
    threading.Thread(target=start_symlink_watcher, daemon=True).start()
    start_discord_flusher()
    start_replacement_cleanup_task(interval_hours=0.25, expiry_hours=12)
    start_periodic_orphans_task(interval_hours=24.0)