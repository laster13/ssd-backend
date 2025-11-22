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
initial_scan_done = Event()
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
from program.radarr_cache import (
    _radarr_index,
    _radarr_catalog,
    _radarr_host,
    _radarr_idx_lock,
    _build_radarr_index,
    enrich_from_radarr_index,
)


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

    def on_any_event(self, event):
        if event.is_directory:
            return

        path = Path(event.src_path)
        logger.debug(f"📂 Événement détecté : {event.event_type} -> {path}")

        # 🔍 Vérifie si le fichier est un symlink brisé (cible manquante)
        if path.is_symlink():
            try:
                target = path.resolve(strict=True)
                if not target.exists():
                    self._handle_broken(path)
            except FileNotFoundError:
                # La cible du lien est manquante → symlink brisé
                self._handle_broken(path)

        # 🟢 Création d’un symlink
        if event.event_type == "created" and path.is_symlink():
            self._handle_created(path)

        # 🔴 Suppression d’un symlink
        elif event.event_type == "deleted":
            self._handle_deleted(path)

    def _handle_created(self, symlink_path: Path):
        """
        Gère la création d'un nouveau symlink avec détection robuste du remplacement :
        - Match par ID média (tmdb/imdb)
        - Sinon match par nom normalisé de dossier
        - Sinon match fuzzy simplifié
        """
        try:
            import re
            config = config_manager.config
            links_dirs = [(Path(ld.path).resolve(), ld.manager) for ld in config.links_dirs]
            mount_dirs = [Path(d).resolve() for d in config.mount_dirs]

            # ───────────────────────────────
            # 0. Détection racine + manager
            # ───────────────────────────────
            root, manager = None, "unknown"
            for ld, mgr in links_dirs:
                if str(symlink_path).startswith(str(ld)):
                    root, manager = ld, mgr
                    break
            if not root:
                return

            # ───────────────────────────────
            # 1. Résolution de la cible
            # ───────────────────────────────
            try:
                target_path = symlink_path.resolve(strict=True)
            except FileNotFoundError:
                target_path = symlink_path.resolve(strict=False)

            matched_mount, relative_target = None, None
            for mount_dir in mount_dirs:
                try:
                    relative_target = target_path.relative_to(mount_dir)
                    matched_mount = mount_dir
                    break
                except ValueError:
                    continue
            full_target = str(matched_mount / relative_target) if matched_mount else str(target_path)

            try:
                relative_path = str(symlink_path.resolve().relative_to(root))
            except Exception:
                relative_path = str(symlink_path).replace(str(root) + "/", "")

            stat = symlink_path.lstat()
            created_at = datetime.fromtimestamp(stat.st_mtime).isoformat()

            # ───────────────────────────────
            # 2. Construction métadonnées
            # ───────────────────────────────
            item = {
                "symlink": str(symlink_path),
                "relative_path": relative_path,
                "target": full_target,
                "target_exists": True,
                "manager": manager,
                "type": manager,
                "created_at": created_at,
                "ref_count": 1,
            }

            # Enrichissement Radarr (tmdbId/imdb)
            if manager == "radarr":
                extra = enrich_from_radarr_index(symlink_path)
                if extra:
                    item.update(extra)

            from routers.secure.symlinks import symlink_store
            with self._lock:
                symlink_store.append(item)

            # ───────────────────────────────
            # 3. MATCHING pour remplacement
            # ───────────────────────────────
            db = SessionLocal()
            now = datetime.utcnow()
            replaced_from = None

            # Helper normalization
            def normalize(s: str):
                s = s.lower()
                s = re.sub(r"[^\w]+", "", s)  # retire espaces/ponctuation
                return s

            new_parent = symlink_path.parent.name
            new_parent_norm = normalize(new_parent)

            # 3.1 Matching par ID média ────────────────
            media_id = item.get("tmdbId") or item.get("imdb_id")
            similar_deleted = None

            if media_id:
                similar_deleted = db.query(SystemActivity).filter(
                    SystemActivity.action == "deleted",
                    SystemActivity.replaced.is_(None),
                    SystemActivity.extra.contains({"tmdbId": media_id})
                ).order_by(SystemActivity.created_at.desc()).first()

            # 3.2 Sinon matching par nom normalisé ─────
            if not similar_deleted:
                deleted_candidates = db.query(SystemActivity).filter(
                    SystemActivity.action == "deleted",
                    SystemActivity.replaced.is_(None),
                    SystemActivity.created_at >= now - timedelta(hours=48)
                ).all()

                for d in deleted_candidates:
                    old_parent = Path(d.path).parent.name
                    if normalize(old_parent) == new_parent_norm:
                        similar_deleted = d
                        break

            # 3.3 Sinon matching fuzzy simple ──────────
            if not similar_deleted:
                for d in deleted_candidates:
                    old_parent = Path(d.path).parent.name
                    if new_parent_norm in normalize(old_parent) or normalize(old_parent) in new_parent_norm:
                        similar_deleted = d
                        break

            # ───────────────────────────────
            # 4. Si replacement trouvé
            # ───────────────────────────────
            if similar_deleted:
                similar_deleted.replaced = True
                similar_deleted.replaced_at = now
                replaced_from = similar_deleted.path
                db.commit()

                logger.info(f"♻️ Remplacement détecté ({similar_deleted.path} → {symlink_path})")

                sse_manager.publish_event("symlink_update", {
                    "event": "symlink_replacement",
                    "action": "replaced",
                    "old_path": str(similar_deleted.path),
                    "new_path": str(symlink_path),
                    "manager": manager,
                    "replaced": True,
                    "replaced_at": now.isoformat(),
                    "update_deleted": True
                })

            # ───────────────────────────────
            # 5. Suppression éventuelle broken
            # ───────────────────────────────
            broken_deleted = db.query(SystemActivity).filter(
                SystemActivity.path == str(symlink_path),
                SystemActivity.action == "broken"
            ).delete()

            if broken_deleted:
                db.commit()
                sse_manager.publish_event("symlink_update", {
                    "event": "symlink_repaired",
                    "action": "repaired",
                    "path": str(symlink_path),
                    "manager": manager
                })
                logger.info(f"🧩 Symlink Brisé -> Réparé et supprimé de la base : {symlink_path}")

            # ───────────────────────────────
            # 6. Enregistrement créé
            # ───────────────────────────────
            db.add(SystemActivity(
                event="symlink_added",
                action="created",
                path=str(symlink_path),
                manager=manager,
                message=f"Symlink ajouté : {symlink_path}",
                extra=item
            ))
            db.commit()
            db.close()

            # ───────────────────────────────
            # 7. SSE création
            # ───────────────────────────────
            sse_manager.publish_event("symlink_update", {
                "event": "symlink_added",
                "action": "created",
                "path": str(symlink_path),
                "item": item,
                "id": str(uuid.uuid4()),
                "count": len(symlink_store),
            })

            # Discord buffer
            with buffer_lock:
                symlink_events_buffer.append({
                    "action": "created",
                    "symlink": str(symlink_path),
                    "path": str(symlink_path),
                    "target": item.get("target"),
                    "manager": item.get("manager"),
                    "title": item.get("title"),
                    "tmdbId": item.get("tmdbId"),
                    "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "replaced_from": replaced_from,
                })

            logger.success(f"Symlink enrichi ajouté au cache : {symlink_path}")

        except Exception as e:
            logger.error(f"Erreur lors de l'ajout du symlink {symlink_path}: {e}", exc_info=True)

    def _handle_deleted(self, symlink_path: Path):
        """
        Gère la suppression d’un symlink.
        Version béton :
        - récupère metadata depuis le symlink_store
        - sinon depuis la dernière entrée "created" de la DB
        - sinon via Radarr index (VF/VO, matching robuste)
        """

        from routers.secure.symlinks import symlink_store
        from integrations.seasonarr.db.database import SessionLocal
        from integrations.seasonarr.db.models import SystemActivity
        import uuid
        import re

        # -------------------------------
        #  Helper normalisation de nom
        # -------------------------------
        def normalize(s: str) -> str:
            if not s:
                return ""
            s = s.lower()
            s = re.sub(r"[^\w]+", "", s)
            return s.strip()

        removed_item = None

        # ------------------------------------
        # 1) Récupération depuis symlink_store
        # ------------------------------------
        with self._lock:
            for idx in range(len(symlink_store) - 1, -1, -1):
                if symlink_store[idx].get("symlink") == str(symlink_path):
                    removed_item = symlink_store[idx]
                    del symlink_store[idx]
                    break

        # manager récupéré depuis item sinon fallback
        manager = removed_item.get("manager") if removed_item else self._detect_manager(symlink_path)

        # ------------------------------------
        # 2) Si pas trouvé dans le store → DB
        # ------------------------------------
        db = SessionLocal()

        metadata = None

        if not removed_item:
            last_created = db.query(SystemActivity).filter(
                SystemActivity.action == "created",
                SystemActivity.path == str(symlink_path),
            ).order_by(SystemActivity.created_at.desc()).first()

            if last_created and isinstance(last_created.extra, dict):
                metadata = dict(last_created.extra)

        else:
            metadata = dict(removed_item)

        # ------------------------------------
        # 3) Si toujours rien → tentative via Radarr
        # ------------------------------------
        if not metadata:
            try:
                parent = symlink_path.parent.name
                parent_norm = normalize(parent)

                best = None

                # Parcourt tout le catalogue Radarr
                for tmdb_id, info in _radarr_catalog.items():
                    titles = [
                        info.get("title"),
                        info.get("originalTitle"),
                    ]

                    # matching titre français ou original
                    for t in titles:
                        if t and normalize(t) == parent_norm:
                            best = info
                            break

                    if best:
                        break

                if best:
                    metadata = {
                        "tmdbId": best.get("tmdbId"),
                        "imdb_id": best.get("imdb_id"),
                        "title": best.get("title"),
                        "originalTitle": best.get("originalTitle"),
                        "year": best.get("year"),
                    }

            except Exception:
                pass

        # ------------------------------------
        # 4) Fallback final si rien trouvé
        # ------------------------------------
        if not metadata:
            metadata = {
                "title": symlink_path.stem,
                "guessed": True
            }

        # ------------------------------------
        # 5) Enregistrement DB : deleted
        # ------------------------------------
        try:
            db.add(SystemActivity(
                event="symlink_removed",
                action="deleted",
                path=str(symlink_path),
                manager=manager,
                replaced=None,   # en attente
                message=f"Symlink supprimé : {symlink_path}",
                extra=metadata
            ))
            db.commit()
            logger.debug(f"🗄️ SystemActivity enregistré (deleted) avec metadata")

        except Exception as e:
            logger.error(f"💥 Erreur insertion SystemActivity (deleted): {e}", exc_info=True)

        finally:
            db.close()

        # ------------------------------------
        # 6) SSE vers frontend
        # ------------------------------------
        sse_manager.publish_event("symlink_update", {
            "id": str(uuid.uuid4()),
            "event": "symlink_removed",
            "action": "deleted",
            "path": str(symlink_path),
            "manager": manager,
            "metadata": metadata,
        })

        logger.success(f"➖ Symlink supprimé du cache et enregistré en base avec metadata : {symlink_path}")

    def _handle_broken(self, symlink_path: Path):
        """Gère un symlink dont la cible est devenue invalide."""
        try:
            target_path = None
            try:
                target_path = symlink_path.resolve(strict=True)
                if target_path.exists():
                    # Si la cible existe, on ne considère pas comme "broken"
                    return
            except FileNotFoundError:
                pass

            manager = self._detect_manager(symlink_path)

            # --- 📡 SSE vers le frontend ---
            sse_manager.publish_event("symlink_update", {
                "event": "symlink_broken",
                "action": "broken",
                "path": str(symlink_path),
                "manager": manager,
                "message": f"Symlink brisé détecté : {symlink_path}",
            })
            logger.warning(f"⚠️ Symlink brisé détecté (live) : {symlink_path}")

            # --- 💾 Enregistrement en base ---
            try:
                db = SessionLocal()
                db.add(SystemActivity(
                    event="symlink_broken_live",
                    action="broken",
                    path=str(symlink_path),
                    manager=manager,
                    message=f"Symlink brisé détecté en live : {symlink_path}",
                    extra={"target": str(target_path) if target_path else None}
                ))
                db.commit()
                db.close()
                logger.debug(f"💾 Enregistré en base (symlink brisé live) : {symlink_path}")
            except Exception as e:
                logger.error(f"💥 Erreur DB symlink brisé (live): {e}", exc_info=True)

            # --- 📨 Ajoute dans le buffer Discord ---
            with buffer_lock:
                symlink_events_buffer.append({
                    "action": "broken",
                    "symlink": str(symlink_path),
                    "path": str(symlink_path),
                    "manager": manager,
                    "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                })

            # --- 💬 Envoi Discord direct si configuré ---
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

            # 🔧 time -> datetime obligatoire
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

                asyncio.run(_build_radarr_index(force=force))

                duration = round(time.time() - start, 1)
                count = len(_radarr_index)
                logger.debug(f"📦 Rebuild Radarr terminé en {duration}s")
            except Exception as e:
                logger.error(f"💥 Erreur rebuild Radarr: {e}", exc_info=True)

    threading.Thread(target=runner, daemon=True).start()

def start_symlink_watcher():
    """
    🛰️ Watcher principal des symlinks :
    - Démarre les observateurs (Inotify/Polling) pour chaque links_dir.
    - Charge le cache Radarr en arrière-plan.
    - Fait un scan initial ultra-rapide des symlinks sans vérifier les cibles.
    - Lance ensuite le monitor léger pour la détection continue des liens brisés.
    - Supprime totalement la logique de détection brisés du scan initial et du scan périodique.
    """
    from routers.secure.symlinks import scan_symlinks, symlink_store
    from watchdog.observers import Observer
    from watchdog.observers.polling import PollingObserver
    from concurrent.futures import ThreadPoolExecutor

    logger.info("🛰️ Symlink watcher démarré (version allégée & optimisée)")
    observers = []

    try:
        config = config_manager.config
        links_dirs = [str(ld.path) for ld in config.links_dirs]

        if not links_dirs:
            logger.warning("⏸️ Aucun links_dirs configuré")
            return

        # --- 1️⃣ Démarrage parallèle des watchers (lazy recursive) ---
        def start_observer(dir_path: str):
            path = Path(dir_path)
            if not path.exists():
                logger.warning(f"⚠️ Dossier symlink introuvable : {path}")
                return None

            # Détection auto : inotify (local) ou polling (montage distant)
            try:
                observer = Observer() if not path.is_mount() else PollingObserver(timeout=5)
            except Exception:
                observer = PollingObserver(timeout=10)

            observer.schedule(SymlinkEventHandler(), path=str(path), recursive=True)
            observer.start()
            logger.info(f"📍 Watcher actif sur {path.resolve()} ({observer.__class__.__name__})")
            return observer

        # Démarrage multi-thread pour accélérer le setup initial
        with ThreadPoolExecutor(max_workers=len(links_dirs)) as executor:
            results = list(executor.map(start_observer, links_dirs))
            observers = [r for r in results if r]

        # --- 2️⃣ Build Radarr en arrière-plan ---
        logger.info("🗄️ Chargement du cache Radarr (asynchrone)...")
        threading.Thread(
            target=lambda: asyncio.run(_build_radarr_index(force=False)),
            daemon=True
        ).start()

        # --- 3️⃣ Scan initial ultra-rapide (sans vérif de cibles) ---
        logger.info("🔍 Scan initial des symlinks (sans vérification de cibles)...")
        symlinks_data = scan_symlinks()
        symlink_store.clear()
        symlink_store.extend(symlinks_data)
        logger.success(f"✔️ Scan initial terminé — {len(symlinks_data)} symlinks chargés")

        try:
            import docker
            from datetime import datetime, timezone

            client = docker.from_env()

            container = client.containers.get("decypharr")
            state = container.attrs["State"]
            status = state.get("Status", "").lower()
            started_at = state.get("StartedAt")

            start_time = None
            if started_at and started_at not in ("", None):
                start_time = datetime.strptime(
                    started_at.split(".")[0],
                    "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)

            # 1️⃣ Si pas running → on attend
            if status != "running":
                logger.warning(f"⏸️ Symlink watcher en pause : Decypharr status = {status}")
                time.sleep(60)

            # 2️⃣ Si uptime < 120 sec → on attend aussi
            if start_time:
                uptime = (datetime.now(timezone.utc) - start_time).total_seconds()
                if uptime < 120:
                    logger.info(f"⏳ Decypharr actif depuis {int(uptime)}s — report du scan initial...")
                    time.sleep(120 - int(uptime))

        except Exception as e:
            logger.warning(f"⚠️ Impossible de vérifier l’état du conteneur Decypharr : {e}")

        # 🧹 Process orphelins initial (scan + suppression)
        run_orphans_process()

        # --- 5️⃣ Fin du scan initial ---
        sse_manager.publish_event("symlink_update", {
            "event": "initial_scan",
            "action": "scan",
            "path": "Scan initial des symlinks",
            "message": "Scan initial terminé",
            "count": len(symlinks_data)
        })

        # ✅ Signal pour le monitor léger
        initial_scan_done.set()
        logger.info("🔔 Signal envoyé : scan initial terminé (monitor léger autorisé à démarrer)")

        # 🚀 Lancement explicite du monitor léger
        threading.Thread(target=start_light_broken_symlink_monitor, args=(5,), daemon=True).start()
        logger.info("🧩 Monitor léger démarré après le scan initial.")

        # --- 6️⃣ Boucle passive (veille) ---
        logger.info("♻️ Boucle passive active (watchers en veille).")
        while True:
            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("⏹️ Arrêt du Symlink watcher manuel")
    except Exception as e:
        logger.exception(f"💥 Erreur watcher symlink : {e}")

    finally:
        for obs in observers:
            obs.stop()
            obs.join()
        logger.warning("✅ Watchers arrêtés proprement")


def run_orphans_process():
    """
    Lance un cycle complet de gestion des orphelins AllDebrid :
    - scan des fichiers non rattachés à un symlink
    - enregistrement dans la DB + buffer Discord + SSE
    - suppression via delete_all_orphans_job
    """
    from routers.secure.orphans import scan_instance, delete_all_orphans_job

    # 🧹 Scan orphelins
    try:
        instances = getattr(config_manager.config, "alldebrid_instances", [])
        if instances:
            logger.info("🧹 Lancement du scan des fichiers Alldebrid non rattachés à un symlink...")
            orphan_count = 0
            for inst in instances:
                if getattr(inst, "enabled", True):
                    result = asyncio.run(scan_instance(inst))
                    orphans = result.get("orphans", []) if isinstance(result, dict) else []
                    logger.debug(f"🔍 Résultat scan_instance({inst.name}) → {len(orphans)} orphelins trouvés")
                    orphan_count += len(orphans)

            if orphan_count > 0:
                logger.success(f"✅ Scan orphelins terminé ({orphan_count} fichiers détectés)")

                # 📦 Nettoyage ancien buffer avant ajout
                with buffer_lock:
                    symlink_events_buffer[:] = [
                        ev for ev in symlink_events_buffer if ev.get("action") != "orphan"
                    ]
                    symlink_events_buffer.append({
                        "action": "orphan",
                        "path": "Scan orphelins",
                        "manager": "alldebrid",
                        "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                        "count": orphan_count
                    })

                # 📡 SSE + DB (détection orphelins)
                for inst in instances:
                    try:
                        db = SessionLocal()
                        db.add(SystemActivity(
                            event="orphan_detected",
                            action="orphan",
                            path=f"Instance {inst.name}",
                            manager="alldebrid",
                            message=f"{orphan_count} fichiers orphelins détectés sur {inst.name}"
                        ))
                        db.commit()
                        db.close()
                    except Exception as e:
                        logger.error(f"💥 Erreur DB orphelins : {e}")
            else:
                logger.info("🧩 Aucun fichier orphelin trouvé — pas de message Discord ni DB.")

    except Exception as e:
        logger.error(f"💥 Erreur durant le scan orphelins : {e}")

    # 🧪 Suppression orphelins
    try:
        logger.info("🧪 Suppression des orphelins...")

        # 🔎 Capture des logs du job pour reconstruire la liste des suppressions si le retour est incomplet
        _captured_logs: list[str] = []
        _sink_id = logger.add(_captured_logs.append, format="{message}")

        try:
            result_delete = asyncio.run(delete_all_orphans_job(dry_run=False))
        finally:
            logger.remove(_sink_id)

        logger.success("✅ Suppression orphelins terminée")

        deleted_names: list[str] = []
        deleted_count: int = 0

        # 1) Lecture directe du résultat si structuré
        if isinstance(result_delete, dict):
            deleted_names = (
                result_delete.get("deleted_torrents")
                or result_delete.get("deleted")
                or result_delete.get("removed")
                or []
            )
            deleted_count = (
                result_delete.get("deleted_count")
                or result_delete.get("count")
                or 0
            )

            # + extraction depuis éventuels logs/summary renvoyés
            possible_logs = []
            for k in ("logs", "output", "stdout", "messages", "details", "summary", "report", "message"):
                v = result_delete.get(k)
                if isinstance(v, list):
                    possible_logs.extend(v)
                elif isinstance(v, str) and v.strip():
                    possible_logs.append(v)

            for line in possible_logs:
                if not isinstance(line, str):
                    continue
                if "→ supprimé" in line or " deleted" in line.lower():
                    name = line.split("]")[-1].split("→")[0].strip()
                    if name and name not in deleted_names:
                        deleted_names.append(name)

        # 2) Fallback robuste : parse des logs réellement émis par le job
        for line in _captured_logs:
            try:
                s = str(line)
            except Exception:
                continue
            if "→ supprimé" in s or " deleted" in s.lower():
                name = s.split("]")[-1].split("→")[0].strip()
                if name and name not in deleted_names:
                    deleted_names.append(name)
            # récupère aussi un compteur implicite s'il n'est pas fourni
            if deleted_count == 0 and "Fin SUPPRESSION" in s and "supprimé(s)" in s:
                # ex: "Fin SUPPRESSION → 2 supprimé(s), 0 introuvable(s), 0 erreur(s)"
                try:
                    part = s.split("Fin SUPPRESSION", 1)[-1]
                    # garde uniquement la portion contenant "supprimé(s)"
                    left = part.split("supprimé(s)")[0]
                    # récupère le dernier entier avant "supprimé(s)"
                    import re
                    m = re.search(r"(\d+)\s*$", left.strip(" →,:-"))
                    if m:
                        deleted_count = int(m.group(1))
                    else:
                        # autre format possible: "→ 2 supprimé(s), ..."
                        m2 = re.search(r"→\s*(\d+)\s+supprimé", part)
                        if m2:
                            deleted_count = int(m2.group(1))
                except Exception:
                    pass

        # 3) Si on a un compteur mais pas de noms, crée un libellé générique
        if not deleted_names and deleted_count > 0:
            deleted_names = [f"{deleted_count} élément(s) supprimé(s)"]

        # 4) Émissions si une suppression a été détectée (noms OU compteur)
        if deleted_names or deleted_count > 0:
            total = deleted_count or len(deleted_names)

            # 📡 SSE vers frontend (toujours, webhook désolidarisé)
            sse_manager.publish_event("symlink_update", {
                "event": "orphans_deleted",
                "action": "deleted",
                "path": "Suppression orphelins",
                "message": f"{total} torrents supprimés",
                "count": total,
                "deleted_torrents": deleted_names,
            })
            logger.info("📡 Événement SSE 'orphans_deleted' envoyé au frontend avec la liste complète")

            # 💾 DB
            try:
                db = SessionLocal()
                db.add(SystemActivity(
                    event="orphans_deleted",
                    action="deleted",
                    path="Suppression orphelins",
                    manager="alldebrid",
                    message=f"{total} torrents supprimés",
                    extra={"deleted_torrents": deleted_names},
                ))
                db.commit()
                db.close()
                logger.debug("💾 Activité DB enregistrée : suppression orphelins")
            except Exception as e:
                logger.error(f"💥 Erreur DB suppression orphelins : {e}")

            # 🔔 Discord optionnel (webhook désolidarisé)
            webhook = config_manager.config.discord_webhook_url
            if webhook:
                sample = "\n".join(f"- {name}" for name in deleted_names)
                asyncio.run(send_discord_message(
                    webhook_url=webhook,
                    title="🗑️ Suppressions AllDebrid",
                    description=sample,
                    action="deleted"
                ))
                logger.info(f"📢 Notification Discord envoyée ({total} suppression(s)).")
        else:
            logger.info("🧩 Aucun torrent supprimé — aucune activité créée ni message envoyé.")

    except Exception as e:
        logger.error(f"💥 Erreur suppression orphelins : {e}", exc_info=True)


def start_periodic_orphans_task(interval_hours: float = 24.0):
    """
    ...
    -⚠️ Attends un premier intervalle avant le premier run pour éviter
      un double appel au démarrage (start_symlink_watcher appelle déjà run_orphans_process()).
    """
    def loop():
        logger.info(
            f"🧹 Tâche périodique orphelins démarrée "
            f"(premier run immédiat, puis toutes les {interval_hours}h)..."
        )

        # ⏳ On attend d'abord un intervalle complet pour ne pas doubler le run initial
        time.sleep(interval_hours * 3600)

        while True:
            try:
                run_orphans_process()
            except Exception as e:
                logger.error(f"💥 Erreur dans la tâche périodique orphelins : {e}", exc_info=True)

            time.sleep(interval_hours * 3600)

    threading.Thread(target=loop, daemon=True).start()



def start_replacement_cleanup_task(interval_hours: int = 6, expiry_hours: int = 12):
    """
    🧹 Tâche périodique de correction du statut replaced :
    - Corrige les suppressions qui ont été recréées plus tard (replacement tardif)
    - Marque comme "non remplacés" seulement les vrais cas après expiry_hours
    - Matching robuste :
        • tmdbId / imdb_id
        • nom normalisé (sans ponctuation / espace)
        • dossier parent exact
        • matching fuzzy léger
    """

    import re

    def normalize(s: str):
        """Nettoyage : minuscules + retire accents, ponctuation, espaces."""
        if not s:
            return ""
        s = s.lower()
        s = re.sub(r"[^\w]+", "", s)  # 🔥 retire tout sauf alphanumérique
        return s.strip()

    def cleanup_loop():
        logger.info("🧠 Tâche cleanup (replacement) démarrée...")
        while True:
            try:
                from integrations.seasonarr.db.database import SessionLocal
                from integrations.seasonarr.db.models import SystemActivity

                db = SessionLocal()
                now = datetime.utcnow()
                cutoff = now - timedelta(hours=expiry_hours)

                # Ne traiter que replaced = NULL
                deleted_entries = db.query(SystemActivity).filter(
                    SystemActivity.action == "deleted",
                    SystemActivity.replaced.is_(None),
                ).all()

                updated = 0
                marked_non_replaced = 0

                for deleted in deleted_entries:
                    deleted_path = deleted.path
                    deleted_parent = Path(deleted_path).parent.name
                    deleted_parent_norm = normalize(deleted_parent)
                    deleted_time = deleted.created_at or (now - timedelta(days=999))

                    # Récupère toutes les créations après la suppression
                    createds = db.query(SystemActivity).filter(
                        SystemActivity.action == "created",
                        SystemActivity.created_at > deleted_time,
                    ).all()

                    match = None

                    # ────────────────────────────────────────
                    # 1️⃣ MATCH PAR ID MEDIA (LE PLUS FIABLE)
                    # ────────────────────────────────────────
                    deleted_tmdb = None
                    deleted_imdb = None

                    if isinstance(deleted.extra, dict):
                        deleted_tmdb = deleted.extra.get("tmdbId")
                        deleted_imdb = deleted.extra.get("imdb_id")

                    if deleted_tmdb or deleted_imdb:
                        for c in createds:
                            extra = c.extra if isinstance(c.extra, dict) else {}
                            if extra.get("tmdbId") == deleted_tmdb or extra.get("imdb_id") == deleted_imdb:
                                match = c
                                break

                    # ────────────────────────────────────────
                    # 2️⃣ MATCH PAR NOM NORMALISÉ
                    # ────────────────────────────────────────
                    if not match:
                        for c in createds:
                            parent = Path(c.path).parent.name
                            if normalize(parent) == deleted_parent_norm:
                                match = c
                                break

                    # ────────────────────────────────────────
                    # 3️⃣ MATCH PAR DOSSIER EXACT
                    # ────────────────────────────────────────
                    if not match:
                        deleted_dir = str(Path(deleted_path).parent)
                        for c in createds:
                            if str(Path(c.path).parent) == deleted_dir:
                                match = c
                                break

                    # ────────────────────────────────────────
                    # 4️⃣ MATCH FUZZY LÉGER
                    # ────────────────────────────────────────
                    if not match:
                        for c in createds:
                            parent = Path(c.path).parent.name
                            pnorm = normalize(parent)
                            if deleted_parent_norm in pnorm or pnorm in deleted_parent_norm:
                                match = c
                                break

                    # ────────────────────────────────────────
                    # 5️⃣ SI MATCH → remplacement tardif
                    # ────────────────────────────────────────
                    if match:
                        deleted.replaced = True
                        deleted.replaced_at = match.created_at
                        updated += 1

                        logger.info(f"♻️ Rattrapage remplacement tardif : {deleted.path} → {match.path}")

                        try:
                            from program.managers.sse_manager import sse_manager
                            sse_manager.publish_event("symlink_update", {
                                "event": "symlink_replacement_cleanup",
                                "action": "replaced",
                                "path": deleted.path,
                                "manager": deleted.manager,
                                "replaced_at": match.created_at.isoformat(),
                                "message": f"Rattrapage remplacement tardif pour {deleted_parent}",
                            })
                        except Exception:
                            pass

                        continue  # on passe au deleted suivant

                    # ────────────────────────────────────────
                    # 6️⃣ SINON → trop vieux (vrai "non remplacé")
                    # ────────────────────────────────────────
                    if deleted.created_at and deleted.created_at < cutoff:
                        deleted.replaced = False
                        deleted.replaced_at = now
                        marked_non_replaced += 1

                db.commit()
                db.close()

                if updated or marked_non_replaced:
                    logger.info(
                        f"♻️ Cleanup : {updated} remplacés corrigés, "
                        f"{marked_non_replaced} marqués non remplacés."
                    )

            except Exception as e:
                logger.error(f"💥 Erreur tâche nettoyage symlinks : {e}", exc_info=True)

            time.sleep(interval_hours * 3600)

    threading.Thread(target=cleanup_loop, daemon=True).start()

def start_light_broken_symlink_monitor(interval_minutes=5):
    """
    🔍 Monitor léger des symlinks brisés.
    Vérifie régulièrement les symlinks déjà connus (symlink_store)
    sans rescanner tout le disque.
    ➕ Ajoute ou met à jour les symlinks brisés dans le store (broken=True).
    🟢 Met à jour le store quand réparés (broken=False).
    ⚙️ Met à jour la base et envoie les événements SSE.
    🧠 Ne s'exécute pas si le conteneur 'decypharr' vient de démarrer (< 2 min).
    ♻️ Se met automatiquement en pause si Decypharr redémarre pendant l’exécution.
    """
    from routers.secure.symlinks import symlink_store
    import docker
    from datetime import datetime, timezone

    client = docker.from_env()

    # ⏳ Attend que le scan initial soit terminé avant de commencer la surveillance
    logger.debug("⏳ En attente du signal de fin de scan initial...")
    initial_scan_done.wait()
    logger.success("🚀 Signal reçu : lancement de la surveillance des symlinks brisés.")

    # symlinks déjà connus comme brisés (à ne pas re-notifier)
    already_notified = {
        s["symlink"]
        for s in symlink_store
        if not s.get("target_exists", True) or s.get("broken", False)
    }

    while True:
        try:
            # 🧩 Vérifie l’état du conteneur Decypharr avant chaque cycle
            try:
                container = client.containers.get("decypharr")
                state = container.attrs["State"]
                status = state.get("Status", "").lower()
                started_at = state.get("StartedAt")

                start_time = None
                if started_at and started_at not in ("", None):
                    start_time = datetime.strptime(started_at.split(".")[0], "%Y-%m-%dT%H:%M:%S").replace(
                        tzinfo=timezone.utc
                    )

                if status != "running":
                    logger.warning(f"⏸️ Monitor léger en pause : Decypharr status = {status}")
                    time.sleep(60)
                    continue

                if start_time:
                    uptime = (datetime.now(timezone.utc) - start_time).total_seconds()
                    if uptime < 120:
                        logger.info(f"⏳ Decypharr actif depuis {int(uptime)}s — report du monitor léger...")
                        time.sleep(60)
                        continue

                last_started_at = getattr(start_light_broken_symlink_monitor, "_last_started_at", None)
                current_started_at = started_at

                if last_started_at and current_started_at and current_started_at != last_started_at:
                    logger.warning("♻️ Redémarrage de Decypharr détecté — mise en pause du monitor léger.")
                    setattr(start_light_broken_symlink_monitor, "_last_started_at", current_started_at)
                    time.sleep(120)
                    continue

                setattr(start_light_broken_symlink_monitor, "_last_started_at", current_started_at)

            except Exception as e:
                logger.warning(f"⚠️ Impossible de vérifier l’état du conteneur Decypharr : {e}")
                time.sleep(30)
                continue

            # --- Routine principale du monitor ---
            broken_now, repaired_now = [], []
            items = list(symlink_store)

            for i in items:
                symlink_path = Path(i["symlink"])
                if not symlink_path.exists() and not symlink_path.is_symlink():
                    # chemin invalide ET pas un lien → on ignore ce cas (store géré ailleurs)
                    continue

                exists = False
                try:
                    if symlink_path.is_symlink():
                        target = os.readlink(symlink_path)
                        if not os.path.isabs(target):
                            target = os.path.join(symlink_path.parent, target)
                        exists = os.path.exists(target)
                    else:
                        exists = symlink_path.exists()
                except Exception:
                    exists = False

                if not exists and str(symlink_path) not in already_notified:
                    already_notified.add(str(symlink_path))
                    broken_now.append(i)
                elif exists and str(symlink_path) in already_notified:
                    already_notified.remove(str(symlink_path))
                    repaired_now.append(i)

            # === 🔴 Nouveaux symlinks brisés ===
            if broken_now:
                db = SessionLocal()
                added_db = 0

                for s in broken_now:
                    # Évite doublon DB
                    exists_db = db.query(SystemActivity).filter(
                        SystemActivity.path == s["symlink"],
                        SystemActivity.action == "broken"
                    ).first()
                    if exists_db:
                        logger.debug(f"↩️ Symlink déjà marqué brisé (DB), ignoré : {s['symlink']}")
                        continue

                    db.add(SystemActivity(
                        event="symlink_broken_light",
                        action="broken",
                        path=s["symlink"],
                        manager=s.get("manager", "unknown"),
                        message=f"Symlink brisé détecté (monitor léger) : {s['symlink']}",
                        extra={"target": s.get("target")},
                    ))
                    added_db += 1

                db.commit()
                db.close()

                # ✅ Met à jour le store (flag broken) même si l’item existe déjà
                updated_store = 0
                for s in broken_now:
                    found = False
                    for x in symlink_store:
                        if x["symlink"] == s["symlink"]:
                            x["broken"] = True
                            x["target_exists"] = False
                            x["ref_count"] = 0
                            found = True
                            updated_store += 1
                            break
                    if not found:
                        symlink_store.append({
                            "symlink": s["symlink"],
                            "target": s.get("target"),
                            "manager": s.get("manager", "unknown"),
                            "broken": True,
                            "target_exists": False,
                            "ref_count": 0,
                        })
                        updated_store += 1

                if updated_store > 0:
                    sse_manager.publish_event("symlink_update", {
                        "event": "broken_symlinks_light",
                        "action": "broken",
                        "path": "Détection symlinks brisés (monitor léger)",
                        "message": f"{updated_store} nouveaux liens brisés détectés",
                        "count": updated_store,
                        "broken_symlinks": [s["symlink"] for s in broken_now],
                    })
                    logger.warning(f"⚠️ {updated_store} symlinks marqués brisés (store) — monitor léger")

            # === 🟢 Symlinks réparés ===
            if repaired_now:
                db = SessionLocal()
                for s in repaired_now:
                    db.query(SystemActivity).filter(
                        SystemActivity.path == s["symlink"],
                        SystemActivity.action == "broken"
                    ).delete()
                db.commit()
                db.close()

                # ✅ Met à jour le store : plus brisé
                fixed = 0
                for s in repaired_now:
                    for x in symlink_store:
                        if x["symlink"] == s["symlink"]:
                            x["broken"] = False
                            x["target_exists"] = True
                            fixed += 1
                            break

                sse_manager.publish_event("symlink_update", {
                    "event": "broken_symlinks_light",
                    "action": "repaired",
                    "path": "Réparation symlinks (monitor léger)",
                    "message": f"{fixed} liens réparés détectés",
                    "count": fixed,
                    "repaired_symlinks": [s["symlink"] for s in repaired_now],
                })
                logger.info(f"🧩 {fixed} symlinks réparés marqués dans le store")

            # === Logs lisibles ===
            if broken_now:
                logger.warning("╭───────────────────────────────────────────────")
                for s in broken_now:
                    logger.warning(f"│   • {s['symlink']}")
                    logger.warning(f"│     ↳ {s.get('target') or '❌ (inconnu)'}")
                logger.warning("╰───────────────────────────────────────────────")
            elif repaired_now:
                logger.info("╭───────────────────────────────────────────────")
                for s in repaired_now:
                    logger.info(f"│   • {s['symlink']}")
                    logger.info(f"│     ↳ {s.get('target') or '   (cible retrouvée)'}")
                logger.info("╰───────────────────────────────────────────────")

        except Exception as e:
            logger.exception(f"💥 Erreur dans le monitor léger : {e}")

        # === 🧠 Validation cohérence DB ↔ store ===
        try:
            logger.debug("🧠 Vérification de cohérence entre la base et le store...")

            db = SessionLocal()
            cleaned_count = 0

            # Entrées 'broken' en base
            broken_db_entries = db.query(SystemActivity).filter(
                SystemActivity.action == "broken"
            ).all()

            # Index des chemins brisés dans le store (basé sur le flag 'broken')
            broken_in_store = {str(s["symlink"]) for s in symlink_store if s.get("broken", False)}

            for entry in broken_db_entries:
                if entry.path not in broken_in_store:
                    # 🧹 Si la base contient 'broken' mais le store ne l'a pas en 'broken=True'
                    logger.info(f"🧹 Nettoyage cohérence base : {entry.path} n'est plus marqué brisé (suppression DB).")
                    db.delete(entry)
                    cleaned_count += 1

            db.commit()
            db.close()

            if cleaned_count > 0:
                sse_manager.publish_event("symlink_update", {
                    "event": "broken_symlinks_cleanup",
                    "action": "cleanup_db",
                    "message": f"{cleaned_count} entrées 'broken' nettoyées dans la base (réparées côté store)",
                    "count": cleaned_count,
                })
                logger.success(f"🧹 Nettoyage cohérence base terminé : {cleaned_count} entrées supprimées.")
            else:
                logger.debug("✅ Base déjà cohérente avec le store.")

            # 🔄 Recalcul du compteur global
            total_broken = sum(1 for s in symlink_store if s.get("broken", False))
            total_ok = len(symlink_store) - total_broken

            sse_manager.publish_event("symlink_update", {
                "event": "symlink_count_refresh",
                "action": "count_update",
                "message": f"Recalcul du compteur global : {total_broken} liens brisés / {total_ok} valides",
                "broken_count": total_broken,
                "ok_count": total_ok,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            })

            logger.info(f"🔄 Compteur global mis à jour : {total_broken} brisés / {total_ok} valides.")

        except Exception as e:
            logger.error(f"💥 Erreur pendant la validation de cohérence (base ↔ store) : {e}")

        time.sleep(interval_minutes * 60)

def start_all_watchers():
    from integrations.seasonarr.db.database import init_db

    logger.info("🧠 Initialisation de la base de données Seasonarr...")
    init_db()
    logger.info("✅ Base de données initialisée avec succès.")

    logger.info("🚀 Lancement des watchers YAML + Symlink...")
    threading.Thread(target=start_yaml_watcher, daemon=True).start()
    threading.Thread(target=start_symlink_watcher, daemon=True).start()
    start_discord_flusher()
    start_replacement_cleanup_task(interval_hours=0.0167, expiry_hours=12)
    start_periodic_orphans_task(interval_hours=24.0)


