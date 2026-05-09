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
from concurrent.futures import ThreadPoolExecutor, as_completed
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

        wait_for_decypharr_containers(min_uptime_seconds=120)

        # 🧹 Process orphelins initial Decypharr
        if is_decypharr_orphans_enabled():
            run_orphans_process()
        else:
            logger.info("🧩 Scan orphelins Decypharr désactivé par configuration.")

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
        threading.Thread(target=start_light_broken_symlink_monitor, daemon=True).start()
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

    Source principale :
        decypharr_torrents_path, par exemple /mnt/alldebrid/__all__
    """
    torrents_path = get_decypharr_torrents_path().rstrip("/")
    torrents_path_prefix = torrents_path + "/"

    torrent_names = list_torrent_names_from_webdav()
    symlink_targets = collect_symlink_targets_from_store_and_disk()

    logger.info(f"📁 Dossiers torrents lus depuis WebDAV : {len(torrent_names)}")
    logger.info(f"🔗 Cibles symlinks collectées : {len(symlink_targets)}")
    logger.info(f"📁 Dossier torrents utilisé : {torrents_path}")

    used_torrent_names: set[str] = set()

    for target in symlink_targets:
        try:
            target_str = os.path.normpath(str(target))

            if not target_str.startswith(torrents_path_prefix):
                continue

            relative = target_str[len(torrents_path_prefix):]
            torrent_folder_name = relative.split("/", 1)[0]

            if torrent_folder_name in torrent_names:
                used_torrent_names.add(torrent_folder_name)

        except Exception:
            continue

    orphan_names = sorted(torrent_names - used_torrent_names, key=str.lower)

    orphans = [
        {
            "name": name,
            "path": str(Path(torrents_path) / name),
        }
        for name in orphan_names
    ]

    logger.info(
        f"🧩 Résultat scan WebDAV : "
        f"{len(used_torrent_names)} dossier(s) utilisé(s), "
        f"{len(orphans)} orphelin(s)"
    )

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

    Version compatible 1 ou plusieurs comptes AllDebrid :
    - utilise config_manager.config.alldebrid_instances
    - crée une entrée orphans_store par compte activé
    - fonctionne aussi avec un seul compte
    """
    from routers.secure.orphans import orphans_store

    detected_orphans = scan_result.get("orphans", [])
    mount_path = get_decypharr_torrents_path()

    fake_orphan_files = [
        str(Path(orphan["path"]) / ".seasonarr_orphan_marker")
        for orphan in detected_orphans
        if orphan.get("path")
    ]

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
        rate_limit = float(getattr(inst, "rate_limit", get_decypharr_rate_limit()) or 0.2)

        orphans_store[instance_name] = {
            "orphans": fake_orphan_files,
            "symlinks_list": scan_result.get("symlink_targets", []),
            "api_key": api_key,
            "mount_path": mount_path,
            "cache_path": getattr(inst, "cache_path", "") or "",
            "rate_limit": rate_limit,
            "stats": {
                "sources": scan_result.get("total_torrents", 0),
                "symlinks": scan_result.get("total_symlink_targets", 0),
                "orphans": len(fake_orphan_files),
            },
        }

        created_names.append(instance_name)

    logger.info(
        f"🧩 orphans_store préparé pour {len(created_names)} compte(s) AllDebrid : "
        f"{', '.join(created_names)} — {len(fake_orphan_files)} torrent(s) orphelin(s)"
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
            db.close()
        except Exception as e:
            logger.error(f"💥 Erreur DB orphelins : {e}", exc_info=True)

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
                "action": "deleted",
                "path": "Suppression orphelins",
                "message": f"{total} torrent(s) supprimé(s)",
                "count": total,
                "deleted_torrents": deleted_names,
            })

            try:
                db = SessionLocal()
                db.add(SystemActivity(
                    event="orphans_deleted",
                    action="deleted",
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
                db.close()
            except Exception as e:
                logger.error(f"💥 Erreur DB suppression orphelins : {e}", exc_info=True)

            webhook = config_manager.config.discord_webhook_url

            if webhook:
                sample = "\n".join(f"- {name}" for name in deleted_names)

                asyncio.run(send_discord_message(
                    webhook_url=webhook,
                    title="🗑️ Suppressions AllDebrid",
                    description=sample,
                    action="deleted",
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

def start_light_broken_symlink_monitor():
    """
    🔍 Monitor léger des symlinks brisés.
    Vérifie régulièrement les symlinks déjà connus (symlink_store)
    sans rescanner tout le disque.
    ➕ Ajoute ou met à jour les symlinks brisés dans le store (broken=True).
    🟢 Met à jour le store quand réparés (broken=False).
    ⚙️ Met à jour la base et envoie les événements SSE.
    🧠 Ne s'exécute pas si le conteneur 'decypharr' vient de démarrer (< 2 min).
    ♻️ Se met automatiquement en pause si Decypharr redémarre pendant l’exécution.
    ⚡ Optimisé :
       - vérification parallèle
       - cache par dossier racine de cible sous mount_dirs
       - évite 1 os.path.exists() par symlink quand plusieurs pointent dans le même dossier source
    """
    from routers.secure.symlinks import symlink_store
    import docker
    from datetime import datetime
    from pathlib import Path
    import os
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    client = docker.from_env()

    # ⏳ Attend que le scan initial soit terminé avant de commencer la surveillance
    logger.debug("⏳ En attente du signal de fin de scan initial...")
    initial_scan_done.wait()
    logger.success("🚀 Signal reçu : lancement de la surveillance des symlinks brisés.")

    mount_dirs = [Path(d).resolve() for d in config_manager.config.mount_dirs]

    def get_probe_path(target_str: str | None) -> str | None:
        """
        Réduit le coût des vérifications :
        - si la cible est sous un mount_dir, on teste seulement le premier dossier sous ce mount
        - sinon on teste la cible complète
        """
        if not target_str:
            return None

        try:
            target_path = Path(target_str)
        except Exception:
            return None

        for mount_dir in mount_dirs:
            try:
                rel = target_path.relative_to(mount_dir)
                parts = rel.parts
                if not parts:
                    return str(mount_dir)
                # On teste seulement le dossier racine du torrent / package sous le mount
                return str(mount_dir / parts[0])
            except ValueError:
                continue

        return str(target_path)

    def check_probe_exists(probe_path: str) -> tuple[str, bool]:
        try:
            return probe_path, os.path.exists(probe_path)
        except Exception:
            return probe_path, False

    while True:
        try:
            cycle_start = time.time()

            # 🔒 Protection : attendre que tous les conteneurs decypharr* soient prêts
            wait_for_decypharr_containers(min_uptime_seconds=120)

            broken_now, repaired_now = [], []
            items = list(symlink_store)

            # ✅ État courant des symlinks déjà considérés comme brisés
            already_notified = {
                str(s["symlink"])
                for s in symlink_store
                if s.get("broken", False) or not s.get("target_exists", True)
            }

            # 1) Construire la map symlink -> probe_path
            symlink_to_probe: dict[str, str | None] = {}
            unique_probes: set[str] = set()

            for item in items:
                symlink_path = str(item["symlink"])
                target = item.get("target")
                probe = get_probe_path(target)
                symlink_to_probe[symlink_path] = probe
                if probe:
                    unique_probes.add(probe)

            logger.info(
                f"🔎 Monitor léger: {len(items)} symlink(s), "
                f"{len(unique_probes)} probe(s) uniques à vérifier"
            )

            # 2) Vérification parallèle des probes uniques
            probe_exists_map: dict[str, bool] = {}
            max_workers = min(64, max(8, (os.cpu_count() or 8) * 4))

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(check_probe_exists, probe) for probe in unique_probes]

                for future in as_completed(futures):
                    probe_path, exists = future.result()
                    probe_exists_map[probe_path] = exists

            # 3) Décision broken / repaired à partir du cache
            for item in items:
                symlink_path = str(item["symlink"])
                probe = symlink_to_probe.get(symlink_path)

                # Si pas de probe, on reste prudent : on tente le fallback sur la cible complète
                if not probe:
                    target = item.get("target")
                    try:
                        exists = os.path.exists(target) if target else False
                    except Exception:
                        exists = False
                else:
                    exists = probe_exists_map.get(probe, False)

                if not exists and symlink_path not in already_notified:
                    broken_now.append(item)
                elif exists and symlink_path in already_notified:
                    repaired_now.append(item)

            # === 🔴 Nouveaux symlinks brisés ===
            if broken_now:
                db = SessionLocal()
                added_db = 0

                for s in broken_now:
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
                        f"🧹 Nettoyage cohérence base : {entry.path} n'est plus marqué brisé (suppression DB)."
                    )
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

            total_broken = sum(
                1 for s in symlink_store
                if s.get("broken", False) or not s.get("target_exists", True)
            )
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

        cycle_duration = round(time.time() - cycle_start, 1)
        logger.info(f"⏱️ Monitor léger terminé en {cycle_duration}s — prochain cycle dans 6h")

        time.sleep(6 * 3600)

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
