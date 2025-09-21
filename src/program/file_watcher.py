import os
import time
import threading
import subprocess
import json
import asyncio
import aiohttp
from datetime import datetime
from pathlib import Path
from loguru import logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.services.fonctions_arrs import RadarrService
from program.utils.text_utils import normalize_name, clean_movie_name
from program.settings.manager import config_manager
from program.managers.sse_manager import sse_manager
from .json_manager import update_json_files
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

        if event.event_type == "created" and path.is_symlink():
            self._handle_created(path)
        elif event.event_type == "deleted":
            self._handle_deleted(path)

    def _handle_created(self, symlink_path: Path):
        try:
            config = config_manager.config
            links_dirs = [(Path(ld.path).resolve(), ld.manager) for ld in config.links_dirs]

            root, manager = None, "unknown"
            for ld, mgr in links_dirs:
                if str(symlink_path).startswith(str(ld)):
                    root, manager = ld, mgr
                    break
            if not root:
                return

            try:
                target_path = symlink_path.resolve(strict=True)
            except FileNotFoundError:
                target_path = symlink_path.resolve(strict=False)

            full_target = str(target_path)

            try:
                relative_path = str(symlink_path.resolve().relative_to(root))
            except Exception:
                relative_path = str(symlink_path).replace(str(root) + "/", "")

            stat = symlink_path.lstat()
            created_at = datetime.fromtimestamp(stat.st_mtime).isoformat()

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

            if manager == "radarr":
                extra = enrich_from_radarr_index(symlink_path)
                if extra:
                    item.update(extra)

            with self._lock:
                from routers.secure.symlinks import symlink_store
                symlink_store.append(item)

            sse_manager.publish_event("symlink_update", {
                "event": "symlink_added",
                "item": item,
                "count": len(symlink_store),
            })

            # 📨 Alimente le buffer Discord
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
                })
                logger.debug(f"📬 Discord buffer += created | size={len(symlink_events_buffer)}")

            logger.success(f"➕ Symlink enrichi ajouté au cache : {symlink_path}")

        except Exception as e:
            logger.error(f"💥 Erreur ajout symlink {symlink_path}: {e}", exc_info=True)


    def _handle_deleted(self, symlink_path: Path):
        from routers.secure.symlinks import symlink_store

        removed = False
        with self._lock:
            for idx in range(len(symlink_store) - 1, -1, -1):
                if symlink_store[idx].get("symlink") == str(symlink_path):
                    del symlink_store[idx]
                    removed = True

        if removed:
            sse_manager.publish_event("symlink_update", {
                "event": "symlink_removed",
                "path": str(symlink_path),
                "count": len(symlink_store)
            })
            logger.success(f"➖ Symlink supprimé du cache : {symlink_path}")
        else:
            logger.warning(f"⚠️ Suppression ignorée, symlink non trouvé en cache : {symlink_path}")

        # 📨 Alimente le buffer Discord (même si non trouvé, on loggue la demande de delete)
        with buffer_lock:
            symlink_events_buffer.append({
                "action": "deleted",
                "symlink": str(symlink_path),
                "path": str(symlink_path),
                "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            })
            logger.debug(f"📬 Discord buffer += deleted | size={len(symlink_events_buffer)}")

    def _detect_manager(self, path: Path) -> str:
        for ld in config_manager.config.links_dirs:
            if str(path).startswith(str(Path(ld.path).resolve())):
                return ld.manager
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
    from routers.secure.symlinks import scan_symlinks, symlink_store
    logger.info("🛰️ Symlink watcher démarré")
    observers = []
    try:
        config = config_manager.config
        links_dirs = [str(ld.path) for ld in config.links_dirs]

        if not links_dirs:
            logger.warning("⏸️ Aucun links_dirs configuré")
            return

        # 1️⃣ Build Radarr initial
        logger.info("🗄️ Chargement initial du cache Radarr...")
        asyncio.run(_build_radarr_index(force=False))

        # 2️⃣ Mise en place des watchers (⚡ déplacé ici)
        for dir_path in links_dirs:
            path = Path(dir_path)
            if not path.exists():
                logger.warning(f"⚠️ Dossier symlink introuvable : {path}")
                continue

            observer = Observer()
            observer.schedule(SymlinkEventHandler(), path=str(path), recursive=True)
            observer.start()
            observers.append(observer)
            logger.info(f"📍 Symlink watcher actif sur {path.resolve()}")

        # 3️⃣ Scan symlinks (après démarrage watchers)
        symlinks_data = scan_symlinks()
        symlink_store.clear()
        symlink_store.extend(symlinks_data)
        logger.success(f"✔️ Scan initial terminé — {len(symlinks_data)} symlinks chargés")

        # 🚨 Détection symlinks brisés (scan initial)
        broken_symlinks = [s for s in symlinks_data if not s.get("target_exists")]
        if broken_symlinks:
            logger.warning(f"⚠️ {len(broken_symlinks)} symlinks brisés détectés (scan initial)")
            with buffer_lock:
                for s in broken_symlinks:
                    symlink_events_buffer.append({
                        "action": "broken",
                        "symlink": s["symlink"],
                        "path": s["symlink"],
                        "target": s.get("target"),
                        "manager": s.get("manager"),
                        "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    })

                    # 🚨 Notification instantanée Discord
                    webhook = config_manager.config.discord_webhook_url
                    if webhook:
                        asyncio.run(send_discord_message(
                            webhook_url=webhook,
                            title="⚠️ Symlink brisé détecté",
                            description=f"Le lien `{s['symlink']}` pointe vers une cible manquante.",
                            action="broken"
                        ))

        sse_manager.publish_event("symlink_update", {
            "event": "initial_scan",
            "message": "Scan initial terminé",
            "count": len(symlinks_data)
        })

        # 4️⃣ Boucle de fond (rescans périodiques)
        scan_interval = 3600  # 1h
        last_scan = time.time()

        while True:
            logger.debug("📡 Symlink thread actif...")

            if time.time() - last_scan >= scan_interval:
                logger.info("🕒 Rebuild Radarr périodique lancé...")
                asyncio.run(_build_radarr_index(force=False))

                symlinks_data = scan_symlinks()
                with threading.Lock():
                    symlink_store.clear()
                    symlink_store.extend(symlinks_data)

                # 🚨 Détection symlinks brisés (rescan périodique)
                broken_symlinks = [s for s in symlinks_data if not s.get("target_exists")]
                if broken_symlinks:
                    logger.warning(f"⚠️ {len(broken_symlinks)} symlinks brisés détectés (rescan)")
                    with buffer_lock:
                        for s in broken_symlinks:
                            symlink_events_buffer.append({
                                "action": "broken",
                                "symlink": s["symlink"],
                                "path": s["symlink"],
                                "target": s.get("target"),
                                "manager": s.get("manager"),
                                "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                            })

                            # 🚨 Notification instantanée Discord
                            webhook = config_manager.config.discord_webhook_url
                            if webhook:
                                asyncio.run(send_discord_message(
                                    webhook_url=webhook,
                                    title="⚠️ Symlink brisé détecté",
                                    description=f"Le lien `{s['symlink']}` pointe vers une cible manquante.",
                                    action="broken"
                                ))

                sse_manager.publish_event("symlink_update", {
                    "event": "periodic_scan",
                    "message": "Rescan automatique",
                    "count": len(symlinks_data)
                })

                logger.success(f"🔄 Scan périodique exécuté — {len(symlinks_data)} symlinks")
                last_scan = time.time()

            time.sleep(30)

    except KeyboardInterrupt:
        logger.info("⏹️ Arrêt du Symlink watcher")
    except Exception as e:
        logger.exception(f"💥 Erreur watcher symlink : {e}")

    finally:
        for obs in observers:
            obs.stop()
            obs.join()
        logger.warning("✅ Watcher arrêté")

def start_all_watchers():
    logger.info("🚀 Lancement des watchers YAML + Symlink...")
    threading.Thread(target=start_yaml_watcher, daemon=True).start()
    threading.Thread(target=start_symlink_watcher, daemon=True).start()
    start_discord_flusher()
