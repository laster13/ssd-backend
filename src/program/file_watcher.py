import os
import time
import threading
import subprocess
import json
import asyncio
import aiohttp
import uuid
from threading import Event
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
        Gère la création d'un nouveau symlink :
        - détecte le manager et enrichit les métadonnées
        - détecte un éventuel remplacement (symlink supprimé récemment)
        - enregistre dans la base et publie les événements SSE / Discord
        """
        try:
            config = config_manager.config
            links_dirs = [(Path(ld.path).resolve(), ld.manager) for ld in config.links_dirs]
            mount_dirs = [Path(d).resolve() for d in config.mount_dirs]

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

            # --- Base de données ---
            db = SessionLocal()

            # 🔎 Recherche d'une suppression récente du même parent → remplacement
            replaced_from = None
            parent_name = symlink_path.parent.name
            recent_deleted = db.query(SystemActivity).filter(
                SystemActivity.action == "deleted",
                SystemActivity.replaced.is_(None),
                SystemActivity.path.contains(parent_name),
                SystemActivity.created_at >= datetime.utcnow() - timedelta(hours=24)
            ).order_by(SystemActivity.created_at.desc()).first()

            if recent_deleted:
                recent_deleted.replaced = True
                recent_deleted.replaced_at = datetime.utcnow()
                replaced_from = recent_deleted.path
                db.commit()
                logger.info(f"Symlink recréé : remplacement détecté ({recent_deleted.path} → {symlink_path})")

                # 🔔 SSE : signale un remplacement + indique de mettre à jour le “deleted”
                sse_manager.publish_event("symlink_update", {
                    "event": "symlink_replacement",
                    "action": "replaced",
                    "path": str(symlink_path),
                    "old_path": str(recent_deleted.path),
                    "new_path": str(symlink_path),
                    "manager": manager,
                    "id": str(uuid.uuid4()),
                    "replaced": True,
                    "replaced_at": datetime.utcnow().isoformat(),
                    "update_deleted": True  # ✅ permettra au front d’actualiser le statut du deleted
                })

            # 🧩 Vérifie si le symlink existait en "brisé" → le supprimer de la base
            broken_deleted = db.query(SystemActivity).filter(
                SystemActivity.path == str(symlink_path),
                SystemActivity.action == "broken"
            ).delete()
            if broken_deleted:
                db.commit()
                # 🔔 Notifie le frontend du retrait du symlink brisé
                sse_manager.publish_event("symlink_update", {
                    "event": "symlink_repaired",
                    "action": "repaired",
                    "path": str(symlink_path),
                    "manager": manager,
                    "message": f"Symlink réparé détecté et supprimé des entrées brisées : {symlink_path}",
                })
                logger.info(f"🧩 Symlink réparé — suppression des entrées 'broken' en base : {symlink_path}")

            # 💾 Ajout de l'activité “created”
            db.add(SystemActivity(
                event="symlink_added",
                action="created",
                path=str(symlink_path),
                manager=item.get("manager", "unknown"),
                message=f"Symlink ajouté : {symlink_path}",
                extra=item
            ))
            db.commit()
            db.close()
            logger.debug(f"Enregistré en base : {symlink_path}")

            # 🔔 SSE : annonce la création

            sse_manager.publish_event("symlink_update", {
                "event": "symlink_added",
                "action": "created",
                "path": str(symlink_path),
                "item": item,
                "id": str(uuid.uuid4()),
                "count": len(symlink_store),
            })

            # 📨 Buffer Discord
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
        from routers.secure.symlinks import symlink_store
        from integrations.seasonarr.db.database import SessionLocal
        from integrations.seasonarr.db.models import SystemActivity
        import uuid

        removed = False
        removed_item = None

        with self._lock:
            for idx in range(len(symlink_store) - 1, -1, -1):
                if symlink_store[idx].get("symlink") == str(symlink_path):
                    removed_item = symlink_store[idx]
                    del symlink_store[idx]
                    removed = True

        manager = removed_item.get("manager") if removed_item else self._detect_manager(symlink_path)

        if removed:
            sse_manager.publish_event("symlink_update", {
                "id": str(uuid.uuid4()),
                "event": "symlink_removed",
                "action": "deleted",
                "path": str(symlink_path),
                "manager": manager,
                "count": len(symlink_store)
            })
            logger.success(f"➖ Symlink supprimé du cache : {symlink_path}")
        else:
            logger.warning(f"⚠️ Suppression ignorée, symlink non trouvé en cache : {symlink_path}")

        try:
            db = SessionLocal()
            db.add(SystemActivity(
                event="symlink_removed",
                action="deleted",
                path=str(symlink_path),
                manager=manager,
                replaced=None,  # 🔸 marqué comme "non encore remplacé"
                message=f"Symlink supprimé : {symlink_path}"
            ))
            db.commit()
            logger.debug(f"🗄️ SystemActivity enregistré pour suppression : {symlink_path}")
        except Exception as e:
            logger.error(f"💥 Erreur insertion SystemActivity (deleted): {e}", exc_info=True)
        finally:
            db.close()

        with buffer_lock:
            symlink_events_buffer.append({
                "action": "deleted",
                "symlink": str(symlink_path),
                "path": str(symlink_path),
                "manager": manager,
                "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            })
            logger.debug(f"📬 Discord buffer += deleted | size={len(symlink_events_buffer)}")

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
                logger.debug(f"📬 Discord buffer += broken | size={len(symlink_events_buffer)}")

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
    from routers.secure.symlinks import scan_symlinks, symlink_store
    from routers.secure.orphans import scan_instance, delete_all_orphans_job

    logger.info("🛰️ Symlink watcher démarré")
    observers = []
    try:
        config = config_manager.config
        links_dirs = [str(ld.path) for ld in config.links_dirs]

        if not links_dirs:
            logger.warning("⏸️ Aucun links_dirs configuré")
            return

        # 1️⃣ Mise en place des watchers
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

        # 2️⃣ Build Radarr initial
        logger.info("🗄️ Chargement du cache Radarr...")
        threading.Thread(target=lambda: asyncio.run(_build_radarr_index(force=False)), daemon=True).start()

        # 3️⃣ Scan symlinks (après démarrage watchers)
        symlinks_data = scan_symlinks()
        symlink_store.clear()
        symlink_store.extend(symlinks_data)
        logger.success(f"✔️ Scan initial terminé — {len(symlinks_data)} symlinks chargés")

        # 🚨 Détection symlinks brisés (scan initial)
        try:
            broken_symlinks = [s for s in symlinks_data if not s.get("target_exists")]

            # 🔧 Correction : marque les symlinks brisés comme tels dans le store
            for s in broken_symlinks:
                s["broken"] = True
                s["target_exists"] = False
                s["ref_count"] = 0

            if broken_symlinks:
                logger.warning(f"⚠️ {len(broken_symlinks)} symlinks brisés détectés (scan initial)")

                # 💾 Enregistrement DB individuel pour chaque symlink brisé
                for s in broken_symlinks:
                    try:
                        db = SessionLocal()
                        db.add(SystemActivity(
                            event="symlink_broken_live",
                            action="broken",
                            path=s["symlink"],
                            manager=s.get("manager", "unknown"),
                            message=f"Symlink brisé détecté au démarrage : {s['symlink']}",
                            extra={"target": s.get("target")},
                        ))
                        db.commit()
                        db.close()
                    except Exception as e:
                        logger.error(f"💥 Erreur DB ajout symlink brisé (scan initial) : {e}")

                # 🧠 Buffer mémoire (SSE local)
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

                # 📡 SSE vers le frontend
                sse_manager.publish_event("symlink_update", {
                    "event": "broken_symlinks_detected",
                    "action": "broken",
                    "path": "Détection symlinks brisés (scan initial)",
                    "message": f"{len(broken_symlinks)} liens brisés détectés",
                    "count": len(broken_symlinks),
                    "broken_symlinks": [s["symlink"] for s in broken_symlinks],
                })

                # 💬 Discord (par symlink)
                webhook = config_manager.config.discord_webhook_url
                if webhook:
                    for s in broken_symlinks:
                        asyncio.run(send_discord_message(
                            webhook_url=webhook,
                            title="⚠️ Symlink brisé détecté (scan initial)",
                            description=f"Le lien `{s['symlink']}` pointe vers une cible manquante.",
                            action="broken"
                        ))
            else:
                logger.info("✅ Aucun symlink brisé détecté (scan initial).")
        except Exception as e:
            logger.error(f"💥 Erreur détection symlinks brisés (scan initial) : {e}", exc_info=True)

        # 🧹 Scan orphelins initial
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
                            "path": "Scan orphelins initial",
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
                            logger.error(f"💥 Erreur DB orphelins initiaux : {e}")

                else:
                    logger.info("🧩 Aucun fichier orphelin trouvé — pas de message Discord ni DB.")

                # 🧪 Suppression orphelins
                try:
                    logger.info("🧪 Suppression des orphelins post-rescan...")
                    result_delete = asyncio.run(delete_all_orphans_job(dry_run=False))
                    logger.success("✅ Suppression orphelins initiale terminée")

                    deleted_names = []

                    if isinstance(result_delete, dict):
                        deleted_names = (
                            result_delete.get("deleted_torrents")
                            or result_delete.get("deleted")
                            or result_delete.get("removed")
                            or []
                        )

                        logs = result_delete.get("logs", [])
                        for line in logs:
                            if "→ supprimé" in line or "deleted" in line.lower():
                                name = line.split("]")[-1].split("→")[0].strip()
                                if name and name not in deleted_names:
                                    deleted_names.append(name)

                    webhook = config_manager.config.discord_webhook_url

                    # 🧩 ✅ Envoi uniquement si au moins un torrent supprimé
                    if webhook and deleted_names:
                        sample = "\n".join(f"- {name}" for name in deleted_names)
                        asyncio.run(send_discord_message(
                            webhook_url=webhook,
                            title="🗑️ Suppressions AllDebrid",
                            description=sample,
                            action="deleted"
                        ))
                        logger.info(f"📢 Notification Discord envoyée ({len(deleted_names)} suppression(s)).")

                        # 🧩 SSE pour frontend
                        sse_manager.publish_event("symlink_update", {
                            "event": "orphans_deleted",
                            "action": "deleted",
                            "path": "Suppression orphelins initiale",
                            "message": f"{len(deleted_names)} torrents supprimés (initiale)",
                            "count": len(deleted_names),
                            "deleted_torrents": deleted_names,
                        })
                        logger.info("📡 Événement SSE 'deleted' envoyé au frontend avec la liste complète")

                        # 💾 Enregistrement DB suppression orphelins initiale
                        try:
                            db = SessionLocal()
                            db.add(SystemActivity(
                                event="orphans_deleted",
                                action="deleted",
                                path="Suppression orphelins initiale",
                                manager="alldebrid",
                                message=f"{len(deleted_names)} torrents supprimés",
                                extra={"deleted_torrents": deleted_names},
                            ))
                            db.commit()
                            db.close()
                            logger.debug("💾 Activité DB enregistrée : suppression orphelins initiale")
                        except Exception as e:
                            logger.error(f"💥 Erreur DB suppression orphelins initiale : {e}")

                    elif not deleted_names:
                        logger.info("🧩 Aucun torrent supprimé — aucune activité créée ni message envoyé.")
                    else:
                        logger.debug("🧩 Aucun webhook configuré, suppression silencieuse.")

                except Exception as e:
                    logger.error(f"💥 Erreur suppression orphelins initiale : {e}", exc_info=True)

            else:
                logger.info("ℹ️ Aucun compte AllDebrid configuré, scan orphelins ignoré.")
        except Exception as e:
            logger.error(f"💥 Erreur durant le scan orphelins initial : {e}")

        # 🔔 SSE fin de scan initial
        sse_manager.publish_event("symlink_update", {
            "event": "initial_scan",
            "action": "scan",
            "path": "Scan initial des symlinks",
            "message": "Scan initial terminé",
            "count": len(symlinks_data)
        })

        # ✅ Signale que le scan initial est terminé (le monitor léger peut démarrer)
        initial_scan_done.set()
        logger.info("🔔 Signal envoyé : scan initial terminé")


        # 4️⃣ Boucle périodique
        scan_interval = 86400  # 6h
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

                # 🚨 Détection symlinks brisés (scan périodique)
                try:
                    broken_symlinks = [s for s in symlinks_data if not s.get("target_exists")]
                    if broken_symlinks:
                        logger.warning(f"⚠️ {len(broken_symlinks)} symlinks brisés détectés (scan périodique)")

                        for s in broken_symlinks:
                            db = SessionLocal()
                            db.add(SystemActivity(
                                event="symlink_broken_live",
                                action="broken",
                                path=s["symlink"],
                                manager=s.get("manager", "unknown"),
                                message=f"Symlink brisé détecté (scan périodique) : {s['symlink']}",
                                extra={"target": s.get("target")},
                            ))
                            db.commit()
                            db.close()

                        sse_manager.publish_event("symlink_update", {
                            "event": "broken_symlinks_periodic",
                            "action": "broken",
                            "path": "Détection symlinks brisés (scan périodique)",
                            "message": f"{len(broken_symlinks)} liens brisés détectés",
                            "count": len(broken_symlinks),
                            "broken_symlinks": [s["symlink"] for s in broken_symlinks],
                        })

                        webhook = config_manager.config.discord_webhook_url
                        if webhook:
                            for s in broken_symlinks:
                                asyncio.run(send_discord_message(
                                    webhook_url=webhook,
                                    title="⚠️ Symlink brisé détecté (périodique)",
                                    description=f"Le lien `{s['symlink']}` pointe vers une cible manquante.",
                                    action="broken"
                                ))
                    else:
                        logger.info("✅ Aucun symlink brisé détecté (scan périodique).")
                except Exception as e:
                    logger.error(f"💥 Erreur détection symlinks brisés (scan périodique) : {e}", exc_info=True)

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

def start_replacement_cleanup_task(interval_hours: int = 6, expiry_hours: int = 12):
    """
    🧹 Tâche périodique :
    - Marque comme "non remplacés" les symlinks supprimés non recréés après X heures.
    - ✅ Corrige aussi les anciens supprimés qui ont été recréés bien plus tard.
    """
    def cleanup_loop():
        logger.info("🧠 Tâche cleanup (replacement) démarrée...")
        while True:
            try:
                from integrations.seasonarr.db.database import SessionLocal
                from integrations.seasonarr.db.models import SystemActivity
                from sqlalchemy import and_, or_

                db = SessionLocal()
                now = datetime.utcnow()
                cutoff = now - timedelta(hours=expiry_hours)

                # 1️⃣ Récupère tous les symlinks supprimés non remplacés (ou marqués False)
                deleted_entries = db.query(SystemActivity).filter(
                    SystemActivity.action == "deleted",
                    or_(
                        SystemActivity.replaced.is_(None),
                        SystemActivity.replaced.is_(False)
                    )
                ).all()

                updated = 0
                marked_non_replaced = 0

                for deleted in deleted_entries:
                    parent_name = Path(deleted.path).parent.name
                    deleted_time = deleted.created_at or now - timedelta(days=999)

                    # 2️⃣ Cherche une création postérieure du même parent
                    created_match = db.query(SystemActivity).filter(
                        SystemActivity.action == "created",
                        SystemActivity.path.contains(parent_name),
                        SystemActivity.created_at > deleted_time
                    ).order_by(SystemActivity.created_at.asc()).first()

                    if created_match:
                        deleted.replaced = True
                        deleted.replaced_at = created_match.created_at
                        updated += 1

                        # 📡 Émet un événement SSE pour mise à jour du front
                        try:
                            from program.managers.sse_manager import sse_manager
                            sse_manager.publish_event("symlink_update", {
                                "event": "symlink_replacement_cleanup",
                                "action": "replaced",
                                "path": deleted.path,
                                "manager": deleted.manager,
                                "replaced_at": created_match.created_at.isoformat(),
                                "message": f"Rattrapage remplacement tardif détecté ({parent_name})"
                            })
                        except Exception:
                            pass

                    # 3️⃣ Si trop ancien sans recréation → considéré définitivement non remplacé
                    elif deleted.created_at < cutoff:
                        deleted.replaced = False
                        deleted.replaced_at = now
                        marked_non_replaced += 1

                db.commit()
                db.close()

                if updated or marked_non_replaced:
                    logger.info(
                        f"♻️ Tâche cleanup Rapport Activité : {updated} remplacés corrigés, "
                        f"{marked_non_replaced} marqués non remplacés."
                    )

            except Exception as e:
                logger.error(f"💥 Erreur tâche nettoyage symlinks : {e}", exc_info=True)

            # 🕒 Pause avant la prochaine itération
            time.sleep(interval_hours * 3600)

    threading.Thread(target=cleanup_loop, daemon=True).start()

def start_light_broken_symlink_monitor(interval_minutes=5):
    """
    🔍 Monitor léger des symlinks brisés.
    Vérifie régulièrement les symlinks déjà connus (symlink_store)
    sans rescanner tout le disque.
    ➕ Ajoute uniquement les nouveaux symlinks brisés au store
       pour qu’ils soient visibles côté frontend.
    🚫 Ne modifie pas le store pour les réparations.
    ⚙️ Met à jour la base et envoie les événements SSE.
    """
    from routers.secure.symlinks import symlink_store

    # ⏳ Attend que le scan initial soit terminé avant de commencer la surveillance
    logger.debug("⏳ En attente du signal de fin de scan initial...")
    initial_scan_done.wait()
    logger.success("🚀 Signal reçu : lancement de la surveillance des symlinks brisés.")

    already_notified = {
        s["symlink"]
        for s in symlink_store
        if not s.get("target_exists", True)
    }

    while True:
        try:
            broken_now, repaired_now = [], []
            items = list(symlink_store)

            for i in items:
                symlink_path = Path(i["symlink"])
                if not symlink_path.exists() and not symlink_path.is_symlink():
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

            # === 🔴 nouveaux symlinks brisés ===
            if broken_now:
                db = SessionLocal()
                for s in broken_now:
                    db.add(SystemActivity(
                        event="symlink_broken_light",
                        action="broken",
                        path=s["symlink"],
                        manager=s.get("manager", "unknown"),
                        message=f"Symlink brisé détecté (monitor léger) : {s['symlink']}",
                        extra={"target": s.get("target")},
                    ))
                db.commit()
                db.close()

                for s in broken_now:
                    symlink_store.append({
                        "symlink": s["symlink"],
                        "target": s.get("target"),
                        "manager": s.get("manager", "unknown"),
                        "broken": True,
                        "target_exists": False,
                        "ref_count": 0,
                    })

                sse_manager.publish_event("symlink_update", {
                    "event": "broken_symlinks_light",
                    "action": "broken",
                    "path": "Détection symlinks brisés (monitor léger)",
                    "message": f"{len(broken_now)} liens brisés détectés",
                    "count": len(broken_now),
                    "broken_symlinks": [s["symlink"] for s in broken_now],
                })
                logger.warning(f"⚠️ {len(broken_now)} nouveaux symlinks brisés détectés (monitor léger)")

            # === 🟢 symlinks réparés ===
            if repaired_now:
                db = SessionLocal()
                for s in repaired_now:
                    db.query(SystemActivity).filter(
                        SystemActivity.path == s["symlink"],
                        SystemActivity.action == "broken"
                    ).delete()
                db.commit()
                db.close()

                sse_manager.publish_event("symlink_update", {
                    "event": "broken_symlinks_light",
                    "action": "repaired",
                    "path": "Réparation symlinks (monitor léger)",
                    "message": f"{len(repaired_now)} liens réparés détectés",
                    "count": len(repaired_now),
                    "repaired_symlinks": [s["symlink"] for s in repaired_now],
                })
                logger.info(f"🧩 {len(repaired_now)} symlinks réparés détectés (Suppression db)")

            # === Logs lisibles ===
            if broken_now:
                logger.warning("╭───────────────────────────────────────────────")
                logger.warning(f"│ ⚠️  {len(broken_now)} nouveaux symlinks brisés :")
                for s in broken_now:
                    logger.warning(f"│   • {s['symlink']}")
                    logger.warning(f"│     ↳ {s.get('target') or '❌ (inconnu)'}")
                logger.warning("╰───────────────────────────────────────────────")

            elif repaired_now:
                logger.info("╭───────────────────────────────────────────────")
                logger.info(f"│ 🧩  {len(repaired_now)} symlinks réparés :")
                for s in repaired_now:
                    logger.info(f"│   • {s['symlink']}")
                    logger.info(f"│     ↳ {s.get('target') or '🎯 (cible retrouvée)'}")
                logger.info("╰───────────────────────────────────────────────")

        except Exception as e:
            logger.exception(f"💥 Erreur dans le monitor léger : {e}")

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
    threading.Thread(target=start_light_broken_symlink_monitor, args=(5,), daemon=True).start()

