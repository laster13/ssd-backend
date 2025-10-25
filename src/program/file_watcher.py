import os
import time
import threading
import subprocess
import json
import asyncio
import aiohttp
import uuid
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
                logger.debug(f"Buffer Discord += created | taille={len(symlink_events_buffer)}")

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
                        logger.debug(f"💾 Enregistré en base (scan initial, symlink brisé) : {s['symlink']}")
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
    🧹 Tâche périodique : marque les symlinks supprimés non remplacés après un certain délai.
    - interval_hours → fréquence de vérification
    - expiry_hours → délai après lequel on considère un symlink comme définitivement non remplacé
    """
    def cleanup_loop():
        while True:
            try:
                from integrations.seasonarr.db.database import SessionLocal
                from integrations.seasonarr.db.models import SystemActivity

                db = SessionLocal()
                cutoff = datetime.utcnow() - timedelta(hours=expiry_hours)

                old_deleted = db.query(SystemActivity).filter(
                    SystemActivity.action == "deleted",
                    SystemActivity.replaced.is_(None),
                    SystemActivity.created_at < cutoff
                ).all()

                if old_deleted:
                    now = datetime.utcnow()
                    for entry in old_deleted:
                        entry.replaced = False
                        entry.replaced_at = now  # ✅ On enregistre aussi la date du marquage
                    db.commit()
                    logger.info(f"🕒 {len(old_deleted)} symlinks marqués comme non remplacés (> {expiry_hours}h).")
                else:
                    logger.debug("✅ Aucun symlink à marquer comme non remplacé.")

                db.close()
            except Exception as e:
                logger.error(f"💥 Erreur tâche nettoyage symlinks non remplacés : {e}", exc_info=True)

            time.sleep(interval_hours * 3600)

    threading.Thread(target=cleanup_loop, daemon=True).start()

def start_broken_symlink_monitor(interval_seconds=600):
    """Vérifie périodiquement si des symlinks en cache sont devenus brisés."""
    def loop():
        from routers.secure.symlinks import symlink_store
        handler = SymlinkEventHandler()
        while True:
            try:
                broken_count = 0
                for s in list(symlink_store):
                    path = Path(s["symlink"])
                    if path.is_symlink():
                        try:
                            path.resolve(strict=True)
                        except FileNotFoundError:
                            handler._handle_broken(path)
                            broken_count += 1
                if broken_count > 0:
                    logger.info(f"🔍 {broken_count} symlinks brisés détectés (vérif périodique).")
            except Exception as e:
                logger.error(f"💥 Erreur vérif symlinks brisés : {e}", exc_info=True)
            time.sleep(interval_seconds)

    threading.Thread(target=loop, daemon=True).start()

def start_all_watchers():
    logger.info("🚀 Lancement des watchers YAML + Symlink...")
    threading.Thread(target=start_yaml_watcher, daemon=True).start()
    threading.Thread(target=start_symlink_watcher, daemon=True).start()
    start_discord_flusher()
    start_replacement_cleanup_task()
    start_broken_symlink_monitor(interval_seconds=600)
