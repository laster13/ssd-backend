from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from program.utils.discord_notifier import send_discord_message
from loguru import logger
from program.settings.manager import config_manager
from program.settings.orphans import OrphanScanResult, OrphanScanStats, OrphanActions
from pathlib import Path
from datetime import datetime
import asyncio
import subprocess
import os
import aiohttp
import shutil



router = APIRouter(
    prefix="/orphans",
    tags=["Orphans"],
)

# ═══════════════════════════════════════════════════════════
# CONFIGURATION GLOBALE
# ═══════════════════════════════════════════════════════════

orphans_store = {}
ALLDEBRID_API_BASE = "https://api.alldebrid.com/v4.1"


# ═══════════════════════════════════════════════════════════
# UTILITAIRES FD
# ═══════════════════════════════════════════════════════════

async def run_fd_command(cmd: str) -> list[str]:
    """Exécute une commande shell fd/readlink et renvoie les lignes en sortie."""
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await process.communicate()
    lines = stdout.decode().strip().split("\n")
    return [os.path.normpath(l) for l in lines if l.strip()]


# ═══════════════════════════════════════════════════════════
# SCAN DES FICHIERS MOUNT ET SYMLINKS
# ═══════════════════════════════════════════════════════════

def list_mount_files(mount_path: Path) -> list[str]:
    """Liste tous les fichiers d’un mount via fd (ou fallback Python)."""
    if not mount_path.exists():
        raise FileNotFoundError(f"Mount introuvable : {mount_path}")

    try:
        result = subprocess.run(
            ["fd", "-t", "f", ".", str(mount_path)],
            capture_output=True, text=True, check=True
        )
        files = [os.path.normpath(f) for f in result.stdout.strip().split("\n") if f]
        logger.debug(f"✅ [fd] {len(files)} fichiers trouvés dans {mount_path}")
        return files
    except (FileNotFoundError, subprocess.SubprocessError):
        logger.warning("⚠️ fd non disponible, fallback Python.")
        return [str(p) for p in mount_path.rglob("*") if p.is_file()]


async def list_symlink_targets_for_dir(link_dir: Path, mount_path: Path) -> list[str]:
    """Liste les cibles de symlinks dans un dossier spécifique."""
    if not link_dir.exists():
        return []

    cmd = f"fd -t l . '{link_dir}' -0 | xargs -0 readlink -f | grep '^{mount_path}'"
    targets = await run_fd_command(cmd)
    logger.debug(f"📁 {link_dir}: {len(targets)} symlinks valides trouvés")
    return targets


async def list_symlink_targets(links_dirs: list, mount_path: Path) -> list[str]:
    """Liste tous les symlinks de tous les dossiers en parallèle."""
    tasks = []
    for entry in links_dirs:
        d = Path(entry.path)
        tasks.append(list_symlink_targets_for_dir(d, mount_path))

    results = await asyncio.gather(*tasks)
    all_targets = [t for sublist in results for t in sublist]
    logger.debug(f"✅ Total global: {len(all_targets)} symlinks valides")
    return sorted(set(all_targets))


def find_orphans(mount_files: list[str], symlink_targets: list[str]) -> list[str]:
    """
    Compare les listes pour trouver les fichiers orphelins.
    ✅ Corrigé : compare les chemins physiques (realpath) au lieu des noms texte.
    Cela évite les faux positifs quand les symlinks ont été renommés (ex: Misfits (2009) - S03E01).
    """
    norm_mount = {os.path.normpath(os.path.realpath(f)) for f in mount_files}
    norm_symlinks = {os.path.normpath(os.path.realpath(f)) for f in symlink_targets}

    # Différence : fichiers présents dans le mount mais sans lien symbolique réel
    orphans = sorted(norm_mount - norm_symlinks)

    logger.debug(f"🧠 Détection orphelins: {len(orphans)} fichiers non liés après normalisation")

    # 🔍 Liste détaillée des orphelins pour le debug
    if orphans:
        logger.warning("📄 Liste des fichiers orphelins détectés :")
        for f in orphans[:50]:  # Limite à 50 pour éviter le spam
            logger.warning(f"   → {f}")
        if len(orphans) > 50:
            logger.warning(f"   ... et {len(orphans) - 50} autres fichiers orphelins.")

    return orphans

# ═══════════════════════════════════════════════════════════
# SCAN D’UNE INSTANCE
# ═══════════════════════════════════════════════════════════

async def scan_instance(instance) -> dict:
    """Scanne une instance AllDebrid et détecte les orphelins."""
    name = instance.name
    mount_path = Path(instance.mount_path)
    api_key = instance.api_key
    rate_limit = instance.rate_limit
    cache_path = getattr(instance, "cache_path", "/app/cache")  # ✅ ajouté ici

    start = datetime.utcnow()
    logger.info(f"🔍 Scan instance: {name}")

    try:
        mount_files = await asyncio.to_thread(list_mount_files, mount_path)
        symlink_targets = await list_symlink_targets(config_manager.config.links_dirs, mount_path)
        orphans = find_orphans(mount_files, symlink_targets)

        duration = (datetime.utcnow() - start).total_seconds()

        result = {
            "scan_date": datetime.utcnow().isoformat() + "Z",
            "instance": name,
            "mount_path": str(mount_path),
            "duration_seconds": duration,
            "stats": {
                "sources": len(mount_files),
                "symlinks": len(symlink_targets),
                "orphans": len(orphans),
            },
            "orphans": orphans,
            "actions": {
                "auto_delete": getattr(config_manager.config.orphan_manager, "auto_delete", False),
                "deletable": len(orphans)
            }
        }

        # ✅ stockage complet des infos pour suppression future
        orphans_store[name] = {
            "orphans": orphans,
            "symlinks_list": symlink_targets,   # ✅ ajout essentiel ici
            "api_key": api_key,
            "mount_path": str(mount_path),
            "cache_path": cache_path,
            "rate_limit": rate_limit,
            "stats": result["stats"],
        }

        logger.info(f"✅ Scan terminé pour {name}: {len(orphans)} orphelins détectés")
        logger.info(f"⏱️ Durée: {duration:.2f}s")
        return result

    except Exception as e:
        logger.error(f"Erreur durant le scan de {name}: {e}")
        return {"instance": name, "error": str(e)}


# ═══════════════════════════════════════════════════════════
# ROUTES API
# ═══════════════════════════════════════════════════════════

@router.get("/instances")
async def get_instances():
    """Retourne les instances AllDebrid configurées."""
    return getattr(config_manager.config, "alldebrid_instances", [])


@router.post("/scan")
async def scan_all_instances(background_tasks: BackgroundTasks):
    """Lance le scan sur toutes les instances AllDebrid actives (lecture seule)."""
    instances = getattr(config_manager.config, "alldebrid_instances", [])
    if not instances:
        raise HTTPException(status_code=400, detail="Aucune instance AllDebrid configurée.")

    active_instances = [i for i in instances if getattr(i, "enabled", True)]
    if not active_instances:
        raise HTTPException(status_code=400, detail="Aucune instance AllDebrid active trouvée.")

    sorted_instances = sorted(active_instances, key=lambda i: getattr(i, "priority", 1))

    start_time = datetime.utcnow()
    results = []
    for inst in sorted_instances:
        res = await scan_instance(inst)
        results.append(res)

    total_duration = (datetime.utcnow() - start_time).total_seconds()
    logger.info(f"⏱️ Scan global terminé en {total_duration:.2f}s")

    return {
        "scan_date": datetime.utcnow().isoformat() + "Z",
        "duration_seconds": total_duration,
        "results": results
    }


@router.get("/report")
async def get_last_report():
    """Retourne le dernier scan complet stocké en mémoire."""
    if not orphans_store:
        raise HTTPException(status_code=404, detail="Aucun rapport trouvé.")
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "instances": list(orphans_store.keys()),
        "details": orphans_store,
    }


@router.get("/stats")
async def get_stats():
    """Retourne les statistiques globales du dernier scan."""
    if not orphans_store:
        raise HTTPException(status_code=404, detail="Aucun scan disponible.")
    total_sources = sum(v["stats"]["sources"] for v in orphans_store.values())
    total_symlinks = sum(v["stats"]["symlinks"] for v in orphans_store.values())
    total_orphans = sum(v["stats"]["orphans"] for v in orphans_store.values())
    return {
        "scan_date": datetime.utcnow().isoformat() + "Z",
        "global_stats": {
            "sources": total_sources,
            "symlinks": total_symlinks,
            "orphans": total_orphans
        }
    }

async def perform_deletion(instance: str, dry_run: bool = False):
    """
    Supprime (ou simule la suppression) des orphelins AllDebrid,
    en évitant de supprimer un torrent complet si un seul fichier est encore lié.
    Supprime aussi les fichiers JSON du cache Decypharr et les fichiers locaux correspondants.
    """
    data = orphans_store.get(instance)
    if not data:
        logger.error(f"<red>[{instance}] Instance introuvable dans orphans_store</red>")
        return

    dry_run = False
    orphans = data.get("orphans", [])
    api_key = data.get("api_key")
    mount_path = data.get("mount_path")
    rate_limit = float(data.get("rate_limit", 0.5))
    cache_path = data.get("cache_path", "/app/cache")

    # Liste complète des symlinks connus pour cette instance (si dispo)
    symlinks_list = data.get("symlinks_list", [])

    dry_label = "DRY-RUN" if dry_run else "SUPPRESSION"
    logger.info(f"🧪 [{instance}] Démarrage {dry_label} en tâche de fond...")

    # --- Extraction du dossier racine du torrent à partir du chemin du fichier ---
    def extract_torrent(file_path: str) -> str | None:
        try:
            rel_path = os.path.relpath(file_path, mount_path)
            return rel_path.split(os.sep, 1)[0]
        except Exception:
            return None

    # --- On ne supprime un torrent que si aucun fichier n'a de symlink valide ---
    all_torrents = sorted(set(filter(None, [extract_torrent(f) for f in orphans])))
    torrents_to_delete = []

    for torrent in all_torrents:
        torrent_dir = os.path.join(mount_path, torrent)
        if not os.path.exists(torrent_dir):
            torrents_to_delete.append(torrent)
            continue

        # Vérifie si un fichier de ce torrent est encore lié
        files_in_torrent = [str(p) for p in Path(torrent_dir).rglob("*") if p.is_file()]
        still_linked = False
        for file in files_in_torrent:
            real_file = os.path.normpath(os.path.realpath(file))
            if any(real_file == os.path.normpath(os.path.realpath(s)) for s in symlinks_list):
                still_linked = True
                break

        if still_linked:
            logger.debug(f"🧩 Torrent conservé (fichiers encore liés) : {torrent}")
        else:
            torrents_to_delete.append(torrent)

    if not torrents_to_delete:
        logger.info(f"<green>🧱[{instance}] Aucun torrent à supprimer (tous ont des liens valides).</green>")
        return

    ok, nf, err = 0, 0, 0
    decypharr_data = []  # [(nom, id)]
    actually_deleted = []  # ✅ torrents réellement supprimés

    # --- Connexion à AllDebrid ---
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{ALLDEBRID_API_BASE}/magnet/status",
            headers={"Authorization": f"Bearer {api_key}"}
        ) as resp:
            try:
                data_status = await resp.json()
            except Exception:
                logger.error(f"<red>[{instance}] Erreur de décodage JSON sur magnet/status</red>")
                return

            if data_status.get("status") != "success":
                logger.error(f"<cyan>[{instance}] Erreur API magnet/status: {data_status}</cyan>")
                return

            magnets = data_status.get("data", {}).get("magnets", [])

        def find_magnet_info(name: str) -> dict | None:
            for m in magnets:
                if m.get("filename") == name or m.get("name") == name:
                    return {"id": str(m["id"]), "name": m.get("filename") or m.get("name")}
            for m in magnets:
                if m.get("filename", "").startswith(name):
                    return {"id": str(m["id"]), "name": m.get("filename") or m.get("name")}
            return None

        for torrent in torrents_to_delete:
            info = find_magnet_info(torrent)
            if not info:
                nf += 1
                logger.warning(f"<yellow>⚠️ [AllDebrid] {torrent} introuvable dans la liste des magnets</yellow>")
                await asyncio.sleep(rate_limit)
                continue

            magnet_id = info["id"]
            magnet_name = info["name"]

            if dry_run:
                logger.info(f"<green>🧱 [AllDebrid] {magnet_name} - ID: {magnet_id} → simulé</green>")
                await asyncio.sleep(rate_limit)
                continue

            try:
                async with session.post(
                    f"{ALLDEBRID_API_BASE}/magnet/delete",
                    headers={"Authorization": f"Bearer {api_key}"},
                    data={"id": magnet_id},
                ) as del_resp:
                    del_json = await del_resp.json()
                    if del_json.get("status") == "success":
                        ok += 1
                        decypharr_data.append((magnet_name, magnet_id))
                        actually_deleted.append(magnet_name)  # ✅ confirmé supprimé
                        logger.info(f"<cyan>   [AllDebrid] {magnet_name} - ID: {magnet_id} → supprimé</cyan>")
                    else:
                        err += 1
                        msg = del_json.get("error", {}).get("message", "Erreur inconnue")
                        logger.warning(f"<yellow>⚠️ [AllDebrid] Échec suppression {magnet_name} : {msg}</yellow>")
            except Exception as e:
                logger.error(f"<red>[{instance}] ✗ Exception suppression AllDebrid: {e}</red>")
                err += 1

            await asyncio.sleep(rate_limit)

    # --- Étape 2 : suppression du cache Decypharr ---
    if not os.path.isdir(cache_path):
        logger.warning(f"<yellow>⚠️ [Decypharr] Dossier cache introuvable : {cache_path}</yellow>")
    else:
        for name, decy_id in decypharr_data:
            json_file = os.path.join(cache_path, f"{decy_id}.json")
            if os.path.exists(json_file):
                try:
                    os.remove(json_file)
                    actually_deleted.append(name)  # ✅ suppression locale confirmée
                    logger.info(f"<fg #FFCCFF>🧹 [Decypharr] {name} - ID: {decy_id} → supprimé</fg #FFCCFF>")
                except Exception as e:
                    logger.error(f"<red>❌ [Decypharr] Erreur suppression {json_file}: {e}</red>")
            else:
                logger.debug(f"[Decypharr] {name} - ID: {decy_id} → non trouvé")

    # --- Étape 3 : suppression locale des fichiers ---
    for torrent in torrents_to_delete:
        torrent_dir = os.path.join(mount_path, torrent)
        if not os.path.exists(torrent_dir):
            continue
        try:
            if os.path.isdir(torrent_dir):
                for f in Path(torrent_dir).rglob("*"):
                    if f.is_file():
                        os.remove(f)
                actually_deleted.append(torrent)  # ✅ dossier supprimé localement
                logger.info(f"<fg 195>🧹 [Local] Torrent supprimé : {torrent_dir}</fg 195>")
            elif os.path.isfile(torrent_dir):
                os.remove(torrent_dir)
                actually_deleted.append(torrent)
                logger.info(f"<cyan>🧹 [Local] Fichier supprimé : {torrent_dir}</cyan>")
        except Exception as e:
            logger.error(f"<red>❌ [Local] Erreur suppression {torrent_dir}: {e}</red>")

    # ✅ Met à jour les stats et la liste réelle des suppressions
    data["orphans"] = []
    data.setdefault("stats", {})["orphans"] = 0
    data["deleted_torrents"] = actually_deleted
    data["deleted_timestamp"] = datetime.utcnow().isoformat() + "Z"

    logger.info(
        f"✅ [{instance}] Fin {dry_label} → "
        f"{ok} supprimé(s), {nf} introuvable(s), {err} erreur(s)"
    )

    return {
        "instance": instance,
        "dry_run": dry_run,
        "deleted": ok,
        "not_found": nf,
        "errors": err,
        "deleted_torrents": actually_deleted,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

@router.delete("/all")
async def delete_all_orphans(
    background_tasks: BackgroundTasks,
    dry_run: bool = Query(False, description="Si true, ne supprime rien (dry-run)")
):
    """
    Supprime (ou simule) les torrents orphelins pour toutes les instances connues.
    """
    if not orphans_store:
        raise HTTPException(status_code=404, detail="Aucun orphelin trouvé.")

    logger.info(f"🚀 Suppression multi-instance (dry_run={dry_run}) lancée pour {len(orphans_store)} instances.")

    for instance in orphans_store.keys():
        background_tasks.add_task(perform_deletion, instance, dry_run)

    return {
        "status": "accepted",
        "mode": "dry-run" if dry_run else "suppression",
        "instances": list(orphans_store.keys()),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

@router.delete("/{instance}")
async def delete_orphans_background(
    instance: str,
    background_tasks: BackgroundTasks,
    dry_run: bool = Query(False, description="Si true, ne supprime rien (dry-run)"),
):
    """
    Lance la suppression ou le dry-run des torrents orphelins en tâche de fond.
    """
    if instance not in orphans_store:
        raise HTTPException(status_code=404, detail=f"Aucune donnée trouvée pour {instance}.")

    data = orphans_store[instance]
    api_key = data["api_key"]

    if not data["orphans"]:
        raise HTTPException(status_code=404, detail="Aucun fichier orphelin à supprimer.")
    if api_key.startswith("YOUR_ALLDEBRID_API_KEY"):
        raise HTTPException(status_code=400, detail="Clé API AllDebrid non configurée.")

    mode = "dry-run" if dry_run else "suppression"
    logger.info(f"🚀 [{instance}] Lancement en tâche de fond")
    background_tasks.add_task(perform_deletion, instance, dry_run)

    return {
        "instance": instance,
        "mode": mode,
        "status": "accepted",
        "orphans_count": len(data["orphans"]),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

# ═══════════════════════════════════════════════════════════
# JOB INTERNE — Utilisé par le watcher (hors contexte HTTP)
# ═══════════════════════════════════════════════════════════

async def delete_all_orphans_job(dry_run: bool = True):
    from routers.secure.orphans import orphans_store, perform_deletion
    import io
    import asyncio

    if not orphans_store:
        logger.info("ℹ️ Aucun orphelin trouvé pour suppression.")
        return

    mode = "dry-run" if dry_run else "suppression"
    logger.info(f"🚀 Lancement interne pour {len(orphans_store)} instances...")

    total_deleted, total_not_found, total_errors = 0, 0, 0
    per_instance = []

    # 🧩 Capture des logs en temps réel pour extraire les torrents supprimés
    buffer = io.StringIO()
    handler_id = logger.add(buffer, level="INFO")

    for instance in list(orphans_store.keys()):
        try:
            result = await perform_deletion(instance, dry_run=dry_run)
            if not result:
                continue

            name = result.get("instance", instance)
            if dry_run:
                deleted = result.get("found_torrents", 0)
                not_found = 0
                errors = 0
            else:
                deleted = result.get("deleted", 0)
                not_found = result.get("not_found", 0)
                errors = result.get("errors", 0)

            per_instance.append({
                "name": name,
                "deleted": deleted,
                "not_found": not_found,
                "errors": errors
            })

            total_deleted += deleted
            total_not_found += not_found
            total_errors += errors

        except Exception as e:
            logger.error(f"💥 Erreur suppression {instance}: {e}")

    # 🔔 Envoi Discord (dans une sous-boucle propre)
    webhook = getattr(config_manager.config, "discord_webhook_url", None)
    if not webhook:
        logger.warning("⚠️ Aucun webhook Discord configuré pour le rapport de suppression.")
        return

    description = (
        f"🧾 **{mode.upper()} AllDebrid terminé**\n\n"
        f"✅ **{total_deleted} fichiers {'simulés' if dry_run else 'supprimés'}**\n"
        f"⚠️ **{total_not_found} non trouvés**\n"
        f"❌ **{total_errors} erreurs**\n\n"
        f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
        f"📂 **Instances traitées :**"
    )

    for inst in per_instance[:10]:
        description += f"\n• **{inst['name']}** → ✅ {inst['deleted']} | ⚠️ {inst['not_found']} | ❌ {inst['errors']}"
    if len(per_instance) > 10:
        description += f"\n… (+{len(per_instance) - 10} autres)"

    async def _send():
        try:
            await send_discord_message(
                webhook_url=webhook,
                title="🧹 Rapport AllDebrid — Orphelins supprimés",
                description=description,
                color=0x3498DB if dry_run else 0x2ECC71,
                module="Orphan Manager",
                action="deleted" if not dry_run else "created",
            )
            logger.info("   Rapport Discord suppression enrichi envoyé.")
        except Exception as e:
            logger.error(f"💥 Erreur envoi Discord : {e}")

    # 🚀 Lancer la sous-tâche dans la bonne boucle
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_send())
    except RuntimeError:
        asyncio.run(_send())

    # 🧩 Lecture des logs capturés pour extraire les torrents supprimés
    logger.remove(handler_id)
    buffer.seek(0)
    log_lines = buffer.read().splitlines()

    deleted_torrents = []
    for line in log_lines:
        if "→ supprimé" in line and "[AllDebrid]" in line:
            # Exemple : [AllDebrid] Beacon.23.S02E01.MULTi.1080p.WEB.H264-FW - ID: 383489232 → supprimé
            name = line.split("[AllDebrid]")[-1].split("→")[0].strip(" -:")
            if name and name not in deleted_torrents:
                deleted_torrents.append(name)

    # ✅ Retourne les infos détaillées pour le watcher (pour Discord et logs)
    return {
        "logs": [
            f"[{inst['name']}] → {inst['deleted']} supprimé(s), {inst['not_found']} introuvable(s), {inst['errors']} erreur(s)"
            for inst in per_instance
        ],
        "deleted_torrents": deleted_torrents,
        "deleted_count": total_deleted,
        "not_found_count": total_not_found,
        "error_count": total_errors,
    }

@router.get("/only")
async def get_only_deleted_orphans():
    """
    Retourne uniquement la liste des torrents réellement supprimés
    (confirmés par AllDebrid, Decypharr ou suppression locale).
    """
    if not orphans_store:
        raise HTTPException(status_code=404, detail="Aucun rapport d’orphelins trouvé.")

    report = {}
    total_deleted = 0

    for instance, data in orphans_store.items():
        deleted = data.get("deleted_torrents", [])
        if not deleted:
            continue  # Ignore si rien supprimé
        report[instance] = {
            "deleted_count": len(deleted),
            "deleted_torrents": deleted,
            "deleted_timestamp": data.get("deleted_timestamp")
        }
        total_deleted += len(deleted)

    if not report:
        raise HTTPException(status_code=404, detail="Aucune suppression enregistrée.")

    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "instances": list(report.keys()),
        "total_deleted": total_deleted,
        "details": report
    }

