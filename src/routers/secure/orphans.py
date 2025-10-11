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

CONFIG = config_manager.config
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
    """Compare les listes pour trouver les fichiers orphelins."""
    norm_mount = set(os.path.normpath(f) for f in mount_files)
    norm_symlinks = set(os.path.normpath(f) for f in symlink_targets)
    return sorted(norm_mount - norm_symlinks)


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
        symlink_targets = await list_symlink_targets(CONFIG.links_dirs, mount_path)
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
                "auto_delete": getattr(CONFIG.orphan_manager, "auto_delete", False),
                "deletable": len(orphans)
            }
        }

        # ✅ cache_path ajouté ici
        orphans_store[name] = {
            "orphans": orphans,
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
    return getattr(CONFIG, "alldebrid_instances", [])


@router.post("/scan")
async def scan_all_instances(background_tasks: BackgroundTasks):
    """Lance le scan sur toutes les instances AllDebrid actives (lecture seule)."""
    instances = getattr(CONFIG, "alldebrid_instances", [])
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
    supprime les fichiers JSON correspondants dans le cache Decypharr
    et les fichiers locaux correspondants sur le mount_path.
    """

    # --- Récupération de la configuration de l'instance ---
    data = orphans_store.get(instance)
    if not data:
        logger.error(f"<red>[{instance}] Instance introuvable dans orphans_store</red>")
        return

    orphans = data.get("orphans", [])
    api_key = data.get("api_key")
    mount_path = data.get("mount_path")
    rate_limit = float(data.get("rate_limit", 0.5))
    cache_path = data.get("cache_path", "/app/cache")

    dry_label = "DRY-RUN" if dry_run else "SUPPRESSION"
    logger.info(f"🧪 [{instance}] Démarrage {dry_label} en tâche de fond...")

    # --- Extraction des torrents à traiter ---
    def extract_torrent(file_path: str) -> str | None:
        """Extrait le dossier racine du torrent à partir du chemin complet."""
        try:
            rel_path = os.path.relpath(file_path, mount_path)
            return rel_path.split(os.sep, 1)[0]
        except Exception:
            return None

    torrents = sorted(set(filter(None, [extract_torrent(f) for f in orphans])))
    if not torrents:
        logger.info(f"<green>🧱[{instance}] Aucun torrent à traiter.</green>")
        return

    ok, nf, err = 0, 0, 0
    decypharr_data = []  # [(nom, id)]

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

        # --- Recherche d'un magnet par nom ---
        def find_magnet_info(name: str) -> dict | None:
            for m in magnets:
                if m.get("filename") == name or m.get("name") == name:
                    return {"id": str(m["id"]), "name": m.get("filename") or m.get("name")}
            for m in magnets:
                if m.get("filename", "").startswith(name):
                    return {"id": str(m["id"]), "name": m.get("filename") or m.get("name")}
            return None

        # --- Suppression ou simulation sur AllDebrid ---
        for torrent in torrents:
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
                decypharr_data.append((magnet_name, magnet_id))
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
                        logger.info(f"<cyan>🧹 [AllDebrid] {magnet_name} - ID: {magnet_id} → supprimé</cyan>")
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
                if dry_run:
                    logger.info(f"<fg #FFCCFF>🧱 [Decypharr] {name} - ID: {decy_id} → simulé</fg #FFCCFF>")
                else:
                    try:
                        os.remove(json_file)
                        logger.info(f"<fg #FFCCFF>🧹 [Decypharr] {name} - ID: {decy_id} → supprimé</fg #FFCCFF>")
                    except Exception as e:
                        logger.error(f"<red>❌ [Decypharr] Erreur suppression {json_file}: {e}</red>")
            else:
                logger.debug(f"[Decypharr] {name} - ID: {decy_id} → non trouvé")

    # --- Étape 3 : suppression locale des fichiers uniquement ---
    for torrent in torrents:
        torrent_dir = os.path.join(mount_path, torrent)
        if not os.path.exists(torrent_dir):
            logger.debug(f"[Local] Dossier non trouvé : {torrent_dir}")
            continue

        try:
            if dry_run:
                logger.info(f"<magenta>🧱 [Local] {torrent_dir} → simulé</magenta>")
            else:
                if os.path.isfile(torrent_dir):
                    os.remove(torrent_dir)
                    logger.info(f"<cyan>🧹 [Local] Fichier supprimé : {torrent_dir}</cyan>")
                elif os.path.isdir(torrent_dir):
                    files = os.listdir(torrent_dir)
                    for f in files:
                        file_path = os.path.join(torrent_dir, f)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            logger.info(f"<fg 195>🧹 [Local] Fichier supprimé : {file_path}</fg 195>")
                    # Laisse le dossier vide en place
        except Exception as e:
            logger.error(f"<red>❌ [Local] Erreur suppression {torrent_dir}: {e}</red>")

    # --- Résumé final ---
    data["orphans"] = []
    data.setdefault("stats", {})["orphans"] = 0

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

    if not orphans_store:
        logger.info("ℹ️ Aucun orphelin trouvé pour suppression.")
        return

    mode = "dry-run" if dry_run else "suppression"
    logger.info(f"🚀 Lancement interne pour {len(orphans_store)} instances...")

    total_deleted, total_not_found, total_errors = 0, 0, 0
    per_instance = []

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
            logger.info("📨 Rapport Discord suppression enrichi envoyé.")
        except Exception as e:
            logger.error(f"💥 Erreur envoi Discord : {e}")

    # 🚀 Lancer la sous-tâche dans la bonne boucle
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_send())
    except RuntimeError:
        asyncio.run(_send())


