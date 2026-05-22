from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from program.utils.discord_notifier import send_discord_message
from loguru import logger
from program.settings.manager import config_manager
from pathlib import Path
from datetime import datetime, timezone
import asyncio
import subprocess
import os
import aiohttp
import docker
import re


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
# VERIFICATION ETAT CONTAINER DECYPHARR
# ═══════════════════════════════════════════════════════════

def check_decypharr_ready():
    """
    Vérifie l'état du conteneur Docker 'decypharr'.
    Empêche le lancement du scan FD ou des suppressions si le conteneur
    vient d'être démarré (moins de 2 minutes) ou n'est pas encore 'running'.
    """
    try:
        client = docker.from_env()
        container = client.containers.get("decypharr")
        state = container.attrs.get("State", {})
        status = state.get("Status", "").lower()
        started_at = state.get("StartedAt")

        if status != "running":
            logger.warning(f"⏸️ Decypharr non prêt (status={status}) — opération bloquée.")
            raise HTTPException(
                status_code=503,
                detail=f"Decypharr n'est pas encore opérationnel (état : {status}). Réessayez dans quelques minutes."
            )

        if started_at and isinstance(started_at, str):
            try:
                started = datetime.strptime(
                    started_at.split(".")[0],
                    "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
            except Exception as e:
                logger.warning(f"⚠️ Erreur de parsing de date Docker : {e}")
            else:
                uptime = (datetime.now(timezone.utc) - started).total_seconds()
                if uptime < 120:
                    logger.info(f"⏳ Decypharr vient de démarrer ({int(uptime)}s) — report de l'opération.")
                    raise HTTPException(
                        status_code=503,
                        detail=f"Decypharr vient de démarrer ({int(uptime)}s). Réessayez dans quelques minutes."
                    )

        logger.debug("✅ Decypharr opérationnel — autorisation du scan FD.")

    except HTTPException:
        raise
    except docker.errors.NotFound:
        logger.error("💥 Conteneur 'decypharr' introuvable dans Docker.")
        raise HTTPException(
            status_code=503,
            detail="Conteneur 'decypharr' introuvable. Vérifiez qu'il est bien lancé."
        )
    except Exception as e:
        logger.error(f"💥 Erreur lors de la vérification du conteneur Decypharr : {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Impossible de vérifier l'état de Decypharr ({e}). Réessayez plus tard."
        )


def is_decypharr_running() -> bool:
    """
    Vérifie silencieusement si le conteneur Decypharr est en cours d’exécution.
    Retourne True si 'running' depuis plus de 2 minutes, sinon False.
    """
    try:
        client = docker.from_env()
        container = client.containers.get("decypharr")
        state = container.attrs.get("State", {})
        status = state.get("Status", "").lower()
        started_at = state.get("StartedAt")

        if status != "running":
            return False

        if started_at and isinstance(started_at, str):
            started = datetime.strptime(
                started_at.split(".")[0],
                "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=timezone.utc)
            uptime = (datetime.now(timezone.utc) - started).total_seconds()
            if uptime < 120:
                return False

        return True
    except Exception:
        return False


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
            capture_output=True,
            text=True,
            check=True
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
    Compare les chemins physiques (realpath) au lieu des noms texte.
    """
    norm_mount = {os.path.normpath(os.path.realpath(f)) for f in mount_files}
    norm_symlinks = {os.path.normpath(os.path.realpath(f)) for f in symlink_targets}

    orphans = sorted(norm_mount - norm_symlinks)

    logger.debug(f"🧠 Détection orphelins: {len(orphans)} fichiers non liés après normalisation")

    if orphans:
        logger.warning("📄 Liste des fichiers orphelins détectés :")
        for f in orphans[:50]:
            logger.warning(f"   → {f}")
        if len(orphans) > 50:
            logger.warning(f"   ... et {len(orphans) - 50} autres fichiers orphelins.")

    return orphans


# ═══════════════════════════════════════════════════════════
# SCAN D’UNE INSTANCE
# ═══════════════════════════════════════════════════════════

async def scan_instance(instance) -> dict:
    """
    Scanne une instance AllDebrid et détecte les orphelins.
    Intègre un watchdog Decypharr pour interrompre le scan si le conteneur
    est redémarré ou encore en phase de démarrage.
    """
    name = instance.name
    mount_path = Path(instance.mount_path)
    api_key = instance.api_key
    rate_limit = instance.rate_limit

    start = datetime.utcnow()
    logger.info(f"🔍 Scan instance: {name}")

    try:
        if not is_decypharr_running():
            logger.warning(f"🛑 Scan interrompu : Decypharr redémarre pendant le scan de {name}.")
            return {"instance": name, "error": "Scan interrompu — Decypharr redémarré."}

        mount_files = await asyncio.to_thread(list_mount_files, mount_path)

        if not is_decypharr_running():
            logger.warning(f"🛑 Scan interrompu après list_mount_files : Decypharr redémarré.")
            return {"instance": name, "error": "Scan interrompu — Decypharr redémarré."}

        symlink_targets = await list_symlink_targets(config_manager.config.links_dirs, mount_path)

        if not is_decypharr_running():
            logger.warning(f"🛑 Scan interrompu avant comparaison : Decypharr redémarré.")
            return {"instance": name, "error": "Scan interrompu — Decypharr redémarré."}

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
                "deletable": len(orphans),
            },
        }

        orphans_store[name] = {
            "orphans": orphans,
            "symlinks_list": symlink_targets,
            "api_key": api_key,
            "mount_path": str(mount_path),
            "rate_limit": rate_limit,
            "stats": result["stats"],
        }

        logger.info(f"✅ Scan terminé pour {name}: {len(orphans)} orphelins détectés")
        logger.info(f"⏱️ Durée: {duration:.2f}s")
        return result

    except Exception as e:
        logger.error(f"💥 Erreur durant le scan de {name}: {e}")
        return {"instance": name, "error": str(e)}


# ═══════════════════════════════════════════════════════════
# ROUTES API
# ═══════════════════════════════════════════════════════════

@router.get("/instances")
async def get_instances():
    """Retourne les instances AllDebrid configurées."""
    return getattr(config_manager.config, "alldebrid_instances", [])


@router.post("/scan")
async def scan_all_instances():
    """Lance le scan sur toutes les instances AllDebrid actives."""
    check_decypharr_ready()

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
    Supprime ou simule la suppression des orphelins AllDebrid.

    Version securisee:
    - utilise uniquement les orphelins deja detectes par le scan WebDAV
    - ignore les noms trop generiques: Saison 2, Season 1, S02, sample, subs...
    - recupere la liste des magnets AllDebrid une seule fois
    - ne traite en suppression que les magnets retrouves de facon fiable
    - ne log plus en WARNING les magnets absents de la liste AllDebrid
    - refuse les matches vagues ou ambigus
    - ne touche pas au cache Decypharr
    """
    import re

    if not is_decypharr_running():
        logger.warning(
            f"[{instance}] Suppression interrompue: Decypharr non operationnel."
        )
        return {
            "instance": instance,
            "error": "Decypharr non operationnel - suppression annulee.",
        }

    data = orphans_store.get(instance)
    if not data:
        logger.error(f"[{instance}] Instance introuvable dans orphans_store")
        return {
            "instance": instance,
            "error": "Instance non trouvee",
        }

    orphans = data.get("orphans", [])
    api_key = data.get("api_key")
    mount_path = data.get("mount_path")
    rate_limit = float(data.get("rate_limit", 0.5))

    dry_label = "DRY-RUN" if dry_run else "SUPPRESSION"
    logger.info(f"[{instance}] Demarrage {dry_label} en tache de fond...")

    if not api_key:
        logger.error(f"[{instance}] Cle API AllDebrid manquante.")
        return {
            "instance": instance,
            "error": "Cle API AllDebrid manquante",
        }

    if not mount_path:
        logger.error(f"[{instance}] mount_path manquant.")
        return {
            "instance": instance,
            "error": "mount_path manquant",
        }

    def extract_torrent(file_path: str) -> str | None:
        """
        Extrait le dossier torrent parent depuis un chemin orphelin.
        Exemple:
        /mount/Torrent.Name/file.mkv -> Torrent.Name
        """
        try:
            rel_path = os.path.relpath(file_path, mount_path)
            first_part = rel_path.split(os.sep, 1)[0]
            first_part = first_part.strip()
            return first_part or None
        except Exception:
            return None

    def normalize_name(value: str) -> str:
        """
        Normalise un nom pour comparaison.
        """
        if not value:
            return ""

        value = value.lower().strip()

        replacements = {
            "é": "e", "è": "e", "ê": "e", "ë": "e",
            "à": "a", "â": "a", "ä": "a",
            "î": "i", "ï": "i",
            "ô": "o", "ö": "o",
            "ù": "u", "û": "u", "ü": "u",
            "ç": "c",
        }

        for src, dst in replacements.items():
            value = value.replace(src, dst)

        value = re.sub(
            r"\.(mkv|mp4|avi|m4v|mov|ts)$",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = value.replace("_", " ").replace(".", " ").replace("-", " ")
        value = re.sub(r"\s+", " ", value).strip()

        return value

    def should_skip_torrent_name(name: str) -> bool:
        """
        Ignore les faux noms de torrents ou noms trop generiques.
        Cela evite de traiter des dossiers comme:
        - Saison 1
        - Saison 2
        - Season 1
        - S01
        - S02
        - sample
        - subtitles
        """
        if not name:
            return True

        clean = normalize_name(name)

        if not clean:
            return True

        generic_patterns = [
            r"^saison\s*\d+$",
            r"^season\s*\d+$",
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
            if re.match(pattern, clean, flags=re.IGNORECASE):
                return True

        if len(clean) < 8:
            return True

        return False

    def strong_prefix(a: str, b: str) -> bool:
        """
        Retourne True seulement si les noms sont vraiment proches des le debut.
        Evite les matches dangereux.
        """
        if not a or not b:
            return False

        na = normalize_name(a)
        nb = normalize_name(b)

        if na == nb:
            return True

        shortest = min(len(na), len(nb))

        if shortest < 12:
            return False

        prefix_len = 0

        for ca, cb in zip(na, nb):
            if ca == cb:
                prefix_len += 1
            else:
                break

        return prefix_len >= max(12, int(shortest * 0.7))

    all_torrents = sorted(
        set(
            filter(
                None,
                [extract_torrent(f) for f in orphans],
            )
        )
    )

    skipped_generic = [
        torrent
        for torrent in all_torrents
        if should_skip_torrent_name(torrent)
    ]

    candidate_torrents = [
        torrent
        for torrent in all_torrents
        if not should_skip_torrent_name(torrent)
    ]

    if skipped_generic:
        logger.info(
            f"[{instance}] {len(skipped_generic)} torrent(s) ignore(s), nom trop generique: "
            f"{skipped_generic[:10]}"
        )

    logger.info(
        f"[{instance}] {len(candidate_torrents)} torrent(s) candidat(s) apres filtrage "
        f"sur {len(all_torrents)} extrait(s) du scan WebDAV"
    )

    if not candidate_torrents:
        logger.info(f"[{instance}] Aucun torrent candidat a traiter.")
        return {
            "instance": instance,
            "dry_run": dry_run,
            "deleted": 0,
            "not_found": 0,
            "skipped": len(skipped_generic),
            "errors": 0,
            "deleted_torrents": [],
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

    timeout = aiohttp.ClientTimeout(
        total=30,
        connect=10,
        sock_read=30,
    )

    ok = 0
    nf = 0
    err = 0
    actually_deleted = []

    async with aiohttp.ClientSession(timeout=timeout) as session:
        if not is_decypharr_running():
            logger.warning(
                f"[{instance}] Suppression interrompue avant requete API: Decypharr redemarre."
            )
            return {
                "instance": instance,
                "error": "Decypharr redemarre avant suppression AllDebrid.",
            }

        logger.info(f"[{instance}] Recuperation de la liste des magnets AllDebrid...")

        async with session.get(
            f"{ALLDEBRID_API_BASE}/magnet/status",
            headers={"Authorization": f"Bearer {api_key}"},
        ) as resp:
            try:
                data_status = await resp.json()
            except Exception as e:
                logger.error(
                    f"[{instance}] Erreur de decodage JSON sur magnet/status: {e}"
                )
                return {
                    "instance": instance,
                    "error": "Reponse JSON invalide sur magnet/status",
                }

            if data_status.get("status") != "success":
                logger.error(
                    f"[{instance}] Erreur API magnet/status: {data_status}"
                )
                return {
                    "instance": instance,
                    "error": "Erreur API magnet/status",
                }

            magnets = data_status.get("data", {}).get("magnets", [])

        logger.info(
            f"[{instance}] {len(magnets)} magnet(s) recupere(s) depuis AllDebrid"
        )

        def find_magnet_info(name: str) -> dict | None:
            """
            Matching securise:
            1. match exact filename/name
            2. match exact normalise
            3. prefixe fort et unique seulement

            Si plusieurs matches existent, on refuse.
            """
            exact_candidates = []
            normalized_candidates = []
            strong_prefix_candidates = []

            wanted_norm = normalize_name(name)

            for magnet in magnets:
                filename = (magnet.get("filename") or "").strip()
                magnet_name = (magnet.get("name") or "").strip()

                candidates = [
                    candidate
                    for candidate in [filename, magnet_name]
                    if candidate
                ]

                if name in candidates:
                    exact_candidates.append(
                        {
                            "id": str(magnet["id"]),
                            "name": filename or magnet_name,
                        }
                    )
                    continue

                matched_normalized = False

                for candidate in candidates:
                    if normalize_name(candidate) == wanted_norm:
                        normalized_candidates.append(
                            {
                                "id": str(magnet["id"]),
                                "name": filename or magnet_name,
                            }
                        )
                        matched_normalized = True
                        break

                if matched_normalized:
                    continue

                for candidate in candidates:
                    if strong_prefix(name, candidate):
                        strong_prefix_candidates.append(
                            {
                                "id": str(magnet["id"]),
                                "name": filename or magnet_name,
                            }
                        )
                        break

            if len(exact_candidates) == 1:
                return exact_candidates[0]

            if len(exact_candidates) > 1:
                logger.info(
                    f"[{instance}] Match exact ambigu ignore pour '{name}': "
                    f"{[x['name'] for x in exact_candidates[:5]]}"
                )
                return None

            if len(normalized_candidates) == 1:
                return normalized_candidates[0]

            if len(normalized_candidates) > 1:
                logger.info(
                    f"[{instance}] Match normalise ambigu ignore pour '{name}': "
                    f"{[x['name'] for x in normalized_candidates[:5]]}"
                )
                return None

            if len(strong_prefix_candidates) == 1:
                logger.info(
                    f"[{instance}] Match prefixe fort retenu pour '{name}' -> "
                    f"{strong_prefix_candidates[0]['name']}"
                )
                return strong_prefix_candidates[0]

            if len(strong_prefix_candidates) > 1:
                logger.info(
                    f"[{instance}] Match prefixe ambigu ignore pour '{name}': "
                    f"{[x['name'] for x in strong_prefix_candidates[:5]]}"
                )
                return None

            return None

        matched_torrents = []
        missing_torrents = []

        for torrent in candidate_torrents:
            info = find_magnet_info(torrent)

            if info:
                matched_torrents.append(
                    {
                        "torrent": torrent,
                        "info": info,
                    }
                )
            else:
                missing_torrents.append(torrent)

        nf = len(missing_torrents)

        if missing_torrents:
            logger.info(
                f"[{instance}] {len(missing_torrents)} torrent(s) ignore(s), absent(s) de la liste AllDebrid. "
                f"Exemples: {missing_torrents[:10]}"
            )

        logger.info(
            f"[{instance}] {len(matched_torrents)} torrent(s) avec match fiable a traiter"
        )

        if not matched_torrents:
            data["orphans"] = []
            data.setdefault("stats", {})["orphans"] = 0
            data["deleted_torrents"] = []
            data["deleted_timestamp"] = datetime.utcnow().isoformat() + "Z"

            logger.info(
                f"[{instance}] Fin {dry_label}: 0 supprime(s), "
                f"{nf} absent(s), {len(skipped_generic)} ignore(s), 0 erreur(s)"
            )

            return {
                "instance": instance,
                "dry_run": dry_run,
                "deleted": 0,
                "not_found": nf,
                "skipped": len(skipped_generic),
                "errors": 0,
                "deleted_torrents": [],
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

        for idx, item in enumerate(matched_torrents, start=1):
            if not is_decypharr_running():
                logger.warning(
                    f"[{instance}] Suppression interrompue pendant le cycle AllDebrid."
                )
                return {
                    "instance": instance,
                    "error": "Decypharr redemarre en cours de suppression.",
                }

            torrent = item["torrent"]
            info = item["info"]

            magnet_id = info["id"]
            magnet_name = info["name"]

            logger.info(
                f"[{instance}] {idx}/{len(matched_torrents)}: {torrent} -> {magnet_name}"
            )

            if dry_run:
                logger.info(
                    f"[AllDebrid] {magnet_name} - ID: {magnet_id} -> simule"
                )
                await asyncio.sleep(rate_limit)
                continue

            try:
                async with session.post(
                    f"{ALLDEBRID_API_BASE}/magnet/delete",
                    headers={"Authorization": f"Bearer {api_key}"},
                    data={"id": magnet_id},
                ) as del_resp:
                    try:
                        del_json = await del_resp.json()
                    except Exception as e:
                        err += 1
                        logger.error(
                            f"[{instance}] Reponse JSON invalide suppression AllDebrid "
                            f"({magnet_name}): {e}"
                        )
                        await asyncio.sleep(rate_limit)
                        continue

                    if del_json.get("status") == "success":
                        ok += 1
                        actually_deleted.append(magnet_name)
                        logger.info(
                            f"[AllDebrid] {magnet_name} - ID: {magnet_id} -> supprime"
                        )
                    else:
                        err += 1
                        msg = del_json.get("error", {}).get(
                            "message",
                            "Erreur inconnue",
                        )
                        logger.warning(
                            f"[AllDebrid] Echec suppression {magnet_name}: {msg}"
                        )

            except Exception as e:
                err += 1
                logger.error(
                    f"[{instance}] Exception suppression AllDebrid ({magnet_name}): {e}"
                )

            await asyncio.sleep(rate_limit)

    data["orphans"] = []
    data.setdefault("stats", {})["orphans"] = 0
    data["deleted_torrents"] = actually_deleted
    data["deleted_timestamp"] = datetime.utcnow().isoformat() + "Z"

    logger.info(
        f"[{instance}] Fin {dry_label}: "
        f"{ok} supprime(s), {nf} absent(s), "
        f"{len(skipped_generic)} ignore(s), {err} erreur(s)"
    )

    return {
        "instance": instance,
        "dry_run": dry_run,
        "deleted": ok,
        "not_found": nf,
        "skipped": len(skipped_generic),
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
    check_decypharr_ready()

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
    check_decypharr_ready()

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

    check_decypharr_ready()

    if not orphans_store:
        logger.info("ℹ️ Aucun orphelin trouvé pour suppression.")
        return {
            "logs": [],
            "deleted_torrents": [],
            "deleted_count": 0,
            "not_found_count": 0,
            "error_count": 0,
            "instances": [],
            "dry_run": dry_run,
        }

    mode = "dry-run" if dry_run else "suppression"
    logger.info(f"🚀 Lancement interne pour {len(orphans_store)} instance(s)...")

    total_deleted = 0
    total_not_found = 0
    total_errors = 0
    per_instance = []
    deleted_torrents = []

    buffer = io.StringIO()
    handler_id = logger.add(buffer, level="INFO", format="{message}")

    try:
        for instance in list(orphans_store.keys()):
            try:
                result = await perform_deletion(instance, dry_run=dry_run)

                if not result:
                    per_instance.append({
                        "name": instance,
                        "deleted": 0,
                        "not_found": 0,
                        "errors": 1,
                        "status": "empty_result",
                    })
                    total_errors += 1
                    continue

                if result.get("error"):
                    per_instance.append({
                        "name": result.get("instance", instance),
                        "deleted": 0,
                        "not_found": 0,
                        "errors": 1,
                        "status": "error",
                        "message": result.get("error"),
                    })
                    total_errors += 1
                    logger.error(f"💥 Erreur suppression {instance}: {result.get('error')}")
                    continue

                name = result.get("instance", instance)
                deleted = int(result.get("deleted", 0) or 0)
                not_found = int(result.get("not_found", 0) or 0)
                errors = int(result.get("errors", 0) or 0)
                instance_deleted_torrents = result.get("deleted_torrents", []) or []

                total_deleted += deleted
                total_not_found += not_found
                total_errors += errors

                for torrent_name in instance_deleted_torrents:
                    if torrent_name and torrent_name not in deleted_torrents:
                        deleted_torrents.append(torrent_name)

                per_instance.append({
                    "name": name,
                    "deleted": deleted,
                    "not_found": not_found,
                    "errors": errors,
                    "status": "ok",
                })

            except Exception as e:
                logger.error(f"💥 Erreur suppression {instance}: {e}", exc_info=True)
                total_errors += 1
                per_instance.append({
                    "name": instance,
                    "deleted": 0,
                    "not_found": 0,
                    "errors": 1,
                    "status": "exception",
                    "message": str(e),
                })

        webhook = getattr(config_manager.config, "discord_webhook_url", None)

        if webhook:
            description = (
                f"🧾 **{mode.upper()} AllDebrid terminé**\n\n"
                f"✅ **{total_deleted} torrent(s) {'simulé(s)' if dry_run else 'supprimé(s)'}**\n"
                f"⚠️ **{total_not_found} introuvable(s)**\n"
                f"❌ **{total_errors} erreur(s)**\n\n"
                f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"📂 **Instances traitées :**"
            )

            for inst in per_instance[:10]:
                line = (
                    f"\n• **{inst['name']}** → "
                    f"✅ {inst['deleted']} | ⚠️ {inst['not_found']} | ❌ {inst['errors']}"
                )
                if inst.get("status") not in ("ok", None):
                    line += f" ({inst['status']})"
                description += line

            if len(per_instance) > 10:
                description += f"\n… (+{len(per_instance) - 10} autres)"

            if deleted_torrents:
                preview = deleted_torrents[:15]
                description += "\n\n🗑️ **Torrents supprimés :**"
                for name in preview:
                    description += f"\n- {name}"
                if len(deleted_torrents) > 15:
                    description += f"\n… (+{len(deleted_torrents) - 15} autres)"

            async def _send():
                try:
                    await send_discord_message(
                        webhook_url=webhook,
                        title="🧹 Rapport AllDebrid — Orphelins",
                        description=description,
                        color=0x3498DB if dry_run else 0x2ECC71,
                        module="Orphan Manager",
                        action="deleted" if not dry_run else "created",
                    )
                    logger.info("📢 Rapport Discord suppression enrichi envoyé.")
                except Exception as e:
                    logger.error(f"💥 Erreur envoi Discord : {e}")

            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_send())
            except RuntimeError:
                asyncio.run(_send())
        else:
            logger.warning("⚠️ Aucun webhook Discord configuré pour le rapport de suppression.")

    finally:
        try:
            logger.remove(handler_id)
        except Exception:
            pass

    buffer.seek(0)
    raw_logs = [line for line in buffer.read().splitlines() if line.strip()]

    summary_logs = [
        f"[{inst['name']}] → {inst['deleted']} supprimé(s), {inst['not_found']} introuvable(s), {inst['errors']} erreur(s)"
        for inst in per_instance
    ]

    return {
        "logs": summary_logs or raw_logs,
        "deleted_torrents": deleted_torrents,
        "deleted_count": total_deleted,
        "not_found_count": total_not_found,
        "error_count": total_errors,
        "instances": per_instance,
        "dry_run": dry_run,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

@router.get("/only")
async def get_only_deleted_orphans():
    """
    Retourne uniquement la liste des torrents réellement supprimés.
    """
    if not orphans_store:
        raise HTTPException(status_code=404, detail="Aucun rapport d’orphelins trouvé.")

    report = {}
    total_deleted = 0

    for instance, data in orphans_store.items():
        deleted = data.get("deleted_torrents", [])
        if not deleted:
            continue

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