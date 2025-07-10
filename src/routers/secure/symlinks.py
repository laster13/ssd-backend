from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Request
from typing import List, Optional
from pathlib import Path
import json
from pydantic import BaseModel
from collections import Counter
from fastapi.responses import StreamingResponse
from program.managers.sse_manager import sse_manager
from loguru import logger

router = APIRouter(prefix="/symlinks", tags=["Symlinks"])

symlink_store = []

class ConfigModel(BaseModel):
    links_dir: str
    mount_dir: str
    radarr_api_key: str
    sonarr_api_key: str

CONFIG_PATH = Path("data/config.json")

class SymlinkItem(BaseModel):
    symlink: str
    target: str
    ref_count: int

def load_config():
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except json.JSONDecodeError:
            logger.warning("Fichier de configuration JSON invalide, utilisation des valeurs par défaut")
    return {"links_dir": "/links", "mount_dir": "/mnt/rd"}

def save_config(data):
    CONFIG_PATH.parent.mkdir(exist_ok=True, parents=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=2)
    logger.success("Configuration enregistrée")

def scan_symlinks():
    config = load_config()
    links_dir = Path(config["links_dir"])
    mount_dir = Path(config["mount_dir"])

    if not links_dir.exists() or not mount_dir.exists():
        raise RuntimeError(f"Dossiers introuvables : {links_dir}, {mount_dir}")

    symlinks_list = []
    for symlink_path in links_dir.iterdir():
        if symlink_path.is_symlink():
            target_path = symlink_path.resolve()
            try:
                relative_target = target_path.relative_to(mount_dir)
            except ValueError:
                relative_target = str(target_path)
            symlinks_list.append({
                "symlink": str(symlink_path),
                "target": str(relative_target),
                "target_exists": target_path.exists(),
            })

    target_counts = Counter(item["target"] for item in symlinks_list if item["target_exists"])

    results = []
    for item in symlinks_list:
        ref_count = target_counts.get(item["target"], 0) if item["target_exists"] else 0
        results.append({
            "symlink": item["symlink"],
            "target": item["target"],
            "ref_count": ref_count
        })
    logger.success(f"{len(results)} liens symboliques scannés")
    return results

@router.get("")
def list_symlinks(page: int = Query(1, gt=0), limit: int = Query(50, gt=0, le=1000),
                  search: Optional[str] = None, sort: Optional[str] = "symlink",
                  order: Optional[str] = "asc", orphans: Optional[bool] = False):
    config = load_config()
    links_dir = Path(config["links_dir"]).resolve()

    all_symlinks, target_map = [], {}
    for path in links_dir.rglob("*"):
        if not path.is_symlink():
            continue
        try:
            target = path.resolve(strict=True)
            target_str = str(target)
            all_symlinks.append((str(path), target_str, True))
            target_map.setdefault(target_str, []).append(str(path))
        except Exception:
            all_symlinks.append((str(path), "BROKEN", False))

    items = []
    for symlink_path, target, exists in all_symlinks:
        ref_count = len(target_map[target]) if exists else 0
        items.append({"symlink": symlink_path, "target": target, "ref_count": ref_count})

    if search:
        search_lower = search.lower()
        items = [i for i in items if search_lower in i["symlink"].lower() or search_lower in i["target"].lower()]
    if orphans:
        items = [i for i in items if i["ref_count"] == 0]

    reverse = order == "desc"
    if sort in {"symlink", "target", "ref_count"}:
        items.sort(key=lambda x: x[sort], reverse=reverse)

    total = len(items)
    start = (page - 1) * limit
    end = start + limit
    paginated = items[start:end]

    logger.info(f"Liste des symlinks paginée : page {page}, {len(paginated)} éléments retournés")

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "data": paginated,
        "orphaned": sum(1 for i in items if i["ref_count"] == 0),
        "unique_targets": len(set(i["target"] for i in items if i["ref_count"] > 0))
    }

@router.post("/scan")
def trigger_scan():
    try:
        data = scan_symlinks()
        symlink_store.clear()
        symlink_store.extend(data)
        return {"message": "Scan terminé", "count": len(data), "data": data}
    except Exception as e:
        logger.error(f"Erreur scan symlinks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete/{symlink_path:path}")
def delete_symlink(symlink_path: str):
    config = load_config()
    links_dir = Path(config["links_dir"]).resolve()
    mount_dir = Path(config["mount_dir"]).resolve()

    try:
        relative_path = Path(symlink_path)
        full_symlink_path = (links_dir / relative_path).absolute()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid symlink path")

    if links_dir not in full_symlink_path.parents and full_symlink_path != links_dir:
        raise HTTPException(status_code=400, detail="Path en dehors de links_dir")

    if not full_symlink_path.is_symlink():
        raise HTTPException(status_code=404, detail="Lien symbolique introuvable")

    try:
        target_path = full_symlink_path.resolve(strict=True)
    except Exception:
        target_path = None

    try:
        full_symlink_path.unlink()
        logger.success(f"Lien supprimé: {full_symlink_path}")
    except Exception as e:
        logger.error(f"Erreur suppression symlink: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    if target_path:
        other_refs = [p for p in links_dir.iterdir() if p.is_symlink() and p.resolve() == target_path]
        if not other_refs and (mount_dir in target_path.parents or target_path == mount_dir):
            try:
                if target_path.exists():
                    target_path.unlink()
                    logger.success(f"Cible supprimée: {target_path}")
            except Exception as e:
                logger.warning(f"Erreur suppression cible: {e}")

    return {"message": "Symlink supprimé"}

@router.get("/export")
def export_symlinks():
    return symlink_store

@router.post("/import")
def import_symlinks(file: UploadFile = File(...)):
    content = file.file.read()
    try:
        imported = json.loads(content)
        if not isinstance(imported, list):
            raise ValueError("Liste attendue")
        for item in imported:
            if not all(k in item for k in ("symlink", "target", "ref_count")):
                raise ValueError("Entrée symlink invalide")
        symlink_store.clear()
        symlink_store.extend(imported)
        logger.success(f"{len(imported)} liens importés")
        return {"imported": len(imported)}
    except Exception as e:
        logger.error(f"Erreur import symlinks: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/config", response_model=ConfigModel)
def get_config():
    return load_config()

@router.post("/config", response_model=ConfigModel)
def save_new_config(config: ConfigModel):
    save_config(config.dict())
    return config

@router.get("/events")
async def symlink_sse(request: Request):
    async def event_generator():
        async for event in sse_manager.subscribe("symlink_update"):
            if await request.is_disconnected():
                break
            yield f"data: {event}\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream")
