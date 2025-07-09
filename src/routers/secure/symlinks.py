from fastapi import APIRouter, HTTPException, UploadFile, File, Body, Query, Request
from typing import List, Optional
from pathlib import Path
import json
from pydantic import BaseModel
from collections import Counter
from fastapi.responses import StreamingResponse
from program.managers.sse_manager import sse_manager


router = APIRouter(prefix="/symlinks", tags=["Symlinks"])


# In-memory store simulating symlink data
symlink_store = []

def scan_symlinks():
    config = load_config()
    links_dir = Path(config["links_dir"])
    mount_dir = Path(config["mount_dir"])

    if not links_dir.exists() or not mount_dir.exists():
        raise RuntimeError(f"One of the configured directories does not exist: {links_dir}, {mount_dir}")

    symlinks_list = []

    # Liste tous les liens symboliques dans links_dir (non récursif ici)
    for symlink_path in links_dir.iterdir():
        if symlink_path.is_symlink():
            target_path = symlink_path.resolve()
            # On convertit le target en chemin relatif à mount_dir pour cohérence
            try:
                relative_target = target_path.relative_to(mount_dir)
            except ValueError:
                # Le target n’est pas sous mount_dir, on le garde tel quel en string
                relative_target = str(target_path)

            symlinks_list.append({
                "symlink": str(symlink_path),
                "target": str(relative_target),
                "target_exists": target_path.exists(),
            })

    # Compte combien de fois chaque target est référencé
    target_counts = Counter(item["target"] for item in symlinks_list if item["target_exists"])

    # Ajoute ref_count et filtre les symlinks dont la cible n’existe pas
    results = []
    for item in symlinks_list:
        ref_count = target_counts.get(item["target"], 0) if item["target_exists"] else 0
        results.append({
            "symlink": item["symlink"],
            "target": item["target"],
            "ref_count": ref_count
        })

    return results

def count_symlink_references(links_dir: Path, target: Path) -> int:
    try:
        return sum(
            1 for link in links_dir.rglob("*")
            if link.is_symlink() and link.resolve(strict=False) == target
        )
    except Exception:
        return 0

class SymlinkItem(BaseModel):
    symlink: str
    target: str
    ref_count: int


# --- Symlinks API ---


@router.get("")
def list_symlinks(
    page: int = Query(1, gt=0),
    limit: int = Query(50, gt=0, le=1000),
    search: Optional[str] = None,
    sort: Optional[str] = "symlink",
    order: Optional[str] = "asc",
    orphans: Optional[bool] = False
):
    global symlink_store
    config = load_config()
    links_dir = Path(config["links_dir"]).resolve()

    all_symlinks = []
    target_map = {}

    # 🧭 Indexation des symlinks
    for path in links_dir.rglob("*"):
        if not path.is_symlink():
            continue
        try:
            target = path.resolve(strict=True)
            target_str = str(target)
            all_symlinks.append((str(path), target_str, True))
            target_map.setdefault(target_str, []).append(str(path))
        except Exception:
            # Lien cassé
            all_symlinks.append((str(path), "BROKEN", False))

    items = []
    for symlink_path, target, exists in all_symlinks:
        if not exists:
            ref_count = 0
        else:
            ref_count = len(target_map[target])
        items.append({
            "symlink": symlink_path,
            "target": target,
            "ref_count": ref_count
        })

    # 🔍 Filtrage recherche
    if search:
        search_lower = search.lower()
        items = [i for i in items if search_lower in i["symlink"].lower() or search_lower in i["target"].lower()]

    # 🛑 Filtrage orphelins
    if orphans:
        items = [i for i in items if i["ref_count"] == 0]

    # 🔢 Tri
    reverse = order == "desc"
    if sort in {"symlink", "target", "ref_count"}:
        items.sort(key=lambda x: x[sort], reverse=reverse)

    # 📊 Statistiques avant pagination
    orphaned_count = sum(1 for i in items if i["ref_count"] == 0)
    unique_targets = len(set(i["target"] for i in items if i["ref_count"] > 0))

    # 📄 Pagination
    total = len(items)
    start = (page - 1) * limit
    end = start + limit
    paginated = items[start:end]

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "data": paginated,
        "orphaned": orphaned_count,
        "unique_targets": unique_targets
    }

@router.get("/stats")
def get_stats():
    total = len(symlink_store)
    unique_targets = len(set(item["target"] for item in symlink_store))
    orphaned = len([item for item in symlink_store if item["ref_count"] == 0])
    return {"total": total, "unique_targets": unique_targets, "orphaned": orphaned}

@router.delete("/delete/{symlink_path:path}")
def delete_symlink(symlink_path: str):
    config = load_config()
    links_dir = Path(config["links_dir"]).resolve()
    mount_dir = Path(config["mount_dir"]).resolve()

    # Résoudre chemin relatif pour éviter les abus (sécurité)
    try:
        # symlink_path est un path relatif dans l'URL, on en fait un Path et on résout avec links_dir
        relative_path = Path(symlink_path)
        full_symlink_path = (links_dir / relative_path).absolute()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid symlink path")

    # Vérifier que le symlink est bien sous links_dir
    if links_dir not in full_symlink_path.parents and full_symlink_path != links_dir:
        raise HTTPException(status_code=400, detail="Symlink path outside configured links_dir")

    # Vérifier que le fichier existe et est un lien symbolique
    if not full_symlink_path.is_symlink():
        raise HTTPException(status_code=404, detail="Symlink not found")

    # Résoudre la cible du lien
    try:
        target_path = full_symlink_path.resolve(strict=True)
    except Exception:
        target_path = None

    # Supprimer le symlink
    try:
        full_symlink_path.unlink()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete symlink: {e}")

    # Vérifier s'il reste d'autres symlinks vers la même cible
    other_symlinks = []
    if target_path:
        other_symlinks = [p for p in links_dir.iterdir()
                          if p.is_symlink() and p.resolve() == target_path]

    # Si aucune autre référence, supprimer la cible si elle est dans mount_dir
    if not other_symlinks and target_path:
        try:
            if mount_dir in target_path.parents or target_path == mount_dir:
                if target_path.exists():
                    target_path.unlink()
        except Exception as e:
            print(f"Erreur suppression cible : {e}")

    return {"message": "Symlink deleted, target deleted if no other references"}

@router.post("/scan")
def trigger_scan():
    try:
        symlinks_data = scan_symlinks()
        global symlink_store
        symlink_store.clear()
        symlink_store.extend(symlinks_data)
        return {"message": "Scan terminé", "count": len(symlinks_data), "data": symlinks_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export")
def export_symlinks():
    return symlink_store

@router.post("/import")
def import_symlinks(file: UploadFile = File(...)):
    content = file.file.read()
    try:
        imported = json.loads(content)
        if not isinstance(imported, list):
            raise ValueError("Expected a list of symlinks")
        for item in imported:
            if not all(k in item for k in ("symlink", "target", "ref_count")):
                raise ValueError("Invalid symlink entry")
        symlink_store.clear()
        symlink_store.extend(imported)
        return {"imported": len(imported)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Config API ---

CONFIG_PATH = Path("data/config.json")

class ConfigModel(BaseModel):
    links_dir: str
    mount_dir: str

def load_config():
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except json.JSONDecodeError:
            pass
    # Valeurs par défaut si fichier absent ou corrompu
    return {"links_dir": "/links", "mount_dir": "/mnt/rd"}

def save_config(data):
    CONFIG_PATH.parent.mkdir(exist_ok=True, parents=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=2)

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



