from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from program.settings.manager import config_manager
from program.settings.models import AllDebridInstance

router = APIRouter(prefix="/instances", tags=["Instances"])


# ✅ Modèle d’entrée depuis le frontend
class AllDebridInstanceInput(BaseModel):
    name: str
    api_key: str = ""
    mount_path: str = ""
    cache_path: str | None = None
    rate_limit: float = 0.2
    priority: int = 1
    enabled: bool = True


@router.get("/alldebrid", response_model=list[AllDebridInstance])
def list_alldebrid_instances():
    """Retourne la liste complète des instances AllDebrid."""
    return config_manager.config.alldebrid_instances


@router.post("/alldebrid")
def add_alldebrid_instance(instance: AllDebridInstanceInput):
    """Ajoute une nouvelle instance AllDebrid."""
    new_instance = AllDebridInstance(**instance.model_dump())

    # Vérifie s’il existe déjà une instance du même nom
    for existing in config_manager.config.alldebrid_instances:
        if existing.name == new_instance.name:
            raise HTTPException(status_code=400, detail=f"L'instance '{new_instance.name}' existe déjà.")

    config_manager.config.alldebrid_instances.append(new_instance)
    config_manager.save()

    return {"message": f"Instance '{new_instance.name}' ajoutée avec succès."}


@router.put("/alldebrid/{name}")
def update_alldebrid_instance(name: str, instance: AllDebridInstanceInput):
    """Met à jour une instance existante."""
    instances = config_manager.config.alldebrid_instances

    for i, existing in enumerate(instances):
        if existing.name == name:
            instances[i] = AllDebridInstance(**instance.model_dump())
            config_manager.config.alldebrid_instances = instances
            config_manager.save()
            return {"message": f"Instance '{name}' mise à jour avec succès."}

    raise HTTPException(status_code=404, detail=f"L'instance '{name}' est introuvable.")


@router.delete("/alldebrid/{name}")
def delete_alldebrid_instance(name: str):
    """Supprime une instance AllDebrid."""
    instances = config_manager.config.alldebrid_instances
    new_instances = [i for i in instances if i.name != name]

    if len(new_instances) == len(instances):
        raise HTTPException(status_code=404, detail=f"L'instance '{name}' est introuvable.")

    config_manager.config.alldebrid_instances = new_instances
    config_manager.save()
    return {"message": f"Instance '{name}' supprimée avec succès."}
