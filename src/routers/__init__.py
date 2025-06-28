from fastapi import Depends, Request
from fastapi.routing import APIRouter

from auth import resolve_api_key
from program.settings.manager import settings_manager
from routers.secure.default import router as default_router
from routers.secure.settings import router as settings_router
from routers.secure.script import router as script_router
from routers.models.shared import RootResponse

API_VERSION = "v1"
app_router = APIRouter(prefix=f"/api/{API_VERSION}")

@app_router.get("/", operation_id="root")
async def root(_: Request) -> RootResponse:
    return {
        "message": "Riven is running!",
        "version": settings_manager.settings.version,
    }

app_router.include_router(default_router)
app_router.include_router(settings_router)
app_router.include_router(script_router)
