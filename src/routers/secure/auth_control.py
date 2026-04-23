from fastapi import APIRouter
from pydantic import BaseModel

from program.settings.manager import settings_manager


router = APIRouter(
    prefix="/auth",
    tags=["auth"],
    responses={404: {"description": "Not found"}},
)


class AuthStatusResponse(BaseModel):
    enabled: bool


class ToggleAuthRequest(BaseModel):
    enabled: bool


@router.get("/status", operation_id="get_auth_status", response_model=AuthStatusResponse)
async def get_auth_status():
    return {"enabled": settings_manager.settings.auth_enabled}


@router.post("/toggle", operation_id="toggle_auth", response_model=AuthStatusResponse)
async def toggle_auth(payload: ToggleAuthRequest):
    settings_manager.settings.auth_enabled = payload.enabled
    settings_manager.save()
    return {"enabled": settings_manager.settings.auth_enabled}