from typing import Literal

import requests
from fastapi import APIRouter, HTTPException, Request
from kink import di
from loguru import logger
from pydantic import BaseModel, Field, HttpUrl

from program.settings.manager import settings_manager
from program.utils import generate_api_key

from ..models.shared import MessageResponse

router = APIRouter(
    responses={404: {"description": "Not found"}},
)


@router.get("/health", operation_id="health")
async def health(request: Request) -> MessageResponse:
    return {
        "message": str(request.app.program.initialized),
    }


class RDUser(BaseModel):
    id: int
    username: str
    email: str
    points: int = Field(description="User's RD points")
    locale: str
    avatar: str = Field(description="URL to the user's avatar")
    type: Literal["free", "premium"]
    premium: int = Field(description="Premium subscription left in seconds")


@router.get("/rd", operation_id="rd")
async def get_rd_user() -> RDUser:
    api_key = settings_manager.settings.downloaders.real_debrid.api_key
    headers = {"Authorization": f"Bearer {api_key}"}

    proxy = (
        settings_manager.settings.downloaders.proxy_url
        if settings_manager.settings.downloaders.proxy_url
        else None
    )

    response = requests.get(
        "https://api.real-debrid.com/rest/1.0/user",
        headers=headers,
        proxies=proxy if proxy else None,
        timeout=10,
    )

    if response.status_code != 200:
        return {"success": False, "message": response.json()}

    return response.json()

@router.post("/generateapikey", operation_id="generateapikey")
async def generate_apikey() -> MessageResponse:
    new_key = generate_api_key()
    settings_manager.settings.api_key = new_key
    settings_manager.save()
    return { "message": new_key}


@router.get("/torbox", operation_id="torbox")
async def get_torbox_user():
    api_key = settings_manager.settings.downloaders.torbox.api_key
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(
        "https://api.torbox.app/v1/api/user/me", headers=headers, timeout=10
    )
    return response.json()


@router.get("/services", operation_id="services")
async def get_services(request: Request) -> dict[str, bool]:
    data = {}
    if hasattr(request.app.program, "services"):
        for service in request.app.program.all_services.values():
            data[service.key] = service.initialized
            if not hasattr(service, "services"):
                continue
            for sub_service in service.services.values():
                data[sub_service.key] = sub_service.initialized
    return data


class TraktOAuthInitiateResponse(BaseModel):
    auth_url: str


@router.get("/trakt/oauth/initiate", operation_id="trakt_oauth_initiate")
async def initiate_trakt_oauth(request: Request) -> TraktOAuthInitiateResponse:
    trakt_api = di[TraktAPI]
    if trakt_api is None:
        raise HTTPException(status_code=404, detail="Trakt service not found")
    auth_url = trakt_api.perform_oauth_flow()
    return {"auth_url": auth_url}


@router.get("/trakt/oauth/callback", operation_id="trakt_oauth_callback")
async def trakt_oauth_callback(code: str, request: Request) -> MessageResponse:
    trakt_api = di[TraktAPI]
    trakt_api_key = settings_manager.settings.content.trakt.api_key
    if trakt_api is None:
        raise HTTPException(status_code=404, detail="Trakt Api not found")
    if trakt_api_key is None:
        raise HTTPException(status_code=404, detail="Trakt Api key not found in settings")
    success = trakt_api.handle_oauth_callback(trakt_api_key, code)
    if success:
        return {"message": "OAuth token obtained successfully"}
    else:
        raise HTTPException(status_code=400, detail="Failed to obtain OAuth token")


@router.get("/logs", operation_id="logs")
async def get_logs() -> str:
    log_file_path = None
    for handler in logger._core.handlers.values():
        if ".log" in handler._name:
            log_file_path = handler._sink._path
            break

    if not log_file_path:
        return {"success": False, "message": "Log file handler not found"}

    try:
        with open(log_file_path, "r") as log_file:
            log_contents = log_file.read()
        return {"logs": log_contents}
    except Exception as e:
        logger.error(f"Failed to read log file: {e}")
        raise HTTPException(status_code=500, detail="Failed to read log file")


@router.get("/mount", operation_id="mount")
async def get_rclone_files() -> dict[str, str]:
    """Get all files in the rclone mount."""
    import os

    rclone_dir = settings_manager.settings.symlink.rclone_path
    file_map = {}

    def scan_dir(path):
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.is_file():
                    file_map[entry.name] = entry.path
                elif entry.is_dir():
                    scan_dir(entry.path)

    scan_dir(rclone_dir)  # dict of `filename: filepath``
    return file_map


class UploadLogsResponse(BaseModel):
    success: bool
    url: HttpUrl = Field(description="URL to the uploaded log file. 50M Filesize limit. 180 day retention.")

@router.post("/upload_logs", operation_id="upload_logs")
async def upload_logs() -> UploadLogsResponse:
    """Upload the latest log file to paste.c-net.org"""

    log_file_path = None
    for handler in logger._core.handlers.values():
        if ".log" in handler._name:
            log_file_path = handler._sink._path
            break

    if not log_file_path:
        raise HTTPException(status_code=500, detail="Log file handler not found")

    try:
        with open(log_file_path, "r") as log_file:
            log_contents = log_file.read()

        response = requests.post(
            "https://paste.c-net.org/",
            data=log_contents.encode('utf-8'),
            headers={"Content-Type": "text/plain"}
        )

        if response.status_code == 200:
            logger.info(f"Uploaded log file to {response.text.strip()}")
            return UploadLogsResponse(success=True, url=response.text.strip())
        else:
            logger.error(f"Failed to upload log file: {response.status_code}")
            raise HTTPException(status_code=500, detail="Failed to upload log file")

    except Exception as e:
        logger.error(f"Failed to read or upload log file: {e}")
        raise HTTPException(status_code=500, detail="Failed to read or upload log file")
