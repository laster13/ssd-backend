from typing import Optional

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from program.settings.manager import settings_manager


def header_auth(
    header: Optional[str] = Security(APIKeyHeader(name="x-api-key", auto_error=False))
):
    return header == settings_manager.settings.api_key


def bearer_auth(
    bearer: Optional[HTTPAuthorizationCredentials] = Security(HTTPBearer(auto_error=False))
):
    return bearer and bearer.credentials == settings_manager.settings.api_key


def resolve_api_key(
    header: Optional[bool] = Security(header_auth),
    bearer: Optional[bool] = Security(bearer_auth),
):
    if not settings_manager.settings.auth_enabled:
        return

    if not (header or bearer):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )