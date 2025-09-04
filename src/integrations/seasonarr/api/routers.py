from typing import List, Optional
from datetime import datetime
import json
import time
import asyncio
import logging
import os
from datetime import timedelta
from fastapi.responses import JSONResponse


# FastAPI
from fastapi import (
    APIRouter, Depends, HTTPException, status,
    WebSocket, WebSocketDisconnect, Request, Query, Response
)
from fastapi.responses import StreamingResponse

# JWT
from jose import jwt, JWTError
from integrations.seasonarr.core.auth import SECRET_KEY, ALGORITHM
from jose.exceptions import ExpiredSignatureError  # utile si tu gères l’expiration

# SQLAlchemy
from sqlalchemy.orm import Session

# === Seasonarr imports ===
from integrations.seasonarr.core.auth import (
    authenticate_user,
    create_access_token,
    verify_token,
    get_current_user,
    COOKIE_SECURE,
    COOKIE_DOMAIN
)
from integrations.seasonarr.db.database import (
    get_db, init_db, SessionLocal, check_if_first_run
)
from integrations.seasonarr.db.models import (
    User, SonarrInstance, UserSettings, Notification, ActivityLog
)
from integrations.seasonarr.db.schemas import (
    UserLogin, UserRegister, SonarrInstanceCreate, SonarrInstanceUpdate,
    SonarrInstanceResponse, SeasonItRequest, UserSettingsResponse, UserSettingsUpdate,
    NotificationResponse, NotificationUpdate, ActivityLogResponse,
    SearchSeasonPacksRequest, DownloadReleaseRequest, ReleaseResponse
)
from integrations.seasonarr.core.websocket_manager import manager
from integrations.seasonarr.core.cache import cache, get_cache_key
from integrations.seasonarr.clients.sonarr_client import SonarrClient, test_sonarr_connection
from integrations.seasonarr.services.season_it_service import SeasonItService
from integrations.seasonarr.services.bulk_operation_manager import bulk_operation_manager
from jose import JWTError
from fastapi import status
from integrations.seasonarr.core.auth import verify_token

import httpx

# Logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Routers
router = APIRouter(tags=["seasonarr"])          # HTTP
router_ws = APIRouter(tags=["seasonarr-ws"])    # WebSocket


# -------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------

@router.websocket("/ws/{user_id}")
async def websocket_secure(websocket: WebSocket, user_id: int, db: Session = Depends(get_db)):
    try:
        # ✅ Récupère le token depuis les cookies
        token = websocket.cookies.get("access_token")
        if not token:
            await websocket.close(code=1008, reason="Missing token")
            return

        # ✅ Décoder le JWT avec ta fonction standard
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            await websocket.close(code=1008, reason="Invalid token payload")
            return

        user = db.query(User).filter(User.username == username).first()
        if not user or user.id != user_id:
            await websocket.close(code=1008, reason="Unauthorized user")
            return

        # Connexion au manager
        await manager.connect(websocket, user_id)
        logger.info(f"WebSocket connecté: {username} (id={user_id})")

        while True:
            data = await websocket.receive_text()
            logger.debug(f"WS message reçu: {data}")

    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)
        logger.info("WebSocket déconnecté")
    finally:
        db.close()

# -------------------------------------------------------------------
# Health / Setup / Auth basique
# -------------------------------------------------------------------
@router.get("/health")
async def health_check():
    return {"status": "healthy", "service": "seasonarr-api"}

@router.on_event("startup")
async def startup_event():
    init_db()
    logger.info("Database initialized")


@router.get("/setup/first-run")
async def check_first_run():
    from src.integrations.seasonarr.db.database import check_if_first_run
    return {"is_first_run": check_if_first_run()}


from fastapi.responses import JSONResponse
from integrations.seasonarr.core.auth import create_access_token, get_password_hash

@router.post("/setup/register")
async def register_first_user(user_data: UserRegister, response: Response, db: Session = Depends(get_db)):
    from src.integrations.seasonarr.db.database import check_if_first_run

    if not check_if_first_run():
        raise HTTPException(status_code=400, detail="Registration is only allowed during first run")

    existing_user = db.query(User).filter(User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")

    hashed_password = get_password_hash(user_data.password)
    user = User(username=user_data.username, hashed_password=hashed_password)
    db.add(user)
    db.commit()
    db.refresh(user)

    # ✅ Génère le token JWT
    access_token = create_access_token(data={"sub": user.username})

    # ✅ Dépose le cookie dans la réponse
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=COOKIE_SECURE,   # dépend de ton HTTPS
        samesite="none",
        path="/"
    )

    return {
        "message": "Registration successful",
        "user": {"id": user.id, "username": user.username}
    }

@router.post("/login")
async def login(response: Response, user_data: dict):
    remember_me = user_data.get("remember_me", False)

    # expire dans 30 jours si remember_me activé
    expires_delta = timedelta(days=30) if remember_me else None

    access_token = create_access_token(
        data={"sub": user_data["username"]},
        expires_delta=expires_delta
    )

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=COOKIE_SECURE,   # True car HTTPS
        samesite="none",         # accepte navigation interne / cross-path
        path="/"                # valable pour /settings et /season

    )

    return {
        "message": "Login successful",
        "user": {"id": 1, "username": user_data["username"]}
    }


@router.get("/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return {"username": current_user.username, "id": current_user.id}


@router.post("/logout")
async def logout():
    response = JSONResponse({"message": "Logged out"})
    response.delete_cookie(
        key="access_token",
        path="/",
        secure=COOKIE_SECURE,
        samesite="none"
    )
    return response

# -------------------------------------------------------------------
# Sonarr instances
# -------------------------------------------------------------------
@router.post("/sonarr", response_model=SonarrInstanceResponse)
async def create_sonarr_instance(
    instance_data: SonarrInstanceCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not await test_sonarr_connection(instance_data.url, instance_data.api_key):
        raise HTTPException(status_code=400, detail="Could not connect to Sonarr instance")

    db_instance = SonarrInstance(
        name=instance_data.name,
        url=instance_data.url,
        api_key=instance_data.api_key,
        owner_id=current_user.id
    )
    db.add(db_instance)
    db.commit()
    db.refresh(db_instance)
    return db_instance


@router.get("/sonarr", response_model=list[SonarrInstanceResponse])
async def get_sonarr_instances(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    instances = (
        db.query(SonarrInstance)
        .filter(SonarrInstance.owner_id == current_user.id, SonarrInstance.is_active == True)
        .order_by(SonarrInstance.created_at.desc())
        .all()
    )
    return instances


@router.put("/sonarr/{instance_id}", response_model=SonarrInstanceResponse)
async def update_sonarr_instance(
    instance_id: int,
    instance_data: SonarrInstanceUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    instance = (
        db.query(SonarrInstance)
        .filter(
            SonarrInstance.id == instance_id,
            SonarrInstance.owner_id == current_user.id,
            SonarrInstance.is_active == True
        )
        .first()
    )
    if not instance:
        raise HTTPException(status_code=404, detail="Sonarr instance not found")

    if instance_data.url or instance_data.api_key:
        test_url = instance_data.url or instance.url
        test_api_key = instance_data.api_key or instance.api_key
        if not await test_sonarr_connection(test_url, test_api_key):
            raise HTTPException(status_code=400, detail="Could not connect to Sonarr instance with provided settings")

    if instance_data.name is not None:
        instance.name = instance_data.name
    if instance_data.url is not None:
        instance.url = instance_data.url
    if instance_data.api_key is not None:
        instance.api_key = instance_data.api_key
    if instance_data.is_active is not None:
        instance.is_active = instance_data.is_active

    db.commit()
    db.refresh(instance)
    return instance


@router.post("/sonarr/test-connection")
async def test_sonarr_connection_endpoint(
    instance_data: SonarrInstanceCreate,
    current_user: User = Depends(get_current_user)
):
    success = await test_sonarr_connection(instance_data.url, instance_data.api_key)
    return {"success": bool(success), "message": "Connection successful" if success else "Connection failed"}


@router.post("/sonarr/{instance_id}/test-connection")
async def test_existing_sonarr_connection(
    instance_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    instance = (
        db.query(SonarrInstance)
        .filter(
            SonarrInstance.id == instance_id,
            SonarrInstance.owner_id == current_user.id,
            SonarrInstance.is_active == True
        )
        .first()
    )
    if not instance:
        raise HTTPException(status_code=404, detail="Sonarr instance not found")

    success = await test_sonarr_connection(instance.url, instance.api_key)
    return {"success": bool(success), "message": "Connection successful" if success else "Connection failed"}


@router.delete("/sonarr/{instance_id}")
async def delete_sonarr_instance(
    instance_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    instance = (
        db.query(SonarrInstance)
        .filter(SonarrInstance.id == instance_id, SonarrInstance.owner_id == current_user.id)
        .first()
    )
    if not instance:
        raise HTTPException(status_code=404, detail="Sonarr instance not found")

    instance.is_active = False
    db.commit()
    return {"message": "Sonarr instance deleted successfully"}


# -------------------------------------------------------------------
# Shows & filters
# -------------------------------------------------------------------
@router.get("/shows")
async def get_shows(
    instance_id: int,
    page: int = 1,
    page_size: int = 35,
    search: str = "",
    show_status: str = "",
    monitored: bool | None = None,
    missing_episodes: bool | None = None,
    network: str = "",
    genres: Optional[List[str]] = Query(None),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    runtime_min: Optional[int] = Query(None),
    runtime_max: Optional[int] = Query(None),
    certification: str = "",
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    instance = (
        db.query(SonarrInstance)
        .filter(
            SonarrInstance.id == instance_id,
            SonarrInstance.owner_id == current_user.id,
            SonarrInstance.is_active == True
        )
        .first()
    )
    if not instance:
        raise HTTPException(status_code=404, detail="Sonarr instance not found")

    client = SonarrClient(instance.url, instance.api_key, instance.id)
    try:
        return await client.get_series(
            page=page,
            page_size=page_size,
            search=search,
            status=show_status,
            monitored=monitored,
            missing_episodes=missing_episodes,
            network=network,
            genres=genres or [],
            year_from=year_from,
            year_to=year_to,
            runtime_min=runtime_min,
            runtime_max=runtime_max,
            certification=certification,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching shows: {e}")


@router.get("/shows/filter-options")
async def get_filter_options(
    instance_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    instance = (
        db.query(SonarrInstance)
        .filter(
            SonarrInstance.id == instance_id,
            SonarrInstance.owner_id == current_user.id,
            SonarrInstance.is_active == True
        )
        .first()
    )
    if not instance:
        raise HTTPException(status_code=404, detail="Sonarr instance not found")

    cache_key = get_cache_key("filter_options", instance_id)
    cached_result = cache.get(cache_key)
    if cached_result is not None:
        return cached_result

    client = SonarrClient(instance.url, instance.api_key, instance.id)
    try:
        all_shows_response = await client.get_series(page=1, page_size=1000)
        shows = all_shows_response["shows"]

        networks, genres, certifications = set(), set(), set()
        years, runtimes = [], []

        for show in shows:
            if getattr(show, "network", None):
                networks.add(show.network)
            if getattr(show, "genres", None):
                genres.update(show.genres)
            if getattr(show, "certification", None):
                certifications.add(show.certification)
            if getattr(show, "year", None):
                years.append(show.year)
            if getattr(show, "runtime", None):
                runtimes.append(show.runtime)

        result = {
            "networks": sorted(list(networks)),
            "genres": sorted(list(genres)),
            "certifications": sorted(list(certifications)),
            "year_range": {"min": min(years) if years else None, "max": max(years) if years else None},
            "runtime_range": {"min": min(runtimes) if runtimes else None, "max": max(runtimes) if runtimes else None},
        }

        cache.set(cache_key, result, ttl=1800)  # 30 min
        return result

    except Exception as e:
        logger.error(f"Error fetching filter options: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching filter options: {e}")


@router.get("/shows/{show_id}")
async def get_show_detail(
    show_id: int,
    instance_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    instance = (
        db.query(SonarrInstance)
        .filter(
            SonarrInstance.id == instance_id,
            SonarrInstance.owner_id == current_user.id,
            SonarrInstance.is_active == True
        )
        .first()
    )
    if not instance:
        raise HTTPException(status_code=404, detail="Sonarr instance not found")

    client = SonarrClient(instance.url, instance.api_key, instance.id)
    try:
        return await client.get_show_detail(show_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching show detail: {e}")


# -------------------------------------------------------------------
# Activity logs
# -------------------------------------------------------------------
@router.get("/activity-logs", response_model=List[ActivityLogResponse])
async def get_activity_logs(
    instance_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 20,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    query = db.query(ActivityLog).filter(ActivityLog.user_id == current_user.id)
    if instance_id:
        query = query.filter(ActivityLog.instance_id == instance_id)

    query = query.order_by(ActivityLog.created_at.desc())
    offset = (page - 1) * page_size
    logs = query.offset(offset).limit(page_size).all()
    return logs

# -------------------------------------------------------------------
# Proxy image
# -------------------------------------------------------------------
@router.get("/proxy-image")
async def proxy_image(
    url: str,
    instance_id: int,
    request: Request
):
    import httpx
    from fastapi.responses import StreamingResponse
    from urllib.parse import unquote
    from sqlalchemy.orm import Session
    from ..db.database import SessionLocal
    from ..db.models import SonarrInstance

    # Connexion DB
    try:
        db: Session = SessionLocal()
    except Exception as e:
        logger.error(f"[proxy_image] ERREUR DB: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        instance = db.query(SonarrInstance).filter(
            SonarrInstance.id == instance_id,
            SonarrInstance.is_active.is_(True)
        ).first()

        if not instance:
            logger.error(f"[proxy_image] Sonarr instance {instance_id} not found or inactive")
            raise HTTPException(status_code=404, detail="Sonarr instance not found")

        instance_url = instance.url.rstrip("/")
        instance_api_key = instance.api_key
    finally:
        db.close()

    decoded_url = unquote(url)
    if decoded_url.startswith('/MediaCover/'):
        api_url = decoded_url.replace('/MediaCover/', '/api/v3/MediaCover/')
    else:
        api_url = decoded_url

    full_url = f"{instance_url}{api_url}"
    headers = {"X-Api-Key": instance_api_key}

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(full_url, headers=headers)
    except Exception as e:
        logger.error(f"[proxy_image] HTTP request failed: {e}")
        raise HTTPException(status_code=500, detail=f"HTTP request failed: {str(e)}")

    if resp.status_code != 200:
        logger.error(f"[proxy_image] Sonarr returned {resp.status_code} for {full_url}")
        raise HTTPException(status_code=resp.status_code, detail="Image not found")

    return StreamingResponse(
        iter([resp.content]),
        media_type=resp.headers.get("content-type", "image/jpeg"),
        headers={
            "Cache-Control": "public, max-age=3600",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET",
            "Access-Control-Allow-Headers": "*",
        }
    )

# -------------------------------------------------------------------
# Season It & bulk
# -------------------------------------------------------------------
@router.post("/season-it")
async def season_it(
    request: SeasonItRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    service = SeasonItService(db, current_user.id)
    try:
        result = await service.process_season_it(request.show_id, request.season_number, request.instance_id)
        return {"message": "Season It process completed", "result": result}
    except Exception as e:
        logger.error(f"Season It error: {e}")
        raise HTTPException(status_code=500, detail=f"Season It failed: {e}")


@router.post("/bulk-season-it")
async def bulk_season_it(
    request: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    show_items = request.get("show_items", [])
    if not show_items:
        raise HTTPException(status_code=400, detail="No shows provided")

    service = SeasonItService(db, current_user.id)
    try:
        result = await service.process_bulk_season_it(show_items)
        return {"message": "Bulk Season It process completed", "result": result}
    except Exception as e:
        logger.error(f"Bulk Season It error: {e}")
        raise HTTPException(status_code=500, detail=f"Bulk Season It failed: {e}")


@router.post("/search-season-packs")
async def search_season_packs(
    request: SearchSeasonPacksRequest,
    http_request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    logger.info(f"🔍 RECHERCHE: show_id={request.show_id}, season={request.season_number}, instance_id={request.instance_id}")
    service = SeasonItService(db, current_user.id)

    try:
        if await http_request.is_disconnected():
            raise HTTPException(status_code=499, detail="Client disconnected")

        search_task = asyncio.create_task(
            service.search_season_packs_interactive(
                request.show_id,
                request.season_number,
                request.instance_id
            )
        )

        while not search_task.done():
            if await http_request.is_disconnected():
                search_task.cancel()
                try:
                    await search_task
                except asyncio.CancelledError:
                    pass
                await manager.send_personal_message({
                    "type": "clear_progress",
                    "operation_type": "interactive_search",
                    "message": "🚫 Search operation cancelled by user"
                }, current_user.id)
                raise HTTPException(status_code=499, detail="Client disconnected")
            await asyncio.sleep(0.1)

        releases = await search_task
        return {"releases": releases}
    except asyncio.CancelledError:
        await manager.send_personal_message({
            "type": "clear_progress",
            "operation_type": "interactive_search",
            "message": "🚫 Search operation cancelled by user"
        }, current_user.id)
        raise HTTPException(status_code=499, detail="Operation cancelled")
    except Exception as e:
        logger.error(f"Season pack search error: {e}")
        raise HTTPException(status_code=500, detail=f"Season pack search failed: {e}")


@router.post("/download-release")
async def download_release(
    request: DownloadReleaseRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    service = SeasonItService(db, current_user.id)
    try:
        result = await service.download_specific_release(
            request.release_guid,
            request.show_id,
            request.season_number,
            request.instance_id,
            request.indexer_id
        )
        return {"message": "Release download initiated", "result": result}
    except Exception as e:
        logger.error(f"Release download error: {e}")
        raise HTTPException(status_code=500, detail=f"Release download failed: {e}")


# -------------------------------------------------------------------
# Operations
# -------------------------------------------------------------------
@router.post("/operations/{operation_id}/cancel")
async def cancel_operation(
    operation_id: str,
    current_user: User = Depends(get_current_user)
):
    try:
        success = bulk_operation_manager.cancel_operation(operation_id)
        if success:
            return {"message": "Operation cancelled successfully"}
        else:
            raise HTTPException(status_code=404, detail="Operation not found")
    except Exception as e:
        logger.error(f"Error cancelling operation {operation_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel operation: {e}")


@router.get("/operations/{operation_id}")
async def get_operation_status(
    operation_id: str,
    current_user: User = Depends(get_current_user)
):
    try:
        operation = bulk_operation_manager.get_operation_status(operation_id)
        if operation:
            return operation
        else:
            raise HTTPException(status_code=404, detail="Operation not found")
    except Exception as e:
        logger.error(f"Error getting operation status {operation_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get operation status: {e}")


@router.get("/operations")
async def get_user_operations(
    current_user: User = Depends(get_current_user)
):
    try:
        operations = bulk_operation_manager.get_user_operations(current_user.id)
        return {"operations": operations}
    except Exception as e:
        logger.error(f"Error getting user operations: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get operations: {e}")


# -------------------------------------------------------------------
# Settings
# -------------------------------------------------------------------
@router.get("/settings", response_model=UserSettingsResponse)
async def get_user_settings(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    settings = db.query(UserSettings).filter(UserSettings.user_id == current_user.id).first()
    if not settings:
        settings = UserSettings(user_id=current_user.id)
        db.add(settings)
        db.commit()
        db.refresh(settings)
    return settings


@router.put("/settings", response_model=UserSettingsResponse)
async def update_user_settings(
    settings_update: UserSettingsUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    settings = db.query(UserSettings).filter(UserSettings.user_id == current_user.id).first()
    if not settings:
        settings = UserSettings(user_id=current_user.id)
        db.add(settings)

    update_data = settings_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(settings, field, value)

    db.commit()
    db.refresh(settings)
    return settings


@router.delete("/purge-database")
async def purge_user_database(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        db.query(ActivityLog).filter(ActivityLog.user_id == current_user.id).delete()
        db.query(Notification).filter(Notification.user_id == current_user.id).delete()
        db.query(UserSettings).filter(UserSettings.user_id == current_user.id).delete()
        db.query(SonarrInstance).filter(SonarrInstance.owner_id == current_user.id).delete()
        db.commit()
        logger.info(f"User {current_user.username} purged all database data")
        return {"message": "Database purged successfully. All user data has been deleted."}
    except Exception as e:
        db.rollback()
        logger.error(f"Purge error for user {current_user.username}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to purge database: {e}")


# -------------------------------------------------------------------
# Notifications
# -------------------------------------------------------------------
@router.get("/notifications", response_model=list[NotificationResponse])
async def get_notifications(
    skip: int = 0,
    limit: int = 50,
    unread_only: bool = False,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if unread_only:
        notifications = (
            db.query(Notification)
            .filter(Notification.user_id == current_user.id, Notification.read == False)
            .order_by(Notification.created_at.desc())
            .offset(skip).limit(limit).all()
        )
    else:
        notifications = (
            db.query(Notification)
            .filter(Notification.user_id == current_user.id)
            .order_by(Notification.created_at.desc())
            .offset(skip).limit(limit).all()
        )
    return notifications


@router.get("/notifications/unread-count")
async def get_unread_count(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    count = db.query(Notification).filter(
        Notification.user_id == current_user.id, Notification.read == False
    ).count()
    return {"count": count}


@router.put("/notifications/{notification_id}", response_model=NotificationResponse)
async def update_notification(
    notification_id: int,
    notification_update: NotificationUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    notification = (
        db.query(Notification)
        .filter(Notification.id == notification_id, Notification.user_id == current_user.id)
        .first()
    )
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")

    if notification_update.read is not None:
        notification.read = notification_update.read
        notification.read_at = datetime.utcnow() if notification_update.read else None

    db.commit()
    db.refresh(notification)
    return notification


@router.put("/notifications/mark-all-read")
async def mark_all_read(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    db.query(Notification).filter(
        Notification.user_id == current_user.id, Notification.read == False
    ).update({"read": True, "read_at": datetime.utcnow()})
    db.commit()
    return {"message": "All notifications marked as read"}


@router.delete("/notifications/{notification_id}")
async def delete_notification(
    notification_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    notification = (
        db.query(Notification)
        .filter(Notification.id == notification_id, Notification.user_id == current_user.id)
        .first()
    )
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")

    db.delete(notification)
    db.commit()
    return {"message": "Notification deleted"}


@router.delete("/notifications")
async def clear_all_notifications(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    db.query(Notification).filter(Notification.user_id == current_user.id).delete()
    db.commit()
    return {"message": "All notifications cleared"}


@router.post("/notifications/test")
async def test_notification(current_user: User = Depends(get_current_user)):
    await manager.send_notification(
        user_id=current_user.id,
        title="Test Notification",
        message="This is a test notification to verify the WebSocket system is working properly.",
        notification_type="info",
        priority="normal",
        persistent=False
    )
    return {"message": "Test notification sent"}


# -------------------------------------------------------------------
# WebSocket / Cache stats
# -------------------------------------------------------------------
@router.get("/websocket/stats")
async def websocket_stats(current_user: User = Depends(get_current_user)):
    return manager.get_connection_stats()


@router.get("/cache/stats")
async def cache_stats(current_user: User = Depends(get_current_user)):
    return cache.stats()


@router.post("/cache/clear")
async def clear_cache(current_user: User = Depends(get_current_user)):
    cache.clear()
    return {"message": "Cache cleared successfully"}
