from datetime import datetime, timedelta
from typing import Optional

from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os

from ..db.database import get_db
from ..db.models import User
from program.settings.manager import settings_manager


COOKIE_DOMAIN = os.getenv("COOKIE_DOMAIN", None)
COOKIE_SECURE = True
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)


def get_password_hash(password: str):
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str):
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta is not None:
        expire = datetime.utcnow() + expires_delta
        to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def _get_default_user_when_auth_disabled(db: Session) -> User:
    """
    Quand l'authentification est désactivée globalement,
    on retourne le premier utilisateur existant pour conserver
    l'accès aux données liées au user_id (settings, instances, notifications, etc.).
    """
    user = db.query(User).order_by(User.id.asc()).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No Seasonarr user found while authentication is disabled"
        )
    return user


def verify_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """
    Vérifie le JWT dans l'Authorization header ou dans le cookie HttpOnly.
    Retourne directement l'utilisateur (User) depuis la base.
    """

    # ✅ bypass global si l'auth SSD est désactivée
    if not settings_manager.settings.auth_enabled:
        return _get_default_user_when_auth_disabled(db)

    token = None

    # 1. Vérifier si un header Authorization: Bearer est présent
    if credentials and credentials.scheme.lower() == "bearer":
        token = credentials.credentials
    else:
        # 2. Sinon, essayer dans le cookie HttpOnly
        token = request.cookies.get("access_token")

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token"
        )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload"
            )
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired"
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

    # 3. Charger l'utilisateur depuis la DB
    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    return user


def authenticate_user(db: Session, username: str, password: str):
    user = db.query(User).filter(User.username == username).first()
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def get_current_user(current_user: User = Depends(verify_token)) -> User:
    """
    Dépendance pratique pour récupérer l'utilisateur courant.
    """
    return current_user