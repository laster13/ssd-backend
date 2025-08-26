from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# --- DB URL (par défaut SQLite mais tu peux mettre PostgreSQL/MySQL via env var) ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./seasonarr.db")

# --- Engine ---
if DATABASE_URL.startswith("sqlite"):
    # SQLite specific config
    engine = create_engine(
        DATABASE_URL,
        connect_args={
            "check_same_thread": False,
            "timeout": 30,
            "isolation_level": None
        },
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )
else:
    # Other DB (Postgres, MySQL…)
    engine = create_engine(
        DATABASE_URL,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Base ORM ---
Base = declarative_base()


# --- Dépendance pour FastAPI ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- Initialisation DB ---
def init_db():
    # Ensure data dir exists (utile pour SQLite local)
    db_path = DATABASE_URL.replace("sqlite:///", "")
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    # ⚡ imports relatifs adaptés à ta structure Svelte/FastAPI
    from integrations.seasonarr.db.models import User, SonarrInstance, UserSettings
    Base.metadata.create_all(bind=engine)


def check_if_first_run():
    from integrations.seasonarr.db.models import User
    db = SessionLocal()
    try:
        user_count = db.query(User).count()
        return user_count == 0
    finally:
        db.close()
