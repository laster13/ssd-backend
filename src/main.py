import os
import sys
from contextlib import asynccontextmanager
from routers.secure.symlinks import scan_symlinks, symlink_store

# 🔽 Variables d'env terminal
os.environ["FORCE_COLOR"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1"

import logging
from loguru import logger

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING) 
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("anyio").setLevel(logging.WARNING)

# Logging bridge
class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelname
        logger.log(level, record.getMessage())

logging.basicConfig(handlers=[InterceptHandler()], level=0)
for name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
    logging.getLogger(name).handlers = [InterceptHandler()]
logging.getLogger("watchdog").setLevel(logging.WARNING)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from program import Program
from program.settings.models import get_version
from routers import app_router
from program.file_watcher import start_all_watchers
import asyncio

load_dotenv()

# Watchers lancés proprement avec asyncio
async def start_watchers_async():
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, start_all_watchers)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 SSDv2 - startup sequence...")
    app.program = Program()
    app.program.start()

    # Lancement des watchers sans bloquer le démarrage
    asyncio.create_task(start_watchers_async())

    logger.info("SSD a bien démarré")
    yield
    logger.info("🛑 SSD - shutdown sequence")

app = FastAPI(
    title="SSD",
    summary="A media management system.",
    version=get_version(),
    redoc_url=None,
    license_info={
        "name": "GPL-3.0",
        "url": "https://www.gnu.org/licenses/gpl-3.0.en.html",
    },
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(app_router)
