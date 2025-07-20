import os
import sys
from routers.secure.symlinks import scan_symlinks, symlink_store

# 🔽 Ajoute ça immédiatement après les imports système
os.environ["FORCE_COLOR"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1"

import logging
from loguru import logger

# Intercepteur pour basculer logging → loguru
class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelname
        logger.log(level, record.getMessage())

# Active loguru pour tous les logs standards
logging.basicConfig(handlers=[InterceptHandler()], level=0)
for name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
    logging.getLogger(name).handlers = [InterceptHandler()]

# ⛔ Supprime les logs DEBUG de watchdog
logging.getLogger("watchdog").setLevel(logging.WARNING)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from program import Program
from program.settings.models import get_version
from routers import app_router
from program.file_watcher import start_all_watchers

load_dotenv()

app = FastAPI(
    title="SSD",
    summary="A media management system.",
    version=get_version(),
    redoc_url=None,
    license_info={
        "name": "GPL-3.0",
        "url": "https://www.gnu.org/licenses/gpl-3.0.en.html",
    },
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Programme principal
app.program = Program()
app.program.start()

@app.on_event("startup")
def launch_watchers():
    start_all_watchers()

# Inclusion des routes
app.include_router(app_router)

logger.info("SSD a bien démarré")
