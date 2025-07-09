from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from loguru import logger

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

# Inclusion des routes sans préfixe
app.include_router(app_router)

logger.info("SSD a bien démarré")
