from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from dotenv import load_dotenv

from program import Program
from program.settings.models import get_version
from routers import app_router

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

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # ✅ dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialisation du programme principal
app.program = Program()
app.program.start()

# Inclusion des routes FastAPI
app.include_router(app_router)

logger.info("SSD a bien demarré")
