import os
import sys
import asyncio
import logging
import time
import re
from contextlib import asynccontextmanager
from loguru import logger
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from program import Program
from program.settings.models import get_version
from routers import app_router
from program.file_watcher import start_all_watchers
from integrations.seasonarr.api.routers import router_ws as seasonarr_ws_router

COOKIE_DOMAIN = os.getenv("COOKIE_DOMAIN")

# ===== ICONES =====
ORIGINAL_LOG_METHOD = logger._log
ICON_MAP = {
    "DEBUG": "🐞",
    "INFO": "ℹ️",
    "SUCCESS": "✅",
    "WARNING": "⚠️",
    "ERROR": "❌",
    "CRITICAL": "💀",
    "SEASONARR": "🎬",
    "WATCHER": "🛰️",
    "SYSTEM": "⚙️",
    "CONTAINER": "📦",
    "SSD": "🖥️",
    "WEBSOCKET": "🔌",
    "API": "📡",
    "DEFAULT": "📝"
}

def safe_log(self, level_id, from_decorator, options, message, args, kwargs):
    extra = options.get("extra") if isinstance(options, dict) else {}
    msg_str = str(message)

    watcher_keywords = ["🛰️", "YAML watcher", "Symlink watcher", "Surveillance active", "🚀 Lancement des watchers"]
    system_keywords = ["Scheduler Docker", "Planificateur", "Initialized services", "Startup sequence", "SSDv2 - Startup sequence"]
    container_keywords = ["Domaines générés"]
    ssd_keywords = ["Exécution du script", "SSDv2", "/ssd-", "Déchiffrement du fichier", "Vérification du fichier"]
    websocket_keywords = ["websocket", "WebSocket", "ws://", "wss://", "WebSocketDisconnect"]

    if any(keyword in msg_str for keyword in watcher_keywords):
        level_id = "WATCHER"
    elif any(keyword in msg_str for keyword in system_keywords):
        level_id = "SYSTEM"
    elif any(keyword in msg_str for keyword in container_keywords):
        level_id = "CONTAINER"
    elif any(keyword in msg_str for keyword in ssd_keywords):
        level_id = "SSD"
    elif any(keyword in msg_str for keyword in websocket_keywords):
        level_id = "WEBSOCKET"

    if "icon" not in extra:
        try:
            level_name = level_id if isinstance(level_id, str) else self.level(level_id).name
        except Exception:
            level_name = "DEFAULT"
        extra["icon"] = ICON_MAP.get(level_name, ICON_MAP["DEFAULT"])
        if isinstance(options, dict):
            options["extra"] = extra

    return ORIGINAL_LOG_METHOD(level_id, from_decorator, options, message, args, kwargs)

logger._log = safe_log.__get__(logger, type(logger))


# ===== CONFIG LOGURU =====
os.environ["FORCE_COLOR"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1"
logger.remove()

LEVEL_STYLES = {
    "DEBUG": {"color": "<fg #00CED1>", "icon": ICON_MAP["DEBUG"]},         
    "INFO": {"color": "<fg #5DAEFF>", "icon": ICON_MAP["INFO"]},
    "SUCCESS": {"color": "<fg #32CD32><bold>", "icon": ICON_MAP["SUCCESS"]},
    "WARNING": {"color": "<fg #FFD700>", "icon": ICON_MAP["WARNING"]},
    "ERROR": {"color": "<fg #FF6B6B>", "icon": ICON_MAP["ERROR"]},
    "CRITICAL": {"color": "<fg #FF0000><bold>", "icon": ICON_MAP["CRITICAL"]},
    "SEASONARR": {"color": "<fg #FECACA>", "icon": ICON_MAP["SEASONARR"]},
    "WATCHER": {"color": "<fg #A1C4FD>", "icon": ICON_MAP["WATCHER"]},
    "SYSTEM": {"color": "<fg #FF9800>", "icon": ICON_MAP["SYSTEM"]},
    "CONTAINER": {"color": "<fg #00CED1>", "icon": ICON_MAP["CONTAINER"]},
    "SSD": {"color": "<fg #9370DB>", "icon": ICON_MAP["SSD"]},
    "WEBSOCKET": {"color": "<fg #FF4500>", "icon": ICON_MAP["WEBSOCKET"]},
    "API": {"color": "<fg #00BFFF>", "icon": ICON_MAP["API"]},  
    "DEFAULT": {"color": "<fg #A9A9A9>", "icon": ICON_MAP["DEFAULT"]}
}

# Fonction pour créer ou mettre à jour un niveau
def ensure_level(name, no=None):
    style = LEVEL_STYLES[name]
    try:
        logger.level(name)
        # Si le niveau existe déjà → on met juste à jour couleur & icône
        logger.level(name, color=style["color"], icon=style["icon"])
    except ValueError:
        # Si le niveau n’existe pas → on le crée avec numéro
        if no is None:
            raise ValueError(f"Level '{name}' needs a severity number for creation")
        logger.level(name, no=no, color=style["color"], icon=style["icon"])

# Niveaux custom
ensure_level("SEASONARR", 25)
ensure_level("WATCHER", 25)
ensure_level("SYSTEM", 26)
ensure_level("CONTAINER", 24)
ensure_level("SSD", 27)
ensure_level("WEBSOCKET", 28)
ensure_level("API", 29)
ensure_level("DEFAULT", 15)

# Override niveaux standard
for lvl in ["DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]:
    ensure_level(lvl)

# ===== FORMAT LOG =====
def mask_sensitive_data(msg: str) -> str:
    return re.sub(r"(token|password|pass|secret)=([^\s&]+)", r"\1=***", msg, flags=re.IGNORECASE)

def custom_format(record):
    allowed_modules = ("src.", "program.", "routers.", "integrations.")
    excluded_levels = ("SEASONARR", "WEBSOCKET", "API")

    if record["name"].startswith(allowed_modules) and record["level"].name not in excluded_levels:
        location = f"| <magenta>{record['name']}</magenta>:<cyan>{record['function']}</cyan>:<yellow>{record['line']}</yellow> "
    else:
        location = ""

    icon = record['extra'].get('icon') or ICON_MAP.get(record['level'].name, ICON_MAP["DEFAULT"])
    safe_message = mask_sensitive_data(str(record['message']).replace("{", "{{").replace("}", "}}"))

    return (
        f"<level>{record['time']:DD/MM/YYYY HH:mm:ss.SSS}</level> "
        f"| <level>{icon}</level> "
        f"<level>{record['level']:<10}</level> "
        f"{location}"
        f"| <level>{safe_message}</level>\n"
    )

# Rotation auto + compression
logger.add(
    sink=sys.stdout,
    colorize=True,
    format=custom_format,
    backtrace=True,
    diagnose=True,
    enqueue=True
)
logger.add("logs/app.log", rotation="7 days", compression="zip", enqueue=True, format=custom_format)


# ===== INTERCEPT HANDLER =====
class InterceptHandler(logging.Handler):
    ALLOWED_MODULES = ("src.main", "program.file_watcher", "routers", "integrations")

    def emit(self, record):
        if not record.name.startswith(self.ALLOWED_MODULES):
            return

        if ".websocket" in record.name.lower():
            level = "WEBSOCKET"
        elif record.name.startswith("integrations.seasonarr"):
            level = "SEASONARR"
        elif record.name.startswith("program.file_watcher"):
            level = "WATCHER"
        else:
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = "DEFAULT"

        logger.opt(depth=6, exception=record.exc_info).log(level, record.getMessage())


def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
    logging.root.handlers = [InterceptHandler()]
    logging.root.setLevel(log_level)
    for name in list(logging.root.manager.loggerDict.keys()):
        log = logging.getLogger(name)
        log.handlers = [InterceptHandler()]
        log.propagate = False
        log.setLevel(log_level)

setup_logging()


# ===== FASTAPI LIFESPAN =====
load_dotenv()
cookie_domain = os.getenv("COOKIE_DOMAIN")
origins = [f"https://{cookie_domain}"] if cookie_domain else []
logger.info(f"🚀 CORS ORIGINS: {origins}")

async def start_watchers_async():
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, start_all_watchers)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.log("SYSTEM", "🚀 SSDv2 - Startup sequence...")
    app.program = Program()
    app.program.start()
    asyncio.create_task(start_watchers_async())
    logger.success("✅ SSD is now running!")
    yield
    logger.log("SYSTEM", "🛑 SSD - Shutdown sequence")

# ===== APP FASTAPI =====
app = FastAPI(
    title="SSD",
    summary="A media management system.",
    version=get_version(),
    redoc_url=None,
    license_info={"name": "GPL-3.0", "url": "https://www.gnu.org/licenses/gpl-3.0.en.html"},
    lifespan=lifespan,
    redirect_slashes=False
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[COOKIE_DOMAIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== MIDDLEWARE LOG API =====
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    try:
        response = await call_next(request)
    except Exception as e:
        import traceback
        print("🔥 TRACEBACK START 🔥")
        traceback.print_exc()
        print("🔥 TRACEBACK END 🔥")
        raise

    # ⛔ Filtrer les logs SSE
    if request.url.path != "/api/v1/symlinks/events":
        process_time = (time.time() - start_time) * 1000
        status = response.status_code
        size_kb = len(response.body or b"") / 1024 if hasattr(response, "body") else 0
        client_ip = request.client.host if request.client else "unknown"

    return response


# ===== ROUTERS =====
app.include_router(app_router)
app.include_router(seasonarr_ws_router)


if __name__ == "__main__":
    import uvicorn
    logger.info("💡 Starting development server...")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
