import os
import threading
import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from program.managers.event_manager import EventManager
from program.settings.manager import settings_manager
from program.settings.models import get_version
from program.utils import data_dir_path
from program.utils.logging import log_cleaner, logger


class DummyService:
    """Service minimaliste pour satisfaire /api/v1/services"""
    def __init__(self, key: str):
        self.key = key
        self.initialized = True
        self.services = {}  # sous-services éventuels


class Program(threading.Thread):
    """Simplified Program class without media/db"""

    def __init__(self):
        super().__init__(name="SSDv2")
        self.initialized = False
        self.running = False
        self.services = {}
        self.requesting_services = {}
        self.all_services = {}
        self.em = EventManager()
        self.scheduler = BackgroundScheduler()

    def initialize_apis(self):
        pass

    def initialize_services(self):
        """Initialize minimal dummy services to support /api/v1/services"""
        self.services = {
            "health": DummyService("health")
        }

        # Combine all into self.all_services
        self.all_services = {
            **self.requesting_services,
            **self.services
        }

        logger.info(f"Initialized services: {list(self.all_services.keys())}")

    def validate(self) -> bool:
        return True

    def start(self):
        latest_version = get_version()
        logger.log("PROGRAM", f"SSDv2 v{latest_version} starting!")

        settings_manager.register_observer(self.initialize_apis)
        settings_manager.register_observer(self.initialize_services)
        os.makedirs(data_dir_path, exist_ok=True)

        if not settings_manager.settings_file.exists():
            logger.log("PROGRAM", "Settings file not found, creating default settings")
            settings_manager.save()

        self.initialize_apis()
        self.initialize_services()

        self._schedule_functions()
        self.scheduler.start()
        super().start()

        logger.success("SSDv2 est lance!")
        self.initialized = True

    def _schedule_functions(self):
        """Schedule periodic maintenance tasks."""
        scheduled_functions = {
            log_cleaner: {"interval": 60 * 60},  # every hour
        }

        for func, config in scheduled_functions.items():
            self.scheduler.add_job(
                func,
                "interval",
                seconds=config["interval"],
                id=f"{func.__name__}",
                max_instances=1,
                replace_existing=True,
                next_run_time=datetime.now(),
                misfire_grace_time=30,
            )
            logger.debug(f"Scheduled {func.__name__} every {config['interval']} seconds.")

    def run(self):
        while self.initialized:
            time.sleep(1)  # Keep-alive loop

    def stop(self):
        if not self.initialized:
            return

        if hasattr(self, "scheduler") and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        logger.log("PROGRAM", "SSDv2 est stoppe")
