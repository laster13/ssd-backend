import os
import sys
import threading
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from queue import Empty
from threading import Lock
from typing import Dict, List

from loguru import logger

from program.managers.sse_manager import sse_manager
from program.types import Event


class EventManager:
    def __init__(self):
        self._executors: list[ThreadPoolExecutor] = []
        self._futures: list[Future] = []
        self._queued_events: list[Event] = []
        self._running_events: list[Event] = []
        self._canceled_futures: list[Future] = []
        self.mutex = Lock()

    def _find_or_create_executor(self, service_cls) -> ThreadPoolExecutor:
        service_name = service_cls.__name__
        env_var_name = f"{service_name.upper()}_MAX_WORKERS"
        max_workers = int(os.environ.get(env_var_name, 1))
        for executor in self._executors:
            if executor["_name_prefix"] == service_name:
                return executor["_executor"]
        _executor = ThreadPoolExecutor(thread_name_prefix=service_name, max_workers=max_workers)
        self._executors.append({"_name_prefix": service_name, "_executor": _executor})
        return _executor

    def _process_future(self, future, service):
        if future.cancelled():
            return
        try:
            result = future.result()
            if future in self._futures:
                self._futures.remove(future)
            sse_manager.publish_event("event_update", self.get_event_updates())
            if isinstance(result, tuple):
                item_id, timestamp = result
            else:
                item_id, timestamp = result, datetime.now()
            if item_id:
                self.remove_event_from_running(future.event)
                if future.cancellation_event.is_set():
                    return
                self.add_event(Event(emitted_by=service, item_id=item_id, run_at=timestamp))
        except Exception as e:
            logger.error(f"Error in future: {e}")
            logger.exception(traceback.format_exc())

    def add_event_to_queue(self, event: Event):
        with self.mutex:
            self._queued_events.append(event)

    def remove_event_from_queue(self, event: Event):
        with self.mutex:
            self._queued_events.remove(event)

    def remove_event_from_running(self, event: Event):
        with self.mutex:
            if event in self._running_events:
                self._running_events.remove(event)

    def submit_job(self, service, program, event=None):
        cancellation_event = threading.Event()
        executor = self._find_or_create_executor(service)
        future = executor.submit(program.all_services[service].run, service, program, event, cancellation_event)
        future.cancellation_event = cancellation_event
        if event:
            future.event = event
        self._futures.append(future)
        sse_manager.publish_event("event_update", self.get_event_updates())
        future.add_done_callback(lambda f: self._process_future(f, service))

    def cancel_all_jobs(self):
        for future in self._futures:
            if not future.done() and not future.cancelled():
                try:
                    future.cancellation_event.set()
                    future.cancel()
                    self._canceled_futures.append(future)
                except Exception as e:
                    logger.error(f"Error cancelling future: {str(e)}")

    def next(self):
        while True:
            if self._queued_events:
                with self.mutex:
                    self._queued_events.sort(key=lambda event: event.run_at)
                    if datetime.now() >= self._queued_events[0].run_at:
                        return self._queued_events.pop(0)
            raise Empty

    def _id_in_queue(self, _id):
        return any(event.item_id == _id for event in self._queued_events)

    def _id_in_running_events(self, _id):
        return any(event.item_id == _id for event in self._running_events)

    def add_event(self, event: Event):
        if self._id_in_queue(event.item_id):
            return False
        if self._id_in_running_events(event.item_id):
            return False
        self.add_event_to_queue(event)
        return True

    def get_event_updates(self) -> Dict[str, List[str]]:
        events = [future.event for future in self._futures if hasattr(future, "event")]
        updates: Dict[str, List[str]] = {}
        for event in events:
            key = event.emitted_by.__name__ if hasattr(event.emitted_by, "__name__") else str(event.emitted_by)
            updates.setdefault(key, []).append(event.item_id)
        return updates