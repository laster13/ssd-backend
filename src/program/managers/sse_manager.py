import asyncio
import json
from typing import Any, Dict
from loguru import logger

class ServerSentEventManager:
    def __init__(self):
        self.subscribers: list[asyncio.Queue] = []

    async def subscribe(self):
        """Un client SSE s’abonne au flux global"""
        queue: asyncio.Queue = asyncio.Queue()
        self.subscribers.append(queue)

        try:
            while True:
                message = await queue.get()
                yield message
        except asyncio.CancelledError:
            if queue in self.subscribers:
                self.subscribers.remove(queue)
            raise

    def publish_event(self, event_type: str, data: Dict[str, Any]):
        """
        Publie un événement SSE vers tous les abonnés
        - event_type devient le vrai 'event:' SSE
        - data est directement sérialisé en JSON (sans double enveloppe)
        """
        try:
            payload = json.dumps(data)
            message = f"event: {event_type}\ndata: {payload}\n\n"

            for queue in list(self.subscribers):
                try:
                    queue.put_nowait(message)
                except Exception as e:
                    logger.warning(f"⚠️ Suppression d’un subscriber mort: {e}")
                    if queue in self.subscribers:
                        self.subscribers.remove(queue)

        except Exception as e:
            logger.error(f"💥 Erreur publish_event SSE: {e}")

# ✅ Singleton global
sse_manager = ServerSentEventManager()
