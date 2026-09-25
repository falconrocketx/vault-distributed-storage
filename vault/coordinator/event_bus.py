import asyncio
import json
import collections
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any, List, Optional

class EventBus:
    def __init__(self, history_size: int = 150):
        self.history_size = history_size
        self.history: collections.deque = collections.deque(maxlen=history_size)
        self.subscribers: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    def create_event(self, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return {
            "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            "level": level.upper(),  # INFO, WARN, ALERT, REPAIR, QUORUM
            "message": message,
            "details": details or {}
        }

    async def publish(self, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        event = self.create_event(level, message, details)
        async with self._lock:
            self.history.append(event)
            dead_queues = []
            for q in self.subscribers:
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    dead_queues.append(q)
            for q in dead_queues:
                self.subscribers.remove(q)
        return event

    def publish_sync(self, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.publish(level, message, details))
        except RuntimeError:
            event = self.create_event(level, message, details)
            self.history.append(event)

    async def subscribe(self) -> AsyncGenerator[str, None]:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        async with self._lock:
            # Yield recent history first
            for past_event in self.history:
                try:
                    q.put_nowait(past_event)
                except asyncio.QueueFull:
                    break
            self.subscribers.append(q)

        try:
            while True:
                event = await q.get()
                yield f"data: {json.dumps(event)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            async with self._lock:
                if q in self.subscribers:
                    self.subscribers.remove(q)

    def get_history(self) -> List[Dict[str, Any]]:
        return list(self.history)

# Singleton global bus for coordinator
coordinator_event_bus = EventBus()
