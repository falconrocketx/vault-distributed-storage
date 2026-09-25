import asyncio
import httpx
from typing import Optional
from vault.coordinator.metadata import MetadataDB
from vault.coordinator.event_bus import EventBus
from vault.config import HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_TIMEOUT_SECONDS, SUSPECT_THRESHOLD, DEAD_THRESHOLD

class FailureDetector:
    def __init__(self, db: MetadataDB, event_bus: EventBus):
        self.db = db
        self.event_bus = event_bus
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._run_loop())

    def stop(self) -> None:
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _ping_node(self, client: httpx.AsyncClient, node: dict) -> None:
        node_id = node["node_id"]
        url = f"{node['url'].rstrip('/')}/health"
        prev_status = node.get("status", "ONLINE")
        prev_failures = node.get("consecutive_failures", 0)

        try:
            resp = await client.get(url, timeout=HEARTBEAT_TIMEOUT_SECONDS)
            if resp.status_code == 200:
                data = resp.json()
                delay_ms = data.get("delay_ms", 0)
                is_frozen = data.get("is_frozen", False)
                if is_frozen:
                    raise Exception(f"Node {node_id} is in FROZEN state")

                # Node is healthy
                if prev_status != "ONLINE":
                    await self.event_bus.publish(
                        "INFO",
                        f"Node {node_id} on port {node['port']} recovered and is now ONLINE.",
                        {"node_id": node_id, "port": node["port"]}
                    )
                self.db.update_node_heartbeat(node_id, status="ONLINE", consecutive_failures=0, delay_ms=delay_ms)
                return
            else:
                raise Exception(f"HTTP status {resp.status_code}")
        except Exception as e:
            # Failure detected
            failures = prev_failures + 1
            new_status = prev_status
            if failures >= DEAD_THRESHOLD:
                new_status = "DEAD"
                if prev_status != "DEAD":
                    await self.event_bus.publish(
                        "WARN",
                        f"Heartbeat lost for Node {node_id} ({DEAD_THRESHOLD} misses). Marked as DEAD.",
                        {"node_id": node_id, "port": node["port"], "error": str(e)}
                    )
            elif failures >= SUSPECT_THRESHOLD:
                new_status = "SUSPECT"
                if prev_status != "SUSPECT":
                    await self.event_bus.publish(
                        "WARN",
                        f"Heartbeat timeout for Node {node_id} (miss {failures}/{DEAD_THRESHOLD}). Marked as SUSPECT.",
                        {"node_id": node_id, "port": node["port"], "error": str(e)}
                    )

            self.db.update_node_heartbeat(node_id, status=new_status, consecutive_failures=failures, delay_ms=0)

    async def _run_loop(self) -> None:
        async with httpx.AsyncClient() as client:
            while self.is_running:
                try:
                    nodes = self.db.get_all_nodes()
                    tasks = [self._ping_node(client, node) for node in nodes]
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    await self.event_bus.publish("ALERT", f"Detector loop error: {str(e)}")

                try:
                    await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                except asyncio.CancelledError:
                    break
