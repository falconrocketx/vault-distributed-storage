import asyncio
from typing import Optional, List, Dict, Any
import httpx

from vault.coordinator.metadata import MetadataDB
from vault.coordinator.event_bus import EventBus
from vault.config import SCRUBBER_INTERVAL_SECONDS, NODE_REQUEST_TIMEOUT_SECONDS, DEFAULT_REPLICATION_FACTOR
from vault.utils import compute_sha256

class ScrubberDaemon:
    def __init__(self, db: MetadataDB, event_bus: EventBus, target_r: int = DEFAULT_REPLICATION_FACTOR):
        self.db = db
        self.event_bus = event_bus
        self.target_r = target_r
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self._repair_lock = asyncio.Lock()

    def start(self) -> None:
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._run_loop())

    def stop(self) -> None:
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def repair_chunk(self, chunk_id: str) -> bool:
        """
        Attempts to restore under-replicated or corrupted chunk to reach target R healthy replicas.
        """
        async with self._repair_lock:
            replicas = self.db.get_replicas_for_chunk(chunk_id)
            all_chunks = self.db.get_all_chunks()
            chunk_record = next((c for c in all_chunks if c["chunk_id"] == chunk_id), None)
            if not chunk_record:
                return False

            expected_sha = chunk_record["checksum_sha256"]
            online_nodes = {n["node_id"]: n for n in self.db.get_online_nodes()}

            # 1. Test each replica and find a valid donor
            donor_node = None
            donor_data = None
            healthy_node_ids = set()

            async with httpx.AsyncClient() as client:
                for rep in replicas:
                    nid = rep["node_id"]
                    if nid not in online_nodes:
                        self.db.update_replica_status(chunk_id, nid, "MISSING")
                        continue

                    url = f"{online_nodes[nid]['url'].rstrip('/')}/chunks/{chunk_id}"
                    try:
                        resp = await client.get(url, timeout=NODE_REQUEST_TIMEOUT_SECONDS)
                        if resp.status_code == 200:
                            content = resp.content
                            actual_sha = compute_sha256(content)
                            if actual_sha == expected_sha:
                                healthy_node_ids.add(nid)
                                self.db.update_replica_status(chunk_id, nid, "HEALTHY")
                                if donor_data is None:
                                    donor_node = online_nodes[nid]
                                    donor_data = content
                            else:
                                self.db.update_replica_status(chunk_id, nid, "CORRUPTED")
                                await self.event_bus.publish(
                                    "ALERT",
                                    f"Bit-rot detected on Node {nid} during scrub (Checksum mismatch).",
                                    {"node_id": nid, "chunk_id": chunk_id}
                                )
                        else:
                            self.db.update_replica_status(chunk_id, nid, "CORRUPTED" if resp.status_code == 422 else "MISSING")
                    except Exception:
                        self.db.update_replica_status(chunk_id, nid, "MISSING")

                # If no healthy donor can be found, we cannot repair
                if not donor_data or not donor_node:
                    return False

                # 2. Check if we need more healthy replicas
                needed = self.target_r - len(healthy_node_ids)
                if needed <= 0:
                    return True  # Already healthy

                # 3. Choose candidate targets:
                # First priority: online nodes that currently have CORRUPTED or MISSING replicas of this chunk
                # Second priority: online nodes that do not have this chunk yet
                candidate_target_nodes = []
                # Check nodes with bad replicas first (repair in-place)
                for rep in replicas:
                    nid = rep["node_id"]
                    if nid in online_nodes and nid not in healthy_node_ids:
                        candidate_target_nodes.append(online_nodes[nid])

                # Then check remaining online nodes that have no replica of this chunk
                replica_nids = {r["node_id"] for r in replicas}
                for nid, n in online_nodes.items():
                    if nid not in replica_nids and n not in candidate_target_nodes:
                        candidate_target_nodes.append(n)

                repaired_count = 0
                for target in candidate_target_nodes:
                    if len(healthy_node_ids) >= self.target_r:
                        break

                    target_nid = target["node_id"]
                    await self.event_bus.publish(
                        "REPAIR",
                        f"Self-healing initiated: Copying chunk {chunk_id[:16]}... from Node {donor_node['node_id']} -> Node {target_nid}.",
                        {
                            "chunk_id": chunk_id,
                            "source_node": donor_node["node_id"],
                            "target_node": target_nid
                        }
                    )

                    write_url = f"{target['url'].rstrip('/')}/chunks/{chunk_id}"
                    try:
                        resp = await client.post(
                            write_url,
                            content=donor_data,
                            headers={"X-Checksum-SHA256": expected_sha},
                            timeout=NODE_REQUEST_TIMEOUT_SECONDS
                        )
                        if resp.status_code == 200:
                            self.db.add_replica(chunk_id, target_nid, status="HEALTHY")
                            healthy_node_ids.add(target_nid)
                            repaired_count += 1
                            await self.event_bus.publish(
                                "INFO",
                                f"Self-healing complete: Chunk {chunk_id[:16]}... restored to Node {target_nid} (Replicas: {len(healthy_node_ids)}/{self.target_r}).",
                                {"chunk_id": chunk_id, "node_id": target_nid}
                            )
                        else:
                            await self.event_bus.publish(
                                "WARN",
                                f"Failed to restore chunk {chunk_id[:16]}... to Node {target_nid}: HTTP {resp.status_code}",
                                {"chunk_id": chunk_id, "node_id": target_nid}
                            )
                    except Exception as e:
                        await self.event_bus.publish(
                            "WARN",
                            f"Network error writing repaired chunk to Node {target_nid}: {str(e)}",
                            {"chunk_id": chunk_id, "node_id": target_nid}
                        )

            return repaired_count > 0

    async def scrub_all(self) -> Dict[str, Any]:
        """Runs a full cluster sweep checking and repairing all chunks."""
        chunks = self.db.get_all_chunks()
        if not chunks:
            return {"status": "ok", "total_chunks": 0, "repaired": 0}

        await self.event_bus.publish("INFO", f"Cluster scrub initiated for {len(chunks)} chunk(s).")
        repaired = 0
        for c in chunks:
            success = await self.repair_chunk(c["chunk_id"])
            if success:
                repaired += 1

        await self.event_bus.publish(
            "INFO",
            f"Cluster scrub finished. {repaired} chunk(s) re-replicated/healed.",
            {"total_chunks": len(chunks), "repaired": repaired}
        )
        return {"status": "ok", "total_chunks": len(chunks), "repaired": repaired}

    async def _run_loop(self) -> None:
        while self.is_running:
            try:
                await asyncio.sleep(SCRUBBER_INTERVAL_SECONDS)
                if not self.is_running:
                    break
                await self.scrub_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.event_bus.publish("ALERT", f"Scrubber loop encountered error: {str(e)}")
