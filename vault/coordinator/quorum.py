import io
import uuid
import asyncio
from typing import List, Dict, Any, Optional, Tuple, AsyncGenerator
import httpx

from vault.coordinator.metadata import MetadataDB
from vault.coordinator.event_bus import EventBus
from vault.config import DEFAULT_CHUNK_SIZE, DEFAULT_REPLICATION_FACTOR, NODE_REQUEST_TIMEOUT_SECONDS
from vault.utils import compute_sha256, compute_md5

class QuorumWriteError(Exception):
    pass

class QuorumReadError(Exception):
    pass

class OCCConflictError(Exception):
    pass

class QuorumEngine:
    def __init__(self, db: MetadataDB, event_bus: EventBus, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.db = db
        self.event_bus = event_bus
        self.chunk_size = chunk_size
        self._file_locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
        self._node_round_robin_idx = 0

    async def _get_file_lock(self, filename: str) -> asyncio.Lock:
        async with self._global_lock:
            if filename not in self._file_locks:
                self._file_locks[filename] = asyncio.Lock()
            return self._file_locks[filename]

    def _select_target_nodes(self, available_nodes: List[Dict[str, Any]], count: int) -> List[Dict[str, Any]]:
        if not available_nodes:
            return []
        n = len(available_nodes)
        count = min(count, n)
        # Select using round-robin distribution
        selected = []
        for i in range(count):
            idx = (self._node_round_robin_idx + i) % n
            selected.append(available_nodes[idx])
        self._node_round_robin_idx = (self._node_round_robin_idx + count) % n
        return selected

    async def _write_chunk_to_node(
        self,
        client: httpx.AsyncClient,
        node: Dict[str, Any],
        chunk_id: str,
        data: bytes,
        checksum: str
    ) -> Tuple[int, bool, Optional[str]]:
        node_id = node["node_id"]
        url = f"{node['url'].rstrip('/')}/chunks/{chunk_id}"
        headers = {"X-Checksum-SHA256": checksum}
        try:
            resp = await client.post(url, content=data, headers=headers, timeout=NODE_REQUEST_TIMEOUT_SECONDS)
            if resp.status_code == 200:
                return (node_id, True, None)
            return (node_id, False, f"HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            return (node_id, False, str(e))

    async def write_file(
        self,
        filename: str,
        data: bytes,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
        if_match: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Implements Quorum-based write and OCC versioning.
        Write Quorum W = floor(R / 2) + 1.
        """
        file_lock = await self._get_file_lock(filename)
        async with file_lock:
            # 1. OCC & Version resolution
            latest_file = self.db.get_latest_file_by_name(filename)
            if if_match:
                # Strip quotation marks if present
                clean_if_match = if_match.strip('"')
                if latest_file is None:
                    raise OCCConflictError(f"Conflict: File '{filename}' does not exist, cannot match ETag.")
                if latest_file["etag"] != clean_if_match:
                    raise OCCConflictError(
                        f"OCC Conflict: Client provided ETag '{clean_if_match}' does not match current ETag '{latest_file['etag']}'."
                    )

            next_version = (latest_file["version"] + 1) if latest_file else 1
            file_id = f"f_{uuid.uuid4().hex[:12]}"
            etag = compute_md5(data)
            size_bytes = len(data)

            # 2. Get online nodes
            online_nodes = self.db.get_online_nodes()
            if not online_nodes:
                raise QuorumWriteError("No online storage nodes available in cluster!")

            # Write quorum calculation
            R = min(replication_factor, len(online_nodes))
            W = (R // 2) + 1  # Quorum formula: floor(R/2) + 1

            await self.event_bus.publish(
                "INFO",
                f"Initiating write for '{filename}' v{next_version} (Size: {size_bytes} B, R={R}, Quorum W={W})",
                {"filename": filename, "version": next_version, "R": R, "W": W}
            )

            # 3. Chunking file
            total_chunks = (size_bytes + self.chunk_size - 1) // self.chunk_size if size_bytes > 0 else 1
            chunks_data = []
            if size_bytes == 0:
                chunks_data.append(b"")
            else:
                for idx in range(total_chunks):
                    start = idx * self.chunk_size
                    end = min(start + self.chunk_size, size_bytes)
                    chunks_data.append(data[start:end])

            # Prepare DB file record
            file_record = self.db.create_file_record(
                file_id=file_id,
                filename=filename,
                version=next_version,
                etag=etag,
                size_bytes=size_bytes,
                status="AVAILABLE"
            )

            # 4. Write each chunk to R nodes
            async with httpx.AsyncClient() as client:
                for idx, chunk_bytes in enumerate(chunks_data):
                    chunk_id = f"{file_id}_c{idx}"
                    checksum = compute_sha256(chunk_bytes)
                    self.db.add_chunk(chunk_id, file_id, idx, checksum, len(chunk_bytes))

                    # Pick R distinct target nodes
                    target_nodes = self._select_target_nodes(online_nodes, R)
                    tasks = [
                        self._write_chunk_to_node(client, node, chunk_id, chunk_bytes, checksum)
                        for node in target_nodes
                    ]

                    results = await asyncio.gather(*tasks)

                    successful_nodes = []
                    failed_nodes = []
                    for node_id, success, err in results:
                        if success:
                            successful_nodes.append(node_id)
                            self.db.add_replica(chunk_id, node_id, status="HEALTHY")
                        else:
                            failed_nodes.append(node_id)
                            self.db.add_replica(chunk_id, node_id, status="MISSING")

                    # Check Quorum W
                    if len(successful_nodes) < W:
                        error_msg = f"Write quorum failed for chunk {chunk_id}: {len(successful_nodes)}/{R} succeeded, required W={W}"
                        await self.event_bus.publish("ALERT", error_msg, {"chunk_id": chunk_id})
                        self.db.update_file_status(file_id, "FAILED")
                        raise QuorumWriteError(error_msg)

                    node_list_str = ", ".join(f"Node {nid}" for nid in successful_nodes)
                    await self.event_bus.publish(
                        "QUORUM",
                        f"Chunk {chunk_id[:16]}... replicated to {node_list_str} ({len(successful_nodes)}/{R} >= W={W})",
                        {"chunk_id": chunk_id, "nodes": successful_nodes}
                    )

            await self.event_bus.publish(
                "INFO",
                f"File '{filename}' v{next_version} committed successfully with ETag \"{etag}\".",
                {"file_id": file_id, "version": next_version, "etag": etag}
            )

            return file_record

    async def read_file(self, file_id: str, auto_repair_callback=None) -> Tuple[bytes, Dict[str, Any]]:
        """
        Implements Quorum-based read with fallback on corrupted/dead replicas.
        """
        file_record = self.db.get_file(file_id)
        if not file_record or file_record.get("status") == "DELETED":
            raise FileNotFoundError(f"File {file_id} not found")

        chunks = self.db.get_chunks_for_file(file_id)
        if not chunks:
            return b"", file_record

        assembled_chunks: List[bytes] = []

        async with httpx.AsyncClient() as client:
            for chunk in chunks:
                chunk_id = chunk["chunk_id"]
                expected_sha = chunk["checksum_sha256"]
                replicas = self.db.get_replicas_for_chunk(chunk_id)

                # Sort replicas: online and HEALTHY first
                def replica_priority(r):
                    is_online = (r.get("node_status") == "ONLINE")
                    is_healthy = (r.get("status") == "HEALTHY")
                    return (not is_online, not is_healthy)

                replicas.sort(key=replica_priority)

                chunk_content = None
                read_success = False

                for rep in replicas:
                    node_id = rep["node_id"]
                    url = f"{rep['url'].rstrip('/')}/chunks/{chunk_id}"
                    try:
                        resp = await client.get(url, timeout=NODE_REQUEST_TIMEOUT_SECONDS)
                        if resp.status_code == 200:
                            data = resp.content
                            actual_sha = compute_sha256(data)
                            if actual_sha == expected_sha:
                                chunk_content = data
                                read_success = True
                                break
                            else:
                                # Corrupted
                                await self.event_bus.publish(
                                    "ALERT",
                                    f"Bit-rot detected on Node {node_id} (Checksum mismatch for {chunk_id[:16]}...)",
                                    {"node_id": node_id, "chunk_id": chunk_id}
                                )
                                self.db.update_replica_status(chunk_id, node_id, "CORRUPTED")
                                if auto_repair_callback:
                                    asyncio.create_task(auto_repair_callback(chunk_id))
                        elif resp.status_code == 422:
                            # Node returned 422 Unprocessable Content
                            await self.event_bus.publish(
                                "ALERT",
                                f"Bit-rot detected on Node {node_id}: {resp.json().get('detail', 'Corrupted')}",
                                {"node_id": node_id, "chunk_id": chunk_id}
                            )
                            self.db.update_replica_status(chunk_id, node_id, "CORRUPTED")
                            if auto_repair_callback:
                                asyncio.create_task(auto_repair_callback(chunk_id))
                        else:
                            self.db.update_replica_status(chunk_id, node_id, "MISSING")
                    except Exception as e:
                        # Node unreachable / network partition
                        self.db.update_replica_status(chunk_id, node_id, "MISSING")

                if not read_success:
                    err_msg = f"Quorum read failure: all replicas for chunk {chunk_id} are unavailable or corrupted!"
                    await self.event_bus.publish("ALERT", err_msg, {"chunk_id": chunk_id})
                    raise QuorumReadError(err_msg)

                assembled_chunks.append(chunk_content)

        full_data = b"".join(assembled_chunks)
        return full_data, file_record
