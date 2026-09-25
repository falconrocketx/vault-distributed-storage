import unittest
import shutil
import tempfile
import asyncio
from pathlib import Path
import uvicorn

from vault.coordinator.metadata import MetadataDB
from vault.coordinator.event_bus import EventBus
from vault.coordinator.quorum import QuorumEngine
from vault.coordinator.scrubber import ScrubberDaemon
from vault.node.storage import NodeStorageManager
from vault.node.main import create_node_app
from vault.utils import compute_sha256

class TestSelfHealingAndQuorum(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.db_path = self.test_dir / "test_metadata.db"
        self.db = MetadataDB(self.db_path)
        self.event_bus = EventBus()
        self.quorum = QuorumEngine(db=self.db, event_bus=self.event_bus, chunk_size=512)
        self.scrubber = ScrubberDaemon(db=self.db, event_bus=self.event_bus, target_r=2)

        # Launch 3 real storage nodes in asyncio tasks on distinct high ports
        self.node_ports = [18001, 18002, 18003]
        self.servers = []
        self.storages = []

        for idx, port in enumerate(self.node_ports, start=1):
            ndir = self.test_dir / f"node_{idx}"
            storage = NodeStorageManager(node_id=idx, base_dir=ndir)
            self.storages.append(storage)
            app = create_node_app(node_id=idx, base_dir=ndir, port=port)
            config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="warning")
            server = uvicorn.Server(config)
            self.servers.append(server)
            asyncio.create_task(server.serve())
            self.db.register_node(node_id=idx, port=port, url=f"http://127.0.0.1:{port}")

        # Small delay for servers to bind
        await asyncio.sleep(0.3)

    async def asyncTearDown(self):
        for server in self.servers:
            server.should_exit = True
        await asyncio.sleep(0.1)
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    async def test_quorum_write_and_read(self):
        filename = "test_document.txt"
        payload = b"A" * 1200  # Will create 3 chunks (512 + 512 + 176)
        record = await self.quorum.write_file(filename=filename, data=payload, replication_factor=2)
        self.assertEqual(record["filename"], filename)
        self.assertEqual(record["size_bytes"], len(payload))

        # Read back
        read_bytes, file_rec = await self.quorum.read_file(record["file_id"])
        self.assertEqual(read_bytes, payload)

    async def test_bit_rot_detection_and_self_healing(self):
        filename = "critical_asset.dat"
        payload = b"Important resilient distributed storage content"
        record = await self.quorum.write_file(filename=filename, data=payload, replication_factor=2)

        chunks = self.db.get_chunks_for_file(record["file_id"])
        target_chunk = chunks[0]["chunk_id"]

        # Find which nodes store replicas of this chunk
        reps = self.db.get_replicas_for_chunk(target_chunk)
        self.assertEqual(len(reps), 2)
        victim_node_id = reps[0]["node_id"]

        # Inject bit-rot directly into the victim node
        victim_storage = self.storages[victim_node_id - 1]
        corrupt_res = victim_storage.corrupt_chunk(target_chunk)
        self.assertEqual(corrupt_res["status"], "CORRUPTED")

        # Quorum read should still succeed by falling back to the healthy donor replica!
        read_bytes, _ = await self.quorum.read_file(record["file_id"])
        self.assertEqual(read_bytes, payload)

        # Now trigger the Scrubber Daemon to auto-heal the cluster
        scrub_res = await self.scrubber.scrub_all()
        self.assertGreaterEqual(scrub_res["repaired"], 1)

        # After healing, victim node's chunk or a replica on another node must be restored
        # Let's verify reading directly from the victim storage manager now succeeds
        restored_data, restored_sha = victim_storage.read_chunk(target_chunk)
        self.assertEqual(restored_data, payload)

if __name__ == "__main__":
    unittest.main()
