import unittest
import shutil
import tempfile
import asyncio
from pathlib import Path
import uvicorn

from vault.coordinator.metadata import MetadataDB
from vault.coordinator.event_bus import EventBus
from vault.coordinator.quorum import QuorumEngine, OCCConflictError
from vault.node.storage import NodeStorageManager
from vault.node.main import create_node_app

class TestConcurrencyAndOCC(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.db_path = self.test_dir / "test_metadata.db"
        self.db = MetadataDB(self.db_path)
        self.event_bus = EventBus()
        self.quorum = QuorumEngine(db=self.db, event_bus=self.event_bus, chunk_size=1024)

        # Launch 3 storage nodes
        self.node_ports = [19001, 19002, 19003]
        self.servers = []
        for idx, port in enumerate(self.node_ports, start=1):
            ndir = self.test_dir / f"node_{idx}"
            app = create_node_app(node_id=idx, base_dir=ndir, port=port)
            config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="warning")
            server = uvicorn.Server(config)
            self.servers.append(server)
            asyncio.create_task(server.serve())
            self.db.register_node(node_id=idx, port=port, url=f"http://127.0.0.1:{port}")

        await asyncio.sleep(0.3)

    async def asyncTearDown(self):
        for server in self.servers:
            server.should_exit = True
        await asyncio.sleep(0.1)
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    async def test_concurrent_writes_monotonic_versions(self):
        target_filename = "shared_asset.txt"
        num_concurrent = 10

        async def worker(idx: int):
            payload = f"Payload from worker {idx}".encode("utf-8")
            return await self.quorum.write_file(filename=target_filename, data=payload, replication_factor=2)

        tasks = [worker(i) for i in range(num_concurrent)]
        results = await asyncio.gather(*tasks)

        self.assertEqual(len(results), num_concurrent)

        # Check versions in DB for this filename
        versions = self.db.list_all_file_versions(target_filename)
        self.assertEqual(len(versions), num_concurrent)

        # Versions must be strictly monotonic from 1 to 10
        version_numbers = sorted([v["version"] for v in versions])
        self.assertEqual(version_numbers, list(range(1, num_concurrent + 1)))

if __name__ == "__main__":
    unittest.main()
