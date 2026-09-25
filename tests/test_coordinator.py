import unittest
import shutil
import tempfile
import asyncio
from pathlib import Path
from starlette.testclient import TestClient

from vault.coordinator.metadata import MetadataDB
from vault.coordinator.event_bus import EventBus
from vault.coordinator.quorum import QuorumEngine, OCCConflictError, QuorumWriteError
from vault.node.storage import NodeStorageManager
from vault.node.main import create_node_app
from vault.utils import compute_sha256, compute_md5

class TestCoordinatorAndQuorum(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.db_path = self.test_dir / "test_metadata.db"
        self.db = MetadataDB(self.db_path)
        self.event_bus = EventBus()
        self.quorum = QuorumEngine(db=self.db, event_bus=self.event_bus, chunk_size=1024)

        # Create 3 real storage managers on disk
        self.nodes = []
        for i in range(1, 4):
            node_dir = self.test_dir / f"node_{i}"
            storage = NodeStorageManager(node_id=i, base_dir=node_dir)
            port = 8000 + i
            url = f"http://127.0.0.1:{port}"
            self.db.register_node(node_id=i, port=port, url=url)
            self.nodes.append({"id": i, "storage": storage, "port": port, "url": url})

    async def asyncTearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_metadata_crud(self):
        file_rec = self.db.create_file_record("f1", "doc.txt", 1, "etag1", 500)
        self.assertEqual(file_rec["filename"], "doc.txt")
        self.assertEqual(file_rec["version"], 1)

        latest = self.db.get_latest_file_by_name("doc.txt")
        self.assertEqual(latest["file_id"], "f1")

        self.db.create_file_record("f2", "doc.txt", 2, "etag2", 600)
        latest_v2 = self.db.get_latest_file_by_name("doc.txt")
        self.assertEqual(latest_v2["version"], 2)
        self.assertEqual(latest_v2["etag"], "etag2")

    async def test_occ_conflict_detection(self):
        # First write
        self.db.create_file_record("f1", "sample.txt", 1, "original_etag", 100)

        # Attempt write with mismatched If-Match
        with self.assertRaises(OCCConflictError):
            await self.quorum.write_file("sample.txt", b"new content", if_match="wrong_etag")

    def test_durability_metrics_calculation(self):
        self.db.create_file_record("f1", "test.bin", 1, "etag1", 2048)
        self.db.add_chunk("c1", "f1", 0, "hash1", 1024)
        self.db.add_chunk("c2", "f1", 1, "hash2", 1024)

        # Add replicas
        self.db.add_replica("c1", 1, "HEALTHY")
        self.db.add_replica("c1", 2, "HEALTHY")
        self.db.add_replica("c2", 1, "HEALTHY")
        self.db.add_replica("c2", 2, "HEALTHY")

        metrics = self.db.get_durability_metrics(replication_factor=2)
        self.assertEqual(metrics["unique_data_bytes"], 2048)
        self.assertEqual(metrics["raw_storage_bytes"], 4096)
        self.assertEqual(metrics["storage_overhead"], 2.0)
        self.assertEqual(metrics["replica_health_pct"], 100.0)

if __name__ == "__main__":
    unittest.main()
