import unittest
import shutil
import tempfile
from pathlib import Path
from starlette.testclient import TestClient

from vault.node.storage import NodeStorageManager, ChecksumMismatchError
from vault.node.main import create_node_app
from vault.utils import compute_sha256

class TestStorageNode(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.node_id = 1
        self.storage = NodeStorageManager(node_id=self.node_id, base_dir=self.test_dir)
        self.app = create_node_app(node_id=self.node_id, base_dir=self.test_dir, port=8001)
        self.client = TestClient(self.app)

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_write_and_read_chunk(self):
        chunk_id = "test_chunk_1"
        data = b"Hello, Vault Distributed Storage!"
        meta = self.storage.write_chunk(chunk_id, data)
        self.assertEqual(meta["size_bytes"], len(data))
        self.assertEqual(meta["checksum_sha256"], compute_sha256(data))

        read_data, sha = self.storage.read_chunk(chunk_id)
        self.assertEqual(read_data, data)
        self.assertEqual(sha, meta["checksum_sha256"])

    def test_corrupt_chunk_detected(self):
        chunk_id = "test_chunk_corrupt"
        data = b"Persistent critical data block to test bit rot."
        self.storage.write_chunk(chunk_id, data)

        # Corrupt chunk
        res = self.storage.corrupt_chunk(chunk_id)
        self.assertEqual(res["status"], "CORRUPTED")

        # Reading must raise ChecksumMismatchError
        with self.assertRaises(ChecksumMismatchError):
            self.storage.read_chunk(chunk_id)

    def test_api_endpoints(self):
        # Health
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ONLINE")

        # Write chunk
        chunk_id = "api_chunk_1"
        payload = b"Binary chunk content via HTTP API"
        resp = self.client.post(f"/chunks/{chunk_id}", content=payload)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["size_bytes"], len(payload))

        # Read chunk
        resp = self.client.get(f"/chunks/{chunk_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.content, payload)
        self.assertEqual(resp.headers.get("x-checksum-sha256"), compute_sha256(payload))

        # Corrupt chunk via API
        resp = self.client.post("/debug/corrupt", json={"chunk_id": chunk_id})
        self.assertEqual(resp.status_code, 200)

        # Read chunk must return 422 Unprocessable Entity
        resp = self.client.get(f"/chunks/{chunk_id}")
        self.assertEqual(resp.status_code, 422)

        # Delete chunk
        resp = self.client.delete(f"/chunks/{chunk_id}")
        self.assertEqual(resp.status_code, 200)

if __name__ == "__main__":
    unittest.main()
