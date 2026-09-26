import unittest
import base64
from starlette.testclient import TestClient

from vault.coordinator.main import app
from vault.config import VAULT_ADMIN_USER, VAULT_ADMIN_PASSWORD, MAX_UPLOAD_SIZE_BYTES
from vault.utils import sanitize_filename, sanitize_id, validate_safe_path
from pathlib import Path
import tempfile
import shutil

class TestSecurityAndAccessibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        auth_str = f"{VAULT_ADMIN_USER}:{VAULT_ADMIN_PASSWORD}"
        cls.valid_auth_header = {"Authorization": f"Basic {base64.b64encode(auth_str.encode()).decode()}"}
        cls.invalid_auth_header = {"Authorization": f"Basic {base64.b64encode(b'baduser:badpass').decode()}"}

    def test_root_landing_hub_accessible(self):
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/html", resp.headers["content-type"])
        self.assertIn("Vault - Distributed Object Storage Hub", resp.text)
        self.assertIn('lang="en"', resp.text)
        self.assertIn('name="viewport"', resp.text)
        self.assertIn("/client", resp.text)
        self.assertIn("/admin", resp.text)

    def test_security_headers_enforced(self):
        resp = self.client.get("/")
        self.assertEqual(resp.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(resp.headers.get("X-Frame-Options"), "DENY")
        self.assertIn("default-src", resp.headers.get("Content-Security-Policy", ""))
        self.assertIn("max-age=31536000", resp.headers.get("Strict-Transport-Security", ""))
        self.assertEqual(resp.headers.get("Referrer-Policy"), "strict-origin-when-cross-origin")

    def test_admin_portal_requires_basic_auth(self):
        # 1. Unauthenticated -> 401
        resp = self.client.get("/admin")
        self.assertEqual(resp.status_code, 401)
        self.assertIn("Basic", resp.headers.get("WWW-Authenticate", ""))

        # 2. Invalid credentials -> 401
        resp = self.client.get("/admin", headers=self.invalid_auth_header)
        self.assertEqual(resp.status_code, 401)

        # 3. Valid credentials -> 200
        resp = self.client.get("/admin", headers=self.valid_auth_header)
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Vault - Admin Console", resp.text)

    def test_chaos_endpoints_require_admin_auth(self):
        # Unauthenticated chaos call
        resp = self.client.post("/api/chaos/kill_node", json={"node_id": 1})
        self.assertEqual(resp.status_code, 401)

        resp = self.client.post("/api/chaos/scrub")
        self.assertEqual(resp.status_code, 401)

    def test_filename_sanitization(self):
        dirty_names = [
            ("../../../etc/passwd", "passwd"),
            ("..\\..\\windows\\system32\\cmd.exe", "cmd.exe"),
            ("file<script>alert(1)</script>.png", "script_.png"),  # Strips directory before '/'
            ("file_with<bad>chars.png", "file_with_bad_chars.png"),
            ("valid-file_name.123.dat", "valid-file_name.123.dat"),
            ("", "unnamed_object"),
            ("   ", "unnamed_object"),
            ("/absolute/path/file.txt", "file.txt"),
        ]
        for dirty, expected in dirty_names:
            sanitized = sanitize_filename(dirty)
            self.assertEqual(sanitized, expected)
            self.assertNotIn("/", sanitized)
            self.assertNotIn("\\", sanitized)
            self.assertNotIn("..", sanitized)

    def test_path_traversal_validation(self):
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            safe_file = tmp_dir / "safe.bin"
            self.assertEqual(validate_safe_path(safe_file, tmp_dir), safe_file.resolve())

            unsafe_file = tmp_dir / ".." / "outside.bin"
            with self.assertRaises(ValueError):
                validate_safe_path(unsafe_file, tmp_dir)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_upload_size_limit_rejection(self):
        # Exceeds 50 MB
        oversized_bytes = b"X" * (MAX_UPLOAD_SIZE_BYTES + 1024)
        files = {"file": ("big_file.bin", oversized_bytes, "application/octet-stream")}
        resp = self.client.post("/api/files/upload", files=files)
        self.assertEqual(resp.status_code, 413)
        self.assertIn("Payload Too Large", resp.json().get("detail", ""))

if __name__ == "__main__":
    unittest.main()
