import os
import json
import random
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from vault.utils import compute_sha256

class ChecksumMismatchError(Exception):
    def __init__(self, chunk_id: str, expected: str, actual: str):
        super().__init__(f"Checksum mismatch for chunk {chunk_id}: expected {expected}, got {actual}")
        self.chunk_id = chunk_id
        self.expected = expected
        self.actual = actual

class NodeStorageManager:
    def __init__(self, node_id: int, base_dir: Path):
        self.node_id = node_id
        self.base_dir = Path(base_dir).resolve()
        self.chunks_dir = self.base_dir / "chunks"
        self.meta_dir = self.base_dir / "meta"
        self.delay_ms = 0
        self.frozen = False
        self.ensure_dirs()

    def ensure_dirs(self) -> None:
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)

    def _chunk_path(self, chunk_id: str) -> Path:
        return self.chunks_dir / f"{chunk_id}.bin"

    def _meta_path(self, chunk_id: str) -> Path:
        return self.meta_dir / f"{chunk_id}.json"

    def write_chunk(self, chunk_id: str, data: bytes, expected_checksum: Optional[str] = None) -> Dict[str, Any]:
        """Writes binary chunk to disk and saves its SHA-256 checksum."""
        actual_sha = compute_sha256(data)
        checksum_to_store = expected_checksum if expected_checksum else actual_sha

        chunk_path = self._chunk_path(chunk_id)
        meta_path = self._meta_path(chunk_id)

        # Atomic-like write
        tmp_chunk = chunk_path.with_suffix(".tmp")
        with open(tmp_chunk, "wb") as f:
            f.write(data)
        if chunk_path.exists():
            chunk_path.unlink()
        tmp_chunk.rename(chunk_path)

        meta = {
            "chunk_id": chunk_id,
            "size_bytes": len(data),
            "checksum_sha256": checksum_to_store,
            "node_id": self.node_id
        }
        tmp_meta = meta_path.with_suffix(".tmp")
        with open(tmp_meta, "w", encoding="utf-8") as f:
            json.dump(meta, f)
        if meta_path.exists():
            meta_path.unlink()
        tmp_meta.rename(meta_path)

        return meta

    def read_chunk(self, chunk_id: str) -> Tuple[bytes, str]:
        """
        Reads chunk, recalculates SHA-256 on the fly.
        Raises ChecksumMismatchError if corrupted.
        Raises FileNotFoundError if chunk does not exist.
        """
        chunk_path = self._chunk_path(chunk_id)
        meta_path = self._meta_path(chunk_id)

        if not chunk_path.exists():
            raise FileNotFoundError(f"Chunk {chunk_id} not found on node {self.node_id}")

        with open(chunk_path, "rb") as f:
            data = f.read()

        actual_sha = compute_sha256(data)

        expected_sha = actual_sha
        if meta_path.exists():
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    expected_sha = meta.get("checksum_sha256", actual_sha)
            except Exception:
                pass

        if actual_sha != expected_sha:
            raise ChecksumMismatchError(chunk_id, expected_sha, actual_sha)

        return data, expected_sha

    def delete_chunk(self, chunk_id: str) -> bool:
        """Deletes chunk binary and metadata from disk."""
        chunk_path = self._chunk_path(chunk_id)
        meta_path = self._meta_path(chunk_id)
        deleted = False

        if chunk_path.exists():
            chunk_path.unlink()
            deleted = True
        if meta_path.exists():
            meta_path.unlink()
            deleted = True
        return deleted

    def list_chunks(self) -> List[str]:
        """Lists all chunk IDs present on this node."""
        if not self.chunks_dir.exists():
            return []
        return [f.stem for f in self.chunks_dir.glob("*.bin")]

    def corrupt_chunk(self, chunk_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Flips random bytes in a chunk file to simulate bit-rot corruption.
        Does NOT update the metadata checksum, ensuring subsequent reads detect mismatch.
        """
        if not chunk_id:
            chunks = self.list_chunks()
            if not chunks:
                raise FileNotFoundError("No chunks available on this node to corrupt")
            chunk_id = random.choice(chunks)

        chunk_path = self._chunk_path(chunk_id)
        if not chunk_path.exists():
            raise FileNotFoundError(f"Chunk {chunk_id} not found on node {self.node_id}")

        with open(chunk_path, "rb") as f:
            data = bytearray(f.read())

        if len(data) == 0:
            data.extend(b"\xaa\xbb\xcc")
            offset = 0
            original_val = 0
            corrupted_val = 0xAA
        else:
            offset = random.randint(0, len(data) - 1)
            original_val = data[offset]
            # Flip bits with XOR
            corrupted_val = original_val ^ 0xFF
            data[offset] = corrupted_val

        with open(chunk_path, "wb") as f:
            f.write(data)

        return {
            "node_id": self.node_id,
            "chunk_id": chunk_id,
            "byte_offset": offset,
            "original_byte": hex(original_val),
            "corrupted_byte": hex(corrupted_val),
            "status": "CORRUPTED"
        }

    def get_stats(self) -> Dict[str, Any]:
        """Computes disk usage and chunk count."""
        chunks = self.list_chunks()
        total_bytes = 0
        for cid in chunks:
            p = self._chunk_path(cid)
            if p.exists():
                total_bytes += p.stat().st_size

        return {
            "node_id": self.node_id,
            "status": "FROZEN" if self.frozen else ("DEGRADED" if self.delay_ms > 0 else "ONLINE"),
            "chunks_count": len(chunks),
            "disk_used_bytes": total_bytes,
            "delay_ms": self.delay_ms,
            "is_frozen": self.frozen
        }
