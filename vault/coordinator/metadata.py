import sqlite3
import threading
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from vault.utils import get_iso_now
from vault.config import METADATA_DB_PATH

class MetadataDB:
    def __init__(self, db_path: Path = METADATA_DB_PATH):
        self.db_path = Path(db_path).resolve()
        self._lock = threading.Lock()
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def init_db(self) -> None:
        with self._lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    file_id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    etag TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'AVAILABLE'
                );
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_filename ON files(filename);")

                cursor.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE
                );
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_file_id ON chunks(file_id);")

                cursor.execute("""
                CREATE TABLE IF NOT EXISTS replicas (
                    chunk_id TEXT NOT NULL,
                    node_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'HEALTHY',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (chunk_id, node_id),
                    FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE
                );
                """)

                cursor.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id INTEGER PRIMARY KEY,
                    port INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ONLINE',
                    last_heartbeat TEXT,
                    consecutive_failures INTEGER DEFAULT 0,
                    delay_ms INTEGER DEFAULT 0
                );
                """)
                conn.commit()

    def register_node(self, node_id: int, port: int, url: str) -> None:
        with self._lock:
            with self.get_connection() as conn:
                conn.execute("""
                INSERT INTO nodes (node_id, port, url, status, last_heartbeat, consecutive_failures, delay_ms)
                VALUES (?, ?, ?, 'ONLINE', ?, 0, 0)
                ON CONFLICT(node_id) DO UPDATE SET
                    port = excluded.port,
                    url = excluded.url,
                    status = 'ONLINE',
                    consecutive_failures = 0
                """, (node_id, port, url, get_iso_now()))
                conn.commit()

    def update_node_heartbeat(self, node_id: int, status: str, consecutive_failures: int, delay_ms: int = 0) -> None:
        with self._lock:
            with self.get_connection() as conn:
                conn.execute("""
                UPDATE nodes
                SET status = ?,
                    consecutive_failures = ?,
                    last_heartbeat = ?,
                    delay_ms = ?
                WHERE node_id = ?
                """, (status, consecutive_failures, get_iso_now(), delay_ms, node_id))
                conn.commit()

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM nodes ORDER BY node_id ASC")
            return [dict(r) for r in cursor.fetchall()]

    def get_online_nodes(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM nodes WHERE status = 'ONLINE' ORDER BY node_id ASC")
            return [dict(r) for r in cursor.fetchall()]

    def get_node(self, node_id: int) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    # Files & Concurrency / Versioning
    def get_latest_file_by_name(self, filename: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM files WHERE filename = ? ORDER BY version DESC LIMIT 1",
                (filename,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM files WHERE file_id = ?", (file_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def list_files(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            # Group by filename to get latest version of each file
            cursor = conn.execute("""
            SELECT f.* FROM files f
            INNER JOIN (
                SELECT filename, MAX(version) AS max_v
                FROM files
                WHERE status != 'DELETED'
                GROUP BY filename
            ) latest ON f.filename = latest.filename AND f.version = latest.max_v
            WHERE f.status != 'DELETED'
            ORDER BY f.created_at DESC
            """)
            return [dict(r) for r in cursor.fetchall()]

    def list_all_file_versions(self, filename: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM files WHERE filename = ? ORDER BY version DESC",
                (filename,)
            )
            return [dict(r) for r in cursor.fetchall()]

    def create_file_record(
        self,
        file_id: str,
        filename: str,
        version: int,
        etag: str,
        size_bytes: int,
        status: str = 'AVAILABLE'
    ) -> Dict[str, Any]:
        with self._lock:
            with self.get_connection() as conn:
                now = get_iso_now()
                conn.execute("""
                INSERT INTO files (file_id, filename, version, etag, size_bytes, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (file_id, filename, version, etag, size_bytes, now, status))
                conn.commit()
                return {
                    "file_id": file_id,
                    "filename": filename,
                    "version": version,
                    "etag": etag,
                    "size_bytes": size_bytes,
                    "created_at": now,
                    "status": status
                }

    def update_file_status(self, file_id: str, status: str) -> None:
        with self._lock:
            with self.get_connection() as conn:
                conn.execute("UPDATE files SET status = ? WHERE file_id = ?", (status, file_id))
                conn.commit()

    def delete_file(self, file_id: str) -> bool:
        with self._lock:
            with self.get_connection() as conn:
                conn.execute("UPDATE files SET status = 'DELETED' WHERE file_id = ?", (file_id,))
                conn.commit()
                return True

    # Chunks & Replicas
    def add_chunk(self, chunk_id: str, file_id: str, chunk_index: int, checksum_sha256: str, size_bytes: int) -> None:
        with self._lock:
            with self.get_connection() as conn:
                conn.execute("""
                INSERT INTO chunks (chunk_id, file_id, chunk_index, checksum_sha256, size_bytes)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(chunk_id) DO UPDATE SET
                    checksum_sha256 = excluded.checksum_sha256,
                    size_bytes = excluded.size_bytes
                """, (chunk_id, file_id, chunk_index, checksum_sha256, size_bytes))
                conn.commit()

    def get_chunks_for_file(self, file_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM chunks WHERE file_id = ? ORDER BY chunk_index ASC",
                (file_id,)
            )
            return [dict(r) for r in cursor.fetchall()]

    def add_replica(self, chunk_id: str, node_id: int, status: str = 'HEALTHY') -> None:
        with self._lock:
            with self.get_connection() as conn:
                conn.execute("""
                INSERT INTO replicas (chunk_id, node_id, status, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chunk_id, node_id) DO UPDATE SET
                    status = excluded.status,
                    updated_at = excluded.updated_at
                """, (chunk_id, node_id, status, get_iso_now()))
                conn.commit()

    def get_replicas_for_chunk(self, chunk_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT r.*, n.port, n.url, n.status as node_status
                FROM replicas r
                LEFT JOIN nodes n ON r.node_id = n.node_id
                WHERE r.chunk_id = ?
                ORDER BY r.node_id ASC
                """,
                (chunk_id,)
            )
            return [dict(r) for r in cursor.fetchall()]

    def update_replica_status(self, chunk_id: str, node_id: int, status: str) -> None:
        with self._lock:
            with self.get_connection() as conn:
                conn.execute(
                    "UPDATE replicas SET status = ?, updated_at = ? WHERE chunk_id = ? AND node_id = ?",
                    (status, get_iso_now(), chunk_id, node_id)
                )
                conn.commit()

    def remove_replica(self, chunk_id: str, node_id: int) -> None:
        with self._lock:
            with self.get_connection() as conn:
                conn.execute("DELETE FROM replicas WHERE chunk_id = ? AND node_id = ?", (chunk_id, node_id))
                conn.commit()

    def get_all_chunks(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM chunks ORDER BY file_id, chunk_index ASC")
            return [dict(r) for r in cursor.fetchall()]

    # Telemetry and Durability Calculations
    def get_durability_metrics(self, replication_factor: int = 2) -> Dict[str, Any]:
        with self.get_connection() as conn:
            # Unique user data size
            cursor = conn.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM files WHERE status != 'DELETED'")
            unique_data_bytes = cursor.fetchone()[0]

            # Raw storage used (sum of chunk sizes * healthy replicas)
            cursor = conn.execute("""
            SELECT COALESCE(SUM(c.size_bytes), 0)
            FROM replicas r
            JOIN chunks c ON r.chunk_id = c.chunk_id
            JOIN files f ON c.file_id = f.file_id
            WHERE r.status = 'HEALTHY' AND f.status != 'DELETED'
            """)
            raw_storage_bytes = cursor.fetchone()[0]

            # Storage overhead ratio
            storage_overhead = (raw_storage_bytes / unique_data_bytes) if unique_data_bytes > 0 else 1.0

            # Replica health
            cursor = conn.execute("SELECT COUNT(*) FROM chunks c JOIN files f ON c.file_id = f.file_id WHERE f.status != 'DELETED'")
            total_chunks = cursor.fetchone()[0]

            cursor = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT r.chunk_id, COUNT(*) as healthy_cnt
                FROM replicas r
                JOIN chunks c ON r.chunk_id = c.chunk_id
                JOIN files f ON c.file_id = f.file_id
                JOIN nodes n ON r.node_id = n.node_id
                WHERE r.status = 'HEALTHY' AND n.status = 'ONLINE' AND f.status != 'DELETED'
                GROUP BY r.chunk_id
                HAVING healthy_cnt >= ?
            )
            """, (replication_factor,))
            fully_replicated_chunks = cursor.fetchone()[0]

            replica_health_pct = 100.0 if total_chunks == 0 else round((fully_replicated_chunks / total_chunks) * 100.0, 1)

            # Node counts
            cursor = conn.execute("SELECT COUNT(*) FROM nodes WHERE status = 'ONLINE'")
            online_nodes = cursor.fetchone()[0]
            cursor = conn.execute("SELECT COUNT(*) FROM nodes")
            total_nodes = cursor.fetchone()[0]

            # File count
            cursor = conn.execute("SELECT COUNT(*) FROM files WHERE status != 'DELETED'")
            total_files = cursor.fetchone()[0]

            return {
                "unique_data_bytes": unique_data_bytes,
                "raw_storage_bytes": raw_storage_bytes,
                "storage_overhead": round(storage_overhead, 2),
                "total_chunks": total_chunks,
                "fully_replicated_chunks": fully_replicated_chunks,
                "replica_health_pct": replica_health_pct,
                "online_nodes": online_nodes,
                "total_nodes": total_nodes,
                "total_files": total_files
            }
