import os
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
METADATA_DB_PATH = DATA_DIR / "metadata.db"

# Storage Node Defaults
DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB
DEFAULT_NODE_COUNT = 3
MIN_NODE_COUNT = 2
MAX_NODE_COUNT = 5
DEFAULT_REPLICATION_FACTOR = 2

# Network Ports & Host
COORDINATOR_PORT = int(os.getenv("COORDINATOR_PORT", "8000"))
BASE_NODE_PORT = int(os.getenv("BASE_NODE_PORT", "8001"))
COORDINATOR_HOST = os.getenv("COORDINATOR_HOST", "0.0.0.0")  # nosec B104

# Security & Access Control
VAULT_ADMIN_USER = os.getenv("VAULT_ADMIN_USER", "admin")
VAULT_ADMIN_PASSWORD = os.getenv("VAULT_ADMIN_PASSWORD", "VaultAdmin2026!Secure")
MAX_UPLOAD_SIZE_BYTES = int(os.getenv("MAX_UPLOAD_SIZE_BYTES", str(50 * 1024 * 1024)))  # 50 MB DoS safeguard

# Resilience & Timeouts
HEARTBEAT_INTERVAL_SECONDS = 2.0
HEARTBEAT_TIMEOUT_SECONDS = 2.0
SUSPECT_THRESHOLD = 1
DEAD_THRESHOLD = 3
SCRUBBER_INTERVAL_SECONDS = 20.0
NODE_REQUEST_TIMEOUT_SECONDS = 5.0

def get_node_dir(node_id: int) -> Path:
    """Returns the dedicated data directory for a specific storage node."""
    return DATA_DIR / f"node_{node_id}"

def get_node_chunks_dir(node_id: int) -> Path:
    """Returns the chunks directory for a specific storage node."""
    return get_node_dir(node_id) / "chunks"

def ensure_data_directories(node_count: int = DEFAULT_NODE_COUNT) -> None:
    """Ensure data root and per-node directories exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for i in range(1, node_count + 1):
        get_node_chunks_dir(i).mkdir(parents=True, exist_ok=True)
