import socket
import hashlib
from datetime import datetime, timezone

def get_lan_ip() -> str:
    """
    Detects the primary LAN IPv4 address of the local machine.
    Falls back to 127.0.0.1 if disconnected.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Does not actually transmit packets over network, just determines routing interface
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except Exception:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def compute_sha256(data: bytes) -> str:
    """Computes SHA-256 hexadecimal digest for binary data."""
    return hashlib.sha256(data).hexdigest()

def compute_md5(data: bytes) -> str:
    """Computes MD5 hexadecimal digest (commonly used for ETag calculation)."""
    return hashlib.md5(data).hexdigest()

def format_bytes(size: int) -> str:
    """Formats byte counts into human-readable string (KB, MB, GB)."""
    if size < 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0 or unit == 'TB':
            if unit == 'B':
                return f"{int(size)} B"
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"

def get_iso_now() -> str:
    """Returns current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()
