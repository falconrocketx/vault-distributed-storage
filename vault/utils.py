import os
import re
import socket
import hashlib
from pathlib import Path
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
    """Computes MD5 hexadecimal digest for HTTP ETag calculation."""
    # usedforsecurity=False indicates MD5 is used for caching/integrity tags, not cryptography
    return hashlib.md5(data, usedforsecurity=False).hexdigest()  # nosec B324

def sanitize_filename(filename: str) -> str:
    """
    Sanitizes untrusted filenames to prevent directory traversal and injection attacks.
    Strictly removes path separators, null bytes, and non-whitelisted characters.
    """
    if not filename:
        return "unnamed_object"

    # Strip directory components
    base_name = os.path.basename(filename.strip().replace("\\", "/"))
    # Remove null bytes and control chars
    base_name = base_name.replace("\x00", "")
    # Whitelist alphanumeric, dots, underscores, dashes
    clean_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', base_name)
    # Prevent leading dots/slashes that could cause hidden files or navigation
    clean_name = clean_name.lstrip(".-")

    if not clean_name:
        clean_name = "unnamed_object"

    return clean_name[:255]

def validate_safe_path(target_path: Path, base_dir: Path) -> Path:
    """
    Ensures that target_path resolves strictly within base_dir.
    Raises ValueError if path traversal is detected.
    """
    canonical_target = target_path.resolve()
    canonical_base = base_dir.resolve()
    try:
        canonical_target.relative_to(canonical_base)
    except ValueError:
        raise ValueError(f"Path traversal attempt detected: {target_path} is outside {base_dir}")
    return canonical_target

def sanitize_id(identifier: str) -> str:
    """Sanitizes identifiers (chunk_id, file_id) to strictly alphanumeric and underscores/dashes."""
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    clean = re.sub(r'[^a-zA-Z0-9_-]', '', identifier)
    if not clean:
        raise ValueError("Invalid identifier format")
    return clean

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
