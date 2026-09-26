# Vault - Resilient Distributed Object Storage System

Vault is a fault-tolerant, resilient distributed object storage system designed for local machines and local area networks. It runs a configurable multi-node topology ($N \in [2, 5]$ storage daemons), guaranteeing persistence, quorum-based consistency, silent bit-rot detection, and automated self-healing across simulated hardware failures, data corruption, and network partitions.

---

## 1. System Architecture

```
                    +--------------------------------+
                    |  Desktop Launcher (CustomTkinter)|
                    |   (Spawns processes & display) |
                    +---------------+----------------+
                                    |
            +-----------------------+-----------------------+
            |                                               |
            v                                               v
+-----------------------+                       +-----------------------+
|  Client Web Portal    |                       |   Admin Dashboard &   |
|  (Upload / Download)  |                       |   Chaos Lab (Split)   |
+-----------+-----------+                       +-----------+-----------+
            |                                               |
            | HTTP / SSE                                    | HTTP / SSE
            +-----------------------+-----------------------+
                                    |
                                    v
                    +--------------------------------+
                    |   Central Coordinator (Proxy)  |
                    |   - Metadata DB (SQLite)       |
                    |   - Replication & Quorum       |
                    |   - Versioning & ETags         |
                    |   - Scrubbing & Self-Healing   |
                    +---------------+----------------+
                                    |
            +-----------------------+-----------------------+
            | (Internal HTTP / RPC)                         |
            v                       v                       v
+-----------------------+ +-------------------+ +-------------------+
|  Storage Node 1       | |  Storage Node 2   | |  Storage Node N   |
|  Port: 8001           | |  Port: 8002       | |  Port: 800N       |
|  Dir: ./data/node_1   | |  Dir: ./data/node_2| |  Dir: ./data/node_N|
+-----------------------+ +-------------------+ +-------------------+
```

---

## 2. Key Components

### 2.1 Desktop Launcher (`run_launcher.py`)
- **Framework**: `customtkinter` with dark/light themes and colored accents.
- **Cluster Sizer**: Interactive slider/selector to choose between 2 and 5 storage nodes ($N \in [2, 5]$).
- **Replication Factor**: Selector for replica count ($R \le N$, default: $R = 2$ or $3$).
- **Lifecycle Controls**:
  - `Start Cluster`: Launches Coordinator (port `8000`) and $N$ Storage Nodes (`8001`..`800N`) as background subprocesses.
  - `Stop Cluster`: Cleanly terminates all active subprocesses.
  - `Purge Data`: Safely purges `./data/` and resets `metadata.db`.
- **Network Discovery**: Auto-detects the host machine's primary LAN IPv4 address (e.g., `192.168.x.x`).
- **Interactive URLs**: Displays local and LAN URLs for `/admin` and `/client` with one-click **Open in Browser** and **Copy** buttons.
- **Status Banner**: Real-time process tracker.

### 2.2 Storage Nodes (Workers) (`run_node.py`)
- Independent FastAPI processes on ports `8001` through `800N`.
- Strict disk isolation in `./data/node_{id}/chunks/`.
- **Endpoints**:
  - `POST /chunks/{chunk_id}`: Saves raw chunk binary and SHA-256 checksum.
  - `GET /chunks/{chunk_id}`: Recalculates SHA-256 on the fly; returns **HTTP 422 Unprocessable Content** if bit-rot is detected, or **200 OK** with binary stream.
  - `DELETE /chunks/{chunk_id}`: Deletes chunk and metadata.
  - `GET /health`: Heartbeat reporting disk footprint, active chunks count, and status.
  - `POST /debug/corrupt`: Injects random byte flips into a chunk file to simulate bit-rot corruption.
  - `POST /debug/delay`: Injects artificial network latency (e.g. $+3000\text{ ms}$).
  - `POST /debug/freeze`: Freezes node to simulate hardware lockup or power failure.

### 2.3 Central Coordinator & Metadata Engine (`run_coordinator.py`)
- Listens on `http://0.0.0.0:8000`.
- **SQLite Registry (`./data/metadata.db`)**:
  - `files`: File metadata, monotonic versioning, MD5 ETag, size, status.
  - `chunks`: Chunk breakdown and SHA-256 digest.
  - `replicas`: Mapping of chunk to node with status (`HEALTHY`, `CORRUPTED`, `MISSING`).
  - `nodes`: Storage nodes directory, ports, URLs, health status (`ONLINE`, `SUSPECT`, `DEAD`).
- **Quorum Consensus**:
  - Write Quorum: $W = \lfloor R/2 \rfloor + 1$. Writes succeed only after $W$ nodes acknowledge persistence.
  - Read Quorum & Fallback: Reads chunks; if a node returns 422 (corrupted) or is dead, immediately falls back to healthy replicas and triggers repair.
- **Optimistic Concurrency Control (OCC)**:
  - Monotonic versions ($v1, v2, ...$) and HTTP `ETag`.
  - Conditional headers (`If-Match`) prevent conflicting concurrent writes with `409 Conflict`.
- **Heartbeat & Failure Detector**:
  - Pings storage nodes every 2.0 seconds. 1 miss $\rightarrow$ `SUSPECT`, 3 misses $\rightarrow$ `DEAD`.
- **Self-Healing Scrubber Daemon**:
  - Automatically sweeps chunks, detects under-replicated or corrupted chunks, fetches valid donor copies from healthy nodes, and restores replica count back to $R$.

### 2.4 Client Storage Portal (`/client`)
- Modern, colored cloud drive interface (Google Drive / S3 style) styled with Tailwind CSS.
- Drag-and-drop file upload with real-time progress bar.
- File explorer with version badges, sizes, ETags, and last modified timestamps.
- **Object Inspector Modal**: Visual breakdown of file chunks and exact replica placement (`Node 1 (Port 8001) [Healthy]`, etc.).
- Direct download and deletion actions.

### 2.5 Admin Dashboard & Chaos Lab (`/admin`)
- **Single-Screen Split-View Layout**:
  - **Left Pane (Chaos Simulator Controls)**:
    - **Scenario 1 - Single Node Kill**: Freeze target node, watch status turn `DEAD` on 3 missed heartbeats.
    - **Scenario 2 - Silent Bit-Rot Corruption**: Inject byte flips into target node chunk.
    - **Scenario 3 - High Concurrency Collision**: Fire 10 simultaneous parallel writes to the exact same file to stress OCC and versioning.
    - **Scenario 4 - Partial Network Partition**: Inject $+3000\text{ ms}$ latency on target node.
    - **Scenario 5 - Automatic Self-Healing**: Trigger immediate full cluster scrub & repair.
    - Recovery controls: Revive Node, Clear Latencies.
  - **Right Pane (Live Observation Dashboard)**:
    - **Cluster Topology Map**: Visual cards for each active node with `ONLINE` (green), `DEGRADED` (yellow), and `DEAD` (red) status badges.
    - **Durability & Health Metrics**: Storage overhead ratio, replica durability percentage, active vs total nodes.
    - **Live Event Stream / Activity Log**: Auto-scrolling terminal streaming real-time Server-Sent Events (SSE) with color-coded tags: `[INFO]`, `[WARN]`, `[ALERT]`, `[REPAIR]`, `[QUORUM]`.

---

## 3. Quickstart & Usage

### 3.1 Prerequisites
Python 3.10+ installed. Install dependencies:
```powershell
pip install -r requirements.txt
```

### 3.2 Launch via Clickable Icon
You can start the Vault application directly by clicking any of the following:
1. **Desktop Shortcut**: Double-click `Vault` on your Windows Desktop.
2. **Project Folder Shortcut**: Double-click `Vault.lnk` or `Launch_Vault.bat` in the project directory.

Alternatively, launch via terminal:
```powershell
python run_launcher.py
```
1. Select cluster size ($N \in [2, 5]$) and replication factor ($R \le N$).
2. Click **Start Cluster**.
3. Click **Open** next to **Client Portal** or **Admin & Chaos Console**.

### 3.3 Launch Manually via CLI
In separate terminal windows:
```powershell
# Start Storage Nodes 1, 2, 3
python run_node.py --node-id 1 --port 8001
python run_node.py --node-id 2 --port 8002
python run_node.py --node-id 3 --port 8003

# Start Central Coordinator
python run_coordinator.py --port 8000 --nodes 3 --replication 2
```

Navigate to:
- Client Portal: `http://localhost:8000/client`
- Admin Console: `http://localhost:8000/admin`

## 4. Security & Credentials

### 4.1 Authentication & Authorization
The Administrative Console (`/admin`) and all destructive chaos engineering endpoints (`/api/chaos/*`) are strictly protected by **HTTP Basic Authentication** using timing-attack safe comparisons (`secrets.compare_digest`).

- **Default Username**: `admin`
- **Default Password**: `VaultAdmin2026!Secure`

#### Configuring Custom Credentials:
Override default credentials by setting environment variables before launching:
```powershell
$env:VAULT_ADMIN_USER = "custom_admin"
$env:VAULT_ADMIN_PASSWORD = "StrongCustomPassword2026!#"
$env:MAX_UPLOAD_SIZE_BYTES = "52428800"  # 50 MB limit
```

### 4.2 Path Traversal & Injection Safeguards
- **Filename Sanitization**: Untrusted upload filenames are sanitized using `sanitize_filename()`, stripping directory separators (`/`, `\`), null bytes, and restricting characters to `[a-zA-Z0-9_.-]`.
- **Canonical Path Bounds Checking**: All node block storage routines enforce `validate_safe_path()`, verifying canonical path resolution strictly resides within `./data/node_{id}/chunks/`.
- **DoS Safeguard**: Enforces a strict 50 MB payload upload limit to prevent memory exhaustion attacks.

### 4.3 HTTP Security Headers
Every HTTP response automatically includes enterprise-grade security headers:
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Content-Security-Policy: default-src 'self' 'unsafe-inline' https: cdn.tailwindcss.com cdnjs.cloudflare.com; ...`
- `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- `Referrer-Policy: strict-origin-when-cross-origin`
- `Permissions-Policy: geolocation=(), camera=(), microphone=()`

### 4.4 Static Security Analysis
The codebase is audited with **Bandit**:
```powershell
python -m bandit -r vault/
```
Result: **0 vulnerabilities identified** (Low: 0, Medium: 0, High: 0).

---

## 5. Accessibility & Compliance (WCAG 2.1 AA)

Vault is designed to score 90+ on Lighthouse accessibility audits and strictly conform to WCAG 2.1 Level AA:

1. **Navigation Hub (`/`)**:
   - The root endpoint serves a semantic HTML landing page with skip navigation, descriptive meta tags, viewport settings, and high-contrast portal cards.
2. **Semantic Structure & Form Controls**:
   - Explicit `<label>` elements for every input and file picker.
   - Screen-reader accessible landmarks (`<header role="banner">`, `<main id="main-content" role="main">`, `<nav>`, `<section>`, `<footer>`).
   - All interactive icons, trigger buttons, and scenario cards feature explicit text or descriptive `aria-label` / `title` attributes.
3. **Contrast Ratios & Focus Rings**:
   - Text elements achieve $\ge 4.5:1$ contrast against dark backgrounds; badges achieve $\ge 3:1$.
   - Universal high-visibility focus indicators (`:focus-visible`) across all interactive controls.
4. **Screen Reader Announcements**:
   - Dynamic telemetry updates, upload progress, and terminal log streams use ARIA live regions (`aria-live="polite"`, `role="status"`, `role="log"`).
5. **Keyboard Shortcuts**:
   - `Tab` / `Shift+Tab`: Cycle through interactive elements.
   - `Enter` / `Space`: Activate upload zone, buttons, and scenario triggers.
   - `Escape`: Instantly dismiss the Object Inspector modal dialog and return focus.

---

## 6. Running Automated Tests

Run the full test suite with automated discovery:
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

Tests cover:
- **Storage Nodes** ([`tests/test_storage_node.py`](file:///c:/Users/Dell/Downloads/Promptathon/Round1-Storage%20system/tests/test_storage_node.py)): Chunk writing, reading, SHA-256 validation, and 422 bit-rot error detection.
- **Coordinator & Quorum** ([`tests/test_coordinator.py`](file:///c:/Users/Dell/Downloads/Promptathon/Round1-Storage%20system/tests/test_coordinator.py)): Quorum consensus, OCC ETag precondition checks, durability metrics.
- **Bit-Rot & Self-Healing** ([`tests/test_self_healing.py`](file:///c:/Users/Dell/Downloads/Promptathon/Round1-Storage%20system/tests/test_self_healing.py)): Read fallback to healthy replicas and automatic scrubber healing.
- **Concurrency & OCC** ([`tests/test_concurrency.py`](file:///c:/Users/Dell/Downloads/Promptathon/Round1-Storage%20system/tests/test_concurrency.py)): 10 parallel collision writes with monotonic versions 1–10.
- **Security & Accessibility** ([`tests/test_security.py`](file:///c:/Users/Dell/Downloads/Promptathon/Round1-Storage%20system/tests/test_security.py)): HTTP Basic Auth, security headers, path traversal prevention, and filename sanitization.

