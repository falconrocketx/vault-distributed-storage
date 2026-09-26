import os
import io
import logging
import asyncio
from pathlib import Path
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, status, Query, Request, Depends
from fastapi.responses import HTMLResponse, Response, StreamingResponse, JSONResponse
from pydantic import BaseModel
import httpx

from vault.config import (
    METADATA_DB_PATH,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_NODE_COUNT,
    BASE_NODE_PORT,
    DEFAULT_REPLICATION_FACTOR,
    PROJECT_ROOT,
    MAX_UPLOAD_SIZE_BYTES
)
from vault.utils import sanitize_filename
from vault.coordinator.metadata import MetadataDB
from vault.coordinator.event_bus import coordinator_event_bus
from vault.coordinator.detector import FailureDetector
from vault.coordinator.quorum import QuorumEngine, QuorumWriteError, QuorumReadError, OCCConflictError
from vault.coordinator.scrubber import ScrubberDaemon
from vault.coordinator.security import verify_admin_auth

logger = logging.getLogger("vault.coordinator")

db = MetadataDB(METADATA_DB_PATH)
quorum_engine = QuorumEngine(db=db, event_bus=coordinator_event_bus, chunk_size=DEFAULT_CHUNK_SIZE)
failure_detector = FailureDetector(db=db, event_bus=coordinator_event_bus)
scrubber_daemon = ScrubberDaemon(db=db, event_bus=coordinator_event_bus, target_r=DEFAULT_REPLICATION_FACTOR)

node_count_configured = int(os.getenv("VAULT_NODE_COUNT", str(DEFAULT_NODE_COUNT)))
replication_factor_configured = int(os.getenv("VAULT_REPLICATION_FACTOR", str(DEFAULT_REPLICATION_FACTOR)))

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize configured storage nodes
    for i in range(1, node_count_configured + 1):
        port = BASE_NODE_PORT + i - 1
        url = f"http://127.0.0.1:{port}"
        db.register_node(node_id=i, port=port, url=url)

    scrubber_daemon.target_r = replication_factor_configured
    failure_detector.start()
    scrubber_daemon.start()

    await coordinator_event_bus.publish(
        "INFO",
        f"Vault Coordinator online (Nodes: {node_count_configured}, Target R: {replication_factor_configured})"
    )

    yield

    failure_detector.stop()
    scrubber_daemon.stop()

app = FastAPI(title="Vault Central Coordinator", version="1.0.0", lifespan=lifespan)

TEMPLATES_DIR = PROJECT_ROOT / "vault" / "web" / "templates"

# HTTP Security Headers Middleware
@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self' 'unsafe-inline' https: cdn.tailwindcss.com cdnjs.cloudflare.com; "
        "img-src 'self' data: https:; font-src 'self' https: cdnjs.cloudflare.com; connect-src 'self' ws: wss:;"
    )
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
    return response

# Web Portals
@app.get("/", response_class=HTMLResponse)
async def root_hub():
    """Accessible navigation hub directing users to Client Portal and Admin Console."""
    template_path = TEMPLATES_DIR / "index.html"
    if not template_path.exists():
        raise HTTPException(status_code=404, detail="Hub template not found")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/client", response_class=HTMLResponse)
async def client_portal():
    template_path = TEMPLATES_DIR / "client.html"
    if not template_path.exists():
        raise HTTPException(status_code=404, detail="Client template not found")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/admin", response_class=HTMLResponse)
async def admin_portal(admin_user: str = Depends(verify_admin_auth)):
    template_path = TEMPLATES_DIR / "admin.html"
    if not template_path.exists():
        raise HTTPException(status_code=404, detail="Admin template not found")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()

# Server-Sent Events (SSE) Stream
@app.get("/api/events")
async def events_stream():
    return StreamingResponse(
        coordinator_event_bus.subscribe(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# Telemetry
@app.get("/api/telemetry")
async def get_telemetry():
    metrics = db.get_durability_metrics(replication_factor=scrubber_daemon.target_r)
    nodes = db.get_all_nodes()

    # Query node disk stats where available
    async with httpx.AsyncClient() as client:
        for n in nodes:
            try:
                resp = await client.get(f"{n['url'].rstrip('/')}/health", timeout=1.0)
                if resp.status_code == 200:
                    data = resp.json()
                    n["disk_used_bytes"] = data.get("disk_used_bytes", 0)
                    n["chunks_count"] = data.get("chunks_count", 0)
                    n["delay_ms"] = data.get("delay_ms", 0)
            except httpx.RequestError:
                n["disk_used_bytes"] = 0
                n["chunks_count"] = 0

    metrics["nodes"] = nodes
    return metrics

# Client Storage API
@app.get("/api/files")
async def list_files():
    return db.list_files()

@app.post("/api/files/upload")
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    if_match: Optional[str] = Header(None, alias="If-Match"),
    replication_factor: Optional[int] = Form(None)
):
    # Enforce maximum upload size to protect against DoS memory exhaustion
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_UPLOAD_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Payload Too Large: File exceeds maximum allowed size of {MAX_UPLOAD_SIZE_BYTES // (1024 * 1024)} MB"
        )

    # Read chunks safely while tracking total bytes
    buffer = io.BytesIO()
    total_read = 0
    chunk_size = 64 * 1024
    while True:
        chunk = await file.read(chunk_size)
        if not chunk:
            break
        total_read += len(chunk)
        if total_read > MAX_UPLOAD_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Payload Too Large: File exceeds maximum allowed size of {MAX_UPLOAD_SIZE_BYTES // (1024 * 1024)} MB"
            )
        buffer.write(chunk)

    content = buffer.getvalue()

    # Sanitize untrusted filename to prevent path traversal and script injection
    safe_filename = sanitize_filename(file.filename)

    try:
        r_factor = replication_factor or scrubber_daemon.target_r
        record = await quorum_engine.write_file(
            filename=safe_filename,
            data=content,
            replication_factor=r_factor,
            if_match=if_match
        )
        return record
    except OCCConflictError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except QuorumWriteError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@app.get("/api/files/{file_id}/download")
async def download_file(file_id: str):
    try:
        data, record = await quorum_engine.read_file(
            file_id=file_id,
            auto_repair_callback=scrubber_daemon.repair_chunk
        )
        safe_filename = sanitize_filename(record.get("filename", "download.bin"))
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{safe_filename}"',
                "ETag": f'"{record.get("etag", "")}"',
                "Content-Length": str(len(data))
            }
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except QuorumReadError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@app.delete("/api/files/{file_id}")
async def delete_file(file_id: str):
    record = db.get_file(file_id)
    if not record:
        raise HTTPException(status_code=404, detail="File not found")

    chunks = db.get_chunks_for_file(file_id)
    async with httpx.AsyncClient() as client:
        for c in chunks:
            reps = db.get_replicas_for_chunk(c["chunk_id"])
            for r in reps:
                node = db.get_node(r["node_id"])
                if node:
                    try:
                        await client.delete(f"{node['url'].rstrip('/')}/chunks/{c['chunk_id']}", timeout=2.0)
                    except httpx.RequestError as exc:
                        logger.debug("Failed to delete chunk replica on node %s: %s", r["node_id"], exc)

    db.delete_file(file_id)
    await coordinator_event_bus.publish(
        "INFO",
        f"File '{record['filename']}' ({file_id}) deleted and purged from replicas."
    )
    return {"status": "deleted", "file_id": file_id}

@app.get("/api/files/{file_id}/inspect")
async def inspect_file(file_id: str):
    file_record = db.get_file(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    chunks = db.get_chunks_for_file(file_id)
    chunk_details = []
    for c in chunks:
        reps = db.get_replicas_for_chunk(c["chunk_id"])
        chunk_details.append({
            "chunk_id": c["chunk_id"],
            "chunk_index": c["chunk_index"],
            "checksum_sha256": c["checksum_sha256"],
            "size_bytes": c["size_bytes"],
            "replicas": reps
        })

    return {
        "file": file_record,
        "chunks": chunk_details
    }

# Chaos Simulator APIs - Protected by HTTP Basic Authentication
class KillNodePayload(BaseModel):
    node_id: int

@app.post("/api/chaos/kill_node")
async def chaos_kill_node(payload: KillNodePayload, admin_user: str = Depends(verify_admin_auth)):
    node = db.get_node(payload.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{node['url'].rstrip('/')}/debug/freeze", timeout=2.0)
        except httpx.RequestError as exc:
            logger.debug("Node freeze request error: %s", exc)

    await coordinator_event_bus.publish(
        "WARN",
        f"Chaos Trigger by [{admin_user}]: Sent freeze/kill signal to Node {payload.node_id}.",
        {"node_id": payload.node_id, "admin": admin_user}
    )
    return {"status": "killed", "node_id": payload.node_id}

@app.post("/api/chaos/revive_node")
async def chaos_revive_node(payload: KillNodePayload, admin_user: str = Depends(verify_admin_auth)):
    node = db.get_node(payload.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{node['url'].rstrip('/')}/debug/unfreeze", timeout=2.0)
        except httpx.RequestError as exc:
            logger.debug("Node unfreeze request error: %s", exc)

    db.update_node_heartbeat(payload.node_id, status="ONLINE", consecutive_failures=0, delay_ms=0)
    await coordinator_event_bus.publish(
        "INFO",
        f"Chaos Trigger by [{admin_user}]: Revived Node {payload.node_id}.",
        {"node_id": payload.node_id, "admin": admin_user}
    )
    return {"status": "revived", "node_id": payload.node_id}

class CorruptChunkPayload(BaseModel):
    node_id: int
    chunk_id: Optional[str] = None

@app.post("/api/chaos/corrupt_chunk")
async def chaos_corrupt_chunk(payload: CorruptChunkPayload, admin_user: str = Depends(verify_admin_auth)):
    node = db.get_node(payload.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"{node['url'].rstrip('/')}/debug/corrupt",
                json={"chunk_id": payload.chunk_id},
                timeout=3.0
            )
            if resp.status_code == 200:
                result = resp.json()
                cid = result.get("chunk_id", "unknown")
                db.update_replica_status(cid, payload.node_id, "CORRUPTED")
                await coordinator_event_bus.publish(
                    "ALERT",
                    f"Chaos Trigger by [{admin_user}]: Injected silent bit-rot into chunk '{cid}' on Node {payload.node_id} (Byte {result.get('byte_offset')}).",
                    result
                )
                return result
            else:
                detail = resp.json().get("detail", "Node error")
                raise HTTPException(status_code=resp.status_code, detail=detail)
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Cannot reach Node {payload.node_id}: {str(e)}")

class PartitionPayload(BaseModel):
    node_id: int
    delay_ms: int = 3000

@app.post("/api/chaos/partition_node")
async def chaos_partition_node(payload: PartitionPayload, admin_user: str = Depends(verify_admin_auth)):
    node = db.get_node(payload.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"{node['url'].rstrip('/')}/debug/delay",
                json={"delay_ms": payload.delay_ms},
                timeout=2.0
            )
        except httpx.RequestError as exc:
            logger.debug("Partition injection error: %s", exc)

    db.update_node_heartbeat(payload.node_id, status="ONLINE", consecutive_failures=0, delay_ms=payload.delay_ms)
    await coordinator_event_bus.publish(
        "WARN",
        f"Chaos Trigger by [{admin_user}]: Injected +{payload.delay_ms}ms latency partition on Node {payload.node_id}.",
        {"node_id": payload.node_id, "delay_ms": payload.delay_ms, "admin": admin_user}
    )
    return {"status": "partitioned", "node_id": payload.node_id, "delay_ms": payload.delay_ms}

@app.post("/api/chaos/clear_latency")
async def chaos_clear_latency(payload: KillNodePayload, admin_user: str = Depends(verify_admin_auth)):
    node = db.get_node(payload.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{node['url'].rstrip('/')}/debug/clear_delay", timeout=2.0)
        except httpx.RequestError as exc:
            logger.debug("Clear latency request error: %s", exc)

    db.update_node_heartbeat(payload.node_id, status="ONLINE", consecutive_failures=0, delay_ms=0)
    await coordinator_event_bus.publish(
        "INFO",
        f"Chaos Trigger by [{admin_user}]: Cleared artificial delays on Node {payload.node_id}.",
        {"node_id": payload.node_id, "admin": admin_user}
    )
    return {"status": "cleared", "node_id": payload.node_id}

class CollisionPayload(BaseModel):
    count: int = 10
    filename: str = "collision_stress_test.txt"

@app.post("/api/chaos/collision")
async def chaos_collision(payload: CollisionPayload, admin_user: str = Depends(verify_admin_auth)):
    """
    Fires N concurrent writes to the same object to test OCC and version resolution.
    Protected by administrative authorization.
    """
    safe_filename = sanitize_filename(payload.filename)

    await coordinator_event_bus.publish(
        "INFO",
        f"Chaos Trigger by [{admin_user}]: Launching {payload.count} concurrent collision writes to '{safe_filename}'."
    )

    results = []
    async def worker(idx: int):
        data = f"Concurrent write payload index={idx} timestamp={os.urandom(8).hex()}".encode("utf-8")
        try:
            rec = await quorum_engine.write_file(safe_filename, data, replication_factor=scrubber_daemon.target_r)
            return {"index": idx, "success": True, "version": rec["version"]}
        except OCCConflictError as e:
            return {"index": idx, "success": False, "conflict": True, "error": str(e)}
        except Exception as e:
            return {"index": idx, "success": False, "conflict": False, "error": str(e)}

    tasks = [worker(i) for i in range(payload.count)]
    res = await asyncio.gather(*tasks)

    successful = [r for r in res if r.get("success")]
    conflicts = [r for r in res if r.get("conflict")]

    await coordinator_event_bus.publish(
        "INFO",
        f"Concurrency stress test finished: {len(successful)} versions committed, {len(conflicts)} OCC conflicts handled.",
        {"total": payload.count, "committed": len(successful), "conflicts": len(conflicts), "admin": admin_user}
    )

    return {
        "status": "completed",
        "total": payload.count,
        "successful": len(successful),
        "conflicts": len(conflicts),
        "details": res
    }

@app.post("/api/chaos/scrub")
async def chaos_scrub(admin_user: str = Depends(verify_admin_auth)):
    result = await scrubber_daemon.scrub_all()
    return result
