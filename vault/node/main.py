import os
import sys
import asyncio
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, Request, Response, HTTPException, status
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel

from vault.node.storage import NodeStorageManager, ChecksumMismatchError
from vault.config import get_node_dir, BASE_NODE_PORT

def create_node_app(node_id: int, base_dir: Path, port: int) -> FastAPI:
    app = FastAPI(title=f"Vault Storage Node {node_id}", version="1.0.0")
    storage = NodeStorageManager(node_id=node_id, base_dir=base_dir)

    @app.middleware("http")
    async def artificial_delay_and_freeze_middleware(request: Request, call_next):
        # Allow debug unfreeze endpoint even if frozen
        if storage.frozen and not request.url.path.endswith("/debug/unfreeze"):
            return Response(
                content=f'{{"detail": "Node {node_id} is frozen/offline"}}',
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                media_type="application/json"
            )

        if storage.delay_ms > 0 and not request.url.path.startswith("/debug/"):
            await asyncio.sleep(storage.delay_ms / 1000.0)

        response = await call_next(request)
        return response

    @app.get("/health")
    async def health():
        stats = storage.get_stats()
        stats["port"] = port
        return stats

    @app.post("/chunks/{chunk_id}")
    async def write_chunk(chunk_id: str, request: Request):
        data = await request.body()
        if not data:
            raise HTTPException(status_code=400, detail="Empty chunk body")
        expected_sha = request.headers.get("X-Checksum-SHA256")
        try:
            meta = storage.write_chunk(chunk_id, data, expected_checksum=expected_sha)
            return meta
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/chunks/{chunk_id}")
    async def read_chunk(chunk_id: str):
        try:
            data, checksum = storage.read_chunk(chunk_id)
            return Response(
                content=data,
                media_type="application/octet-stream",
                headers={
                    "X-Checksum-SHA256": checksum,
                    "Content-Disposition": f'attachment; filename="{chunk_id}.bin"'
                }
            )
        except ChecksumMismatchError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Bit-rot detected on node {node_id}: {str(e)}"
            )
        except FileNotFoundError as e:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    @app.delete("/chunks/{chunk_id}")
    async def delete_chunk(chunk_id: str):
        deleted = storage.delete_chunk(chunk_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Chunk not found")
        return {"status": "deleted", "chunk_id": chunk_id, "node_id": node_id}

    class DelayPayload(BaseModel):
        delay_ms: int = 2000

    class CorruptPayload(BaseModel):
        chunk_id: Optional[str] = None

    @app.post("/debug/corrupt")
    async def debug_corrupt(payload: Optional[CorruptPayload] = None):
        target_chunk = payload.chunk_id if payload else None
        try:
            result = storage.corrupt_chunk(target_chunk)
            return result
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/debug/delay")
    async def debug_delay(payload: Optional[DelayPayload] = None):
        ms = payload.delay_ms if payload else 2000
        storage.delay_ms = ms
        return {"status": "delay_injected", "node_id": node_id, "delay_ms": ms}

    @app.post("/debug/clear_delay")
    async def debug_clear_delay():
        storage.delay_ms = 0
        return {"status": "delay_cleared", "node_id": node_id, "delay_ms": 0}

    @app.post("/debug/freeze")
    async def debug_freeze():
        storage.frozen = True
        return {"status": "frozen", "node_id": node_id}

    @app.post("/debug/unfreeze")
    async def debug_unfreeze():
        storage.frozen = False
        return {"status": "unfrozen", "node_id": node_id}

    return app

# If executed directly via uvicorn with env vars:
node_id_env = int(os.getenv("NODE_ID", "1"))
port_env = int(os.getenv("NODE_PORT", str(BASE_NODE_PORT + node_id_env - 1)))
dir_env = Path(os.getenv("NODE_DATA_DIR", str(get_node_dir(node_id_env))))

app = create_node_app(node_id=node_id_env, base_dir=dir_env, port=port_env)
