import argparse
import sys
from pathlib import Path
import uvicorn

from vault.config import BASE_NODE_PORT, get_node_dir
from vault.node.main import create_node_app

def main():
    parser = argparse.ArgumentParser(description="Vault Storage Node Runner")
    parser.add_argument("--node-id", type=int, required=True, help="Storage Node ID (e.g. 1)")
    parser.add_argument("--port", type=int, default=None, help="Port to listen on (e.g. 8001)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host interface to bind to")
    parser.add_argument("--data-dir", type=str, default=None, help="Custom data directory for this node")

    args = parser.parse_args()
    node_id = args.node_id
    port = args.port or (BASE_NODE_PORT + node_id - 1)
    data_dir = Path(args.data_dir) if args.data_dir else get_node_dir(node_id)

    print(f"[*] Starting Vault Storage Node {node_id} on {args.host}:{port}")
    print(f"[*] Storage directory: {data_dir.resolve()}")

    app = create_node_app(node_id=node_id, base_dir=data_dir, port=port)
    uvicorn.run(app, host=args.host, port=port, log_level="warning")

if __name__ == "__main__":
    main()
