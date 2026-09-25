import argparse
import uvicorn
from vault.config import COORDINATOR_PORT, COORDINATOR_HOST

def main():
    parser = argparse.ArgumentParser(description="Vault Central Coordinator Runner")
    parser.add_argument("--port", type=int, default=COORDINATOR_PORT, help="Port to listen on (default: 8000)")
    parser.add_argument("--host", type=str, default=COORDINATOR_HOST, help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--nodes", type=int, default=3, help="Number of storage nodes (default: 3)")
    parser.add_argument("--replication", type=int, default=2, help="Replication factor R (default: 2)")

    args = parser.parse_args()

    import os
    os.environ["VAULT_NODE_COUNT"] = str(args.nodes)
    os.environ["VAULT_REPLICATION_FACTOR"] = str(args.replication)

    print(f"[*] Starting Vault Central Coordinator on {args.host}:{args.port}")
    print(f"[*] Cluster configuration: {args.nodes} nodes, replication factor R={args.replication}")

    uvicorn.run("vault.coordinator.main:app", host=args.host, port=args.port, log_level="warning")

if __name__ == "__main__":
    main()
