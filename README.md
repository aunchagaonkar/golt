# Golt

A distributed key-value store built with the Raft consensus algorithm in Go.

## Features

- **Raft Consensus** – Leader election, log replication, and snapshotting
- **HTTP API** – Simple REST interface for key-value operations
- **gRPC** – Inter-node communication via Protocol Buffers
- **LSM Storage** – Log-structured merge-tree with memtable and SSTables
- **Write-Ahead Log** – Durable log persistence
- **Docker Ready** – Multi-node cluster with Docker Compose

**[Documentation & Benchmarks](demo.md)**

## Quick Start

```bash
# build and run a 5 node cluster
docker compose up --build -d

# or run a single node locally
go run ./cmd/golt -id=node1 -address=localhost:7000 -http=localhost:8000
# the node will be running at localhost:8000 and the grpc service running at localhost:7000
```

## API

```bash
# set a key
curl -X POST http://localhost:8001/set -H "Content-Type: application/json" -d '{"key":"name","value":"ameya"}'

# get a key
curl "http://localhost:8001/get?key=name"

# check node status
curl http://localhost:8001/status
```

## Configuration

| Flag       | Description                              | Required |
|------------|------------------------------------------|----------|
| `-id`      | Unique node identifier                   | Yes      |
| `-address` | Raft listen address (e.g., `host:7000`)  | Yes      |
| `-http`    | HTTP API address (e.g., `host:8000`)     | No       |
| `-peers`   | Comma-separated peer addresses           | No       |
| `-data`    | Data directory (default: `/tmp/golt-<id>`) | No       |

## Architecture

```
mygolt/
├── api/        # HTTP gateway
├── cmd/golt/   # Entry point
├── logger/     # Structured logging
├── proto/      # gRPC definitions
├── raft/       # Consensus implementation
└── storage/    # LSM-tree storage engine
```

## License

[MIT](LICENSE)
