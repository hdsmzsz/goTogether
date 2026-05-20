# goTogether - Real-time Collaborative Document Editing Platform

## Project Overview

A microservice-based real-time collaborative document editing platform. Users can create, edit, search documents, and collaborate with others in real-time with conflict-free concurrent editing.

## Architecture

```
                    ┌──────────────┐
                    │   Frontend   │
                    │  (Yjs CRDT)  │
                    └──────┬───────┘
                           │ HTTP / WebSocket
                    ┌──────▼───────┐
                    │   Gateway    │
                    │  (Gin+JWT)   │
                    └──────┬───────┘
                           │ gRPC
          ┌────────────────┼────────────────┐
          │                │                │
   ┌──────▼──────┐ ┌──────▼──────┐ ┌───────▼──────┐
   │ User Service│ │  Doc Service │ │Search Service│
   │ (PostgreSQL)│ │(MongoDB+MinIO)│ │(Elasticsearch)│
   └─────────────┘ └──────┬──────┘ └──────▲───────┘
                          │                │
                   ┌──────▼──────┐         │
                   │ Collab Svc  │   RabbitMQ
                   │(WS+Redis)   │─────────┘
                   └─────────────┘
```

### Microservices

| Service | Port (HTTP) | Port (gRPC) | Responsibility |
|---------|------------|-------------|----------------|
| gateway | 8080 | - | API routing, JWT auth, rate limiting |
| user-service | - | 9001 | User CRUD, authentication, PostgreSQL |
| doc-service | - | 9002 | Document CRUD, MongoDB storage, MinIO for images |
| collab-service | 8082 (WS) | 9003 | Real-time collaboration via WebSocket, Redis Stream buffering |
| search-service | - | 9004 | Full-text search, Elasticsearch indexing |

### Service Discovery
- **etcd** for service registration and discovery
- Each gRPC service registers itself on startup, gateway discovers services dynamically

### Storage Engine Design
| Engine | Purpose |
|--------|---------|
| PostgreSQL | User accounts, permissions, relational data |
| MongoDB | Document content, edit snapshots, operation logs |
| Redis | Hot document metadata cache, Redis Stream for op buffering, session store |
| Elasticsearch | Full-text document search index |
| MinIO | Image and file attachments (S3-compatible) |

### Real-time Collaboration Flow
1. Frontend uses **Yjs (CRDT)** for lock-free concurrent editing
2. Backend **WebSocket** relay forwards incremental updates between clients
3. Edit operations append to **Redis Stream** as buffer
4. **Dual-trigger snapshot merge**: idle detection (no edits for 5s) + timed fallback (every 30s)
5. Merged snapshots persist to **MongoDB**
6. Disconnected clients auto-recover state on reconnect

### Async Index Update
1. Document save triggers message to **RabbitMQ**
2. Search service consumes message and rebuilds **Elasticsearch** index
3. Eventual consistency - search results may lag a few seconds

### Observability
- **Prometheus**: metrics collection (request latency, error rates, goroutine count)
- **OpenTelemetry**: distributed tracing across gRPC calls

## Tech Stack

- **Language**: Go 1.23+
- **Web Framework**: Gin
- **RPC**: gRPC + protobuf
- **Service Discovery**: etcd v3
- **Databases**: PostgreSQL 16, MongoDB 7, Redis 7
- **Search**: Elasticsearch 8
- **Object Storage**: MinIO
- **Message Queue**: RabbitMQ
- **WebSocket**: gorilla/websocket
- **CRDT**: Yjs (frontend)
- **Monitoring**: Prometheus + Grafana, OpenTelemetry + Jaeger
- **Containerization**: Docker + Docker Compose

## Project Structure

```
goTogether/
├── CLAUDE.md
├── docker-compose.yml
├── Makefile
├── go.work                    # Go workspace
├── proto/                     # Protobuf definitions
│   ├── user/user.proto
│   ├── doc/doc.proto
│   ├── collab/collab.proto
│   └── search/search.proto
├── pkg/                       # Shared packages
│   ├── auth/                  # JWT utilities
│   ├── discovery/             # etcd service discovery
│   ├── middleware/             # Common middleware
│   └── tracing/               # OpenTelemetry setup
├── gateway/                   # API Gateway service
│   ├── main.go
│   ├── router/
│   └── handler/
├── user-service/              # User management
│   ├── main.go
│   ├── model/
│   ├── repo/
│   └── service/
├── doc-service/               # Document storage
│   ├── main.go
│   ├── model/
│   ├── repo/
│   └── service/
├── collab-service/            # Real-time collaboration
│   ├── main.go
│   ├── hub/                   # WebSocket hub
│   ├── stream/                # Redis Stream consumer
│   └── service/
├── search-service/            # Search & indexing
│   ├── main.go
│   ├── consumer/              # RabbitMQ consumer
│   ├── indexer/
│   └── service/
└── web/                       # Frontend (minimal demo)
    ├── index.html
    └── editor.html
```

## Development Commands

```bash
# Start all infrastructure
docker compose up -d

# Generate protobuf code
make proto

# Run a specific service
go run ./gateway
go run ./user-service
go run ./doc-service
go run ./collab-service
go run ./search-service

# Run all services (dev mode)
make run-all

# Run tests
make test
```

## Configuration

All services read config from environment variables with sensible defaults for local development. See `docker-compose.yml` for the full list.

Key environment variables:
- `POSTGRES_DSN` - PostgreSQL connection string
- `MONGO_URI` - MongoDB connection string
- `REDIS_ADDR` - Redis address
- `ETCD_ENDPOINTS` - etcd endpoints
- `RABBITMQ_URL` - RabbitMQ connection URL
- `MINIO_ENDPOINT` - MinIO endpoint
- `ES_ADDRESSES` - Elasticsearch addresses
- `JWT_SECRET` - JWT signing secret
