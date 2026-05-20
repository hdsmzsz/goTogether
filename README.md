# goTogether

Real-time collaborative document editing platform built with Go microservices.

## Features

- Multi-user real-time collaborative editing (CRDT-based via Yjs)
- Microservice architecture with gRPC communication
- Service discovery via etcd
- Full-text document search powered by Elasticsearch
- Async index updates via RabbitMQ
- Distributed tracing with OpenTelemetry + Jaeger
- Metrics monitoring with Prometheus

## Architecture

| Service | Tech | Responsibility |
|---------|------|----------------|
| Gateway | Gin, JWT | API routing, authentication |
| User Service | PostgreSQL | User management |
| Doc Service | MongoDB, MinIO | Document storage |
| Collab Service | WebSocket, Redis Stream | Real-time collaboration |
| Search Service | Elasticsearch, RabbitMQ | Full-text search |

## Quick Start

```bash
# Start everything with Docker
docker compose up --build -d

# Access the app
open http://localhost:8080/static/index.html

# View Jaeger traces
open http://localhost:16686

# View RabbitMQ management
open http://localhost:15672

# View MinIO console
open http://localhost:9090
```

## Development

```bash
# Generate protobuf code
make proto

# Tidy all modules
make tidy

# Build all services
make build

# Run tests
make test
```

## Tech Stack

Go, Gin, gRPC, PostgreSQL, MongoDB, Redis, Elasticsearch, RabbitMQ, MinIO, etcd, WebSocket, Yjs, Prometheus, OpenTelemetry, Docker
