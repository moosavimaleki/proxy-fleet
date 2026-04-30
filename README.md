# Proxy Fleet

Proxy Fleet is a Python service for importing, testing, scoring, and serving V2Ray-compatible proxy configurations.

It provides:

- Subscription and manual config import with deduplication.
- Fast candidate testing with batched real-ping checks.
- Active, probation, dead, and waiting pools.
- Global VIP/hot port routing to the best current node.
- Network sentinel checks to pause work when direct internet access is unavailable.
- SQLite persistence.
- HTTP API and built-in UI for fleet, client status, diagnostics, logs, history, and API docs.

## Run

```bash
docker compose up -d --build
```

Open:

```text
http://127.0.0.1:8080/
http://127.0.0.1:8080/logs
http://127.0.0.1:8080/docs
```

## Main API

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/api/v1/nodes
curl http://127.0.0.1:8080/api/v1/network
curl http://127.0.0.1:8080/api/v1/logs
```

## Data

Runtime data is stored under `data/` and ignored by git except for `data/.gitkeep`.
