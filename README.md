# Proxy Fleet

Proxy Fleet is a Python service for importing, testing, scoring, and serving V2Ray-compatible proxy configurations.

It provides:

- Subscription and manual config import with deduplication.
- A staged local-health funnel: config validation, batched relay checks, then real downloads only for survivors.
- Progressive retry backoff for transient failures and long quarantine for invalid configs.
- Periodic relay and download revalidation of active nodes on the same host/network that will use them.
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

## Public tested subscription

The running local Docker service publishes a rolling, de-duplicated union of the current ACTIVE snapshot and the two immediately previous ACTIVE snapshots. This gives clients recently healthy fallbacks while they run their own real-delay tests:

- v2rayN/base64 subscription: `https://raw.githubusercontent.com/moosavimaleki/proxy-fleet/main/subscriptions/active.txt`
- Plain share URLs: `https://raw.githubusercontent.com/moosavimaleki/proxy-fleet/main/subscriptions/active-raw.txt`

Publication uses a read-only SSH key mounted into the container. No GitHub token or private key is stored in the image or repository. A 60-second reconciliation pass retries failed pushes but creates no commit when the three-snapshot union is unchanged.

## How health is decided

A proxy is not promoted merely because its server answers. It must pass the cheap relay stage and produce a measured download speed of at least `download_test.min_download_kbps` from this machine. See [docs/ALGORITHM.md](docs/ALGORITHM.md) for scheduling and retry details and [docs/PERFORMANCE_REPORT.md](docs/PERFORMANCE_REPORT.md) for the measured optimization results.
