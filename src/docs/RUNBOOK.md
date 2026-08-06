# Proxy Fleet Rust runbook

## Start and verify

Run `docker compose up -d --build` from the repository root.  The service uses
the host network, persists SQLite at `data/app.db`, and exposes the panel and
API on port 8080.  Verify the running instance without exposing a config:

```bash
docker compose ps
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/api/v1/scheduler
curl -fsS http://127.0.0.1:8080/api/v1/publisher
```

The health response contains lifecycle counts and runtime resource gauges. The
scheduler response contains bounded queue depth/overdue counts; it is the
right diagnostic endpoint instead of downloading the whole node list.

## Configuration

`config/config.yml` is mounted read-only.  The important groups are:

- `subscriptions`: upstream feeds and generation refresh interval;
- `health` and `download_test`: staged test timeouts, evidence thresholds,
  retry ranges and adaptive-concurrency bounds;
- `ports` and `vip_port`: local Xray allocation and optional stable VIP port;
- `network_guard`: global incident detector that pauses destructive work;
- `publishing`: Git remote/branch and reconciliation timing;
- `retention`: bounded system-event retention.

Only these non-secret environment overrides are supported:
`PROXY_FLEET_API_HOST`, `PROXY_FLEET_API_PORT`, `PROXY_FLEET_DATABASE_PATH`,
`PROXY_FLEET_XRAY_BIN`, and `PROXY_FLEET_PUBLISHING_ENABLED`.  Secrets stay in
the read-only SSH mount and must never be placed in Git, logs or config output.

## API and panel

The panel routes are `/`, `/clients`, `/diag`, `/logs`, `/history`,
`/manual-import`, and `/docs`.  Stable JSON endpoints include:

- `GET /health`, `GET /api/v1/nodes?page=1&page_size=50`;
- `GET /api/v1/{scheduler,health-model,upstream,incidents,publisher,clients,logs,vip,network}`;
- `POST /api/v1/manual-import`, `/api/v1/nodes/{id}/test`,
  `/api/v1/nodes/{id}/revive`, `/api/v1/feedback`, and `/api/v1/best`.

Node listings clamp `page_size` to 1–200 and never include raw credentials in
their summary response.  Retrieve a single config only through the explicit
node-config endpoint when operationally necessary.

Sanitized representative JSON payloads live at
`src/tests/fixtures/api-response-samples.json`; contract tests retain legacy
keys and reject accidental credential exposure.

## Database, backups and rollback

SQLite migrations are forward-only and run before background workers start.
Before a version change, stop publication (set `PROXY_FLEET_PUBLISHING_ENABLED=false`),
copy `data/app.db` with SQLite's backup facility or while the container is
stopped, and retain that snapshot until the rollback window ends.  Do not use
downgrade SQL.  A rollback is: stop Rust, restore the snapshot, start the
immutable previous image, then run `/health` and subscription smoke checks.

Temporary snapshots belong outside the repository and should be removed after
the agreed rollback window.  The publisher writes only a de-duplicated rolling
three-snapshot union to `subscriptions/active.txt` and
`subscriptions/active-raw.txt`; a valid previous subscription remains
available until a newer valid snapshot is committed.
