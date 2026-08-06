# Baseline snapshot

This repeatable snapshot was collected on 2026-08-06 from the running local
`config-orchestrator` container. It contains no proxy credentials or raw
configuration values.

## Database

The SQLite schema contains the operational tables `nodes`, `proxy_test_events`,
`upstream_sources`, `upstream_refresh_runs`, `upstream_generation_members`,
`scheduler_state`, `service_state`, `system_events`, `test_history`,
`assignment_events`, `usage_events`, and `client_node_state`.

| Measure | Value |
|---|---:|
| ACTIVE nodes | 92 |
| CANDIDATE nodes | 2,827 |
| TESTING nodes | 53 |
| INVALID nodes | 5 |
| RETIRED nodes | 3,960 |
| Proxy test events | 26,776 |
| Configured upstream sources | 8 |

## Live smoke and response measurements

The following local requests returned HTTP 200. Times are one local baseline
sample, not a performance guarantee.

| Endpoint | Response bytes | Time |
|---|---:|---:|
| `/health` | 270 | 0.000568 s |
| `/api/v1/nodes?page=1&page_size=100` | 138,862 | 0.010840 s |
| `/api/v1/scheduler` | 1,099 | 0.001331 s |
| `/api/v1/publisher` | 173 | 0.000386 s |

The UI routes `/`, `/clients`, `/diag`, `/logs`, `/history`, `/manual-import`,
and `/docs`, plus diagnostics endpoints, are covered by API smoke tests and
the live smoke check. Full action coverage remains a separate task.

## Runtime and subscription contract

- Docker healthcheck is `curl -fsS http://127.0.0.1:8080/health`.
- The current container sample was `0.03%` CPU and `587 MiB` memory; the
  process count reflects the deliberately bounded active Xray runtimes. FD
  trend and test-throughput baseline remain separate acceptance work.
- The public base64 and raw subscription paths are fixed at
  `subscriptions/active.txt` and `subscriptions/active-raw.txt` on the main
  branch. The raw feed was reachable during this snapshot and contained 119
  unique leased configurations.

## Migration and multi-cycle runtime evidence

The retained pre-Rust SQLite snapshot
`data/app.db.bak-before-rust-20260805-130812` passed `PRAGMA integrity_check`.
On 2026-08-06, its legacy `nodes` / `test_history` counts were `2281` /
`19118`. The live migrated database also passed integrity checking and had
`6937` / `19118`; legacy history was preserved while later complete upstream
generations added nodes. Rust-added event, source, assignment and system-event
tables are present in the live database.

After a Rust recreate, seven scheduler ticks ran in the first 40 seconds. The
diagnostics response exposed bounded queue depths, 4 Xray test slots, 2
download slots, 88 owned child processes and 280 open FDs; the publisher
reported a successful changed commit/push. Structured probe logs contained the
component, proxy id, run id, stage and failure class fields.

## Live API query sample

On the same host and migrated live DB, 25 sequential local samples produced:

| Request | Response bytes | p50 | p95 |
|---|---:|---:|---:|
| `nodes?page=1&page_size=100` | 138,974 | 7.676 ms | 9.086 ms |
| `nodes?status=CANDIDATE&page_size=50` | 68,261 | 5.380 ms | 6.482 ms |
| `nodes/{id}/history?limit=50` | 34,472 | 0.929 ms | 1.149 ms |
| `scheduler` | 1,094 | 0.800 ms | 0.968 ms |

These are local samples, not an internet-proxy throughput comparison. The
repeatable command and the remaining comparison/soak work stay in the task
list until a matching Python/corpus run exists.

## Isolated shadow rehearsal

The Rust image was started against a SQLite backup in `data/shadow/app.db` on
API port 18080, using `config/config.shadow.yml`.  It passed health and DB
integrity checks with the same `6937` node and `19118` legacy-history counts.
The shadow used independent Xray ranges, ran one bounded test worker, and
completed an upstream ingestion/test cycle while its publisher endpoint
reported `enabled: false`. It was then stopped and removed; the production
container remained healthy throughout.
