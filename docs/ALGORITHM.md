# Fleet discovery algorithm

## Objective

Minimize the time required to discover proxies that can perform a real download from the machine and network where Proxy Fleet runs. Remote reachability is useful only as a cheap prefilter; it is not the final health verdict.

## Local funnel

1. **Static validation (no process/network cost)**
   - Parse and deduplicate subscription entries.
   - Reject unsupported transports, incompatible REALITY combinations, malformed user IDs, invalid REALITY short IDs, empty Shadowsocks passwords, and Shadowsocks methods unsupported by the installed Xray generation.
   - Invalid configurations receive a long quarantine and do not repeatedly poison Xray batches.
2. **Relay check (cheap network stage)**
   - Start one temporary Xray process for a batch.
   - Probe candidates concurrently through their dedicated local SOCKS ports.
   - All relay fallback URLs share one total timeout budget.
   - Reject connection failures and excessive relay latency immediately.
3. **Download check (expensive survivor stage)**
   - Run only for stage-2 survivors.
   - Try mirrors within one total download deadline.
   - Promote only when a numeric speed at or above `min_download_kbps` is measured. Mirror errors without a measurement are failures, not healthy results.

## Scheduling policy

- Never-tested candidates have first priority.
- A transient candidate failure is retried after the configured progressive backoff. The default sequence is 5 minutes, 30 minutes, 2 hours, and 6 hours.
- The fifth consecutive failure moves a candidate out of the hot queue and into the dead pool for 24 hours.
- A syntactically invalid/incompatible configuration is quarantined for 30 days.
- Host-wide failures are suppressed only when Network Guard independently confirms that the host network is offline.
- Candidate cycles are bounded, batched, and concurrent so the API remains responsive under a large fleet.

## Active-node refresh

- A cheap relay check runs every 10 seconds by default.
- A real download revalidation runs every 5 minutes by default.
- Three consecutive relay failures are required before an active node moves to probation. A failed real-download revalidation moves it immediately so broken nodes are not selected for local traffic.
- Probation performs the complete relay-then-download test. One full success restores ACTIVE; three failures move the node to the dead pool.
- A node with a successful real download remains eligible for the public subscription for 24 hours even while it is in probation or the transient dead pool. It is not selected for local hot routing unless it is ACTIVE.
- Recheckable dead nodes are fed back through the same relay-then-download funnel. Recently verified nodes retry every 2 hours; nodes that never passed a download retry every 6 hours. Invalid configurations are not periodically retried.

The important distinction is that `last_health_check_at` tracks frequent relay health while `last_download_test_at` tracks the more expensive proof that the node still downloads successfully.

## Upstream reconciliation

- Identity uses the normalized technical configuration hash; display remarks are not part of that hash.
- Pruning runs only after every configured source completed successfully and returned at least one valid node.
- A node must be absent from two complete refreshes before it becomes removable.
- ACTIVE nodes, manual imports, and nodes with a successful download in the last 24 hours are protected.
- An absent and unhealthy candidate, probation, waiting, or dead node is deleted with its test/client history. This bounds the database and prevents stale upstream entries from consuming test capacity.

## Optional GitHub Actions prefilter

GitHub Actions can be used as a **stage -1** to fetch public subscriptions, normalize syntax, remove duplicates, validate Xray-compatible configuration shapes, and publish a smaller candidate list. It must not mark a proxy healthy: GitHub runners do not share the censorship, routing, ISP, or congestion conditions of the production host in Iran.

The final relay and download stages therefore remain local. An Action becomes worthwhile only when a repository and publication URL are chosen; the local service can then consume that URL as one of its subscription sources.

## Main tuning controls

```yaml
health:
  active_pool_relay_check_interval_seconds: 10
  active_pool_download_check_interval_seconds: 300
  active_relay_failure_threshold: 3
  probation_failure_threshold: 3
  probation_success_threshold: 1
  recent_success_retention_hours: 24
  dead_recheck_batch_size: 32
  dead_retry_recent_seconds: 7200
  dead_retry_unverified_seconds: 21600
  candidate_max_failures: 5
  candidate_retry_backoff_seconds: [300, 1800, 7200, 21600]
  candidate_cycle_limit: 256
  candidate_batch_size: 32
  candidate_batch_concurrency: 16
  candidate_parallel_batches: 4

download_test:
  min_download_kbps: 100

subscriptions:
  prune_missing_after_cycles: 2
```

Raise `min_download_kbps` if “healthy” should require more bandwidth. Increasing concurrency can find survivors sooner, but only until CPU, file descriptors, router NAT state, or ISP connection limits become the bottleneck.
