# Performance review

Measurements were taken on the current fleet database and host while candidate testing was active.

## Results

| Area | Before | After |
|---|---:|---:|
| Fleet API response | 55,463,218 bytes | about 120–140 KB for 100 rows |
| Fleet API latency | 2.293 s | about 0.028–0.061 s under live load |
| Browser rows created | all 32,023 nodes | 100 by default |
| Container memory | about 659 MiB | about 151–238 MiB under live load |
| Worst historical candidate cycle | 25,769 nodes / about 59 h | bounded 256-node cycles, observed about 17–48 s |

## Changes responsible for the improvement

- Server-side pagination, filtering, counts, and on-demand raw configuration loading.
- SQLite indexes for fleet filters, candidate scheduling, retry eligibility, and download revalidation.
- Bounded candidate cycles with controlled batch/process/probe concurrency.
- A single relay timeout budget across fallback URLs.
- Static rejection of configurations that would make Xray exit and recursively split a batch.
- Progressive retry backoff so known failures do not continuously displace untested candidates.
- Suppression of high-frequency no-op health/event writes.
- SQLite connections are now explicitly closed after each transaction context.

## Remaining constraints

- Public subscription quality is the dominant factor: most candidates currently fail before the download stage.
- The existing database contains a large amount of historical test/event data. It was intentionally preserved; cleanup is a separate, explicit operation.
- External prefiltering can reduce syntax and duplicate noise, but only the local host can measure the final proxy health relevant to this deployment.
