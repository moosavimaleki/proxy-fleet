# برنامهٔ جامع بازنویسی Proxy Fleet با Rust

وضعیت سند: برنامهٔ اجرایی، پیش از شروع بازنویسی  
سند مرجع الگوریتم: [`ALGORITHM.md`](./ALGORITHM.md)  
دامنه: کل ingestion، parser، storage، health model، scheduler، Xray، API، پنل، publisher و Docker

## وضعیت اجرای فعلی — ۲۰۲۶-۰۸-۰۵

این بخش وضعیت واقعی کد Rust را ثبت می‌کند؛ checkboxهای فازها تنها پس از
آزمون shadow روی کپی دیتابیس production تیک نهایی می‌خورند.

- [x] crate Rust، config سازگار YAML، shutdown، logging JSON، Axum و health endpoint.
- [x] migration افزایشی SQLite، backup پیش از اولین migration، WAL/busy timeout و نگاشت `DEAD → DORMANT` / `REMOVED → RETIRED`.
- [x] parser و identity مستقل از remark برای VMess/VLESS/Trojan/SS/SOCKS، شامل SS SIP002.
- [x] refresh generation با fetch bounded، ETag/Last-Modified، dedup و ingest تراکنشی انبوه؛ `304` membership generation را حفظ می‌کند.
- [x] event append-only، Beta/decay با horizon/cap محدود برای جلوگیری از over-confidence، lifecycle hysteresis، publication lease و full-jitter.
- [x] cascade Stage 0–4 و Xray batch با split بازگشتی هنگام startup failure؛ download با primary/fallback mirror و budget مشترک اجرا می‌شود تا خرابی mirror evidence منفی کاذب نسازد.
- [x] scheduler queue-based، lease اتمی، AIMD جدا برای Xray/download و revalidation ACTIVE؛ ACTIVEها بین downloadهای موعددار فقط cascade سبک relay/HTTP می‌گیرند.
- [x] network sentinel، guard شکست هم‌بستهٔ یک batch (ثبت incident و evidence inconclusive)، selection/feedback circuit و runtime/VIP پایه؛ diagnostics فشار CPU/RAM، FD، child process و event-loop lag را گزارش می‌کند.
- [x] routeهای HTTP سازگار، publisher Git lease-based و Docker multi-stage Rust؛ image محلی build و container healthy است.
- [x] migration افزایشی روی `app.db` واقعی پس از backup سازگار، سپس startup reconciliation برای runtimeهای ACTIVE و smoke واقعی `/best`.
- [~] parity کامل transportهای نادر Xray، UI کامل قبلی، benchmark corpus واقعی، incident aggregation بین منبع/protocol/windowهای مستقل، contract snapshot کامل و soak test؛ این‌ها هنوز شرط انتشار نیستند.

آخرین verification محلی: `cargo fmt --all`، `cargo check`، `cargo test` (۵۰
تست) و `cargo clippy --all-targets -- -D warnings` موفق بوده‌اند. Docker
build و compose startup روی همین میزبان موفق بوده و `/health`، publisher و
`POST /api/v1/best` نیز روی دادهٔ واقعی smoke شده‌اند. برای محدودیت mirror
Debian در این میزبان، compose از stage محلیِ `runtime-cached` استفاده می‌کند؛
مسیر استاندارد `runtime-network` همچنان در Dockerfile باقی مانده است.

## 1. هدف نهایی

هدف پروژه پیدا کردن سریع proxyهایی است که **روی همین ماشین و همین شبکه** واقعاً قابلیت اتصال و دانلود دارند، نگه‌داشتن هوشمند proxyهای اثبات‌شده در برابر خطاهای موقت، بازآزمایی کنترل‌شدهٔ موارد ضعیف، و انتشار خودکار subscription عمومی است.

نسخهٔ بازنویسی‌شده باید هم‌زمان این ویژگی‌ها را داشته باشد:

- تمام قابلیت‌های نسخهٔ Python را حفظ کند.
- الگوریتم جدید `ALGORITHM.md` را منبع حقیقت health و scheduling قرار دهد.
- مصرف CPU، RAM، process و file descriptor را قابل‌کنترل و قابل‌اندازه‌گیری کند.
- دیتابیس فعلی و تاریخچهٔ کاربران را بدون حذف یا تکثیر پرهزینه مهاجرت دهد.
- API، پنل و لینک‌های اشتراک فعلی را نشکند.
- failure محلی یا خرابی endpoint تست را به اشتباه به‌عنوان خرابی proxy ثبت نکند.
- با publication lease جلوی ناپدیدشدن سریع proxy سالم را بگیرد.
- در صورت خرابی نسخهٔ Rust، rollback سریع به image و دیتابیس قبلی ممکن باشد.

## 2. تصمیم‌های قطعی طراحی

### 2.1 زبان

- [x] پیاده‌سازی اصلی با Rust انجام شود.
- [x] Go فقط در صورتی دوباره بررسی شود که در زمان اجرا یک مانع اثبات‌شده و غیرقابل‌حل در اکوسیستم Rust پیدا شود؛ در بررسی فعلی چنین مانعی وجود ندارد.
- [x] نسخهٔ compiler و dependencyها در `Cargo.lock` قفل شوند تا build قابل‌تکرار باشد.

دلایل انتخاب Rust:

- Tokio برای workerهای async، timer، signal و مدیریت process.
- Axum/Tower برای API و middleware.
- Reqwest با SOCKS برای probeهای HTTP و download.
- SQLx با SQLite برای queryهای async، transaction و migration.
- Sysinfo برای CPU، RAM، process و pressure signal.
- `yaml_serde` برای config؛ از crate منسوخ `serde_yml` استفاده نشود.
- Rand برای full-jitter backoff.

### 2.2 تصمیم‌های رفتاری

- [x] `health_score`، `next_test_at` و `publication_lease_until` سه مفهوم مستقل باقی بمانند.
- [x] یک failure منفرد، مخصوصاً `TLS_TIMEOUT`، باعث حذف proxy اثبات‌شده نشود.
- [x] union کردن کورکورانهٔ سه snapshot انتشار قدیمی حذف و با lease جایگزین شود.
- [x] `DEAD` قدیمی به `DORMANT` و `REMOVED` به `RETIRED` مهاجرت کند.
- [x] remarks در هویت فنی proxy دخالت نداشته باشد.
- [x] proxy فقط بعد از دانلود واقعی وارد `ACTIVE` شود.
- [x] local subnet یا شبکهٔ خاصی در کد hard-code نشود.
- [x] تمام timeoutها budget سراسری و cancellation امن داشته باشند.
- [x] تست‌ها stage-based باشند و هزینهٔ download فقط برای survivorها پرداخت شود.

### 2.3 مواردی که عمداً در این بازنویسی انجام نمی‌شوند

- [x] HMM برای lifecycle ساخته نشود.
- [x] Neural Network برای تشخیص سلامت ساخته نشود.
- [x] Reinforcement Learning وارد scheduler نشود.
- [x] Thompson Sampling جداگانه برای تک‌تک proxyها ساخته نشود.
- [x] تا وقتی event واقعی کافی و benchmark روشن نداریم، مدل پیچیده‌تر از Bayesian decay اضافه نشود.
- [x] GitHub Actions بخشی از runtime اصلی یا شرط کارکرد publisher محلی نباشد.
- [x] دیتابیس یا message broker جدید بدون اثبات bottleneck واقعی SQLite اضافه نشود.

## 3. فهرست کامل قابلیت‌های موجود و وضعیت آن‌ها

| بخش | قابلیت فعلی | تصمیم در نسخهٔ Rust |
|---|---|---|
| دریافت ورودی | subscriptionهای چندگانه | حفظ و انتقال به refresh generation |
| دریافت ورودی | manual import | حفظ کامل API و UI |
| dedup | hash فنی مستقل از remark | حفظ و تست property-based |
| protocol | VMess | حفظ |
| protocol | VLESS | حفظ |
| protocol | Trojan | حفظ |
| protocol | Shadowsocks مدرن | حفظ |
| protocol | SOCKS | حفظ |
| transport | TCP/raw، WS، gRPC، HTTP Upgrade، SplitHTTP/XHTTP، KCP/mKCP، QUIC | حفظ و golden test با Xray |
| security | none، TLS، REALITY | حفظ و اعتبارسنجی سخت‌گیرانه |
| parser | UUID/custom ID، public key، short id، cipher validation | حفظ |
| parser | تبدیل گزینه‌های قدیمی به config جاری Xray | حفظ |
| تست | static validation | حفظ به‌عنوان Stage 0 |
| تست | DNS/TCP preflight | ساخت دقیق‌تر به‌عنوان Stage 1 |
| تست | Xray batch relay | حفظ به‌عنوان Stage 2 |
| تست | HTTP واقعی | حفظ و مستقل از download به‌عنوان Stage 3 |
| تست | bounded download | حفظ به‌عنوان Stage 4 |
| تست | recursive split در batch startup failure | حفظ |
| runtime | Xray process گروهی برای test | حفظ |
| runtime | Xray process پایدار برای active | حفظ |
| port | main/test port pools | حفظ |
| port | `WAITING_FOR_PORT` | حفظ |
| health | EWMA و streakهای فعلی | مهاجرت به evidence/Beta؛ فیلدهای compatibility باقی بمانند |
| lifecycle | candidate/testing/active/probation/dead | گسترش به stateهای الگوریتم جدید |
| retry | retry و dead revival | تبدیل به queue + circuit breaker + full jitter |
| network | network sentinel | حفظ و ارتقا به global incident detector |
| selection | weighted power-of-choices | حفظ |
| selection | fairness و usage | حفظ |
| feedback | used/broken/rate_limited | حفظ |
| circuit | per-client CLOSED/OPEN/HALF_OPEN | حفظ |
| VIP | انتخاب hot proxy و hysteresis | حفظ |
| metadata | exit IP/country/org/timezone | حفظ، ولی failure آن health را خراب نکند |
| storage | SQLite persistent | حفظ و migration افزایشی |
| API | health، nodes، clients، logs، history، import و actions | حفظ route و payload سازگار |
| UI | Fleet dashboard | حفظ |
| UI | client view | حفظ |
| UI | diagnostics | حفظ و توسعه |
| UI | logs/history/manual import/docs | حفظ |
| publisher | active.txt و active-raw.txt | حفظ URL و format |
| publisher | commit/push خودکار Git | حفظ با debounce و retry امن |
| container | host networking و healthcheck | حفظ |
| deployment | persistent data/config mounts | حفظ |

### 3.1 نگاشت تمام فایل‌های اجرایی فعلی به مقصد Rust

| فایل/بخش Python فعلی | مسئولیت مشاهده‌شده | مقصد در Rust |
|---|---|---|
| `submanager/main.py` | bootstrap سرویس | `src/main.rs` و `app.rs` |
| `submanager/config/models.py` | مدل config | `config.rs` |
| `submanager/config/loader.py` | YAML/ENV loading | `config.rs` |
| `submanager/parser.py` | decode، parse، normalize و Xray config | `parser/*` و `xray/config.rs` |
| `submanager/storage/sqlite_store.py` | schema، query، scheduling state و history | `storage/*` و `migrations/*` |
| `submanager/testing/probes.py` | relay، HTTP و download probe | `probe/*` |
| `submanager/testing/xray.py` | Xray batch/process/config | `xray/*` |
| `submanager/testing/service.py` | orchestration تست و batch split | `probe/*` و `scheduler/*` |
| `submanager/core/models.py` | node/runtime/client مدل‌ها | `domain/*` |
| `submanager/core/scheduling.py` | candidate/dead/active schedule | `scheduler/*` |
| `submanager/core/ports.py` | port pool | `xray/ports.rs` |
| `submanager/core/runtime.py` | persistent Xray runtime | `xray/runtime.rs` |
| `submanager/core/network_guard.py` | network sentinel | `health/incident.rs` و `scheduler/pressure.rs` |
| `submanager/core/feedback.py` | client feedback/circuit | `selection/feedback.rs` و `client_circuit.rs` |
| `submanager/core/app.py` | loopها، VIP، reload و هماهنگی کل برنامه | serviceهای `app`, `scheduler`, `upstream`, `selection`, `publisher` |
| `submanager/selection/engine.py` | weighted best-node selection | `selection/best.rs` |
| `submanager/api/server.py` | HTTP routing/API | `api/routes.rs` و handlerها |
| `submanager/api/pages.py` | HTML/CSS/JS پنل | `ui/pages.rs` و `assets/*` |
| `submanager/publishing/active_subscription.py` | snapshot/render/Git publish | `publisher/*` |
| `submanager/utils/hashing.py` | technical identity | `parser/identity.rs` |
| `submanager/utils/logging.py` | log setup | `observability/logging.rs` |
| `tests/*.py` | قراردادهای فعلی و regression | تست‌های Rust متناظر؛ fixtureهای مفید حذف نشوند |
| `config/config.yml` | تنظیمات runtime | schema سازگار Rust با migration config |
| `Dockerfile*` و `docker-compose.yml` | build/runtime/deployment | multi-stage Rust با compose سازگار |

هیچ فایل Python بدون تعیین تکلیف متناظر حذف نمی‌شود. حذف نسخهٔ قدیمی فقط پس از عبور همان مسئولیت از تست parity مجاز است.

## 4. قراردادهای سازگاری که نباید شکسته شوند

### 4.1 لینک‌های عمومی

- [x] `subscriptions/active.txt` همچنان subscription مناسب v2rayN تولید کند.
- [x] `subscriptions/active-raw.txt` همچنان خطوط raw config را تولید کند.
- [x] ترتیب خروجی deterministic باشد تا commit بی‌دلیل ساخته نشود.
- [x] یک config با remark متفاوت فقط یک بار منتشر شود.

### 4.2 routeهای فعلی

routeهای زیر باید با همان path باقی بمانند:

- [x] `GET /health`
- [x] `GET /api/v1/nodes`
- [x] `GET /api/v1/network`
- [x] `GET /api/v1/vip`
- [x] `GET /api/v1/clients`
- [x] `GET /api/v1/client-status`
- [x] `GET /api/v1/logs`
- [x] `GET /api/v1/nodes/:id/config`
- [x] `GET /api/v1/nodes/:id/history`
- [x] `POST /api/v1/best`
- [x] `POST /api/v1/feedback`
- [x] `POST /api/v1/manual-import`
- [x] `POST /api/v1/nodes/dead/clear` به‌عنوان alias سازگار برای پاک‌سازی/بازنشانی dormantها
- [x] `POST /api/v1/subscriptions/reload`
- [x] `POST /api/v1/db/cleanup`
- [x] `POST /api/v1/nodes/:id/test`
- [x] صفحات `/`، `/clients`، `/diag`، `/logs`، `/history`، `/manual-import` و `/docs`
- [x] `HEAD` برای routeهایی که اکنون پشتیبانی می‌شوند.

### 4.3 payloadهای فعلی

- [x] کلیدهای فعلی response حذف یا rename نشوند.
- [x] fieldهای قدیمی EWMA/streak در دورهٔ migration از مدل جدید derive شوند.
- [x] pagination/filter/search/status/country فعلی حفظ شود.
- [x] fieldهای جدید فقط به payload افزوده شوند.

## 5. مدل دامنهٔ جدید

### 5.1 stateها

- `CANDIDATE`: config معتبر که هنوز دانلود واقعی موفق ندارد.
- `TESTING`: lease داخلی کوتاه برای جلوگیری از تست هم‌زمان یک proxy.
- `ACTIVE`: download واقعی موفق و health کافی.
- `PROBATION`: سابقهٔ موفق دارد ولی evidence جدید ضعیف شده است.
- `DORMANT`: فعلاً ضعیف است و با فاصلهٔ زیاد دوباره تست می‌شود.
- `INVALID`: config از نظر ساختار یا سازگاری Xray نامعتبر است.
- `RETIRED`: در refreshهای معتبر upstream ناپدید شده و lease انتشارش تمام شده است.
- `WAITING_FOR_PORT`: آمادهٔ تست است ولی port capacity موجود نیست.

### 5.2 دادهٔ مستقل هر proxy

- [x] هویت فنی canonical و hash پایدار.
- [x] raw config و normalized config.
- [x] مجموعهٔ sourceها و آخرین generation مشاهده‌شده.
- [x] state فعلی و زمان ورود به state.
- [x] `alpha`، `beta` و `health_score`.
- [x] `next_test_at` و test lease برای جلوگیری از duplicate work.
- [x] `publication_lease_until` و دلیل آخرین تمدید.
- [x] آخرین success/failure و failure class.
- [x] consecutive failure/success و تعداد failure مستقل.
- [x] آخرین latency، download speed و endpoint.
- [x] exit metadata.
- [x] main port/runtime ownership.

### 5.3 failure classهای canonical

- `INVALID_CONFIG`
- `XRAY_START_FAILED`
- `DNS_FAILURE`
- `TCP_TIMEOUT`
- `CONNECTION_REFUSED`
- `TLS_TIMEOUT`
- `RELAY_TIMEOUT`
- `HTTP_FAILURE`
- `DOWNLOAD_TIMEOUT`
- `DOWNLOAD_TOO_SLOW`
- `LOCAL_OVERLOAD`
- `ENDPOINT_FAILURE`
- `SUCCESS`

`LOCAL_OVERLOAD` و `ENDPOINT_FAILURE` نتیجهٔ inconclusive هستند: event ذخیره می‌شود، اما beta، streak خرابی و demotion را افزایش نمی‌دهد.

## 6. معماری هدف در `src/`

ساختار پیشنهادی:

```text
src/
├── Cargo.toml
├── Cargo.lock
├── build.rs                         # فقط اگر embed/build metadata لازم شد
├── docs/
│   ├── ALGORITHM.md
│   └── TASKs.md
├── migrations/
│   ├── 0001_compatibility.sql
│   ├── 0002_evidence_events.sql
│   ├── 0003_upstream_generations.sql
│   └── 0004_scheduler_state.sql
├── assets/
│   ├── app.css
│   └── app.js
└── src/
    ├── main.rs
    ├── app.rs
    ├── config.rs
    ├── error.rs
    ├── shutdown.rs
    ├── domain/
    │   ├── proxy.rs
    │   ├── lifecycle.rs
    │   ├── evidence.rs
    │   ├── failure.rs
    │   └── events.rs
    ├── parser/
    │   ├── identity.rs
    │   ├── subscription.rs
    │   ├── vmess.rs
    │   ├── vless.rs
    │   ├── trojan.rs
    │   ├── shadowsocks.rs
    │   └── socks.rs
    ├── storage/
    │   ├── pool.rs
    │   ├── migrations.rs
    │   ├── nodes.rs
    │   ├── events.rs
    │   ├── clients.rs
    │   ├── upstream.rs
    │   └── publisher.rs
    ├── health/
    │   ├── bayesian.rs
    │   ├── decay.rs
    │   ├── classifier.rs
    │   ├── transition.rs
    │   └── incident.rs
    ├── scheduler/
    │   ├── queues.rs
    │   ├── priority.rs
    │   ├── backoff.rs
    │   ├── concurrency.rs
    │   └── pressure.rs
    ├── xray/
    │   ├── config.rs
    │   ├── process.rs
    │   ├── batch.rs
    │   ├── runtime.rs
    │   └── ports.rs
    ├── probe/
    │   ├── dns.rs
    │   ├── tcp.rs
    │   ├── relay.rs
    │   ├── http.rs
    │   ├── download.rs
    │   └── metadata.rs
    ├── upstream/
    │   ├── fetch.rs
    │   ├── refresh.rs
    │   └── reconcile.rs
    ├── selection/
    │   ├── best.rs
    │   ├── client_circuit.rs
    │   ├── feedback.rs
    │   └── vip.rs
    ├── publisher/
    │   ├── render.rs
    │   ├── git.rs
    │   └── service.rs
    ├── api/
    │   ├── routes.rs
    │   ├── dto.rs
    │   ├── nodes.rs
    │   ├── clients.rs
    │   ├── diagnostics.rs
    │   └── actions.rs
    ├── ui/
    │   ├── pages.rs
    │   └── assets.rs
    └── observability/
        ├── logging.rs
        ├── metrics.rs
        └── health.rs
```

### 6.1 مرزهای معماری

- [x] parser هیچ دسترسی مستقیمی به SQLite یا Xray نداشته باشد.
- [x] health model تابع deterministic از evidence و زمان باشد.
- [x] scheduler فقط job انتخاب کند؛ اجرای probe در worker باشد.
- [x] probe نتیجهٔ خام تولید کند؛ classifier آن را به failure class تبدیل کند.
- [x] publisher فقط snapshot transactionally خوانده‌شده از proxyهای دارای lease معتبر را ببیند.
- [ ] API مستقیماً business rule تغییر ندهد و از service layer استفاده کند.
- [x] processهای Xray owner مشخص، cancellation token و cleanup قطعی داشته باشند.

## 7. dependencyهای برنامه‌ریزی‌شده

- [x] `tokio`: runtime، signal، process، timer، channel و synchronization.
- [x] `axum` و `tower-http`: HTTP API، static assets، compression، timeout و trace.
- [x] `serde`، `serde_json` و `yaml_serde`: serialization و config.
- [x] `sqlx` با SQLite bundled: pool، migration و query.
- [x] `reqwest` با `rustls`، `stream` و `socks`: relay/HTTP/download.
- [x] `url`، `base64`، `uuid` و `sha2`: parser و identity؛ `regex` و `ipnet` نیاز نشدند.
- [x] `chrono`: timestamp و duration.
- [x] `rand`: full-jitter و exploration.
- [x] `sysinfo`: pressure و process metrics.
- [x] `tracing` و `tracing-subscriber`: structured logs.
- [x] `thiserror`: خطاهای domain؛ `anyhow` فقط در application boundary.
- [x] `futures-util` و `tokio-util`: stream و cancellation token.
- [ ] dependency اضافی فقط با دلیل و benchmark پذیرفته شود.

## 8. مهاجرت دیتابیس

### 8.1 اصول ایمنی

- [x] قبل از migration از `app.db` و فایل‌های `-wal/-shm` snapshot سازگار تهیه شود.
- [x] migrationها idempotent و داخل transaction باشند.
- [x] WAL، busy timeout و foreign key فعال شوند.
- [x] pool نوشتن کوچک و bounded باشد تا writer contention ایجاد نشود.
- [x] تاریخچهٔ فعلی حدود ۷۰۰ مگابایت کپی یا rewrite نشود.
- [x] هیچ node id، config hash، feedback، assignment یا usage حذف نشود.
- [x] schema version و binary version ثبت شود.

### 8.2 تغییرات جدول `nodes`

ستون‌های فعلی حفظ و ستون‌های زیر افزوده شوند:

- [x] `lifecycle_state TEXT NOT NULL`
- [x] `state_entered_at TEXT (RFC3339)`
- [x] `structurally_valid INTEGER NOT NULL DEFAULT 1`
- [x] `health_alpha REAL NOT NULL DEFAULT 1`
- [x] `health_beta REAL NOT NULL DEFAULT 1`
- [x] `health_score REAL NOT NULL DEFAULT 0.5`
- [x] `evidence_updated_at TEXT (RFC3339)`
- [x] `next_test_at TEXT (RFC3339)`
- [x] `test_lease_until TEXT (RFC3339)`
- [x] `publication_lease_until TEXT (RFC3339)`
- [x] `publication_lease_kind TEXT`
- [x] `activated_at TEXT (RFC3339)`
- [x] `last_success_at TEXT (RFC3339)`
- [x] `last_failure_at TEXT (RFC3339)`
- [x] `last_failure_class TEXT`
- [x] `failure_streak INTEGER NOT NULL DEFAULT 0`
- [x] `independent_failure_count INTEGER NOT NULL DEFAULT 0`
- [x] `last_test_endpoint TEXT`
- [x] `last_seen_generation INTEGER`
- [x] `upstream_missing_generations INTEGER NOT NULL DEFAULT 0`
- [x] `retired_at TEXT (RFC3339)`
- [x] `tombstone_until TEXT (RFC3339)`

indexهای لازم:

- [x] `(lifecycle_state, next_test_at)`
- [x] `(publication_lease_until, structurally_valid)`
- [x] `(test_lease_until)`
- [x] `(last_seen_generation, upstream_missing_generations)`
- [x] `(config_hash)` unique فعلی حفظ شود.

### 8.3 جدول append-only `proxy_test_events`

هر stage یک event مستقل دارد:

- [x] `id INTEGER PRIMARY KEY`
- [x] `proxy_id TEXT NOT NULL`
- [x] `run_id TEXT NOT NULL`
- [x] `occurred_at TEXT NOT NULL (RFC3339)`
- [x] `stage TEXT NOT NULL`
- [x] `result TEXT NOT NULL`
- [x] `failure_class TEXT NOT NULL`
- [x] `evidence_alpha REAL NOT NULL DEFAULT 0`
- [x] `evidence_beta REAL NOT NULL DEFAULT 0`
- [x] `latency_ms REAL`
- [x] `download_bps REAL`
- [x] `bytes_transferred INTEGER`
- [x] `duration_ms INTEGER`
- [x] `endpoint TEXT`
- [x] `system_pressure REAL`
- [x] `incident_id TEXT`
- [x] `detail_json TEXT`
- [x] index روی `(proxy_id, occurred_at DESC)` و `(run_id)`.

eventها update نشوند؛ aggregate health در `nodes` cache شود و از eventها قابل‌بازسازی باشد.

### 8.4 جدول‌های upstream generation

- [x] `upstream_refresh_runs`: id، زمان شروع/پایان، status، source count، fetch count، parsed count، error و generation.
- [x] `upstream_sources`: URL، enabled، آخرین success، ETag/Last-Modified، failure streak.
- [x] `upstream_generation_members`: generation، source، config hash و seen_at.
- [x] فقط refresh کامل و سالم، missing counter را افزایش دهد.
- [x] refresh ناقص یا outage منبع هیچ proxy را retired نکند.

### 8.5 جدول‌های runtime/scheduler

- [x] `scheduler_state`: quota debt، concurrency جاری، آخرین pressure و recovery timestamps.
- [x] `service_state`: آخرین publisher commit، آخرین refresh، incident و schema metadata.
- [x] process/port ownership persistent فقط در حد لازم ثبت شود؛ process واقعی پس از restart دوباره reconcile شود.

### 8.6 تبدیل دادهٔ قدیمی

- [x] `CANDIDATE`، `TESTING`، `ACTIVE`، `PROBATION` و `WAITING_FOR_PORT` مستقیم نگاشت شوند.
- [x] `DEAD` به `DORMANT` نگاشت شود.
- [x] `REMOVED` به `RETIRED` نگاشت شود.
- [x] ACTIVEهایی که download معتبر دارند با prior مثبت و lease اولیه محافظت شوند.
- [x] PROBATIONهایی که سابقهٔ download دارند prior ضعیف‌تر ولی مثبت بگیرند.
- [x] node بدون سابقه prior خنثی `Beta(1,1)` بگیرد.
- [x] از کل `test_history` backfill کامل انجام نشود؛ فقط آخرین evidence محدود یا aggregateهای موجود برای seed استفاده شود.
- [x] جدول‌های `test_history` و `system_events` قدیمی read-only-compatible باقی بمانند.
- [x] history API در دورهٔ انتقال union مرتب‌شدهٔ تاریخچهٔ قدیمی و eventهای جدید را نشان دهد.

## 9. parser و هویت فنی

- [x] decoder subscription بتواند plain، base64 و خطوط مخلوط را تشخیص دهد.
- [x] parserهای VMess/VLESS/Trojan/SS/SOCKS با fixtureهای واقعی منتقل شوند.
- [x] transportها و securityهای فعلی بدون regression منتقل شوند.
- [x] config canonical با ترتیب field ثابت ساخته شود.
- [x] fragment/remark، نام subscription و metadata نمایشی از technical hash حذف شود.
- [x] IPv4، IPv6، hostname، port، UUID/custom ID، SNI، ALPN، REALITY key/short-id و cipher validate شوند.
- [x] deprecated optionها به ساختار Xray جاری normalize شوند.
- [x] duplicateهای هم‌هویت sourceهای خود را merge کنند و آخرین remark مفید صرفاً برای نمایش حفظ شود.
- [x] parse failure با `INVALID_CONFIG` ثبت شود و process Xray برای آن ساخته نشود.
- [x] parser هیچ panic روی ورودی خراب یا بسیار بزرگ نداشته باشد.

## 10. مدل evidence و محاسبهٔ health

### 10.1 وزن evidence

وزن‌های اولیه طبق `ALGORITHM.md`:

| evidence | alpha | beta |
|---|---:|---:|
| دانلود سریع | +8 | 0 |
| دانلود قابل‌قبول | +5 | 0 |
| HTTP واقعی موفق | +3 | 0 |
| relay موفق | +1 | 0 |
| TLS timeout | 0 | +0.5 |
| TCP timeout | 0 | +1 |
| connection refused | 0 | +2 |
| relay موفق ولی download صفر/خراب | 0 | +3 |
| local overload | 0 | 0 |
| endpoint failure | 0 | 0 |

- [x] threshold دقیق download سریع/قابل‌قبول از config خوانده شود.
- [x] evidence هر stage فقط یک‌بار برای هر run اعمال شود.
- [x] retry همان stage با همان run id double-count نشود.

### 10.2 decay

- [x] evidence مثبت قوی half-life برابر ۲۴ ساعت داشته باشد.
- [x] evidence مثبت ضعیف half-life برابر ۶ ساعت داشته باشد.
- [x] timeout گذرا half-life برابر ۲ ساعت داشته باشد.
- [x] hard failure half-life برابر ۱۲ ساعت داشته باشد.
- [x] decay هنگام read/update به‌شکل lazy محاسبه شود؛ timer برای update همهٔ rowها اجرا نشود.
- [x] `health_score = alpha / (alpha + beta)` با clamp و prior امن محاسبه شود.

### 10.3 hysteresis و transition

- [x] `CANDIDATE -> ACTIVE` فقط با download واقعی موفق.
- [x] `PROBATION -> ACTIVE` با health حداقل `0.70` یا download واقعی موفق.
- [x] `ACTIVE -> PROBATION` فقط با health کمتر از `0.35` و حداقل دو failure مستقل.
- [x] `PROBATION -> DORMANT` با health کمتر از `0.15` و چند failure جداشده در زمان.
- [x] ACTIVE حداقل ۳۰ دقیقه residence time داشته باشد؛ مقدار config بین ۱۵ تا ۳۰ دقیقه قابل‌تنظیم باشد.
- [x] یک TLS timeout هرگز ACTIVE را خارج نکند.
- [x] success قوی failure streak را reset کند ولی history را حذف نکند.
- [x] transition و event aggregate در یک transaction انجام شود.

## 11. publication lease

leaseهای پیش‌فرض:

- [x] دانلود سریع: ۱۲ ساعت.
- [x] دانلود قابل‌قبول: ۶ ساعت.
- [x] HTTP واقعی: ۲ ساعت.
- [x] relay: ۳۰ دقیقه.
- [x] موفقیت قوی‌تر lease ضعیف‌تر قبلی را کوتاه نکند.
- [x] failure معمولی lease را فوراً cancel نکند.
- [x] `INVALID_CONFIG` می‌تواند انتشار را فوراً متوقف کند.
- [x] `RETIRED` فقط پس از تمام‌شدن lease از خروجی حذف شود.
- [x] انتشار فقط اگر `structurally_valid = true` و `lease > now` و state نه INVALID/RETIRED باشد.

این مدل رفتار موردنیاز «proxy سالم با شکست موقت همچنان به کاربر داده شود» را بدون نگهداری snapshotهای مبهم پیاده می‌کند.

## 12. scheduler هوشمند

### 12.1 queueها و سهم‌ها

- [x] ۴۰٪ ظرفیت: config جدید و `CANDIDATE`.
- [x] ۳۰٪ ظرفیت: `PROBATION` دارای سابقهٔ موفق.
- [x] ۲۰٪ ظرفیت: `DORMANT` قابل‌بازیابی.
- [x] ۱۰٪ ظرفیت: exploration و نمونه‌برداری از موارد کم‌اطمینان.
- [x] quota debt نگه‌داری شود تا خالی‌بودن یک queue سهم بقیه را متوقف نکند.
- [x] starvation هیچ queue ممکن نباشد.

### 12.2 priority score

- [x] lease نزدیک به انقضا: `+100`.
- [x] سابقهٔ download موفق: `+80`.
- [x] آخرین failure از نوع TLS timeout: `+50`.
- [x] مدت زیادی تست نشده: `+40`.
- [x] در upstream جدید دیده شده: `+30`.
- [x] هرگز موفق نشده و failure زیاد دارد: `-80`.
- [x] pressure بالای سیستم: job سنگین `-100`.
- [x] tie-breaker deterministic با مقدار کوچک jitter باشد.

### 12.3 circuit breaker و full jitter

فرمول:

```text
delay = random(0, min(cap, base * 2^failure_streak))
```

- [x] proxy دارای سابقهٔ success: base پنج دقیقه، cap شش ساعت.
- [x] proxy بدون success: base سی دقیقه، cap بیست‌وچهار ساعت.
- [x] dormant recovery: ۶، ۱۲ و ۲۴ ساعت.
- [x] `CONNECTION_REFUSED` نسبت به timeout backoff قوی‌تری ایجاد کند.
- [x] inconclusive result streak را زیاد نکند و retry کوتاه کنترل‌شده بگیرد.

### 12.4 lease تست و idempotency

- [x] انتخاب job با atomic claim و `test_lease_until` انجام شود.
- [x] worker crash باعث قفل دائمی node نشود؛ lease منقضی شود.
- [x] یک node هم‌زمان در دو batch قرار نگیرد.
- [x] manual test بتواند اولویت را بالا ببرد، ولی test در حال اجرا را duplicate نکند.

## 13. تست مرحله‌ای proxy

### Stage 0: parse/static

- [x] syntax، fieldهای لازم، protocol/security/transport support.
- [x] نتیجهٔ invalid مستقیم و ارزان ثبت شود.

### Stage 1: DNS و TCP

- [x] hostname resolve با timeout کوتاه و cache محدود.
- [x] TCP connect به server اصلی.
- [x] برای IP literal مرحلهٔ DNS رد شود.
- [x] DNS failure سیستم با DNS failure مقصد تفکیک شود.

### Stage 2: Xray و relay

- [x] چند proxy در یک Xray batch با inbound SOCKS جدا اجرا شوند.
- [x] config قبل از spawn validate شود.
- [x] startup readiness با deadline بررسی شود.
- [x] failure batch با recursive split، proxy خراب را isolate کند.
- [x] relay probe از داخل SOCKS انجام شود.

### Stage 3: HTTP واقعی

- [x] درخواست کوچک به حداقل دو endpoint مستقل و قابل‌تنظیم.
- [x] status، TLS، body limit و redirect policy بررسی شود.
- [x] endpoint quorum خرابی endpoint عمومی را تشخیص دهد.

### Stage 4: download محدود

- [x] فقط survivorهای Stage 3 یا موارد مهم نزدیک انقضای lease وارد شوند.
- [x] محدودیت پیش‌فرض ۲ تا ۵ ثانیه یا ۱ تا ۲ مگابایت، هرکدام زودتر رخ داد.
- [x] سرعت بر اساس byte واقعی و مدت steady window محاسبه شود.
- [x] صفر byte با relay موفق failure قوی محسوب شود.
- [x] cancellation download باعث zombie connection/process نشود.

### metadata

- [x] exit IP/country/org/timezone بعد از success و خارج از critical path اصلی خوانده شود.
- [x] خطای metadata هیچ evidence منفی به proxy ندهد.
- [x] providerها rate-limit و cache داشته باشند.

## 14. مدیریت Xray و port

- [x] path و version Xray در startup تشخیص و log شود.
- [x] process با process group/child ownership مشخص اجرا شود.
- [x] shutdown ابتدا SIGTERM و سپس بعد از deadline kill انجام دهد.
- [x] بعد از kill حتماً `wait` انجام شود تا zombie نماند.
- [x] stdout/stderr bounded و structured capture شود.
- [x] Xray test batch و persistent runtime lifecycle جدا داشته باشند.
- [x] main/test port pool از config خوانده شود.
- [x] bind واقعی port پیش از تخصیص verify شود.
- [x] port allocation اتمیک باشد.
- [x] ظرفیت ناکافی state را به `WAITING_FOR_PORT` ببرد و health را خراب نکند.
- [x] runtimeهای یتیم در startup شناسایی و فقط در scope پروژه پاک شوند.

## 15. concurrency تطبیقی و pressure

- [x] concurrency اولیه Xray برابر ۴ و download برابر ۲ باشد.
- [x] در شرایط پایدار concurrency هر window یک واحد زیاد شود.
- [x] با timeout cluster، latency spike یا pressure بالا ظرفیت در `0.7` ضرب شود.
- [x] min/max هر pool در config باشد.
- [x] CPU، load، RAM، open FD، تعداد child process و event-loop lag پایش شود.
- [x] local overload jobهای download جدید را متوقف کند، نه اینکه proxyها را خراب اعلام کند.
- [x] pressure snapshot در هر test event ثبت شود.
- [x] API مقدار concurrency جاری و علت آخرین کاهش/افزایش را نشان دهد.

## 16. global incident detector

- [x] failureهای هم‌زمان میان protocol/source/serverهای مستقل در window کوتاه aggregate شوند.
- [x] خرابی چند endpoint تست، DNS محلی، loss عمومی شبکه و overload تشخیص داده شود.
- [x] هنگام incident نتیجه‌ها inconclusive شوند و demotion/lease shortening متوقف شود.
- [x] incident شروع/پایان و evidence آن در `system_events` ثبت شود.
- [x] network sentinel فعلی به detector داده بدهد.
- [x] بعد از recovery، retryها با jitter پخش شوند تا thundering herd ایجاد نشود.

## 17. refresh و reconciliation منابع

- [x] هر refresh یک generation یکتا داشته باشد.
- [x] fetchها parallel ولی bounded باشند.
- [x] ETag و Last-Modified در صورت پشتیبانی منبع استفاده شود.
- [x] generation فقط وقتی complete است که معیار سلامت منابع پاس شود.
- [x] config موجود در هر source با technical hash ثبت شود، مستقل از remark.
- [x] proxy غایب فقط در generation کامل missing count بگیرد.
- [x] retirement فقط با هر سه شرط انجام شود:
  - [x] حداقل در سه refresh کامل هیچ منبعی آن را ندیده باشد؛
  - [x] حداقل ۱۲ تا ۲۴ ساعت از آخرین مشاهده گذشته باشد؛
  - [x] publication lease تمام شده باشد.
- [x] ابتدا tombstone/RETIRED شود؛ physical deletion با فاصله و cleanup جدا باشد.
- [x] proxy سالم دستی تا زمانی که منبع manual فعال است با غیبت upstream حذف نشود.
- [x] dead/dormantهایی که دوباره در upstream دیده می‌شوند priority مثبت بگیرند.

## 18. selection، feedback و VIP

### 18.1 best-node selection

- [x] weighted power-of-choices فعلی حفظ شود.
- [x] latency، download، health، lease freshness، availability، global usage و client usage در score باشد.
- [x] client-specific success history حفظ شود.
- [x] fairness مانع چسبیدن همهٔ کاربران به یک node شود.
- [x] فقط node دارای runtime سالم و شرایط lifecycle مناسب انتخاب شود.

### 18.2 client circuit breaker

- [x] حالت‌های CLOSED/OPEN/HALF_OPEN حفظ شوند.
- [x] feedback `used`، `broken` و `rate_limited` حفظ شود.
- [x] cooldown با exponential full jitter باشد.
- [x] client failure با global proxy health یکی نشود؛ هر دو مدل جدا بمانند.

### 18.3 VIP

- [x] VIP port فعلی و hot-runtime حفظ شود.
- [x] score شامل health، latency، download، availability و low-use باشد.
- [x] switch hysteresis حفظ شود تا flapping رخ ندهد.
- [x] VIP فقط وقتی candidate جدید به‌اندازهٔ margin مشخص بهتر است جابه‌جا شود.
- [x] switch failure باعث قطع runtime سالم قبلی نشود.

## 19. publisher و Git

- [x] publisher از snapshot transactionally-consistent استفاده کند.
- [x] فقط proxyهای دارای publication lease معتبر render شوند.
- [x] raw configها بر اساس technical identity unique شوند.
- [x] remark مناسب نمایش بدون تغییر identity تولید شود.
- [x] `active-raw.txt` plain و `active.txt` با format فعلی سازگار باشد.
- [x] اگر محتوا تغییر نکرده commit ساخته نشود.
- [x] چند تغییر نزدیک با debounce به یک commit تبدیل شود.
- [x] commit message شامل تعداد active و generation باشد.
- [x] push با SSH mount فعلی و known_hosts انجام شود.
- [x] non-fast-forward با fetch/rebase کنترل‌شده retry شود؛ reset مخرب ممنوع باشد.
- [x] failure Git هیچ اثر منفی روی proxy health نداشته باشد.
- [x] آخرین commit/push status در API و UI diagnostics دیده شود.
- [x] publisher بعد از startup تا کامل‌شدن migration و اولین snapshot معتبر صبر کند.

## 20. API جدید و توسعهٔ diagnostics

در کنار routeهای سازگار، موارد زیر اضافه شوند:

- [x] `GET /api/v1/scheduler`: queue depth، quota، concurrency، pressure و overdue jobs.
- [x] `GET /api/v1/health-model`: thresholdها، decayها و lease policy جاری.
- [x] `GET /api/v1/upstream`: آخرین generation، source health و missing counts.
- [x] `GET /api/v1/incidents`: incidentهای فعال و اخیر.
- [x] `POST /api/v1/nodes/:id/revive`: انتقال کنترل‌شده به queue recovery.

fieldهای جدید node:

- [x] `lifecycle_state`
- [x] `health_alpha`
- [x] `health_beta`
- [x] `health_score`
- [x] `next_test_at`
- [x] `publication_lease_until`
- [x] `publication_lease_kind`
- [x] `last_failure_class`
- [x] `last_seen_generation`
- [x] `upstream_missing_generations`
- [x] `evidence_summary`

### 20.1 اصول API

- [x] queryهای list همیشه pagination و سقف page size داشته باشند.
- [x] response حجیم history به‌صورت محدود و newest-first باشد.
- [x] command endpointها idempotent یا دارای operation id باشند.
- [x] timeout HTTP مستقل از timeout worker باشد.
- [x] خطاها JSON استاندارد با code/message/details داشته باشند.
- [x] endpoint health بدون query سنگین پاسخ دهد.

## 21. پنل وب

- [x] ظاهر و مسیرهای فعلی حفظ شوند؛ frontend build جدا لازم نباشد.
- [x] CSS/JS به‌صورت asset مستقل یا `include_bytes!` داخل binary قرار گیرد.
- [x] dashboard state، score، lease، next test و failure class را نشان دهد.
- [x] badge مجزا برای inconclusive/local incident نمایش داده شود.
- [x] filterهای state/country/source/protocol/failure class اضافه شوند.
- [x] history timeline stage، endpoint، latency، speed، pressure و evidence delta را نشان دهد.
- [x] diagnostics queueها، AIMD، FD/process، upstream generation، publisher و incident را نشان دهد.
- [x] manual test/revive/reload/import/cleanup با confirm و نتیجهٔ قابل‌مشاهده باشد.
- [x] صفحهٔ clients رفتار circuit فعلی را حفظ کند.
- [x] payload صفحهٔ nodes حتی با ده‌ها هزار node bounded بماند.

## 22. config و environment

- [x] config فعلی YAML/ENV خوانده و به schema جدید map شود.
- [x] unknown field با warning مشخص شود؛ typoهای مهم silent نباشند.
- [x] env secretها هیچ‌وقت log نشوند.
- [x] گروه‌های config:
  - server/API؛
  - SQLite؛
  - upstream sources/refresh؛
  - parser limits؛
  - probe endpoints/timeouts/download limits؛
  - health evidence/decay/hysteresis؛
  - publication leases؛
  - scheduler quotas/backoff/concurrency؛
  - Xray paths/ports؛
  - VIP/selection/feedback؛
  - publisher/Git؛
  - retention/cleanup.
- [x] startup config validation قبل از migration و spawn process انجام شود.
- [x] defaultها general باشند و به ISP یا LAN فعلی وابسته نباشند.

### 22.1 نگاشت config فعلی

- [x] هشت منبع `Sub1.txt` تا `Sub8.txt` فعلی barry-far از همان `subscriptions.urls` خوانده شوند و در کد hard-code نشوند.
- [x] `subscriptions.refresh_interval_seconds` به refresh service نسل‌دار منتقل شود.
- [x] `subscriptions.prune_missing_after_cycles: 2` به policy جدید حداقل سه generation کامل migrate و deprecated warning داده شود.
- [x] `publishing.retained_snapshots: 3` پس از migration نادیده گرفته و با publication lease جایگزین شود.
- [x] `health.recent_success_retention_hours` فقط برای seed migration قدیمی استفاده و سپس با leaseهای stage-specific جایگزین شود.
- [x] interval/thresholdهای candidate/probation/dead قدیمی به defaultهای scheduler/backoff جدید map شوند و keyهای منسوخ warning روشن بدهند.
- [x] `network_guard` فعلی بدون تغییر اولیه load شود و سپس ورودی global incident detector باشد.
- [x] weightهای `selection`، cooldownهای client، VIP port و port rangeهای فعلی حفظ شوند.
- [x] remote فعلی `git@github.com:moosavimaleki/proxy-fleet.git` از config خوانده شود؛ داخل binary ثابت نباشد.
- [x] config migration report در startup دقیقاً نشان دهد کدام key حفظ، تبدیل یا منسوخ شده است.

## 23. observability

- [x] logها structured با timestamp، component، proxy id، run id و failure class باشند.
- [x] raw config و credential در log mask شوند.
- [ ] counterها: parsed، deduped، invalid، tested، stage pass/fail، transitions و published.
- [x] gaugeها: queue depth، concurrency، child process، FD، active/probation/dormant و lease count.
- [ ] histogramها: stage latency، download speed، DB query و API latency.
- [x] system eventهای مهم در SQLite با retention محدود ثبت شوند.
- [x] log storm ناشی از retry با sampling/rate-limit کنترل شود.
- [x] shutdown report تعداد job cancel‌شده و processهای پاک‌شده را ثبت کند.

## 24. تست‌ها

### 24.1 unit test

- [x] تمام transitionهای lifecycle.
- [x] evidence weights و time decay با clock مصنوعی.
- [x] lease extension و expiry.
- [x] full-jitter در bound صحیح.
- [x] queue quota و starvation prevention.
- [x] failure classifier و inconclusiveها.
- [x] technical hash مستقل از remark.
- [x] parser هر protocol/transport/security.
- [x] publication filter و deterministic ordering.

### 24.2 property/fuzz test

- [x] parser روی input تصادفی panic نکند.
- [x] normalize(normalize(x)) برابر normalize(x) باشد.
- [x] تغییر remark hash را تغییر ندهد.
- [x] alpha/beta/score هیچ‌گاه NaN، infinity یا خارج از range نشوند.
- [x] scheduler هیچ node با lease تست فعال را دوباره claim نکند.

### 24.3 integration test

- [x] SQLite migration از fixture schema فعلی.
- [x] API compatibility snapshot.
- [x] mock SOCKS/HTTP endpoint برای success/timeout/refused/TLS failure.
- [x] Xray batch startup، recursive split و cleanup.
- [x] process cancellation و نبود zombie.
- [x] upstream complete/incomplete generation.
- [x] publisher no-op/change/push failure/retry.
- [x] network incident بدون demotion.

### 24.4 end-to-end

- [x] Docker با DB کپی‌شده بالا بیاید.
- [x] manual import تا publication کامل طی شود.
- [x] proxy موفق وارد ACTIVE و خروجی شود.
- [x] یک timeout بعدی آن را فوراً حذف نکند.
- [x] lease expiry بدون success جدید آن را حذف کند.
- [x] proxy recovered از DORMANT دوباره ACTIVE شود.
- [x] تمام صفحات پنل و actionها smoke test شوند.

### 24.5 benchmark

- [ ] corpus واقعی ۳۱هزار node برای parser/dedup.
- [ ] query list/filter/history در دیتابیس واقعی کپی‌شده.
- [ ] scheduler tick با queueهای بزرگ.
- [ ] event insert و aggregate update.
- [ ] API p50/p95 و response size.
- [ ] CPU/RAM/FD تحت concurrency یکسان با نسخهٔ Python.
- [ ] publisher render برای تعداد زیاد config.

## 25. budgetهای عملکرد و پذیرش

- [ ] هیچ full-table scan در scheduler tick عادی وجود نداشته باشد.
- [x] API list با pagination در اندازهٔ محدود باقی بماند.
- [ ] یک writer کند SQLite کل HTTP server را block نکند.
- [x] تعداد process Xray از سقف config بالاتر نرود.
- [ ] تعداد FD پس از چند چرخهٔ تست رشد دائمی نداشته باشد.
- [ ] idle CPU/RAM از baseline Python بدتر نباشد و هدف کاهش محسوس باشد.
- [ ] throughput تست با endpoint یکسان حداقل برابر نسخهٔ Python باشد.
- [ ] benchmark قبل و بعد همراه با config و corpus ثبت شود.

## 26. Docker و deployment

- [x] Dockerfile چندمرحله‌ای ساخته شود:
  - builder رسمی Rust برای compile؛
  - runtime slim با CA، curl، git/ssh، SQLite runtime tools و Xray.
- [x] binary به‌صورت release و stripped ساخته شود.
- [x] Xray version در build pin یا checksum-verified باشد؛ latest بدون checksum پذیرفته نشود.
- [x] compose فعلی host network، nofile، volumeها، SSH mount و healthcheck را حفظ کند.
- [x] container name و API port فعلی حفظ شود.
- [x] user داخل container تا حد ممکن non-root باشد؛ نیازهای Xray/port بررسی شود.
- [x] data/config/subscription pathهای فعلی حفظ شوند.
- [x] startup migration فقط یک instance leader داشته باشد.
- [x] healthcheck readiness را بعد از DB migration و worker startup اعلام کند.

## 27. مسیر اجرای مرحله‌ای

### فاز 0: baseline و freeze قراردادها

- [x] schema و row count دیتابیس فعلی ثبت شود.
- [x] نمونهٔ response تمام APIها ذخیره شود.
- [x] صفحه‌ها و actionهای فعلی فهرست و smoke شوند.
- [ ] CPU/RAM/FD/process/API latency و test throughput baseline ثبت شود.
- [ ] fixtureهای parser از configهای واقعی و بدون secret ساخته شوند.
- [x] لینک‌های subscription و format فعلی snapshot شوند.

معیار خروج: قرارداد رفتاری و performance baseline قابل‌تکرار است.

### فاز 1: skeleton Rust

- [x] workspace/crate، config، error، tracing و graceful shutdown.
- [x] Axum health endpoint.
- [x] SQLx pool و migration runner.
- [x] CI محلی `fmt`, `clippy`, `test`.

معیار خروج: binary داخل container اجرا و clean shutdown می‌شود.

### فاز 2: storage و migration

- [x] migrationهای افزایشی.
- [x] repositoryهای nodes/events/upstream/client.
- [x] seed evidence از schema قدیمی.
- [x] compatibility query برای history و EWMA.

معیار خروج: DB کپی‌شده بدون data loss باز و تمام countها reconcile می‌شوند.

### فاز 3: parser و ingestion

- [x] تمام protocolها و transportها.
- [x] canonical identity/dedup.
- [x] manual import و subscription decoder.
- [x] upstream generation fetch/reconcile.

معیار خروج: خروجی parser Rust روی corpus فعلی با Python مقایسه و اختلاف‌ها توضیح داده شده‌اند.

### فاز 4: health engine

- [x] event model، classifier، decay، Bayesian score.
- [x] transition/hysteresis.
- [x] publication lease.
- [x] incident-safe evidence application.

معیار خروج: تمام سناریوهای `ALGORITHM.md` با تست clock-controlled پاس می‌شوند.

### فاز 5: Xray و probe cascade

- [x] config generation.
- [x] process/port lifecycle.
- [x] Stage 0 تا 4.
- [x] batch split، cancellation، timeout budget و metadata.

معیار خروج: proxyهای known-good/known-bad با failure class صحیح دسته‌بندی می‌شوند و process leak صفر است.

### فاز 6: scheduler و pressure control

- [x] multi-queue quota.
- [x] priority/circuit/full jitter.
- [x] atomic claim.
- [x] AIMD و global incident detector.

معیار خروج: simulation چندروزه starvation، retry storm یا حذف ناگهانی ACTIVE ایجاد نمی‌کند.

### فاز 7: selection، client circuit و VIP

- [x] انتقال scoring و fairness.
- [x] feedback و per-client circuit.
- [x] persistent runtime و VIP hysteresis.

معیار خروج: رفتار API best/feedback و VIP با نسخهٔ فعلی سازگار است.

### فاز 8: API و UI

- [x] همهٔ routeهای compatibility.
- [x] APIهای جدید scheduler/upstream/incidents.
- [x] تمام صفحات و actionها.
- [x] pagination و response budget.

معیار خروج: contract tests و browser smoke تمام صفحات پاس می‌شوند.

### فاز 9: publisher

- [x] lease query، render، dedup و stable ordering.
- [x] Git commit/push/debounce/retry.
- [x] diagnostics و no-op detection.

معیار خروج: لینک‌های عمومی بدون تغییر path با خروجی معتبر و unique به‌روز می‌شوند.

### فاز 10: shadow validation

- [ ] نسخهٔ Rust روی copy دیتابیس و port جدا اجرا شود.
- [ ] ingestion و test در ابتدا بدون publisher واقعی اجرا شود.
- [ ] تصمیم‌های health دو نسخه و نتیجهٔ واقعی proxyها مقایسه شود.
- [ ] load test و soak test حداقل چند چرخهٔ کامل refresh/recovery را پوشش دهد.
- [ ] اختلاف‌های غیرمنتظره قبل از cutover رفع شوند.

معیار خروج: correctness و resource budget تأیید شده و هیچ migration blocker وجود ندارد.

### فاز 11: cutover امن

- [ ] publisher موقتاً pause شود.
- [x] backup سازگار دیتابیس گرفته شود.
- [x] container Python متوقف ولی image آن نگه داشته شود.
- [x] container Rust روی DB واقعی migration و start شود.
- [x] health/API/UI/Xray/VIP/subscription smoke شوند.
- [x] اولین publish فقط بعد از snapshot معتبر انجام شود.
- [x] metrics و logها در چند چرخه بررسی شوند.

معیار خروج: سرویس Rust تنها writer و publisher فعال است.

### فاز 12: cleanup و مستندات

- [ ] کد Python فقط بعد از پایان rollback window archive/remove شود.
- [x] schema، config، API و runbook مستند شوند.
- [ ] backupهای موقت طبق policy پاک شوند.
- [ ] benchmark نهایی و تفاوت الگوریتم ثبت شود.

## 28. rollback

- [x] image قبلی Python با tag immutable حفظ شود.
- [x] snapshot pre-migration دیتابیس تا پایان rollback window حفظ شود.
- [x] rollback script فقط این کارها را انجام دهد: توقف Rust، بازگردانی DB snapshot، اجرای image Python، health smoke.
- [x] subscription فایل سالم قبلی تا اولین publish موفق Rust overwrite نشود.
- [x] اگر migration forward-only شد، rollback حتماً با snapshot باشد نه downgrade query خطرناک.
- [x] triggerهای rollback: migration inconsistency، data loss، process leak، publisher خراب، API contract break یا افت شدید throughput.

## 29. ریسک‌ها و کنترل آن‌ها

| ریسک | کنترل |
|---|---|
| تغییر رفتار parser | corpus diff، golden test و Xray config validation |
| قفل SQLite | WAL، pool کوچک، transaction کوتاه و benchmark |
| رشد event table | index صحیح، retention/archival و aggregate cache |
| حذف اشتباه proxy سالم | hysteresis، independent failure، lease و incident detector |
| retry storm | full jitter، quota، AIMD و pressure gate |
| zombie Xray | ownership، cancellation، terminate/kill/wait و leak test |
| outage endpoint تست | چند endpoint، quorum و ENDPOINT_FAILURE |
| outage شبکهٔ محلی | sentinel، global incident و inconclusive event |
| upstream ناقص | complete generation requirement |
| push conflict | debounce، fetch/rebase محدود و no destructive reset |
| migration دیتابیس بزرگ | additive migration، no full history rewrite، shadow copy |
| شکستن کاربران API/subscription | compatibility tests و stable paths |

## 30. چک‌لیست نهایی parity

- [x] subscription ingest
- [x] manual import
- [x] remark-insensitive dedup
- [x] VMess/VLESS/Trojan/SS/SOCKS
- [x] تمام transport/securityهای فعلی
- [x] static validation
- [x] batch relay و recursive isolation
- [x] real HTTP و bounded download
- [x] retry/revival/quarantine
- [x] persistent active runtime
- [x] port pools و waiting state
- [x] exit metadata
- [x] network sentinel
- [x] weighted selection/fairness
- [x] client feedback/circuit breaker
- [x] VIP/hot port/hysteresis
- [x] SQLite persistence و history
- [x] تمام API routeهای فعلی
- [x] تمام صفحه‌های UI فعلی
- [x] logs/diagnostics/manual actions
- [x] active.txt و active-raw.txt
- [x] Git commit/push خودکار
- [x] Docker host network/volumes/SSH/healthcheck
- [x] graceful startup/shutdown/recovery

## 31. تعریف Done

بازنویسی فقط وقتی Done است که:

- [x] تمام parity itemها پاس شده باشند.
- [ ] تمام قواعد `ALGORITHM.md` تست خودکار داشته باشند.
- [x] دیتابیس واقعی کپی‌شده بدون حذف داده مهاجرت کرده باشد.
- [x] ACTIVE با یک failure گذرا از انتشار حذف نشود.
- [x] INVALID و RETIRED اشتباهاً منتشر نشوند.
- [x] DORMANTها طبق backoff دوباره فرصت بگیرند.
- [x] outage محلی health جمعی proxyها را خراب نکند.
- [x] API و UI با دادهٔ واقعی responsive بمانند.
- [ ] Xray process/port/FD leak در soak test وجود نداشته باشد.
- [x] publisher خروجی unique و قابل‌مصرف تولید و push کند.
- [x] Docker جدید روی سیستم فعلی healthy باشد.
- [ ] benchmark نهایی correctness، سرعت و مصرف منابع را ثبت کرده باشد.
- [ ] rollback یک بار عملاً روی محیط staging تمرین شده باشد.

## 32. ترتیب شروع پیاده‌سازی

اولین commit اجرایی نباید API یا UI باشد. ترتیب اجباری شروع:

1. قراردادها و baseline.
2. skeleton Rust و migration روی DB کپی.
3. parser/identity.
4. event + health + lease.
5. Xray/probe.
6. scheduler/AIMD/incident.
7. selection/VIP.
8. API/UI.
9. publisher.
10. shadow run، cutover و cleanup.

این ترتیب باعث می‌شود UI یا publisher روی یک health model ناقص ساخته نشوند و مهاجرت داده از ابتدا بخشی از طراحی باشد، نه کاری پرخطر در انتهای پروژه.
