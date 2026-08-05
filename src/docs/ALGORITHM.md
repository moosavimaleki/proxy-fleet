## پیشنهاد نهایی من

برای `proxy-fleet` یک سیستم **Evidence-Based Health Manager** بساز؛ نه یک state machine که فقط آخرین تست را ببیند.

هستهٔ نهایی باید این ترکیب باشد:

```text
Canonical Identity
+ Event History
+ Weighted Time-Decayed Evidence
+ Bayesian Health Score
+ Hysteresis
+ Circuit Breaker
+ Publication Lease
+ Cost-Aware Scheduler
+ Adaptive Concurrency
+ Generation-Based Upstream Reconciliation
```

## ۱. وضعیت‌ها را ساده نگه دار

همین وضعیت‌ها کافی‌اند:

```text
CANDIDATE
TESTING
ACTIVE
PROBATION
DORMANT
INVALID
RETIRED
WAITING_FOR_PORT
```

تغییر پیشنهادی:

* `DEAD` را حذف یا فقط در UI نگه دار.
* موارد قابل‌بازیابی وارد `DORMANT` شوند.
* کانفیگ ساختاری خراب وارد `INVALID` شود.
* کانفیگ حذف‌شدهٔ پایدار از upstream وارد `RETIRED` شود.

`WAITING_FOR_PORT` وضعیت سلامت نیست؛ صرفاً وضعیت اجرایی scheduler است.

---

## ۲. سه مفهوم را کاملاً از هم جدا کن

هر کانفیگ باید مستقل از status این سه مقدار را داشته باشد:

```text
health_score
next_test_at
publication_lease_until
```

یعنی یک کانفیگ ممکن است:

```text
status = PROBATION
health_score = 0.58
publication_lease_until = 6 ساعت دیگر
```

پس با اینکه فعلاً مشکوک است، هنوز در subscription باقی می‌ماند چون اخیراً دانلود موفق داشته است.

---

## ۳. تمام نتایج تست را به‌صورت event ذخیره کن

جدول اصلی تصمیم‌گیری باید تاریخچهٔ append-only داشته باشد:

```text
proxy_test_events
-----------------
proxy_id
tested_at
test_stage
result
failure_class
latency_ms
download_speed_kbps
system_pressure
duration_ms
endpoint
```

هیچ نتیجه‌ای overwrite نشود. جدول proxy فقط خلاصهٔ محاسبه‌شده را نگه دارد.

---

## ۴. خطاها را طبقه‌بندی کن

حداقل این کلاس‌ها را داشته باش:

```text
INVALID_CONFIG
XRAY_START_FAILED
DNS_FAILURE
TCP_TIMEOUT
CONNECTION_REFUSED
TLS_TIMEOUT
RELAY_TIMEOUT
HTTP_FAILURE
DOWNLOAD_TIMEOUT
DOWNLOAD_TOO_SLOW
LOCAL_OVERLOAD
ENDPOINT_FAILURE
SUCCESS
```

دو مورد زیر failure پروکسی محسوب نشوند:

```text
LOCAL_OVERLOAD
ENDPOINT_FAILURE
```

این‌ها باید `INCONCLUSIVE` باشند.

---

## ۵. مدل سلامت: Beta-Bayesian با decay زمانی

برای هر کانفیگ دو پارامتر نگه دار:

```text
alpha
beta
```

برآورد سلامت:

```text
health_score = alpha / (alpha + beta)
```

وزن پیشنهادی اولیه:

```text
دانلود موفق و سریع             alpha += 8
دانلود موفق با سرعت متوسط      alpha += 5
HTTP واقعی موفق                alpha += 3
relay موفق                     alpha += 1

TLS timeout                    beta += 0.5
TCP timeout                    beta += 1
connection refused             beta += 2
relay موفق ولی دانلود صفر      beta += 3
```

شواهد قدیمی به‌تدریج ضعیف شوند:

```text
effective_weight = original_weight × exp(-age / τ)
```

پیشنهاد اولیه برای نیمه‌عمر شواهد:

```text
شواهد مثبت قوی: 24 ساعت
شواهد مثبت ضعیف: 6 ساعت
timeout موقت: 2 ساعت
شکست نسبتاً قطعی: 12 ساعت
```

بهتر است decay مثبت و منفی یکسان نباشد؛ موفقیت دانلود باید بیشتر از یک timeout دوام بیاورد.

---

## ۶. انتقال وضعیت با Hysteresis

آستانهٔ ورود و خروج یکسان نباشد.

پیشنهاد اولیه:

```text
CANDIDATE → ACTIVE
دانلود موفق واقعی

PROBATION → ACTIVE
health_score >= 0.70
یا یک دانلود موفق واقعی

ACTIVE → PROBATION
health_score < 0.35
و حداقل دو شکست مستقل

PROBATION → DORMANT
health_score < 0.15
و چند شکست در زمان‌های جدا

DORMANT → TESTING
بر اساس scheduler احیا
```

یک `TLS_TIMEOUT` به‌تنهایی هرگز نباید `ACTIVE` را خارج کند.

برای کانفیگی که اخیراً دانلود موفق داشته، حداقل زمان ماندن تعیین کن:

```text
minimum_active_residence = 15 تا 30 دقیقه
```

مگر اینکه خطای قطعی مانند invalid config رخ دهد.

---

## ۷. انتشار با lease زمانی

خروجی subscription را مستقیماً از `ACTIVE` نساز.

هر موفقیت یک lease انتشار بدهد:

```text
دانلود سریع موفق:       12 ساعت
دانلود قابل‌قبول:        6 ساعت
HTTP واقعی موفق:         2 ساعت
فقط relay موفق:          30 دقیقه
```

شرط انتشار:

```text
structurally_valid
AND publication_lease_until > now
AND status NOT IN (INVALID, RETIRED)
```

یک timeout موقت lease را لغو نکند.

lease فقط با خطاهای قطعی یا انقضای طبیعی پایان یابد.

این قسمت باید کاملاً جایگزین union سه snapshot شود.

---

## ۸. Circuit Breaker برای retest

رفتار هر کانفیگ:

```text
ACTIVE      = Closed
PROBATION   = Open
TESTING     = Half-Open
DORMANT     = Open با cooldown طولانی
```

زمان تست بعدی:

```text
delay = random(0, min(cap, base × 2^failure_streak))
```

پیشنهاد:

```text
قبلاً دانلود موفق داشته:
base = 5 دقیقه
cap = 6 ساعت

هیچ‌وقت موفق نبوده:
base = 30 دقیقه
cap = 24 ساعت

DORMANT:
تست در 6، 12 و 24 ساعت
```

حتماً jitter اضافه شود تا هزاران تست هم‌زمان بیدار نشوند.

---

## ۹. scheduler چندصفی

یک priority queue واحد کافی نیست. چهار سهم منابع داشته باش:

```text
40٪  کانفیگ‌های تازه
30٪  PROBATIONهایی که قبلاً موفق بوده‌اند
20٪  DORMANTهای قابل‌بازیابی
10٪  exploration تصادفی
```

اولویت هر کانفیگ:

```text
priority =
    publication_value
    × uncertainty
    × staleness
    × recovery_probability
    ÷ estimated_test_cost
```

اما نسخهٔ اول لازم نیست فرمول پیچیده باشد. یک امتیاز rule-based شفاف کافی است:

```text
+100  lease نزدیک انقضا
+80   قبلاً دانلود موفق داشته
+50   آخرین خطا TLS timeout بوده
+40   مدت زیادی از آخرین تست گذشته
+30   هنوز در upstream دیده می‌شود
-80   هرگز موفق نبوده و شکست‌های زیاد دارد
-100  فشار سیستم بالاست
```

---

## ۱۰. تست آبشاری

ترتیب تست:

```text
۱. parse و validation
۲. DNS و TCP
۳. راه‌اندازی Xray و relay
۴. HTTP واقعی کوچک
۵. دانلود محدود و سرعت
```

کانفیگ فقط در صورت عبور از مرحلهٔ قبلی وارد تست گران‌تر شود.

برای تست دانلود، حجم یا زمان را محدود کن:

```text
حداکثر ۲ تا ۵ ثانیه
یا
حداکثر ۱ تا ۲ مگابایت
```

برای تشخیص قابلیت استفاده همین مقدار کافی است و منابع را کنترل می‌کند.

---

## ۱۱. کنترل خودکار concurrency

concurrency ثابت نگذار.

ابتدا:

```text
xray_concurrency = 4
download_concurrency = 2
```

سپس AIMD:

```text
شرایط پایدار:
concurrency += 1

افزایش timeout عمومی یا latency:
concurrency = floor(concurrency × 0.7)
```

CPU، load، file descriptor و نرخ timeout کلی بررسی شود.

اگر در یک بازهٔ کوتاه بسیاری از پروکسی‌های مستقل هم‌زمان fail شدند:

```text
global_incident = true
```

در این حالت تست‌ها `INCONCLUSIVE` شوند و هیچ کانفیگی تنزل وضعیت نگیرد.

---

## ۱۲. reconciliation صحیح upstream

برای هر refresh یک generation بساز:

```text
upstream_refresh_runs
```

تنها refreshهای زیر معتبرند:

```text
fetch_success = true
parse_success = true
item_count غیرعادی کم نباشد
```

برای هر proxy:

```text
last_seen_upstream_at
upstream_miss_count
last_seen_generation
```

ورود به `RETIRED` فقط وقتی:

```text
در تمام sourceها غایب باشد
و حداقل ۳ refresh کامل گذشته باشد
و حداقل ۱۲ تا ۲۴ ساعت گذشته باشد
و publication lease منقضی شده باشد
```

حذف فیزیکی رکورد را دیرتر انجام بده؛ ابتدا tombstone نگه دار تا با ظهور مجدد همان `config_hash` تاریخچه برگردد.

---

# ترتیب اجرای پیشنهادی

## نسخهٔ اول

این‌ها را ابتدا اجرا کن:

```text
۱. error classification
۲. event history
۳. جلوگیری از demotion با یک timeout
۴. hysteresis
۵. publication lease
۶. backoff + jitter
۷. تفکیک INVALID / DORMANT / RETIRED
۸. upstream generations
```

این مرحله احتمالاً بخش عمدهٔ نوسان و false negative فعلی را رفع می‌کند.

## نسخهٔ دوم

```text
۹. Bayesian alpha/beta
۱۰. time decay
۱۱. priority scheduler
۱۲. adaptive concurrency
۱۳. global incident detection
```

## فعلاً اجرا نکن

```text
HMM
Neural Network
Reinforcement Learning
Thompson Sampling برای تک‌تک proxyها
```

این‌ها در شرایط فعلی پیچیدگی اضافه می‌کنند بدون اینکه دادهٔ کافی برای کالیبراسیون داشته باشید.

## تصمیم نهایی

هستهٔ مناسب پروژهٔ شما این است:

> **یک مدل Bayesian دارای حافظهٔ زمانی، همراه با hysteresis برای lifecycle، circuit breaker برای retest، lease زمانی برای publication و scheduler هزینه‌محور برای مصرف منابع.**

مهم‌ترین تغییر فوری نیز این است:

> **`ACTIVE` بودن را از «واجد شرایط انتشار بودن» جدا کن و یک timeout منفرد را هرگز دلیل حذف فوری کانفیگی با دانلود موفق اخیر قرار نده.**
