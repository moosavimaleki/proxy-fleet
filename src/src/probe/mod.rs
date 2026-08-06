use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

use futures_util::{StreamExt, stream};
use reqwest::Proxy;

use crate::{
    config::AppConfig,
    domain::{evidence::TestStage, failure::FailureClass},
    parser::{ParsedProxy, parse_share_url},
    xray::{XrayBatchSession, XraySession, allocate_port, allocate_ports},
};

const DNS_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const DNS_CACHE_CAPACITY: usize = 1024;
type DnsCacheKey = (String, u16);
type DnsCacheEntry = (Instant, Vec<SocketAddr>);

/// A tiny process-local DNS cache prevents a large candidate cohort from
/// repeatedly asking the host resolver for the same endpoint. It is only a
/// performance cache: failed lookups are never cached and every entry expires
/// quickly enough for subscription endpoints that rotate addresses.
static DNS_CACHE: LazyLock<Mutex<HashMap<DnsCacheKey, DnsCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone)]
pub struct ProbeEvent {
    pub stage: TestStage,
    pub class: FailureClass,
    pub fast_download: bool,
    pub latency_ms: Option<f64>,
    pub download_bps: Option<f64>,
    pub bytes_transferred: Option<i64>,
    pub duration_ms: Option<i64>,
    pub endpoint: Option<String>,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct ProbeReport {
    pub proxy: Option<ParsedProxy>,
    pub events: Vec<ProbeEvent>,
}

pub async fn test(raw_config: &str, source: &str, config: &AppConfig) -> ProbeReport {
    let deadline = probe_deadline(config);
    let proxy = match parse_share_url(raw_config, source) {
        Ok(proxy) => proxy,
        Err(error) => {
            return ProbeReport {
                proxy: None,
                events: vec![event(
                    TestStage::Static,
                    FailureClass::InvalidConfig,
                    None,
                    serde_json::json!({"error":error.to_string()}),
                )],
            };
        }
    };
    if let Err(failure) = preflight(&proxy, config, deadline).await {
        return ProbeReport {
            proxy: Some(proxy),
            events: vec![failure],
        };
    }
    let reservation = match allocate_port(config.ports.test.start..=config.ports.test.end).await {
        Ok(reservation) => reservation,
        Err(error) => {
            return ProbeReport {
                proxy: Some(proxy),
                events: vec![event(
                    TestStage::Relay,
                    FailureClass::LocalOverload,
                    None,
                    serde_json::json!({"error":error.to_string()}),
                )],
            };
        }
    };
    let socks_port = reservation.port();
    let mut session = match XraySession::start(&config.xray_bin, &proxy, reservation).await {
        Ok(session) => session,
        Err(error) => {
            return ProbeReport {
                proxy: Some(proxy),
                events: vec![event(
                    TestStage::Relay,
                    FailureClass::XrayStartFailed,
                    None,
                    serde_json::json!({"error":error.to_string()}),
                )],
            };
        }
    };
    let events = test_through_xray(socks_port, config, deadline, true).await;
    session.stop().await;
    ProbeReport {
        proxy: Some(proxy),
        events,
    }
}

/// Tests a scheduler cohort with one Xray process.  A startup error is split
/// recursively, so a malformed config cannot make the complete cohort look
/// dead.  The cheap preflight remains per-proxy and happens before Xray is
/// allocated.
pub async fn test_batch(
    configs: Vec<(String, String, bool)>,
    source: &str,
    config: &AppConfig,
    download_concurrency: usize,
) -> Vec<(String, ProbeReport)> {
    let source = source.to_owned();
    let preflight_results = stream::iter(configs.into_iter().map(|(id, raw, download_due)| {
        let source = source.clone();
        async move {
            let deadline = probe_deadline(config);
            let report = match parse_share_url(&raw, &source) {
                Ok(proxy) => match preflight(&proxy, config, deadline).await {
                    Ok(()) => ProbeReport {
                        proxy: Some(proxy),
                        events: Vec::new(),
                    },
                    Err(failure) => ProbeReport {
                        proxy: Some(proxy),
                        events: vec![failure],
                    },
                },
                Err(error) => ProbeReport {
                    proxy: None,
                    events: vec![event(
                        TestStage::Static,
                        FailureClass::InvalidConfig,
                        None,
                        serde_json::json!({"error":error.to_string()}),
                    )],
                },
            };
            (id, report, deadline, download_due)
        }
    }))
    .buffer_unordered(download_concurrency.max(1).saturating_mul(2))
    .collect::<Vec<_>>()
    .await;

    let mut results = std::collections::BTreeMap::<String, ProbeReport>::new();
    let mut survivors = Vec::new();
    for (id, report, deadline, download_due) in preflight_results {
        if let Some(proxy) = report.proxy.clone() {
            if report.events.is_empty() {
                survivors.push((id.clone(), proxy, deadline, download_due));
            }
        }
        results.insert(id, report);
    }
    for (id, events) in test_survivor_batch(survivors, config, download_concurrency).await {
        if let Some(report) = results.get_mut(&id) {
            report.events = events;
        }
    }
    results.into_iter().collect()
}

async fn preflight(
    proxy: &ParsedProxy,
    config: &AppConfig,
    deadline: Instant,
) -> Result<(), ProbeEvent> {
    let tcp_started = Instant::now();
    let (addresses, dns_detail) = resolve_destination(&proxy.address, proxy.port, deadline).await?;
    let Some(remaining) = remaining_budget(deadline) else {
        return Err(event(
            TestStage::DnsTcp,
            FailureClass::TcpTimeout,
            Some(tcp_started.elapsed()),
            serde_json::json!({"error":"global probe deadline exceeded before TCP"}),
        ));
    };
    let connected = tokio::time::timeout(
        remaining.min(Duration::from_millis(config.health.relay_timeout_ms)),
        tokio::net::TcpStream::connect(addresses[0]),
    )
    .await;
    match connected {
        Ok(Ok(_)) => {}
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::ConnectionRefused,
                Some(tcp_started.elapsed()),
                serde_json::json!({"error":error.to_string(),"dns":dns_detail}),
            ));
        }
        Ok(Err(error)) => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::TcpTimeout,
                Some(tcp_started.elapsed()),
                serde_json::json!({"error":error.to_string(),"dns":dns_detail}),
            ));
        }
        Err(_) => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::TcpTimeout,
                Some(tcp_started.elapsed()),
                serde_json::json!({"error":"TCP timeout","dns":dns_detail}),
            ));
        }
    }
    Ok(())
}

async fn resolve_destination(
    address: &str,
    port: u16,
    deadline: Instant,
) -> Result<(Vec<SocketAddr>, &'static str), ProbeEvent> {
    if let Ok(ip) = address.parse::<IpAddr>() {
        // IP literals must not be sent through the DNS resolver. This is both
        // cheaper and separates a destination TCP failure from local DNS.
        return Ok((vec![SocketAddr::new(ip, port)], "literal"));
    }
    let key = (address.to_owned(), port);
    if let Some((inserted_at, addresses)) = DNS_CACHE
        .lock()
        .expect("DNS cache mutex is not poisoned")
        .get(&key)
        .cloned()
        .filter(|(inserted_at, _)| inserted_at.elapsed() < DNS_CACHE_TTL)
    {
        let _ = inserted_at;
        return Ok((addresses, "cache"));
    }
    let Some(remaining) = remaining_budget(deadline) else {
        return Err(event(
            TestStage::DnsTcp,
            FailureClass::DnsFailure,
            None,
            serde_json::json!({"error":"global probe deadline exceeded before DNS","scope":"local_resolver"}),
        ));
    };
    let addresses = match tokio::time::timeout(
        remaining.min(Duration::from_secs(3)),
        tokio::net::lookup_host((address, port)),
    )
    .await
    {
        Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
        Ok(Err(error)) => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::DnsFailure,
                None,
                serde_json::json!({"error":error.to_string(),"scope":"destination_resolver"}),
            ));
        }
        Err(_) => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::DnsFailure,
                None,
                serde_json::json!({"error":"DNS timeout","scope":"destination_resolver"}),
            ));
        }
    };
    if addresses.is_empty() {
        return Err(event(
            TestStage::DnsTcp,
            FailureClass::DnsFailure,
            None,
            serde_json::json!({"error":"no address returned","scope":"destination_resolver"}),
        ));
    }
    let mut cache = DNS_CACHE.lock().expect("DNS cache mutex is not poisoned");
    if cache.len() >= DNS_CACHE_CAPACITY {
        cache.retain(|_, (inserted_at, _)| inserted_at.elapsed() < DNS_CACHE_TTL);
        if cache.len() >= DNS_CACHE_CAPACITY {
            cache.clear();
        }
    }
    cache.insert(key, (Instant::now(), addresses.clone()));
    Ok((addresses, "lookup"))
}

async fn test_survivor_batch(
    survivors: Vec<(String, ParsedProxy, Instant, bool)>,
    config: &AppConfig,
    download_concurrency: usize,
) -> Vec<(String, Vec<ProbeEvent>)> {
    if survivors.is_empty() {
        return Vec::new();
    }
    let reservations = match allocate_ports(
        config.ports.test.start..=config.ports.test.end,
        survivors.len(),
    )
    .await
    {
        Ok(reservations) => reservations,
        Err(error) => {
            return survivors
                .into_iter()
                .map(|(id, _, _, _)| {
                    (
                        id,
                        vec![event(
                            TestStage::Relay,
                            FailureClass::LocalOverload,
                            None,
                            serde_json::json!({"error":error.to_string(), "reason":"port_capacity"}),
                        )],
                    )
                })
                .collect();
        }
    };
    let proxies: Vec<_> = survivors
        .iter()
        .map(|(_, proxy, _, _)| proxy.clone())
        .collect();
    let ports: Vec<_> = reservations
        .iter()
        .map(|reservation| reservation.port())
        .collect();
    match XrayBatchSession::start(&config.xray_bin, &proxies, reservations).await {
        Ok(mut session) => {
            let results = stream::iter(survivors.into_iter().zip(ports).map(
                |((id, _, deadline, download_due), port)| async move {
                    (
                        id,
                        test_through_xray(port, config, deadline, download_due).await,
                    )
                },
            ))
            .buffer_unordered(download_concurrency.max(1))
            .collect::<Vec<_>>()
            .await;
            session.stop().await;
            results
        }
        Err(error) if survivors.len() == 1 => vec![(
            survivors[0].0.clone(),
            vec![event(
                TestStage::Relay,
                FailureClass::XrayStartFailed,
                None,
                serde_json::json!({"error":error.to_string()}),
            )],
        )],
        Err(_) => {
            let midpoint = survivors.len() / 2;
            let mut left = Box::pin(test_survivor_batch(
                survivors[..midpoint].to_vec(),
                config,
                download_concurrency,
            ))
            .await;
            left.extend(
                Box::pin(test_survivor_batch(
                    survivors[midpoint..].to_vec(),
                    config,
                    download_concurrency,
                ))
                .await,
            );
            left
        }
    }
}

async fn test_through_xray(
    socks_port: u16,
    config: &AppConfig,
    deadline: Instant,
    run_download: bool,
) -> Vec<ProbeEvent> {
    let client = match reqwest::Client::builder()
        .proxy(
            Proxy::all(format!("socks5h://127.0.0.1:{socks_port}"))
                .expect("constant SOCKS proxy URL"),
        )
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(config.download_test.timeout_seconds))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return vec![event(
                TestStage::Relay,
                FailureClass::LocalOverload,
                None,
                serde_json::json!({"error":error.to_string()}),
            )];
        }
    };
    let endpoints = http_endpoints(config);
    let endpoint_count = endpoints
        .len()
        .min(config.health.http_probe_max_endpoints.max(1));
    let quorum = config
        .health
        .http_probe_success_quorum
        .clamp(1, endpoint_count.max(1));
    let mut successes = Vec::new();
    let mut timeouts = 0_usize;
    let mut tls_failures = 0_usize;
    let mut failures = Vec::new();
    for endpoint in endpoints.iter().take(endpoint_count) {
        let started = Instant::now();
        let Some(remaining) = remaining_budget(deadline) else {
            return vec![event(
                TestStage::Relay,
                FailureClass::RelayTimeout,
                Some(started.elapsed()),
                serde_json::json!({"error":"global probe deadline exceeded"}),
            )];
        };
        match tokio::time::timeout(remaining, client.get(endpoint).send()).await {
            Ok(Ok(response))
                if response.status().is_success() || response.status().is_redirection() =>
            {
                let status = response.status().as_u16();
                let body_read = read_http_body_limited(
                    response,
                    config.health.http_probe_body_limit_bytes,
                    deadline,
                )
                .await;
                match body_read {
                    Ok(bytes) => successes.push(serde_json::json!({
                        "endpoint":endpoint,
                        "status":status,
                        "bytes":bytes,
                        "latency_ms":started.elapsed().as_secs_f64() * 1000.0,
                    })),
                    Err(detail) => failures.push(detail),
                }
            }
            Ok(Ok(response)) => {
                failures.push(
                    serde_json::json!({"endpoint":endpoint,"status":response.status().as_u16()}),
                );
            }
            Ok(Err(error)) => {
                match classify_request_error(&error, FailureClass::RelayTimeout) {
                    FailureClass::RelayTimeout => timeouts += 1,
                    FailureClass::TlsTimeout => tls_failures += 1,
                    _ => {}
                }
                failures.push(serde_json::json!({"endpoint":endpoint,"error":error.to_string()}));
            }
            Err(_) => {
                return vec![event(
                    TestStage::Relay,
                    FailureClass::RelayTimeout,
                    Some(started.elapsed()),
                    serde_json::json!({"error":"global probe deadline exceeded"}),
                )];
            }
        }
    }
    if successes.len() >= quorum {
        let first = &successes[0];
        let latency_ms = first["latency_ms"].as_f64();
        let endpoint = first["endpoint"].as_str().map(str::to_owned);
        let details = serde_json::json!({"quorum":quorum,"successful":successes,"failed":failures});
        let mut events = vec![
            ProbeEvent {
                stage: TestStage::Relay,
                class: FailureClass::Success,
                fast_download: false,
                latency_ms,
                download_bps: None,
                bytes_transferred: None,
                duration_ms: latency_ms.map(|value| value.round() as i64),
                endpoint: endpoint.clone(),
                detail: details.clone(),
            },
            ProbeEvent {
                stage: TestStage::Http,
                class: FailureClass::Success,
                fast_download: false,
                latency_ms,
                download_bps: None,
                bytes_transferred: None,
                duration_ms: latency_ms.map(|value| value.round() as i64),
                endpoint,
                detail: details,
            },
        ];
        if config.download_test.enabled && run_download {
            events.push(download(&client, config, deadline).await);
        }
        return events;
    }
    let class = if timeouts == endpoint_count && endpoint_count > 0 {
        FailureClass::RelayTimeout
    } else if tls_failures == endpoint_count && endpoint_count > 0 {
        FailureClass::TlsTimeout
    } else {
        FailureClass::EndpointFailure
    };
    vec![event(
        TestStage::Http,
        class,
        None,
        serde_json::json!({"error":"HTTP endpoint quorum not reached","quorum":quorum,"failed":failures}),
    )]
}

fn http_endpoints(config: &AppConfig) -> Vec<String> {
    let mut endpoints = Vec::with_capacity(1 + config.health.fallback_urls.len());
    endpoints.push(config.health.test_url.clone());
    endpoints.extend(config.health.fallback_urls.iter().cloned());
    let mut seen = std::collections::HashSet::new();
    endpoints.retain(|endpoint| seen.insert(endpoint.clone()));
    endpoints
}

async fn read_http_body_limited(
    response: reqwest::Response,
    limit: usize,
    deadline: Instant,
) -> Result<usize, serde_json::Value> {
    let mut stream = response.bytes_stream();
    let mut bytes = 0_usize;
    while bytes < limit {
        let Some(remaining) = remaining_budget(deadline) else {
            return Err(
                serde_json::json!({"error":"global probe deadline exceeded while reading body"}),
            );
        };
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(chunk))) => bytes = bytes.saturating_add(chunk.len()),
            Ok(Some(Err(error))) => return Err(serde_json::json!({"error":error.to_string()})),
            Ok(None) => break,
            Err(_) => return Err(serde_json::json!({"error":"HTTP body read deadline exceeded"})),
        }
    }
    Ok(bytes.min(limit))
}

async fn download(client: &reqwest::Client, config: &AppConfig, deadline: Instant) -> ProbeEvent {
    // A single download mirror is not evidence that the proxy is bad.  Try
    // the configured mirrors in order and only produce negative proxy
    // evidence when independent endpoints time out.  An HTTP/status failure
    // from every mirror is explicitly inconclusive: it is usually a mirror or
    // route policy issue, not a property of the proxy.
    let endpoints = download_endpoints(config);

    let mut failures = Vec::new();
    for endpoint in endpoints.into_iter().take(3) {
        let probe = download_endpoint(client, config, deadline, &endpoint).await;
        if probe.class == FailureClass::Success || probe.class == FailureClass::DownloadTooSlow {
            return probe;
        }
        failures.push(probe);
        if remaining_budget(deadline).is_none() {
            break;
        }
    }

    let all_timed_out = !failures.is_empty()
        && failures
            .iter()
            .all(|item| item.class == FailureClass::DownloadTimeout);
    let all_tls_failed = !failures.is_empty()
        && failures
            .iter()
            .all(|item| item.class == FailureClass::TlsTimeout);
    let elapsed_ms = failures
        .iter()
        .filter_map(|item| item.duration_ms)
        .max()
        .unwrap_or_default();
    let details = failures
        .iter()
        .map(|item| {
            serde_json::json!({
                "endpoint": item.endpoint,
                "class": item.class.as_str(),
                "detail": item.detail,
            })
        })
        .collect::<Vec<_>>();
    ProbeEvent {
        stage: TestStage::Download,
        class: if all_timed_out {
            FailureClass::DownloadTimeout
        } else if all_tls_failed {
            FailureClass::TlsTimeout
        } else {
            FailureClass::EndpointFailure
        },
        fast_download: false,
        latency_ms: None,
        download_bps: None,
        bytes_transferred: None,
        duration_ms: Some(elapsed_ms),
        endpoint: None,
        detail: serde_json::json!({"attempts":details}),
    }
}

fn download_endpoints(config: &AppConfig) -> Vec<String> {
    let mut endpoints = Vec::with_capacity(1 + config.download_test.fallback_urls.len());
    endpoints.push(config.download_test.test_url.clone());
    endpoints.extend(config.download_test.fallback_urls.iter().cloned());
    let mut seen = std::collections::HashSet::new();
    endpoints.retain(|endpoint| seen.insert(endpoint.clone()));
    endpoints
}

async fn download_endpoint(
    client: &reqwest::Client,
    config: &AppConfig,
    deadline: Instant,
    endpoint: &str,
) -> ProbeEvent {
    let started = Instant::now();
    let per_url_timeout = Duration::from_secs_f64(
        config
            .download_test
            .per_url_timeout_seconds
            .max(0.1)
            .min(config.download_test.timeout_seconds.max(1) as f64),
    );
    let Some(remaining) = remaining_budget(deadline) else {
        return event(
            TestStage::Download,
            FailureClass::DownloadTimeout,
            Some(started.elapsed()),
            serde_json::json!({"endpoint":endpoint,"error":"global probe deadline exceeded"}),
        );
    };
    let response =
        match tokio::time::timeout(remaining.min(per_url_timeout), client.get(endpoint).send())
            .await
        {
            Ok(Ok(response)) if response.status().is_success() => response,
            Ok(Ok(response)) => {
                return ProbeEvent {
                    endpoint: Some(endpoint.to_owned()),
                    ..event(
                        TestStage::Download,
                        FailureClass::EndpointFailure,
                        Some(started.elapsed()),
                        serde_json::json!({"status":response.status().as_u16()}),
                    )
                };
            }
            Ok(Err(error)) => {
                return ProbeEvent {
                    endpoint: Some(endpoint.to_owned()),
                    ..event(
                        TestStage::Download,
                        classify_request_error(&error, FailureClass::DownloadTimeout),
                        Some(started.elapsed()),
                        serde_json::json!({"error":error.to_string()}),
                    )
                };
            }
            Err(_) => {
                return ProbeEvent {
                    endpoint: Some(endpoint.to_owned()),
                    ..event(
                        TestStage::Download,
                        FailureClass::DownloadTimeout,
                        Some(started.elapsed()),
                        serde_json::json!({"error":"per-endpoint deadline exceeded"}),
                    )
                };
            }
        };
    let mut stream = response.bytes_stream();
    let mut bytes = 0_usize;
    const MAX_BYTES: usize = 1_000_000;
    while bytes < MAX_BYTES {
        let Some(remaining) = remaining_budget(deadline) else {
            return ProbeEvent {
                endpoint: Some(endpoint.to_owned()),
                ..event(
                    TestStage::Download,
                    FailureClass::DownloadTimeout,
                    Some(started.elapsed()),
                    serde_json::json!({"error":"global probe deadline exceeded"}),
                )
            };
        };
        match tokio::time::timeout(remaining.min(per_url_timeout), stream.next()).await {
            Ok(Some(Ok(chunk))) => bytes += chunk.len(),
            Ok(Some(Err(error))) => {
                return ProbeEvent {
                    endpoint: Some(endpoint.to_owned()),
                    ..event(
                        TestStage::Download,
                        FailureClass::DownloadTimeout,
                        Some(started.elapsed()),
                        serde_json::json!({"error":error.to_string()}),
                    )
                };
            }
            Ok(None) => break,
            Err(_) => {
                return ProbeEvent {
                    endpoint: Some(endpoint.to_owned()),
                    ..event(
                        TestStage::Download,
                        FailureClass::DownloadTimeout,
                        Some(started.elapsed()),
                        serde_json::json!({"error":"download stream deadline exceeded"}),
                    )
                };
            }
        }
    }
    let duration = started.elapsed();
    let bps = bytes as f64 / duration.as_secs_f64().max(0.001);
    let kbps = bps / 1024.0;
    let class = if kbps < config.download_test.min_download_kbps as f64 {
        FailureClass::DownloadTooSlow
    } else {
        FailureClass::Success
    };
    ProbeEvent {
        stage: TestStage::Download,
        class,
        fast_download: kbps >= config.download_test.target_download_kbps as f64,
        latency_ms: None,
        download_bps: Some(bps),
        bytes_transferred: Some(bytes as i64),
        duration_ms: Some(duration.as_millis() as i64),
        endpoint: Some(endpoint.to_owned()),
        detail: serde_json::json!({"kbps":kbps}),
    }
}

fn remaining_budget(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    (!remaining.is_zero()).then_some(remaining)
}

/// reqwest keeps TLS failures behind a transport error rather than exposing a
/// dedicated enum. Keep this deliberately narrow: ordinary HTTP/status and
/// connect failures remain endpoint-inconclusive, while rustls' stable
/// diagnostics identify a failed TLS tunnel.
fn classify_request_error(error: &reqwest::Error, timeout: FailureClass) -> FailureClass {
    if error.is_timeout() {
        return timeout;
    }
    // Display intentionally redacts much of the nested rustls cause. Debug
    // preserves the transport chain while still containing no proxy secret.
    let message = format!("{error:?}").to_ascii_lowercase();
    if message.contains("tls")
        || message.contains("certificate")
        || message.contains("corrupt message")
        || message.contains("wrong version number")
        || message.contains("invalidcontenttype")
    {
        FailureClass::TlsTimeout
    } else {
        FailureClass::EndpointFailure
    }
}

fn probe_deadline(config: &AppConfig) -> Instant {
    Instant::now()
        + Duration::from_secs(
            config
                .health
                .candidate_batch_timeout_seconds
                .max(config.download_test.timeout_seconds)
                .max(1),
        )
}

fn event(
    stage: TestStage,
    class: FailureClass,
    duration: Option<Duration>,
    detail: serde_json::Value,
) -> ProbeEvent {
    ProbeEvent {
        stage,
        class,
        fast_download: false,
        latency_ms: duration.map(|item| item.as_secs_f64() * 1000.0),
        download_bps: None,
        bytes_transferred: None,
        duration_ms: duration.map(|item| item.as_millis() as i64),
        endpoint: None,
        detail,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::config::AppConfig;

    use super::{
        download_endpoints, http_endpoints, remaining_budget, resolve_destination,
        test_through_xray,
    };

    #[derive(Clone, Copy)]
    enum SocksBehavior {
        SuccessHttp,
        Stall,
        CorruptTls,
    }

    async fn mock_socks(behavior: SocksBehavior) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind SOCKS mock");
        let port = listener.local_addr().expect("SOCKS address").port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("SOCKS client");
            let mut greeting = [0_u8; 2];
            stream
                .read_exact(&mut greeting)
                .await
                .expect("SOCKS greeting");
            let mut methods = vec![0_u8; greeting[1] as usize];
            stream
                .read_exact(&mut methods)
                .await
                .expect("SOCKS methods");
            stream
                .write_all(&[5, 0])
                .await
                .expect("SOCKS method response");
            if matches!(behavior, SocksBehavior::Stall) {
                tokio::time::sleep(Duration::from_secs(2)).await;
                return;
            }
            let mut request = [0_u8; 4];
            stream
                .read_exact(&mut request)
                .await
                .expect("SOCKS request");
            match request[3] {
                1 => {
                    let mut address = [0_u8; 6];
                    stream.read_exact(&mut address).await.expect("IPv4 request");
                }
                4 => {
                    let mut address = [0_u8; 18];
                    stream.read_exact(&mut address).await.expect("IPv6 request");
                }
                3 => {
                    let mut size = [0_u8; 1];
                    stream.read_exact(&mut size).await.expect("domain size");
                    let mut address = vec![0_u8; size[0] as usize + 2];
                    stream
                        .read_exact(&mut address)
                        .await
                        .expect("domain request");
                }
                atyp => panic!("unexpected SOCKS address type {atyp}"),
            }
            stream
                .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .expect("SOCKS connect response");
            let mut payload = [0_u8; 1024];
            let _ = stream.read(&mut payload).await.expect("proxied request");
            match behavior {
                SocksBehavior::SuccessHttp => stream
                    .write_all(
                        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .expect("HTTP response"),
                SocksBehavior::CorruptTls => stream
                    .write_all(b"HTTP/1.1 200 Not TLS\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .expect("corrupt TLS response"),
                SocksBehavior::Stall => unreachable!(),
            }
        });
        port
    }

    fn mock_config(endpoint: String) -> AppConfig {
        let mut config = AppConfig::default();
        config.health.test_url = endpoint;
        config.health.fallback_urls.clear();
        config.health.http_probe_max_endpoints = 1;
        config.health.http_probe_success_quorum = 1;
        config.download_test.enabled = false;
        config.download_test.timeout_seconds = 1;
        config
    }

    #[tokio::test]
    async fn socks_http_mocks_cover_success_timeout_refusal_and_tls_failure() {
        let success_port = mock_socks(SocksBehavior::SuccessHttp).await;
        let events = test_through_xray(
            success_port,
            &mock_config("http://example.test/health".to_owned()),
            Instant::now() + Duration::from_secs(2),
            false,
        )
        .await;
        assert!(events.iter().any(|event| {
            event.stage == crate::domain::evidence::TestStage::Http
                && event.class == crate::domain::failure::FailureClass::Success
        }));

        let timeout_port = mock_socks(SocksBehavior::Stall).await;
        let timeout = test_through_xray(
            timeout_port,
            &mock_config("http://example.test/health".to_owned()),
            Instant::now() + Duration::from_secs(2),
            false,
        )
        .await;
        assert_eq!(
            timeout[0].class,
            crate::domain::failure::FailureClass::RelayTimeout
        );

        let refused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind refusal port");
        let refused_port = refused_listener
            .local_addr()
            .expect("refusal address")
            .port();
        drop(refused_listener);
        let refused = test_through_xray(
            refused_port,
            &mock_config("http://example.test/health".to_owned()),
            Instant::now() + Duration::from_secs(2),
            false,
        )
        .await;
        assert_eq!(
            refused[0].class,
            crate::domain::failure::FailureClass::EndpointFailure
        );

        let tls_port = mock_socks(SocksBehavior::CorruptTls).await;
        let tls = test_through_xray(
            tls_port,
            &mock_config("https://example.test/health".to_owned()),
            Instant::now() + Duration::from_secs(2),
            false,
        )
        .await;
        assert_eq!(
            tls[0].class,
            crate::domain::failure::FailureClass::TlsTimeout
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_batch_is_recursively_isolated_and_cleans_temp_xray_configs() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("temporary Xray script directory");
        let binary = temp.path().join("failing-xray");
        tokio::fs::write(&binary, "#!/bin/sh\nexit 1\n")
            .await
            .expect("write Xray failure script");
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))
            .expect("make Xray failure script executable");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("relay preflight listener");
        let port = listener.local_addr().expect("relay address").port();
        let mut config = AppConfig {
            xray_bin: binary.to_string_lossy().to_string(),
            ..AppConfig::default()
        };
        config.download_test.enabled = false;
        let raw = |id: u8| {
            format!(
                "vless://123e4567-e89b-12d3-a456-4266141740{id:02}@127.0.0.1:{port}?security=tls&sni=example.test#fixture"
            )
        };
        let reports = super::test_batch(
            vec![
                ("one".to_owned(), raw(1), false),
                ("two".to_owned(), raw(2), false),
                ("three".to_owned(), raw(3), false),
            ],
            "test",
            &config,
            1,
        )
        .await;
        assert_eq!(reports.len(), 3);
        assert!(reports.iter().all(|(_, report)| {
            report.events.len() == 1
                && report.events[0].class == crate::domain::failure::FailureClass::XrayStartFailed
        }));
        let leftovers = std::fs::read_dir(std::env::temp_dir())
            .expect("temporary directory")
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("proxy-fleet-xray-batch-")
            })
            .count();
        assert_eq!(leftovers, 0, "failed batch leaked a temporary Xray config");
        drop(listener);
    }

    #[test]
    fn global_budget_never_returns_a_non_positive_timeout() {
        assert!(remaining_budget(Instant::now() - Duration::from_millis(1)).is_none());
        assert!(remaining_budget(Instant::now() + Duration::from_secs(1)).is_some());
    }

    #[test]
    fn download_mirrors_preserve_primary_order_and_deduplicate() {
        let mut config = AppConfig::default();
        config.download_test.test_url = "https://primary.example/file".to_owned();
        config.download_test.fallback_urls = vec![
            "https://fallback.example/file".to_owned(),
            "https://primary.example/file".to_owned(),
        ];
        assert_eq!(
            download_endpoints(&config),
            vec![
                "https://primary.example/file".to_owned(),
                "https://fallback.example/file".to_owned(),
            ]
        );
    }

    #[test]
    fn http_probe_defaults_to_independent_deduplicated_endpoints() {
        let mut config = AppConfig::default();
        config.health.test_url = "https://one.example/health".to_owned();
        config.health.fallback_urls = vec![
            "https://two.example/health".to_owned(),
            "https://one.example/health".to_owned(),
        ];
        assert_eq!(
            http_endpoints(&config),
            vec![
                "https://one.example/health".to_owned(),
                "https://two.example/health".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn ip_literal_skips_hostname_resolution() {
        let (addresses, mode) =
            resolve_destination("192.0.2.1", 443, Instant::now() + Duration::from_secs(1))
                .await
                .expect("literal resolution");
        assert_eq!(mode, "literal");
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0].to_string(), "192.0.2.1:443");
    }
}
