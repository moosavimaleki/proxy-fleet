use std::time::{Duration, Instant};

use futures_util::{StreamExt, stream};
use reqwest::Proxy;

use crate::{
    config::AppConfig,
    domain::{evidence::TestStage, failure::FailureClass},
    parser::{ParsedProxy, parse_share_url},
    xray::{XrayBatchSession, XraySession, allocate_port, allocate_ports},
};

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
    if let Err(failure) = preflight(&proxy, config).await {
        return ProbeReport {
            proxy: Some(proxy),
            events: vec![failure],
        };
    }
    let socks_port = match allocate_port(config.ports.test.start..=config.ports.test.end).await {
        Ok(port) => port,
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
    let mut session = match XraySession::start(&config.xray_bin, &proxy, socks_port).await {
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
    let events = test_through_xray(socks_port, config).await;
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
    configs: Vec<(String, String)>,
    source: &str,
    config: &AppConfig,
    download_concurrency: usize,
) -> Vec<(String, ProbeReport)> {
    let source = source.to_owned();
    let preflight_results = stream::iter(configs.into_iter().map(|(id, raw)| {
        let source = source.clone();
        async move {
            let report = match parse_share_url(&raw, &source) {
                Ok(proxy) => match preflight(&proxy, config).await {
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
            (id, report)
        }
    }))
    .buffer_unordered(download_concurrency.max(1).saturating_mul(2))
    .collect::<Vec<_>>()
    .await;

    let mut results = std::collections::BTreeMap::<String, ProbeReport>::new();
    let mut survivors = Vec::new();
    for (id, report) in preflight_results {
        if let Some(proxy) = report.proxy.clone() {
            if report.events.is_empty() {
                survivors.push((id.clone(), proxy));
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

async fn preflight(proxy: &ParsedProxy, config: &AppConfig) -> Result<(), ProbeEvent> {
    let tcp_started = Instant::now();
    let address = proxy.address.clone();
    let port = proxy.port;
    let addresses = match tokio::time::timeout(
        Duration::from_secs(3),
        tokio::net::lookup_host((address.as_str(), port)),
    )
    .await
    {
        Ok(Ok(addresses)) => addresses.collect::<Vec<_>>(),
        Ok(Err(error)) => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::DnsFailure,
                None,
                serde_json::json!({"error":error.to_string()}),
            ));
        }
        Err(_) => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::DnsFailure,
                None,
                serde_json::json!({"error":"DNS timeout"}),
            ));
        }
    };
    if addresses.is_empty() {
        return Err(event(
            TestStage::DnsTcp,
            FailureClass::DnsFailure,
            None,
            serde_json::json!({"error":"no address returned"}),
        ));
    }
    let connected = tokio::time::timeout(
        Duration::from_millis(config.health.relay_timeout_ms),
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
                serde_json::json!({"error":error.to_string()}),
            ));
        }
        Ok(Err(error)) => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::TcpTimeout,
                Some(tcp_started.elapsed()),
                serde_json::json!({"error":error.to_string()}),
            ));
        }
        Err(_) => {
            return Err(event(
                TestStage::DnsTcp,
                FailureClass::TcpTimeout,
                Some(tcp_started.elapsed()),
                serde_json::json!({"error":"TCP timeout"}),
            ));
        }
    }
    Ok(())
}

async fn test_survivor_batch(
    survivors: Vec<(String, ParsedProxy)>,
    config: &AppConfig,
    download_concurrency: usize,
) -> Vec<(String, Vec<ProbeEvent>)> {
    if survivors.is_empty() {
        return Vec::new();
    }
    let ports = match allocate_ports(
        config.ports.test.start..=config.ports.test.end,
        survivors.len(),
    )
    .await
    {
        Ok(ports) => ports,
        Err(error) => {
            return survivors
                .into_iter()
                .map(|(id, _)| {
                    (
                        id,
                        vec![event(
                            TestStage::Relay,
                            FailureClass::LocalOverload,
                            None,
                            serde_json::json!({"error":error.to_string()}),
                        )],
                    )
                })
                .collect();
        }
    };
    let proxies: Vec<_> = survivors.iter().map(|(_, proxy)| proxy.clone()).collect();
    match XrayBatchSession::start(&config.xray_bin, &proxies, ports.clone()).await {
        Ok(mut session) => {
            let results =
                stream::iter(
                    survivors
                        .into_iter()
                        .zip(ports)
                        .map(|((id, _), port)| async move {
                            (id, test_through_xray(port, config).await)
                        }),
                )
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

async fn test_through_xray(socks_port: u16, config: &AppConfig) -> Vec<ProbeEvent> {
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
    let mut endpoints = vec![config.health.test_url.clone()];
    endpoints.extend(config.health.fallback_urls.iter().cloned());
    let mut endpoint_failure_count = 0;
    for endpoint in endpoints.iter().take(3) {
        let started = Instant::now();
        match client.get(endpoint).send().await {
            Ok(response)
                if response.status().is_success() || response.status().is_redirection() =>
            {
                let mut events = vec![ProbeEvent {
                    stage: TestStage::Relay,
                    class: FailureClass::Success,
                    fast_download: false,
                    latency_ms: Some(started.elapsed().as_secs_f64() * 1000.0),
                    download_bps: None,
                    bytes_transferred: None,
                    duration_ms: Some(started.elapsed().as_millis() as i64),
                    endpoint: Some(endpoint.clone()),
                    detail: serde_json::json!({"status":response.status().as_u16()}),
                }];
                events.push(ProbeEvent {
                    stage: TestStage::Http,
                    class: FailureClass::Success,
                    fast_download: false,
                    latency_ms: Some(started.elapsed().as_secs_f64() * 1000.0),
                    download_bps: None,
                    bytes_transferred: None,
                    duration_ms: Some(started.elapsed().as_millis() as i64),
                    endpoint: Some(endpoint.clone()),
                    detail: serde_json::json!({"status":response.status().as_u16()}),
                });
                if config.download_test.enabled {
                    events.push(download(&client, config).await);
                }
                return events;
            }
            Ok(response) => {
                endpoint_failure_count += 1;
                if response.status().is_server_error() {
                    continue;
                }
                return vec![event(
                    TestStage::Http,
                    FailureClass::HttpFailure,
                    Some(started.elapsed()),
                    serde_json::json!({"status":response.status().as_u16()}),
                )];
            }
            Err(error) if error.is_timeout() => {
                endpoint_failure_count += 1;
                if endpoint_failure_count >= 2 {
                    return vec![event(
                        TestStage::Relay,
                        FailureClass::RelayTimeout,
                        Some(started.elapsed()),
                        serde_json::json!({"error":error.to_string()}),
                    )];
                }
            }
            Err(error) => {
                endpoint_failure_count += 1;
                if endpoint_failure_count >= 2 {
                    return vec![event(
                        TestStage::Http,
                        FailureClass::EndpointFailure,
                        Some(started.elapsed()),
                        serde_json::json!({"error":error.to_string()}),
                    )];
                }
            }
        }
    }
    vec![event(
        TestStage::Http,
        FailureClass::EndpointFailure,
        None,
        serde_json::json!({"error":"all HTTP endpoints failed"}),
    )]
}

async fn download(client: &reqwest::Client, config: &AppConfig) -> ProbeEvent {
    let endpoint = config.download_test.test_url.clone();
    let started = Instant::now();
    let response = match tokio::time::timeout(
        Duration::from_secs(config.download_test.timeout_seconds),
        client.get(&endpoint).send(),
    )
    .await
    {
        Ok(Ok(response)) if response.status().is_success() => response,
        Ok(Ok(response)) => {
            return event(
                TestStage::Download,
                FailureClass::HttpFailure,
                Some(started.elapsed()),
                serde_json::json!({"status":response.status().as_u16()}),
            );
        }
        Ok(Err(error)) if error.is_timeout() => {
            return event(
                TestStage::Download,
                FailureClass::DownloadTimeout,
                Some(started.elapsed()),
                serde_json::json!({"error":error.to_string()}),
            );
        }
        Ok(Err(error)) => {
            return event(
                TestStage::Download,
                FailureClass::EndpointFailure,
                Some(started.elapsed()),
                serde_json::json!({"error":error.to_string()}),
            );
        }
        Err(_) => {
            return event(
                TestStage::Download,
                FailureClass::DownloadTimeout,
                Some(started.elapsed()),
                serde_json::json!({"error":"download deadline exceeded"}),
            );
        }
    };
    let mut stream = response.bytes_stream();
    let mut bytes = 0_usize;
    let max_bytes = 1_000_000_usize;
    while bytes < max_bytes {
        match tokio::time::timeout(
            Duration::from_secs(config.download_test.timeout_seconds),
            stream.next(),
        )
        .await
        {
            Ok(Some(Ok(chunk))) => bytes += chunk.len(),
            Ok(Some(Err(error))) => {
                return event(
                    TestStage::Download,
                    FailureClass::DownloadTimeout,
                    Some(started.elapsed()),
                    serde_json::json!({"error":error.to_string()}),
                );
            }
            Ok(None) => break,
            Err(_) => {
                return event(
                    TestStage::Download,
                    FailureClass::DownloadTimeout,
                    Some(started.elapsed()),
                    serde_json::json!({"error":"download stream deadline exceeded"}),
                );
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
        endpoint: Some(endpoint),
        detail: serde_json::json!({"kbps":kbps}),
    }
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
