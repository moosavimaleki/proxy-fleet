//! Best-effort exit metadata enrichment for a verified persistent runtime.
//!
//! This module never writes health evidence.  It runs after the scheduler has
//! already accepted a real download, and its cache lease bounds both provider
//! traffic and duplicate work.

use std::time::Duration;

use reqwest::Proxy;

use crate::{config::MetadataConfig, domain::proxy::ExitMetadata, storage::Store};

pub async fn refresh_via_runtime(
    store: &Store,
    node_id: &str,
    socks_port: u16,
    config: &MetadataConfig,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .proxy(
            Proxy::all(format!("socks5h://127.0.0.1:{socks_port}"))
                .expect("constant local SOCKS URL"),
        )
        .connect_timeout(Duration::from_secs(config.timeout_seconds.max(1)))
        .timeout(Duration::from_secs(config.timeout_seconds.max(1)))
        .build()?;
    let value: serde_json::Value = client
        .get(&config.endpoint)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    store.record_exit_metadata(node_id, &parse(value)).await
}

fn field(value: &serde_json::Value, names: &[&str]) -> String {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(serde_json::Value::as_str))
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn parse(raw: serde_json::Value) -> ExitMetadata {
    let loc = field(&raw, &["loc"]);
    let loc = if loc.is_empty() {
        match (
            raw.get("latitude").and_then(serde_json::Value::as_f64),
            raw.get("longitude").and_then(serde_json::Value::as_f64),
        ) {
            (Some(latitude), Some(longitude)) => format!("{latitude},{longitude}"),
            _ => String::new(),
        }
    } else {
        loc
    };
    ExitMetadata {
        ip: field(&raw, &["ip", "query"]),
        hostname: field(&raw, &["hostname"]),
        city: field(&raw, &["city"]),
        region: field(&raw, &["region", "region_name"]),
        country: field(&raw, &["country_code", "country"]),
        loc,
        org: field(&raw, &["org", "asn_org"]),
        postal: field(&raw, &["postal", "zip"]),
        timezone: field(&raw, &["timezone"]),
        raw,
    }
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn parses_common_provider_field_variants() {
        let metadata = parse(serde_json::json!({
            "query":"203.0.113.10", "city":"Tehran", "region_name":"Tehran",
            "country_code":"IR", "latitude":35.7, "longitude":51.4,
            "asn_org":"Example ASN", "zip":"12345", "timezone":"Asia/Tehran"
        }));
        assert_eq!(metadata.ip, "203.0.113.10");
        assert_eq!(metadata.region, "Tehran");
        assert_eq!(metadata.loc, "35.7,51.4");
    }
}
