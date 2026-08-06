//! Subscription decoding, strict structural validation, and remark-independent identity.

use std::{collections::BTreeMap, net::IpAddr, str::FromStr};

use anyhow::Context;
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedProxy {
    pub source: String,
    pub raw_config: String,
    pub protocol: String,
    pub address: String,
    pub port: u16,
    pub remark: String,
    pub normalized_config: serde_json::Value,
    pub config_hash: String,
}

#[derive(Debug, Clone, Default)]
pub struct ParseReport {
    pub accepted: Vec<ParsedProxy>,
    pub rejected: Vec<String>,
}

pub fn parse_subscription(payload: &str, source: &str) -> ParseReport {
    let lines = subscription_lines(payload);
    let mut report = ParseReport::default();
    let mut seen = std::collections::BTreeSet::new();
    for line in lines {
        match parse_share_url(&line, source) {
            Ok(proxy) if seen.insert(proxy.config_hash.clone()) => report.accepted.push(proxy),
            Ok(_) => {}
            Err(error) => report.rejected.push(format!("{error}: {}", redact(&line))),
        }
    }
    report
}

pub fn parse_share_url(raw: &str, source: &str) -> anyhow::Result<ParsedProxy> {
    let raw = raw.trim();
    let (protocol, address, port, remark, normalized_config) = if raw.starts_with("vmess://") {
        parse_vmess(raw)?
    } else if raw.starts_with("ss://") {
        parse_shadowsocks(raw)?
    } else {
        parse_url_proxy(raw)?
    };
    anyhow::ensure!(
        matches!(
            protocol.as_str(),
            "vmess" | "vless" | "trojan" | "ss" | "socks" | "socks5"
        ),
        "unsupported protocol {protocol}"
    );
    anyhow::ensure!(!address.is_empty(), "proxy address is empty");
    validate_address(&address)?;
    anyhow::ensure!(port > 0, "proxy port is invalid");
    validate_transport_parameters(&normalized_config)?;
    let canonical = serde_json::to_vec(&normalized_config)?;
    let config_hash = format!("{:x}", Sha256::digest(canonical));
    Ok(ParsedProxy {
        source: source.to_owned(),
        raw_config: raw.to_owned(),
        protocol,
        address,
        port,
        remark,
        normalized_config,
        config_hash,
    })
}

/// Produces one current Xray outbound. This is deliberately generated from the
/// canonical parsed representation rather than shelling out to Python.
pub fn xray_outbound(proxy: &ParsedProxy) -> anyhow::Result<serde_json::Value> {
    match proxy.protocol.as_str() {
        "vless" => {
            let user = value_string(&proxy.normalized_config, "user");
            validate_user_id(&user)?;
            let params = params(&proxy.normalized_config);
            let network = param(&params, "type").unwrap_or("tcp");
            let security = param(&params, "security").unwrap_or("none");
            Ok(
                serde_json::json!({"tag":"proxy","protocol":"vless","settings":{"vnext":[{"address":proxy.address,"port":proxy.port,"users":[{"id":user,"encryption":param(&params,"encryption").unwrap_or("none"),"flow":param(&params,"flow").unwrap_or(""),"level":0}]}]},"streamSettings":stream_settings(network, security, &params, &proxy.address)? ,"mux":{"enabled":false}}),
            )
        }
        "trojan" => {
            let password = value_string(&proxy.normalized_config, "user");
            anyhow::ensure!(!password.is_empty(), "trojan password is empty");
            let params = params(&proxy.normalized_config);
            let network = param(&params, "type").unwrap_or("tcp");
            let security = param(&params, "security").unwrap_or("tls");
            Ok(
                serde_json::json!({"tag":"proxy","protocol":"trojan","settings":{"servers":[{"address":proxy.address,"port":proxy.port,"password":password,"level":0}]},"streamSettings":stream_settings(network, security, &params, &proxy.address)?,"mux":{"enabled":false}}),
            )
        }
        "ss" => Ok(
            serde_json::json!({"tag":"proxy","protocol":"shadowsocks","settings":{"servers":[{"address":proxy.address,"port":proxy.port,"method":value_string(&proxy.normalized_config,"method"),"password":value_string(&proxy.normalized_config,"password"),"level":0}]}}),
        ),
        "socks" | "socks5" => {
            let user = value_string(&proxy.normalized_config, "user");
            let password = value_string(&proxy.normalized_config, "password");
            let users = if user.is_empty() && password.is_empty() {
                vec![]
            } else {
                vec![serde_json::json!({"user":user,"pass":password})]
            };
            Ok(
                serde_json::json!({"tag":"proxy","protocol":"socks","settings":{"servers":[{"address":proxy.address,"port":proxy.port,"users":users}]}}),
            )
        }
        "vmess" => xray_vmess(proxy),
        protocol => anyhow::bail!("unsupported protocol {protocol}"),
    }
}

fn xray_vmess(proxy: &ParsedProxy) -> anyhow::Result<serde_json::Value> {
    let payload = proxy
        .normalized_config
        .get("payload")
        .and_then(serde_json::Value::as_object)
        .context("invalid vmess normalized config")?;
    let id = payload
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    validate_user_id(&id)?;
    let network = payload
        .get("net")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("tcp");
    let security = payload
        .get("tls")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("none");
    let mut values = BTreeMap::new();
    for key in [
        "sni",
        "host",
        "path",
        "serviceName",
        "fp",
        "alpn",
        "pcs",
        "vcn",
        "pbk",
        "sid",
        "spx",
        "headerType",
    ] {
        if let Some(value) = payload.get(key).and_then(serde_json::Value::as_str) {
            values.insert(key.to_ascii_lowercase(), value.to_owned());
        }
    }
    let alter_id = payload
        .get("aid")
        .and_then(number_or_string_u16)
        .unwrap_or(0);
    let cipher = payload
        .get("scy")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("auto");
    Ok(
        serde_json::json!({"tag":"proxy","protocol":"vmess","settings":{"vnext":[{"address":proxy.address,"port":proxy.port,"users":[{"id":id,"alterId":alter_id,"security":cipher,"level":0}]}]},"streamSettings":stream_settings(network, security, &values, &proxy.address)?,"mux":{"enabled":false}}),
    )
}

fn params(value: &serde_json::Value) -> BTreeMap<String, String> {
    value
        .get("params")
        .and_then(serde_json::Value::as_object)
        .map(|items| {
            items
                .iter()
                .filter_map(|(key, value)| {
                    value
                        .as_str()
                        .map(|value| (key.to_ascii_lowercase(), value.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}
fn value_string(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}
fn param<'a>(params: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    params.get(&key.to_ascii_lowercase()).map(String::as_str)
}

fn stream_settings(
    network: &str,
    security: &str,
    params: &BTreeMap<String, String>,
    server: &str,
) -> anyhow::Result<serde_json::Value> {
    let network = match network.to_ascii_lowercase().as_str() {
        "ws" | "websocket" => "ws",
        "tcp" | "raw" => "tcp",
        "grpc" => "grpc",
        "httpupgrade" => "httpupgrade",
        "splithttp" => "splithttp",
        "xhttp" => "xhttp",
        "kcp" | "mkcp" => "kcp",
        "quic" => "quic",
        other => anyhow::bail!("unsupported transport network {other}"),
    };
    let security = security.to_ascii_lowercase();
    anyhow::ensure!(
        matches!(security.as_str(), "" | "none" | "tls" | "reality"),
        "unsupported transport security {security}"
    );
    if security == "reality" {
        anyhow::ensure!(
            matches!(network, "tcp" | "grpc" | "splithttp" | "xhttp"),
            "REALITY is incompatible with transport {network}"
        );
        anyhow::ensure!(
            !param(params, "pbk")
                .or_else(|| param(params, "password"))
                .unwrap_or_default()
                .is_empty(),
            "REALITY public key is required"
        );
    }
    let mut stream = serde_json::Map::new();
    stream.insert("network".to_owned(), serde_json::json!(network));
    if security == "tls" {
        stream.insert("security".to_owned(), serde_json::json!("tls"));
        let mut tls = serde_json::json!({"serverName":param(params,"sni").unwrap_or(server)});
        if let Some(object) = tls.as_object_mut() {
            if let Some(value) = param(params, "pcs")
                .or_else(|| param(params, "pinnedpeercertsha256"))
                .filter(|value| !value.is_empty())
            {
                object.insert("pinnedPeerCertSha256".to_owned(), serde_json::json!(value));
            }
            if let Some(value) = param(params, "vcn")
                .or_else(|| param(params, "verifypeercertbyname"))
                .filter(|value| !value.is_empty())
            {
                object.insert("verifyPeerCertByName".to_owned(), serde_json::json!(value));
            }
            if let Some(value) = param(params, "fp").filter(|value| !value.is_empty()) {
                object.insert("fingerprint".to_owned(), serde_json::json!(value));
            }
            if let Some(value) = param(params, "alpn").filter(|value| !value.is_empty()) {
                object.insert(
                    "alpn".to_owned(),
                    serde_json::json!(
                        value
                            .split(',')
                            .map(str::trim)
                            .filter(|part| !part.is_empty())
                            .collect::<Vec<_>>()
                    ),
                );
            }
        }
        stream.insert("tlsSettings".to_owned(), tls);
    } else if security == "reality" {
        stream.insert("security".to_owned(), serde_json::json!("reality"));
        stream.insert("realitySettings".to_owned(), serde_json::json!({"serverName":param(params,"sni").unwrap_or(server),"fingerprint":param(params,"fp").unwrap_or("chrome"),"publicKey":param(params,"pbk").or_else(|| param(params,"password")).unwrap_or_default(),"shortId":param(params,"sid").unwrap_or_default(),"spiderX":param(params,"spx").unwrap_or_default()}));
    }
    let path = param(params, "path").unwrap_or("/");
    let host = param(params, "host").unwrap_or(server);
    match network {
        "ws" => {
            stream.insert(
                "wsSettings".to_owned(),
                serde_json::json!({"path":path,"host":host}),
            );
        }
        "grpc" => {
            stream.insert(
                "grpcSettings".to_owned(),
                serde_json::json!({"serviceName":param(params,"servicename").unwrap_or("")}),
            );
        }
        "httpupgrade" => {
            stream.insert(
                "httpupgradeSettings".to_owned(),
                serde_json::json!({"host":host,"path":path}),
            );
        }
        "splithttp" => {
            stream.insert(
                "splithttpSettings".to_owned(),
                serde_json::json!({"host":host,"path":path}),
            );
        }
        "xhttp" => {
            stream.insert(
                "xhttpSettings".to_owned(),
                serde_json::json!({"host":host,"path":path}),
            );
        }
        "kcp" => {
            stream.insert(
                "kcpSettings".to_owned(),
                serde_json::json!({"header":{"type":param(params,"headertype").unwrap_or("none")}}),
            );
        }
        _ => {}
    }
    Ok(serde_json::Value::Object(stream))
}

fn validate_user_id(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!value.trim().is_empty(), "proxy user id is empty");
    if uuid::Uuid::parse_str(value).is_err() {
        anyhow::ensure!(
            value.len() < 30,
            "custom proxy user id must be shorter than 30 bytes"
        );
    }
    Ok(())
}

fn validate_address(address: &str) -> anyhow::Result<()> {
    if IpAddr::from_str(address).is_ok() {
        return Ok(());
    }
    anyhow::ensure!(address.len() <= 253, "proxy hostname is too long");
    anyhow::ensure!(
        address.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .chars()
                    .all(|item| item.is_ascii_alphanumeric() || item == '-')
        }),
        "proxy hostname is invalid"
    );
    Ok(())
}

fn validate_transport_parameters(config: &serde_json::Value) -> anyhow::Result<()> {
    let Some(params) = config.get("params").and_then(serde_json::Value::as_object) else {
        // VMess carries legacy fields in its payload. Xray config generation
        // validates those fields before any process is spawned.
        return Ok(());
    };
    for key in ["sni", "host"] {
        if let Some(value) = params.get(key).and_then(serde_json::Value::as_str) {
            for hostname in value.split(',').filter(|value| !value.is_empty()) {
                validate_address(hostname)?;
            }
        }
    }
    if let Some(alpn) = params.get("alpn").and_then(serde_json::Value::as_str) {
        anyhow::ensure!(
            alpn.split(',').all(|token| {
                !token.is_empty()
                    && token.len() <= 255
                    && token
                        .bytes()
                        .all(|byte| byte.is_ascii_graphic() && byte != b',')
            }),
            "ALPN contains an invalid token"
        );
    }
    let security = params
        .get("security")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("none");
    if security.eq_ignore_ascii_case("reality") {
        let key = params
            .get("pbk")
            .or_else(|| params.get("password"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        anyhow::ensure!(
            key.len() >= 32
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
            "REALITY public key is invalid"
        );
        if let Some(short_id) = params.get("sid").and_then(serde_json::Value::as_str) {
            anyhow::ensure!(
                short_id.len() <= 16
                    && short_id.len() % 2 == 0
                    && short_id.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "REALITY short id is invalid"
            );
        }
    }
    Ok(())
}

fn subscription_lines(payload: &str) -> Vec<String> {
    let trimmed = payload.trim();
    let decoded = if trimmed.contains("://") {
        None
    } else {
        decode_b64(trimmed).and_then(|bytes| String::from_utf8(bytes).ok())
    };
    decoded
        .unwrap_or_else(|| trimmed.to_owned())
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect()
}

fn parse_url_proxy(raw: &str) -> anyhow::Result<(String, String, u16, String, serde_json::Value)> {
    let url = Url::parse(raw)?;
    let protocol = url.scheme().to_ascii_lowercase();
    anyhow::ensure!(
        matches!(protocol.as_str(), "vless" | "trojan" | "socks" | "socks5"),
        "unsupported URL proxy scheme"
    );
    let address = url
        .host_str()
        .unwrap_or_default()
        .trim_matches(['[', ']'])
        .to_ascii_lowercase();
    let port = url.port().context("proxy URL has no port")?;
    let remark = url.fragment().unwrap_or_default().to_owned();
    let user = if url.username().is_empty() {
        String::new()
    } else {
        percent_decode(url.username())
    };
    let password = percent_decode(url.password().unwrap_or_default());
    if matches!(protocol.as_str(), "vless" | "trojan") {
        anyhow::ensure!(!user.is_empty(), "proxy URL has no credential");
    }
    if protocol == "vless" {
        validate_user_id(&user)?;
    }
    let params = canonical_query(&url);
    let normalized = serde_json::json!({"protocol":protocol,"address":address,"port":port,"user":user,"password":password,"path":url.path(),"params":params});
    Ok((
        normalized["protocol"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        normalized["address"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        port,
        remark,
        normalized,
    ))
}

fn parse_vmess(raw: &str) -> anyhow::Result<(String, String, u16, String, serde_json::Value)> {
    let encoded = raw
        .strip_prefix("vmess://")
        .context("invalid vmess URL")?
        .split('#')
        .next()
        .unwrap_or_default();
    let decoded = decode_b64(encoded).context("vmess payload is not base64")?;
    let mut payload: BTreeMap<String, serde_json::Value> =
        serde_json::from_slice(&decoded).context("vmess payload is not JSON")?;
    let address = payload
        .get("add")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let port = payload
        .get("port")
        .and_then(number_or_string_u16)
        .context("vmess payload has invalid port")?;
    let id = payload
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    validate_user_id(id)?;
    let remark = payload
        .remove("ps")
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default();
    payload.remove("remarks");
    payload.remove("remark");
    payload.insert("add".to_owned(), serde_json::Value::String(address.clone()));
    payload.insert(
        "port".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(port)),
    );
    Ok((
        "vmess".to_owned(),
        address,
        port,
        remark,
        serde_json::json!({"protocol":"vmess","payload":payload}),
    ))
}

fn parse_shadowsocks(
    raw: &str,
) -> anyhow::Result<(String, String, u16, String, serde_json::Value)> {
    let no_fragment = raw.strip_prefix("ss://").context("invalid ss URL")?;
    let (body, remark) = no_fragment
        .split_once('#')
        .map(|(body, remark)| (body, percent_decode(remark)))
        .unwrap_or((no_fragment, String::new()));
    let body_without_query = body.split('?').next().unwrap_or(body);
    let decoded = if body_without_query.contains('@') {
        body_without_query.to_owned()
    } else {
        String::from_utf8(
            decode_b64(body_without_query).context("shadowsocks payload is not base64")?,
        )?
    };
    let (credential_raw, server) = decoded
        .rsplit_once('@')
        .context("shadowsocks URL has no server separator")?;
    // SIP002 allows the entire method:password credential to be base64url
    // encoded even when host:port is plaintext.
    let credential = if credential_raw.contains(':') {
        percent_decode(credential_raw)
    } else {
        String::from_utf8(
            decode_b64(credential_raw).context("shadowsocks user info is not base64")?,
        )?
    };
    let parsed = Url::parse(&format!("ss://placeholder@{server}"))?;
    let address = parsed
        .host_str()
        .unwrap_or_default()
        .trim_matches(['[', ']'])
        .to_ascii_lowercase();
    let port = parsed.port().context("shadowsocks URL has no port")?;
    let (method, password) = credential
        .split_once(':')
        .context("shadowsocks URL has invalid credential")?;
    anyhow::ensure!(
        matches!(
            method.to_ascii_lowercase().as_str(),
            "aes-128-gcm"
                | "aes-256-gcm"
                | "chacha20-poly1305"
                | "chacha20-ietf-poly1305"
                | "xchacha20-poly1305"
                | "xchacha20-ietf-poly1305"
                | "2022-blake3-aes-128-gcm"
                | "2022-blake3-aes-256-gcm"
                | "2022-blake3-chacha20-poly1305"
        ),
        "unsupported shadowsocks cipher"
    );
    anyhow::ensure!(!password.is_empty(), "shadowsocks password is empty");
    let normalized = serde_json::json!({"protocol":"ss","address":address,"port":port,"method":method.to_ascii_lowercase(),"password":password,"params":canonical_query(&parsed)});
    Ok((
        "ss".to_owned(),
        normalized["address"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        port,
        remark,
        normalized,
    ))
}

fn canonical_query(url: &Url) -> BTreeMap<String, String> {
    url.query_pairs()
        .filter_map(|(key, value)| {
            let key = key.to_ascii_lowercase();
            (!matches!(key.as_str(), "remark" | "remarks" | "ps" | "name")).then(|| {
                let value = value.into_owned();
                let value = match key.as_str() {
                    // These are protocol/hostname tokens, not
                    // case-sensitive credentials.  Canonicalizing them
                    // prevents duplicate technical identities from
                    // differently-capitalized subscription remarks.
                    "sni" | "host" | "security" | "type" | "net" | "fp" | "flow" | "headertype" => {
                        value.to_ascii_lowercase()
                    }
                    _ => value,
                };
                (key, value)
            })
        })
        .collect()
}

fn number_or_string_u16(value: &serde_json::Value) -> Option<u16> {
    value
        .as_u64()
        .and_then(|number| u16::try_from(number).ok())
        .or_else(|| value.as_str()?.parse().ok())
}
fn decode_b64(input: &str) -> Option<Vec<u8>> {
    STANDARD
        .decode(input)
        .or_else(|_| URL_SAFE_NO_PAD.decode(input.trim_end_matches('=')))
        .ok()
}
fn percent_decode(value: &str) -> String {
    url::form_urlencoded::parse(value.as_bytes())
        .map(|(key, _)| key.into_owned())
        .next()
        .unwrap_or_else(|| value.to_owned())
}
fn redact(raw: &str) -> String {
    raw.split_once("://")
        .map(|(scheme, _)| format!("{scheme}://<redacted>"))
        .unwrap_or_else(|| "<invalid-line>".to_owned())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn remark_does_not_change_vless_identity() {
        let first = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=tls#one",
            "source",
        )
        .unwrap();
        let second = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=tls#two",
            "source",
        )
        .unwrap();
        assert_eq!(first.config_hash, second.config_hash);
    }

    #[test]
    fn canonical_normalization_is_stable_across_remarks_and_case() {
        let first = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@EXAMPLE.com:443?security=tls&sni=EXAMPLE.com#one",
            "source",
        )
        .expect("first");
        let second = parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.COM:443?security=tls&sni=example.com#two",
            "source",
        )
        .expect("second");
        assert_eq!(first.normalized_config, second.normalized_config);
        assert_eq!(first.config_hash, second.config_hash);
    }

    #[test]
    fn accepts_base64_subscription() {
        let line = "vless://11111111-1111-1111-1111-111111111111@example.com:443?security=tls#one";
        let encoded = STANDARD.encode(line);
        let report = parse_subscription(&encoded, "source");
        assert_eq!(report.accepted.len(), 1);
    }

    #[test]
    fn accepts_sip002_shadowsocks_user_info() {
        let credential = URL_SAFE_NO_PAD.encode("chacha20-ietf-poly1305:secret");
        let parsed = parse_share_url(
            &format!("ss://{credential}@example.com:443#label"),
            "source",
        )
        .unwrap();
        assert_eq!(parsed.protocol, "ss");
        assert_eq!(parsed.port, 443);
        assert_eq!(parsed.normalized_config["method"], "chacha20-ietf-poly1305");
    }

    #[test]
    fn validates_ip_literals_hostnames_and_required_proxy_fields() {
        for address in ["198.51.100.10", "2001:db8::1", "proxy.example-test.com"] {
            let authority = if address.contains(':') {
                format!("[{address}]")
            } else {
                address.to_owned()
            };
            let raw = format!(
                "vless://123e4567-e89b-12d3-a456-426614174000@{authority}:443?security=tls"
            );
            assert!(parse_share_url(&raw, "fixture").is_ok(), "{address}");
        }
        assert!(
            parse_share_url(
                "vless://123e4567-e89b-12d3-a456-426614174000@bad_host!:443?security=tls",
                "fixture"
            )
            .is_err()
        );
        assert!(
            parse_share_url(
                "vless://this-custom-user-id-is-deliberately-too-long@example.com:443?security=tls",
                "fixture"
            )
            .is_err()
        );
    }

    #[test]
    fn validates_sni_alpn_and_reality_material_before_xray() {
        let base = "vless://123e4567-e89b-12d3-a456-426614174000@example.com:443";
        assert!(
            parse_share_url(
                &format!("{base}?security=tls&sni=cdn.example.com&alpn=h2,http%2F1.1"),
                "fixture"
            )
            .is_ok()
        );
        assert!(parse_share_url(&format!("{base}?security=tls&sni=bad_host!"), "fixture").is_err());
        let public_key = "A".repeat(43);
        assert!(
            parse_share_url(
                &format!("{base}?security=reality&type=tcp&pbk={public_key}&sid=a1b2"),
                "fixture"
            )
            .is_ok()
        );
        assert!(
            parse_share_url(
                &format!("{base}?security=reality&type=tcp&pbk=short&sid=not-hex"),
                "fixture"
            )
            .is_err()
        );
    }

    #[test]
    fn supports_all_documented_vless_transports_and_security_modes() {
        for (network, extra) in [
            ("tcp", ""),
            ("ws", "&host=cdn.example.com&path=%2Fws"),
            ("grpc", "&serviceName=grpc-service"),
            ("httpupgrade", "&host=cdn.example.com&path=%2Fupgrade"),
            ("splithttp", "&host=cdn.example.com&path=%2Fsplit"),
            ("xhttp", "&host=cdn.example.com&path=%2Fxhttp"),
            ("kcp", "&headerType=srtp"),
            ("quic", ""),
        ] {
            let raw = format!(
                "vless://123e4567-e89b-12d3-a456-426614174000@example.com:443?type={network}&security=tls&sni=example.com{extra}#label"
            );
            let proxy = parse_share_url(&raw, "fixture").expect(network);
            let outbound = xray_outbound(&proxy).expect(network);
            assert_eq!(outbound["streamSettings"]["network"], network);
            assert_eq!(outbound["streamSettings"]["security"], "tls");
        }
        let reality = parse_share_url(
            "vless://123e4567-e89b-12d3-a456-426614174000@example.com:443?type=tcp&security=reality&pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&sid=abcd&sni=example.com#label",
            "fixture",
        )
        .expect("reality parse");
        assert_eq!(
            xray_outbound(&reality).unwrap()["streamSettings"]["security"],
            "reality"
        );
    }

    #[test]
    fn supports_vmess_trojan_and_socks_outbounds() {
        let vmess_json = serde_json::json!({
            "v":"2", "ps":"display", "add":"Example.COM", "port":"443",
            "id":"123e4567-e89b-12d3-a456-426614174001", "aid":"0", "scy":"auto",
            "net":"ws", "host":"cdn.example.com", "path":"/ws", "tls":"tls", "sni":"example.com"
        });
        let vmess = parse_share_url(
            &format!("vmess://{}", STANDARD.encode(vmess_json.to_string())),
            "fixture",
        )
        .expect("vmess");
        assert_eq!(xray_outbound(&vmess).unwrap()["protocol"], "vmess");
        let trojan = parse_share_url(
            "trojan://password@example.com:443?security=tls&sni=example.com#label",
            "fixture",
        )
        .expect("trojan");
        assert_eq!(xray_outbound(&trojan).unwrap()["protocol"], "trojan");
        let socks =
            parse_share_url("socks5://user:pass@example.com:1080#label", "fixture").expect("socks");
        assert_eq!(xray_outbound(&socks).unwrap()["protocol"], "socks");
    }

    proptest! {
        #[test]
        fn arbitrary_subscription_input_never_panics(value in ".{0,8192}") {
            let result = std::panic::catch_unwind(|| parse_subscription(&value, "fuzz"));
            prop_assert!(result.is_ok());
        }
    }

    #[test]
    fn oversized_malformed_input_is_rejected_without_panicking() {
        let input = format!("vless://{}", "x".repeat(1_000_000));
        let result = std::panic::catch_unwind(|| parse_subscription(&input, "fixture"));
        assert!(result.is_ok());
        assert!(result.expect("report").accepted.is_empty());
    }
}
