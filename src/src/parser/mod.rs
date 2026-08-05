//! Subscription decoding, strict structural validation, and remark-independent identity.

use std::collections::BTreeMap;

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
    anyhow::ensure!(port > 0, "proxy port is invalid");
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
    let address = url.host_str().unwrap_or_default().to_ascii_lowercase();
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
    let address = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
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
            (!matches!(key.as_str(), "remark" | "remarks" | "ps" | "name"))
                .then(|| (key, value.into_owned()))
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
}
