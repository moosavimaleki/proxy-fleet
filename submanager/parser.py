from __future__ import annotations

import base64
import json
import re
import urllib.parse
import urllib.request
import uuid
from typing import Any

from submanager.core.models import ParsedNode
from submanager.utils.hashing import stable_json_hash


SUPPORTED_SCHEMES = ("vmess", "vless", "trojan", "ss", "socks")
SUPPORTED_NETWORKS = {
    "tcp",
    "raw",
    "ws",
    "websocket",
    "grpc",
    "httpupgrade",
    "splithttp",
    "xhttp",
    "kcp",
    "mkcp",
    "quic",
}
SUPPORTED_SECURITY = {"", "none", "tls", "reality"}
SUPPORTED_SHADOWSOCKS_METHODS = {
    "2022-blake3-aes-128-gcm",
    "2022-blake3-aes-256-gcm",
    "2022-blake3-chacha20-poly1305",
    "aes-128-gcm",
    "aes-256-gcm",
    "chacha20-poly1305",
    "chacha20-ietf-poly1305",
    "xchacha20-poly1305",
    "xchacha20-ietf-poly1305",
}
REALITY_NETWORKS = {"tcp", "raw", "xhttp", "splithttp", "grpc"}


class SubscriptionParser:
    def fetch_bytes(self, url: str, timeout: float = 20.0) -> bytes:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "submanager/0.1", "Accept": "*/*"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read()

    def load_nodes(self, source_url: str, timeout: float = 20.0) -> tuple[list[ParsedNode], list[str]]:
        payload = self.fetch_bytes(source_url, timeout)
        return self.parse_subscription_payload(payload, source_url)

    def parse_subscription_payload(self, payload: bytes, source_url: str) -> tuple[list[ParsedNode], list[str]]:
        warnings: list[str] = []
        nodes: list[ParsedNode] = []
        seen_hashes: set[str] = set()
        seen_raw: set[str] = set()
        for share_url in self.subscription_bytes_to_links(payload):
            try:
                parsed = self.parse_share_url(share_url, source_url)
                if parsed.config_hash in seen_hashes or parsed.raw_config in seen_raw:
                    continue
                seen_hashes.add(parsed.config_hash)
                seen_raw.add(parsed.raw_config)
                nodes.append(parsed)
            except Exception as exc:
                warnings.append(f"{source_url}: skipped {share_url[:80]}... ({exc})")
        return nodes, warnings

    def parse_share_url(self, url: str, source_url: str) -> ParsedNode:
        scheme = urllib.parse.urlsplit(url).scheme.lower()
        if scheme == "vmess":
            return self._parse_vmess(url, source_url)
        if scheme == "vless":
            return self._parse_vless(url, source_url)
        if scheme == "trojan":
            return self._parse_trojan(url, source_url)
        if scheme == "ss":
            return self._parse_ss(url, source_url)
        if scheme == "socks":
            return self._parse_socks(url, source_url)
        raise ValueError(f"unsupported scheme: {scheme}")

    def subscription_bytes_to_links(self, payload: bytes) -> list[str]:
        text = payload.decode("utf-8", errors="ignore")
        direct = self._extract_share_links(text)
        if direct:
            return direct

        non_comment_lines = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
        if non_comment_lines:
            decoded = self._try_decode_base64_text("\n".join(non_comment_lines))
            if decoded:
                via_b64 = self._extract_share_links(decoded)
                if via_b64:
                    return via_b64

        all_decoded = self._try_decode_base64_text(text)
        if all_decoded:
            via_full_b64 = self._extract_share_links(all_decoded)
            if via_full_b64:
                return via_full_b64
        return []

    def _extract_share_links(self, text: str) -> list[str]:
        pattern = re.compile(rf"(?im)\b(?:{'|'.join(SUPPORTED_SCHEMES)})://[^\s\"'<>]+")
        matches = pattern.findall(text)
        deduped: list[str] = []
        seen: set[str] = set()
        for item in matches:
            if item not in seen:
                seen.add(item)
                deduped.append(item)
        return deduped

    def _parse_vmess(self, url: str, source_url: str) -> ParsedNode:
        payload = url.split("://", 1)[1]
        decoded = self._try_decode_base64_text(payload)
        if not decoded:
            raise ValueError("invalid vmess payload")
        data = json.loads(decoded)
        normalized = {
            "protocol": "vmess",
            "server": data["add"],
            "port": int(data["port"]),
            "id": self._normalize_user_id(data["id"]),
            "aid": int(data.get("aid", 0) or 0),
            "network": self._normalize_network(data.get("net", "tcp")),
            "tls": self._normalize_security(data.get("tls", "")),
            "sni": data.get("sni", ""),
            "host": data.get("host", ""),
            "path": data.get("path", ""),
            "scy": data.get("scy", "auto"),
        }
        outbound = {
            "protocol": "vmess",
            "settings": {
                "vnext": [
                    {
                        "address": normalized["server"],
                        "port": normalized["port"],
                        "users": [
                            {
                                "id": normalized["id"],
                                "alterId": normalized["aid"],
                                "security": normalized["scy"],
                                "level": 0,
                            }
                        ],
                    }
                ]
            },
            "streamSettings": self._build_stream_settings_from_vmess(data),
            "mux": {"enabled": False},
            "tag": "proxy",
        }
        return ParsedNode(
            source_url=source_url,
            raw_config=url,
            share_url=url,
            protocol="vmess",
            address=normalized["server"],
            port=normalized["port"],
            remark=data.get("ps", ""),
            outbound=outbound,
            normalized_config=normalized,
            config_hash=stable_json_hash(normalized),
        )

    def _parse_vless(self, url: str, source_url: str) -> ParsedNode:
        parsed = urllib.parse.urlsplit(url)
        if not parsed.hostname or not parsed.port or not parsed.username:
            raise ValueError("invalid vless url")
        query = urllib.parse.parse_qs(parsed.query)
        normalized = {
            "protocol": "vless",
            "server": parsed.hostname,
            "port": parsed.port,
            "id": self._normalize_user_id(urllib.parse.unquote(parsed.username)),
            "network": self._normalize_network(query.get("type", ["tcp"])[0]),
            "security": self._normalize_security(query.get("security", ["none"])[0]),
            "sni": query.get("sni", [""])[0],
            "path": query.get("path", [""])[0],
            "host": query.get("host", [""])[0],
            "flow": query.get("flow", [""])[0],
            "encryption": self._normalize_vless_encryption(query.get("encryption", ["none"])[0]),
        }
        self._validate_transport_security(normalized["network"], normalized["security"], query)
        outbound = {
            "protocol": "vless",
            "settings": {
                "vnext": [
                    {
                        "address": normalized["server"],
                        "port": normalized["port"],
                        "users": [
                            {
                                "id": normalized["id"],
                                "encryption": normalized["encryption"],
                                "flow": normalized["flow"],
                                "level": 0,
                            }
                        ],
                    }
                ]
            },
            "streamSettings": self._build_stream_settings_from_query(
                network=normalized["network"],
                security=normalized["security"],
                query=query,
                host=parsed.hostname,
            ),
            "mux": {"enabled": False},
            "tag": "proxy",
        }
        return ParsedNode(
            source_url=source_url,
            raw_config=url,
            share_url=url,
            protocol="vless",
            address=normalized["server"],
            port=normalized["port"],
            remark=self._remark_from_fragment(parsed.fragment),
            outbound=outbound,
            normalized_config=normalized,
            config_hash=stable_json_hash(normalized),
        )

    def _parse_trojan(self, url: str, source_url: str) -> ParsedNode:
        parsed = urllib.parse.urlsplit(url)
        password = parsed.username or parsed.password
        if not parsed.hostname or not parsed.port or password is None:
            raise ValueError("invalid trojan url")
        query = urllib.parse.parse_qs(parsed.query)
        normalized = {
            "protocol": "trojan",
            "server": parsed.hostname,
            "port": parsed.port,
            "password": urllib.parse.unquote(password),
            "network": self._normalize_network(query.get("type", ["tcp"])[0]),
            "security": self._normalize_security(query.get("security", ["tls"])[0]),
            "sni": query.get("sni", [""])[0],
            "path": query.get("path", [""])[0],
            "host": query.get("host", [""])[0],
        }
        outbound = {
            "protocol": "trojan",
            "settings": {
                "servers": [
                    {
                        "address": normalized["server"],
                        "port": normalized["port"],
                        "password": normalized["password"],
                        "level": 0,
                    }
                ]
            },
            "streamSettings": self._build_stream_settings_from_query(
                network=normalized["network"],
                security=normalized["security"],
                query=query,
                host=parsed.hostname,
            ),
            "mux": {"enabled": False},
            "tag": "proxy",
        }
        return ParsedNode(
            source_url=source_url,
            raw_config=url,
            share_url=url,
            protocol="trojan",
            address=normalized["server"],
            port=normalized["port"],
            remark=self._remark_from_fragment(parsed.fragment),
            outbound=outbound,
            normalized_config=normalized,
            config_hash=stable_json_hash(normalized),
        )

    def _parse_ss(self, url: str, source_url: str) -> ParsedNode:
        parsed = urllib.parse.urlsplit(url)
        if parsed.hostname and parsed.port and parsed.username:
            server_host = parsed.hostname
            server_port = parsed.port
            if parsed.password is not None:
                method = urllib.parse.unquote(parsed.username)
                password = urllib.parse.unquote(parsed.password)
            else:
                userinfo = self._try_decode_base64_text(urllib.parse.unquote(parsed.username))
                if not userinfo or ":" not in userinfo:
                    raise ValueError("invalid ss user info")
                method, password = userinfo.split(":", 1)
        else:
            payload = url.split("://", 1)[1]
            before_hash = payload.split("#", 1)[0]
            before_query = before_hash.split("?", 1)[0]
            decoded = self._try_decode_base64_text(before_query)
            if not decoded or "@" not in decoded:
                raise ValueError("invalid ss payload")
            userinfo, server = decoded.rsplit("@", 1)
            method, password = userinfo.split(":", 1)
            server_host, server_port = server.rsplit(":", 1)
        method = method.strip().lower()
        if method not in SUPPORTED_SHADOWSOCKS_METHODS:
            raise ValueError(f"unsupported shadowsocks method: {method or '<empty>'}")
        if not password:
            raise ValueError("empty shadowsocks password")
        normalized = {
            "protocol": "ss",
            "server": server_host,
            "port": int(server_port),
            "method": method,
            "password": password,
        }
        outbound = {
            "protocol": "shadowsocks",
            "settings": {
                "servers": [
                    {
                        "address": normalized["server"],
                        "port": normalized["port"],
                        "method": normalized["method"],
                        "password": normalized["password"],
                        "level": 0,
                    }
                ]
            },
            "tag": "proxy",
        }
        return ParsedNode(
            source_url=source_url,
            raw_config=url,
            share_url=url,
            protocol="ss",
            address=normalized["server"],
            port=normalized["port"],
            remark=self._remark_from_fragment(parsed.fragment),
            outbound=outbound,
            normalized_config=normalized,
            config_hash=stable_json_hash(normalized),
        )

    def _parse_socks(self, url: str, source_url: str) -> ParsedNode:
        parsed = urllib.parse.urlsplit(url)
        if not parsed.hostname or not parsed.port:
            raise ValueError("invalid socks url")
        normalized = {
            "protocol": "socks",
            "server": parsed.hostname,
            "port": parsed.port,
            "user": urllib.parse.unquote(parsed.username or ""),
            "pass": urllib.parse.unquote(parsed.password or ""),
        }
        outbound = {
            "protocol": "socks",
            "settings": {
                "servers": [
                    {
                        "address": normalized["server"],
                        "port": normalized["port"],
                        "users": (
                            [{"user": normalized["user"], "pass": normalized["pass"]}]
                            if normalized["user"] or normalized["pass"]
                            else []
                        ),
                    }
                ]
            },
            "tag": "proxy",
        }
        return ParsedNode(
            source_url=source_url,
            raw_config=url,
            share_url=url,
            protocol="socks",
            address=normalized["server"],
            port=normalized["port"],
            remark=self._remark_from_fragment(parsed.fragment),
            outbound=outbound,
            normalized_config=normalized,
            config_hash=stable_json_hash(normalized),
        )

    def _build_stream_settings_from_vmess(self, data: dict[str, Any]) -> dict[str, Any]:
        stream: dict[str, Any] = {"network": self._normalize_network(data.get("net", "tcp"))}
        host = data.get("host", "")
        path = data.get("path", "")
        tls_mode = self._normalize_security(data.get("tls", ""))
        network = stream["network"]
        if tls_mode in ("tls", "reality"):
            stream["security"] = tls_mode
            tls_settings = {"serverName": data.get("sni") or host or data.get("add", "")}
            pinned_cert = data.get("pcs") or data.get("pinnedPeerCertSha256") or ""
            verify_name = data.get("vcn") or data.get("verifyPeerCertByName") or ""
            if pinned_cert:
                tls_settings["pinnedPeerCertSha256"] = pinned_cert
            if verify_name:
                tls_settings["verifyPeerCertByName"] = verify_name
            stream["tlsSettings"] = tls_settings
        if network == "ws":
            stream["wsSettings"] = {"path": path or "/", **({"host": host} if host else {})}
        elif network == "grpc":
            stream["grpcSettings"] = {"serviceName": path or data.get("serviceName", "")}
        elif network == "httpupgrade":
            stream["httpupgradeSettings"] = {"host": host, "path": path or "/"}
        elif network == "splithttp":
            stream["splithttpSettings"] = {"host": host, "path": path or "/"}
        return stream

    def _build_stream_settings_from_query(
        self,
        *,
        network: str,
        security: str,
        query: dict[str, list[str]],
        host: str,
    ) -> dict[str, Any]:
        network = self._normalize_network(network)
        security = self._normalize_security(security)
        stream: dict[str, Any] = {"network": network}
        if security and security != "none":
            stream["security"] = security
            if security == "tls":
                tls_settings: dict[str, Any] = {
                    "serverName": query.get("sni", [host])[0],
                }
                pinned_cert = (
                    query.get("pcs", [""])[0]
                    or query.get("pinnedPeerCertSha256", [""])[0]
                )
                verify_name = (
                    query.get("vcn", [""])[0]
                    or query.get("verifyPeerCertByName", [""])[0]
                )
                if pinned_cert:
                    tls_settings["pinnedPeerCertSha256"] = pinned_cert
                if verify_name:
                    tls_settings["verifyPeerCertByName"] = verify_name
                fingerprint = query.get("fp", [""])[0]
                alpn = self._split_csv(query.get("alpn", [""])[0])
                if fingerprint:
                    tls_settings["fingerprint"] = fingerprint
                if alpn:
                    tls_settings["alpn"] = alpn
                stream["tlsSettings"] = tls_settings
            elif security == "reality":
                stream["realitySettings"] = {
                    "serverName": query.get("sni", [host])[0],
                    "fingerprint": query.get("fp", ["chrome"])[0],
                    "publicKey": query.get("pbk", [""])[0],
                    "shortId": query.get("sid", [""])[0],
                    "spiderX": query.get("spx", [""])[0],
                }
        path = query.get("path", ["/"])[0] or "/"
        header_host = query.get("host", [host])[0]
        if network == "ws":
            stream["wsSettings"] = {"path": path, **({"host": header_host} if header_host else {})}
        elif network == "grpc":
            stream["grpcSettings"] = {"serviceName": query.get("serviceName", [""])[0]}
        elif network == "httpupgrade":
            stream["httpupgradeSettings"] = {"host": header_host, "path": path}
        elif network == "splithttp":
            stream["splithttpSettings"] = {"host": header_host, "path": path}
        elif network == "kcp":
            stream["kcpSettings"] = {"header": {"type": query.get("headerType", ["none"])[0]}}
        return stream

    def _normalize_network(self, value: object) -> str:
        network = str(value or "tcp").strip().lower()
        if network not in SUPPORTED_NETWORKS:
            raise ValueError(f"unsupported transport network: {network}")
        return network

    def _normalize_security(self, value: object) -> str:
        security = str(value or "").strip().lower()
        if security not in SUPPORTED_SECURITY:
            raise ValueError(f"unsupported transport security: {security}")
        return security

    def _normalize_uuid(self, value: object) -> str:
        raw_value = str(value or "").strip()
        try:
            return str(uuid.UUID(raw_value))
        except (AttributeError, ValueError) as exc:
            raise ValueError("invalid proxy UUID") from exc

    def _normalize_user_id(self, value: object) -> str:
        raw_value = str(value or "").strip()
        if not raw_value:
            raise ValueError("empty proxy user id")
        try:
            return str(uuid.UUID(raw_value))
        except (AttributeError, ValueError):
            pass
        if len(raw_value.encode("utf-8")) >= 30:
            raise ValueError("proxy user id must be a UUID or a custom string shorter than 30 bytes")
        return raw_value

    def _normalize_vless_encryption(self, value: object) -> str:
        encryption = str(value or "").strip()
        if not encryption:
            raise ValueError("VLESS encryption must not be empty")
        if len(encryption) > 1024 or not re.fullmatch(r"[A-Za-z0-9._-]+", encryption):
            raise ValueError("malformed VLESS encryption")
        return encryption

    def _validate_transport_security(
        self,
        network: str,
        security: str,
        query: dict[str, list[str]],
    ) -> None:
        if security != "reality":
            return
        if network not in REALITY_NETWORKS:
            raise ValueError(f"REALITY is incompatible with transport network: {network}")
        public_key = query.get("pbk", [""])[0] or query.get("password", [""])[0]
        if not public_key:
            raise ValueError("REALITY public key/password is required")
        short_id = query.get("sid", [""])[0]
        if len(short_id) > 16 or len(short_id) % 2 or not re.fullmatch(r"[0-9a-fA-F]*", short_id):
            raise ValueError("invalid REALITY short id")

    def _try_decode_base64_text(self, value: str) -> str | None:
        compact = re.sub(r"\s+", "", value)
        if not compact:
            return None
        if not re.fullmatch(r"[A-Za-z0-9+/=_-]+", compact):
            return None
        compact = compact.replace("-", "+").replace("_", "/")
        try:
            decoded = base64.b64decode(compact + "=" * ((4 - len(compact) % 4) % 4), validate=False)
        except Exception:
            return None
        try:
            return decoded.decode("utf-8")
        except UnicodeDecodeError:
            return decoded.decode("utf-8", errors="ignore")

    def _remark_from_fragment(self, fragment: str) -> str:
        return urllib.parse.unquote(fragment) if fragment else ""

    def _split_csv(self, value: str) -> list[str]:
        return [item for item in (part.strip() for part in value.split(",")) if item]

    def _query_bool(self, query: dict[str, list[str]], key: str, default: bool) -> bool:
        value = query.get(key, [""])[0].strip().lower()
        if not value:
            return default
        return value in {"1", "true", "yes", "on"}
