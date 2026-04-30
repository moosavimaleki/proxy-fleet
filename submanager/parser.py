from __future__ import annotations

import base64
import json
import re
import urllib.parse
import urllib.request
from typing import Any

from submanager.core.models import ParsedNode
from submanager.utils.hashing import stable_json_hash


SUPPORTED_SCHEMES = ("vmess", "vless", "trojan", "ss", "socks")


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
            "id": data["id"],
            "aid": int(data.get("aid", 0) or 0),
            "network": data.get("net", "tcp"),
            "tls": data.get("tls", ""),
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
            "id": urllib.parse.unquote(parsed.username),
            "network": query.get("type", ["tcp"])[0],
            "security": query.get("security", ["none"])[0],
            "sni": query.get("sni", [""])[0],
            "path": query.get("path", [""])[0],
            "host": query.get("host", [""])[0],
            "flow": query.get("flow", [""])[0],
            "encryption": query.get("encryption", ["none"])[0],
        }
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
            "network": query.get("type", ["tcp"])[0],
            "security": query.get("security", ["tls"])[0],
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
        stream: dict[str, Any] = {"network": data.get("net", "tcp")}
        host = data.get("host", "")
        path = data.get("path", "")
        tls_mode = data.get("tls", "")
        network = stream["network"]
        if tls_mode in ("tls", "reality"):
            stream["security"] = tls_mode
            stream["tlsSettings"] = {
                "serverName": data.get("sni") or host or data.get("add", ""),
                "allowInsecure": True,
            }
        if network == "ws":
            stream["wsSettings"] = {"path": path or "/", "headers": {"Host": host} if host else {}}
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
        stream: dict[str, Any] = {"network": network}
        if security and security != "none":
            stream["security"] = security
            if security == "tls":
                stream["tlsSettings"] = {
                    "serverName": query.get("sni", [host])[0],
                    "allowInsecure": True,
                    "fingerprint": query.get("fp", [""])[0],
                    "alpn": self._split_csv(query.get("alpn", [""])[0]),
                }
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
            stream["wsSettings"] = {"path": path, "headers": {"Host": header_host} if header_host else {}}
        elif network == "grpc":
            stream["grpcSettings"] = {"serviceName": query.get("serviceName", [""])[0]}
        elif network == "httpupgrade":
            stream["httpupgradeSettings"] = {"host": header_host, "path": path}
        elif network == "splithttp":
            stream["splithttpSettings"] = {"host": header_host, "path": path}
        elif network == "kcp":
            stream["kcpSettings"] = {"header": {"type": query.get("headerType", ["none"])[0]}}
        return stream

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
