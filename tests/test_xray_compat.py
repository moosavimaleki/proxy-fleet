from __future__ import annotations

import unittest

from submanager.core.models import ParsedNode
from submanager.parser import SubscriptionParser
from submanager.testing.xray import XrayConfigBuilder


class XrayCompatibilityTests(unittest.TestCase):
    def test_share_url_uses_current_tls_fields_and_websocket_host(self) -> None:
        parsed = SubscriptionParser().parse_share_url(
            "vless://11111111-1111-1111-1111-111111111111@example.com:443"
            "?security=tls&type=ws&sni=front.example&host=cdn.example"
            "&allowInsecure=1&pcs=AA%3ABB&vcn=cert.example#test",
            "test://source",
        )

        stream = parsed.outbound["streamSettings"]
        self.assertEqual("cdn.example", stream["wsSettings"]["host"])
        self.assertNotIn("headers", stream["wsSettings"])
        self.assertNotIn("allowInsecure", stream["tlsSettings"])
        self.assertEqual("AA:BB", stream["tlsSettings"]["pinnedPeerCertSha256"])
        self.assertEqual("cert.example", stream["tlsSettings"]["verifyPeerCertByName"])

    def test_builder_sanitizes_legacy_outbound_without_mutating_source(self) -> None:
        source_outbound = {
            "tag": "proxy",
            "protocol": "freedom",
            "streamSettings": {
                "security": "tls",
                "tlsSettings": {"serverName": "example.com", "allowInsecure": True},
                "wsSettings": {"path": "/", "headers": {"Host": "cdn.example"}},
            },
        }
        parsed = ParsedNode(
            source_url="test://source",
            raw_config="test://node",
            share_url="test://node",
            protocol="test",
            address="example.com",
            port=443,
            remark="",
            outbound=source_outbound,
            normalized_config={},
            config_hash="hash",
        )

        config = XrayConfigBuilder().build_single(parsed, 12345)
        built_stream = config["outbounds"][0]["streamSettings"]

        self.assertNotIn("allowInsecure", built_stream["tlsSettings"])
        self.assertEqual("cdn.example", built_stream["wsSettings"]["host"])
        self.assertNotIn("headers", built_stream["wsSettings"])
        self.assertTrue(source_outbound["streamSettings"]["tlsSettings"]["allowInsecure"])

    def test_malformed_transport_is_rejected_before_it_poison_batches(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported transport network"):
            SubscriptionParser().parse_share_url(
                "vless://11111111-1111-1111-1111-111111111111@example.com:443"
                "?security=tls&type=ws%40channel",
                "test://source",
            )

    def test_malformed_uuid_is_rejected_before_xray_startup(self) -> None:
        with self.assertRaisesRegex(ValueError, "custom string shorter than 30 bytes"):
            SubscriptionParser().parse_share_url(
                "vless://37a0bd7c-8b9f-4693-8916-bd1e2dba0a817@example.com:443"
                "?security=tls&type=ws",
                "test://source",
            )

    def test_short_custom_user_id_is_supported_by_xray(self) -> None:
        parsed = SubscriptionParser().parse_share_url(
            "vless://short-custom-id@example.com:443?security=tls&type=ws",
            "test://source",
        )

        self.assertEqual("short-custom-id", parsed.normalized_config["id"])

    def test_reality_websocket_is_rejected_before_xray_startup(self) -> None:
        with self.assertRaisesRegex(ValueError, "REALITY is incompatible"):
            SubscriptionParser().parse_share_url(
                "vless://11111111-1111-1111-1111-111111111111@example.com:443"
                "?security=reality&type=ws&pbk=public-key",
                "test://source",
            )

    def test_unsupported_shadowsocks_cipher_is_rejected_before_xray_startup(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported shadowsocks method"):
            SubscriptionParser().parse_share_url(
                "ss://YWVzLTI1Ni1jZmI6cGFzc3dvcmQ@example.com:443",
                "test://source",
            )

    def test_current_shadowsocks_cipher_with_plain_userinfo_is_accepted(self) -> None:
        parsed = SubscriptionParser().parse_share_url(
            "ss://chacha20-ietf-poly1305:password@example.com:443",
            "test://source",
        )

        self.assertEqual("chacha20-ietf-poly1305", parsed.normalized_config["method"])
        self.assertEqual("password", parsed.normalized_config["password"])


if __name__ == "__main__":
    unittest.main()
