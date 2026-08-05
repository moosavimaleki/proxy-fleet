from __future__ import annotations

from submanager.parser import SubscriptionParser


def test_vless_identity_ignores_remark() -> None:
    parser = SubscriptionParser()
    base = "vless://11111111-1111-4111-8111-111111111111@example.com:443?security=tls&type=ws&path=%2Fws"

    first = parser.parse_share_url(f"{base}#first-name", "test://source")
    second = parser.parse_share_url(f"{base}#another-name", "test://source")

    assert first.config_hash == second.config_hash
