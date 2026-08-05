from __future__ import annotations

import base64
import subprocess
from dataclasses import dataclass
from pathlib import Path

from submanager.publishing.active_subscription import ActiveSubscriptionPublisher


@dataclass
class FakeNode:
    config_hash: str
    raw_config: str


class FakeStore:
    def __init__(self, nodes: list[FakeNode]) -> None:
        self.nodes = nodes

    def list_nodes_by_status(self, status):  # type: ignore[no-untyped-def]
        return list(self.nodes)


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def test_publisher_commits_only_when_active_content_changes(tmp_path: Path) -> None:
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    remote.mkdir()
    seed.mkdir()
    _git(remote, "init", "--bare")
    _git(seed, "init", "-b", "main")
    _git(seed, "config", "user.name", "Test")
    _git(seed, "config", "user.email", "test@example.invalid")
    (seed / "README.md").write_text("seed\n", encoding="utf-8")
    _git(seed, "add", "README.md")
    _git(seed, "commit", "-m", "seed")
    _git(seed, "remote", "add", "origin", str(remote))
    _git(seed, "push", "-u", "origin", "main")
    _git(remote, "symbolic-ref", "HEAD", "refs/heads/main")

    store = FakeStore([FakeNode("a", "vmess://a")])
    publisher = ActiveSubscriptionPublisher(
        store,  # type: ignore[arg-type]
        data_dir=tmp_path / "data",
        remote=str(remote),
        branch="main",
        author_name="Proxy Fleet Test",
        author_email="proxy-fleet@example.invalid",
    )

    first = publisher.publish()
    first_head = _git(remote, "rev-parse", "main")
    assert first.pushed is True
    raw = _git(remote, "show", "main:subscriptions/active-raw.txt") + "\n"
    encoded = _git(remote, "show", "main:subscriptions/active.txt")
    assert raw == "vmess://a\n"
    assert base64.b64decode(encoded).decode() == raw

    second = publisher.publish()
    assert second.pushed is False
    assert _git(remote, "rev-parse", "main") == first_head

    store.nodes = [FakeNode("b", "vless://b")]
    third = publisher.publish()
    assert third.pushed is True
    assert _git(remote, "rev-parse", "main") != first_head
    assert _git(remote, "show", "main:subscriptions/active-raw.txt") == "vless://b\nvmess://a"

    store.nodes = [FakeNode("c", "trojan://c"), FakeNode("b-copy", "vless://b")]
    fourth = publisher.publish()
    assert fourth.published_count == 3
    assert fourth.retained_snapshots == 3
    assert _git(remote, "show", "main:subscriptions/active-raw.txt") == "vless://b\ntrojan://c\nvmess://a"

    store.nodes = [FakeNode("d", "ss://d")]
    fifth = publisher.publish()
    assert fifth.published_count == 3
    assert _git(remote, "show", "main:subscriptions/active-raw.txt") == "ss://d\nvless://b\ntrojan://c"

    reloaded = ActiveSubscriptionPublisher(
        store,  # type: ignore[arg-type]
        data_dir=tmp_path / "data",
        remote=str(remote),
        branch="main",
        author_name="Proxy Fleet Test",
        author_email="proxy-fleet@example.invalid",
    )
    after_restart = reloaded.publish()
    assert after_restart.pushed is False
    assert after_restart.published_count == 3
