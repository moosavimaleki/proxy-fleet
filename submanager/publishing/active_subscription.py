from __future__ import annotations

import base64
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from submanager.core.models import NodeStatus
from submanager.storage.sqlite_store import SqliteStore


@dataclass(frozen=True)
class PublishResult:
    active_count: int
    snapshot_changed: bool
    committed: bool
    pushed: bool
    commit: str = ""


class ActiveSubscriptionPublisher:
    """Export ACTIVE nodes and push content changes to a Git repository."""

    def __init__(
        self,
        store: SqliteStore,
        *,
        data_dir: Path,
        remote: str,
        branch: str,
        author_name: str,
        author_email: str,
        command_timeout_seconds: int = 45,
    ) -> None:
        self.store = store
        self.remote = remote
        self.branch = branch
        self.author_name = author_name
        self.author_email = author_email
        self.command_timeout_seconds = command_timeout_seconds
        self.snapshot_dir = data_dir / "publish"
        self.repo_dir = data_dir / "publisher-repo"

    def publish(self) -> PublishResult:
        raw_payload, encoded_payload, active_count = self._build_payloads()
        snapshot_changed = self._write_if_changed(self.snapshot_dir / "active-raw.txt", raw_payload)
        snapshot_changed = self._write_if_changed(self.snapshot_dir / "active.txt", encoded_payload) or snapshot_changed

        if not self.remote:
            return PublishResult(active_count, snapshot_changed, False, False)

        self._ensure_clone()
        self._write_if_changed(self.repo_dir / "subscriptions" / "active-raw.txt", raw_payload)
        self._write_if_changed(self.repo_dir / "subscriptions" / "active.txt", encoded_payload)

        changed = self._git_has_subscription_changes()
        if changed:
            self._run_git("add", "--", "subscriptions/active-raw.txt", "subscriptions/active.txt")
            self._run_git(
                "commit",
                "-m",
                f"chore(subscription): publish {active_count} active proxies",
            )

        self._run_git("fetch", "origin", self.branch)
        self._run_git("rebase", f"origin/{self.branch}")
        pending = int(self._run_git("rev-list", "--count", f"origin/{self.branch}..HEAD").strip() or "0")
        if pending <= 0:
            return PublishResult(active_count, snapshot_changed, changed, False)

        self._run_git("push", "origin", f"HEAD:{self.branch}")
        commit = self._run_git("rev-parse", "--short", "HEAD").strip()
        return PublishResult(active_count, snapshot_changed, changed, True, commit)

    def _build_payloads(self) -> tuple[bytes, bytes, int]:
        nodes = sorted(
            self.store.list_nodes_by_status(NodeStatus.ACTIVE),
            key=lambda item: (item.config_hash, item.raw_config),
        )
        configs = list(dict.fromkeys(node.raw_config.strip() for node in nodes if node.raw_config.strip()))
        raw_payload = ("\n".join(configs) + ("\n" if configs else "")).encode("utf-8")
        encoded_payload = base64.b64encode(raw_payload)
        if encoded_payload:
            encoded_payload += b"\n"
        return raw_payload, encoded_payload, len(configs)

    def _ensure_clone(self) -> None:
        self.repo_dir.parent.mkdir(parents=True, exist_ok=True)
        if not (self.repo_dir / ".git").is_dir():
            if self.repo_dir.exists() and any(self.repo_dir.iterdir()):
                raise RuntimeError(f"publisher directory is not an empty Git repository: {self.repo_dir}")
            self._run(
                "git",
                "clone",
                "--branch",
                self.branch,
                "--single-branch",
                self.remote,
                str(self.repo_dir),
                cwd=self.repo_dir.parent,
            )
        self._run_git("remote", "set-url", "origin", self.remote)
        self._run_git("config", "user.name", self.author_name)
        self._run_git("config", "user.email", self.author_email)

    def _git_has_subscription_changes(self) -> bool:
        result = self._run(
            "git",
            "status",
            "--porcelain",
            "--",
            "subscriptions/active-raw.txt",
            "subscriptions/active.txt",
            cwd=self.repo_dir,
        )
        return bool(result.strip())

    def _run_git(self, *args: str) -> str:
        return self._run("git", *args, cwd=self.repo_dir)

    def _run(self, *args: str, cwd: Path) -> str:
        env = os.environ.copy()
        env.setdefault("GIT_TERMINAL_PROMPT", "0")
        env.setdefault("GIT_SSH_COMMAND", "ssh -o BatchMode=yes -o ConnectTimeout=10")
        try:
            result = subprocess.run(
                list(args),
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                timeout=self.command_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"publisher command timed out: {args[0]} {args[1] if len(args) > 1 else ''}") from exc
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "unknown error").strip().replace("\n", " ")[:500]
            raise RuntimeError(f"publisher command failed ({result.returncode}): {message}")
        return result.stdout

    @staticmethod
    def _write_if_changed(path: Path, payload: bytes) -> bool:
        if path.exists() and path.read_bytes() == payload:
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_bytes(payload)
        os.replace(temporary, path)
        return True
