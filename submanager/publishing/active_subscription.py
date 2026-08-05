from __future__ import annotations

import base64
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from submanager.core.models import NodeStatus
from submanager.storage.sqlite_store import SqliteStore


@dataclass(frozen=True)
class PublishResult:
    active_count: int
    published_count: int
    retained_snapshots: int
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
        retained_snapshots: int = 3,
        command_timeout_seconds: int = 45,
    ) -> None:
        self.store = store
        self.remote = remote
        self.branch = branch
        self.author_name = author_name
        self.author_email = author_email
        self.retained_snapshots = max(1, retained_snapshots)
        self.command_timeout_seconds = command_timeout_seconds
        self.snapshot_dir = data_dir / "publish"
        self.repo_dir = data_dir / "publisher-repo"
        self.history_path = self.snapshot_dir / "active-history.json"

    def publish(self) -> PublishResult:
        current_configs = self._current_configs()
        history = self._update_history(current_configs)
        published_configs = self._merge_history(history)
        raw_payload, encoded_payload = self._encode_configs(published_configs)
        active_count = len(current_configs)
        published_count = len(published_configs)
        snapshot_changed = self._write_if_changed(self.snapshot_dir / "active-raw.txt", raw_payload)
        snapshot_changed = self._write_if_changed(self.snapshot_dir / "active.txt", encoded_payload) or snapshot_changed

        if not self.remote:
            return PublishResult(active_count, published_count, len(history), snapshot_changed, False, False)

        self._ensure_clone()
        self._write_if_changed(self.repo_dir / "subscriptions" / "active-raw.txt", raw_payload)
        self._write_if_changed(self.repo_dir / "subscriptions" / "active.txt", encoded_payload)

        changed = self._git_has_subscription_changes()
        if changed:
            self._run_git("add", "--", "subscriptions/active-raw.txt", "subscriptions/active.txt")
            self._run_git(
                "commit",
                "-m",
                f"chore(subscription): publish {published_count} recent proxies ({active_count} active)",
            )

        self._run_git("fetch", "origin", self.branch)
        self._run_git("rebase", f"origin/{self.branch}")
        pending = int(self._run_git("rev-list", "--count", f"origin/{self.branch}..HEAD").strip() or "0")
        if pending <= 0:
            return PublishResult(active_count, published_count, len(history), snapshot_changed, changed, False)

        self._run_git("push", "origin", f"HEAD:{self.branch}")
        commit = self._run_git("rev-parse", "--short", "HEAD").strip()
        return PublishResult(active_count, published_count, len(history), snapshot_changed, changed, True, commit)

    def _current_configs(self) -> list[str]:
        nodes = sorted(
            self.store.list_nodes_by_status(NodeStatus.ACTIVE),
            key=lambda item: (item.config_hash, item.raw_config),
        )
        return list(dict.fromkeys(node.raw_config.strip() for node in nodes if node.raw_config.strip()))

    def _update_history(self, current_configs: list[str]) -> list[list[str]]:
        history = self._load_history()
        changed = not self.history_path.exists()
        if not history or history[-1] != current_configs:
            history.append(current_configs)
            history = history[-self.retained_snapshots :]
            changed = True
        if changed:
            payload = json.dumps(
                {"version": 1, "snapshots": history},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            self._write_if_changed(self.history_path, payload)
        return history

    def _load_history(self) -> list[list[str]]:
        if self.history_path.exists():
            try:
                payload = json.loads(self.history_path.read_text(encoding="utf-8"))
                raw_snapshots = payload.get("snapshots", []) if isinstance(payload, dict) else []
                snapshots = [
                    list(dict.fromkeys(item.strip() for item in snapshot if isinstance(item, str) and item.strip()))
                    for snapshot in raw_snapshots
                    if isinstance(snapshot, list)
                ]
                return snapshots[-self.retained_snapshots :]
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                pass

        # One-time migration from the old publisher. Its Git history contains
        # exact ACTIVE snapshots, so preserve up to the newest configured count.
        snapshots: list[list[str]] = []
        if (self.repo_dir / ".git").is_dir():
            try:
                revisions = self._run_git(
                    "log",
                    f"-{self.retained_snapshots}",
                    "--format=%H",
                    "--",
                    "subscriptions/active-raw.txt",
                ).splitlines()
                for revision in reversed(revisions):
                    content = self._run_git("show", f"{revision}:subscriptions/active-raw.txt")
                    configs = list(dict.fromkeys(line.strip() for line in content.splitlines() if line.strip()))
                    if configs and (not snapshots or snapshots[-1] != configs):
                        snapshots.append(configs)
            except RuntimeError:
                snapshots = []

        # The local legacy file may be newer than the last successful push.
        legacy_path = self.snapshot_dir / "active-raw.txt"
        if legacy_path.exists():
            try:
                configs = [line.strip() for line in legacy_path.read_text(encoding="utf-8").splitlines() if line.strip()]
                configs = list(dict.fromkeys(configs))
                if configs and (not snapshots or snapshots[-1] != configs):
                    snapshots.append(configs)
            except (OSError, UnicodeDecodeError):
                pass
        return snapshots[-self.retained_snapshots :]

    @staticmethod
    def _merge_history(history: list[list[str]]) -> list[str]:
        # Prefer the current snapshot, then the immediately previous ones.
        return list(dict.fromkeys(config for snapshot in reversed(history) for config in snapshot))

    @staticmethod
    def _encode_configs(configs: list[str]) -> tuple[bytes, bytes]:
        raw_payload = ("\n".join(configs) + ("\n" if configs else "")).encode("utf-8")
        encoded_payload = base64.b64encode(raw_payload)
        if encoded_payload:
            encoded_payload += b"\n"
        return raw_payload, encoded_payload

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
