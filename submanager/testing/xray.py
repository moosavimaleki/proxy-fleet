from __future__ import annotations

import copy
import json
import os
import platform
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from submanager.constants import XRAY_LATEST_API
from submanager.core.models import BatchNode, ParsedNode
from submanager.utils.logging import get_logger


logger = get_logger(__name__)
TEMP_STARTUP_WAIT_SECONDS = 0.35
PERSISTENT_STARTUP_WAIT_SECONDS = 0.2


class XrayBinaryResolver:
    def ensure(self, xray_bin_arg: str, cache_dir: Path) -> Path:
        if xray_bin_arg:
            path = Path(xray_bin_arg).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"xray binary not found: {path}")
            return path

        existing = shutil.which("xray")
        if existing:
            return Path(existing)

        cache_dir.mkdir(parents=True, exist_ok=True)
        xray_path = cache_dir / "xray"
        if xray_path.exists():
            return xray_path

        if platform.system() != "Linux":
            raise RuntimeError("auto-download currently supports Linux only")

        asset_name = self._detect_linux_asset()
        logger.info("Fetching Xray release metadata from %s", XRAY_LATEST_API)
        release = json.loads(self._fetch_bytes(XRAY_LATEST_API, timeout=20).decode("utf-8"))
        tag_name = release.get("tag_name", "")
        asset_url = ""
        for asset in release.get("assets", []):
            if asset.get("name") == asset_name:
                asset_url = asset.get("browser_download_url", "")
                break
        if not asset_url:
            raise RuntimeError(f"Could not find {asset_name} in release {tag_name}")

        archive_path = cache_dir / asset_name
        logger.info("Downloading Xray %s -> %s", tag_name, archive_path)
        archive_path.write_bytes(self._fetch_bytes(asset_url, timeout=60))
        with zipfile.ZipFile(archive_path) as zf:
            member = next((name for name in zf.namelist() if name.endswith("/xray") or name == "xray"), None)
            if not member:
                raise RuntimeError("Downloaded Xray archive does not contain xray binary")
            with zf.open(member) as src, open(xray_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
        os.chmod(xray_path, 0o755)
        return xray_path

    def _fetch_bytes(self, url: str, timeout: float) -> bytes:
        req = urllib.request.Request(url, headers={"User-Agent": "submanager/0.1", "Accept": "*/*"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read()

    def _detect_linux_asset(self) -> str:
        machine = platform.machine().lower()
        if machine in ("x86_64", "amd64"):
            return "Xray-linux-64.zip"
        if machine in ("aarch64", "arm64"):
            return "Xray-linux-arm64-v8a.zip"
        raise RuntimeError(f"unsupported machine for auto-download: {machine}")


class XrayConfigBuilder:
    def reserve_ports(self, count: int) -> tuple[list[int], list[socket.socket]]:
        sockets: list[socket.socket] = []
        ports: list[int] = []
        try:
            for _ in range(count):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(("127.0.0.1", 0))
                sock.listen()
                sockets.append(sock)
                ports.append(int(sock.getsockname()[1]))
            return ports, sockets
        except Exception:
            for sock in sockets:
                sock.close()
            raise

    def build_single(self, parsed_node: ParsedNode, local_port: int, listen: str = "127.0.0.1") -> dict:
        return {
            "log": {"loglevel": "warning"},
            "inbounds": [
                {
                    "tag": "socks-in",
                    "listen": listen,
                    "port": local_port,
                    "protocol": "socks",
                    "settings": {"auth": "noauth", "udp": True},
                }
            ],
            "outbounds": [
                parsed_node.outbound,
                {"tag": "direct", "protocol": "freedom"},
                {"tag": "block", "protocol": "blackhole"},
            ],
            "routing": {
                "domainStrategy": "AsIs",
                "rules": [
                    {
                        "type": "field",
                        "inboundTag": ["socks-in"],
                        "outboundTag": "proxy",
                    }
                ],
            },
        }

    def build_batch(self, batch_nodes: list[BatchNode]) -> dict:
        config = {
            "log": {"loglevel": "warning"},
            "inbounds": [],
            "outbounds": [
                {"tag": "direct", "protocol": "freedom"},
                {"tag": "block", "protocol": "blackhole"},
            ],
            "routing": {"domainStrategy": "AsIs", "rules": []},
        }
        for item in batch_nodes:
            config["inbounds"].append(
                {
                    "tag": item.inbound_tag,
                    "listen": "127.0.0.1",
                    "port": item.local_port,
                    "protocol": "socks",
                    "settings": {"auth": "noauth", "udp": True},
                }
            )
            outbound = copy.deepcopy(item.node.outbound)
            outbound["tag"] = item.outbound_tag
            config["outbounds"].append(outbound)
            config["routing"]["rules"].append(
                {
                    "type": "field",
                    "inboundTag": [item.inbound_tag],
                    "outboundTag": item.outbound_tag,
                }
            )
        return config


class XrayRunner:
    def __init__(self, xray_bin: Path) -> None:
        self.xray_bin = xray_bin

    @contextmanager
    def run_temp(self, config: dict) -> Iterator[subprocess.Popen[bytes]]:
        with tempfile.TemporaryDirectory(prefix="submanager-") as tmp:
            config_path = Path(tmp) / "config.json"
            config_path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
            process = subprocess.Popen(
                [str(self.xray_bin), "run", "-config", str(config_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                self._wait_for_startup(process, TEMP_STARTUP_WAIT_SECONDS)
                yield process
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    def start_persistent(self, config: dict) -> subprocess.Popen[bytes]:
        temp_dir = tempfile.TemporaryDirectory(prefix="submanager-runtime-")
        config_path = Path(temp_dir.name) / "config.json"
        config_path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
        process = subprocess.Popen(
            [str(self.xray_bin), "run", "-config", str(config_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        process._submanager_tmpdir = temp_dir  # type: ignore[attr-defined]
        self._wait_for_startup(process, PERSISTENT_STARTUP_WAIT_SECONDS)
        if process.poll() is not None:
            temp_dir.cleanup()
            raise RuntimeError("xray exited early")
        return process

    def stop_persistent(self, process: subprocess.Popen[bytes]) -> None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        temp_dir = getattr(process, "_submanager_tmpdir", None)
        if temp_dir is not None:
            temp_dir.cleanup()

    def _wait_for_startup(self, process: subprocess.Popen[bytes], seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if process.poll() is not None:
                return
            time.sleep(0.02)
