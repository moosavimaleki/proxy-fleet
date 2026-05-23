from __future__ import annotations

import socket
import ssl
import time
import json
import urllib.parse
from dataclasses import dataclass


@dataclass
class OpenedHttpStream:
    sock: socket.socket
    remainder: bytes


class UnexpectedHttpStatusError(OSError):
    def __init__(self, status_line: str) -> None:
        super().__init__(f"unexpected HTTP status: {status_line}")
        self.status_line = status_line
        self.status_code = self._parse_status_code(status_line)

    def _parse_status_code(self, status_line: str) -> int | None:
        parts = status_line.split()
        if len(parts) < 2:
            return None
        try:
            return int(parts[1])
        except ValueError:
            return None


class Socks5Client:
    def recv_exact(self, sock: socket.socket, count: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < count:
            chunk = sock.recv(count - len(chunks))
            if not chunk:
                raise OSError("connection closed unexpectedly")
            chunks.extend(chunk)
        return bytes(chunks)

    def connect(self, proxy_host: str, proxy_port: int, target_host: str, target_port: int, timeout: float) -> socket.socket:
        sock = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
        sock.settimeout(timeout)
        sock.sendall(b"\x05\x01\x00")
        response = self.recv_exact(sock, 2)
        if response != b"\x05\x00":
            raise OSError("SOCKS5 auth negotiation failed")

        host_bytes = target_host.encode("idna")
        request = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes + target_port.to_bytes(2, "big")
        sock.sendall(request)
        reply = self.recv_exact(sock, 4)
        if reply[1] != 0x00:
            raise OSError(f"SOCKS5 connect failed with code {reply[1]}")

        atyp = reply[3]
        if atyp == 0x01:
            self.recv_exact(sock, 4 + 2)
        elif atyp == 0x03:
            size = self.recv_exact(sock, 1)[0]
            self.recv_exact(sock, size + 2)
        elif atyp == 0x04:
            self.recv_exact(sock, 16 + 2)
        else:
            raise OSError("unknown SOCKS5 address type")
        return sock


class HttpProxyProbe:
    def __init__(self) -> None:
        self.socks5 = Socks5Client()

    def measure_latency(
        self,
        proxy_port: int,
        test_url: str,
        timeout: float,
        attempts: int = 2,
        accept_any_status: bool = False,
    ) -> int:
        parsed = urllib.parse.urlsplit(test_url)
        host = parsed.hostname
        if parsed.scheme not in ("http", "https") or not host:
            raise ValueError("invalid probe url")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        one_shot: list[int] = []
        for _ in range(max(1, attempts)):
            start = time.perf_counter()
            with self.socks5.connect("127.0.0.1", proxy_port, host, port, timeout) as base_sock:
                if parsed.scheme == "https":
                    sock = ssl.create_default_context().wrap_socket(base_sock, server_hostname=host)
                else:
                    sock = base_sock
                request = (
                    f"GET {path} HTTP/1.1\r\n"
                    f"Host: {host}\r\n"
                    "User-Agent: submanager/0.1\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("utf-8")
                sock.sendall(request)
                status_line = b""
                while b"\r\n" not in status_line:
                    chunk = sock.recv(1)
                    if not chunk:
                        raise OSError("unexpected EOF before HTTP status")
                    status_line += chunk
                status_text = status_line.decode("iso-8859-1", errors="replace").strip()
                status_code = UnexpectedHttpStatusError(status_text).status_code
                if accept_any_status and status_code is not None:
                    pass
                elif status_code not in {200, 204, 301, 302}:
                    raise UnexpectedHttpStatusError(status_text)
            one_shot.append(int((time.perf_counter() - start) * 1000))
            time.sleep(0.1)
        return min(one_shot)

    def open_stream(self, proxy_port: int, url: str, timeout: float) -> OpenedHttpStream:
        parsed = urllib.parse.urlsplit(url)
        host = parsed.hostname
        if parsed.scheme not in ("http", "https") or not host:
            raise ValueError("invalid download url")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        base_sock = self.socks5.connect("127.0.0.1", proxy_port, host, port, timeout)
        if parsed.scheme == "https":
            sock = ssl.create_default_context().wrap_socket(base_sock, server_hostname=host)
        else:
            sock = base_sock

        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "User-Agent: submanager/0.1\r\n"
            "Connection: close\r\n\r\n"
        ).encode("utf-8")
        sock.settimeout(timeout)
        sock.sendall(request)

        header_bytes = bytearray()
        while b"\r\n\r\n" not in header_bytes:
            chunk = sock.recv(4096)
            if not chunk:
                raise OSError("unexpected EOF before HTTP headers")
            header_bytes.extend(chunk)
            if len(header_bytes) > 1024 * 1024:
                raise OSError("HTTP headers too large")
        raw_headers, remainder = bytes(header_bytes).split(b"\r\n\r\n", 1)
        status_line = raw_headers.split(b"\r\n", 1)[0].decode("iso-8859-1", errors="replace").strip()
        if not any(code in status_line for code in (" 200 ", " 204 ", " 206 ", " 301 ", " 302 ")):
            raise UnexpectedHttpStatusError(status_line)
        return OpenedHttpStream(sock=sock, remainder=remainder)

    def fetch_json(self, proxy_port: int, url: str, timeout: float, max_bytes: int = 65536) -> dict[str, object]:
        opened = self.open_stream(proxy_port, url, timeout)
        body = bytearray()
        with opened.sock as sock:
            if opened.remainder:
                body.extend(opened.remainder[:max_bytes])
            while len(body) < max_bytes:
                chunk = sock.recv(min(4096, max_bytes - len(body)))
                if not chunk:
                    break
                body.extend(chunk)
        payload = json.loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("expected JSON object from metadata endpoint")
        return payload


class DownloadSpeedProbe:
    def __init__(self) -> None:
        self.http_probe = HttpProxyProbe()

    def measure_speed_kbps(
        self,
        proxy_port: int,
        speed_url: str,
        timeout: float,
        stop_after_bytes: int = 256 * 1024,
        min_sample_seconds: float = 0.15,
    ) -> int:
        start = time.perf_counter()
        total_bytes = 0

        opened = self.http_probe.open_stream(proxy_port, speed_url, timeout)
        with opened.sock as sock:
            if opened.remainder:
                total_bytes += len(opened.remainder)

            while True:
                elapsed = time.perf_counter() - start
                if elapsed >= timeout:
                    break
                if total_bytes >= stop_after_bytes and elapsed >= min_sample_seconds:
                    break
                chunk = sock.recv(65536)
                if not chunk:
                    break
                total_bytes += len(chunk)

        if total_bytes <= 0:
            return 0
        total_elapsed = max(time.perf_counter() - start, 0.001)
        return int(((total_bytes / total_elapsed) * 8) / 1000)
