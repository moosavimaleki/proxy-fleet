from __future__ import annotations

import socket
import ssl
import time
import urllib.parse
from dataclasses import dataclass


@dataclass
class OpenedHttpStream:
    sock: socket.socket
    remainder: bytes


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

    def measure_latency(self, proxy_port: int, test_url: str, timeout: float, attempts: int = 2) -> int:
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
                if not any(code in status_text for code in (" 200 ", " 204 ", " 301 ", " 302 ")):
                    raise OSError(f"unexpected HTTP status: {status_text}")
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
            raise OSError(f"unexpected HTTP status: {status_line}")
        return OpenedHttpStream(sock=sock, remainder=remainder)


class DownloadSpeedProbe:
    def __init__(self) -> None:
        self.http_probe = HttpProxyProbe()

    def measure_speed_kbps(self, proxy_port: int, speed_url: str, timeout: float) -> int:
        start = time.perf_counter()
        max_speed = 0.0
        last_tick = start
        bytes_since_tick = 0
        total_bytes = 0
        has_value = False

        opened = self.http_probe.open_stream(proxy_port, speed_url, timeout)
        with opened.sock as sock:
            if opened.remainder:
                total_bytes += len(opened.remainder)
                bytes_since_tick += len(opened.remainder)
                has_value = True

            while True:
                if time.perf_counter() - start >= timeout:
                    break
                chunk = sock.recv(65536)
                if not chunk:
                    break
                has_value = True
                total_bytes += len(chunk)
                bytes_since_tick += len(chunk)
                now = time.perf_counter()
                elapsed = now - last_tick
                if elapsed >= 1.0:
                    speed = bytes_since_tick / elapsed
                    if speed > max_speed:
                        max_speed = speed
                    last_tick = now
                    bytes_since_tick = 0

        total_elapsed = max(time.perf_counter() - start, 0.001)
        if bytes_since_tick > 0:
            tail_speed = bytes_since_tick / max(time.perf_counter() - last_tick, 0.001)
            if tail_speed > max_speed:
                max_speed = tail_speed
        if not has_value:
            return 0
        if max_speed <= 0:
            max_speed = total_bytes / total_elapsed
        return int((max_speed * 8) / 1000)
