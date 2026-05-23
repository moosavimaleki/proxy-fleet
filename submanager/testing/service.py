from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
import time
from urllib.parse import urlsplit

from submanager.config.models import AppSettings
from submanager.core.models import BatchNode, ParsedNode, TestResult
from submanager.testing.probes import DownloadSpeedProbe, HttpProxyProbe, UnexpectedHttpStatusError
from submanager.testing.xray import XrayBinaryResolver, XrayConfigBuilder, XrayRunner

IPINFO_URL = "https://ipinfo.io/json"
IP_API_URL = "http://ip-api.com/json/"
IPWHO_URL = "https://ipwho.is/"
IPAPI_CO_URL = "https://ipapi.co/json/"


class TestService:
    def __init__(self, settings: AppSettings, cache_dir: Path) -> None:
        self.settings = settings
        xray_bin = XrayBinaryResolver().ensure(settings.xray_bin, cache_dir)
        self.builder = XrayConfigBuilder()
        self.runner = XrayRunner(xray_bin)
        self.http_probe = HttpProxyProbe()
        self.download_probe = DownloadSpeedProbe()

    def run_fast_test(self, node: ParsedNode, local_port: int, fetch_exit_info: bool = False) -> TestResult:
        return self.run_fast_test_batch([node], ports=[local_port], fetch_exit_info=fetch_exit_info)[0]

    def run_fast_test_existing_port(
        self,
        node: ParsedNode,
        proxy_port: int,
        fetch_exit_info: bool = False,
        include_download: bool = True,
    ) -> TestResult:
        return self.run_fast_test_existing_ports(
            [(node, proxy_port)],
            fetch_exit_info=fetch_exit_info,
            include_download=include_download,
        )[0]

    def run_fast_test_existing_ports(
        self,
        items: list[tuple[ParsedNode, int]],
        concurrency: int = 0,
        fetch_exit_info: bool = False,
        include_download: bool = True,
    ) -> list[TestResult]:
        if not items:
            return []
        max_workers = len(items) if concurrency <= 0 else min(concurrency, len(items))
        results: list[TestResult | None] = [None] * len(items)
        pool = ThreadPoolExecutor(max_workers=max_workers)
        futures = {
            pool.submit(self._probe_existing_port, node, proxy_port, fetch_exit_info, include_download): idx
            for idx, (node, proxy_port) in enumerate(items)
        }
        batches = (len(items) + max_workers - 1) // max_workers
        relay_timeout = max(1.0, self.settings.health.relay_timeout_ms / 1000)
        download_timeout = max(1, self.settings.download_test.timeout_seconds) if self.settings.download_test.enabled and include_download else 0
        exit_timeout = max(2.0, self.settings.health.relay_timeout_ms / 1000) if fetch_exit_info else 0
        timeout = (relay_timeout + download_timeout + exit_timeout + 1) * max(1, batches)
        done, pending = wait(futures, timeout=timeout)
        for future in done:
            results[futures[future]] = future.result()
        for future in pending:
            future.cancel()
            idx = futures[future]
            node, _ = items[idx]
            results[idx] = TestResult(parsed_node=node, ok=False, latency_ms=-1, download_kbps=0, error="existing port fast test timed out")
        pool.shutdown(wait=False, cancel_futures=True)
        return [result for result in results if result is not None]

    def run_fast_test_batch(
        self,
        nodes: list[ParsedNode],
        concurrency: int = 0,
        ports: list[int] | None = None,
        fetch_exit_info: bool = False,
    ) -> list[TestResult]:
        if not nodes:
            return []
        if ports is not None and len(ports) != len(nodes):
            raise ValueError("ports length must match nodes length")

        fetch_exit_info_during_realping = fetch_exit_info and not self.settings.download_test.enabled
        realping_results = self.run_realping_batch(
            nodes,
            concurrency=concurrency,
            ports=ports,
            fetch_exit_info=fetch_exit_info_during_realping,
        )
        realping_map = {result.parsed_node.config_hash: result for result in realping_results}
        final_by_hash: dict[str, TestResult] = {}
        survivors: list[ParsedNode] = []
        survivor_realping: dict[str, TestResult] = {}
        ports_by_hash = {node.config_hash: port for node, port in zip(nodes, ports or [], strict=False)}

        for node in nodes:
            result = realping_map.get(node.config_hash)
            if result is None:
                final_by_hash[node.config_hash] = TestResult(
                    parsed_node=node,
                    ok=False,
                    latency_ms=-1,
                    download_kbps=None,
                    error="realping result missing from batch",
                )
                continue
            if not result.ok:
                final_by_hash[node.config_hash] = result
                continue
            if result.latency_ms > self.settings.health.max_relay_delay_ms:
                final_by_hash[node.config_hash] = TestResult(
                    parsed_node=node,
                    ok=False,
                    latency_ms=result.latency_ms,
                    download_kbps=None,
                    exit_info=result.exit_info,
                    error="relay delay too high",
                )
                continue
            survivors.append(node)
            survivor_realping[node.config_hash] = result

        if not survivors:
            return [final_by_hash[node.config_hash] for node in nodes]

        if not self.settings.download_test.enabled:
            for node in survivors:
                realping = survivor_realping[node.config_hash]
                final_by_hash[node.config_hash] = TestResult(
                    parsed_node=node,
                    ok=True,
                    latency_ms=realping.latency_ms,
                    download_kbps=0,
                    exit_info=realping.exit_info,
                )
            return [final_by_hash[node.config_hash] for node in nodes]

        survivor_ports = None
        if ports is not None:
            survivor_ports = [ports_by_hash[node.config_hash] for node in survivors]
        download_results = self.run_download_batch(
            survivors,
            concurrency=concurrency,
            ports=survivor_ports,
            fetch_exit_info=fetch_exit_info,
        )
        download_map = {result.parsed_node.config_hash: result for result in download_results}
        for node in survivors:
            realping = survivor_realping[node.config_hash]
            download = download_map.get(node.config_hash)
            if download is None:
                final_by_hash[node.config_hash] = TestResult(
                    parsed_node=node,
                    ok=False,
                    latency_ms=realping.latency_ms,
                    download_kbps=0,
                    exit_info=realping.exit_info,
                    error="download result missing from batch",
                )
                continue
            final_by_hash[node.config_hash] = TestResult(
                parsed_node=node,
                ok=download.ok,
                latency_ms=realping.latency_ms,
                download_kbps=download.download_kbps,
                exit_info=download.exit_info or realping.exit_info,
                error=download.error,
            )
        return [final_by_hash[node.config_hash] for node in nodes]

    def run_realping_batch(
        self,
        nodes: list[ParsedNode],
        concurrency: int = 0,
        ports: list[int] | None = None,
        fetch_exit_info: bool = False,
    ) -> list[TestResult]:
        if not nodes:
            return []
        held_sockets = []
        if ports is None:
            ports, held_sockets = self.builder.reserve_ports(len(nodes))
        if len(ports) != len(nodes):
            raise ValueError("ports length must match nodes length")
        batch_nodes = [
            BatchNode(node=node, local_port=ports[idx], inbound_tag=f"in-{idx}", outbound_tag=f"proxy-{idx}")
            for idx, node in enumerate(nodes)
        ]
        for sock in held_sockets:
            sock.close()
        return self._run_realping_batch_recursive(batch_nodes, concurrency, fetch_exit_info)

    def run_download_batch(
        self,
        nodes: list[ParsedNode],
        concurrency: int = 0,
        ports: list[int] | None = None,
        fetch_exit_info: bool = False,
    ) -> list[TestResult]:
        if not nodes:
            return []
        held_sockets = []
        if ports is None:
            ports, held_sockets = self.builder.reserve_ports(len(nodes))
        if len(ports) != len(nodes):
            raise ValueError("ports length must match nodes length")
        batch_nodes = [
            BatchNode(node=node, local_port=ports[idx], inbound_tag=f"in-{idx}", outbound_tag=f"proxy-{idx}")
            for idx, node in enumerate(nodes)
        ]
        for sock in held_sockets:
            sock.close()
        return self._run_download_batch_recursive(batch_nodes, concurrency, fetch_exit_info)

    def _run_realping_batch_recursive(self, batch_nodes: list[BatchNode], concurrency: int, fetch_exit_info: bool) -> list[TestResult]:
        config = self.builder.build_batch(batch_nodes)
        with self.runner.run_temp(config) as process:
            if process.poll() is not None:
                if len(batch_nodes) == 1:
                    return [TestResult(parsed_node=batch_nodes[0].node, ok=False, latency_ms=-1, error="xray exited early")]
                mid = len(batch_nodes) // 2
                return self._run_realping_batch_recursive(batch_nodes[:mid], concurrency, fetch_exit_info) + self._run_realping_batch_recursive(batch_nodes[mid:], concurrency, fetch_exit_info)

            max_workers = len(batch_nodes) if concurrency <= 0 else min(concurrency, len(batch_nodes))
            results: list[TestResult] = []
            pool = ThreadPoolExecutor(max_workers=max_workers)
            futures = {pool.submit(self._probe_batch_node, item, fetch_exit_info): item for item in batch_nodes}
            done, pending = wait(futures, timeout=self.settings.health.candidate_batch_timeout_seconds)
            for future in done:
                results.append(future.result())
            for future in pending:
                future.cancel()
                item = futures[future]
                results.append(TestResult(parsed_node=item.node, ok=False, latency_ms=-1, error="candidate probe timed out"))
            pool.shutdown(wait=False, cancel_futures=True)
            return results

    def _run_download_batch_recursive(self, batch_nodes: list[BatchNode], concurrency: int, fetch_exit_info: bool) -> list[TestResult]:
        config = self.builder.build_batch(batch_nodes)
        with self.runner.run_temp(config) as process:
            if process.poll() is not None:
                if len(batch_nodes) == 1:
                    return [TestResult(parsed_node=batch_nodes[0].node, ok=False, latency_ms=-1, download_kbps=0, error="xray exited early")]
                mid = len(batch_nodes) // 2
                return self._run_download_batch_recursive(batch_nodes[:mid], concurrency, fetch_exit_info) + self._run_download_batch_recursive(batch_nodes[mid:], concurrency, fetch_exit_info)

            max_workers = len(batch_nodes) if concurrency <= 0 else min(concurrency, len(batch_nodes))
            results: list[TestResult] = []
            pool = ThreadPoolExecutor(max_workers=max_workers)
            futures = {pool.submit(self._probe_download_batch_node, item, fetch_exit_info): item for item in batch_nodes}
            batches = (len(batch_nodes) + max_workers - 1) // max_workers
            timeout = max(1, self.settings.download_test.timeout_seconds) * max(1, batches) + 2
            done, pending = wait(futures, timeout=timeout)
            for future in done:
                results.append(future.result())
            for future in pending:
                future.cancel()
                item = futures[future]
                results.append(TestResult(parsed_node=item.node, ok=False, latency_ms=-1, download_kbps=0, error="download probe timed out"))
            pool.shutdown(wait=False, cancel_futures=True)
            return results

    def _probe_batch_node(self, item: BatchNode, fetch_exit_info: bool) -> TestResult:
        try:
            latency = self._probe_relay_latency(item.local_port)
            exit_info = self._safe_fetch_exit_info(item.local_port) if fetch_exit_info else {}
            return TestResult(parsed_node=item.node, ok=True, latency_ms=latency, exit_info=exit_info)
        except Exception as exc:
            return TestResult(parsed_node=item.node, ok=False, latency_ms=-1, error=str(exc))

    def _probe_download_batch_node(self, item: BatchNode, fetch_exit_info: bool) -> TestResult:
        return self._probe_download_result(
            parsed_node=item.node,
            proxy_port=item.local_port,
            fetch_exit_info=fetch_exit_info,
            latency_ms=-1,
        )

    def _probe_existing_port(self, node: ParsedNode, proxy_port: int, fetch_exit_info: bool, include_download: bool) -> TestResult:
        try:
            latency = self._probe_relay_latency(proxy_port)
        except Exception as exc:
            return TestResult(parsed_node=node, ok=False, latency_ms=-1, download_kbps=0, error=str(exc))

        if latency > self.settings.health.max_relay_delay_ms:
            exit_info = self._safe_fetch_exit_info(proxy_port) if fetch_exit_info else {}
            return TestResult(parsed_node=node, ok=False, latency_ms=latency, download_kbps=0, exit_info=exit_info, error="relay delay too high")

        if not include_download:
            exit_info = self._safe_fetch_exit_info(proxy_port) if fetch_exit_info else {}
            return TestResult(parsed_node=node, ok=True, latency_ms=latency, download_kbps=None, exit_info=exit_info)

        if not self.settings.download_test.enabled:
            exit_info = self._safe_fetch_exit_info(proxy_port) if fetch_exit_info else {}
            return TestResult(parsed_node=node, ok=True, latency_ms=latency, download_kbps=0, exit_info=exit_info)

        return self._probe_download_result(
            parsed_node=node,
            proxy_port=proxy_port,
            fetch_exit_info=fetch_exit_info,
            latency_ms=latency,
        )

    def _probe_download_result(
        self,
        parsed_node: ParsedNode,
        proxy_port: int,
        fetch_exit_info: bool,
        latency_ms: int,
    ) -> TestResult:
        deadline = time.perf_counter() + max(0.5, float(self.settings.download_test.timeout_seconds))
        observed_speeds: list[int] = []
        inconclusive_errors: list[str] = []
        hard_errors: list[str] = []

        for url in self._download_test_urls():
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                break
            attempt_timeout = min(
                max(0.5, float(self.settings.download_test.per_url_timeout_seconds)),
                remaining,
            )
            try:
                download_kbps = self.download_probe.measure_speed_kbps(proxy_port, url, attempt_timeout)
            except Exception as exc:
                formatted_error = self._format_download_mirror_error(url, exc)
                if self._is_inconclusive_download_error(exc):
                    inconclusive_errors.append(formatted_error)
                else:
                    hard_errors.append(formatted_error)
                continue

            observed_speeds.append(download_kbps)
            if download_kbps >= self.settings.download_test.min_download_kbps:
                exit_info = self._safe_fetch_exit_info(proxy_port) if fetch_exit_info else {}
                return TestResult(
                    parsed_node=parsed_node,
                    ok=True,
                    latency_ms=latency_ms,
                    download_kbps=download_kbps,
                    exit_info=exit_info,
                )

        exit_info = self._safe_fetch_exit_info(proxy_port) if fetch_exit_info else {}
        if observed_speeds:
            best_speed = max(observed_speeds)
            error = "download speed zero" if best_speed <= 0 else "download speed below minimum"
            return TestResult(
                parsed_node=parsed_node,
                ok=False,
                latency_ms=latency_ms,
                download_kbps=best_speed,
                exit_info=exit_info,
                error=error,
            )
        if hard_errors:
            return TestResult(
                parsed_node=parsed_node,
                ok=False,
                latency_ms=latency_ms,
                download_kbps=0,
                exit_info=exit_info,
                error=self._summarize_download_errors("download failed", hard_errors),
            )
        if inconclusive_errors:
            return TestResult(
                parsed_node=parsed_node,
                ok=True,
                latency_ms=latency_ms,
                download_kbps=None,
                exit_info=exit_info,
                error=self._summarize_download_errors("download mirrors inconclusive", inconclusive_errors),
            )
        return TestResult(
            parsed_node=parsed_node,
            ok=False,
            latency_ms=latency_ms,
            download_kbps=0,
            exit_info=exit_info,
            error="download probe timed out",
        )

    def _probe_relay_latency(self, proxy_port: int) -> int:
        timeout = self.settings.health.relay_timeout_ms / 1000
        errors: list[str] = []
        for url in self._health_test_urls():
            try:
                return self.http_probe.measure_latency(
                    proxy_port,
                    url,
                    timeout,
                    attempts=1,
                    accept_any_status=True,
                )
            except Exception as exc:
                errors.append(self._format_download_mirror_error(url, exc))
        raise RuntimeError(self._summarize_download_errors("relay probe failed", errors))

    def _health_test_urls(self) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for candidate in [self.settings.health.test_url, *self.settings.health.fallback_urls]:
            url = candidate.strip()
            if not url or url in seen:
                continue
            seen.add(url)
            ordered.append(url)
        return ordered

    def _download_test_urls(self) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for candidate in [self.settings.download_test.test_url, *self.settings.download_test.fallback_urls]:
            url = candidate.strip()
            if not url or url in seen:
                continue
            seen.add(url)
            ordered.append(url)
        return ordered

    def _is_inconclusive_download_error(self, exc: Exception) -> bool:
        if isinstance(exc, UnexpectedHttpStatusError):
            return True
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "unexpected eof before http status",
                "unexpected eof before http headers",
                "http headers too large",
            )
        )

    def _format_download_mirror_error(self, url: str, exc: Exception) -> str:
        host = urlsplit(url).netloc or url
        return f"{host}: {exc}"

    def _summarize_download_errors(self, prefix: str, errors: list[str]) -> str:
        unique: list[str] = []
        seen: set[str] = set()
        for error in errors:
            if error in seen:
                continue
            seen.add(error)
            unique.append(error)
        preview = "; ".join(unique[:2])
        if len(unique) > 2:
            preview = f"{preview}; +{len(unique) - 2} more"
        return f"{prefix}: {preview}"

    def fetch_exit_info(self, proxy_port: int, timeout: float | None = None) -> dict[str, object]:
        effective_timeout = timeout or max(2.0, self.settings.health.relay_timeout_ms / 1000)
        providers = (
            ("ipinfo", IPINFO_URL),
            ("ip-api", IP_API_URL),
            ("ipwho", IPWHO_URL),
            ("ipapi.co", IPAPI_CO_URL),
        )
        errors: list[str] = []
        for provider, url in providers:
            try:
                payload = self.http_probe.fetch_json(proxy_port, url, effective_timeout)
                return self._normalize_exit_info(provider, payload)
            except Exception as exc:
                errors.append(f"{provider}: {exc}")
        raise RuntimeError("; ".join(errors) or "exit info lookup failed")

    def _safe_fetch_exit_info(self, proxy_port: int) -> dict[str, object]:
        try:
            return self.fetch_exit_info(proxy_port)
        except Exception:
            return {}

    def _normalize_exit_info(self, provider: str, payload: dict[str, object]) -> dict[str, object]:
        if provider == "ipinfo":
            normalized = dict(payload)
            normalized["provider"] = provider
            return normalized

        if provider == "ip-api":
            if str(payload.get("status", "")).lower() != "success":
                raise ValueError(f"ip-api lookup failed: {payload.get('message', 'unknown error')}")
            lat = payload.get("lat")
            lon = payload.get("lon")
            loc = ""
            if lat not in (None, "") and lon not in (None, ""):
                loc = f"{lat},{lon}"
            return {
                "provider": provider,
                "ip": str(payload.get("query", "") or ""),
                "hostname": str(payload.get("reverse", "") or ""),
                "city": str(payload.get("city", "") or ""),
                "region": str(payload.get("regionName", "") or payload.get("region", "") or ""),
                "country": str(payload.get("countryCode", "") or ""),
                "loc": loc,
                "org": str(payload.get("org", "") or payload.get("isp", "") or ""),
                "postal": str(payload.get("zip", "") or ""),
                "timezone": str(payload.get("timezone", "") or ""),
                "country_name": str(payload.get("country", "") or ""),
                "as": str(payload.get("as", "") or ""),
            }

        if provider == "ipwho":
            if not bool(payload.get("success")):
                raise ValueError(f"ipwho lookup failed: {payload.get('message', 'unknown error')}")
            latitude = payload.get("latitude")
            longitude = payload.get("longitude")
            loc = ""
            if latitude not in (None, "") and longitude not in (None, ""):
                loc = f"{latitude},{longitude}"
            connection = payload.get("connection")
            timezone = payload.get("timezone")
            flag = payload.get("flag")
            return {
                "provider": provider,
                "ip": str(payload.get("ip", "") or ""),
                "hostname": "",
                "city": str(payload.get("city", "") or ""),
                "region": str(payload.get("region", "") or ""),
                "country": str(payload.get("country_code", "") or ""),
                "loc": loc,
                "org": str((connection or {}).get("org", "") or (connection or {}).get("isp", "") or ""),
                "postal": str(payload.get("postal", "") or ""),
                "timezone": str((timezone or {}).get("id", "") or ""),
                "country_name": str(payload.get("country", "") or ""),
                "continent": str(payload.get("continent", "") or ""),
                "continent_code": str(payload.get("continent_code", "") or ""),
                "as": str((connection or {}).get("asn", "") or ""),
                "isp": str((connection or {}).get("isp", "") or ""),
                "domain": str((connection or {}).get("domain", "") or ""),
                "flag_emoji": str((flag or {}).get("emoji", "") or ""),
            }

        if provider == "ipapi.co":
            latitude = payload.get("latitude")
            longitude = payload.get("longitude")
            loc = ""
            if latitude not in (None, "") and longitude not in (None, ""):
                loc = f"{latitude},{longitude}"
            return {
                "provider": provider,
                "ip": str(payload.get("ip", "") or ""),
                "hostname": "",
                "city": str(payload.get("city", "") or ""),
                "region": str(payload.get("region", "") or ""),
                "country": str(payload.get("country_code", "") or payload.get("country", "") or ""),
                "loc": loc,
                "org": str(payload.get("org", "") or ""),
                "postal": str(payload.get("postal", "") or ""),
                "timezone": str(payload.get("timezone", "") or ""),
                "country_name": str(payload.get("country_name", "") or ""),
                "continent_code": str(payload.get("continent_code", "") or ""),
                "as": str(payload.get("asn", "") or ""),
                "network": str(payload.get("network", "") or ""),
            }

        raise ValueError(f"unsupported exit info provider: {provider}")
