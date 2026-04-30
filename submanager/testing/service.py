from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path

from submanager.config.models import AppSettings
from submanager.core.models import BatchNode, ParsedNode, TestResult
from submanager.testing.probes import DownloadSpeedProbe, HttpProxyProbe
from submanager.testing.xray import XrayBinaryResolver, XrayConfigBuilder, XrayRunner


class TestService:
    def __init__(self, settings: AppSettings, cache_dir: Path) -> None:
        self.settings = settings
        xray_bin = XrayBinaryResolver().ensure(settings.xray_bin, cache_dir)
        self.builder = XrayConfigBuilder()
        self.runner = XrayRunner(xray_bin)
        self.http_probe = HttpProxyProbe()
        self.download_probe = DownloadSpeedProbe()

    def run_realping_batch(self, nodes: list[ParsedNode], concurrency: int = 0, ports: list[int] | None = None) -> list[TestResult]:
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
        return self._run_realping_batch_recursive(batch_nodes, concurrency)

    def _run_realping_batch_recursive(self, batch_nodes: list[BatchNode], concurrency: int) -> list[TestResult]:
        config = self.builder.build_batch(batch_nodes)
        with self.runner.run_temp(config) as process:
            if process.poll() is not None:
                if len(batch_nodes) == 1:
                    return [TestResult(parsed_node=batch_nodes[0].node, ok=False, latency_ms=-1, error="xray exited early")]
                mid = len(batch_nodes) // 2
                return self._run_realping_batch_recursive(batch_nodes[:mid], concurrency) + self._run_realping_batch_recursive(batch_nodes[mid:], concurrency)

            max_workers = len(batch_nodes) if concurrency <= 0 else min(concurrency, len(batch_nodes))
            results: list[TestResult] = []
            pool = ThreadPoolExecutor(max_workers=max_workers)
            futures = {pool.submit(self._probe_batch_node, item): item for item in batch_nodes}
            done, pending = wait(futures, timeout=self.settings.health.candidate_batch_timeout_seconds)
            for future in done:
                results.append(future.result())
            for future in pending:
                future.cancel()
                item = futures[future]
                results.append(TestResult(parsed_node=item.node, ok=False, latency_ms=-1, error="candidate probe timed out"))
            pool.shutdown(wait=False, cancel_futures=True)
            return results

    def run_full_test(self, node: ParsedNode, local_port: int) -> TestResult:
        config = self.builder.build_single(node, local_port)
        with self.runner.run_temp(config) as process:
            if process.poll() is not None:
                return TestResult(parsed_node=node, ok=False, latency_ms=-1, error="xray exited early")
            try:
                latency = self.http_probe.measure_latency(
                    local_port,
                    self.settings.health.test_url,
                    self.settings.health.relay_timeout_ms / 1000,
                )
            except Exception as exc:
                return TestResult(parsed_node=node, ok=False, latency_ms=-1, error=str(exc))

            if latency > self.settings.health.max_relay_delay_ms:
                return TestResult(parsed_node=node, ok=False, latency_ms=latency, error="relay delay too high")

            if not self.settings.download_test.enabled:
                return TestResult(parsed_node=node, ok=True, latency_ms=latency, download_kbps=0)

            try:
                download_kbps = self.download_probe.measure_speed_kbps(
                    local_port,
                    self.settings.download_test.test_url,
                    self.settings.download_test.timeout_seconds,
                )
            except Exception as exc:
                return TestResult(parsed_node=node, ok=False, latency_ms=latency, error=str(exc))

            if download_kbps < self.settings.download_test.min_download_kbps:
                return TestResult(parsed_node=node, ok=False, latency_ms=latency, download_kbps=download_kbps, error="download speed below minimum")

            return TestResult(parsed_node=node, ok=True, latency_ms=latency, download_kbps=download_kbps)

    def probe_running_port(self, port: int) -> int:
        return self.http_probe.measure_latency(port, self.settings.health.test_url, self.settings.health.relay_timeout_ms / 1000)

    def _probe_batch_node(self, item: BatchNode) -> TestResult:
        try:
            latency = self.http_probe.measure_latency(
                item.local_port,
                self.settings.health.test_url,
                self.settings.health.relay_timeout_ms / 1000,
                attempts=1,
            )
            return TestResult(parsed_node=item.node, ok=True, latency_ms=latency)
        except Exception as exc:
            return TestResult(parsed_node=item.node, ok=False, latency_ms=-1, error=str(exc))
