from __future__ import annotations

from datetime import datetime, timezone

from submanager.core.models import ParsedNode, RuntimeHandle, ServiceState
from submanager.testing.xray import XrayConfigBuilder, XrayRunner


class RuntimeSupervisor:
    def __init__(self, state: ServiceState, runner: XrayRunner) -> None:
        self.state = state
        self.runner = runner
        self.builder = XrayConfigBuilder()

    def start_active_runtime(self, node_id: str, parsed_node: ParsedNode, port: int) -> RuntimeHandle:
        config = self.builder.build_single(parsed_node, port, listen="0.0.0.0")
        process = self.runner.start_persistent(config)
        handle = RuntimeHandle(node_id=node_id, port=port, process=process, started_at=datetime.now(timezone.utc))
        self.state.active_runtimes[node_id] = handle
        return handle

    def stop_active_runtime(self, node_id: str) -> None:
        handle = self.state.active_runtimes.pop(node_id, None)
        if handle is not None:
            self.runner.stop_persistent(handle.process)

    def is_running(self, node_id: str) -> bool:
        handle = self.state.active_runtimes.get(node_id)
        return handle is not None and handle.process.poll() is None

    def get_port(self, node_id: str) -> int | None:
        handle = self.state.active_runtimes.get(node_id)
        return handle.port if handle else None

    def start_vip_runtime(self, node_id: str, parsed_node: ParsedNode, port: int) -> RuntimeHandle:
        self.stop_vip_runtime()
        config = self.builder.build_single(parsed_node, port, listen="0.0.0.0")
        process = self.runner.start_persistent(config)
        handle = RuntimeHandle(node_id=node_id, port=port, process=process, started_at=datetime.now(timezone.utc))
        self.state.vip_runtime = handle
        self.state.vip_node_id = node_id
        return handle

    def stop_vip_runtime(self) -> None:
        handle = self.state.vip_runtime
        self.state.vip_runtime = None
        self.state.vip_node_id = None
        self.state.vip_score = None
        if handle is not None:
            self.runner.stop_persistent(handle.process)

    def is_vip_running(self) -> bool:
        handle = self.state.vip_runtime
        return handle is not None and handle.process.poll() is None
