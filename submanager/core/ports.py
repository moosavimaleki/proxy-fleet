from __future__ import annotations

import threading
from collections.abc import Iterable

from submanager.config.models import PortRange


class PortManager:
    def __init__(self, main_range: PortRange, test_range: PortRange) -> None:
        self._main_range = main_range
        self._test_range = test_range
        self._lock = threading.RLock()
        self._used_main: set[int] = set()
        self._used_test: set[int] = set()

    def reserve_existing_main(self, ports: Iterable[int]) -> None:
        with self._lock:
            for port in ports:
                if port:
                    self._used_main.add(port)

    def allocate_main(self) -> int | None:
        return self._allocate(self._main_range, self._used_main)

    def release_main(self, port: int | None) -> None:
        if port is None:
            return
        with self._lock:
            self._used_main.discard(port)

    def allocate_test(self) -> int | None:
        return self._allocate(self._test_range, self._used_test)

    def release_test(self, port: int | None) -> None:
        if port is None:
            return
        with self._lock:
            self._used_test.discard(port)

    def _allocate(self, port_range: PortRange, used: set[int]) -> int | None:
        with self._lock:
            for port in range(port_range.start, port_range.end + 1):
                if port not in used:
                    used.add(port)
                    return port
        return None
