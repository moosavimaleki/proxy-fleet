from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from submanager.core.models import NodeRecord, NodeStatus
from submanager.storage.sqlite_store import SqliteStore


class StoreSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = SqliteStore(Path(self.temp_dir.name) / "test.db")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _save_node(
        self,
        suffix: str,
        *,
        status: NodeStatus = NodeStatus.CANDIDATE,
        last_test_at: datetime | None = None,
        next_test_at: datetime | None = None,
        country: str = "",
    ) -> NodeRecord:
        now = datetime.now(timezone.utc)
        node = NodeRecord(
            id=suffix.rjust(32, "0"),
            config_hash=f"hash-{suffix}",
            raw_config=f"vless://secret-{suffix}",
            normalized_config={"protocol": "vless", "server": f"node-{suffix}.example", "port": 443},
            source_subs=["test://source"],
            status=status,
            exit_country=country,
            created_at=now,
            last_test_at=last_test_at,
            next_test_at=next_test_at,
        )
        self.store.save_node(node)
        return node

    def test_candidate_selection_is_bounded_and_prioritizes_untested_nodes(self) -> None:
        now = datetime.now(timezone.utc)
        tested = self._save_node("1", last_test_at=now - timedelta(hours=2))
        first_untested = self._save_node("2")
        second_untested = self._save_node("3")

        selected = self.store.list_candidate_nodes_for_testing(limit=2)

        self.assertEqual([first_untested.id, second_untested.id], [node.id for node in selected])
        self.assertNotIn(tested.id, [node.id for node in selected])

    def test_candidate_backoff_excludes_nodes_until_retry_time(self) -> None:
        now = datetime.now(timezone.utc)
        due = self._save_node("1", last_test_at=now, next_test_at=now - timedelta(seconds=1))
        deferred = self._save_node("2", last_test_at=now, next_test_at=now + timedelta(hours=1))

        selected = self.store.list_candidate_nodes_for_testing(limit=10)

        self.assertIn(due.id, [node.id for node in selected])
        self.assertNotIn(deferred.id, [node.id for node in selected])

    def test_node_page_is_filtered_counted_and_does_not_load_every_node(self) -> None:
        self._save_node("1", country="US")
        expected = self._save_node("2", country="DE")
        self._save_node("3", status=NodeStatus.DEAD, country="DE")

        nodes, filtered_total = self.store.list_nodes_page(
            limit=1,
            offset=0,
            status=NodeStatus.CANDIDATE.value,
            country="DE",
            search="node-2",
        )

        self.assertEqual(1, filtered_total)
        self.assertEqual([expected.id], [node.id for node in nodes])
        self.assertEqual({"CANDIDATE": 2, "DEAD": 1}, self.store.count_nodes_by_status())
        self.assertEqual(["DE", "US"], self.store.list_exit_countries())


if __name__ == "__main__":
    unittest.main()
