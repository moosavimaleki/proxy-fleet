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

    def test_dead_recheck_selects_only_due_recheckable_nodes(self) -> None:
        now = datetime.now(timezone.utc)
        due = self._save_node("1", status=NodeStatus.DEAD, next_test_at=now - timedelta(seconds=1))
        deferred = self._save_node("2", status=NodeStatus.DEAD, next_test_at=now + timedelta(hours=1))
        invalid = self._save_node("3", status=NodeStatus.DEAD, next_test_at=now - timedelta(seconds=1))
        invalid.dead_recheckable = False
        self.store.save_node(invalid)

        selected = self.store.list_dead_nodes_for_testing(limit=10)

        self.assertEqual([due.id], [node.id for node in selected])
        self.assertNotIn(deferred.id, [node.id for node in selected])
        self.assertNotIn(invalid.id, [node.id for node in selected])

    def test_upstream_reconciliation_prunes_only_after_two_complete_misses(self) -> None:
        now = datetime.now(timezone.utc)
        stale = self._save_node("1", status=NodeStatus.DEAD)
        stale.dead_until = now - timedelta(hours=1)
        self.store.save_node(stale)
        current = self._save_node("2")
        recent = self._save_node("3", status=NodeStatus.DEAD)
        recent.last_download_test_at = now - timedelta(hours=1)
        self.store.save_node(recent)
        manual = self._save_node("4")
        manual.source_subs = ["manual://import"]
        self.store.save_node(manual)

        first = self.store.reconcile_upstream_nodes(
            seen_sources_by_hash={current.config_hash: {"test://source"}},
            prune_after_cycles=2,
            recent_success_since=now - timedelta(hours=24),
        )
        self.assertEqual(0, first["deleted"])
        self.assertEqual(1, self.store.get_node(stale.id).upstream_missing_cycles)  # type: ignore[union-attr]
        self.assertEqual(0, self.store.delete_expired_dead_nodes())
        self.assertIsNotNone(self.store.get_node(stale.id))

        second = self.store.reconcile_upstream_nodes(
            seen_sources_by_hash={current.config_hash: {"test://source"}},
            prune_after_cycles=2,
            recent_success_since=now - timedelta(hours=24),
        )
        self.assertEqual(1, second["deleted"])
        self.assertIsNone(self.store.get_node(stale.id))
        self.assertIsNotNone(self.store.get_node(current.id))
        self.assertIsNotNone(self.store.get_node(recent.id))
        self.assertIsNotNone(self.store.get_node(manual.id))

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
