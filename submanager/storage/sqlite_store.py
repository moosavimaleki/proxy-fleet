from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from submanager.core.models import (
    AssignmentRecord,
    ClientCircuitState,
    ClientNodeStateRecord,
    NodeRecord,
    NodeStatus,
    ParsedNode,
    SystemEventRecord,
    TestHistoryRecord,
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_str(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def str_to_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class SqliteStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self.lock = threading.RLock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                  id TEXT PRIMARY KEY,
                  config_hash TEXT UNIQUE NOT NULL,
                  raw_config TEXT NOT NULL,
                  normalized_config TEXT NOT NULL,
                  source_subs TEXT NOT NULL,
                  status TEXT NOT NULL,
                  main_port INTEGER NULL,
                  relay_delay_ms INTEGER NULL,
                  download_kbps INTEGER NULL,
                  exit_ip TEXT NOT NULL DEFAULT '',
                  exit_hostname TEXT NOT NULL DEFAULT '',
                  exit_city TEXT NOT NULL DEFAULT '',
                  exit_region TEXT NOT NULL DEFAULT '',
                  exit_country TEXT NOT NULL DEFAULT '',
                  exit_loc TEXT NOT NULL DEFAULT '',
                  exit_org TEXT NOT NULL DEFAULT '',
                  exit_postal TEXT NOT NULL DEFAULT '',
                  exit_timezone TEXT NOT NULL DEFAULT '',
                  exit_info_json TEXT NOT NULL DEFAULT '{}',
                  health_success_ewma REAL DEFAULT 1.0,
                  consecutive_relay_failures INTEGER DEFAULT 0,
                  consecutive_relay_successes INTEGER DEFAULT 0,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  last_health_check_at TEXT NULL,
                  last_test_at TEXT NULL,
                  exit_info_fetched_at TEXT NULL,
                  dead_until TEXT NULL
                );

                CREATE TABLE IF NOT EXISTS client_node_state (
                  client_id TEXT NOT NULL,
                  node_id TEXT NOT NULL,
                  state TEXT NOT NULL DEFAULT 'CLOSED',
                  fail_streak INTEGER DEFAULT 0,
                  rate_limit_streak INTEGER DEFAULT 0,
                  cooldown_until TEXT NULL,
                  usage_count INTEGER DEFAULT 0,
                  success_count INTEGER DEFAULT 0,
                  broken_count INTEGER DEFAULT 0,
                  rate_limited_count INTEGER DEFAULT 0,
                  recent_usage_score REAL DEFAULT 0,
                  success_rate_ewma REAL DEFAULT 0.5,
                  last_assigned_at TEXT NULL,
                  last_feedback_at TEXT NULL,
                  last_failure_at TEXT NULL,
                  last_success_at TEXT NULL,
                  PRIMARY KEY (client_id, node_id)
                );

                CREATE TABLE IF NOT EXISTS assignment_events (
                  id TEXT PRIMARY KEY,
                  client_id TEXT NOT NULL,
                  node_id TEXT NOT NULL,
                  port INTEGER NOT NULL,
                  assigned_at TEXT NOT NULL,
                  feedback_status TEXT NULL,
                  feedback_at TEXT NULL
                );

                CREATE TABLE IF NOT EXISTS usage_events (
                  id TEXT PRIMARY KEY,
                  client_id TEXT NOT NULL,
                  node_id TEXT NOT NULL,
                  event_type TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS test_history (
                  id TEXT PRIMARY KEY,
                  node_id TEXT NOT NULL,
                  test_kind TEXT NOT NULL,
                  trigger TEXT NOT NULL,
                  started_at TEXT NOT NULL,
                  finished_at TEXT NOT NULL,
                  network_online INTEGER NOT NULL,
                  ok INTEGER NOT NULL,
                  latency_ms INTEGER NULL,
                  download_kbps INTEGER NULL,
                  error TEXT NOT NULL DEFAULT '',
                  status_before TEXT NOT NULL DEFAULT '',
                  status_after TEXT NOT NULL DEFAULT '',
                  details_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS system_events (
                  id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  level TEXT NOT NULL,
                  component TEXT NOT NULL,
                  event TEXT NOT NULL,
                  message TEXT NOT NULL,
                  details_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_system_events_created_at
                  ON system_events(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_system_events_component_level_created_at
                  ON system_events(component, level, created_at DESC);
                """
            )
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(nodes)").fetchall()}
            if "consecutive_relay_successes" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN consecutive_relay_successes INTEGER DEFAULT 0")
            if "exit_ip" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_ip TEXT NOT NULL DEFAULT ''")
            if "exit_hostname" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_hostname TEXT NOT NULL DEFAULT ''")
            if "exit_city" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_city TEXT NOT NULL DEFAULT ''")
            if "exit_region" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_region TEXT NOT NULL DEFAULT ''")
            if "exit_country" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_country TEXT NOT NULL DEFAULT ''")
            if "exit_loc" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_loc TEXT NOT NULL DEFAULT ''")
            if "exit_org" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_org TEXT NOT NULL DEFAULT ''")
            if "exit_postal" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_postal TEXT NOT NULL DEFAULT ''")
            if "exit_timezone" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_timezone TEXT NOT NULL DEFAULT ''")
            if "exit_info_json" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_info_json TEXT NOT NULL DEFAULT '{}'")
            if "exit_info_fetched_at" not in columns:
                conn.execute("ALTER TABLE nodes ADD COLUMN exit_info_fetched_at TEXT NULL")

    def get_node_by_hash(self, config_hash: str) -> NodeRecord | None:
        with self.lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE config_hash = ?", (config_hash,)).fetchone()
            return self._row_to_node(row) if row else None

    def get_node_by_raw_config(self, raw_config: str) -> NodeRecord | None:
        with self.lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE raw_config = ?", (raw_config,)).fetchone()
            return self._row_to_node(row) if row else None

    def get_node(self, node_id: str) -> NodeRecord | None:
        with self.lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
            return self._row_to_node(row) if row else None

    def create_or_merge_candidate(self, parsed: ParsedNode) -> NodeRecord:
        now = utcnow()
        existing = self.get_node_by_hash(parsed.config_hash) or self.get_node_by_raw_config(parsed.raw_config)
        if existing:
            if parsed.source_url and parsed.source_url not in existing.source_subs:
                existing.source_subs = sorted({*existing.source_subs, parsed.source_url})
                self.save_node(existing)
            return existing
        record = NodeRecord(
            id=uuid.uuid4().hex,
            config_hash=parsed.config_hash,
            raw_config=parsed.raw_config,
            normalized_config=parsed.normalized_config,
            source_subs=[parsed.source_url],
            status=NodeStatus.CANDIDATE,
            created_at=now,
            updated_at=now,
        )
        self.save_node(record)
        return record

    def save_node(self, node: NodeRecord) -> None:
        now = utcnow()
        node.updated_at = now
        node.created_at = node.created_at or now
        with self.lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO nodes (
                    id, config_hash, raw_config, normalized_config, source_subs, status,
                    main_port, relay_delay_ms, download_kbps, exit_ip, exit_hostname, exit_city,
                    exit_region, exit_country, exit_loc, exit_org, exit_postal, exit_timezone, exit_info_json, health_success_ewma,
                    consecutive_relay_failures, consecutive_relay_successes, created_at, updated_at,
                    last_health_check_at, last_test_at, exit_info_fetched_at, dead_until
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    config_hash=excluded.config_hash,
                    raw_config=excluded.raw_config,
                    normalized_config=excluded.normalized_config,
                    source_subs=excluded.source_subs,
                    status=excluded.status,
                    main_port=excluded.main_port,
                    relay_delay_ms=excluded.relay_delay_ms,
                    download_kbps=excluded.download_kbps,
                    exit_ip=excluded.exit_ip,
                    exit_hostname=excluded.exit_hostname,
                    exit_city=excluded.exit_city,
                    exit_region=excluded.exit_region,
                    exit_country=excluded.exit_country,
                    exit_loc=excluded.exit_loc,
                    exit_org=excluded.exit_org,
                    exit_postal=excluded.exit_postal,
                    exit_timezone=excluded.exit_timezone,
                    exit_info_json=excluded.exit_info_json,
                    health_success_ewma=excluded.health_success_ewma,
                    consecutive_relay_failures=excluded.consecutive_relay_failures,
                    consecutive_relay_successes=excluded.consecutive_relay_successes,
                    updated_at=excluded.updated_at,
                    last_health_check_at=excluded.last_health_check_at,
                    last_test_at=excluded.last_test_at,
                    exit_info_fetched_at=excluded.exit_info_fetched_at,
                    dead_until=excluded.dead_until
                """,
                (
                    node.id,
                    node.config_hash,
                    node.raw_config,
                    json.dumps(node.normalized_config, ensure_ascii=False),
                    json.dumps(node.source_subs, ensure_ascii=False),
                    node.status.value,
                    node.main_port,
                    node.relay_delay_ms,
                    node.download_kbps,
                    node.exit_ip,
                    node.exit_hostname,
                    node.exit_city,
                    node.exit_region,
                    node.exit_country,
                    node.exit_loc,
                    node.exit_org,
                    node.exit_postal,
                    node.exit_timezone,
                    json.dumps(node.exit_info, ensure_ascii=False),
                    node.health_success_ewma,
                    node.consecutive_relay_failures,
                    node.consecutive_relay_successes,
                    dt_to_str(node.created_at),
                    dt_to_str(node.updated_at),
                    dt_to_str(node.last_health_check_at),
                    dt_to_str(node.last_test_at),
                    dt_to_str(node.exit_info_fetched_at),
                    dt_to_str(node.dead_until),
                ),
            )

    def list_nodes_by_status(self, status: NodeStatus) -> list[NodeRecord]:
        with self.lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM nodes WHERE status = ?", (status.value,)).fetchall()
            return [self._row_to_node(row) for row in rows]

    def list_nodes_by_exit_ip(self, exit_ip: str) -> list[NodeRecord]:
        if not exit_ip:
            return []
        with self.lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM nodes WHERE exit_ip = ?", (exit_ip,)).fetchall()
            return [self._row_to_node(row) for row in rows]

    def reset_testing_nodes(self) -> int:
        with self.lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM nodes WHERE status = ?", (NodeStatus.TESTING.value,)).fetchall()
            if not rows:
                return 0
            now = dt_to_str(utcnow())
            conn.execute(
                """
                UPDATE nodes
                SET status = ?, updated_at = ?
                WHERE status = ?
                """,
                (NodeStatus.CANDIDATE.value, now, NodeStatus.TESTING.value),
            )
            return len(rows)

    def list_nodes(self) -> list[NodeRecord]:
        with self.lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM nodes
                ORDER BY
                  CASE status
                    WHEN 'ACTIVE' THEN 0
                    WHEN 'PROBATION' THEN 1
                    WHEN 'TESTING' THEN 2
                    WHEN 'CANDIDATE' THEN 3
                    WHEN 'WAITING_FOR_PORT' THEN 4
                    WHEN 'DEAD' THEN 5
                    ELSE 6
                  END,
                  COALESCE(relay_delay_ms, 999999) ASC,
                  updated_at DESC
                """
            ).fetchall()
            return [self._row_to_node(row) for row in rows]

    def record_test_history(
        self,
        node_id: str,
        test_kind: str,
        trigger: str,
        started_at: datetime,
        finished_at: datetime,
        network_online: bool,
        ok: bool,
        latency_ms: int | None,
        download_kbps: int | None,
        error: str,
        status_before: str,
        status_after: str,
        details: dict[str, object] | None = None,
    ) -> TestHistoryRecord:
        record = TestHistoryRecord(
            id=uuid.uuid4().hex,
            node_id=node_id,
            test_kind=test_kind,
            trigger=trigger,
            started_at=started_at,
            finished_at=finished_at,
            network_online=network_online,
            ok=ok,
            latency_ms=latency_ms,
            download_kbps=download_kbps,
            error=error,
            status_before=status_before,
            status_after=status_after,
            details=details or {},
        )
        with self.lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO test_history (
                  id, node_id, test_kind, trigger, started_at, finished_at, network_online, ok,
                  latency_ms, download_kbps, error, status_before, status_after, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.node_id,
                    record.test_kind,
                    record.trigger,
                    dt_to_str(record.started_at),
                    dt_to_str(record.finished_at),
                    1 if record.network_online else 0,
                    1 if record.ok else 0,
                    record.latency_ms,
                    record.download_kbps,
                    record.error,
                    record.status_before,
                    record.status_after,
                    json.dumps(record.details, ensure_ascii=False),
                ),
            )
        return record

    def list_test_history(self, node_id: str, limit: int = 50) -> list[TestHistoryRecord]:
        with self.lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM test_history
                WHERE node_id = ?
                ORDER BY finished_at DESC
                LIMIT ?
                """,
                (node_id, limit),
            ).fetchall()
            return [self._row_to_test_history(row) for row in rows]

    def record_system_event(
        self,
        level: str,
        component: str,
        event: str,
        message: str,
        details: dict[str, object] | None = None,
    ) -> SystemEventRecord:
        record = SystemEventRecord(
            id=uuid.uuid4().hex,
            created_at=utcnow(),
            level=level,
            component=component,
            event=event,
            message=message,
            details=details or {},
        )
        with self.lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO system_events (
                  id, created_at, level, component, event, message, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    dt_to_str(record.created_at),
                    record.level,
                    record.component,
                    record.event,
                    record.message,
                    json.dumps(record.details, ensure_ascii=False),
                ),
            )
        return record

    def list_system_events(self, limit: int = 200, component: str = "", level: str = "") -> list[SystemEventRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if component:
            clauses.append("component = ?")
            params.append(component)
        if level:
            clauses.append("level = ?")
            params.append(level)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self.lock, self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM system_events
                {where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
            return [self._row_to_system_event(row) for row in rows]

    def list_dashboard_stats(self, ttl_seconds: int) -> dict[str, dict[str, int | str | None]]:
        cutoff = (utcnow() - timedelta(seconds=ttl_seconds)).isoformat()
        results: dict[str, dict[str, int | str | None]] = {}
        with self.lock, self._connect() as conn:
            assignment_rows = conn.execute(
                """
                SELECT
                  node_id,
                  SUM(CASE WHEN feedback_status IS NULL AND assigned_at >= ? THEN 1 ELSE 0 END) AS open_assignments,
                  COUNT(*) AS total_assignments,
                  SUM(CASE WHEN feedback_status = 'used' THEN 1 ELSE 0 END) AS used_count,
                  SUM(CASE WHEN feedback_status = 'broken' THEN 1 ELSE 0 END) AS broken_count,
                  SUM(CASE WHEN feedback_status = 'rate_limited' THEN 1 ELSE 0 END) AS rate_limited_count,
                  MAX(assigned_at) AS last_assigned_at,
                  MAX(feedback_at) AS last_feedback_at
                FROM assignment_events
                GROUP BY node_id
                """
                ,
                (cutoff,),
            ).fetchall()
            for row in assignment_rows:
                results[row["node_id"]] = {
                    "open_assignments": int(row["open_assignments"] or 0),
                    "total_assignments": int(row["total_assignments"] or 0),
                    "used_count": int(row["used_count"] or 0),
                    "broken_count": int(row["broken_count"] or 0),
                    "rate_limited_count": int(row["rate_limited_count"] or 0),
                    "last_assigned_at": row["last_assigned_at"],
                    "last_feedback_at": row["last_feedback_at"],
                }

            client_rows = conn.execute(
                """
                SELECT
                  node_id,
                  COUNT(*) AS total_clients,
                  SUM(CASE WHEN state = 'OPEN' THEN 1 ELSE 0 END) AS open_clients,
                  SUM(CASE WHEN state = 'HALF_OPEN' THEN 1 ELSE 0 END) AS half_open_clients,
                  SUM(CASE WHEN state = 'CLOSED' THEN 1 ELSE 0 END) AS closed_clients,
                  MAX(last_assigned_at) AS last_client_assigned_at,
                  MAX(last_feedback_at) AS last_client_feedback_at
                FROM client_node_state
                GROUP BY node_id
                """
            ).fetchall()
            for row in client_rows:
                stats = results.setdefault(row["node_id"], {})
                stats.update(
                    {
                        "total_clients": int(row["total_clients"] or 0),
                        "open_clients": int(row["open_clients"] or 0),
                        "half_open_clients": int(row["half_open_clients"] or 0),
                        "closed_clients": int(row["closed_clients"] or 0),
                        "last_client_assigned_at": row["last_client_assigned_at"],
                        "last_client_feedback_at": row["last_client_feedback_at"],
                    }
                )
        return results

    def list_client_ids(self) -> list[str]:
        with self.lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT client_id FROM client_node_state
                UNION
                SELECT client_id FROM assignment_events
                ORDER BY client_id ASC
                """
            ).fetchall()
            return [str(row["client_id"]) for row in rows]

    def list_client_dashboard_stats(self, client_id: str) -> dict[str, dict[str, int | str | None]]:
        results: dict[str, dict[str, int | str | None]] = {}
        with self.lock, self._connect() as conn:
            client_rows = conn.execute(
                """
                SELECT *
                FROM client_node_state
                WHERE client_id = ?
                """,
                (client_id,),
            ).fetchall()
            for row in client_rows:
                results[row["node_id"]] = {
                    "client_state": row["state"],
                    "fail_streak": int(row["fail_streak"] or 0),
                    "rate_limit_streak": int(row["rate_limit_streak"] or 0),
                    "cooldown_until": row["cooldown_until"],
                    "usage_count": int(row["usage_count"] or 0),
                    "success_count": int(row["success_count"] or 0),
                    "broken_count": int(row["broken_count"] or 0),
                    "rate_limited_count": int(row["rate_limited_count"] or 0),
                    "recent_usage_score": float(row["recent_usage_score"] or 0.0),
                    "success_rate_ewma": float(row["success_rate_ewma"] or 0.0),
                    "last_assigned_at": row["last_assigned_at"],
                    "last_feedback_at": row["last_feedback_at"],
                    "last_failure_at": row["last_failure_at"],
                    "last_success_at": row["last_success_at"],
                }

            assignment_rows = conn.execute(
                """
                SELECT
                  node_id,
                  COUNT(*) AS total_assignments,
                  SUM(CASE WHEN feedback_status IS NULL THEN 1 ELSE 0 END) AS open_assignments,
                  SUM(CASE WHEN feedback_status = 'used' THEN 1 ELSE 0 END) AS used_count,
                  SUM(CASE WHEN feedback_status = 'broken' THEN 1 ELSE 0 END) AS broken_feedback_count,
                  SUM(CASE WHEN feedback_status = 'rate_limited' THEN 1 ELSE 0 END) AS rate_limited_feedback_count,
                  MAX(assigned_at) AS latest_assigned_at,
                  MAX(feedback_at) AS latest_feedback_at
                FROM assignment_events
                WHERE client_id = ?
                GROUP BY node_id
                """,
                (client_id,),
            ).fetchall()
            for row in assignment_rows:
                stats = results.setdefault(row["node_id"], {})
                stats.update(
                    {
                        "client_total_assignments": int(row["total_assignments"] or 0),
                        "client_open_assignments": int(row["open_assignments"] or 0),
                        "client_used_feedback_count": int(row["used_count"] or 0),
                        "client_broken_feedback_count": int(row["broken_feedback_count"] or 0),
                        "client_rate_limited_feedback_count": int(row["rate_limited_feedback_count"] or 0),
                        "latest_assigned_at": row["latest_assigned_at"],
                        "latest_feedback_at": row["latest_feedback_at"],
                    }
                )
        return results

    def delete_expired_dead_nodes(self) -> int:
        now = utcnow().isoformat()
        with self.lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT id FROM nodes WHERE status = ? AND dead_until IS NOT NULL AND dead_until <= ?",
                (NodeStatus.DEAD.value, now),
            ).fetchall()
            node_ids = [str(row["id"]) for row in rows]
            if not node_ids:
                return 0
            placeholders = ",".join("?" for _ in node_ids)
            conn.execute(f"DELETE FROM test_history WHERE node_id IN ({placeholders})", node_ids)
            conn.execute(f"DELETE FROM client_node_state WHERE node_id IN ({placeholders})", node_ids)
            conn.execute(f"DELETE FROM assignment_events WHERE node_id IN ({placeholders})", node_ids)
            conn.execute(f"DELETE FROM usage_events WHERE node_id IN ({placeholders})", node_ids)
            conn.execute(f"DELETE FROM nodes WHERE id IN ({placeholders})", node_ids)
            return len(node_ids)

    def delete_node(self, node_id: str) -> None:
        with self.lock, self._connect() as conn:
            conn.execute("DELETE FROM test_history WHERE node_id = ?", (node_id,))
            conn.execute("DELETE FROM client_node_state WHERE node_id = ?", (node_id,))
            conn.execute("DELETE FROM assignment_events WHERE node_id = ?", (node_id,))
            conn.execute("DELETE FROM usage_events WHERE node_id = ?", (node_id,))
            conn.execute("DELETE FROM nodes WHERE id = ?", (node_id,))

    def delete_nodes_by_status(self, status: NodeStatus) -> int:
        with self.lock, self._connect() as conn:
            rows = conn.execute("SELECT id FROM nodes WHERE status = ?", (status.value,)).fetchall()
            if not rows:
                return 0
            node_ids = [str(row["id"]) for row in rows]
            placeholders = ",".join("?" for _ in node_ids)
            conn.execute(f"DELETE FROM test_history WHERE node_id IN ({placeholders})", node_ids)
            conn.execute(f"DELETE FROM client_node_state WHERE node_id IN ({placeholders})", node_ids)
            conn.execute(f"DELETE FROM assignment_events WHERE node_id IN ({placeholders})", node_ids)
            conn.execute(f"DELETE FROM usage_events WHERE node_id IN ({placeholders})", node_ids)
            conn.execute(f"DELETE FROM nodes WHERE id IN ({placeholders})", node_ids)
            return len(node_ids)

    def cleanup_database(self) -> dict[str, int]:
        with self.lock, self._connect() as conn:
            test_history = int(conn.execute("SELECT COUNT(*) AS count FROM test_history").fetchone()["count"])
            usage_events = int(conn.execute("SELECT COUNT(*) AS count FROM usage_events").fetchone()["count"])
            assignment_events = int(conn.execute("SELECT COUNT(*) AS count FROM assignment_events").fetchone()["count"])
            orphan_client_rows = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM client_node_state
                    WHERE node_id NOT IN (SELECT id FROM nodes)
                    """
                ).fetchone()["count"]
            )
            system_events = int(conn.execute("SELECT COUNT(*) AS count FROM system_events").fetchone()["count"])
            conn.execute("DELETE FROM test_history")
            conn.execute("DELETE FROM system_events")
            conn.execute("DELETE FROM usage_events")
            conn.execute("DELETE FROM assignment_events")
            conn.execute(
                """
                DELETE FROM client_node_state
                WHERE node_id NOT IN (SELECT id FROM nodes)
                """
            )
        with self._connect() as vacuum_conn:
            vacuum_conn.isolation_level = None
            vacuum_conn.execute("VACUUM")
        return {
            "test_history_removed": test_history,
            "usage_events_removed": usage_events,
            "assignment_events_removed": assignment_events,
            "orphan_client_rows_removed": orphan_client_rows,
            "system_events_removed": system_events,
        }

    def record_assignment(self, client_id: str, node_id: str, port: int) -> AssignmentRecord:
        record = AssignmentRecord(
            id=uuid.uuid4().hex,
            client_id=client_id,
            node_id=node_id,
            port=port,
            assigned_at=utcnow(),
        )
        with self.lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO assignment_events (id, client_id, node_id, port, assigned_at, feedback_status, feedback_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (record.id, record.client_id, record.node_id, record.port, dt_to_str(record.assigned_at), None, None),
            )
        return record

    def has_open_assignment(self, client_id: str, node_id: str, ttl_seconds: int) -> bool:
        cutoff = utcnow() - timedelta(seconds=ttl_seconds)
        with self.lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM assignment_events
                WHERE client_id = ? AND node_id = ? AND feedback_status IS NULL AND assigned_at >= ?
                LIMIT 1
                """,
                (client_id, node_id, cutoff.isoformat()),
            ).fetchone()
            return row is not None

    def count_active_assignments(self, node_id: str, ttl_seconds: int) -> int:
        cutoff = utcnow() - timedelta(seconds=ttl_seconds)
        with self.lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS count FROM assignment_events
                WHERE node_id = ? AND feedback_status IS NULL AND assigned_at >= ?
                """,
                (node_id, cutoff.isoformat()),
            ).fetchone()
            return int(row["count"])

    def count_recent_usage(self, node_id: str, seconds: int) -> int:
        cutoff = utcnow() - timedelta(seconds=seconds)
        with self.lock, self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM usage_events WHERE node_id = ? AND created_at >= ?",
                (node_id, cutoff.isoformat()),
            ).fetchone()
            return int(row["count"])

    def count_recent_client_usage(self, client_id: str, node_id: str, seconds: int) -> int:
        cutoff = utcnow() - timedelta(seconds=seconds)
        with self.lock, self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM usage_events WHERE client_id = ? AND node_id = ? AND created_at >= ?",
                (client_id, node_id, cutoff.isoformat()),
            ).fetchone()
            return int(row["count"])

    def get_client_node_state(self, client_id: str, node_id: str) -> ClientNodeStateRecord:
        with self.lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM client_node_state WHERE client_id = ? AND node_id = ?",
                (client_id, node_id),
            ).fetchone()
            if row:
                return self._row_to_client_state(row)
        return ClientNodeStateRecord(client_id=client_id, node_id=node_id)

    def save_client_node_state(self, state: ClientNodeStateRecord) -> None:
        with self.lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO client_node_state (
                  client_id, node_id, state, fail_streak, rate_limit_streak, cooldown_until,
                  usage_count, success_count, broken_count, rate_limited_count, recent_usage_score,
                  success_rate_ewma, last_assigned_at, last_feedback_at, last_failure_at, last_success_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(client_id, node_id) DO UPDATE SET
                  state=excluded.state,
                  fail_streak=excluded.fail_streak,
                  rate_limit_streak=excluded.rate_limit_streak,
                  cooldown_until=excluded.cooldown_until,
                  usage_count=excluded.usage_count,
                  success_count=excluded.success_count,
                  broken_count=excluded.broken_count,
                  rate_limited_count=excluded.rate_limited_count,
                  recent_usage_score=excluded.recent_usage_score,
                  success_rate_ewma=excluded.success_rate_ewma,
                  last_assigned_at=excluded.last_assigned_at,
                  last_feedback_at=excluded.last_feedback_at,
                  last_failure_at=excluded.last_failure_at,
                  last_success_at=excluded.last_success_at
                """,
                (
                    state.client_id,
                    state.node_id,
                    state.state.value,
                    state.fail_streak,
                    state.rate_limit_streak,
                    dt_to_str(state.cooldown_until),
                    state.usage_count,
                    state.success_count,
                    state.broken_count,
                    state.rate_limited_count,
                    state.recent_usage_score,
                    state.success_rate_ewma,
                    dt_to_str(state.last_assigned_at),
                    dt_to_str(state.last_feedback_at),
                    dt_to_str(state.last_failure_at),
                    dt_to_str(state.last_success_at),
                ),
            )

    def append_usage_event(self, client_id: str, node_id: str, event_type: str) -> None:
        with self.lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO usage_events (id, client_id, node_id, event_type, created_at) VALUES (?, ?, ?, ?, ?)",
                (uuid.uuid4().hex, client_id, node_id, event_type, dt_to_str(utcnow())),
            )

    def mark_assignment_feedback(self, client_id: str, node_id: str, status: str) -> None:
        now = dt_to_str(utcnow())
        with self.lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE assignment_events
                SET feedback_status = ?, feedback_at = ?
                WHERE id = (
                  SELECT id FROM assignment_events
                  WHERE client_id = ? AND node_id = ? AND feedback_status IS NULL
                  ORDER BY assigned_at DESC LIMIT 1
                )
                """,
                (status, now, client_id, node_id),
            )

    def _row_to_node(self, row: sqlite3.Row) -> NodeRecord:
        return NodeRecord(
            id=row["id"],
            config_hash=row["config_hash"],
            raw_config=row["raw_config"],
            normalized_config=json.loads(row["normalized_config"]),
            source_subs=json.loads(row["source_subs"]),
            status=NodeStatus(row["status"]),
            main_port=row["main_port"],
            relay_delay_ms=row["relay_delay_ms"],
            download_kbps=row["download_kbps"],
            exit_ip=row["exit_ip"] or "",
            exit_hostname=row["exit_hostname"] or "",
            exit_city=row["exit_city"] or "",
            exit_region=row["exit_region"] or "",
            exit_country=row["exit_country"] or "",
            exit_loc=row["exit_loc"] or "",
            exit_org=row["exit_org"] or "",
            exit_postal=row["exit_postal"] or "",
            exit_timezone=row["exit_timezone"] or "",
            exit_info=json.loads(row["exit_info_json"] or "{}"),
            health_success_ewma=row["health_success_ewma"],
            consecutive_relay_failures=row["consecutive_relay_failures"],
            consecutive_relay_successes=row["consecutive_relay_successes"] or 0,
            created_at=str_to_dt(row["created_at"]),
            updated_at=str_to_dt(row["updated_at"]),
            last_health_check_at=str_to_dt(row["last_health_check_at"]),
            last_test_at=str_to_dt(row["last_test_at"]),
            exit_info_fetched_at=str_to_dt(row["exit_info_fetched_at"]),
            dead_until=str_to_dt(row["dead_until"]),
        )

    def _row_to_client_state(self, row: sqlite3.Row) -> ClientNodeStateRecord:
        return ClientNodeStateRecord(
            client_id=row["client_id"],
            node_id=row["node_id"],
            state=ClientCircuitState(row["state"]),
            fail_streak=row["fail_streak"],
            rate_limit_streak=row["rate_limit_streak"],
            cooldown_until=str_to_dt(row["cooldown_until"]),
            usage_count=row["usage_count"],
            success_count=row["success_count"],
            broken_count=row["broken_count"],
            rate_limited_count=row["rate_limited_count"],
            recent_usage_score=row["recent_usage_score"],
            success_rate_ewma=row["success_rate_ewma"],
            last_assigned_at=str_to_dt(row["last_assigned_at"]),
            last_feedback_at=str_to_dt(row["last_feedback_at"]),
            last_failure_at=str_to_dt(row["last_failure_at"]),
            last_success_at=str_to_dt(row["last_success_at"]),
        )

    def _row_to_test_history(self, row: sqlite3.Row) -> TestHistoryRecord:
        return TestHistoryRecord(
            id=row["id"],
            node_id=row["node_id"],
            test_kind=row["test_kind"],
            trigger=row["trigger"],
            started_at=str_to_dt(row["started_at"]) or utcnow(),
            finished_at=str_to_dt(row["finished_at"]) or utcnow(),
            network_online=bool(row["network_online"]),
            ok=bool(row["ok"]),
            latency_ms=row["latency_ms"],
            download_kbps=row["download_kbps"],
            error=row["error"],
            status_before=row["status_before"],
            status_after=row["status_after"],
            details=json.loads(row["details_json"] or "{}"),
        )

    def _row_to_system_event(self, row: sqlite3.Row) -> SystemEventRecord:
        return SystemEventRecord(
            id=row["id"],
            created_at=str_to_dt(row["created_at"]) or utcnow(),
            level=row["level"],
            component=row["component"],
            event=row["event"],
            message=row["message"],
            details=json.loads(row["details_json"] or "{}"),
        )
