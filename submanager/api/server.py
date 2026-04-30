from __future__ import annotations

import json
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from submanager.api.pages import (
    render_client_status_html,
    render_dashboard_html,
    render_diag_html,
    render_docs_html,
    render_logs_html,
    render_manual_import_html,
    render_node_history_html,
)
from submanager.core.app import OrchestratorApp
from submanager.core.models import FeedbackInput


class ApiServer:
    def __init__(self, app: OrchestratorApp) -> None:
        self.app = app
        self.httpd = ThreadingHTTPServer((app.settings.api.host, app.settings.api.port), self._make_handler())

    def _make_handler(self):
        app = self.app
        node_history_re = re.compile(r"^/api/v1/nodes/([a-f0-9]+)/history$")
        node_test_re = re.compile(r"^/api/v1/nodes/([a-f0-9]+)/test$")

        class Handler(BaseHTTPRequestHandler):
            def do_HEAD(self) -> None:
                parsed = urlsplit(self.path)
                if parsed.path in {"/", "/clients", "/diag", "/docs", "/logs", "/history", "/manual-import", "/health", "/api/v1/nodes", "/api/v1/clients", "/api/v1/client-status", "/api/v1/network", "/api/v1/vip", "/api/v1/logs"} or node_history_re.match(parsed.path):
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                else:
                    self.send_response(HTTPStatus.NOT_FOUND)
                    self.send_header("Content-Length", "0")
                    self.end_headers()

            def do_GET(self) -> None:
                parsed = urlsplit(self.path)
                path = parsed.path
                query = parse_qs(parsed.query)
                if path == "/":
                    self._html(HTTPStatus.OK, render_dashboard_html())
                elif path == "/clients":
                    self._html(HTTPStatus.OK, render_client_status_html())
                elif path == "/diag":
                    self._html(HTTPStatus.OK, render_diag_html())
                elif path == "/docs":
                    self._html(HTTPStatus.OK, render_docs_html())
                elif path == "/logs":
                    self._html(HTTPStatus.OK, render_logs_html())
                elif path == "/history":
                    self._html(HTTPStatus.OK, render_node_history_html())
                elif path == "/manual-import":
                    self._html(HTTPStatus.OK, render_manual_import_html())
                elif path == "/health":
                    self._json(HTTPStatus.OK, {"ok": True})
                elif path == "/api/v1/nodes":
                    self._json(HTTPStatus.OK, app.get_dashboard_payload())
                elif path == "/api/v1/network":
                    self._json(HTTPStatus.OK, app.get_network_status_payload())
                elif path == "/api/v1/vip":
                    self._json(HTTPStatus.OK, app.get_vip_status_payload())
                elif path == "/api/v1/clients":
                    self._json(HTTPStatus.OK, {"clients": app.store.list_client_ids()})
                elif path == "/api/v1/client-status":
                    client = (query.get("client", [""])[0] or "").strip()
                    self._json(HTTPStatus.OK, app.get_client_dashboard_payload(client))
                elif path == "/api/v1/logs":
                    limit = int((query.get("limit", ["200"])[0] or "200"))
                    component = (query.get("component", [""])[0] or "").strip()
                    level = (query.get("level", [""])[0] or "").strip()
                    self._json(HTTPStatus.OK, app.get_system_logs_payload(limit=max(1, min(limit, 1000)), component=component, level=level))
                elif node_history_re.match(path):
                    node_id = node_history_re.match(path).group(1)  # type: ignore[union-attr]
                    limit = int((query.get("limit", ["50"])[0] or "50"))
                    try:
                        self._json(HTTPStatus.OK, app.get_node_test_history_payload(node_id, limit=max(1, min(limit, 200))))
                    except KeyError:
                        self._json(HTTPStatus.NOT_FOUND, {"error": "NODE_NOT_FOUND"})
                else:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})

            def do_POST(self) -> None:
                if self.path == "/api/v1/best":
                    self._handle_best()
                elif self.path == "/api/v1/feedback":
                    self._handle_feedback()
                elif self.path == "/api/v1/manual-import":
                    self._handle_manual_import()
                elif self.path == "/api/v1/nodes/dead/clear":
                    self._handle_clear_dead()
                elif self.path == "/api/v1/subscriptions/reload":
                    self._handle_reload_subscriptions()
                elif self.path == "/api/v1/db/cleanup":
                    self._handle_db_cleanup()
                elif node_test_re.match(urlsplit(self.path).path):
                    self._handle_manual_test(urlsplit(self.path).path)
                else:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})

            def _handle_best(self) -> None:
                try:
                    payload = self._read_json()
                except Exception:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": "INVALID_JSON"})
                    return
                client = payload.get("client", "").strip()
                if not client:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": "INVALID_CLIENT"})
                    return
                decision = app.get_best_node(client)
                if decision is None:
                    self._json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {
                            "error": "NO_AVAILABLE_NODE",
                            "message": "No healthy node is currently available for this client.",
                        },
                    )
                    return
                self._json(
                    HTTPStatus.OK,
                    {
                        "node_id": decision.node_id,
                        "port": decision.port,
                        "client": client,
                        "assignment_id": decision.assignment_id,
                        "relay_delay_ms": decision.relay_delay_ms,
                        "expires_in_seconds": decision.expires_in_seconds,
                    },
                )

            def _handle_feedback(self) -> None:
                try:
                    payload = self._read_json()
                except Exception:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": "INVALID_JSON"})
                    return
                client = payload.get("client", "").strip()
                node_id = payload.get("node_id", "").strip()
                status = payload.get("status", "").strip()
                if not client or not node_id or status not in {"used", "broken", "rate_limited"}:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": "INVALID_FEEDBACK"})
                    return
                try:
                    app.handle_feedback(FeedbackInput(client=client, node_id=node_id, status=status))
                except KeyError:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": "NODE_NOT_FOUND"})
                    return
                self._json(HTTPStatus.OK, {"ok": True})

            def _handle_manual_test(self, path: str) -> None:
                match = node_test_re.match(path)
                if not match:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
                    return
                node_id = match.group(1)
                try:
                    payload = app.trigger_manual_test(node_id)
                except KeyError:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "NODE_NOT_FOUND"})
                    return
                except RuntimeError as exc:
                    self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "TEST_UNAVAILABLE", "message": str(exc)})
                    return
                self._json(HTTPStatus.OK, payload)

            def _handle_manual_import(self) -> None:
                try:
                    payload = self._read_json()
                except Exception:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": "INVALID_JSON"})
                    return
                raw_text = str(payload.get("configs", ""))
                result = app.import_manual_configs(raw_text)
                self._json(HTTPStatus.OK, result)

            def _handle_clear_dead(self) -> None:
                self._json(HTTPStatus.OK, app.clear_dead_pool())

            def _handle_reload_subscriptions(self) -> None:
                payload = app.reload_subscriptions_now()
                status = HTTPStatus.OK if payload.get("ok", False) else HTTPStatus.SERVICE_UNAVAILABLE
                self._json(status, payload)

            def _handle_db_cleanup(self) -> None:
                self._json(HTTPStatus.OK, app.cleanup_database())

            def _read_json(self) -> dict[str, Any]:
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length else b"{}"
                return json.loads(raw.decode("utf-8"))

            def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
                encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                try:
                    self.wfile.write(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    return

            def _html(self, status: HTTPStatus, payload: str) -> None:
                encoded = payload.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                try:
                    self.wfile.write(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    return

            def log_message(self, format: str, *args) -> None:
                return

        return Handler

    def serve_forever(self) -> None:
        self.httpd.serve_forever()

    def shutdown(self) -> None:
        self.httpd.shutdown()
