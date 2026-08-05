from __future__ import annotations

import unittest

from submanager.api.pages import render_client_status_html, render_dashboard_html


class DashboardPageTests(unittest.TestCase):
    def test_dashboard_has_server_side_pagination_controls(self) -> None:
        html = render_dashboard_html()

        self.assertIn('id="page-prev"', html)
        self.assertIn('id="page-next"', html)
        self.assertIn('id="page-size"', html)
        self.assertIn("params.set(\"page\"", html)
        self.assertIn("params.set(\"page_size\"", html)

    def test_client_dashboard_is_paginated_and_fetches_configs_on_demand(self) -> None:
        html = render_client_status_html()

        self.assertIn('id="client-page-prev"', html)
        self.assertIn('id="client-page-next"', html)
        self.assertIn('id="client-page-size"', html)
        self.assertIn('data-client-action="copy-config"', html)
        self.assertIn("/config`,", html)
        self.assertNotIn("normalized_config: node.normalized_config", html)


if __name__ == "__main__":
    unittest.main()
