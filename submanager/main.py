from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path

from submanager.api.server import ApiServer
from submanager.config.loader import ConfigLoader
from submanager.core.app import OrchestratorApp
from submanager.utils.logging import configure_logging, get_logger


logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Subscription manager service")
    parser.add_argument("--config", default="config/config.yml", help="Path to YAML config")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    args = parse_args()
    settings = ConfigLoader().load(args.config)
    work_dir = Path.cwd()
    Path(settings.database.path).parent.mkdir(parents=True, exist_ok=True)

    app = OrchestratorApp(settings, work_dir)
    server = ApiServer(app)

    def shutdown_handler(signum, frame) -> None:  # type: ignore[no-untyped-def]
        logger.info("Received signal %s, shutting down", signum)
        server.shutdown()
        app.stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    app.start()
    logger.info("API listening on %s:%s", settings.api.host, settings.api.port)
    try:
        server.serve_forever()
    finally:
        app.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
