
"""Structured JSON logging configuration."""

import json
import logging

from datetime import datetime, timezone
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """Format log messages as JSON Lines."""

    def format(self, record):
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for field in (
            "run_id",
            "stage",
            "status",
            "records",
            "pages",
            "requests",
            "retries",
            "duration_seconds",
        ):
            if hasattr(record, field):
                payload[field] = getattr(record, field)

        if record.exc_info:
            payload["exception"] = self.formatException(
                record.exc_info
            )

        return json.dumps(payload, default=str)


def configure_logging(
    directory: str,
    level: str = "INFO",
) -> None:
    """Configure console and JSONL file logging."""

    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level.upper())

    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()

    console = logging.StreamHandler()
    console.setFormatter(JSONFormatter())

    file_handler = logging.FileHandler(
        path / "ingestion.jsonl",
        encoding="utf-8",
    )
    file_handler.setFormatter(JSONFormatter())

    root.addHandler(console)
    root.addHandler(file_handler)
