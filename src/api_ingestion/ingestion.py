
"""Run the initial GitHub issue ingestion."""

import json
import logging
import uuid

from datetime import datetime, timezone
from pathlib import Path

from .client import GitHubAPIClient


logger = logging.getLogger(__name__)


def run_ingestion(config: dict, token: str) -> Path:
    """Fetch GitHub issues and write a raw Bronze JSON file."""

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    api_config = config["api"]
    storage_config = config["storage"]

    bronze_path = Path(
        storage_config["bronze_directory"]
    )
    bronze_path.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Starting ingestion | run_id=%s",
        run_id,
    )

    client = GitHubAPIClient(
        base_url=api_config["base_url"],
        token=token,
        timeout=api_config["timeout_seconds"],
        api_version=api_config["api_version"],
    )

    try:
        user = client.verify_authentication()

        logger.info(
            "Authentication successful | user=%s",
            user["login"],
        )

        records = client.get_issues(
            owner=api_config["owner"],
            repository=api_config["repository"],
            per_page=api_config["per_page"],
            state=api_config["state"],
        )

        logger.info(
            "API records retrieved | count=%d",
            len(records),
        )

        # Save the complete first-page API response.
        output_file = bronze_path / f"github_issues_{run_id}.json"

        with output_file.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                records,
                file,
                indent=2,
                ensure_ascii=False,
            )

        duration = (
            datetime.now(timezone.utc) - started_at
        ).total_seconds()

        logger.info(
            "Ingestion successful | run_id=%s | "
            "records=%d | duration=%.2fs | file=%s",
            run_id,
            len(records),
            duration,
            output_file,
        )

        return output_file

    except Exception:
        logger.exception(
            "Ingestion failed | run_id=%s",
            run_id,
        )
        raise

    finally:
        client.close()
