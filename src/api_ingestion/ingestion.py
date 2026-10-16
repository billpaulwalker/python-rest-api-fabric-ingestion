
"""Coordinate incremental GitHub issue extraction."""

import json
import logging
import os
import uuid

from datetime import datetime, timedelta, timezone
from pathlib import Path

from .client import GitHubAPIClient
from .state import load_state, save_state


logger = logging.getLogger(__name__)


def parse_timestamp(value: str) -> datetime:
    """Parse a GitHub UTC timestamp."""

    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    """Format a UTC timestamp for GitHub's since parameter."""

    return value.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def write_bronze(path: Path, records: list) -> None:
    """Write JSON through a temporary file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")

    with temporary.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary, path)


def run_ingestion(config: dict, token: str) -> Path:
    """Execute incremental API extraction and persist state."""

    run_id = str(uuid.uuid4())
    run_started = datetime.now(timezone.utc)

    api = config["api"]
    storage = config["storage"]

    source_key = f"{api['owner']}_{api['repository']}"

    bronze_directory = Path(storage["bronze_directory"])
    state_directory = Path(storage["state_directory"])

    state_path = state_directory / f"{source_key}.json"
    previous_state = load_state(state_path)

    previous_watermark = previous_state.get("watermark")

    if previous_watermark:
        since_date = parse_timestamp(previous_watermark) - timedelta(
            minutes=api["watermark_overlap_minutes"]
        )
    else:
        since_date = run_started - timedelta(
            days=api["initial_lookback_days"]
        )

    since = format_timestamp(since_date)

    logger.info(
        "Ingestion started",
        extra={
            "run_id": run_id,
            "stage": "extract",
            "status": "STARTED",
        },
    )

    logger.info(
        "Extraction window | since=%s | run_cutoff=%s",
        since,
        format_timestamp(run_started),
    )

    client = GitHubAPIClient(
        base_url=api["base_url"],
        token=token,
        timeout=api["timeout_seconds"],
        api_version=api["api_version"],
        max_attempts=api["max_attempts"],
        initial_backoff=api["initial_backoff_seconds"],
        max_rate_limit_wait=api["max_rate_limit_wait_seconds"],
    )

    all_records = []
    page_count = 0

    try:
        user = client.verify_authentication()

        logger.info(
            "Authentication successful | user=%s",
            user["login"],
        )

        for page in client.get_issues(
            owner=api["owner"],
            repository=api["repository"],
            per_page=api["per_page"],
            state=api["state"],
            since=since,
            max_pages=api["max_pages"],
        ):
            page_count += 1

            # Do not advance the watermark using records
            # updated after this execution began.
            for record in page:
                updated_at = parse_timestamp(
                    record["updated_at"]
                )

                if updated_at <= run_started:
                    all_records.append(record)

        # Preserve all returned issue-like source records.
        # Phase 3 will exclude pull requests from Silver.
        output_file = bronze_directory / (
            f"github_issues_{run_id}.json"
        )

        write_bronze(output_file, all_records)

        # The source watermark only moves after a complete
        # successful extraction and durable Bronze write.
        new_watermark = previous_watermark

        if all_records:
            maximum = max(
                parse_timestamp(r["updated_at"])
                for r in all_records
            )

            if (
                not previous_watermark
                or maximum > parse_timestamp(previous_watermark)
            ):
                new_watermark = format_timestamp(maximum)

        # An empty initial run still establishes a conservative
        # checkpoint after a successful complete extraction.
        if not new_watermark:
            new_watermark = since

        new_state = {
            "source": f"{api['owner']}/{api['repository']}",
            "watermark": new_watermark,
            "last_run_id": run_id,
            "last_success_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "last_bronze_file": str(output_file),
            "last_record_count": len(all_records),
            "last_page_count": page_count,
        }

        save_state(state_path, new_state)

        duration = (
            datetime.now(timezone.utc) - run_started
        ).total_seconds()

        logger.info(
            "Ingestion completed",
            extra={
                "run_id": run_id,
                "stage": "complete",
                "status": "SUCCESS",
                "records": len(all_records),
                "pages": page_count,
                "requests": client.request_count,
                "retries": client.retry_count,
                "duration_seconds": round(duration, 2),
            },
        )

        return output_file

    except Exception:
        logger.exception(
            "Ingestion failed",
            extra={
                "run_id": run_id,
                "stage": "extract",
                "status": "FAILED",
                "pages": page_count,
                "requests": client.request_count,
                "retries": client.retry_count,
            },
        )
        raise

    finally:
        client.close()
