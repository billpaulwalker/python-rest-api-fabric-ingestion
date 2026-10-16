
"""Normalize GitHub issue JSON into Silver records."""

import json

from datetime import datetime

from .validation import (
    parse_utc_timestamp,
    validate_source_issue,
)


def is_pull_request(record: dict) -> bool:
    """GitHub issue results also include pull requests."""

    return isinstance(record, dict) and "pull_request" in record


def transform_issue(
    record: dict,
    repository: str,
    ingested_at: datetime,
    run_id: str,
) -> dict:
    """Validate and normalize a single GitHub issue."""

    validate_source_issue(record)

    user = record.get("user") or {}
    labels = record.get("labels", [])

    label_names = sorted({
        label["name"].strip()
        for label in labels
    })

    return {
        "issue_id": record["id"],
        "issue_number": record["number"],
        "repository": repository,
        "title": record["title"].strip(),
        "state": record["state"].lower(),
        "author_login": user.get("login"),
        "labels_json": json.dumps(label_names),
        "created_at": parse_utc_timestamp(
            record["created_at"], "created_at"
        ),
        "updated_at": parse_utc_timestamp(
            record["updated_at"], "updated_at"
        ),
        "closed_at": parse_utc_timestamp(
            record.get("closed_at"),
            "closed_at",
            required=False,
        ),
        "ingested_at": ingested_at,
        "source_run_id": run_id,
    }
