
"""Tests for source validation and transformation."""

from datetime import datetime, timezone

import pytest

from api_ingestion.transform import (
    is_pull_request,
    transform_issue,
)
from api_ingestion.validation import RecordValidationError


def sample_issue():
    return {
        "id": 1001,
        "number": 25,
        "title": "Example issue",
        "state": "open",
        "user": {"login": "tester"},
        "labels": [
            {"name": "bug"},
            {"name": "priority"},
        ],
        "created_at": "2026-10-01T10:00:00Z",
        "updated_at": "2026-10-02T10:00:00Z",
        "closed_at": None,
    }


def transform(record):
    return transform_issue(
        record=record,
        repository="microsoft/vscode",
        ingested_at=datetime(
            2026, 10, 3, tzinfo=timezone.utc
        ),
        run_id="test-run",
    )


def test_valid_issue_transformation():
    result = transform(sample_issue())

    assert result["issue_id"] == 1001
    assert result["author_login"] == "tester"
    assert result["labels_json"] == '["bug", "priority"]'
    assert result["updated_at"].tzinfo is not None


def test_pull_request_is_identified():
    record = sample_issue()
    record["pull_request"] = {
        "url": "https://api.github.com/example"
    }

    assert is_pull_request(record)


def test_missing_issue_id_is_rejected():
    record = sample_issue()
    del record["id"]

    with pytest.raises(RecordValidationError):
        transform(record)


def test_invalid_timestamp_is_rejected():
    record = sample_issue()
    record["updated_at"] = "not-a-date"

    with pytest.raises(RecordValidationError):
        transform(record)


def test_nested_labels_are_normalized():
    record = sample_issue()
    record["labels"] = [
        {"name": "priority"},
        {"name": "bug"},
        {"name": "bug"},
    ]

    result = transform(record)

    assert result["labels_json"] == '["bug", "priority"]'


def test_inconsistent_dates_are_rejected():
    record = sample_issue()
    record["updated_at"] = "2026-09-01T10:00:00Z"

    with pytest.raises(RecordValidationError):
        transform(record)
