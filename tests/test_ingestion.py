
"""Tests for ingestion checkpoints and safe replay."""

import json

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from api_ingestion.ingestion import run_ingestion


def make_config(tmp_path):
    return {
        "api": {
            "base_url": "https://api.github.com",
            "owner": "microsoft",
            "repository": "vscode",
            "timeout_seconds": 5,
            "api_version": "2026-03-10",
            "per_page": 100,
            "state": "all",
            "max_attempts": 2,
            "initial_backoff_seconds": 0,
            "max_rate_limit_wait_seconds": 120,
            "max_pages": 10,
            "initial_lookback_days": 7,
            "watermark_overlap_minutes": 5,
        },
        "storage": {
            "bronze_directory": str(tmp_path / "bronze"),
            "state_directory": str(tmp_path / "state"),
        },
    }


def make_issue(issue_id=1):
    updated = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "id": issue_id,
        "updated_at": updated,
        "title": "Test issue",
    }


def test_successful_run_saves_checkpoint(tmp_path):
    config = make_config(tmp_path)
    issue = make_issue()

    with patch(
        "api_ingestion.ingestion.GitHubAPIClient"
    ) as mock_class:
        client = mock_class.return_value
        client.verify_authentication.return_value = {
            "login": "tester"
        }
        client.get_issues.return_value = [[issue]]
        client.request_count = 2
        client.retry_count = 0

        output = run_ingestion(config, "fake-token")

    assert output.exists()

    records = json.loads(output.read_text())
    assert records[0]["id"] == 1

    state_path = (
        tmp_path / "state" / "microsoft_vscode.json"
    )

    state = json.loads(state_path.read_text())
    assert state["watermark"] == issue["updated_at"]


def test_failed_run_does_not_advance_checkpoint(tmp_path):
    config = make_config(tmp_path)

    state_path = (
        tmp_path / "state" / "microsoft_vscode.json"
    )
    state_path.parent.mkdir(parents=True)

    previous = {
        "watermark": "2026-10-10T12:00:00Z",
        "last_run_id": "previous-success",
    }

    state_path.write_text(json.dumps(previous))

    with patch(
        "api_ingestion.ingestion.GitHubAPIClient"
    ) as mock_class:
        client = mock_class.return_value
        client.verify_authentication.return_value = {
            "login": "tester"
        }
        client.get_issues.side_effect = RuntimeError(
            "Simulated interrupted extraction"
        )
        client.request_count = 1
        client.retry_count = 0

        with pytest.raises(RuntimeError):
            run_ingestion(config, "fake-token")

    after = json.loads(state_path.read_text())

    assert after == previous


def test_rerun_uses_watermark_overlap(tmp_path):
    config = make_config(tmp_path)
    issue = make_issue()

    with patch(
        "api_ingestion.ingestion.GitHubAPIClient"
    ) as mock_class:
        client = mock_class.return_value
        client.verify_authentication.return_value = {
            "login": "tester"
        }
        client.get_issues.return_value = [[issue]]
        client.request_count = 2
        client.retry_count = 0

        run_ingestion(config, "fake-token")
        run_ingestion(config, "fake-token")

        second_call = client.get_issues.call_args_list[1]
        since = second_call.kwargs["since"]

    expected = (
        datetime.fromisoformat(
            issue["updated_at"].replace("Z", "+00:00")
        ) - timedelta(minutes=5)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    assert since == expected
