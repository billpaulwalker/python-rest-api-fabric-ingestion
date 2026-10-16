
"""Tests for Silver Parquet processing and reruns."""

import json

import pyarrow.parquet as pq
import pytest

from api_ingestion.silver import build_silver


def sample_issue(
    issue_id=1001,
    title="Original title",
    updated_at="2026-10-02T10:00:00Z",
):
    return {
        "id": issue_id,
        "number": 25,
        "title": title,
        "state": "open",
        "user": {"login": "tester"},
        "labels": [{"name": "bug"}],
        "created_at": "2026-10-01T10:00:00Z",
        "updated_at": updated_at,
        "closed_at": None,
    }


def config_for(tmp_path):
    return {
        "api": {
            "owner": "microsoft",
            "repository": "vscode",
        },
        "storage": {
            "bronze_directory": str(tmp_path / "bronze"),
            "silver_file": str(
                tmp_path / "silver" / "issues.parquet"
            ),
            "quarantine_file": str(
                tmp_path / "quarantine" / "rejected.jsonl"
            ),
        },
    }


def write_bronze(tmp_path, filename, records):
    directory = tmp_path / "bronze"
    directory.mkdir(exist_ok=True)

    path = directory / filename
    path.write_text(json.dumps(records), encoding="utf-8")
    return path


def read_silver(config):
    return pq.read_table(
        config["storage"]["silver_file"]
    ).to_pylist()


def test_bronze_to_silver(tmp_path):
    config = config_for(tmp_path)

    write_bronze(
        tmp_path,
        "github_issues_run1.json",
        [sample_issue()],
    )

    result = build_silver(config)
    rows = read_silver(config)

    assert result["status"] == "SUCCESS"
    assert result["silver_records"] == 1
    assert rows[0]["issue_id"] == 1001


def test_latest_update_wins(tmp_path):
    config = config_for(tmp_path)

    old = sample_issue(
        title="Original title",
        updated_at="2026-10-02T10:00:00Z",
    )

    new = sample_issue(
        title="Updated title",
        updated_at="2026-10-03T10:00:00Z",
    )

    write_bronze(
        tmp_path,
        "github_issues_run1.json",
        [old],
    )

    write_bronze(
        tmp_path,
        "github_issues_run2.json",
        [new],
    )

    build_silver(config)
    rows = read_silver(config)

    assert len(rows) == 1
    assert rows[0]["title"] == "Updated title"


def test_pull_requests_are_excluded(tmp_path):
    config = config_for(tmp_path)

    issue = sample_issue(1001)
    pull_request = sample_issue(2002)
    pull_request["pull_request"] = {"url": "example"}

    write_bronze(
        tmp_path,
        "github_issues_run1.json",
        [issue, pull_request],
    )

    result = build_silver(config)

    assert result["pull_requests_skipped"] == 1
    assert len(read_silver(config)) == 1


def test_invalid_record_is_quarantined(tmp_path):
    config = config_for(tmp_path)

    bad = sample_issue(2002)
    bad["id"] = None

    write_bronze(
        tmp_path,
        "github_issues_run1.json",
        [sample_issue(), bad],
    )

    result = build_silver(config)

    assert result["rejected_records"] == 1
    assert result["silver_records"] == 1

    quarantine = config["storage"]["quarantine_file"]

    with open(quarantine, encoding="utf-8") as file:
        rejected = [json.loads(line) for line in file]

    assert len(rejected) == 1
    assert "issue_id" not in rejected[0]["record"]
    assert rejected[0]["record"]["id"] is None


def test_rerun_does_not_duplicate_rows(tmp_path):
    config = config_for(tmp_path)

    write_bronze(
        tmp_path,
        "github_issues_run1.json",
        [sample_issue()],
    )

    build_silver(config)
    first = read_silver(config)

    build_silver(config)
    second = read_silver(config)

    assert first == second
    assert len(second) == 1


def test_corrupt_bronze_does_not_replace_silver(tmp_path):
    config = config_for(tmp_path)

    write_bronze(
        tmp_path,
        "github_issues_run1.json",
        [sample_issue()],
    )

    build_silver(config)
    first = read_silver(config)

    bad_path = tmp_path / "bronze" / "github_issues_bad.json"
    bad_path.write_text("{invalid json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        build_silver(config)

    assert read_silver(config) == first
