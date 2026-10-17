"""Tests for the Fabric Silver upsert and recovery policy."""

from datetime import datetime, timedelta, timezone

from api_ingestion.fabric_policy import (
    CONTENT_COLUMNS,
    candidate_watermark,
    classify_bronze_runs,
    merge_update_condition,
    select_latest_versions,
    should_replace,
)


T0 = datetime(2026, 10, 2, 10, 0, tzinfo=timezone.utc)


def silver_row(
    issue_id=1001,
    title="Original title",
    updated_at=T0,
    ingested_at=T0 + timedelta(hours=1),
    run_id="run-a",
):
    return {
        "issue_id": issue_id,
        "issue_number": 25,
        "repository": "microsoft/vscode",
        "title": title,
        "state": "open",
        "author_login": "tester",
        "labels_json": '["bug"]',
        "created_at": T0 - timedelta(days=1),
        "updated_at": updated_at,
        "closed_at": None,
        "ingested_at": ingested_at,
        "source_run_id": run_id,
    }


LATER = T0 + timedelta(hours=2)
EARLIER = T0 + timedelta(minutes=30)


def test_insert_when_target_missing():
    assert should_replace(silver_row(), None)


def test_newer_updated_at_replaces():
    existing = silver_row()
    incoming = silver_row(updated_at=T0 + timedelta(seconds=1))

    assert should_replace(incoming, existing)


def test_older_updated_at_never_replaces():
    existing = silver_row()
    incoming = silver_row(
        title="Stale",
        updated_at=T0 - timedelta(seconds=1),
        ingested_at=LATER,
    )

    assert not should_replace(incoming, existing)


def test_same_timestamp_identical_content_is_noop():
    existing = silver_row()
    incoming = silver_row(ingested_at=LATER, run_id="run-b")

    assert not should_replace(incoming, existing)


def test_same_timestamp_later_observation_with_changes_replaces():
    existing = silver_row()
    incoming = silver_row(
        title="Edited in same second",
        ingested_at=LATER,
        run_id="run-b",
    )

    assert should_replace(incoming, existing)


def test_same_timestamp_earlier_observation_does_not_roll_back():
    existing = silver_row()
    incoming = silver_row(
        title="Older observation",
        ingested_at=EARLIER,
        run_id="run-old",
    )

    assert not should_replace(incoming, existing)


def test_same_timestamp_same_observation_does_not_replace():
    existing = silver_row()
    incoming = silver_row(title="Different")

    assert not should_replace(incoming, existing)


def test_merge_condition_compares_every_content_column():
    condition = merge_update_condition("s", "t")

    assert "s.updated_at > t.updated_at" in condition
    assert "s.ingested_at > t.ingested_at" in condition

    for column in CONTENT_COLUMNS:
        assert f"s.{column} <=> t.{column}" in condition


def test_batch_tie_keeps_later_fetched_record():
    first = silver_row(title="First page")
    second = silver_row(title="Later page")

    [selected] = select_latest_versions([first, second])

    assert selected["title"] == "Later page"


def test_batch_keeps_newest_regardless_of_position():
    newer = silver_row(
        title="Newer",
        updated_at=T0 + timedelta(minutes=1),
    )
    older = silver_row(title="Older")

    [selected] = select_latest_versions([newer, older])

    assert selected["title"] == "Newer"


def test_batch_output_is_one_row_per_issue_sorted():
    records = [
        silver_row(issue_id=3),
        silver_row(issue_id=1),
        silver_row(issue_id=3),
    ]

    selected = select_latest_versions(records)

    assert [r["issue_id"] for r in selected] == [1, 3]


def test_candidate_watermark():
    records = [
        silver_row(issue_id=1, updated_at=T0),
        silver_row(issue_id=2, updated_at=LATER),
    ]

    assert candidate_watermark(records) == LATER
    assert candidate_watermark([]) is None


def run_row(run_id, status, started_at, recovered_from=None):
    return {
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "recovered_from_run_id": recovered_from,
    }


def test_classify_bronze_runs():
    rows = [
        run_row("ok", "SUCCESS", T0),
        run_row("failed", "FAILED", T0),
        run_row("recovered", "FAILED", T0),
        run_row("fix", "SUCCESS", LATER, recovered_from="recovered"),
        run_row("killed", "INTERRUPTED", T0),
    ]

    result = classify_bronze_runs(
        ["ok", "failed", "recovered", "killed", "unknown"],
        rows,
    )

    assert result == {
        "failed": "FAILED",
        "killed": "INTERRUPTED",
        "unknown": "UNTRACKED",
    }


def test_failed_recovery_attempt_leaves_file_unrecovered():
    rows = [
        run_row("orig", "FAILED", T0),
        run_row("retry", "FAILED", LATER, recovered_from="orig"),
    ]

    assert classify_bronze_runs(["orig"], rows) == {"orig": "FAILED"}
