
"""Tests for pipeline execution and recovery."""

import sqlite3

from pathlib import Path
from unittest.mock import patch

import pytest

from api_ingestion.pipeline import execute


@pytest.fixture
def config(tmp_path):
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
            "operations_db": str(
                tmp_path / "state" / "operations.db"
            ),
        },
    }


def fake_bronze(config, token):
    path = (
        Path(config["storage"]["bronze_directory"])
        / "github_issues_test.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('[{"id": 1}]', encoding="utf-8")
    return path


def fake_silver(config):
    path = Path(config["storage"]["silver_file"])
    path.parent.mkdir(parents=True, exist_ok=True)

    # Test the orchestration contract, not Parquet internals.
    path.write_bytes(b"mock silver output")

    return {
        "silver_file": str(path),
        "silver_records": 1,
        "status": "SUCCESS",
    }


def query_one(config, sql):
    conn = sqlite3.connect(
        config["storage"]["operations_db"]
    )
    try:
        return conn.execute(sql).fetchone()
    finally:
        conn.close()


def test_full_pipeline_succeeds(config):
    with (
        patch(
            "api_ingestion.pipeline.run_ingestion",
            side_effect=fake_bronze,
        ),
        patch(
            "api_ingestion.pipeline.build_silver",
            side_effect=fake_silver,
        ),
    ):
        result = execute(config, token="fake-token")

    assert result["status"] == "SUCCESS"

    run = query_one(
        config,
        "SELECT status FROM pipeline_runs",
    )
    assert run[0] == "SUCCESS"

    checkpoint = query_one(
        config,
        "SELECT run_id FROM silver_checkpoints",
    )
    assert checkpoint is not None


def test_failure_after_bronze_preserves_checkpoint(config):
    with (
        patch(
            "api_ingestion.pipeline.run_ingestion",
            side_effect=fake_bronze,
        ),
        patch(
            "api_ingestion.pipeline.build_silver",
            side_effect=fake_silver,
        ),
    ):
        execute(config, token="fake-token")

        before = query_one(
            config,
            "SELECT run_id FROM silver_checkpoints",
        )

        with pytest.raises(
            RuntimeError,
            match="after Bronze",
        ):
            execute(
                config,
                token="fake-token",
                inject_failure="after-bronze",
            )

    after = query_one(
        config,
        "SELECT run_id FROM silver_checkpoints",
    )

    assert before == after

    latest = query_one(
        config,
        """
        SELECT status
        FROM pipeline_runs
        ORDER BY started_at DESC, rowid DESC
        LIMIT 1
        """,
    )

    assert latest[0] == "FAILED"


def test_silver_only_recovery_skips_api(config):
    fake_bronze(config, "fake-token")

    with (
        patch(
            "api_ingestion.pipeline.run_ingestion"
        ) as api_mock,
        patch(
            "api_ingestion.pipeline.build_silver",
            side_effect=fake_silver,
        ),
    ):
        result = execute(
            config,
            mode="silver-only",
        )

    assert result["status"] == "SUCCESS"
    api_mock.assert_not_called()


def test_failure_after_silver_is_recoverable(config):
    fake_bronze(config, "fake-token")

    with patch(
        "api_ingestion.pipeline.build_silver",
        side_effect=fake_silver,
    ):
        with pytest.raises(
            RuntimeError,
            match="after Silver",
        ):
            execute(
                config,
                mode="silver-only",
                inject_failure="after-silver",
            )

        assert query_one(
            config,
            "SELECT COUNT(*) FROM silver_checkpoints",
        )[0] == 0

        result = execute(
            config,
            mode="silver-only",
        )

    assert result["status"] == "SUCCESS"

    assert query_one(
        config,
        "SELECT COUNT(*) FROM silver_checkpoints",
    )[0] == 1


def test_interrupted_run_is_reclassified(config):
    from api_ingestion.operations import RunStore

    store = RunStore(config["storage"]["operations_db"])
    store.start_run("stale-run", "microsoft/vscode", "full")
    store.start_stage("stale-run", "bronze")
    store.close()

    fake_bronze(config, "fake-token")

    with patch(
        "api_ingestion.pipeline.build_silver",
        side_effect=fake_silver,
    ):
        execute(config, mode="silver-only")

    run = query_one(
        config,
        "SELECT status FROM pipeline_runs "
        "WHERE run_id='stale-run'",
    )

    stage = query_one(
        config,
        "SELECT status FROM pipeline_stages "
        "WHERE run_id='stale-run'",
    )

    assert run[0] == "INTERRUPTED"
    assert stage[0] == "INTERRUPTED"
