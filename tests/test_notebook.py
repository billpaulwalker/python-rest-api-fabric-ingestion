"""Tests for the exported Fabric notebook.

The notebook's code cells are executed against stubbed Spark and
Fabric objects so the run control flow can be checked locally.
"""

import json
import re
import sys
import types

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import api_ingestion
import api_ingestion.client
import api_ingestion.fabric_policy
import api_ingestion.transform
import api_ingestion.validation


NOTEBOOK = (
    Path(__file__).resolve().parents[1]
    / "notebooks"
    / "nb_github_issues_ingestion.ipynb"
)

# Cells after the definitions run the pipeline or query Spark.
DEFINITION_CELLS = 6


def load_notebook():
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))


def code_cells(notebook):
    return [
        cell for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    ]


def raw_issue(issue_id, updated_at, title="Issue"):
    return {
        "id": issue_id,
        "number": issue_id,
        "title": title,
        "state": "open",
        "user": {"login": "tester"},
        "labels": [],
        "created_at": "1999-12-01T00:00:00Z",
        "updated_at": updated_at,
        "closed_at": None,
    }


@pytest.fixture
def nb(monkeypatch, tmp_path):
    """Execute the notebook definitions with stubbed dependencies."""

    for name in (
        "pyspark", "pyspark.sql", "pyspark.sql.types",
        "delta", "delta.tables",
    ):
        monkeypatch.setitem(sys.modules, name, MagicMock())

    builtin = types.ModuleType("builtin")
    builtin.api_ingestion = api_ingestion
    monkeypatch.setitem(sys.modules, "builtin", builtin)

    for name in (
        "", ".client", ".fabric_policy", ".transform", ".validation",
    ):
        monkeypatch.setitem(
            sys.modules,
            f"builtin.api_ingestion{name}",
            sys.modules[f"api_ingestion{name}"],
        )

    source = "\n\n".join(
        "".join(cell["source"])
        for cell in code_cells(load_notebook())[:DEFINITION_CELLS]
    ).replace(
        'Path("/lakehouse/default/Files")',
        f"Path({str(tmp_path)!r})",
    )

    namespace = {
        "spark": MagicMock(),
        "notebookutils": MagicMock(),
        "display": MagicMock(),
    }
    exec(compile(source, str(NOTEBOOK), "exec"), namespace)

    calls = []
    runs = []

    def record(name, result=None):
        def stub(*args, **kwargs):
            calls.append(name)
            return result
        return stub

    namespace.update(
        ensure_control_tables=record("ensure_control_tables"),
        mark_interrupted_runs=record("mark_interrupted_runs"),
        write_run=lambda cfg, run: runs.append(dict(run)),
        get_github_token=record("get_github_token", "token"),
        write_quarantine=record(
            "write_quarantine", tmp_path / "rejected.jsonl"
        ),
        merge_silver=record("merge_silver", object()),
        verify_silver=record("verify_silver"),
        advance_checkpoint=MagicMock(
            side_effect=lambda *a: calls.append("advance_checkpoint")
        ),
    )

    def extract(records):
        def stub(cfg, source, token, run_id, run_started):
            calls.append("extract_to_bronze")
            return tmp_path / f"github_issues_{run_id}.json", records
        namespace["extract_to_bronze"] = stub

    namespace["_calls"] = calls
    namespace["_runs"] = runs
    namespace["_extract"] = extract

    return namespace


def run(nb, **kwargs):
    return nb["run_ingestion"](nb["config"], **kwargs)


def test_success_advances_checkpoint_after_verification(nb):
    nb["_extract"]([
        raw_issue(1, "2000-01-01T00:00:00Z"),
        raw_issue(2, "2000-01-02T00:00:00Z"),
    ])

    result = run(nb)

    calls = nb["_calls"]
    assert calls.index("merge_silver") < calls.index("verify_silver")
    assert calls.index("verify_silver") < calls.index("advance_checkpoint")

    watermark = nb["advance_checkpoint"].call_args.args[2]
    assert watermark.isoformat() == "2000-01-02T00:00:00+00:00"

    final = nb["_runs"][-1]
    assert result["status"] == final["status"] == "SUCCESS"
    assert final["silver_candidates"] == 2
    assert final["completed_at"] is not None


def test_verification_failure_blocks_checkpoint_and_is_logged(nb):
    nb["_extract"]([raw_issue(1, "2000-01-01T00:00:00Z")])

    def fail(*args):
        raise RuntimeError("1 Silver candidates were not committed")

    nb["verify_silver"] = fail

    with pytest.raises(RuntimeError, match="not committed"):
        run(nb)

    assert "advance_checkpoint" not in nb["_calls"]

    final = nb["_runs"][-1]
    assert final["status"] == "FAILED"
    assert final["failed_stage"] == "silver_verify"
    assert "not committed" in final["error_message"]


def test_rejected_records_merge_valid_rows_but_block_checkpoint(nb):
    nb["_extract"]([
        raw_issue(1, "2000-01-01T00:00:00Z"),
        {"id": "bad"},
    ])

    with pytest.raises(RuntimeError, match="rejected records"):
        run(nb)

    assert "merge_silver" in nb["_calls"]
    assert "advance_checkpoint" not in nb["_calls"]

    final = nb["_runs"][-1]
    assert final["failed_stage"] == "checkpoint"
    assert final["rejected_records"] == 1


def test_no_candidates_skips_merge_and_checkpoint(nb):
    nb["_extract"]([])

    assert run(nb)["status"] == "SUCCESS"
    assert "merge_silver" not in nb["_calls"]
    assert "advance_checkpoint" not in nb["_calls"]


def test_logging_failure_does_not_mask_original_error(nb):
    def boom(*args):
        raise ConnectionError("GitHub unavailable")

    nb["extract_to_bronze"] = boom

    def broken_write(cfg, run_row):
        if run_row["status"] != "RUNNING":
            raise OSError("Delta write failed")

    nb["write_run"] = broken_write

    with pytest.raises(ConnectionError, match="GitHub unavailable"):
        run(nb)


def test_logging_failure_after_success_is_raised(nb):
    nb["_extract"]([])

    def broken_write(cfg, run_row):
        if run_row["status"] == "SUCCESS":
            raise OSError("Delta write failed")

    nb["write_run"] = broken_write

    with pytest.raises(OSError, match="Delta write failed"):
        run(nb)


def test_cancellation_is_recorded_as_interrupted(nb):
    def cancel(*args):
        raise KeyboardInterrupt

    nb["extract_to_bronze"] = cancel

    with pytest.raises(KeyboardInterrupt):
        run(nb)

    final = nb["_runs"][-1]
    assert final["status"] == "INTERRUPTED"
    assert final["failed_stage"] == "bronze"


@pytest.mark.parametrize("kwargs", [
    {"mode": "recover"},
    {"mode": "incremental", "recover_run_id": "abc"},
    {"mode": "full"},
])
def test_invalid_mode_arguments_rejected(nb, kwargs):
    with pytest.raises(ValueError):
        run(nb, **kwargs)

    assert nb["_runs"] == []


def test_recover_mode_reuses_original_bronze_identity(nb, tmp_path):
    bronze_run_id = "5b97b1a1-4fb5-4ea9-bd4a-cf6e70edd773"
    path = tmp_path / f"github_issues_{bronze_run_id}.json"
    path.write_text(
        json.dumps([raw_issue(1, "2000-01-01T00:00:00Z")]),
        encoding="utf-8",
    )

    original_start = nb["datetime"](
        2000, 1, 3, tzinfo=nb["timezone"].utc
    )

    nb["resolve_recovery_target"] = lambda cfg, source, rid: (
        path, original_start
    )

    captured = {}

    def merge(cfg, candidates):
        captured["candidates"] = candidates
        return object()

    nb["merge_silver"] = merge

    run(nb, mode="recover", recover_run_id=bronze_run_id)

    [candidate] = captured["candidates"]
    assert candidate["source_run_id"] == bronze_run_id
    assert candidate["ingested_at"] == original_start
    assert "get_github_token" not in nb["_calls"]

    final = nb["_runs"][-1]
    assert final["recovered_from_run_id"] == bronze_run_id
    assert final["status"] == "SUCCESS"


def test_recovery_path_rejects_non_uuid_run_id(nb):
    with pytest.raises(ValueError):
        nb["bronze_file"](nb["config"], "../../secrets")


# Committed notebook hygiene -------------------------------------------


def test_notebook_has_no_outputs_or_widget_state():
    notebook = load_notebook()

    assert "synapse_widget" not in notebook["metadata"]

    for cell in code_cells(notebook):
        assert cell["outputs"] == []
        assert cell["execution_count"] is None


def test_runtime_settings_are_parameters_without_defaults():
    [parameters] = [
        cell for cell in code_cells(load_notebook())
        if "parameters" in cell["metadata"].get("tags", [])
    ]

    source = "".join(parameters["source"])

    assert re.search(r'^key_vault_url = ""', source, re.M)
    assert re.search(r'^github_token_secret_name = ""', source, re.M)


def test_notebook_contains_no_secrets_or_vault_urls():
    text = NOTEBOOK.read_text(encoding="utf-8")

    patterns = [
        r"gh[pousr]_[A-Za-z0-9]{20,}",
        r"github_pat_[A-Za-z0-9_]{20,}",
        r"\.vault\.azure\.net",
        r"Bearer [A-Za-z0-9_\-\.]{20,}",
    ]

    for pattern in patterns:
        assert not re.search(pattern, text), pattern
