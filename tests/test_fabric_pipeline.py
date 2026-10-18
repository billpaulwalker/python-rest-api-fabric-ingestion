"""Tests for the exported Fabric pipeline definition."""

import json
import re

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "pipelines" / "pl_github_issues_ingestion.json"
NOTEBOOK = ROOT / "notebooks" / "nb_github_issues_ingestion.ipynb"


def load_pipeline():
    return json.loads(PIPELINE.read_text(encoding="utf-8"))


def notebook_activity(pipeline):
    [activity] = [
        a for a in pipeline["properties"]["activities"]
        if a["type"] == "TridentNotebook"
    ]
    return activity


def test_runs_do_not_overlap():
    assert load_pipeline()["properties"]["concurrency"] == 1


def test_run_mode_comes_from_pipeline_parameters():
    pipeline = load_pipeline()
    declared = pipeline["properties"]["parameters"]

    assert declared["mode"]["defaultValue"] == "incremental"
    assert declared["recover_run_id"]["defaultValue"] == ""

    parameters = notebook_activity(pipeline)["typeProperties"]["parameters"]

    for name in ("mode", "recover_run_id"):
        assert parameters[name]["value"] == {
            "value": f"@pipeline().parameters.{name}",
            "type": "Expression",
        }


def test_activity_parameters_exist_in_notebook():
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))

    [cell] = [
        c for c in notebook["cells"]
        if "parameters" in c["metadata"].get("tags", [])
    ]

    defined = set(re.findall(
        r"^(\w+) =", "".join(cell["source"]), re.M
    ))
    passed = set(
        notebook_activity(load_pipeline())["typeProperties"]["parameters"]
    )

    assert passed <= defined, passed - defined


def test_activity_does_not_retry_automatically():
    assert notebook_activity(load_pipeline())["policy"]["retry"] == 0


def test_pipeline_contains_no_secrets():
    text = PIPELINE.read_text(encoding="utf-8")

    for pattern in (
        r"gh[pousr]_[A-Za-z0-9]{20,}",
        r"github_pat_[A-Za-z0-9_]{20,}",
        r"Bearer [A-Za-z0-9_\-\.]{20,}",
    ):
        assert not re.search(pattern, text), pattern
