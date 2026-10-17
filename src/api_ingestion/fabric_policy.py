"""Upsert, checkpoint, and recovery rules for the Fabric notebook.

Same-timestamp policy
---------------------
GitHub ``updated_at`` has one-second precision, so two different
versions of an issue can share a timestamp. The API always returns
the current state at fetch time, so for equal ``updated_at`` the
later observation is the more current one:

* A newer ``updated_at`` always replaces an older one.
* For equal ``updated_at``, a later observation (greater
  ``ingested_at``) replaces the stored row only when business
  content differs. Identical re-fetches, such as those from the
  watermark overlap window, are no-ops, so ``ingested_at`` and
  ``source_run_id`` keep identifying the run that first delivered
  the stored version.
* An earlier or equal observation never replaces the stored row,
  so reprocessing an old Bronze file cannot roll Silver back.
* Within one Bronze file, the record fetched later (later position)
  wins a tie.
"""

from datetime import datetime


# Business columns compared when two versions share updated_at.
CONTENT_COLUMNS = (
    "issue_number",
    "repository",
    "title",
    "state",
    "author_login",
    "labels_json",
    "created_at",
    "closed_at",
)


def should_replace(incoming: dict, existing: dict | None) -> bool:
    """Apply the Silver upsert policy to one pair of versions."""

    if existing is None:
        return True

    if incoming["updated_at"] != existing["updated_at"]:
        return incoming["updated_at"] > existing["updated_at"]

    if incoming["ingested_at"] <= existing["ingested_at"]:
        return False

    return any(
        incoming[column] != existing[column]
        for column in CONTENT_COLUMNS
    )


def merge_update_condition(
    source: str = "s",
    target: str = "t",
) -> str:
    """Spark SQL equivalent of ``should_replace`` for Delta MERGE."""

    content_equal = " AND ".join(
        f"{source}.{column} <=> {target}.{column}"
        for column in CONTENT_COLUMNS
    )

    return (
        f"{source}.updated_at > {target}.updated_at"
        f" OR ({source}.updated_at = {target}.updated_at"
        f" AND {source}.ingested_at > {target}.ingested_at"
        f" AND NOT ({content_equal}))"
    )


def select_latest_versions(records: list) -> list:
    """Keep one version per issue ID from a single Bronze file.

    Records must be in Bronze (fetch) order so that a later
    observation wins a same-timestamp tie.
    """

    by_id = {}

    for record in records:
        key = record["issue_id"]
        previous = by_id.get(key)

        if previous is None or (
            record["updated_at"],
            record["ingested_at"],
        ) >= (
            previous["updated_at"],
            previous["ingested_at"],
        ):
            by_id[key] = record

    return sorted(by_id.values(), key=lambda r: r["issue_id"])


def candidate_watermark(records: list) -> datetime | None:
    """Highest source update committed by this run."""

    return max(
        (record["updated_at"] for record in records),
        default=None,
    )


def classify_bronze_runs(
    bronze_run_ids: list,
    run_rows: list,
) -> dict:
    """Return Bronze run IDs not yet committed, with their status.

    A Bronze file is committed when its own run succeeded or a
    later recovery run that reprocessed it succeeded. Anything else
    is reported with the latest recorded status of its run, or
    ``UNTRACKED`` when no run row exists.
    """

    committed = set()
    latest = {}

    for row in run_rows:
        if row["status"] == "SUCCESS":
            committed.add(row["run_id"])

            if row.get("recovered_from_run_id"):
                committed.add(row["recovered_from_run_id"])

        current = latest.get(row["run_id"])

        if current is None or row["started_at"] > current["started_at"]:
            latest[row["run_id"]] = row

    return {
        run_id: (
            latest[run_id]["status"]
            if run_id in latest
            else "UNTRACKED"
        )
        for run_id in bronze_run_ids
        if run_id not in committed
    }
