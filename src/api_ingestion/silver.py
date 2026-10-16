
"""Build a local Silver Parquet snapshot from Bronze JSON."""

import argparse
import json
import logging
import os
import uuid

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from .logging_utils import configure_logging
from .transform import is_pull_request, transform_issue
from .validation import RecordValidationError


logger = logging.getLogger(__name__)


SILVER_SCHEMA = pa.schema([
    pa.field("issue_id", pa.int64(), nullable=False),
    pa.field("issue_number", pa.int64(), nullable=False),
    pa.field("repository", pa.string(), nullable=False),
    pa.field("title", pa.string(), nullable=False),
    pa.field("state", pa.string(), nullable=False),
    pa.field("author_login", pa.string()),
    pa.field("labels_json", pa.string(), nullable=False),
    pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("closed_at", pa.timestamp("us", tz="UTC")),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("source_run_id", pa.string(), nullable=False),
])


def load_bronze_file(path: Path) -> list:
    """Load one complete Bronze extraction."""

    with path.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list):
        raise ValueError(
            f"Bronze file is not a JSON array: {path}"
        )

    return records


def process_bronze(
    bronze_directory: Path,
    repository: str,
) -> tuple[list, list, Counter]:
    """Transform all Bronze files into one logical dataset."""

    files = sorted(
        bronze_directory.glob("github_issues_*.json")
    )

    if not files:
        raise FileNotFoundError(
            f"No Bronze files found in {bronze_directory}"
        )

    valid = []
    rejected = []
    metrics = Counter()

    for path in files:
        records = load_bronze_file(path)

        # Stable provenance for this existing Bronze file.
        run_id = path.stem.removeprefix("github_issues_")

        ingested_at = datetime.fromtimestamp(
            path.stat().st_mtime,
            tz=timezone.utc,
        )

        metrics["bronze_files"] += 1

        for index, record in enumerate(records):
            metrics["source_records"] += 1

            if is_pull_request(record):
                metrics["pull_requests_skipped"] += 1
                continue

            try:
                transformed = transform_issue(
                    record=record,
                    repository=repository,
                    ingested_at=ingested_at,
                    run_id=run_id,
                )
                valid.append(transformed)
                metrics["valid_records"] += 1

            except (RecordValidationError, KeyError, TypeError) as exc:
                rejected.append({
                    "source_file": path.name,
                    "record_index": index,
                    "reason": str(exc),
                    "record": record,
                })
                metrics["rejected_records"] += 1

    return valid, rejected, metrics


def deduplicate(records: list) -> list:
    """Select one latest source version per issue ID."""

    by_id = {}

    # A deterministic ordering makes repeated processing
    # independent of filesystem traversal order.
    ordered = sorted(
        records,
        key=lambda r: (
            r["updated_at"],
            r["ingested_at"],
            r["source_run_id"],
            json.dumps(
                {
                    "title": r["title"],
                    "state": r["state"],
                    "labels_json": r["labels_json"],
                },
                sort_keys=True,
            ),
        ),
    )

    for record in ordered:
        by_id[record["issue_id"]] = record

    return sorted(
        by_id.values(),
        key=lambda r: r["issue_id"],
    )


def write_parquet(path: Path, records: list) -> None:
    """Write an explicitly typed Parquet snapshot."""

    path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(
        records,
        schema=SILVER_SCHEMA,
    )

    temp_path = path.with_name(
        f".{path.name}.{uuid.uuid4().hex}.tmp"
    )

    try:
        pq.write_table(
            table,
            temp_path,
            compression="snappy",
        )
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def write_quarantine(path: Path, records: list) -> None:
    """Write rejected records as JSON Lines."""

    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_name(
        f".{path.name}.{uuid.uuid4().hex}.tmp"
    )

    try:
        with temp_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            for record in records:
                file.write(
                    json.dumps(record, default=str)
                    + "\n"
                )

            file.flush()
            os.fsync(file.fileno())

        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def build_silver(config: dict) -> dict:
    """Execute Bronze-to-Silver processing."""

    run_id = str(uuid.uuid4())

    storage = config["storage"]
    api = config["api"]

    bronze_dir = Path(storage["bronze_directory"])
    silver_path = Path(storage["silver_file"])
    quarantine_path = Path(storage["quarantine_file"])

    repository = f"{api['owner']}/{api['repository']}"

    logger.info(
        "Silver processing started",
        extra={
            "run_id": run_id,
            "stage": "silver",
            "status": "STARTED",
        },
    )

    try:
        valid, rejected, metrics = process_bronze(
            bronze_directory=bronze_dir,
            repository=repository,
        )

        silver_records = deduplicate(valid)

        metrics["silver_records"] = len(silver_records)
        metrics["duplicates_removed"] = (
            len(valid) - len(silver_records)
        )

        # Fail validation before replacing the target.
        table = pa.Table.from_pylist(
            silver_records,
            schema=SILVER_SCHEMA,
        )

        if table.num_rows != len(silver_records):
            raise RuntimeError("Silver row count mismatch")

        # Each output is individually replaced atomically.
        # Publishing both is not a multi-file transaction.
        write_parquet(silver_path, silver_records)
        write_quarantine(quarantine_path, rejected)

        result = {
            "run_id": run_id,
            "status": "SUCCESS",
            "silver_file": str(silver_path),
            "quarantine_file": str(quarantine_path),
            **dict(metrics),
        }

        logger.info(
            "Silver processing completed | %s",
            json.dumps(result),
            extra={
                "run_id": run_id,
                "stage": "silver",
                "status": "SUCCESS",
                "records": len(silver_records),
            },
        )

        return result

    except Exception:
        logger.exception(
            "Silver processing failed",
            extra={
                "run_id": run_id,
                "stage": "silver",
                "status": "FAILED",
            },
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build local GitHub issues Silver Parquet"
    )

    parser.add_argument(
        "--config",
        default="config/settings.yaml",
    )

    args = parser.parse_args()

    with Path(args.config).open(
        "r",
        encoding="utf-8",
    ) as file:
        config = yaml.safe_load(file)

    configure_logging(
        directory=config["logging"]["directory"],
        level=config["logging"]["level"],
    )

    result = build_silver(config)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
