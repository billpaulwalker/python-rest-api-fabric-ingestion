
"""Run and recover the local Bronze/Silver pipeline."""

import argparse
import hashlib
import json
import logging
import os
import uuid

from pathlib import Path

import yaml
from dotenv import load_dotenv

from .ingestion import run_ingestion
from .silver import build_silver
from .logging_utils import configure_logging
from .operations import RunStore


logger = logging.getLogger(__name__)


def bronze_fingerprint(directory):
    """Hash Bronze filenames and content to detect changes."""

    paths = sorted(
        Path(directory).glob("github_issues_*.json")
    )

    if not paths:
        raise FileNotFoundError(
            "No Bronze files available for Silver processing"
        )

    digest = hashlib.sha256()

    for path in paths:
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")

        with path.open("rb") as stream:
            for block in iter(
                lambda: stream.read(1024 * 1024),
                b"",
            ):
                digest.update(block)

    return digest.hexdigest()


def execute(
    config: dict,
    mode: str = "full",
    inject_failure: str | None = None,
    token: str | None = None,
) -> dict:
    """Execute the pipeline with SQLite run tracking."""

    if mode not in {"full", "silver-only"}:
        raise ValueError("Invalid mode")

    if inject_failure not in {
        None, "after-bronze", "after-silver"
    }:
        raise ValueError("Invalid failure point")

    if mode == "silver-only" and inject_failure == "after-bronze":
        raise ValueError(
            "after-bronze is only valid in full mode"
        )

    if mode == "full" and not token:
        raise ValueError("GITHUB_TOKEN required in full mode")

    source = (
        f"{config['api']['owner']}/"
        f"{config['api']['repository']}"
    )

    store = RunStore(
        config["storage"]["operations_db"]
    )

    pipeline_run_id = str(uuid.uuid4())
    active_stage = None

    try:
        store.mark_interrupted()

        store.start_run(
            pipeline_run_id, source, mode
        )

        logger.info(
            "Pipeline started | run_id=%s | mode=%s",
            pipeline_run_id,
            mode,
        )

        # ---------------------------------------------
        # Stage 1: Bronze extraction
        # ---------------------------------------------

        if mode == "full":
            active_stage = "bronze"

            store.start_stage(
                pipeline_run_id, "bronze"
            )

            if token is None or not token.strip():
                raise ValueError(
                    "GITHUB_TOKEN is required for Bronze extraction"
                )

            bronze_path = run_ingestion(config, token)

            store.set_bronze_file(
                pipeline_run_id, bronze_path
            )

            store.finish_stage(
                pipeline_run_id,
                "bronze",
                "SUCCESS",
                str(bronze_path),
            )

            active_stage = None

            if inject_failure == "after-bronze":
                raise RuntimeError(
                    "Injected failure after Bronze"
                )

        # ---------------------------------------------
        # Stage 2: Silver processing
        # ---------------------------------------------

        active_stage = "silver"

        store.start_stage(
            pipeline_run_id, "silver"
        )

        result = build_silver(config)

        silver_path = Path(result["silver_file"])

        if not silver_path.is_file():
            raise RuntimeError(
                "Silver output was not created"
            )

        fingerprint = bronze_fingerprint(
            config["storage"]["bronze_directory"]
        )

        if inject_failure == "after-silver":
            raise RuntimeError(
                "Injected failure after Silver write"
            )

        # Commit the stage success and checkpoint
        # within one SQLite transaction.
        store.publish_silver(
            pipeline_run_id,
            source,
            silver_path,
            fingerprint,
        )

        active_stage = None

        logger.info(
            "Pipeline completed | run_id=%s | silver_rows=%s",
            pipeline_run_id,
            result["silver_records"],
        )

        return {
            **result,
            "pipeline_run_id": pipeline_run_id,
            "status": "SUCCESS",
        }

    except Exception as exc:
        if active_stage:
            store.finish_stage(
                pipeline_run_id,
                active_stage,
                "FAILED",
                str(exc),
            )

        store.finish_run(
            pipeline_run_id,
            "FAILED",
            str(exc),
        )

        logger.exception(
            "Pipeline failed | run_id=%s",
            pipeline_run_id,
        )
        raise

    finally:
        store.close()


def main():
    parser = argparse.ArgumentParser(
        description="Local reliable ingestion pipeline"
    )

    parser.add_argument(
        "--config",
        default="config/settings.yaml",
    )

    parser.add_argument(
        "--mode",
        choices=["full", "silver-only"],
        default="full",
    )

    parser.add_argument(
        "--inject-failure",
        choices=["after-bronze", "after-silver"],
    )

    args = parser.parse_args()

    config = yaml.safe_load(
        Path(args.config).read_text(encoding="utf-8")
    )

    configure_logging(
        config["logging"]["directory"],
        config["logging"]["level"],
    )

    load_dotenv()

    result = execute(
        config,
        mode=args.mode,
        inject_failure=args.inject_failure,
        token=os.getenv("GITHUB_TOKEN"),
    )

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
