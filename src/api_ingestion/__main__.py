
"""Command-line entry point for API ingestion."""

import argparse
import logging
import os

from pathlib import Path

import yaml
from dotenv import load_dotenv

from .ingestion import run_ingestion


def main():
    parser = argparse.ArgumentParser(
        description="GitHub REST API ingestion"
    )

    parser.add_argument(
        "--config",
        default="config/settings.yaml",
        help="Path to YAML configuration",
    )

    args = parser.parse_args()

    # Load local development credentials.
    load_dotenv()

    token = os.getenv("GITHUB_TOKEN")

    if not token:
        raise ValueError(
            "GITHUB_TOKEN is missing from the environment"
        )

    config_path = Path(args.config)

    with config_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = yaml.safe_load(file)

    log_directory = Path(
        config["logging"]["directory"]
    )
    log_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    logging.basicConfig(
        level=config["logging"]["level"],
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                log_directory / "ingestion.log",
                encoding="utf-8",
            ),
        ],
    )

    run_ingestion(
        config=config,
        token=token,
    )


if __name__ == "__main__":
    main()
