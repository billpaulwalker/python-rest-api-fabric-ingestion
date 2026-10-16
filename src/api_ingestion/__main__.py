
"""Command-line entry point."""

import argparse
import logging
import os

from pathlib import Path

import yaml
from dotenv import load_dotenv

from .ingestion import run_ingestion
from .logging_utils import configure_logging


def main():
    parser = argparse.ArgumentParser(
        description="Incremental GitHub REST API ingestion"
    )

    parser.add_argument(
        "--config",
        default="config/settings.yaml",
        help="Path to configuration file",
    )

    args = parser.parse_args()

    load_dotenv()

    token = os.getenv("GITHUB_TOKEN")

    if not token:
        raise ValueError("GITHUB_TOKEN is missing")

    with Path(args.config).open(
        "r",
        encoding="utf-8",
    ) as file:
        config = yaml.safe_load(file)

    configure_logging(
        directory=config["logging"]["directory"],
        level=config["logging"]["level"],
    )

    logging.info("Configuration loaded")

    run_ingestion(
        config=config,
        token=token,
    )


if __name__ == "__main__":
    main()
