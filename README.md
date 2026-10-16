
# Python REST API Ingestion Framework

A Python-based REST API ingestion framework with planned
Microsoft Fabric Lakehouse integration.

## Current Status

Phase 1: Local GitHub API ingestion.

## Implemented Features

- GitHub REST API authentication
- Configurable API endpoint and request parameters
- HTTP requests with timeout configuration
- Raw JSON Bronze file landing
- Unique execution identifiers
- Basic execution logging

## Prerequisites

- Python 3.11+
- Git
- GitHub personal access token

## Local Setup

1. Create and activate a Python virtual environment.
2. Run `python -m pip install -e .`
3. Copy `.env.example` to `.env`.
4. Set the `GITHUB_TOKEN` environment variable.

## Run Ingestion

```powershell
python -m api_ingestion --config config/settings.yaml
```

## Output

Raw JSON responses are stored in `data/bronze/`.

Execution logs are stored in `logs/`.

## Roadmap

- Pagination and incremental extraction
- Retry and rate-limit handling
- Bronze/Silver data validation
- Checkpointing and rerun safety
- Automated testing
- Microsoft Fabric Lakehouse integration
- Fabric Data Pipeline orchestration
