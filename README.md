
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

## Local Bronze/Silver Processing

The ingestion framework implements a two-stage local data pipeline.

**Bronze — Raw Ingestion**

Retrieves GitHub issue data via REST API, including pagination, incremental retrieval, retries, and rate-limit handling.

```powershell
python -m api_ingestion --config config/settings.yaml
```

Raw API responses are stored in `data/bronze/`.

**Silver — Standardization and Validation**

Processes Bronze JSON files using reusable Python transformation and validation functions.

The processing stage:

- Excludes pull requests returned by the GitHub issues endpoint.
- Flattens nested JSON fields.
- Validates required fields, data types, and timestamps.
- Quarantines malformed records with rejection reasons.
- Deduplicates records by issue ID, retaining the latest version.
- Publishes an explicitly typed Parquet dataset.

```powershell
python -m api_ingestion.silver --config config/settings.yaml
```

**Outputs**

- `data/silver/github_issues.parquet` — current deduplicated issue snapshot
- `data/quarantine/github_issues_rejected.jsonl` — rejected source records

**Testing**

```powershell
python -m pytest tests/ -v
```

**Current limitations**

The local Silver stage rebuilds its dataset from all available Bronze files. This approach supports safe reruns for small datasets but is not optimized for large-scale incremental processing.

The extraction checkpoint is separate from Silver processing completion. A later phase will introduce improved state management and recovery controls.

Microsoft Fabric integration and Delta Lake incremental upserts are planned for subsequent phases.

## Operational Reliability and Recovery

The local pipeline tracks each run in SQLite and can rebuild Silver from existing Bronze files.

```powershell
# Normal execution
python -m api_ingestion.pipeline --mode full

# Recover Silver from existing Bronze
python -m api_ingestion.pipeline --mode silver-only
```

For the Fabric pipeline (`pipelines/pl_github_issues_ingestion.json`) and notebook, see [docs/fabric-setup.md](docs/fabric-setup.md) and the runbook in [docs/fabric-operations.md](docs/fabric-operations.md).
