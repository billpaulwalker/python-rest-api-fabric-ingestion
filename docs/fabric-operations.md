# Fabric Operations and Recovery Runbook

## Purpose

This runbook describes how to run, investigate, recover, and validate the
GitHub REST API ingestion pipeline in Microsoft Fabric.

| Component | Name |
| --- | --- |
| Pipeline | `pl_github_issues_ingestion` (concurrency 1, 2-hour timeout, no automatic retry) |
| Notebook activity | `nb_ingest_github_issues` |
| Notebook | `nb_github_issues_ingestion` |
| Lakehouse | `lh_api_ingestion` |
| Silver table | `silver_github_issues` |
| Checkpoint | `ctl_ingestion_state` (`watermark` per source; only moves forward) |
| Run history | `ctl_ingestion_runs` |
| Bronze files | `Files/bronze/github_issues/github_issues_<run_id>.json` |
| Quarantine | `Files/quarantine/github_issues/rejected_<run_id>.jsonl` |

Every run writes one row to `ctl_ingestion_runs`. Its final `status` is
`SUCCESS`, `FAILED`, or `INTERRUPTED`. For failures, `failed_stage`
records where the run stopped: `setup`, `bronze` (`recovery_load` in
recover mode), `transform`, `silver_merge`, `silver_verify`, or
`checkpoint`. `error_message` holds the exception text.

Queries below are Spark SQL for a notebook attached to `lh_api_ingestion`.

## Normal Execution

1. Open `pl_github_issues_ingestion` and select **Run**.
2. Keep the pipeline parameters at `mode = incremental` and
   `recover_run_id` empty.
3. Confirm the `nb_ingest_github_issues` activity succeeds. Its output
   `result.exitValue` contains the run result: `run_id`, `bronze_records`,
   `silver_candidates`, and `watermark`.
4. Review the latest run:

   ```sql
   SELECT run_id, status, failed_stage, bronze_records,
          silver_candidates, rejected_records, started_at, completed_at
   FROM ctl_ingestion_runs ORDER BY started_at DESC LIMIT 5;
   ```

5. Verify the watermark and Silver uniqueness (see
   [Standard Recovery Validation](#standard-recovery-validation)).

A watermark that does not change is expected when a run only re-fetches
issues inside the 5-minute overlap window.

Do not run the notebook interactively while the pipeline is running. A
new run marks any `RUNNING` row for the source as `INTERRUPTED`.

## Authentication Failure — HTTP 401/403

**Symptoms:** The run fails at `failed_stage = bronze`, with one of:

- `HTTPError: 401 Client Error: Unauthorized for url: https://api.github.com/user`
- `403 Client Error` with no rate-limit message
- a Key Vault error from `getSecret`
- `Missing runtime setting`

**Likely causes:**

- Expired or revoked GitHub token.
- Token without read access to the repository.
- Missing Key Vault access for the execution identity.
- `key_vault_url` or `github_token_secret_name` not supplied.

**Investigation:**

- Review the activity error and `error_message`. GitHub authentication
  is the first API call, and 401 and 403 are not retried.
- In Key Vault, confirm the secret exists, is enabled, and is not
  expired.
- Confirm the execution identity has the *Key Vault Secrets User* role.
  Pipeline runs and interactive runs can use different identities.
- Check the token in GitHub under **Settings → Developer settings →
  Personal access tokens**.

**Recovery:** Add a new version of the existing Key Vault secret. The
notebook reads the latest version, so no notebook or pipeline change is
needed. Then rerun the pipeline. The checkpoint did not move, so no data
is skipped. Never log or print the token.

**Validation:** The notebook snapshot shows `Authenticated | login=...`,
and the run ends `SUCCESS`.

## Rate Limiting — HTTP 429 or Rate-Limited 403

**Symptoms:** The run fails at `bronze` with `429 Client Error`, a
rate-limited `403`, or
`Rate-limit wait exceeds configured maximum: N seconds`. A high
`retries=` value in the `Bronze written` log line is an early warning.

**Likely causes:**

- Frequent runs.
- Another workload sharing the token.
- A large catch-up window after a pause.

**Investigation:**

- The client honours `Retry-After`. Without it, the client waits for
  `x-ratelimit-reset` when `x-ratelimit-remaining` is `0`; otherwise it
  waits 60 seconds.
- Each request makes at most `max_attempts` (4) attempts. The client
  fails immediately when the required wait exceeds
  `max_rate_limit_wait_seconds` (120).
- Check the remaining quota without consuming it:
  `GET https://api.github.com/rate_limit`.

**Recovery:** Wait until the reset time, then rerun. For recurring limits:

- reduce the schedule frequency or use a dedicated token, or
- raise `max_rate_limit_wait_seconds` in the activity's base parameters,
  keeping total waits well within the 2-hour activity timeout.

**Validation:** Pagination completes, the run ends `SUCCESS`, and the
checkpoint advances only after Silver verification.

## Failure After Bronze

**Symptoms:** A Bronze file exists, but the run ended `FAILED` at
`transform` or `INTERRUPTED` before Silver. The notebook's post-run cell
lists the file as awaiting recovery.

**Investigation:**

- Read `failed_stage`, `error_message`, and `bronze_path` for the run.
- Confirm the checkpoint did not move:
  `SELECT * FROM ctl_ingestion_state;`

**Recovery:** Correct the failure, then choose one:

- **Replay incremental extraction (default):** rerun the pipeline with
  `mode = incremental`. The unchanged watermark re-extracts the same
  window, plus any newer changes. Afterwards, archive the superseded file
  from the notebook:
  `archive_bronze_file(config, "<run_id>", "<reason>")`.
- **Reprocess the existing file without calling the API:** run the
  pipeline with `mode = recover` and `recover_run_id = <run_id>`. The
  original run ID and start time are kept as Silver lineage, so the
  replay cannot overwrite newer data.
- **Archive a corrupt file:** if the JSON does not parse (typically a
  session killed mid-write), archive it.

Bronze files are never deleted by the notebook. Archived files move to
`Files/bronze/github_issues/_archive/`.

**Validation:** The run ends `SUCCESS`, Silver has no duplicate issue IDs,
and the post-run cell prints `No unrecovered Bronze files`.

## Failure After Silver MERGE

**Symptoms:** The run failed at `silver_merge`, `silver_verify`, or
`checkpoint`. Silver may contain new records while the checkpoint is
unchanged.

**Investigation:**

- Compare Silver with the checkpoint:

  ```sql
  SELECT s.watermark,
         (SELECT MAX(updated_at) FROM silver_github_issues) AS silver_max_updated
  FROM ctl_ingestion_state s;
  ```

  A `silver_max_updated` later than `watermark` is expected here.
- Run `DESCRIBE HISTORY silver_github_issues LIMIT 5;` to confirm whether
  a MERGE committed during the run.
- `Silver candidates were not committed` points to a concurrent writer.
  `rejected records ... require review` means validation blocked the
  checkpoint (see [Malformed Records](#malformed-records)).

**Recovery:** Correct the failure, then replay the extraction window with
`mode = incremental`, or use `mode = recover` with the failed run's ID.
The MERGE is idempotent:

- It inserts only missing issue IDs.
- It replaces a row only with a newer `updated_at`, or with a later
  observation of the same `updated_at` whose content differs.
- It never overwrites a newer source version with an older one.

**Validation:**

- Silver issue IDs are unique.
- `watermark` is now at or after the earlier `silver_max_updated`.
- A new `SUCCESS` row exists in `ctl_ingestion_runs`.

## Malformed Records

**Symptoms:** `rejected_records > 0`, and the run fails at `checkpoint`
with `rejected records ... require review before the checkpoint can
advance`. Valid records from the same batch are already merged into
Silver.

**Investigation:** Open
`Files/quarantine/github_issues/rejected_<run_id>.jsonl`. Each line
contains `record_index`, `reason`, and the original `record`.

**Recovery:** Decide whether the cause is a source-data anomaly or a
validation or schema defect.

- **Defect:** fix `api_ingestion/validation.py` or `transform.py`,
  re-upload the shared modules to the notebook resources, and rerun.
- **Source anomaly:** every run re-extracts the window and fails at the
  same point until the source record changes or the rule is relaxed.
  This is deliberate: rejected records are never silently skipped.

**Validation:** The rerun ends `SUCCESS` with `rejected_records = 0`, and
the corrected records appear in Silver.

## Incomplete or Missing Data

**Symptoms:** Unexpected row counts, stale `updated_at` values, or
updates visible on GitHub but missing from Silver.

**Investigation:**

- Review recent runs for failures. Check `bronze_records`,
  `silver_candidates`, and `rejected_records`.
- Compare the stored `watermark` with the GitHub `updated_at` of the
  missing issue.
- Check for `Pagination exceeded max_pages`. The run window cannot exceed
  `max_pages` × `per_page` (10,000 items by default).
- Run `DESCRIBE HISTORY silver_github_issues` to see MERGE outcomes.

**Recovery:**

- **Window too large:** raise the `max_pages` base parameter and rerun.
- **Update older than the watermark (late-arriving data):** move the
  checkpoint back, then rerun in incremental mode.
  `initial_lookback_days` applies only when no watermark exists, so it
  does not widen an existing window.

  ```sql
  UPDATE ctl_ingestion_state
  SET watermark = TIMESTAMP '2026-10-10 00:00:00'
  WHERE source = 'microsoft/vscode';
  ```

  The replay is safe because the MERGE does not roll back newer rows.

**Validation:** Spot-check the affected issues against GitHub. The final
watermark should equal the latest committed `updated_at`.

## Standard Recovery Validation

After a recovery run:

- Pipeline status is *Succeeded*, and the latest `ctl_ingestion_runs` row
  is `SUCCESS`.
- Source records were captured in Bronze (`bronze_records`).
- Silver issue IDs are unique:

  ```sql
  SELECT COUNT(*) AS row_count, COUNT(DISTINCT issue_id) AS unique_issues
  FROM silver_github_issues;
  ```

- Quarantined records are reviewed (`rejected_records = 0` on the latest
  run).
- The source checkpoint is correct: `SELECT * FROM ctl_ingestion_state;`
- Run history includes the recovery outcome. Recover-mode runs set
  `recovered_from_run_id`.
- The notebook's post-run cell reports no unrecovered Bronze files.

## Known Limitations

This implementation assumes serial processing of each source, enforced by
pipeline concurrency 1. It uses separate Delta commits for Silver data,
checkpoint state, and run history. A failed run can therefore leave a
committed Silver update without an updated checkpoint.

Safe replay and idempotent target MERGE operations are the primary
recovery mechanisms. Because the checkpoint advances only after Silver is
verified, a failure causes re-extraction, never data loss.
