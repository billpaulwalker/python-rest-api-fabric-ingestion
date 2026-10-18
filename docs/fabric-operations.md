# Fabric Operations Runbook: GitHub Issues Ingestion

Runbook for the Fabric pipeline `pl_github_issues_ingestion`. Its single
notebook activity, `nb_ingest_github_issues`, runs
`notebooks/nb_github_issues_ingestion.ipynb` against the default Lakehouse
`lh_api_ingestion`.

## Quick reference

| Item | Location |
| --- | --- |
| Pipeline definition | `pipelines/pl_github_issues_ingestion.json` (concurrency 1, activity timeout 2 hours, no automatic retry) |
| Silver table | `silver_github_issues` |
| Checkpoint | `ctl_ingestion_state` (one row per source; `watermark` only moves forward) |
| Run log | `ctl_ingestion_runs` (one row per execution) |
| Bronze files | `Files/bronze/github_issues/github_issues_<run_id>.json` |
| Archived Bronze | `Files/bronze/github_issues/_archive/` |
| Quarantine | `Files/quarantine/github_issues/rejected_<run_id>.jsonl` |
| Retry and rate-limit logic | `src/api_ingestion/client.py` |
| Upsert policy | `src/api_ingestion/fabric_policy.py` |

**Run statuses** in `ctl_ingestion_runs.status`:

| Status | Meaning |
| --- | --- |
| `SUCCESS` | Silver committed and verified; checkpoint advanced if there were candidates |
| `FAILED` | Exception raised; see `failed_stage` and `error_message` |
| `INTERRUPTED` | Cancelled, or still `RUNNING` when the next run started (session killed) |
| `ARCHIVED` | Audit row from `archive_bronze_file()`; not an execution |

**Stages**, in order, as recorded in `failed_stage`: `setup`, `bronze`
(`recovery_load` in recover mode), `transform`, `silver_merge`,
`silver_verify`, `checkpoint`.

**Where did it fail?** Use this table to choose the recovery procedure:

| `failed_stage` | Bronze file? | Silver changed? | Checkpoint moved? | Procedure |
| --- | --- | --- | --- | --- |
| `setup` | No | No | No | [Diagnose failures](#diagnose-failures) |
| `bronze` | No (or partial) | No | No | [HTTP 401](#recover-from-http-401), [HTTP 429](#recover-from-http-429), or rerun |
| `transform` | Yes | No | No | [Recover after Bronze](#recover-after-bronze) |
| `silver_merge` | Yes | Possibly | No | [Recover after Silver](#recover-after-silver) |
| `silver_verify` | Yes | Yes | No | [Recover after Silver](#recover-after-silver) |
| `checkpoint` | Yes | Yes | Possibly | [Recover after Silver](#recover-after-silver) |

The queries below are Spark SQL. Run them in a notebook `%%sql` cell
attached to `lh_api_ingestion`.

---

## Start ingestion

1. In the Fabric workspace, open `pl_github_issues_ingestion`.
2. Select **Run**. The pipeline prompts for its two parameters. Keep the
   incremental defaults:

   | Pipeline parameter | Value |
   | --- | --- |
   | `mode` | `incremental` |
   | `recover_run_id` | *(empty)* |

   For a recovery run, enter `mode = recover` and the Bronze `run_id` to
   reprocess (see [Recover after Bronze](#recover-after-bronze)).
3. Select **OK**, then follow the run from **View run status** or the
   **Monitor** hub.

All other settings are base parameters on the `nb_ingest_github_issues`
activity (**Settings → Base parameters**). They include `key_vault_url`,
`github_token_secret_name`, `owner`, `repository_name`, and the API,
retry, and lookback settings. Change them only when a procedure below
says to, and publish the pipeline afterwards.

The pipeline's concurrency is 1, so a run started while another is in
progress is queued. Do not run the notebook interactively while the
pipeline is running: a new run marks any `RUNNING` row for the source as
`INTERRUPTED`.

**Verification:** follow [Inspect successful run](#inspect-successful-run).

---

## Inspect successful run

1. **Pipeline output.** In the pipeline run, open the
   `nb_ingest_github_issues` activity's **Output**. Confirm the activity
   status is *Succeeded*. The notebook's final cell calls
   `notebookutils.notebook.exit()`, so `result.exitValue` holds the run
   result as a JSON string:

   ```json
   {"run_id": "...", "status": "SUCCESS", "mode": "incremental",
    "bronze_path": "...", "bronze_records": 6, "silver_candidates": 1,
    "watermark": "2026-10-17 23:37:17+00:00"}
   ```

   Downstream activities can read it with
   `@json(activity('nb_ingest_github_issues').output.result.exitValue)`.
   A failed run raises before the exit cell, so a failed activity has no
   exit value. For log lines, open the notebook snapshot link in the
   activity output.
2. **Run log.** Confirm the run's row:

   ```sql
   SELECT run_id, status, started_at, completed_at,
          bronze_records, silver_candidates, rejected_records
   FROM ctl_ingestion_runs
   ORDER BY started_at DESC
   LIMIT 5;
   ```

   Expect `status = SUCCESS`, `rejected_records = 0`, and
   `silver_candidates <= bronze_records`. The difference is pull requests,
   which the issues endpoint returns and the notebook skips.
3. **Lakehouse Silver.**

   ```sql
   SELECT COUNT(*) AS row_count,
          COUNT(DISTINCT issue_id) AS unique_issues,
          MAX(updated_at) AS latest_update
   FROM silver_github_issues;

   SELECT * FROM ctl_ingestion_state;
   ```

   Expect `row_count = unique_issues`, and `watermark` equal to or later
   than the previous run's.

A watermark that does not move is normal when the run only re-fetched
issues inside the 5-minute overlap window, or found no new issues.

The cell's **Diagnostics** tab can list a `Spark_User_UserApp_ValueError`
even after a successful run. That entry reflects an exception handled
inside a library. Treat a run as failed only when the notebook shows a
traceback, logs `Run failed | stage=...`, or records a `FAILED` row.

---

## Diagnose failures

**Symptoms:** pipeline run *Failed*, or a scheduled run produced no new
`SUCCESS` row.

**Investigation:**

1. **Monitoring hub.** Open **Monitor**, filter to
   `pl_github_issues_ingestion`, and open the failed run. Select the
   `nb_ingest_github_issues` activity to see its error and the link to the
   notebook snapshot. If the activity shows *Timed out*, the run exceeded
   the 2-hour activity timeout. The next run marks it `INTERRUPTED`.
2. **Notebook execution.** In the snapshot, find the cell with the
   traceback. The log line `Run failed | run_id=... | stage=...` names
   the stage. For Spark-level detail, open the Spark application from
   the run details. Retry warnings from the API client appear in the
   driver logs.
3. **Run log.**

   ```sql
   SELECT run_id, status, failed_stage, error_message,
          bronze_path, started_at
   FROM ctl_ingestion_runs
   WHERE status IN ('FAILED', 'INTERRUPTED')
   ORDER BY started_at DESC
   LIMIT 10;
   ```

   If no row exists for the failed run, the failure was in `setup`
   (Lakehouse, Spark, or control tables) before the run row was written,
   or the run row write itself failed. Use the snapshot traceback.
4. **Delta audit.** Check what each table actually committed and when:

   ```sql
   DESCRIBE HISTORY silver_github_issues LIMIT 10;
   DESCRIBE HISTORY ctl_ingestion_state LIMIT 10;
   ```

   Compare the `timestamp` and `operationMetrics` values
   (`numTargetRowsInserted`, `numTargetRowsUpdated`) with the failed
   run's `started_at` and `completed_at`.

**Common errors:**

| `error_message` contains | Likely cause | Procedure |
| --- | --- | --- |
| `401 Client Error` | Invalid, expired, or revoked token | [HTTP 401](#recover-from-http-401) |
| `429 Client Error`, `Rate-limit wait exceeds configured maximum`, or `403` with rate limit | GitHub rate limit | [HTTP 429](#recover-from-http-429) |
| `Missing runtime setting` | Key Vault URL or secret name not supplied | [fabric-setup.md](fabric-setup.md) |
| `Pagination exceeded max_pages` | Window larger than `max_pages` × `per_page` (default 10,000 items), usually after a long outage | Raise the `max_pages` base parameter on the activity, publish, and rerun |
| `rejected records ... require review` | Source records failed validation | [Verify data quality](#verify-data-quality) |
| `Silver candidates were not committed` | Concurrent writer or MERGE problem | [Recover after Silver](#recover-after-silver) |
| `Checkpoint verification failed` | Checkpoint write did not persist | [Recover after Silver](#recover-after-silver) |
| `No module named 'builtin.api_ingestion...'` | Shared modules not uploaded to notebook resources | [fabric-setup.md](fabric-setup.md) |

**Final verification:** after the corrective action, a new run reaches
`SUCCESS`.

---

## Recover from HTTP 401

**Symptoms:** run `FAILED` at `bronze` with
`HTTPError: 401 Client Error: Unauthorized for url: https://api.github.com/user`.

**Likely cause:** the token in Key Vault is expired, revoked, or wrong.
The client does not retry 401, because retrying cannot fix it. If instead
the error is from Key Vault (forbidden, or secret not found), the notebook
identity cannot read the secret.

**Investigation:**

1. **Key Vault secret.** In the Azure portal, open the vault and the
   secret named by `github_token_secret_name`. Confirm the secret exists,
   its current version is enabled, and it has not passed its expiry date.
2. **Identity.** Confirm the identity running the notebook has the
   *Key Vault Secrets User* role, or a *Get* secret access policy, on the
   vault. Interactive runs use your account. Pipeline runs use the
   identity shown in the pipeline run details.
3. **Token.** In an interactive notebook session, check what Key Vault
   returns without printing it:

   ```python
   token = notebookutils.credentials.getSecret("<vault-url>", "<secret-name>")
   print(len(token), token[:4])   # prefix only, e.g. "gith" or "ghp_"
   ```

   Then confirm the token itself on GitHub. Under
   **Settings → Developer settings → Personal access tokens**, check that
   it is not expired or revoked and still has read access to
   `microsoft/vscode` issues.

**Corrective action:**

1. Generate a new token on GitHub.
2. In Key Vault, add a **new version** of the same secret. The notebook
   always reads the latest version, so no notebook or pipeline change is
   needed.
3. Rerun the pipeline in incremental mode. The checkpoint did not move,
   so nothing is skipped.

**Final verification:** the new run logs `Authenticated | login=...` and
ends `SUCCESS`.

---

## Recover from HTTP 429

**Symptoms:** run `FAILED` at `bronze` with `429 Client Error`,
`Rate-limit wait exceeds configured maximum: N seconds`, or a `403` that
mentions a rate limit. Successful runs with a high `retries=` count in
the `Bronze written` log line are an early warning.

**Likely cause:** the token's GitHub rate limit is exhausted. This can
come from frequent runs, other jobs sharing the token, or a large
catch-up window.

**How the client handles it** (`client.py`):

| Setting (notebook parameter) | Default | Effect |
| --- | --- | --- |
| `max_attempts` | 4 | Total attempts per request, including the first |
| `max_rate_limit_wait_seconds` | 120 | Fail immediately if the required wait is longer than this |
| `initial_backoff_seconds` | 1 | Backoff for timeouts, 408, and 5xx: doubles per attempt, capped at 30 s, plus jitter |

The wait for a rate-limited response is taken from `Retry-After`. If that
header is missing, the client uses `x-ratelimit-reset` when
`x-ratelimit-remaining` is 0, and otherwise waits 60 seconds.

**Investigation:**

1. Read `error_message` in `ctl_ingestion_runs`. A
   `wait exceeds configured maximum` error includes the reset wait GitHub
   required.
2. Check the token's current limits. This call does not count against the
   limit:

   ```bash
   curl -s -H "Authorization: Bearer <token>" https://api.github.com/rate_limit
   ```

   Look at `resources.core.remaining` and `reset` (epoch seconds).
3. Check whether other workloads use the same token, and how often the
   pipeline schedule runs.

**Corrective action:**

* **Primary limit exhausted** (`remaining` is 0): wait until `reset`,
  then rerun. The checkpoint did not move, so the next run covers the
  same window.
* **Recurring:** reduce the schedule frequency (see
  [Change schedule](#change-schedule)) or give this pipeline its own
  token.
* **Waits slightly over 120 s:** raise the `max_rate_limit_wait_seconds`
  base parameter on the activity, and publish. Each request can wait
  this long up to `max_attempts - 1` times, so keep the total well
  under the 2-hour activity timeout.

**Final verification:** the rerun ends `SUCCESS`, and the `Bronze written`
log line shows `retries=0` or a low count.

---

## Recover after Bronze

Use this when Bronze landed but Silver did not change:
`failed_stage = transform`, or `INTERRUPTED` with `bronze_path` set.

**Symptoms:** a `FAILED` or `INTERRUPTED` row with `bronze_path` set. The
post-run check cell lists the file under *Bronze file(s) await recovery*.

**Likely cause:** a transform error, or a session that ended mid-run. The
checkpoint did not move, so **no data is lost**.

**Investigation:**

1. Read `failed_stage` and `error_message` for the run.
2. List unrecovered Bronze files. This is the notebook's last cell:

   ```python
   display(spark.createDataFrame(unrecovered_bronze_files(config)))
   ```

**Corrective action:**

1. Fix the cause shown in `error_message`, if it is persistent.
2. **Replay incremental extraction.** Rerun the pipeline with
   `mode = incremental`. The run extracts again from the unchanged
   watermark (minus the 5-minute overlap), so it re-fetches everything the
   failed run had, plus any newer changes.
3. Account for the leftover Bronze file. Choose one:
   * **Archive it** (normal after a successful replay):
     `archive_bronze_file(config, "<run_id>", "Superseded by replay <new_run_id>")`
   * **Reprocess it** instead of replaying, if the API cannot be reached:
     run the pipeline with `mode = recover` and
     `recover_run_id = <run_id>`. This
     re-runs transform, MERGE, verification, and checkpoint from the
     existing file, without calling the API.
   * **Archive it as corrupt** if the JSON does not parse. This is
     typical of a session killed while the file was being written.

**Final verification:** the replay run is `SUCCESS`, and the post-run
check cell prints `No unrecovered Bronze files`.

---

## Recover after Silver

Use this when the Silver MERGE may have committed but the run did not
complete: `failed_stage` is `silver_merge`, `silver_verify`, or
`checkpoint`.

**Symptoms:** `FAILED` at one of those stages. Silver may already contain
the run's rows while the checkpoint is behind.

**Likely cause:**

* `silver_merge`: a Spark or Delta error during MERGE.
* `silver_verify`: a concurrent writer, or duplicate `issue_id` values in
  Silver.
* `checkpoint`: rejected records blocked the checkpoint (the most common
  case), or the checkpoint write failed.

**Investigation:**

1. **Verify the checkpoint.** Compare the stored watermark with the
   newest data in Silver:

   ```sql
   SELECT s.watermark, s.last_run_id, s.last_success_at,
          (SELECT MAX(updated_at) FROM silver_github_issues) AS silver_max_updated
   FROM ctl_ingestion_state s;
   ```

   `silver_max_updated` later than `watermark` is expected after a failure
   in these stages. It means the checkpoint correctly did not advance.
2. **Check whether the MERGE committed.** Run
   `DESCRIBE HISTORY silver_github_issues LIMIT 5;` and look for a
   `MERGE` between the run's `started_at` and `completed_at`.
3. **If rejected records caused it**, follow
   [Verify data quality](#verify-data-quality) first.

**Corrective action:**

1. Resolve the cause (rejected records, duplicates, or a Spark error).
2. **Replay the Delta MERGE.** Rerun with `mode = incremental`. The
   watermark is unchanged, so the run re-fetches the window and MERGEs
   again. The MERGE is idempotent: rows already committed are no-ops,
   because only newer, or same-timestamp-but-changed, versions replace
   stored rows. To replay from the existing Bronze file without the API,
   use `mode = recover` with the failed run's ID.
3. If the replay succeeded in incremental mode, archive the failed run's
   Bronze file, as in [Recover after Bronze](#recover-after-bronze).

**Final verification:**

* The replay run is `SUCCESS`, and `watermark` now equals or exceeds the
  `silver_max_updated` from step 1.
* The uniqueness check in [Verify data quality](#verify-data-quality)
  passes.

---

## Verify data quality

Run after any recovery, and periodically.

**Uniqueness** (expect `duplicate_issue_ids = 0`):

```sql
SELECT COUNT(*) - COUNT(DISTINCT issue_id) AS duplicate_issue_ids
FROM silver_github_issues;
```

**Null and consistency checks** (expect 0 for each). Required columns are
`NOT NULL` in the table definition, so Delta rejects nulls on write. These
checks cover the rules the schema cannot enforce:

```sql
SELECT
  SUM(CASE WHEN title = '' OR repository = '' THEN 1 ELSE 0 END) AS blank_required,
  SUM(CASE WHEN state NOT IN ('open', 'closed') THEN 1 ELSE 0 END) AS invalid_state,
  SUM(CASE WHEN state = 'closed' AND closed_at IS NULL THEN 1 ELSE 0 END) AS closed_without_date,
  SUM(CASE WHEN updated_at < created_at THEN 1 ELSE 0 END) AS updated_before_created,
  SUM(CASE WHEN closed_at < created_at THEN 1 ELSE 0 END) AS closed_before_created
FROM silver_github_issues;
```

`closed_without_date` is a source anomaly, not a pipeline fault. Report
it rather than correcting it.

**Rejected-record counts** (expect 0 recently):

```sql
SELECT run_id, started_at, rejected_records, status, failed_stage
FROM ctl_ingestion_runs
WHERE rejected_records > 0
ORDER BY started_at DESC;
```

For each such run, inspect
`Files/quarantine/github_issues/rejected_<run_id>.jsonl`. Each line has
`record_index`, `reason`, and the original `record`.

* **Validation rule too strict** (the source is legitimate): fix
  `api_ingestion/validation.py`, re-upload the shared modules, and rerun.
* **Bad source data**: the run is blocked until the record changes at the
  source, or the validation rule is changed. Rejected records stop the
  checkpoint on purpose, so they cannot be silently skipped.

**Final verification:** all checks return 0, and the latest run is
`SUCCESS` with `rejected_records = 0`.

---

## Review execution history

```sql
-- Recent runs
SELECT run_id, mode, status, failed_stage, started_at, completed_at,
       bronze_records, silver_candidates, rejected_records,
       recovered_from_run_id, error_message
FROM ctl_ingestion_runs
ORDER BY started_at DESC
LIMIT 50;

-- Outcomes and duration over the last 7 days
SELECT status, COUNT(*) AS runs,
       ROUND(AVG(unix_timestamp(completed_at) - unix_timestamp(started_at))) AS avg_seconds
FROM ctl_ingestion_runs
WHERE started_at >= current_timestamp() - INTERVAL 7 DAYS
  AND status <> 'ARCHIVED'
GROUP BY status;

-- Recoveries and archives
SELECT run_id, mode, status, recovered_from_run_id, error_message, started_at
FROM ctl_ingestion_runs
WHERE recovered_from_run_id IS NOT NULL
ORDER BY started_at DESC;
```

Rows written before the hardening release have `NULL` in `mode`,
`failed_stage`, `bronze_path`, and `recovered_from_run_id`.

**What to look for:** repeated `FAILED` rows at the same stage, rising
`avg_seconds`, `INTERRUPTED` rows (overlapping or killed runs), and
gaps longer than the schedule interval.

---

## Change schedule

1. Open the pipeline and select **Schedule** on the toolbar.
2. Turn the schedule **On**. Set **Repeat** and the interval, the start
   and end dates, and the **time zone**, then select **Apply**.
3. To pause ingestion, set the schedule **Off**. No catch-up is needed
   afterwards: the next run extracts from the checkpoint.

**Choosing an interval:**

* Keep the interval longer than the longest recent run (`avg_seconds` in
  [Review execution history](#review-execution-history)). With
  concurrency 1, a run that is still going when the next is due makes
  the next one wait in the queue.
* More frequent runs use more of the GitHub rate limit. See
  [HTTP 429](#recover-from-http-429).
* Long pauses produce larger windows. A window over `max_pages` ×
  `per_page` items fails with `Pagination exceeded max_pages`. Raise
  `max_pages` for the first run after a long pause.

**Final verification:** the next scheduled run appears in the
**Monitor** hub at the expected time and records a `SUCCESS` row.

---

## Reference

### Checkpoint rules

The watermark advances only when all of the following hold:

1. The extraction is complete. The client raises instead of truncating at
   `max_pages`.
2. The MERGE has executed.
3. Verification confirms every candidate is in Silver and `issue_id` is
   unique.
4. No records were quarantined.

The Silver MERGE and the checkpoint are separate Delta transactions. The
order (Silver first, then checkpoint) gives at-least-once delivery: a
failure between them causes a safe re-extraction.

### Same-timestamp upsert policy

GitHub `updated_at` has one-second precision. The rules are in
`fabric_policy.py` and covered by `tests/test_fabric_policy.py`.

| Incoming vs Silver | Result |
| --- | --- |
| Newer `updated_at` | Replace |
| Older `updated_at` | Keep Silver |
| Equal `updated_at`, later `ingested_at`, content differs | Replace (later observation is more current) |
| Equal `updated_at`, content identical | Keep Silver (no rewrite; lineage keeps the first delivering run) |
| Equal `updated_at`, earlier or equal `ingested_at` | Keep Silver (reprocessing old Bronze cannot roll back) |

Content comparison covers `issue_number`, `repository`, `title`, `state`,
`author_login`, `labels_json`, `created_at`, and `closed_at`, using
null-safe equality. Within one Bronze file, the later-fetched record wins
a tie.
