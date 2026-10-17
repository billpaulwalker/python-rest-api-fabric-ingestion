# Fabric Operations: GitHub Issues Notebook

Operating guide for `notebooks/nb_github_issues_ingestion.ipynb`.

## Execution model

All work runs inside `run_ingestion()`. Each execution writes one row to
`ctl_ingestion_runs`. The row is created as `RUNNING` and updated after
the Bronze file is written. It ends in exactly one final status:

| Status | Meaning |
| --- | --- |
| `SUCCESS` | Silver committed and verified, checkpoint advanced (if any candidates) |
| `FAILED` | An exception occurred; `failed_stage` and `error_message` say where and why |
| `INTERRUPTED` | The run was cancelled, or a later run found it still `RUNNING` (session killed) |
| `ARCHIVED` | Audit row written by `archive_bronze_file()`; not a pipeline execution |

Stages, in order: `setup`, `bronze` (or `recovery_load`), `transform`,
`silver_merge`, `silver_verify`, `checkpoint`.

A failure re-raises after it is logged, so the notebook activity and any
pipeline fail too. If the final status cannot be written, the original
error is still raised. A successful run whose status cannot be recorded
is reported as a failure.

Run one execution per source at a time (pipeline concurrency = 1). On
startup a run marks any `RUNNING` row for its source as `INTERRUPTED`.

## Checkpoint rules

`ctl_ingestion_state.watermark` is the highest `updated_at` known to be in
Silver. It advances only when all of the following hold:

1. The Bronze extraction is complete. `get_issues` raises instead of
   truncating at `max_pages`.
2. The Delta MERGE has executed.
3. Verification confirms that every candidate is in Silver, at a version
   the upsert policy would not replace, and that `issue_id` is unique.
4. No records were quarantined.

The checkpoint MERGE only moves the watermark forward. After writing, the
stored value is read back and checked.

The Silver MERGE and the checkpoint are separate Delta transactions. The
ordering gives at-least-once behaviour: a failure between them leaves the
watermark behind, and the next run re-extracts the window. That is safe
because the MERGE is idempotent.

**Rejected records.** Valid records from the batch are merged, but the
run fails at `checkpoint` and the window is re-extracted on every run
until the records in `Files/quarantine/github_issues/rejected_<run_id>.jsonl`
are resolved, either by fixing validation or by confirming the source
data changed.

## Same-timestamp upsert policy

GitHub `updated_at` has one-second precision. The rules live in
`api_ingestion/fabric_policy.py` (`should_replace`,
`merge_update_condition`) and are covered by `tests/test_fabric_policy.py`.

| Incoming vs Silver | Result |
| --- | --- |
| Newer `updated_at` | Replace |
| Older `updated_at` | Keep Silver |
| Equal `updated_at`, later `ingested_at`, content differs | Replace (later observation is more current) |
| Equal `updated_at`, content identical | Keep Silver (no rewrite; lineage keeps first delivering run) |
| Equal `updated_at`, earlier or equal `ingested_at` | Keep Silver (reprocessing old Bronze cannot roll back) |

Content comparison covers `issue_number`, `repository`, `title`,
`state`, `author_login`, `labels_json`, `created_at`, and `closed_at`,
using null-safe equality.

Within one Bronze file, the later-fetched record wins a tie.

## Recovering Bronze files from unsuccessful runs

A run that fails after the Bronze stage leaves
`Files/bronze/github_issues/github_issues_<run_id>.json` behind. Its
checkpoint did not advance, so **no data is lost**: the next successful
incremental run re-extracts the same window. Recovery is about
accounting for the file, not repairing Silver.

1. **Find them.** The notebook's last cell runs
   `unrecovered_bronze_files(config)`. It lists every Bronze file not
   committed by a successful run or recovery, with its run status
   (`FAILED`, `INTERRUPTED`, or `UNTRACKED` for files with no run row).
2. **Check the cause.** Look in `ctl_ingestion_runs` for `failed_stage`
   and `error_message`. Fix the cause before reprocessing. For example,
   resolve quarantined records first, or the recovery run will fail at
   the same point.
3. **Choose one action per file:**
   - **Reprocess** a file that holds complete data you want committed
     (for example, the API window has since aged out). Run the notebook
     with `mode="recover"` and `recover_run_id="<run_id>"`. The run reads
     the existing file without calling the API and keeps the original
     run ID and start time as Silver lineage, so the upsert policy cannot
     overwrite newer data. It then goes through the same verify and
     checkpoint gates. On success the file counts as committed.
   - **Archive** a file that is corrupt (the JSON does not parse;
     typically `INTERRUPTED` during the Bronze write) or that you
     deliberately will not reprocess:
     `archive_bronze_file(config, "<run_id>", "<reason>")`. This moves it
     to `Files/bronze/github_issues/_archive/` and writes an `ARCHIVED`
     audit row.
   - **Leave it** if a later incremental run already covered the window.
     It stays listed as a reminder until it is reprocessed or archived.
4. **Confirm.** Re-run the last cell. The file should no longer be listed.

Committed files cannot be reprocessed or archived through these helpers.
Bronze files are never deleted by the notebook.

## Run history queries

```sql
SELECT run_id, mode, status, failed_stage, started_at, completed_at,
       bronze_records, silver_candidates, rejected_records,
       recovered_from_run_id, error_message
FROM ctl_ingestion_runs
ORDER BY started_at DESC;

SELECT * FROM ctl_ingestion_state;
```
