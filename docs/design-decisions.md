# Design Decisions

Key decisions behind the ingestion framework, with the reasoning and
trade-offs. Each entry records a decision already made in the code.

## 1. One shared Python package for local and Fabric runtimes

**Decision:** The API client, pagination, transformation, validation, and
Fabric upsert policy live in `src/api_ingestion`. The Fabric notebook
imports the same package from its built-in resources
(`builtin.api_ingestion`).

**Why:** Validation and transformation rules must behave the same in both
environments. Keeping them in a plain Python package means they are unit
tested locally, without Spark.

**Trade-off:** The package must be re-uploaded to the notebook resources
after each change. Storage, state, and orchestration are implemented
separately per runtime.

## 2. Bronze is one immutable raw JSON file per run

**Decision:** Each extraction lands the complete API response as
`github_issues_<run_id>.json`, unmodified. Bronze files are never edited
or deleted by the pipeline.

**Why:**

- Raw data stays available for audit and reprocessing.
- Naming the file by run ID ties it to the run history.

**Trade-off:** Bronze grows with every run, including overlap and
pull-request records. Retention is a separate operational concern.

## 3. Incremental extraction by `updated_at` watermark with overlap

**Decision:** Each run requests issues with `since = watermark − 5
minutes`, sorted by `updated` ascending. The first run looks back 7 days.

- Records updated after the run started are left for the next run.
- Pagination that would exceed `max_pages` fails the run instead of
  returning a partial window.

**Why:**

- The overlap tolerates clock skew and same-second updates at the
  boundary.
- Failing rather than truncating guarantees that a Bronze file always
  covers its whole window, which is what makes advancing the watermark
  safe.

**Trade-off:**

- Every run re-fetches a few overlapping records.
- Changes older than the overlap need a manual checkpoint rewind (see the
  runbook).

## 4. The checkpoint advances only after a verified Silver commit

**Decision:** The watermark in `ctl_ingestion_state` moves only after all
of the following:

- the Delta MERGE has run;
- verification confirms that every candidate is present in Silver and
  that issue IDs are unique;
- no records were quarantined.

The watermark is monotonic: the checkpoint MERGE never moves it backward.

**Why:** Silver and the checkpoint are separate Delta transactions, and
Fabric offers no cross-table transaction. With Silver committed first and
the checkpoint second, any failure leaves the watermark behind. The next
run re-extracts the window, so delivery is at-least-once with no data
loss.

**Trade-off:** A failed run can leave Silver ahead of the checkpoint. That
is safe only because the MERGE is idempotent (decision 5).

## 5. Same-timestamp upsert policy

**Decision:** Silver keeps one row per `issue_id`. The MERGE update rule
in `fabric_policy.py` is:

| Incoming vs Silver | Result |
| --- | --- |
| Newer `updated_at` | Replace |
| Older `updated_at` | Keep Silver |
| Equal `updated_at`, later observation, content differs | Replace |
| Equal `updated_at`, identical content | Keep Silver (no rewrite) |
| Equal `updated_at`, earlier or same observation | Keep Silver |

Within one Bronze file, the later-fetched record wins a tie.

**Why:**

- GitHub `updated_at` has one-second precision, so two versions can share
  a timestamp. The API returns current state at fetch time, so the later
  observation is the more current one.
- Skipping identical content means overlap re-fetches cause no rewrites,
  and lineage (`ingested_at`, `source_run_id`) keeps pointing at the run
  that first delivered the version.
- Comparing observation time stops a reprocessed old Bronze file from
  rolling Silver back.

**Trade-off:** The rule compares a fixed list of business columns. A new
Silver column must be added to `CONTENT_COLUMNS` to take part in
same-timestamp comparisons.

## 6. Rejected records block the checkpoint

**Decision:** Records that fail validation are written to quarantine with
the rejection reason. Valid records from the same batch are still merged,
but the run fails before advancing the checkpoint.

**Why:** Skipping a bad record and advancing past it would lose that
issue silently. Blocking forces a decision: fix the validation rule, or
confirm the source anomaly.

**Trade-off:** One persistent bad record stops the watermark until someone
acts. Each run re-extracts the window and fails at the same point.

## 7. Every execution is tracked, including failures

**Decision:** All notebook work runs in one `run_ingestion()` function
wrapped in try/except/finally. Each run writes a row to
`ctl_ingestion_runs`: `RUNNING` at start, then `SUCCESS`, `FAILED` (with
`failed_stage` and `error_message`), or `INTERRUPTED`.

- A run left `RUNNING` by a killed session is marked `INTERRUPTED` by the
  next run.
- If the final status cannot be written, the original error is still
  raised.
- A success that cannot be recorded is reported as a failure.

**Why:** Linear notebook cells only produced a run-log row on success, so
failures left no trace. A single entry point makes logging consistent and
lets the pipeline see failures as failures.

**Trade-off:** The run log is another Delta table outside any
transaction, so it is best-effort. Recovery relies on the checkpoint, not
on the log.

## 8. Recovery is replay first, with explicit Bronze accounting

**Decision:** The default recovery is to rerun incrementally, because the
unchanged watermark re-extracts the failed window. Bronze files from
unsuccessful runs are tracked, not deleted:

- `recover` mode reprocesses a specific Bronze file without calling the
  API, keeping the original run ID and start time as lineage;
- `archive_bronze_file()` moves corrupt or superseded files to
  `_archive/` and records an `ARCHIVED` row.

**Why:** Replay needs no special tooling and is always safe (decisions 4
and 5). Recover mode covers cases where the API is unavailable, or the
original observation needs to be preserved.

**Trade-off:** Uncommitted Bronze files accumulate until an operator
archives or reprocesses them. The notebook lists them after every run.

## 9. API retries are bounded and rate-limit aware

**Decision:**

- Timeouts, 408, and 5xx responses retry with exponential backoff and
  jitter.
- Rate-limit responses (429, or 403 with an exhausted quota) wait for
  `Retry-After` or the quota reset.
- Each request makes at most 4 attempts. The client fails immediately if
  the required wait exceeds 120 seconds.
- 401, other 403s, and other 4xx responses are not retried.
- Pagination URLs are validated against the GitHub API host.

**Why:** Transient errors should not fail a run, but a long rate-limit
wait or a bad credential should fail fast and visibly. Waiting for an
hour inside a notebook session is worse than rerunning after the reset.

**Trade-off:** A run near the rate limit fails rather than waiting it
out, and needs a rerun.

## 10. Secrets and environment settings stay outside source control

**Decision:**

- The GitHub token is read from Azure Key Vault at runtime.
- The Key Vault URL and secret name come from pipeline parameters, or
  from Spark properties on the Fabric environment.
- The notebook parameters cell has blank defaults for both.
- Exported notebooks are committed without outputs or widget state.
- Tests fail if outputs, token patterns, or Key Vault URLs appear in the
  notebook.

**Why:** Notebook exports capture cell outputs, including identities, run
IDs, and data previews, and hardcoded settings tie the source to one
environment. Tests catch regressions when the notebook is re-exported.

**Trade-off:** Interactive runs need the environment Spark properties.
The pipeline definition still carries environment-specific (non-secret)
values, which deployment rules must override per environment.

## 11. The Fabric pipeline runs serially and does not auto-retry

**Decision:** `pl_github_issues_ingestion` has concurrency 1, a 2-hour
activity timeout, and retry 0. `mode` and `recover_run_id` are pipeline
parameters. The notebook returns its result as the exit value.

**Why:**

- Overlapping runs would mark each other `INTERRUPTED` and race on the
  same tables.
- An automatic retry would hide failures that need attention, such as
  rejected records or an expired token.
- The exit value lets downstream activities use the run result.

**Trade-off:** A transient failure waits for the next scheduled run or a
manual rerun.

## 12. Local runtime uses snapshots and SQLite

**Decision:** Locally, Silver is rebuilt from all Bronze files as one
Parquet snapshot, replaced atomically. Run, stage, and checkpoint history
is kept in SQLite. The Silver stage result and its checkpoint commit in
one SQLite transaction.

**Why:** A full rebuild is simple and naturally idempotent at small
volumes. SQLite gives durable, transactional run tracking with no
infrastructure.

**Trade-off:**

- Rebuild cost grows with Bronze volume.
- Same-run ties are broken by content ordering, not fetch order as in
  Fabric.
- The extraction watermark is a separate JSON file, so it can advance
  before Silver completes.

## 13. Notebook logic is tested locally against stubs

**Decision:** `tests/test_notebook.py` executes the notebook's own code
cells, with Spark, Delta, and `notebookutils` replaced by stubs. It checks
stage ordering, failure logging, checkpoint gating, cancellation, and
recover mode.

**Why:** The notebook cannot run outside Fabric, but its control flow is
where the reliability guarantees live. Testing the real cell source
avoids a second copy of the logic drifting from the notebook.

**Trade-off:** Spark and Delta behaviour itself (MERGE semantics, schema
enforcement) is not exercised locally. It is verified in Fabric.
