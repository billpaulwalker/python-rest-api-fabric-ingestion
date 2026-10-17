
"""SQLite execution history for local ingestion."""

import sqlite3

from datetime import datetime, timezone
from pathlib import Path


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class RunStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")

        self._init_schema()

    def _init_schema(self):
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            bronze_file TEXT,
            silver_file TEXT,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_stages (
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            details TEXT,
            PRIMARY KEY(run_id, stage),
            FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS silver_checkpoints (
            source TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            silver_file TEXT NOT NULL,
            bronze_fingerprint TEXT NOT NULL
        );
        """)

    def mark_interrupted(self):
        """Classify unfinished executions from previous attempts."""

        with self.conn:
            self.conn.execute("""
                UPDATE pipeline_stages
                SET status='INTERRUPTED', ended_at=?
                WHERE status='RUNNING'
            """, (utc_now(),))

            self.conn.execute("""
                UPDATE pipeline_runs
                SET status='INTERRUPTED', ended_at=?
                WHERE status='RUNNING'
            """, (utc_now(),))

    def start_run(self, run_id, source, mode):
        with self.conn:
            self.conn.execute("""
                INSERT INTO pipeline_runs
                    (run_id, source, mode, status, started_at)
                VALUES (?, ?, ?, 'RUNNING', ?)
            """, (run_id, source, mode, utc_now()))

    def start_stage(self, run_id, stage):
        with self.conn:
            self.conn.execute("""
                INSERT INTO pipeline_stages
                    (run_id, stage, status, started_at)
                VALUES (?, ?, 'RUNNING', ?)
            """, (run_id, stage, utc_now()))

    def finish_stage(self, run_id, stage, status, details=None):
        with self.conn:
            self.conn.execute("""
                UPDATE pipeline_stages
                SET status=?, ended_at=?, details=?
                WHERE run_id=? AND stage=?
            """, (
                status, utc_now(), details, run_id, stage
            ))

    def set_bronze_file(self, run_id, file):
        with self.conn:
            self.conn.execute("""
                UPDATE pipeline_runs
                SET bronze_file=?
                WHERE run_id=?
            """, (str(file), run_id))

    def finish_run(self, run_id, status, error=None):
        with self.conn:
            self.conn.execute("""
                UPDATE pipeline_runs
                SET status=?, ended_at=?, error=?
                WHERE run_id=?
            """, (status, utc_now(), error, run_id))

    def publish_silver(
        self, run_id, source, silver_file, fingerprint
    ):
        """Commit Silver stage success and checkpoint together."""

        now = utc_now()

        with self.conn:
            self.conn.execute("""
                UPDATE pipeline_stages
                SET status='SUCCESS', ended_at=?, details=?
                WHERE run_id=? AND stage='silver'
            """, (
                now, str(silver_file), run_id
            ))

            self.conn.execute("""
                INSERT INTO silver_checkpoints
                    (source, run_id, completed_at,
                     silver_file, bronze_fingerprint)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source) DO UPDATE SET
                    run_id=excluded.run_id,
                    completed_at=excluded.completed_at,
                    silver_file=excluded.silver_file,
                    bronze_fingerprint=excluded.bronze_fingerprint
            """, (
                source, run_id, now,
                str(silver_file), fingerprint
            ))

            self.conn.execute("""
                UPDATE pipeline_runs
                SET status='SUCCESS',
                    ended_at=?,
                    silver_file=?
                WHERE run_id=?
            """, (
                now, str(silver_file), run_id
            ))

    def close(self):
        self.conn.close()
