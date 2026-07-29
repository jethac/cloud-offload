"""SQLite-based job queue."""

import hashlib
import hmac
import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any


# What a worker may report about itself. ``starting`` is a runner that has told
# the coordinator it exists but is still bringing ComfyUI up, and ``failed`` is
# one that never managed to; only the first two are a worker the dispatcher can
# still expect work from.
WORKER_STATUSES = ("starting", "active", "failed")
LIVE_WORKER_STATUSES = ("starting", "active")


class JobStatus(str, Enum):
    """Job lifecycle states."""
    PENDING = "pending"           # Created, needs local preview
    PREVIEW_DONE = "preview_done" # Local preview complete, waiting for user
    QUEUED = "queued"             # User approved, waiting for cloud
    DISPATCHED = "dispatched"     # Sent to cloud worker
    RUNNING = "running"           # Worker processing
    COMPLETED = "completed"       # Done, result available
    FAILED = "failed"             # Error occurred
    DEAD_LETTER = "dead_letter"   # Retry limit exceeded


@dataclass
class Job:
    """A cloud offload job."""
    id: str
    model: str
    status: JobStatus

    # Input
    input_path: str  # Path/URI to input
    params: dict = field(default_factory=dict)  # Scheduling/routing params
    request: dict = field(default_factory=dict)
    provider: str | None = None

    # Output
    preview_path: str | None = None
    result_path: str | None = None
    result: dict | None = None
    progress: int = 0

    # Metadata
    created_at: str = ""
    updated_at: str = ""
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    worker_id: str | None = None
    attempts: int = 0
    max_attempts: int = 3
    schema_version: int = 3

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.utcnow().isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at
        if isinstance(self.status, str):
            self.status = JobStatus(self.status)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Job":
        return cls(**d)


class JobQueue:
    """SQLite-backed job queue."""

    SCHEMA_VERSION = 5

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    model TEXT NOT NULL,
                    status TEXT NOT NULL,
                    input_path TEXT NOT NULL,
                    params TEXT,
                    preview_path TEXT,
                    result_path TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    error TEXT,
                    worker_id TEXT,
                    attempts INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 3,
                    schema_version INTEGER DEFAULT 3,
                    request_json TEXT,
                    provider TEXT,
                    result_json TEXT,
                    progress INTEGER DEFAULT 0
                )
            """)
            existing_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
            }
            migrations = {
                "attempts": "ALTER TABLE jobs ADD COLUMN attempts INTEGER DEFAULT 0",
                "max_attempts": "ALTER TABLE jobs ADD COLUMN max_attempts INTEGER DEFAULT 3",
                "schema_version": "ALTER TABLE jobs ADD COLUMN schema_version INTEGER DEFAULT 3",
                "request_json": "ALTER TABLE jobs ADD COLUMN request_json TEXT",
                "provider": "ALTER TABLE jobs ADD COLUMN provider TEXT",
                "result_json": "ALTER TABLE jobs ADD COLUMN result_json TEXT",
                "progress": "ALTER TABLE jobs ADD COLUMN progress INTEGER DEFAULT 0",
            }
            for column, statement in migrations.items():
                if column not in existing_columns:
                    conn.execute(statement)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS queue_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            conn.execute(
                """
                INSERT INTO queue_meta (key, value) VALUES ('schema_version', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(self.SCHEMA_VERSION),),
            )
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_provider_status ON jobs(provider, status)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_job_events_job_sequence
                ON job_events(job_id, sequence)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS partition_cache (
                    cache_key TEXT PRIMARY KEY,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    idle_since TEXT,
                    runtime_profile TEXT,
                    capabilities TEXT,
                    detail TEXT
                )
            """)
            worker_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(workers)").fetchall()
            }
            if "runtime_profile" not in worker_columns:
                conn.execute("ALTER TABLE workers ADD COLUMN runtime_profile TEXT")
            if "capabilities" not in worker_columns:
                conn.execute("ALTER TABLE workers ADD COLUMN capabilities TEXT")
            if "idle_since" not in worker_columns:
                conn.execute("ALTER TABLE workers ADD COLUMN idle_since TEXT")
            if "detail" not in worker_columns:
                conn.execute("ALTER TABLE workers ADD COLUMN detail TEXT")

    def _get_meta(self, key: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT value FROM queue_meta WHERE key = ?", (key,)
            ).fetchone()
            return row[0] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO queue_meta (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    @staticmethod
    def _hash_worker_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def set_worker_token(self, token: str) -> None:
        """Require workers to present this token before claiming jobs."""
        if not token:
            raise ValueError("Worker token must not be empty")
        self._set_meta("worker_token_sha256", self._hash_worker_token(token))

    def _verify_worker_token(self, token: str | None) -> None:
        expected = self._get_meta("worker_token_sha256")
        if not expected:
            return
        if not token:
            raise PermissionError("Worker token is required to claim jobs")
        actual = self._hash_worker_token(token)
        if not hmac.compare_digest(actual, expected):
            raise PermissionError("Worker token is invalid")

    def _row_to_job(self, row: tuple) -> Job:
        """Convert database row to Job object."""
        return Job(
            id=row[0],
            model=row[1],
            status=JobStatus(row[2]),
            input_path=row[3],
            params=json.loads(row[4]) if row[4] else {},
            preview_path=row[5],
            result_path=row[6],
            created_at=row[7],
            updated_at=row[8],
            started_at=row[9],
            completed_at=row[10],
            error=row[11],
            worker_id=row[12],
            attempts=row[13] if len(row) > 13 and row[13] is not None else 0,
            max_attempts=row[14] if len(row) > 14 and row[14] is not None else 3,
            schema_version=(
                row[15]
                if len(row) > 15 and row[15] is not None
                else self.SCHEMA_VERSION
            ),
            request=json.loads(row[16]) if len(row) > 16 and row[16] else {},
            provider=row[17] if len(row) > 17 else None,
            result=json.loads(row[18]) if len(row) > 18 and row[18] else None,
            progress=row[19] if len(row) > 19 and row[19] is not None else 0,
        )

    def create(
        self,
        model: str,
        input_path: str,
        params: dict | None = None,
        request: dict | None = None,
        provider: str | None = None,
        status: JobStatus = JobStatus.PENDING,
    ) -> Job:
        """Create a new job."""
        job = Job(
            id=str(uuid.uuid4()),
            model=model,
            status=status,
            input_path=input_path,
            params=params or {},
            request=request or {},
            provider=provider,
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    id, model, status, input_path, params,
                    preview_path, result_path, created_at, updated_at,
                    started_at, completed_at, error, worker_id,
                    attempts, max_attempts, schema_version, request_json,
                    provider, result_json, progress
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id, job.model, job.status.value, job.input_path,
                    json.dumps(job.params), job.preview_path, job.result_path,
                    job.created_at, job.updated_at, job.started_at,
                    job.completed_at, job.error, job.worker_id,
                    job.attempts, job.max_attempts, job.schema_version,
                    json.dumps(job.request), job.provider,
                    json.dumps(job.result) if job.result is not None else None,
                    job.progress,
                ),
            )
        return job

    def get(self, job_id: str) -> Job | None:
        """Get job by ID."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            return self._row_to_job(row) if row else None

    def update(self, job: Job) -> None:
        """Update job in database."""
        job.updated_at = datetime.utcnow().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE jobs SET
                    model = ?, status = ?, input_path = ?, params = ?,
                    preview_path = ?, result_path = ?, updated_at = ?,
                    started_at = ?, completed_at = ?, error = ?, worker_id = ?,
                    attempts = ?, max_attempts = ?, schema_version = ?,
                    request_json = ?, provider = ?, result_json = ?, progress = ?
                WHERE id = ?
                """,
                (
                    job.model, job.status.value, job.input_path,
                    json.dumps(job.params), job.preview_path, job.result_path,
                    job.updated_at, job.started_at, job.completed_at,
                    job.error, job.worker_id, job.attempts, job.max_attempts,
                    job.schema_version, json.dumps(job.request), job.provider,
                    json.dumps(job.result) if job.result is not None else None,
                    job.progress, job.id,
                ),
            )

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        error: str | None = None,
        **kwargs,
    ) -> Job | None:
        """Update job status and optional fields."""
        job = self.get(job_id)
        if not job:
            return None

        # Terminal states are immutable. In particular, a worker may finish a
        # blocking download just after the user cancels; its late `running`,
        # `complete`, or `fail` callback must not resurrect the job or enqueue
        # another retry behind the user's back.
        if job.status in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.DEAD_LETTER,
        }:
            return job

        job.status = status
        if error is not None:
            job.error = error
        elif status in (JobStatus.RUNNING, JobStatus.COMPLETED):
            # A successful retry must not continue to expose the previous
            # attempt's failure through the job API.
            job.error = None

        for key, value in kwargs.items():
            if hasattr(job, key):
                setattr(job, key, value)

        if status == JobStatus.RUNNING:
            job.started_at = datetime.utcnow().isoformat()
        elif status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.DEAD_LETTER):
            job.completed_at = datetime.utcnow().isoformat()
        else:
            job.completed_at = None

        self.update(job)
        return job

    def list_by_status(
        self, *statuses: JobStatus, provider: str | None = None
    ) -> list[Job]:
        """Get all jobs with given status(es)."""
        placeholders = ",".join("?" * len(statuses))
        status_values = [s.value for s in statuses]

        provider_clause = " AND provider = ?" if provider else ""
        values: list[Any] = [*status_values]
        if provider:
            values.append(provider)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"SELECT * FROM jobs WHERE status IN ({placeholders})"
                f"{provider_clause} ORDER BY created_at",
                values,
            ).fetchall()
            return [self._row_to_job(row) for row in rows]

    def count_by_status(
        self, *statuses: JobStatus, provider: str | None = None
    ) -> int:
        """Count jobs with given status(es)."""
        placeholders = ",".join("?" * len(statuses))
        status_values = [s.value for s in statuses]

        provider_clause = " AND provider = ?" if provider else ""
        values: list[Any] = [*status_values]
        if provider:
            values.append(provider)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM jobs WHERE status IN ({placeholders})"
                f"{provider_clause}",
                values,
            ).fetchone()
            return row[0] if row else 0

    def claim_jobs(
        self,
        worker_id: str,
        limit: int = 5,
        token: str | None = None,
        provider: str | None = None,
        models: list[str] | None = None,
        runtime_profile: str | None = None,
        gpu_vram_gb: float | None = None,
        gpu_name: str | None = None,
    ) -> list[Job]:
        """
        Atomically claim queued jobs for a worker.
        Returns list of claimed jobs.
        """
        self._verify_worker_token(token)
        with sqlite3.connect(self.db_path) as conn:
            # Get job IDs to claim
            provider_clause = " AND provider = ?" if provider else ""
            models_clause = ""
            gpu_clause = ""
            values: list[Any] = [JobStatus.QUEUED.value]
            if provider:
                values.append(provider)
            if models is not None:
                if not models:
                    return []
                model_placeholders = ",".join("?" * len(models))
                models_clause = f" AND model IN ({model_placeholders})"
                values.extend(models)
            if gpu_vram_gb is not None:
                gpu_clause += (
                    " AND COALESCE(CAST(json_extract(params, '$.min_gpu_ram_gb') "
                    "AS REAL), 0) <= ?"
                )
                # A requirement is typed in the size a card is sold as, while a
                # driver reports what it can actually address: an A5000 sold as
                # 24 GB reports 24564 MiB, or 23.99 GiB. Comparing those raw made
                # a worker refuse every job its own GPU was rented to run, and
                # the job simply waited. Round to the nearest whole GiB so both
                # sides speak the same units.
                values.append(round(max(0.0, float(gpu_vram_gb))))
            if gpu_name:
                # Treat "any"/missing as unconstrained. Normalizing separators makes
                # provider labels such as RTX_4090 match NVIDIA GeForce RTX 4090.
                gpu_clause += """
                    AND (
                        COALESCE(lower(json_extract(params, '$.gpu_type')), 'any') = 'any'
                        OR replace(replace(lower(?), '_', ' '), '-', ' ')
                           LIKE '%' || replace(replace(
                               lower(json_extract(params, '$.gpu_type')), '_', ' '
                           ), '-', ' ') || '%'
                    )
                """
                values.append(str(gpu_name))
            values.append(limit)
            rows = conn.execute(
                f"""
                SELECT id FROM jobs
                WHERE status = ?
                {provider_clause}
                {models_clause}
                {gpu_clause}
                ORDER BY created_at
                LIMIT ?
                """,
                values,
            ).fetchall()

            job_ids = [row[0] for row in rows]
            if not job_ids:
                return []

            # Claim them atomically
            now = datetime.utcnow().isoformat()
            placeholders = ",".join("?" * len(job_ids))
            conn.execute(
                f"""
                UPDATE jobs SET
                    status = ?,
                    worker_id = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id IN ({placeholders})
                """,
                [JobStatus.DISPATCHED.value, worker_id, now] + job_ids,
            )

        return [job for job in (self.get(job_id) for job_id in job_ids) if job is not None]

    def authorize_worker(self, token: str | None) -> None:
        """Verify a worker credential for coordinator operations."""
        if not self._get_meta("worker_token_sha256"):
            raise PermissionError("Worker authentication is not configured")
        self._verify_worker_token(token)

    def set_progress(self, job_id: str, progress: int) -> Job | None:
        """Update bounded job progress."""
        job = self.get(job_id)
        if not job:
            return None
        if job.status in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.DEAD_LETTER,
        }:
            return job
        job.progress = max(0, min(100, int(progress)))
        self.update(job)
        return job

    def append_event(self, job_id: str, event: dict[str, Any]) -> dict[str, Any]:
        """Append an immutable, resumable execution event for a cloud job."""
        if not self.get(job_id):
            raise KeyError(f"Job not found: {job_id}")
        if not isinstance(event, dict) or not event.get("type"):
            raise ValueError("Job events require a non-empty type")
        try:
            encoded = json.dumps(event, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("Job event is not finite JSON") from exc
        if len(encoded.encode("utf-8")) > 4 * 1024 * 1024:
            raise ValueError("Job event exceeds the 4 MiB limit")
        created_at = datetime.utcnow().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO job_events (job_id, event_json, created_at)
                VALUES (?, ?, ?)
                """,
                (job_id, encoded, created_at),
            )
            sequence = int(cursor.lastrowid)
        return {
            "sequence": sequence,
            "job_id": job_id,
            "created_at": created_at,
            "event": event,
        }

    def list_events(
        self, job_id: str, *, after: int = 0, limit: int = 250
    ) -> list[dict[str, Any]]:
        """Read an ordered event page, allowing clients to resume after disconnects."""
        bounded_limit = max(1, min(1000, int(limit)))
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT sequence, event_json, created_at
                FROM job_events
                WHERE job_id = ? AND sequence > ?
                ORDER BY sequence
                LIMIT ?
                """,
                (job_id, max(0, int(after)), bounded_limit),
            ).fetchall()
        return [
            {
                "sequence": int(sequence),
                "job_id": job_id,
                "created_at": created_at,
                "event": json.loads(event_json),
            }
            for sequence, event_json, created_at in rows
        ]

    def complete_job(self, job_id: str, result: dict) -> Job | None:
        """Store a completed worker result."""
        completed = self.update_status(
            job_id,
            JobStatus.COMPLETED,
            result=result,
            progress=100,
        )
        cache_key = (
            completed.params.get("partition_cache_key")
            if completed and completed.status == JobStatus.COMPLETED
            else None
        )
        if cache_key:
            self.put_partition_cache(str(cache_key), result)
        return completed

    def get_partition_cache(self, cache_key: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT result_json FROM partition_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def put_partition_cache(self, cache_key: str, result: dict[str, Any]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO partition_cache (cache_key, result_json, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    result_json = excluded.result_json,
                    created_at = excluded.created_at
                """,
                (cache_key, json.dumps(result), datetime.utcnow().isoformat()),
            )

    def record_worker(
        self,
        worker_id: str,
        provider: str,
        status: str = "active",
        runtime_profile: str | None = None,
        capabilities: list[str] | None = None,
        idle: bool = False,
        detail: str | None = None,
    ) -> None:
        """Record a worker heartbeat, or its own report of what went wrong.

        ``detail`` is where a runner that never got as far as a job leaves its
        reason. A container that dies takes its logs with it, so the row is the
        only place the answer can survive.
        """
        now = datetime.utcnow().isoformat()
        idle_since = now if idle else None
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO workers (
                    worker_id, provider, status, last_seen,
                    idle_since, runtime_profile, capabilities, detail
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    provider = excluded.provider,
                    status = excluded.status,
                    last_seen = excluded.last_seen,
                    idle_since = CASE
                        WHEN excluded.idle_since IS NULL THEN NULL
                        ELSE COALESCE(workers.idle_since, excluded.idle_since)
                    END,
                    runtime_profile = excluded.runtime_profile,
                    capabilities = excluded.capabilities,
                    detail = excluded.detail
                """,
                (
                    worker_id,
                    provider,
                    status,
                    now,
                    idle_since,
                    runtime_profile,
                    json.dumps(capabilities or []),
                    detail,
                ),
            )

    def list_active_workers(self, max_age_seconds: int = 90) -> list[dict]:
        """Return workers that have sent a recent heartbeat and are still alive.

        A worker that has registered as ``starting`` counts: it is a pod already
        being paid for, coming up for this profile, and treating it as absent is
        what makes a dispatcher rent a second one for the same queue.
        """
        return self._list_workers(max_age_seconds, LIVE_WORKER_STATUSES)

    def list_recent_workers(self, max_age_seconds: int = 900) -> list[dict]:
        """Return every worker seen recently, including ones that failed to start."""
        return self._list_workers(max_age_seconds, None)

    def _list_workers(
        self, max_age_seconds: int, statuses: tuple[str, ...] | None
    ) -> list[dict]:
        cutoff = (datetime.utcnow() - timedelta(seconds=max_age_seconds)).isoformat()
        clause = ""
        parameters: list[Any] = [cutoff]
        if statuses:
            clause = f" AND status IN ({','.join('?' * len(statuses))})"
            parameters.extend(statuses)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT worker_id, provider, status, last_seen, idle_since,
                       runtime_profile, capabilities, detail
                FROM workers
                WHERE last_seen >= ?{clause}
                ORDER BY provider, worker_id
                """,
                parameters,
            ).fetchall()
        now = datetime.utcnow()
        return [
            {
                "worker_id": row[0],
                "provider": row[1],
                "status": row[2],
                "last_seen": row[3],
                "idle_since": row[4],
                "idle_seconds": max(
                    0,
                    int((now - datetime.fromisoformat(row[4])).total_seconds()),
                ) if row[4] else 0,
                "runtime_profile": row[5],
                "capabilities": json.loads(row[6]) if row[6] else [],
                "detail": row[7],
            }
            for row in rows
        ]

    def fail_job(self, job_id: str, error: str) -> Job | None:
        """Record a worker failure, requeueing until retry attempts are exhausted."""
        job = self.get(job_id)
        if not job:
            return None
        if job.status in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.DEAD_LETTER,
        }:
            return job

        job.error = error
        job.worker_id = None
        now = datetime.utcnow().isoformat()
        if job.attempts >= job.max_attempts:
            job.status = JobStatus.DEAD_LETTER
            job.completed_at = now
        else:
            job.status = JobStatus.QUEUED
        job.updated_at = now
        self.update(job)
        return job

    def delete(self, job_id: str) -> bool:
        """Delete a job."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            return cursor.rowcount > 0

    def cleanup_old(self, days: int = 7) -> int:
        """Delete terminal jobs older than N days."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM jobs
                WHERE status IN (?, ?, ?)
                AND completed_at < ?
                """,
                (
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.DEAD_LETTER.value,
                    cutoff,
                ),
            )
            return cursor.rowcount
