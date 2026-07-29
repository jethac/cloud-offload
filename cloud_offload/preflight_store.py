"""Durable storage for safe preflight reports.

The stored report is the redacted API projection. The raw partition and
workflow stay with the submitter and the partition submission request.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cloud_offload.preflight import PREFLIGHT_SCHEMA


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PreflightStore:
    """SQLite-backed safe report store that shares the coordinator database."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS preflight_reports (
                    preflight_id TEXT PRIMARY KEY,
                    manifest_digest TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    stored_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_preflight_expiry "
                "ON preflight_reports(expires_at)"
            )

    def put(self, report: dict[str, Any]) -> dict[str, Any]:
        if report.get("schema") != PREFLIGHT_SCHEMA:
            raise ValueError("Unsupported preflight report schema")
        preflight_id = str(report.get("preflight_id") or "")
        manifest_digest = str(report.get("manifest_digest") or "")
        status = str(report.get("status") or "")
        created_at = str(report.get("created_at") or "")
        expires_at = str(report.get("expires_at") or "")
        if not all((preflight_id, manifest_digest, status, created_at, expires_at)):
            raise ValueError("Preflight report identity and lifetime are required")
        encoded = json.dumps(
            report,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                """
                INSERT INTO preflight_reports (
                    preflight_id, manifest_digest, status, created_at,
                    expires_at, report_json, stored_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(preflight_id) DO UPDATE SET
                    manifest_digest=excluded.manifest_digest,
                    status=excluded.status,
                    created_at=excluded.created_at,
                    expires_at=excluded.expires_at,
                    report_json=excluded.report_json,
                    stored_at=excluded.stored_at
                """,
                (
                    preflight_id,
                    manifest_digest,
                    status,
                    created_at,
                    expires_at,
                    encoded,
                    _now(),
                ),
            )
        return report

    def get(self, preflight_id: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as connection:
            row = connection.execute(
                "SELECT report_json FROM preflight_reports WHERE preflight_id=?",
                (str(preflight_id),),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def delete_expired(self, before: str | None = None) -> int:
        with sqlite3.connect(self.path) as connection:
            cursor = connection.execute(
                "DELETE FROM preflight_reports WHERE expires_at < ?",
                (before or _now(),),
            )
            return int(cursor.rowcount)
