"""Redacted, replayable diagnostics for one Cloud Offload job."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from cloud_offload.queue import Job, JobQueue


_SECRET_MARKERS = (
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)
_OMITTED_MARKERS = (
    "data_base64",
    "image_base64",
    "input_value",
    "output_value",
    "preview_base64",
    "prompt",
    "result",
    "workflow",
)
_SECRET_PREFIXES = (
    "bearer ",
    "gho_",
    "ghp_",
    "hf_",
    "rpa_",
    "rps_",
    "user_",
)
_INLINE_SECRET = re.compile(
    r"(?i)(?:bearer\s+|gh[op]_+|hf_|rpa_|rps_|user_)[A-Za-z0-9._-]+"
)
_SAFE_ASSET_FIELDS = (
    "category",
    "filename",
    "format",
    "origin",
    "sha256",
    "size",
)


def _redact(value: Any, *, key: str = "") -> Any:
    normalized_key = key.lower()
    if any(marker in normalized_key for marker in _OMITTED_MARKERS):
        return "[omitted]"
    if any(marker in normalized_key for marker in _SECRET_MARKERS):
        return "[redacted]"
    if isinstance(value, dict):
        return {
            str(item_key): _redact(item_value, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact(item, key=key) for item in value]
    if isinstance(value, str):
        lowered = value.strip().lower()
        if any(lowered.startswith(prefix) for prefix in _SECRET_PREFIXES):
            return "[redacted]"
        value = _INLINE_SECRET.sub("[redacted]", value)
        if "://" in value:
            try:
                parsed = urlsplit(value)
                if parsed.username or parsed.password:
                    host = parsed.hostname or ""
                    if parsed.port:
                        host = f"{host}:{parsed.port}"
                    parsed = parsed._replace(netloc=host)
                if parsed.query or parsed.fragment:
                    value = urlunsplit(
                        (parsed.scheme, parsed.netloc, parsed.path, "[redacted]", "")
                    )
                else:
                    value = urlunsplit(parsed)
            except ValueError:
                pass
        if len(value) > 2048:
            return value[:2048] + "…[truncated]"
    return value


def _node_type_counts(
    value: Any, counts: Counter[str], *, remaining: list[int]
) -> None:
    if remaining[0] <= 0:
        return
    if isinstance(value, dict):
        remaining[0] -= 1
        class_type = value.get("class_type")
        if class_type:
            counts[str(class_type)] += 1
        for child in value.values():
            _node_type_counts(child, counts, remaining=remaining)
    elif isinstance(value, list):
        for child in value:
            _node_type_counts(child, counts, remaining=remaining)


def _request_summary(request: dict[str, Any]) -> dict[str, Any]:
    partition = request.get("partition")
    partition = partition if isinstance(partition, dict) else {}
    counts: Counter[str] = Counter()
    _node_type_counts(partition.get("workflow"), counts, remaining=[10_000])
    assets = request.get("assets")
    safe_assets = []
    for asset in assets if isinstance(assets, list) else []:
        if isinstance(asset, dict):
            safe_assets.append(
                {key: asset.get(key) for key in _SAFE_ASSET_FIELDS if key in asset}
            )
    return {
        "kind": request.get("kind"),
        "partition": {
            "schema": partition.get("schema"),
            "partition_id": partition.get("partition_id"),
            "runner": _redact(partition.get("runner") or {}),
            "node_count": sum(counts.values()),
            "node_types": dict(sorted(counts.items())),
            "input_count": len(partition.get("inputs") or []),
            "output_count": len(partition.get("outputs") or []),
        },
        "input_artifact_count": len(request.get("input_artifacts") or {}),
        "timeout_seconds": request.get("timeout_seconds"),
        "assets": safe_assets,
    }


def _redacted_event(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": item.get("schema"),
        "sequence": item.get("sequence"),
        "job_id": item.get("job_id"),
        "occurred_at": item.get("occurred_at"),
        "observed_at": item.get("observed_at"),
        "producer": _redact(item.get("producer") or {}),
        "type": item.get("type"),
        "phase": item.get("phase"),
        "phase_owner": item.get("phase_owner"),
        "partition_id": item.get("partition_id"),
        "status": item.get("status"),
        "metrics": _redact(item.get("metrics") or {}),
        "resources": _redact(item.get("resources") or {}),
        "evidence": _redact(item.get("evidence") or {}),
        "event": _redact(item.get("event") or {}),
    }


def _all_events(
    queue: JobQueue, job_id: str, *, maximum: int = 10_000
) -> tuple[list, bool]:
    events: list[dict[str, Any]] = []
    cursor = 0
    while len(events) < maximum:
        requested = min(1000, maximum - len(events))
        page = queue.list_events(
            job_id,
            after=cursor,
            limit=requested,
        )
        if not page:
            return events, False
        events.extend(page)
        cursor = int(page[-1]["sequence"])
        if len(page) < requested:
            return events, False
    return events, bool(queue.list_events(job_id, after=cursor, limit=1))


def build_support_bundle(queue: JobQueue, job: Job) -> dict[str, Any]:
    """Build a bounded diagnostic artifact without workflow values or secrets."""
    events, truncated = _all_events(queue, job.id)
    snapshot = queue.event_snapshot(job.id)
    safe_snapshot = {
        key: snapshot.get(key)
        for key in (
            "schema",
            "status",
            "state_source",
            "lifecycle_phase",
            "progress",
            "event_cursor",
            "event_count",
            "updated_at",
        )
        if snapshot and key in snapshot
    }
    if snapshot and snapshot.get("last_event"):
        safe_snapshot["last_event"] = _redacted_event(snapshot["last_event"])
    return {
        "schema": "cloud-offload.support-bundle.v1",
        "generated_at": datetime.utcnow().isoformat(),
        "job": {
            "id": job.id,
            "model": job.model,
            "status": job.status.value,
            "provider": job.provider,
            "progress": job.progress,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
            "worker_id": job.worker_id,
            "error": _redact(job.error),
            "params": _redact(job.params),
            "request": _request_summary(job.request),
        },
        "snapshot": safe_snapshot,
        "events": [_redacted_event(item) for item in events],
        "events_truncated": truncated,
    }
