"""Safe, read-only timing history for GPU recommendations."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_VOLATILE_INPUT_NAMES = frozenset(
    {
        "artifact_id",
        "control_after_generate",
        "filename_prefix",
        "job_id",
        "noise_seed",
        "negative_prompt",
        "partition_id",
        "prompt",
        "random_seed",
        "seed",
        "text",
        "unique_id",
        "workflow_id",
    }
)
_PERFORMANCE_STRING_INPUTS = frozenset(
    {
        "device",
        "dtype",
        "precision",
        "sampler",
        "sampler_name",
        "scheduler",
        "upscale_method",
    }
)
_MAX_HISTORY_JOBS = 1000
_MAX_PHASE_SECONDS = 24 * 60 * 60


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _safe_scalar(name: str, value: Any) -> Any:
    """Keep performance facts and remove private or run-specific values."""

    normalized_name = str(name).strip().lower()
    if normalized_name in _VOLATILE_INPUT_NAMES or normalized_name.endswith("_id"):
        return None
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    if isinstance(value, str):
        if normalized_name in _PERFORMANCE_STRING_INPUTS:
            return value.strip().lower()[:100]
        # The value can contain a prompt, file name, path, or provider detail.
        # Its type and broad size can still describe workload shape safely.
        length = len(value)
        return {"type": "string", "length_bucket": min(4096, (length // 64) * 64)}
    if isinstance(value, list):
        return [
            item
            for index, child in enumerate(value)
            if (item := _safe_scalar(f"{normalized_name}_{index}", child)) is not None
        ]
    if isinstance(value, dict):
        return {
            str(key): item
            for key, child in sorted(value.items(), key=lambda pair: str(pair[0]))
            if (item := _safe_scalar(str(key), child)) is not None
        }
    return {"type": type(value).__name__}


def _workflow_shape(workflow: dict[str, Any]) -> list[str]:
    nodes = {
        str(key): value for key, value in workflow.items() if isinstance(value, dict)
    }
    memo: dict[str, str] = {}

    def node_hash(node_id: str, stack: frozenset[str] = frozenset()) -> str:
        if node_id in memo:
            return memo[node_id]
        node = nodes.get(node_id) or {}
        class_type = str(node.get("class_type") or "unknown")[:200]
        if node_id in stack:
            return _digest({"class_type": class_type, "cycle": True})
        inputs = node.get("inputs") if isinstance(node.get("inputs"), dict) else {}
        scalar_inputs: dict[str, Any] = {}
        links: list[dict[str, Any]] = []
        next_stack = stack | {node_id}
        for name, value in sorted(inputs.items(), key=lambda pair: str(pair[0])):
            input_name = str(name)
            if (
                isinstance(value, list)
                and len(value) == 2
                and str(value[0]) in nodes
                and isinstance(value[1], int)
            ):
                links.append(
                    {
                        "input": input_name,
                        "source": node_hash(str(value[0]), next_stack),
                        "output": int(value[1]),
                    }
                )
                continue
            safe = _safe_scalar(input_name, value)
            if safe is not None:
                scalar_inputs[input_name] = safe
        value = _digest(
            {
                "class_type": class_type,
                "inputs": scalar_inputs,
                "links": links,
            }
        )
        memo[node_id] = value
        return value

    return sorted(node_hash(node_id) for node_id in nodes)


def workload_digest(
    partition: dict[str, Any],
    *,
    profile_name: str | None = None,
    minimum_vram_gb: int | float | None = None,
) -> str:
    """Return a private-data-free identity for comparable workflow work."""

    runner = (
        partition.get("runner") if isinstance(partition.get("runner"), dict) else {}
    )
    workflow = partition.get("workflow")
    workflow = workflow if isinstance(workflow, dict) else {}
    assets = (
        partition.get("assets") if isinstance(partition.get("assets"), list) else []
    )
    node_packs = (
        partition.get("node_packs")
        if isinstance(partition.get("node_packs"), list)
        else []
    )
    asset_digests = sorted(
        str(item.get("sha256") or "").lower()
        for item in assets
        if isinstance(item, dict) and item.get("sha256")
    )
    pack_digests = sorted(
        str(item.get("digest") or "").lower()
        for item in node_packs
        if isinstance(item, dict) and item.get("digest")
    )
    try:
        minimum_vram = int(
            minimum_vram_gb
            if minimum_vram_gb is not None
            else (runner or {}).get("min_gpu_ram_gb") or 16
        )
    except (TypeError, ValueError):
        minimum_vram = 16
    return _digest(
        {
            "schema": "cloud-offload.workload-shape.v1",
            "profile": str(profile_name or (runner or {}).get("profile") or "comfyui"),
            "minimum_vram_gb": max(1, min(256, minimum_vram)),
            "residency": str(partition.get("residency") or "cloud"),
            "workflow_shape": _workflow_shape(workflow),
            "asset_digests": asset_digests,
            "node_pack_digests": pack_digests,
        }
    )


def candidate_class(
    *, provider: str, gpu_type: str, region: str | None, prepared: bool
) -> dict[str, Any]:
    """Return safe candidate facts that can affect measured phase time."""

    return {
        "provider": str(provider).strip().lower(),
        "gpu_type": re.sub(r"[^a-z0-9]+", "", str(gpu_type).strip().lower()),
        "region": str(region or "").strip().lower(),
        "preparation_class": "prepared" if prepared else "cold",
    }


def candidate_class_digest(value: dict[str, Any]) -> str:
    return _digest(value)


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _valid_duration(value: float) -> float | None:
    return value if math.isfinite(value) and 0 <= value <= _MAX_PHASE_SECONDS else None


def _observed_range(values: list[float]) -> list[float]:
    """Add a small safety margin to the complete observed range."""

    return [round(max(0.0, min(values) * 0.9), 3), round(max(values) * 1.1, 3)]


class RecommendationHistory:
    """Read completed-job timing samples without changing the queue database."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._index: dict[tuple[str, str], list[dict[str, float]]] | None = None

    def lookup(self, workload: str, candidate: dict[str, Any]) -> dict[str, Any] | None:
        if self._index is None:
            self._index = self._load()
        samples = (
            self._index.get((str(workload), candidate_class_digest(candidate))) or []
        )
        if not samples:
            return None
        startup = [item["startup"] for item in samples]
        preparation = [item["preparation"] for item in samples]
        execution = [item["execution"] for item in samples]
        count = len(samples)
        confidence = "high" if count >= 5 else "medium" if count >= 2 else "low"
        return {
            "schema": "cloud-offload.recommendation-history.v1",
            "sample_count": count,
            "candidate_class_digest": candidate_class_digest(candidate),
            "startup_seconds": _observed_range(startup),
            "preparation_seconds": _observed_range(preparation),
            "execution_seconds": _observed_range(execution),
            "confidence": confidence,
            "basis": "matched_completed_jobs",
        }

    def _load(self) -> dict[tuple[str, str], list[dict[str, float]]]:
        index: dict[tuple[str, str], list[dict[str, float]]] = {}
        if not self.db_path.is_file():
            return index
        try:
            database = self.db_path.resolve().as_uri() + "?mode=ro"
            with sqlite3.connect(database, uri=True) as connection:
                jobs = connection.execute(
                    """
                    SELECT id, params, request_json, created_at
                    FROM jobs
                    WHERE status = 'completed' AND model = 'comfyui-partition-v1'
                    ORDER BY completed_at DESC
                    LIMIT ?
                    """,
                    (_MAX_HISTORY_JOBS,),
                ).fetchall()
                for job_id, params_json, request_json, created_at in jobs:
                    sample = self._sample(
                        connection,
                        str(job_id),
                        params_json,
                        request_json,
                        created_at,
                    )
                    if sample is None:
                        continue
                    key, timings = sample
                    index.setdefault(key, []).append(timings)
        except (OSError, sqlite3.Error, TypeError, ValueError, json.JSONDecodeError):
            return {}
        return index

    @staticmethod
    def _sample(
        connection: sqlite3.Connection,
        job_id: str,
        params_json: str | None,
        request_json: str | None,
        created_at: str | None,
    ) -> tuple[tuple[str, str], dict[str, float]] | None:
        params = json.loads(params_json or "{}")
        request = json.loads(request_json or "{}")
        preflight = (
            params.get("preflight") if isinstance(params.get("preflight"), dict) else {}
        )
        partition = (
            request.get("partition")
            if isinstance(request.get("partition"), dict)
            else {}
        )
        workload = str(preflight.get("workload_digest") or "") or workload_digest(
            partition,
            profile_name=str(params.get("runtime_profile") or "comfyui"),
            minimum_vram_gb=params.get("min_gpu_ram_gb"),
        )
        candidate = candidate_class(
            provider=str(preflight.get("provider") or params.get("provider") or ""),
            gpu_type=str(preflight.get("gpu_type") or params.get("gpu_type") or ""),
            region=preflight.get("region"),
            prepared=bool(preflight.get("prepared_volume_id")),
        )
        rows = connection.execute(
            """
            SELECT event_json, observed_at
            FROM job_events
            WHERE job_id = ? AND event_type = 'phase_timing'
            ORDER BY sequence
            """,
            (job_id,),
        ).fetchall()
        phases: dict[str, tuple[float, datetime | None]] = {}
        for event_json, observed_at in rows:
            event = json.loads(event_json)
            phase = str(event.get("phase") or "")
            try:
                monotonic_ms = float(event.get("monotonic_ms"))
            except (TypeError, ValueError):
                continue
            if phase and math.isfinite(monotonic_ms) and phase not in phases:
                phases[phase] = (monotonic_ms, _parse_time(observed_at))
        if not all(
            name in phases
            for name in ("staging_started", "execution_started", "result_available")
        ):
            return None
        created = _parse_time(created_at)
        staging_observed = phases["staging_started"][1]
        if created is None or staging_observed is None:
            return None
        startup = _valid_duration((staging_observed - created).total_seconds())
        preparation = _valid_duration(
            (phases["execution_started"][0] - phases["staging_started"][0]) / 1000
        )
        execution = _valid_duration(
            (phases["result_available"][0] - phases["execution_started"][0]) / 1000
        )
        if startup is None or preparation is None or execution is None:
            return None
        return (
            (workload, candidate_class_digest(candidate)),
            {"startup": startup, "preparation": preparation, "execution": execution},
        )
