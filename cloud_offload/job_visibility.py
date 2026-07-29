"""Safe, reloadable job projections for the Cloud Jobs user interface."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Iterable

from cloud_offload.queue import JOB_PHASE_ORDER, Job, JobStatus


VISIBILITY_SCHEMA = "cloud-offload.job-visibility.v1"
TERMINAL_STATUSES = {
    JobStatus.COMPLETED.value,
    JobStatus.FAILED.value,
    JobStatus.DEAD_LETTER.value,
}

STAGE_ORDER = {
    "readiness": 0,
    "provisioning": 10,
    "worker_boot": 20,
    "dependency_preparation": 30,
    "execution": 40,
    "result_transfer": 50,
    "resource_closure": 60,
}
STAGE_BANDS = {
    "readiness": (0.0, 1.0),
    "provisioning": (1.0, 2.0),
    "worker_boot": (2.0, 5.0),
    "dependency_preparation": (5.0, 10.0),
    "execution": (10.0, 95.0),
    "result_transfer": (95.0, 99.0),
    "resource_closure": (99.0, 100.0),
}
STAGE_LABELS = {
    "readiness": "Checking the job",
    "provisioning": "Renting a GPU",
    "worker_boot": "Starting the cloud worker",
    "dependency_preparation": "Preparing models and nodes",
    "execution": "Running the workflow",
    "result_transfer": "Returning the result",
    "resource_closure": "Closing the cloud resource",
    "failure": "Job failed",
    "cancelled": "Job cancelled",
}

EVENT_STAGES = {
    "preflight_started": "readiness",
    "preflight_ready": "readiness",
    "preflight_changed": "readiness",
    "provider_request_started": "provisioning",
    "provider_request_progress": "provisioning",
    "provider_request_completed": "provisioning",
    "provider_request_failed": "provisioning",
    "lease_created": "provisioning",
    "lease_bound": "provisioning",
    "lease_job_attached": "provisioning",
    "lease_closed_without_resource": "provisioning",
    "provisioning_failed": "provisioning",
    "runner_starting": "worker_boot",
    "runner_starting_progress": "worker_boot",
    "runner_ready": "worker_boot",
    "lease_job_claimed": "worker_boot",
    "cache_mount_ready": "dependency_preparation",
    "cache_restore_started": "dependency_preparation",
    "cache_restore_completed": "dependency_preparation",
    "cache_artifact_hit": "dependency_preparation",
    "cache_artifact_miss": "dependency_preparation",
    "cache_artifact_refused": "dependency_preparation",
    "cache_population_started": "dependency_preparation",
    "cache_population_progress": "dependency_preparation",
    "cache_population_commit": "dependency_preparation",
    "cache_population_completed": "dependency_preparation",
    "weights_staging": "dependency_preparation",
    "weight_download_progress": "dependency_preparation",
    "node_pack_staging": "dependency_preparation",
    "partition_staging": "dependency_preparation",
    "execution_submitted": "execution",
    "execution_start": "execution",
    "executing": "execution",
    "executed": "execution",
    "execution_cached": "execution",
    "progress": "execution",
    "progress_state": "execution",
    "partition_uploading": "result_transfer",
    "result_available": "result_transfer",
    "termination_requested": "resource_closure",
    "provider_termination_requested": "resource_closure",
    "termination_confirmed": "resource_closure",
    "provider_termination_completed": "resource_closure",
    "resource_terminated": "resource_closure",
    "worker_termination_confirmed": "resource_closure",
    "lease_revoked": "resource_closure",
    "circuit_breaker_triggered": "resource_closure",
    "provider_resource_lost": "resource_closure",
}

TERMINATION_RECEIPTS = {
    "termination_confirmed",
    "provider_termination_completed",
    "resource_terminated",
    "worker_termination_confirmed",
}

EVENT_LABELS = {
    "job_created": "Job created",
    "job_state_seeded": "Job state restored",
    "job_status_changed": "Job state changed",
    "preflight_started": "Preflight started",
    "preflight_ready": "GPU recommendation ready",
    "provider_request_started": "GPU request started",
    "provider_request_progress": "GPU request is in progress",
    "provider_request_completed": "GPU allocated",
    "provider_request_failed": "GPU request failed",
    "provisioning_failed": "GPU start failed; retry is pending",
    "lease_created": "GPU resource lease created",
    "lease_bound": "GPU resource lease is active",
    "lease_job_attached": "Job attached to GPU resource lease",
    "lease_job_claimed": "Cloud worker claimed the leased job",
    "lease_closed_without_resource": "GPU request closed without a resource",
    "runner_starting": "Cloud worker is starting",
    "runner_starting_progress": "Cloud worker is still starting",
    "runner_ready": "Cloud worker is ready",
    "cache_mount_ready": "Prepared storage mounted",
    "cache_restore_started": "Prepared cache check started",
    "cache_artifact_hit": "Prepared cache hit",
    "cache_artifact_miss": "Prepared cache miss",
    "cache_artifact_refused": "Prepared cache item was not usable",
    "cache_restore_completed": "Prepared cache is ready",
    "cache_population_started": "Cache save started",
    "cache_population_progress": "Cache save is in progress",
    "cache_population_commit": "Cache save is becoming durable",
    "cache_population_completed": "Cache save completed",
    "weights_staging": "Model preparation is in progress",
    "weight_download_progress": "Model download is in progress",
    "node_pack_staging": "Node preparation is in progress",
    "partition_staging": "Input transfer is in progress",
    "execution_submitted": "Workflow submitted",
    "execution_start": "Workflow started",
    "executing": "A workflow node started",
    "executed": "A workflow node completed",
    "execution_cached": "Cached workflow nodes completed",
    "progress": "Workflow execution is in progress",
    "progress_state": "Workflow state updated",
    "partition_uploading": "Result transfer started",
    "result_available": "Result is ready",
    "cancellation_requested": "Cancellation requested",
    "execution_cancelled": "Cloud execution cancelled",
    "termination_requested": "Cloud resource closure requested",
    "provider_termination_requested": "Cloud resource closure requested",
    "termination_confirmed": "Cloud resource closed",
    "provider_termination_completed": "Cloud resource closed",
    "resource_terminated": "Cloud resource closed",
    "worker_termination_confirmed": "Cloud resource closed",
    "lease_revoked": "GPU resource lease revoked",
    "circuit_breaker_triggered": "Cloud safety limit reached",
    "provider_resource_lost": "Cloud GPU resource ended before completion",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round(max(0.0, (end - start).total_seconds()), 1)


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _finite_range(value: Any) -> list[float] | None:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        return None
    low = _finite_number(value[0])
    high = _finite_number(value[1])
    if low is None or high is None or low < 0 or high < low:
        return None
    return [round(low, 3), round(high, 3)]


def _raw_event(envelope: dict[str, Any]) -> dict[str, Any]:
    event = envelope.get("event")
    return event if isinstance(event, dict) else envelope


def _event_type(envelope: dict[str, Any]) -> str:
    return str(envelope.get("type") or _raw_event(envelope).get("type") or "unknown")


def _event_time(envelope: dict[str, Any]) -> datetime | None:
    return _parse_time(
        envelope.get("occurred_at")
        or envelope.get("observed_at")
        or envelope.get("created_at")
    )


def _event_stage(envelope: dict[str, Any]) -> str | None:
    event_type = _event_type(envelope)
    if event_type == "phase_timing":
        phase = str(_raw_event(envelope).get("phase") or "")
        return {
            "staging_started": "dependency_preparation",
            "comfyui_ready": "execution",
            "execution_started": "execution",
            "first_sampler": "execution",
            "result_available": "result_transfer",
        }.get(phase)
    mapped = EVENT_STAGES.get(event_type)
    if mapped:
        return mapped
    phase = str(envelope.get("phase") or _raw_event(envelope).get("phase") or "")
    if phase in STAGE_ORDER:
        return phase
    if phase in JOB_PHASE_ORDER:
        return {
            "preflight": "readiness",
            "provider_request": "provisioning",
            "weights_staging": "dependency_preparation",
            "node_pack_staging": "dependency_preparation",
            "cache_restore": "dependency_preparation",
        }.get(phase, phase)
    return None


def _lifecycle_status(job: Job, events: list[dict[str, Any]]) -> str:
    status = job.status.value
    for envelope in events:
        if _event_type(envelope) not in {
            "job_created",
            "job_state_seeded",
            "job_status_changed",
        }:
            continue
        candidate = _raw_event(envelope).get("status") or envelope.get("status")
        if candidate in {item.value for item in JobStatus}:
            status = str(candidate)
    return status


def _selected_stage(
    status: str, events: list[dict[str, Any]], cancellation_requested: bool
) -> str:
    if status == JobStatus.COMPLETED.value:
        return "resource_closure"
    if status in {JobStatus.FAILED.value, JobStatus.DEAD_LETTER.value}:
        return "cancelled" if cancellation_requested else "failure"
    candidates = [stage for item in events if (stage := _event_stage(item))]
    if candidates:
        return max(candidates, key=lambda item: STAGE_ORDER.get(item, -1))
    return {
        JobStatus.DISPATCHED.value: "worker_boot",
        JobStatus.RUNNING.value: "execution",
    }.get(status, "readiness")


def _event_progress(envelope: dict[str, Any]) -> float | None:
    event = _raw_event(envelope)
    metrics = envelope.get("metrics") if isinstance(envelope.get("metrics"), dict) else {}
    for value in (
        event.get("overall_progress"),
        metrics.get("overall_progress"),
        event.get("progress") if _event_type(envelope).startswith("job_") else None,
        metrics.get("progress") if _event_type(envelope).startswith("job_") else None,
    ):
        number = _finite_number(value)
        if number is not None:
            return max(0.0, min(100.0, number))
    return None


def _estimate(job: Job) -> tuple[dict[str, Any], dict[str, Any]]:
    preflight = job.params.get("preflight") if isinstance(job.params, dict) else None
    preflight = preflight if isinstance(preflight, dict) else {}
    estimate = preflight.get("estimate")
    return preflight, estimate if isinstance(estimate, dict) else {}


def _result_time_range(estimate: dict[str, Any]) -> list[float] | None:
    parts = [
        _finite_range(estimate.get("startup_seconds")),
        _finite_range(estimate.get("preparation_seconds")),
        _finite_range(estimate.get("execution_seconds")),
    ]
    if all(parts):
        return [
            round(sum(part[0] for part in parts if part), 3),
            round(sum(part[1] for part in parts if part), 3),
        ]
    paid = _finite_range(estimate.get("paid_lifetime_seconds"))
    if not paid:
        return None
    idle = max(0.0, _finite_number(estimate.get("paid_idle_seconds")) or 0.0)
    termination = _finite_range(estimate.get("termination_seconds")) or [0.0, 0.0]
    return [
        round(max(0.0, paid[0] - idle - termination[1]), 3),
        round(max(0.0, paid[1] - idle - termination[0]), 3),
    ]


def _stage_duration(stage: str, estimate: dict[str, Any]) -> float:
    estimate_key = {
        "worker_boot": "startup_seconds",
        "dependency_preparation": "preparation_seconds",
        "execution": "execution_seconds",
        "resource_closure": "termination_seconds",
    }.get(stage)
    estimated = _finite_range(estimate.get(estimate_key)) if estimate_key else None
    if estimated:
        return max(1.0, estimated[1])
    return {
        "readiness": 20.0,
        "provisioning": 90.0,
        "worker_boot": 180.0,
        "dependency_preparation": 300.0,
        "execution": 300.0,
        "result_transfer": 60.0,
        "resource_closure": 30.0,
    }.get(stage, 60.0)


def _project_progress(
    *,
    status: str,
    stage: str,
    job: Job,
    events: list[dict[str, Any]],
    estimate: dict[str, Any],
    now: datetime,
) -> tuple[float, str]:
    observed = [number for item in events if (number := _event_progress(item)) is not None]
    observed_max = max([float(job.progress or 0), *observed], default=0.0)
    if status == JobStatus.COMPLETED.value:
        return 100.0, "terminal"
    if status in {JobStatus.FAILED.value, JobStatus.DEAD_LETTER.value}:
        return round(max(0.0, min(99.0, observed_max)), 1), "observed"
    band = STAGE_BANDS.get(stage)
    if not band:
        return round(max(0.0, min(99.0, observed_max)), 1), "observed"
    stage_times = [
        stamp
        for item in events
        if _event_stage(item) == stage and (stamp := _event_time(item)) is not None
    ]
    stage_started = min(stage_times) if stage_times else _parse_time(job.created_at)
    stage_elapsed = _seconds(stage_started, now) or 0.0
    fraction = min(0.95, stage_elapsed / _stage_duration(stage, estimate))
    projected = band[0] + (band[1] - band[0]) * fraction
    if observed_max >= projected:
        return round(max(0.0, min(99.0, observed_max)), 1), "observed"
    return round(max(0.0, min(99.0, projected)), 1), "stage_time_estimate"


def _transfer_key(event_type: str, event: dict[str, Any]) -> tuple[str, str]:
    if event_type.startswith("cache_population_"):
        kind = "cache_population"
    elif event_type == "cache_artifact_hit":
        kind = "cache_restore"
    elif event_type.startswith("weight_download"):
        kind = "weight_download"
    else:
        kind = event_type
    opaque = str(
        event.get("digest")
        or event.get("artifact_id")
        or event.get("file")
        or event.get("repo_id")
        or "default"
    )
    return kind, opaque


def _transfer_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    operations: dict[tuple[str, str], dict[str, Any]] = {}
    samples: list[tuple[datetime, float]] = []
    previous: dict[tuple[str, str], tuple[datetime, int]] = {}
    transfer_types = {
        "weight_download_progress",
        "cache_population_started",
        "cache_population_progress",
        "cache_population_completed",
        "cache_artifact_hit",
    }
    for envelope in events:
        event_type = _event_type(envelope)
        if event_type not in transfer_types:
            continue
        event = _raw_event(envelope)
        key = _transfer_key(event_type, event)
        state = operations.setdefault(key, {"completed": 0, "total": 0})
        completed = _finite_number(event.get("bytes_completed"))
        total = _finite_number(event.get("bytes_total") or event.get("total_bytes"))
        byte_value = _finite_number(event.get("bytes"))
        if event_type in {"cache_population_completed", "cache_artifact_hit"}:
            completed = max(completed or 0.0, byte_value or 0.0)
            total = max(total or 0.0, byte_value or 0.0)
        if completed is not None:
            state["completed"] = max(int(completed), int(state["completed"]))
        if total is not None:
            state["total"] = max(int(total), int(state["total"]))
        stamp = _event_time(envelope)
        if stamp is not None and completed is not None:
            prior = previous.get(key)
            if prior and stamp > prior[0] and int(completed) > prior[1]:
                rate = (int(completed) - prior[1]) / (stamp - prior[0]).total_seconds()
                if math.isfinite(rate) and rate > 0:
                    samples.append((stamp, rate))
            else:
                elapsed = _finite_number(event.get("elapsed_seconds"))
                if elapsed and elapsed > 0 and completed > 0:
                    samples.append((stamp, completed / elapsed))
            if prior is None or int(completed) >= prior[1]:
                previous[key] = (stamp, int(completed))
    completed_sum = sum(int(item["completed"]) for item in operations.values())
    total_sum = sum(max(int(item["total"]), int(item["completed"])) for item in operations.values())
    throughput = None
    for _, rate in sorted(samples, key=lambda item: item[0]):
        throughput = rate if throughput is None else (0.35 * rate + 0.65 * throughput)
    eta = None
    confidence = "unavailable"
    if throughput and total_sum > completed_sum:
        seconds = (total_sum - completed_sum) / throughput
        eta = [round(max(0.0, seconds * 0.8), 1), round(max(0.0, seconds * 1.25), 1)]
        confidence = "medium" if len(samples) >= 2 else "low"
    return {
        "bytes_completed": completed_sum,
        "bytes_total": total_sum or None,
        "throughput_bps": round(throughput, 1) if throughput else None,
        "eta_seconds": eta,
        "eta_confidence": confidence,
        "sample_count": len(samples),
    }


def _resources(job: Job, events: list[dict[str, Any]], preflight: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {
        "provider": job.provider or preflight.get("provider"),
        "gpu_type": preflight.get("gpu_type"),
        "region": preflight.get("region"),
        "pod_id": None,
        "volume_id": preflight.get("prepared_volume_id"),
        "hourly_rate_usd": _finite_number(preflight.get("hourly_rate")),
        "lease_id": None,
    }
    for envelope in events:
        event = _raw_event(envelope)
        resources = envelope.get("resources")
        resources = resources if isinstance(resources, dict) else {}
        values["provider"] = resources.get("provider") or event.get("provider") or values["provider"]
        values["gpu_type"] = resources.get("gpu_type") or event.get("gpu_type") or values["gpu_type"]
        values["region"] = (
            resources.get("region")
            or resources.get("datacenter_id")
            or event.get("region")
            or event.get("datacenter_id")
            or values["region"]
        )
        values["pod_id"] = (
            resources.get("pod_id")
            or resources.get("worker_instance_id")
            or event.get("pod_id")
            or event.get("worker_instance_id")
            or values["pod_id"]
        )
        values["lease_id"] = (
            resources.get("lease_id")
            or event.get("lease_id")
            or values["lease_id"]
        )
        values["volume_id"] = (
            resources.get("cache_provider_volume_id")
            or resources.get("cache_volume_id")
            or event.get("volume_id")
            or event.get("cache_provider_volume_id")
            or event.get("cache_volume_id")
            or values["volume_id"]
        )
        rate = _finite_number(resources.get("hourly_rate") or event.get("hourly_rate"))
        if rate is not None:
            values["hourly_rate_usd"] = rate
    return values


def _cache_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    hits: set[str] = set()
    misses: set[str] = set()
    hit_bytes: dict[str, int] = {}
    populated: set[str] = set()
    for envelope in events:
        event_type = _event_type(envelope)
        event = _raw_event(envelope)
        key = str(event.get("digest") or event.get("file") or event.get("artifact_id") or event_type)
        if event_type in {"cache_artifact_hit", "partition_cache_hit"}:
            hits.add(key)
            hit_bytes[key] = max(hit_bytes.get(key, 0), int(_finite_number(event.get("bytes")) or 0))
        elif event_type in {"cache_artifact_miss", "cache_artifact_refused"}:
            misses.add(key)
        elif event_type == "cache_population_completed":
            populated.add(key)
    return {
        "hits": len(hits),
        "misses": len(misses),
        "hit_bytes": sum(hit_bytes.values()),
        "items_saved": len(populated),
        "prepared": bool(hits or populated),
    }


def _safe_event_summaries(events: list[dict[str, Any]], limit: int = 16) -> list[dict[str, Any]]:
    summaries = []
    for envelope in events:
        event_type = _event_type(envelope)
        message = EVENT_LABELS.get(event_type)
        if not message:
            continue
        event = _raw_event(envelope)
        occurred = _event_time(envelope)
        item: dict[str, Any] = {
            "sequence": int(envelope.get("sequence") or 0),
            "occurred_at": occurred.isoformat() if occurred else None,
            "type": event_type,
            "stage": _event_stage(envelope),
            "message": message,
        }
        progress = _event_progress(envelope)
        if progress is not None:
            item["progress"] = round(progress, 1)
        completed = _finite_number(event.get("bytes_completed"))
        total = _finite_number(event.get("bytes_total") or event.get("total_bytes"))
        if completed is not None:
            item["bytes_completed"] = int(completed)
        if total is not None:
            item["bytes_total"] = int(total)
        elapsed = _finite_number(event.get("elapsed_seconds"))
        if elapsed is not None:
            item["elapsed_seconds"] = round(max(0.0, elapsed), 1)
        summaries.append(item)
    return summaries[-max(1, int(limit)) :]


def project_job_visibility(
    job: Job,
    events: Iterable[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build one allow-listed view. It never returns a workflow or a raw event."""
    now = (now or _utc_now()).astimezone(timezone.utc)
    event_list = list(events)
    status = _lifecycle_status(job, event_list)
    cancellation_requested = any(
        _event_type(item) in {"cancellation_requested", "execution_cancelled"}
        for item in event_list
    ) or (status in TERMINAL_STATUSES and str(job.error or "").lower().startswith("cancel"))
    terminal = status in TERMINAL_STATUSES
    stage = _selected_stage(status, event_list, cancellation_requested)
    preflight, estimate = _estimate(job)
    progress, progress_basis = _project_progress(
        status=status,
        stage=stage,
        job=job,
        events=event_list,
        estimate=estimate,
        now=now,
    )
    created_at = _parse_time(job.created_at)
    latest_event_at = max(
        [stamp for item in event_list if (stamp := _event_time(item)) is not None],
        default=None,
    )
    completed_at = _parse_time(job.completed_at)
    elapsed_end = completed_at if terminal and completed_at else now
    transfer = _transfer_metrics(event_list)
    resources = _resources(job, event_list, preflight)
    provider_started = next(
        (
            _event_time(item)
            for item in event_list
            if _event_type(item) == "provider_request_completed" and _event_time(item)
        ),
        None,
    )
    paid_start_basis = "provider_allocation"
    if provider_started is None:
        provider_started = next(
            (
                _event_time(item)
                for item in event_list
                if _event_type(item) in {"runner_starting", "runner_starting_progress"}
                and (
                    _raw_event(item).get("worker_instance_id")
                    or (item.get("resources") or {}).get("worker_instance_id")
                )
                and _event_time(item)
            ),
            None,
        )
        paid_start_basis = "first_pod_observation"
    termination_receipt = next(
        (
            item
            for item in reversed(event_list)
            if _event_type(item) in TERMINATION_RECEIPTS
        ),
        None,
    )
    termination_confirmed = termination_receipt is not None
    if not resources["pod_id"]:
        billing_state = "not_started"
    elif termination_confirmed:
        billing_state = "stopped"
    elif terminal:
        billing_state = "termination_unconfirmed"
    else:
        billing_state = "accruing"
    paid_end = _event_time(termination_receipt) if termination_receipt else now
    paid_elapsed = _seconds(provider_started, paid_end) if provider_started else None
    spend_seconds = paid_elapsed
    spend_basis = f"{paid_start_basis}_elapsed"
    hourly_rate = resources["hourly_rate_usd"]
    estimated_spend = (
        round(hourly_rate * spend_seconds / 3600, 6)
        if hourly_rate is not None and spend_seconds is not None
        else None
    )
    result_range = _result_time_range(estimate)
    if terminal:
        eta = [0.0, 0.0]
        eta_confidence = "observed"
        eta_basis = "terminal"
    elif transfer["eta_seconds"] and stage == "result_transfer":
        eta = transfer["eta_seconds"]
        eta_confidence = transfer["eta_confidence"]
        eta_basis = "measured_transfer"
    elif result_range:
        estimate_elapsed = paid_elapsed or 0.0
        eta = [
            round(max(0.0, result_range[0] - estimate_elapsed), 1),
            round(max(0.0, result_range[1] - estimate_elapsed), 1),
        ]
        eta_confidence = str(estimate.get("confidence") or "low")
        eta_basis = "matched_history" if estimate.get("history_used") else "preflight_defaults"
    else:
        eta = None
        eta_confidence = "unavailable"
        eta_basis = "unavailable"
    partition = job.request.get("partition") if isinstance(job.request, dict) else None
    partition_id = partition.get("partition_id") if isinstance(partition, dict) else None
    recent_events = _safe_event_summaries(event_list)
    active_summary = next(
        (
            item
            for item in reversed(recent_events)
            if not item["type"].startswith("job_")
        ),
        None,
    )
    active_operation = (
        active_summary["message"]
        if active_summary
        else STAGE_LABELS.get(stage, "Working")
    )
    total_cost = _finite_range(estimate.get("total_job_cost_usd"))
    if terminal and billing_state == "termination_unconfirmed":
        active_operation = (
            "Result is ready; GPU closure is not confirmed"
            if status == JobStatus.COMPLETED.value
            else "Job ended; GPU closure is not confirmed"
        )
    return {
        "schema": VISIBILITY_SCHEMA,
        "job_id": job.id,
        "partition_id": str(partition_id) if partition_id is not None else None,
        "status": status,
        "terminal": terminal,
        "lifecycle_stage": stage,
        "stage_label": STAGE_LABELS.get(stage, "Working"),
        "active_operation": active_operation,
        "progress": progress,
        "progress_basis": progress_basis,
        "created_at": job.created_at,
        "updated_at": (
            latest_event_at.isoformat() if latest_event_at else job.updated_at
        ),
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "elapsed_seconds": _seconds(created_at, elapsed_end),
        "paid_elapsed_seconds": paid_elapsed,
        "eta_seconds": eta,
        "eta_confidence": eta_confidence,
        "eta_basis": eta_basis,
        "transfer": transfer,
        "resource": resources,
        "cost": {
            "hourly_rate_usd": hourly_rate,
            "estimated_spend_usd": estimated_spend,
            "estimated_total_usd": total_cost,
            "estimate_complete": bool(estimate.get("cost_complete") and total_cost),
            "spend_basis": spend_basis if estimated_spend is not None else "unavailable",
        },
        "cache": _cache_summary(event_list),
        "preflight": {
            "state": "confirmed" if preflight else "not_available",
            "confirmation_action": (
                (preflight.get("confirmation") or {}).get("action")
                if isinstance(preflight.get("confirmation"), dict)
                else None
            ),
            "confidence": str(estimate.get("confidence") or "unavailable"),
            "history_sample_count": int(estimate.get("history_sample_count") or 0),
        },
        "recommendation": {
            "provider": resources["provider"],
            "gpu_type": resources["gpu_type"],
            "region": resources["region"],
            "preparation_class": preflight.get("preparation_class"),
            "hourly_rate_usd": hourly_rate,
            "estimated_total_usd": total_cost,
            "estimated_result_seconds": result_range,
        },
        "cancellation": {
            "requested": cancellation_requested,
            "can_cancel": not terminal,
        },
        "billing": {
            "state": billing_state,
            "termination_confirmed": termination_confirmed,
            "termination_confirmed_at": (
                _event_time(termination_receipt).isoformat()
                if termination_receipt and _event_time(termination_receipt)
                else None
            ),
        },
        "event_cursor": int(event_list[-1].get("sequence") or 0) if event_list else 0,
        "event_count": len(event_list),
        "recent_events": recent_events,
    }


def visibility_page(
    queue: Any,
    *,
    limit: int = 50,
    active_only: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Project a bounded recent page. Active jobs sort before recent terminal jobs."""
    generated = (now or _utc_now()).astimezone(timezone.utc)
    jobs = queue.list_recent(limit=limit, active_only=active_only)
    projected = []
    for job in jobs:
        event_reader = getattr(queue, "list_recent_events", queue.list_events)
        view = project_job_visibility(job, event_reader(job.id, limit=1000), now=generated)
        bounds_reader = getattr(queue, "event_bounds", None)
        if callable(bounds_reader):
            view["event_count"], view["event_cursor"] = bounds_reader(job.id)
        projected.append(view)
    return {
        "schema": VISIBILITY_SCHEMA,
        "generated_at": generated.isoformat(),
        "jobs": projected,
    }
