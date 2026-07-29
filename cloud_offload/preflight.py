"""Read-only readiness proof and GPU recommendation for one partition.

Preflight is the free part of Cloud Offload. It can read local configuration,
prepared-state metadata, provider storage, and current offers. It must not
create a provider resource, change storage, or queue a job.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Callable

from cloud_offload.assets import (
    NAME_MATCHED_WARNING,
    normalized_partition_assets,
    resolve_partition_assets,
    unresolved_assets_message,
)
from cloud_offload.cache_registry import CacheRegistry, CacheVolume
from cloud_offload.cache_scheduler import (
    resolve_prepared_requirements,
    scheduler_runtime,
)
from cloud_offload.config import estimate_runpod_storage_monthly
from cloud_offload.credentials import (
    RUNPOD_S3_ACCESS_CREDENTIAL,
    RUNPOD_S3_SECRET_CREDENTIAL,
    get_credential,
)
from cloud_offload.node_packs import (
    missing_node_packs,
    missing_node_packs_message,
    node_pack_version_warnings,
    normalized_partition_node_packs,
)
from cloud_offload.profiles import (
    worker_profile_gpu_type,
    worker_profile_min_gpu_ram,
)
from cloud_offload.providers import connector_metadata, create_connector
from cloud_offload.providers.base import PlacementConstraints, StorageAttachment
from cloud_offload.recommendation_history import (
    candidate_class,
    workload_digest,
)
from cloud_offload.router import resolve_worker_profile
from cloud_offload.storage import partition_artifact_key
from cloud_offload.storage_plan import (
    GIB,
    exceeds_ceiling_message,
    plan_disk_gb,
    plan_storage,
    plan_summary,
)
from cloud_offload.weight_sizes import cached_weight_sizes


PREFLIGHT_SCHEMA = "cloud-offload.preflight.v1"
PARTITION_JOB_SCHEMA = "comfy.partition.job.v1"
RECOMMENDATION_POLICIES = frozenset({"balanced", "cheapest", "fastest", "manual"})
QUOTE_LIFETIME_SECONDS = 60
DEFAULT_STARTUP_RANGE_SECONDS = (60.0, 180.0)
DEFAULT_EXECUTION_RANGE_SECONDS = (120.0, 300.0)
DEFAULT_TERMINATION_RANGE_SECONDS = (10.0, 30.0)
DOWNLOAD_THROUGHPUT_RANGE_BPS = (25 * 1024**2, 100 * 1024**2)
RESTORE_THROUGHPUT_RANGE_BPS = (100 * 1024**2, 500 * 1024**2)
RUNPOD_CONTAINER_DISK_USD_PER_GB_MONTH = 0.10
PRICING_MONTH_SECONDS = 30 * 24 * 60 * 60
_PINNED_IMAGE = re.compile(r"@sha256:([0-9a-fA-F]{64})$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _issue(
    code: str,
    message: str,
    *,
    action: str | None = None,
    field: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "message": message,
        **({"action": action} if action else {}),
        **({"field": field} if field else {}),
        **({"details": details} if details else {}),
    }


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _provider_name(value: str) -> str:
    normalized = str(value or "auto").strip().lower()
    return "vast.ai" if normalized == "vast" else normalized


def _safe_error(exc: Exception) -> str:
    """Name an external failure without returning its URL, payload, or secret."""

    return type(exc).__name__


def _safe_offer(offer: dict[str, Any], provider: str) -> dict[str, Any]:
    datacenter_ids = offer.get("datacenter_ids") or []
    region = (
        offer.get("datacenter_id")
        or offer.get("location")
        or (datacenter_ids[0] if len(datacenter_ids) == 1 else None)
    )
    gpu_ram_gb = float(offer.get("gpu_ram_gb") or 0)
    hourly_rate = float(offer.get("hourly_rate") or 0)
    if not math.isfinite(gpu_ram_gb) or gpu_ram_gb < 0:
        raise ValueError("invalid GPU memory")
    if not math.isfinite(hourly_rate) or hourly_rate < 0:
        raise ValueError("invalid hourly rate")
    safe = {
        "offer_id": str(offer.get("id") or ""),
        "provider": provider,
        "gpu_type": str(offer.get("gpu_type") or "unknown"),
        "gpu_count": max(1, int(offer.get("gpu_count") or 1)),
        "gpu_ram_gb": gpu_ram_gb,
        "hourly_rate": hourly_rate,
        "region": str(region) if region else None,
    }
    for name in (
        "compute_hourly_rate",
        "storage_hourly_rate",
        "download_cost_per_gb",
        "upload_cost_per_gb",
    ):
        raw = offer.get("raw") if isinstance(offer.get("raw"), dict) else {}
        provider_field = {
            "compute_hourly_rate": "dph_base",
            "storage_hourly_rate": "storage_total_cost",
            "download_cost_per_gb": "inet_down_cost",
            "upload_cost_per_gb": "inet_up_cost",
        }[name]
        value = offer.get(name)
        if value is None and provider == "vast.ai":
            value = raw.get(provider_field)
        if value is None:
            continue
        normalized = float(value)
        if not math.isfinite(normalized) or normalized < 0:
            raise ValueError(f"invalid {name}")
        safe[name] = normalized
    return safe


def _estimate(
    *,
    provider: str,
    offer: dict[str, Any],
    required_bytes: int,
    cached_bytes: int,
    container_disk_gb: int,
    idle_shutdown_seconds: int,
    keep_warm: bool,
    keep_warm_warning_seconds: int,
    timing_history: dict[str, Any] | None = None,
    existing_storage_monthly_usd: float | None = None,
) -> dict[str, Any]:
    cached = max(0, min(int(cached_bytes), int(required_bytes)))
    missing = max(0, int(required_bytes) - cached)
    preparation_low = (
        cached / RESTORE_THROUGHPUT_RANGE_BPS[1]
        + missing / DOWNLOAD_THROUGHPUT_RANGE_BPS[1]
    )
    preparation_high = (
        cached / RESTORE_THROUGHPUT_RANGE_BPS[0]
        + missing / DOWNLOAD_THROUGHPUT_RANGE_BPS[0]
    )
    if timing_history:
        startup_range = list(timing_history["startup_seconds"])
        preparation_range = list(timing_history["preparation_seconds"])
        execution_range = list(timing_history["execution_seconds"])
        confidence = str(timing_history["confidence"])
    else:
        startup_range = list(DEFAULT_STARTUP_RANGE_SECONDS)
        preparation_range = [preparation_low, preparation_high]
        execution_range = list(DEFAULT_EXECUTION_RANGE_SECONDS)
        confidence = "low"
    paid_idle_seconds = (
        max(0, int(keep_warm_warning_seconds))
        if keep_warm
        else max(0, int(idle_shutdown_seconds))
    )
    total_low = (
        float(startup_range[0])
        + float(preparation_range[0])
        + float(execution_range[0])
        + paid_idle_seconds
        + DEFAULT_TERMINATION_RANGE_SECONDS[0]
    )
    total_high = (
        float(startup_range[1])
        + float(preparation_range[1])
        + float(execution_range[1])
        + paid_idle_seconds
        + DEFAULT_TERMINATION_RANGE_SECONDS[1]
    )
    hourly_rate = float(offer["hourly_rate"])
    compute_hourly_rate = float(offer.get("compute_hourly_rate", hourly_rate))
    compute_cost = [
        compute_hourly_rate * total_low / 3600,
        compute_hourly_rate * total_high / 3600,
    ]
    cost_complete = not keep_warm
    assumptions: list[str] = []
    cost_basis: dict[str, str] = {
        "compute": "current provider offer",
    }
    if provider == "runpod":
        transfer_cost: list[float] | None = [0.0, 0.0]
        storage_cost: list[float] | None = [
            container_disk_gb
            * RUNPOD_CONTAINER_DISK_USD_PER_GB_MONTH
            * total_low
            / PRICING_MONTH_SECONDS,
            container_disk_gb
            * RUNPOD_CONTAINER_DISK_USD_PER_GB_MONTH
            * total_high
            / PRICING_MONTH_SECONDS,
        ]
        cost_basis.update(
            {
                "transfer": "RunPod publishes no ingress or egress fee",
                "storage": "RunPod container disk at 0.10 USD per GB-month, prorated for paid lifetime",
            }
        )
    elif provider == "vast.ai" and all(
        name in offer
        for name in (
            "storage_hourly_rate",
            "download_cost_per_gb",
            "upload_cost_per_gb",
        )
    ):
        # Required misses enter the rented machine. Result size is not known
        # before execution, so use zero through one required-data set as the
        # explicit output-transfer range.
        gib = float(1024**3)
        download_rate = float(offer["download_cost_per_gb"])
        upload_rate = float(offer["upload_cost_per_gb"])
        transfer_cost = [
            missing / gib * download_rate,
            missing / gib * download_rate
            + max(required_bytes, 1024**2) / gib * upload_rate,
        ]
        storage_hourly_rate = float(offer["storage_hourly_rate"])
        storage_cost = [
            storage_hourly_rate * total_low / 3600,
            storage_hourly_rate * total_high / 3600,
        ]
        cost_basis.update(
            {
                "transfer": "current Vast.ai per-GB offer rates with a bounded result-size assumption",
                "storage": "current Vast.ai offer storage rate",
            }
        )
        assumptions.append(
            "Vast.ai result transfer is estimated from zero bytes through one required-data set."
        )
    else:
        transfer_cost = None
        storage_cost = None
        cost_complete = False
        cost_basis.update(
            {
                "transfer": "provider rate unavailable",
                "storage": "provider rate unavailable",
            }
        )
    total_cost = None
    if cost_complete and transfer_cost is not None and storage_cost is not None:
        total_cost = [
            compute_cost[0] + transfer_cost[0] + storage_cost[0],
            compute_cost[1] + transfer_cost[1] + storage_cost[1],
        ]
    if timing_history:
        assumptions.append(
            f"Timing uses {int(timing_history['sample_count'])} matched completed job observations."
        )
    else:
        assumptions.append(
            "No comparable execution history is available; timing uses conservative defaults."
        )
    if keep_warm:
        assumptions.append(
            "Keep-warm has no fixed billing end; the shown paid lifetime stops at the first configured warning."
        )
    else:
        assumptions.append(
            "Paid lifetime includes the configured idle shutdown period and provider termination time."
        )
    return {
        "startup_seconds": [
            round(float(startup_range[0]), 3),
            round(float(startup_range[1]), 3),
        ],
        "preparation_seconds": [
            round(float(preparation_range[0]), 3),
            round(float(preparation_range[1]), 3),
        ],
        "execution_seconds": [
            round(float(execution_range[0]), 3),
            round(float(execution_range[1]), 3),
        ],
        "paid_idle_seconds": paid_idle_seconds,
        "termination_seconds": [
            round(DEFAULT_TERMINATION_RANGE_SECONDS[0], 3),
            round(DEFAULT_TERMINATION_RANGE_SECONDS[1], 3),
        ],
        "paid_lifetime_seconds": [round(total_low, 3), round(total_high, 3)],
        "compute_cost_usd": [round(item, 6) for item in compute_cost],
        "incremental_transfer_cost_usd": (
            [round(item, 6) for item in transfer_cost]
            if transfer_cost is not None
            else None
        ),
        "incremental_storage_cost_usd": (
            [round(item, 6) for item in storage_cost]
            if storage_cost is not None
            else None
        ),
        "total_job_cost_usd": (
            [round(item, 6) for item in total_cost] if total_cost is not None else None
        ),
        "cost_complete": cost_complete and total_cost is not None,
        "cost_basis": cost_basis,
        "existing_storage_commitment_usd_per_month": (
            round(float(existing_storage_monthly_usd), 2)
            if existing_storage_monthly_usd is not None
            else None
        ),
        "history_sample_count": int((timing_history or {}).get("sample_count") or 0),
        "confidence": confidence,
        "assumptions": assumptions,
    }


def _midpoint(values: list[float]) -> float:
    return (float(values[0]) + float(values[1])) / 2


def _rank_candidates(
    candidates: list[dict[str, Any]], policy: str
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if not candidates:
        return [], None

    complete = [
        item
        for item in candidates
        if isinstance(item["estimate"].get("total_job_cost_usd"), list)
    ]
    known_costs = [
        _midpoint(item["estimate"]["total_job_cost_usd"]) for item in complete
    ]
    unknown_cost = max(known_costs, default=0.0) + 1_000_000.0
    costs = [
        _midpoint(item["estimate"]["total_job_cost_usd"])
        if isinstance(item["estimate"].get("total_job_cost_usd"), list)
        else unknown_cost
        for item in candidates
    ]
    times = [
        _midpoint(item["estimate"]["paid_lifetime_seconds"]) for item in candidates
    ]
    cost_low, cost_high = min(costs), max(costs)
    time_low, time_high = min(times), max(times)

    for index, candidate in enumerate(candidates):
        cost = costs[index]
        duration = times[index]
        cost_score = (
            0.0 if cost_high == cost_low else (cost - cost_low) / (cost_high - cost_low)
        )
        time_score = (
            0.0
            if time_high == time_low
            else (duration - time_low) / (time_high - time_low)
        )
        if policy == "cheapest":
            score = cost
        elif policy == "fastest":
            score = duration
        else:
            score = 0.65 * time_score + 0.35 * cost_score
        candidate["ranking_score"] = round(score, 9)

    ranked = sorted(
        candidates,
        key=lambda item: (
            item["ranking_score"],
            -float(item["preparation"]["coverage_percent"]),
            float(item["hourly_rate"]),
            str(item["provider"]),
            str(item["offer_id"]),
            str(item.get("region") or ""),
        ),
    )
    for rank, item in enumerate(ranked, 1):
        item["rank"] = rank

    if policy == "manual":
        return ranked, None

    selectable = [item for item in ranked if item["estimate"].get("cost_complete")]
    if not selectable:
        return ranked, None

    selected = selectable[0]
    rationale = [
        f"Meets the {selected['gpu_requirement']['minimum_vram_gb']} GiB minimum VRAM requirement.",
        f"Has {selected['preparation']['coverage_percent']:.1f}% prepared-data coverage.",
    ]
    if policy == "cheapest":
        rationale.append("Has the lowest estimated total job cost among viable offers.")
    elif policy == "fastest":
        rationale.append("Has the lowest estimated time to result among viable offers.")
    else:
        rationale.append(
            "Has the best balanced time and total-cost score among viable offers."
        )
    return ranked, {
        "policy": policy,
        "candidate_id": selected["candidate_id"],
        "rank": int(selected["rank"]),
        "rationale": rationale,
    }


def _storage_credentials_configured() -> bool:
    return bool(
        get_credential(RUNPOD_S3_ACCESS_CREDENTIAL)
        and get_credential(RUNPOD_S3_SECRET_CREDENTIAL)
    )


def build_partition_preflight(
    *,
    config: Any,
    partition: dict[str, Any],
    input_artifacts: dict[str, str],
    provider: str = "auto",
    recommendation_policy: str | None = None,
    max_hourly_rate: float | None = None,
    max_total_job_cost: float | None = None,
    allowed_regions: list[str] | None = None,
    storage: Any,
    cache_registry: CacheRegistry,
    worker_auth_configured: bool | None = None,
    connector_factory: Callable[[str, Any], Any] | None = None,
    history_lookup: Callable[[str, dict[str, Any]], dict[str, Any] | None]
    | None = None,
    now: Callable[[], datetime] = _utc_now,
) -> dict[str, Any]:
    """Build a versioned report without creating or changing paid resources."""

    connector_factory = connector_factory or create_connector
    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    unknowns: list[dict[str, Any]] = []
    requested_provider = _provider_name(provider)
    policy = (
        str(recommendation_policy or config.recommendation_policy or "balanced")
        .strip()
        .lower()
    )
    configured_regions = {
        str(item).strip()
        for item in (getattr(config, "allowed_regions", None) or [])
        if str(item).strip()
    }
    requested_regions = {
        str(item).strip() for item in (allowed_regions or []) if str(item).strip()
    }
    if configured_regions and requested_regions:
        effective_regions = configured_regions.intersection(requested_regions)
        region_allowlist = sorted(effective_regions)
        if not effective_regions:
            blockers.append(
                _issue(
                    "region_policy_conflict",
                    "The requested regions are outside the configured region allowlist.",
                    action="Choose one configured region or change the hard allowlist.",
                    field="allowed_regions",
                )
            )
    else:
        region_allowlist = sorted(configured_regions or requested_regions)
    configured_rate_limit = float(config.max_hourly_rate)
    if not math.isfinite(configured_rate_limit) or configured_rate_limit <= 0:
        blockers.append(
            _issue(
                "invalid_hourly_rate_limit",
                "The hourly price limit must be a positive finite number.",
                field="max_hourly_rate",
            )
        )
        configured_rate_limit = 0.5
    try:
        requested_rate_limit = (
            configured_rate_limit if max_hourly_rate is None else float(max_hourly_rate)
        )
    except (TypeError, ValueError):
        requested_rate_limit = float("nan")
    if not math.isfinite(requested_rate_limit) or requested_rate_limit <= 0:
        blockers.append(
            _issue(
                "invalid_hourly_rate_limit",
                "The hourly price limit must be a positive finite number.",
                field="max_hourly_rate",
            )
        )
        requested_rate_limit = configured_rate_limit
    rate_limit = min(configured_rate_limit, requested_rate_limit)
    configured_total_cost_limit = getattr(config, "max_total_job_cost", None)
    try:
        requested_total_cost_limit = (
            None if max_total_job_cost is None else float(max_total_job_cost)
        )
    except (TypeError, ValueError):
        requested_total_cost_limit = float("nan")
    if requested_total_cost_limit is not None and (
        not math.isfinite(requested_total_cost_limit) or requested_total_cost_limit <= 0
    ):
        blockers.append(
            _issue(
                "invalid_total_cost_limit",
                "The total job cost limit must be a positive finite number.",
                field="max_total_job_cost",
            )
        )
        requested_total_cost_limit = None
    total_cost_limits = [
        float(value)
        for value in (configured_total_cost_limit, requested_total_cost_limit)
        if value is not None
    ]
    total_cost_limit = min(total_cost_limits) if total_cost_limits else None

    if policy not in RECOMMENDATION_POLICIES:
        blockers.append(
            _issue(
                "invalid_recommendation_policy",
                "The recommendation policy is not supported.",
                action="Select balanced, cheapest, fastest, or manual.",
                field="recommendation_policy",
            )
        )
        policy = "balanced"
    if partition.get("schema") != PARTITION_JOB_SCHEMA:
        blockers.append(
            _issue(
                "unsupported_partition_schema",
                "The partition schema is not supported.",
                action=f"Use {PARTITION_JOB_SCHEMA}.",
                field="partition.schema",
            )
        )
    if not isinstance(partition.get("workflow"), dict) or not partition.get("workflow"):
        blockers.append(
            _issue(
                "partition_workflow_required",
                "The partition workflow is required.",
                field="partition.workflow",
            )
        )

    runner = (
        partition.get("runner") if isinstance(partition.get("runner"), dict) else {}
    )
    profile_name = str((runner or {}).get("profile") or "comfyui").strip()[:100]
    if not profile_name.startswith("comfyui"):
        blockers.append(
            _issue(
                "invalid_runner_profile",
                "The ComfyUI runner profile is invalid.",
                field="partition.runner.profile",
            )
        )
    try:
        requested_vram = max(
            1, min(256, int((runner or {}).get("min_gpu_ram_gb") or 16))
        )
    except (TypeError, ValueError):
        requested_vram = 16
        blockers.append(
            _issue(
                "invalid_gpu_vram_requirement",
                "The GPU VRAM requirement must be an integer from 1 through 256 GiB.",
                field="partition.runner.min_gpu_ram_gb",
            )
        )
    requested_gpu_type = (
        str((runner or {}).get("gpu_type") or "any").strip()[:100] or "any"
    )
    residency = str(partition.get("residency") or "cloud")
    if residency not in {"cloud", "on-prem"}:
        blockers.append(
            _issue(
                "invalid_residency",
                "The partition residency must be cloud or on-prem.",
                field="partition.residency",
            )
        )

    profile = resolve_worker_profile(config, profile_name)
    if not profile:
        blockers.append(
            _issue(
                "runner_profile_not_configured",
                f"No configured worker profile provides {profile_name}.",
                action="Configure a pinned worker profile that provides comfyui-partition-v1.",
            )
        )
        profile = {}
    elif "comfyui-partition-v1" not in (profile.get("models") or []):
        blockers.append(
            _issue(
                "partition_capability_not_supported",
                f"Worker profile {profile.get('name') or profile_name} does not provide comfyui-partition-v1.",
                action="Use a worker profile that provides the partition capability.",
            )
        )

    image = str(profile.get("image") or "")
    image_match = _PINNED_IMAGE.search(image)
    image_digest = "sha256:" + image_match.group(1).lower() if image_match else None
    if profile and not image_digest:
        blockers.append(
            _issue(
                "worker_image_not_pinned",
                "The worker image is not pinned by a valid sha256 digest.",
                action="Set the worker profile image to a digest-pinned image.",
            )
        )

    declared_assets: list[dict[str, Any]] = []
    declared_packs: list[dict[str, Any]] = []
    resolved_assets: list[dict[str, Any]] = []
    pack_warnings: list[dict[str, Any]] = []
    try:
        declared_assets = normalized_partition_assets(partition.get("assets"))
    except ValueError as exc:
        blockers.append(
            _issue("invalid_asset_manifest", str(exc), field="partition.assets")
        )
    try:
        declared_packs = normalized_partition_node_packs(partition.get("node_packs"))
    except ValueError as exc:
        blockers.append(
            _issue("invalid_node_pack_manifest", str(exc), field="partition.node_packs")
        )

    for boundary_key, artifact_id in input_artifacts.items():
        if not str(boundary_key).startswith("input_"):
            blockers.append(
                _issue(
                    "invalid_input_boundary",
                    f"Input boundary {boundary_key} is invalid.",
                    field="input_artifacts",
                )
            )
            continue
        try:
            artifact_exists = storage.exists(partition_artifact_key(str(artifact_id)))
        except Exception as exc:  # noqa: BLE001 - a failed proof is a blocker
            blockers.append(
                _issue(
                    "input_artifact_unverifiable",
                    f"Input artifact for {boundary_key} could not be verified ({_safe_error(exc)}).",
                    action="Upload the input artifact again.",
                    field="input_artifacts",
                )
            )
            continue
        if not artifact_exists:
            blockers.append(
                _issue(
                    "input_artifact_not_found",
                    f"Input artifact for {boundary_key} was not found.",
                    action="Upload the input artifact again.",
                    field="input_artifacts",
                )
            )

    if profile and declared_assets:
        resolved_assets, unresolved_assets = resolve_partition_assets(
            config, declared_assets, profile, storage
        )
        if unresolved_assets:
            blockers.append(
                _issue(
                    "unresolved_assets",
                    unresolved_assets_message(unresolved_assets),
                    action="Register or upload every unresolved model file.",
                    details={"count": len(unresolved_assets)},
                )
            )
        for asset in resolved_assets:
            if asset.get("warning") == NAME_MATCHED_WARNING:
                warnings.append(
                    _issue(
                        "asset_matched_by_name",
                        f"{asset['filename']} was matched by name, not by digest.",
                        action="Register a digest-keyed source for stronger identity.",
                    )
                )

    if profile and declared_packs:
        missing_packs = missing_node_packs(declared_packs, profile)
        if missing_packs:
            blockers.append(
                _issue(
                    "missing_node_packs",
                    missing_node_packs_message(missing_packs),
                    action="Add every required custom node pack to the worker profile.",
                    details={"count": len(missing_packs)},
                )
            )
        pack_warnings = node_pack_version_warnings(declared_packs, profile)
        for item in pack_warnings:
            warnings.append(
                _issue(
                    "node_pack_version_differs",
                    f"Custom node pack {item['id']} has a different configured version.",
                    action="Verify the worker image content digest before release.",
                )
            )

    image_bytes = int(float(profile.get("image_size_gb") or 0) * GIB) or None
    storage_plan = plan_storage(
        resolved_assets,
        profile,
        image_bytes=image_bytes,
        weight_bytes=cached_weight_sizes(config, profile),
    )
    disk_gb = plan_disk_gb(storage_plan)
    storage_summary = plan_summary(storage_plan)
    if disk_gb > config.max_container_disk_gb:
        blockers.append(
            _issue(
                "storage_plan_exceeds_ceiling",
                exceeds_ceiling_message(
                    storage_plan, disk_gb, config.max_container_disk_gb
                ),
                action="Raise the disk ceiling or reduce staged data.",
            )
        )
    for item in storage_summary["unknown"]:
        unknowns.append(
            _issue(
                "storage_size_assumption",
                str(item),
                action="Declare or measure this size to improve the estimate.",
            )
        )

    uses_huggingface = bool(profile.get("weights")) or any(
        isinstance(asset.get("source"), dict) and asset["source"].get("repo_id")
        for asset in resolved_assets
    )
    if uses_huggingface and not config.huggingface_configured:
        blockers.append(
            _issue(
                "huggingface_credential_missing",
                "Authenticated Hugging Face downloads are required for this plan.",
                action="Save a Hugging Face token in Cloud Offload settings.",
            )
        )
    prepared_policy = config.prepared_storage or {}
    prepared_enabled = bool(prepared_policy.get("enabled"))
    if prepared_enabled and not _storage_credentials_configured():
        blockers.append(
            _issue(
                "prepared_storage_credentials_missing",
                "RunPod S3 credentials are required for prepared storage.",
                action="Save the RunPod S3 access key and secret key in Cloud Offload settings.",
            )
        )
    if worker_auth_configured is None:
        worker_auth_configured = bool(config.worker_token)
    if not worker_auth_configured:
        blockers.append(
            _issue(
                "worker_token_missing",
                "The worker authentication token is not configured.",
                action="Configure CLOUD_OFFLOAD_WORKER_TOKEN.",
            )
        )
    if config.ingress == "none" and not config.coordinator_url:
        blockers.append(
            _issue(
                "worker_coordinator_route_missing",
                "A rented worker has no configured route to the coordinator.",
                action="Configure a coordinator URL or enable the supported ingress mode.",
            )
        )

    minimum_vram = max(
        requested_vram,
        worker_profile_min_gpu_ram(profile) if profile else 0,
    )
    workload_identity = workload_digest(
        partition,
        profile_name=profile_name,
        minimum_vram_gb=requested_vram,
    )
    # Worker and dispatcher lifetime policy is coordinator-owned. A legacy
    # runner.keep_warm field does not change the paid resource lifecycle.
    effective_keep_warm = bool(getattr(config, "keep_warm", False))
    gpu_type = (
        requested_gpu_type
        if requested_gpu_type.lower() != "any"
        else worker_profile_gpu_type(profile, config.gpu_type)
    )
    supported_providers = [
        name
        for name in (profile.get("providers") or [])
        if name in config.provider_order
        and (
            residency != "on-prem"
            or connector_metadata(name).get("residency_class") == "on-prem"
        )
    ]
    if requested_provider not in {"auto", "cloud"}:
        supported_providers = [
            name for name in supported_providers if name == requested_provider
        ]
    configured_providers = [
        name for name in supported_providers if bool(config.api_key_for(name))
    ]
    if not supported_providers:
        blockers.append(
            _issue(
                "provider_not_supported",
                "No allowed provider supports this worker profile and residency.",
                action="Change the provider, profile, or residency policy.",
            )
        )
    elif not configured_providers:
        blockers.append(
            _issue(
                "provider_credential_missing",
                "No supported provider has a configured credential.",
                action="Save a credential for an allowed provider.",
            )
        )

    ready_prepared_volumes = [
        volume
        for volume in cache_registry.list_volumes(status="ready")
        if volume.provider in configured_providers
        and (not region_allowlist or volume.datacenter_id in region_allowlist)
        and (
            prepared_policy.get("policy") != "pinned"
            or volume.datacenter_id == prepared_policy.get("region")
        )
    ]
    if (
        prepared_enabled
        and prepared_policy.get("policy") in {"strict", "pinned"}
        and not ready_prepared_volumes
    ):
        blockers.append(
            _issue(
                "prepared_volume_required",
                "The strict prepared-storage policy has no verified local volume binding for this plan.",
                action="Verify or adopt a prepared volume in an allowed region.",
            )
        )

    requirements = resolve_prepared_requirements(
        str(profile.get("name") or profile_name),
        profile,
        [SimpleNamespace(request={"assets": resolved_assets})],
    )
    runtime = scheduler_runtime(requirements)
    coverage_by_volume = {
        item["volume"].id: item
        for item in cache_registry.volume_coverage(
            requirements["required"],
            runtime=runtime,
            tenant=str(prepared_policy.get("tenant") or "default"),
            profile_fingerprint=str(requirements["profile_fingerprint"]),
            allow_private=bool(prepared_policy.get("cache_private_assets")),
            logical_required=requirements.get("logical_required") or [],
        )
    }
    required_data_bytes = int(storage_plan.get("assets") or 0) + int(
        storage_plan.get("weights") or 0
    )
    candidates: list[dict[str, Any]] = []

    if not blockers:
        for provider_name in configured_providers:
            try:
                connector = connector_factory(provider_name, config)
            except Exception as exc:  # noqa: BLE001 - becomes a safe report fact
                unknowns.append(
                    _issue(
                        "provider_connector_unavailable",
                        f"{provider_name} could not be inspected ({_safe_error(exc)}).",
                        action="Test the provider connection and run preflight again.",
                    )
                )
                continue

            provider_candidates: list[
                tuple[dict[str, Any], CacheVolume | None, int, bool]
            ] = []
            try:
                for offer in connector.list_available(
                    gpu_type=gpu_type,
                    min_gpu_ram=minimum_vram,
                    max_hourly_rate=rate_limit,
                ):
                    provider_candidates.append((offer, None, 0, False))
            except Exception as exc:  # noqa: BLE001
                unknowns.append(
                    _issue(
                        "provider_offers_unavailable",
                        f"Current {provider_name} offers could not be read ({_safe_error(exc)}).",
                        action="Test the provider connection and run preflight again.",
                    )
                )

            if prepared_enabled:
                for volume in cache_registry.list_volumes(status="ready"):
                    if volume.provider != provider_name:
                        continue
                    if (
                        region_allowlist
                        and volume.datacenter_id not in region_allowlist
                    ):
                        continue
                    if prepared_policy.get(
                        "policy"
                    ) == "pinned" and volume.datacenter_id != prepared_policy.get(
                        "region"
                    ):
                        continue
                    try:
                        actual = connector.get_storage(volume.provider_volume_id)
                    except Exception as exc:  # noqa: BLE001
                        unknowns.append(
                            _issue(
                                "prepared_volume_unverified",
                                f"Prepared volume {volume.id} could not be verified ({_safe_error(exc)}).",
                                action="Verify the prepared volume and run preflight again.",
                            )
                        )
                        continue
                    if actual is None or actual.datacenter_id != volume.datacenter_id:
                        warnings.append(
                            _issue(
                                "prepared_volume_unavailable",
                                f"Prepared volume {volume.id} is absent or in a different region.",
                                action="Repair or remove the prepared volume binding.",
                            )
                        )
                        continue
                    constraints = PlacementConstraints(
                        datacenter_ids=(volume.datacenter_id,),
                        storage_attachments=(
                            StorageAttachment(
                                provider_volume_id=volume.provider_volume_id,
                                datacenter_id=volume.datacenter_id,
                            ),
                        ),
                    )
                    try:
                        offers = connector.list_available(
                            gpu_type=gpu_type,
                            min_gpu_ram=minimum_vram,
                            max_hourly_rate=rate_limit,
                            placement=constraints,
                        )
                    except Exception as exc:  # noqa: BLE001
                        unknowns.append(
                            _issue(
                                "prepared_region_capacity_unknown",
                                f"Capacity near prepared volume {volume.id} could not be read ({_safe_error(exc)}).",
                                action="Run preflight again or allow a cold fallback.",
                            )
                        )
                        continue
                    coverage = coverage_by_volume.get(volume.id) or {}
                    for offer in offers:
                        provider_candidates.append(
                            (
                                offer,
                                volume,
                                int(coverage.get("cached_bytes") or 0),
                                bool(coverage.get("complete")),
                            )
                        )

            seen: set[tuple[str, str, str, str]] = set()
            for offer, volume, cached_bytes, complete in provider_candidates:
                if (
                    volume is None
                    and prepared_enabled
                    and (
                        prepared_policy.get("policy") in {"strict", "pinned"}
                        or prepared_policy.get("cold_fallback") in {"ask", "deny"}
                    )
                ):
                    continue
                try:
                    safe = _safe_offer(offer, provider_name)
                except (TypeError, ValueError, OverflowError) as exc:
                    unknowns.append(
                        _issue(
                            "invalid_provider_offer",
                            f"{provider_name} returned an offer with invalid normalized data ({_safe_error(exc)}).",
                            action="Test the provider connector and run preflight again.",
                        )
                    )
                    continue
                region = volume.datacenter_id if volume else safe["region"]
                if region_allowlist and region not in region_allowlist:
                    continue
                identity = (
                    provider_name,
                    safe["offer_id"],
                    str(region or ""),
                    volume.id if volume else "",
                )
                if identity in seen or not safe["offer_id"]:
                    continue
                seen.add(identity)
                cached = (
                    required_data_bytes
                    if complete
                    else min(required_data_bytes, max(0, cached_bytes))
                )
                performance_class = candidate_class(
                    provider=provider_name,
                    gpu_type=safe["gpu_type"],
                    region=region,
                    prepared=volume is not None,
                )
                timing_history = (
                    history_lookup(workload_identity, performance_class)
                    if history_lookup is not None
                    else None
                )
                existing_storage_monthly_usd = None
                if provider_name == "runpod" and volume is not None:
                    existing_storage_monthly_usd = estimate_runpod_storage_monthly(
                        float(volume.capacity_bytes) / GIB
                    )
                estimate = _estimate(
                    provider=provider_name,
                    offer=safe,
                    required_bytes=required_data_bytes,
                    cached_bytes=cached,
                    container_disk_gb=disk_gb,
                    idle_shutdown_seconds=int(config.idle_shutdown_seconds),
                    keep_warm=effective_keep_warm,
                    keep_warm_warning_seconds=int(config.keep_warm_warning_seconds),
                    timing_history=timing_history,
                    existing_storage_monthly_usd=existing_storage_monthly_usd,
                )
                if total_cost_limit is not None and (
                    not isinstance(estimate["total_job_cost_usd"], list)
                    or estimate["total_job_cost_usd"][1] > total_cost_limit
                ):
                    continue
                coverage_percent = (
                    100.0
                    if complete
                    else (
                        cached / required_data_bytes * 100
                        if required_data_bytes
                        else 0.0
                    )
                )
                candidate_id = _digest(
                    {
                        "provider": provider_name,
                        "offer_id": safe["offer_id"],
                        "region": region,
                        "volume_id": volume.id if volume else None,
                    }
                )
                candidates.append(
                    {
                        "candidate_id": candidate_id,
                        **safe,
                        "region": region,
                        "prepared_volume_id": volume.id if volume else None,
                        "preparation_class": performance_class["preparation_class"],
                        "gpu_requirement": {
                            "requested_type": gpu_type or "any",
                            "minimum_vram_gb": minimum_vram,
                        },
                        "preparation": {
                            "required_bytes": required_data_bytes,
                            "cached_bytes": cached,
                            "missing_bytes": max(0, required_data_bytes - cached),
                            "coverage_percent": round(coverage_percent, 3),
                            "complete": bool(complete),
                        },
                        "estimate": estimate,
                    }
                )

    ranked, recommendation = _rank_candidates(candidates, policy)
    relevant_candidates = (
        [
            item
            for item in ranked
            if recommendation and item["candidate_id"] == recommendation["candidate_id"]
        ]
        if recommendation
        else ranked
    )
    if relevant_candidates and not any(
        int(item["estimate"].get("history_sample_count") or 0) > 0
        for item in relevant_candidates
    ):
        unknowns.append(
            _issue(
                "execution_history_unavailable",
                "No comparable execution history is available; the timing estimate uses conservative defaults.",
                action="Collect completed job observations to improve this estimate.",
            )
        )
    if relevant_candidates and any(
        not bool(item["estimate"].get("cost_complete")) for item in relevant_candidates
    ):
        unknowns.append(
            _issue(
                "incremental_costs_unmeasured",
                "A complete transfer or storage rate is unavailable for one or more current choices.",
                action="Choose a fully priced offer or review provider pricing before launch.",
            )
        )
    if not blockers and not ranked:
        unknowns.append(
            _issue(
                "no_current_viable_offer",
                "No current offer satisfies all GPU, price, provider, region, and storage limits.",
                action="Run preflight again, choose another GPU, or change a hard limit.",
            )
        )

    if blockers:
        status = "blocked"
    elif not ranked or (policy != "manual" and recommendation is None):
        status = "uncertain"
    elif any(item["preparation"]["missing_bytes"] > 0 for item in ranked[:1]):
        status = "ready_with_preparation"
    else:
        status = "ready"

    created_at = now()
    confirmation_policy = str(
        getattr(config, "rental_confirmation", "always") or "always"
    )
    countdown_seconds = int(getattr(config, "confirmation_countdown_seconds", 10))
    confirmation_required = (
        status in {"ready", "ready_with_preparation"}
        and confirmation_policy == "always"
    )
    manifest = {
        "partition": partition,
        "input_artifacts": input_artifacts,
        "profile": profile.get("name") or profile_name,
        "image_digest": image_digest,
        "gpu_requirement": {
            "requested_type": gpu_type or "any",
            "minimum_vram_gb": minimum_vram,
        },
        "container_disk_gb": disk_gb,
        "residency": residency,
        "provider": requested_provider,
        "recommendation_policy": policy,
        "max_hourly_rate": rate_limit,
        "max_total_job_cost": total_cost_limit,
        "allowed_regions": region_allowlist,
        "resolved_asset_digests": sorted(asset["sha256"] for asset in resolved_assets),
        "required_node_pack_digests": sorted(pack["digest"] for pack in declared_packs),
    }
    manifest_digest = _digest(manifest)
    selected = None
    if recommendation:
        selected = next(
            item
            for item in ranked
            if item["candidate_id"] == recommendation["candidate_id"]
        )

    return {
        "schema": PREFLIGHT_SCHEMA,
        "preflight_id": str(uuid.uuid4()),
        "manifest_digest": manifest_digest,
        "workload_digest": workload_identity,
        "status": status,
        "created_at": _iso(created_at),
        "expires_at": _iso(created_at + timedelta(seconds=QUOTE_LIFETIME_SECONDS)),
        "blockers": blockers,
        "warnings": warnings,
        "unknowns": unknowns,
        "request_policy": {
            "provider": requested_provider,
            "recommendation_policy": policy,
            "max_hourly_rate": rate_limit,
            "max_total_job_cost": total_cost_limit,
            "allowed_regions": region_allowlist,
            "material_price_change_percent": float(
                getattr(config, "material_price_change_percent", 5.0)
            ),
            "material_cost_change_percent": float(
                getattr(config, "material_cost_change_percent", 10.0)
            ),
        },
        "confirmation": {
            "policy": confirmation_policy,
            "required": confirmation_required,
            "mandatory": False,
            "reason": (
                "policy_always"
                if confirmation_required
                else f"policy_{confirmation_policy}"
            ),
            "countdown_seconds": countdown_seconds,
            "not_before": (
                _iso(created_at + timedelta(seconds=countdown_seconds))
                if confirmation_required
                else None
            ),
        },
        "execution_plan": {
            "profile": profile.get("name") or profile_name,
            "image_digest": image_digest,
            "gpu_requirement": {
                "requested_type": gpu_type or "any",
                "minimum_vram_gb": minimum_vram,
            },
            "container_disk_gb": disk_gb,
            "residency": residency,
            "provider": selected["provider"] if selected else None,
            "offer_id": selected["offer_id"] if selected else None,
            "region": selected["region"] if selected else None,
            "prepared_volume_id": selected["prepared_volume_id"] if selected else None,
        },
        "preparation": (
            selected["preparation"]
            if selected
            else {
                "required_bytes": required_data_bytes,
                "cached_bytes": 0,
                "missing_bytes": required_data_bytes,
                "coverage_percent": 0.0,
                "complete": False,
            }
        ),
        "storage": storage_summary,
        "estimate": selected["estimate"] if selected else None,
        "recommendation": recommendation,
        "candidates": ranked,
        "quote": {
            "volatile": True,
            "valid_for_seconds": QUOTE_LIFETIME_SECONDS,
            "revalidate_before_launch": [
                "offer availability",
                "hourly price",
                "region",
                "prepared volume",
            ],
        },
    }


def finite_report(report: dict[str, Any]) -> bool:
    """Return false when a report contains a non-finite numeric value."""

    def walk(value: Any):
        yield value
        if isinstance(value, dict):
            for item in value.values():
                yield from walk(item)
        elif isinstance(value, list):
            for item in value:
                yield from walk(item)

    return all(
        not isinstance(value, float) or math.isfinite(value) for value in walk(report)
    )
