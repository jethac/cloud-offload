"""Demand-weighted regional replication shadow recommendations."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from cloud_offload.cache_registry import CacheRegistry, CacheVolume
from cloud_offload.config import estimate_runpod_storage_monthly


SCHEMA = "cloud-offload.replication-shadow.v1"
GIB = 1024**3


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _identity(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _manifest_bytes(manifest: dict[str, Any]) -> int:
    return sum(
        max(0, int(item.get("size") or 0))
        for item in manifest.get("artifacts") or []
        if isinstance(item, dict)
    )


def _preparation_seconds(value: Any) -> float:
    if isinstance(value, list) and value:
        value = value[0]
    try:
        return max(0.0, float(value or 0))
    except (TypeError, ValueError):
        return 0.0


def _volume_by_id(registry: CacheRegistry) -> dict[str, CacheVolume]:
    return {item.id: item for item in registry.list_volumes()}


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


def shadow_accuracy(
    registry: CacheRegistry,
    prepared_storage_policy: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Score unique mature recommendations against later paid demand."""

    current = (now or _utc_now()).astimezone(timezone.utc)
    replication = dict(prepared_storage_policy.get("replication") or {})
    validation_hours = max(
        1, int(replication.get("shadow_validation_hours") or 24)
    )
    required = max(
        3, int(replication.get("shadow_required_recommendations") or 10)
    )
    minimum_precision = float(replication.get("shadow_min_precision") or 0.8)
    unique: dict[str, dict[str, Any]] = {}
    for report in reversed(registry.list_shadow_evaluations(limit=1000)):
        created_at = str(report.get("created_at") or "")
        for item in report.get("recommendations") or []:
            recommendation_id = str(item.get("recommendation_id") or "")
            if recommendation_id and created_at:
                unique.setdefault(
                    recommendation_id,
                    {**item, "first_recommended_at": created_at},
                )
    observations = registry.list_regional_demand()
    matured = []
    validated = []
    for item in unique.values():
        first = _parse_time(item["first_recommended_at"])
        if current < first + timedelta(hours=validation_hours):
            continue
        expiry = _parse_time(str(item.get("expires_at") or _iso(current)))
        followup = any(
            demand.get("profile_fingerprint") == item.get("profile_fingerprint")
            and demand.get("provider") == item.get("provider")
            and demand.get("datacenter_id") == item.get("target_region")
            and first < _parse_time(str(demand.get("created_at"))) <= expiry
            for demand in observations
        )
        matured.append(item)
        if followup:
            validated.append(item)
    precision = len(validated) / len(matured) if matured else 0.0
    return {
        "schema": "cloud-offload.replication-shadow-accuracy.v1",
        "created_at": _iso(current),
        "unique_recommendation_count": len(unique),
        "mature_recommendation_count": len(matured),
        "validated_recommendation_count": len(validated),
        "precision": round(precision, 6),
        "required_recommendations": required,
        "required_precision": minimum_precision,
        "validation_hours": validation_hours,
        "automation_gate_passed": len(matured) >= required
        and precision >= minimum_precision,
    }


def build_shadow_report(
    registry: CacheRegistry,
    prepared_storage_policy: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a safe read-only recommendation report from paid demand."""

    current = (now or _utc_now()).astimezone(timezone.utc)
    replication = dict(prepared_storage_policy.get("replication") or {})
    mode = str(replication.get("mode") or "shadow")
    window_days = max(1, int(replication.get("demand_window_days") or 30))
    ttl_days = max(1, int(replication.get("ttl_days") or 30))
    minimum_hits = max(1, int(replication.get("min_hits") or 3))
    minimum_saved_seconds = max(
        0.0, float(replication.get("min_avoided_gpu_seconds") or 0)
    )
    approved_regions = {
        str(item) for item in replication.get("approved_regions") or [] if str(item)
    }
    monthly_budget = replication.get("monthly_budget_usd")
    monthly_budget = None if monthly_budget is None else float(monthly_budget)
    transfer_rate = replication.get("transfer_cost_per_gb_usd")
    transfer_rate = None if transfer_rate is None else float(transfer_rate)
    since = _iso(current - timedelta(days=window_days))
    demand = registry.list_regional_demand(since=since)
    volumes = _volume_by_id(registry)

    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for item in demand:
        if int(item.get("missing_bytes") or 0) <= 0:
            continue
        key = (
            str(item.get("profile_fingerprint") or ""),
            str(item.get("provider") or ""),
            str(item.get("datacenter_id") or ""),
        )
        if all(key):
            groups.setdefault(key, []).append(item)

    candidates: list[dict[str, Any]] = []
    for (profile, provider, target_region), observations in groups.items():
        manifests = registry.query_manifests(profile_fingerprint=profile)
        existing_target = next(
            (
                item
                for item in manifests
                if item.get("datacenter_id") == target_region
                and (volume := volumes.get(str(item.get("volume_id") or "")))
                and volume.provider == provider
                and volume.status == "ready"
                and volume.s3_compatible
            ),
            None,
        )
        if existing_target:
            candidates.append(
                {
                    "profile_fingerprint": profile,
                    "provider": provider,
                    "target_region": target_region,
                    "status": "covered",
                    "reason_codes": ["compatible_replica_already_present"],
                    "expected_hits": len(observations),
                }
            )
            continue
        source = next(
            (
                item
                for item in manifests
                if item.get("datacenter_id") != target_region
                and (volume := volumes.get(str(item.get("volume_id") or "")))
                and volume.provider == provider
                and volume.status == "ready"
                and volume.s3_compatible
            ),
            None,
        )
        source_volume = (
            volumes.get(str(source.get("volume_id") or "")) if source else None
        )
        target_volume = next(
            (
                item
                for item in volumes.values()
                if item.provider == provider
                and item.datacenter_id == target_region
                and item.status == "ready"
                and item.s3_compatible
                and (source_volume is None or item.id != source_volume.id)
            ),
            None,
        )
        artifact_bytes = _manifest_bytes(source or {})
        expected_hits = len(observations)
        saved_seconds = round(
            sum(_preparation_seconds(item.get("preparation_seconds")) for item in observations),
            3,
        )
        saved_gpu_cost = round(
            sum(
                _preparation_seconds(item.get("preparation_seconds"))
                * max(0.0, float(item.get("hourly_rate") or 0))
                / 3600
                for item in observations
            ),
            6,
        )
        target_size_gb = (
            float(target_volume.capacity_bytes) / GIB
            if target_volume
            else float(prepared_storage_policy.get("managed_size_gb") or 0)
        )
        incremental_storage_cost = (
            0.0
            if target_volume
            else estimate_runpod_storage_monthly(target_size_gb)
        )
        copy_cost = (
            None
            if transfer_rate is None
            else round(artifact_bytes / GIB * transfer_rate, 6)
        )
        reason_codes: list[str] = []
        if mode == "off":
            reason_codes.append("replication_off")
        if approved_regions and target_region not in approved_regions:
            reason_codes.append("region_not_approved")
        if source is None or source_volume is None:
            reason_codes.append("compatible_source_missing")
        if artifact_bytes <= 0:
            reason_codes.append("source_manifest_has_no_copyable_bytes")
        if expected_hits < minimum_hits:
            reason_codes.append("demand_below_minimum")
        if saved_seconds < minimum_saved_seconds:
            reason_codes.append("benefit_below_minimum")
        if monthly_budget is None:
            reason_codes.append("monthly_budget_unknown")
        elif incremental_storage_cost > monthly_budget:
            reason_codes.append("monthly_budget_exceeded")
        if target_volume is None:
            reason_codes.append("target_volume_missing")
        if transfer_rate is None:
            reason_codes.append("transfer_cost_unknown")

        recommendation_key = {
            "profile_fingerprint": profile,
            "provider": provider,
            "source_volume_id": source_volume.id if source_volume else None,
            "source_manifest_id": source.get("manifest_id") if source else None,
            "target_region": target_region,
            "target_volume_id": target_volume.id if target_volume else None,
        }
        automatic_blockers = [
            code
            for code in reason_codes
            if code
            in {
                "replication_off",
                "region_not_approved",
                "compatible_source_missing",
                "source_manifest_has_no_copyable_bytes",
                "demand_below_minimum",
                "benefit_below_minimum",
                "monthly_budget_unknown",
                "monthly_budget_exceeded",
                "target_volume_missing",
                "transfer_cost_unknown",
            }
        ]
        candidates.append(
            {
                "recommendation_id": _identity(recommendation_key),
                **recommendation_key,
                "source_region": source_volume.datacenter_id if source_volume else None,
                "bytes": artifact_bytes,
                "expected_hits": expected_hits,
                "expected_time_saved_seconds": saved_seconds,
                "expected_gpu_idle_cost_saved_usd": saved_gpu_cost,
                "estimated_copy_cost_usd": copy_cost,
                "incremental_monthly_storage_cost_usd": round(
                    incremental_storage_cost, 6
                ),
                "monthly_budget_usd": monthly_budget,
                "expires_at": _iso(current + timedelta(days=ttl_days)),
                "status": (
                    "recommended"
                    if not {
                        "replication_off",
                        "region_not_approved",
                        "compatible_source_missing",
                        "source_manifest_has_no_copyable_bytes",
                        "demand_below_minimum",
                        "benefit_below_minimum",
                        "monthly_budget_exceeded",
                    }.intersection(reason_codes)
                    else "observe"
                ),
                "reason_codes": reason_codes or ["measured_value"],
                "eligible_for_automatic": mode == "automatic"
                and not automatic_blockers,
                "automatic_blockers": automatic_blockers,
                "cold_fallback_visible": True,
            }
        )

    candidates.sort(
        key=lambda item: (
            item.get("status") != "recommended",
            -float(item.get("expected_time_saved_seconds") or 0),
            str(item.get("target_region") or ""),
            str(item.get("profile_fingerprint") or ""),
        )
    )
    projected_monthly_cost = 0.0
    planned_target_regions: set[tuple[str, str]] = set()
    for item in candidates:
        if item.get("status") != "recommended":
            continue
        region_key = (str(item.get("provider")), str(item.get("target_region")))
        incremental = float(item.get("incremental_monthly_storage_cost_usd") or 0)
        if item.get("target_volume_id") is None and region_key in planned_target_regions:
            incremental = 0.0
            item["incremental_monthly_storage_cost_usd"] = 0.0
        if monthly_budget is not None and projected_monthly_cost + incremental > monthly_budget:
            item["status"] = "observe"
            if "monthly_budget_exceeded" not in item["reason_codes"]:
                item["reason_codes"].append("monthly_budget_exceeded")
            if "monthly_budget_exceeded" not in item["automatic_blockers"]:
                item["automatic_blockers"].append("monthly_budget_exceeded")
            item["eligible_for_automatic"] = False
            continue
        projected_monthly_cost += incremental
        if item.get("target_volume_id") is None:
            planned_target_regions.add(region_key)
    recommendations = [
        item for item in candidates if item.get("status") == "recommended"
    ]
    return {
        "schema": SCHEMA,
        "evaluation_id": str(uuid.uuid4()),
        "created_at": _iso(current),
        "mode": mode,
        "shadow": True,
        "provider_mutation": False,
        "window_days": window_days,
        "demand_observation_count": len(demand),
        "demand_group_count": len(groups),
        "projected_incremental_monthly_storage_cost_usd": round(
            projected_monthly_cost, 6
        ),
        "recommendations": recommendations,
        "decisions": candidates,
        "policy": {
            "approved_regions": sorted(approved_regions),
            "monthly_budget_usd": monthly_budget,
            "ttl_days": ttl_days,
            "min_hits": minimum_hits,
            "min_avoided_gpu_seconds": minimum_saved_seconds,
            "transfer_cost_known": transfer_rate is not None,
        },
    }
