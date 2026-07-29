"""Deterministic, explainable storage-aware placement policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from cloud_offload.cache_registry import CacheVolume
from cloud_offload.providers.base import PlacementConstraints, StorageAttachment
from cloud_offload.prepared_state import (
    custom_node_requirement_key,
    profile_key,
    profile_weight_requirement_key,
)


@dataclass(frozen=True)
class PlacementCandidate:
    offer: dict[str, Any]
    volume: CacheVolume | None
    cached_bytes: int = 0
    required_bytes: int = 0
    complete: bool = False
    manifest_ids: tuple[str, ...] = ()
    estimate: dict[str, float] = field(default_factory=dict)

    @property
    def hourly_rate(self) -> float:
        return float(self.offer.get("hourly_rate", float("inf")))

    @property
    def datacenter_id(self) -> str | None:
        return self.volume.datacenter_id if self.volume else None


@dataclass(frozen=True)
class PlacementDecision:
    action: str
    candidate: PlacementCandidate | None
    reason: str
    considered: tuple[dict[str, Any], ...]
    fallback: bool = False

    def placement(self) -> PlacementConstraints | None:
        if not self.candidate or not self.candidate.volume:
            return None
        volume = self.candidate.volume
        return PlacementConstraints(
            datacenter_ids=(volume.datacenter_id,),
            storage_attachments=(
                StorageAttachment(
                    provider_volume_id=volume.provider_volume_id,
                    mount_path="/workspace",
                    datacenter_id=volume.datacenter_id,
                ),
            ),
        )

    def explanation(self) -> dict[str, Any]:
        candidate = self.candidate
        return {
            "action": self.action,
            "reason": self.reason,
            "fallback": self.fallback,
            "provider": candidate.offer.get("provider") if candidate else None,
            "offer_id": candidate.offer.get("id") if candidate else None,
            "datacenter_id": candidate.datacenter_id if candidate else None,
            "volume_id": candidate.volume.id if candidate and candidate.volume else None,
            "provider_volume_id": (
                candidate.volume.provider_volume_id
                if candidate and candidate.volume else None
            ),
            "cached_bytes": candidate.cached_bytes if candidate else 0,
            "required_bytes": candidate.required_bytes if candidate else 0,
            "complete": candidate.complete if candidate else False,
            "manifest_ids": list(candidate.manifest_ids) if candidate else [],
            "hourly_rate": candidate.hourly_rate if candidate else None,
            "considered": list(self.considered),
        }


def resolve_prepared_requirements(
    profile_name: str, profile: dict[str, Any], jobs: list[Any]
) -> dict[str, Any]:
    """Canonical queue-time requirement identity from declared immutable inputs."""
    artifacts: dict[str, dict[str, Any]] = {}
    for job in jobs:
        for asset in ((getattr(job, "request", None) or {}).get("assets") or []):
            if not isinstance(asset, dict) or not asset.get("sha256"):
                continue
            digest = "sha256:" + str(asset["sha256"]).removeprefix("sha256:").lower()
            artifacts[digest] = {
                "digest": digest,
                "size": int(asset.get("size") or 0),
                "kind": "model-weight",
                "category": str(asset.get("category") or ""),
                "filename": str(asset.get("filename") or ""),
                "policy": {
                    "tenant": str(asset.get("tenant") or "default"),
                    "cacheable": bool(asset.get("cacheable", True)),
                    "private": bool(asset.get("private") or asset.get("gated")),
                },
            }
    weight_identity = [
        {
            "repo_id": str(item.get("repo_id") or ""),
            "revision": str(item.get("revision") or ""),
            "dest": str(item.get("dest") or ""),
            "files": sorted(str(name) for name in (item.get("files") or [])),
            "gated": bool(item.get("gated")),
        }
        for item in profile.get("weights") or []
    ]
    runtime_identity = {
        "profile": profile_name,
        "image": profile.get("image"),
        "custom_nodes": profile.get("custom_nodes") or [],
        "weights": sorted(
            weight_identity,
            key=lambda item: (
                item["repo_id"], item["revision"], item["dest"], item["files"]
            ),
        ),
        "wheelhouse_sha256": profile.get("wheelhouse_sha256") or None,
    }
    logical_required = []
    for item in profile.get("weights") or []:
        files = item.get("files") or []
        if files:
            logical_required.extend(
                profile_weight_requirement_key(
                    str(item.get("repo_id") or ""),
                    str(item.get("revision") or ""),
                    str(filename),
                )
                for filename in files
            )
        else:
            logical_required.append(
                profile_weight_requirement_key(
                    str(item.get("repo_id") or ""),
                    str(item.get("revision") or ""),
                )
            )
    if profile.get("custom_nodes"):
        from cloud_offload.profiles import profile_pack_identifier

        logical_required.extend(
            custom_node_requirement_key(profile_pack_identifier(item))
            for item in profile["custom_nodes"]
        )
    keys = sorted([*artifacts, "runtime:" + json_fingerprint(runtime_identity)])
    return {
        "profile_fingerprint": profile_key(profile_name, keys),
        "runtime_identity": runtime_identity,
        "artifacts": [artifacts[key] for key in sorted(artifacts)],
        "required": {key: item["size"] for key, item in sorted(artifacts.items())},
        "logical_required": sorted(set(logical_required)),
    }


def scheduler_runtime(requirements: dict[str, Any]) -> dict[str, Any]:
    """Known control-plane runtime fields; unknown ABI fields deliberately stay absent."""
    from cloud_offload.prepared_state import fingerprint

    identity = requirements.get("runtime_identity") or {}
    image = str(identity.get("image") or "")
    image_digest = (
        "sha256:" + image.rsplit("@sha256:", 1)[1]
        if "@sha256:" in image
        else ""
    )
    return {
        "image_digest": image_digest,
        "dependency_lock": fingerprint(
            {
                "custom_nodes": identity.get("custom_nodes") or [],
                "wheelhouse_sha256": identity.get("wheelhouse_sha256"),
            }
        ),
    }


def json_fingerprint(value: Any) -> str:
    from cloud_offload.prepared_state import fingerprint

    return fingerprint(value)


def estimate_completion_cost(
    *,
    provider_startup_ms: float = 0,
    cache_lookup_ms: float = 0,
    missing_bytes: int = 0,
    source_bytes_per_second: float = 0,
    materialization_ms: float = 0,
    runtime_setup_ms: float = 0,
    execution_ms: float = 0,
    expected_dollars: float = 0,
    monetary_cost_weight: float = 0,
) -> dict[str, float]:
    transfer_ms = (
        missing_bytes / source_bytes_per_second * 1000
        if missing_bytes and source_bytes_per_second > 0
        else 0
    )
    total = (
        provider_startup_ms + cache_lookup_ms + transfer_ms + materialization_ms
        + runtime_setup_ms + execution_ms + monetary_cost_weight * expected_dollars
    )
    return {
        "provider_startup_ms": float(provider_startup_ms),
        "cache_lookup_ms": float(cache_lookup_ms),
        "missing_transfer_ms": float(transfer_ms),
        "materialization_ms": float(materialization_ms),
        "runtime_setup_ms": float(runtime_setup_ms),
        "execution_ms": float(execution_ms),
        "expected_dollars": float(expected_dollars),
        "monetary_cost_weight": float(monetary_cost_weight),
        "estimated_completion_cost": float(total),
    }


def choose_placement(
    *,
    policy: dict[str, Any],
    cached_candidates: list[PlacementCandidate],
    cold_offers: list[dict[str, Any]],
) -> PlacementDecision:
    """Apply hard policy, complete→coverage→price preference, then fallback."""
    mode = str(policy.get("policy") or "off")
    region = str(policy.get("region") or "auto")
    fallback_policy = str(policy.get("cold_fallback") or "allow")
    enabled = bool(policy.get("enabled")) and mode != "off"
    if not enabled:
        cold = _cheapest(cold_offers)
        return PlacementDecision(
            "launch" if cold else "unavailable",
            PlacementCandidate(cold, None) if cold else None,
            "prepared_storage_disabled" if cold else "no_compute_capacity",
            tuple(_summaries(cached_candidates, cold_offers)),
        )

    eligible = [candidate for candidate in cached_candidates if candidate.volume]
    if mode == "pinned":
        eligible = [item for item in eligible if item.datacenter_id == region]
    eligible.sort(
        key=lambda item: (
            -int(item.complete),
            -int(item.cached_bytes),
            item.hourly_rate,
            str(item.datacenter_id or ""),
            str(item.offer.get("id") or ""),
            item.volume.id if item.volume else "",
        )
    )
    considered = tuple(_summaries(cached_candidates, cold_offers))
    if eligible:
        chosen = eligible[0]
        reason = "complete_compatible_cache" if chosen.complete else "greatest_compatible_byte_coverage"
        if chosen.cached_bytes == 0:
            reason = "eligible_storage_population"
        return PlacementDecision("launch", chosen, reason, considered)

    if mode in {"strict", "pinned"} or fallback_policy == "deny":
        return PlacementDecision(
            "unavailable", None, "eligible_cache_placement_has_no_capacity", considered
        )
    if fallback_policy == "ask":
        return PlacementDecision(
            "ask", None, "cold_fallback_requires_user_decision", considered
        )
    cold = _cheapest(cold_offers)
    if not cold:
        return PlacementDecision(
            "unavailable", None, "no_cached_or_cold_compute_capacity", considered
        )
    return PlacementDecision(
        "launch",
        PlacementCandidate(cold, None),
        "cached_datacenter_unavailable_running_cold",
        considered,
        fallback=True,
    )


def _cheapest(offers: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not offers:
        return None
    return min(
        offers,
        key=lambda item: (
            float(item.get("hourly_rate", float("inf"))),
            str(item.get("provider") or ""),
            str(item.get("id") or ""),
        ),
    )


def _summaries(
    cached: list[PlacementCandidate], cold: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    summaries = [
        {
            "offer_id": item.offer.get("id"),
            "provider": item.offer.get("provider"),
            "datacenter_id": item.datacenter_id,
            "volume_id": item.volume.id if item.volume else None,
            "cached_bytes": item.cached_bytes,
            "required_bytes": item.required_bytes,
            "complete": item.complete,
            "hourly_rate": item.hourly_rate,
            "estimate": item.estimate,
        }
        for item in cached
    ]
    summaries.extend(
        {
            "offer_id": item.get("id"), "provider": item.get("provider"),
            "datacenter_id": None, "volume_id": None, "cached_bytes": 0,
            "required_bytes": 0, "complete": False,
            "hourly_rate": float(item.get("hourly_rate", float("inf"))),
            "estimate": {},
        }
        for item in cold
    )
    return sorted(
        summaries,
        key=lambda item: (
            str(item.get("provider") or ""), str(item.get("datacenter_id") or ""),
            str(item.get("offer_id") or ""), str(item.get("volume_id") or ""),
        ),
    )
