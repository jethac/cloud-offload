import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.config import CloudConfig
from cloud_offload.prepared_state import ManifestSigner, blob_key, build_manifest, fingerprint
from cloud_offload.regional_replication import build_shadow_report


GIB = 1024**3


def replication_policy(**updates):
    replication_updates = updates.pop("replication", {})
    replication = {
        "mode": "shadow",
        "approved_regions": ["B"],
        "monthly_budget_usd": 20,
        "ttl_days": 14,
        "demand_window_days": 30,
        "min_hits": 3,
        "min_avoided_gpu_seconds": 600,
        "transfer_cost_per_gb_usd": 0,
        "max_inflight": 1,
    }
    if isinstance(replication_updates, dict):
        replication.update(replication_updates)
    else:
        replication = replication_updates
    return {
        "enabled": True,
        "provider": "runpod",
        "policy": "smart",
        "region": "auto",
        "cold_fallback": "allow",
        "managed_size_gb": 100,
        "existing_volume_id": "source-provider-volume",
        "max_monthly_storage_cost": 30,
        "confirmed": True,
        "tenant": "default",
        "cache_private_assets": False,
        "shadow_admission": True,
        "replication": replication,
        **updates,
    }


def artifact(data: bytes):
    digest = hashlib.sha256(data).hexdigest()
    return {
        "digest": "sha256:" + digest,
        "kind": "model-weight",
        "size": len(data),
        "storage_key": blob_key(digest),
        "portability": "portable",
        "requirements": {},
        "policy": {"tenant": "default", "cacheable": True},
    }


def add_volume(registry, provider_volume_id, region):
    return registry.upsert_volume(
        provider="runpod",
        provider_volume_id=provider_volume_id,
        datacenter_id=region,
        ownership="managed",
        capacity_bytes=100 * GIB,
        policy=replication_policy(),
        s3_compatible=True,
    )


def add_manifest(registry, volume, profile, content=b"prepared-state"):
    manifest = build_manifest(
        profile_fingerprint=profile,
        producer={
            "image_digest": "sha256:" + "a" * 64,
            "cloud_offload_version": "test",
            "platform": "linux-x86_64",
            "python_abi": "cp311",
        },
        artifacts=[artifact(content)],
        signer=ManifestSigner(b"r" * 32),
        claims={"cache_volume_id": volume.id},
    )
    registry.reconcile_index(
        volume.id,
        {
            "schema": "cloud-offload.prepared-state.index.v1",
            "generation": "generation-1",
            "manifests": [manifest],
        },
        manifest_documents={manifest["manifest_id"]: manifest},
    )
    return manifest


def record_demand(registry, profile, *, count=3, region="B"):
    for index in range(count):
        registry.record_regional_demand(
            job_id=f"job-{region}-{index}",
            profile_fingerprint=profile,
            provider="runpod",
            datacenter_id=region,
            prepared_volume_id=None,
            required_bytes=100,
            cached_bytes=0,
            missing_bytes=100,
            preparation_seconds=300,
            hourly_rate=1.2,
        )


def test_replication_policy_is_bounded_and_automatic_requires_budget():
    config = CloudConfig(prepared_storage=replication_policy())
    replication = config.prepared_storage["replication"]
    assert replication["mode"] == "shadow"
    assert replication["approved_regions"] == ["B"]
    assert replication["ttl_days"] == 14
    assert replication["monthly_budget_usd"] == 20.0

    with pytest.raises(ValueError, match="monthly_budget_usd is required"):
        CloudConfig(
            prepared_storage=replication_policy(
                replication={"mode": "automatic", "monthly_budget_usd": None}
            )
        )
    with pytest.raises(ValueError, match="cannot exceed"):
        CloudConfig(
            prepared_storage=replication_policy(
                max_monthly_storage_cost=10,
                replication={"monthly_budget_usd": 11},
            )
        )
    with pytest.raises(ValueError, match="must be an object"):
        CloudConfig(prepared_storage=replication_policy(replication=[]))


def test_paid_regional_demand_is_idempotent_and_hides_job_identity(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    profile = fingerprint({"profile": "demand"})
    first = registry.record_regional_demand(
        job_id="paid-job",
        profile_fingerprint=profile,
        provider="runpod",
        datacenter_id="B",
        prepared_volume_id=None,
        required_bytes=100,
        cached_bytes=10,
        missing_bytes=90,
        preparation_seconds=20,
        hourly_rate=1.0,
    )
    duplicate = registry.record_regional_demand(
        job_id="paid-job",
        profile_fingerprint=profile,
        provider="runpod",
        datacenter_id="B",
        prepared_volume_id=None,
        required_bytes=100,
        cached_bytes=10,
        missing_bytes=90,
        preparation_seconds=20,
        hourly_rate=1.0,
    )

    assert first["created"] is True
    assert duplicate["created"] is False
    demand = registry.list_regional_demand()
    assert len(demand) == 1
    assert "job_id" not in demand[0]


def test_shadow_recommends_measured_copy_to_existing_target(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    source = add_volume(registry, "source-provider-volume", "A")
    target = add_volume(registry, "target-provider-volume", "B")
    profile = fingerprint({"profile": "shape"})
    manifest = add_manifest(registry, source, profile)
    record_demand(registry, profile)

    report = build_shadow_report(
        registry,
        replication_policy(),
        now=datetime(2026, 7, 30, tzinfo=timezone.utc),
    )

    assert report["schema"] == "cloud-offload.replication-shadow.v1"
    assert report["provider_mutation"] is False
    assert report["demand_observation_count"] == 3
    assert len(report["recommendations"]) == 1
    recommendation = report["recommendations"][0]
    assert recommendation["source_volume_id"] == source.id
    assert recommendation["source_manifest_id"] == manifest["manifest_id"]
    assert recommendation["source_region"] == "A"
    assert recommendation["target_volume_id"] == target.id
    assert recommendation["target_region"] == "B"
    assert recommendation["bytes"] == len(b"prepared-state")
    assert recommendation["expected_hits"] == 3
    assert recommendation["expected_time_saved_seconds"] == 900
    assert recommendation["incremental_monthly_storage_cost_usd"] == 0
    assert recommendation["estimated_copy_cost_usd"] == 0
    assert recommendation["cold_fallback_visible"] is True
    assert recommendation["eligible_for_automatic"] is False
    assert recommendation["automatic_blockers"] == []

    evaluation_id = registry.record_shadow_evaluation(report)
    recorded = registry.list_shadow_evaluations(limit=1)[0]
    assert recorded["evaluation_id"] == evaluation_id
    assert recorded["recommendations"][0]["recommendation_id"] == recommendation[
        "recommendation_id"
    ]


def test_shadow_keeps_low_demand_as_observation(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    source = add_volume(registry, "source-provider-volume", "A")
    add_volume(registry, "target-provider-volume", "B")
    profile = fingerprint({"profile": "parts"})
    add_manifest(registry, source, profile)
    record_demand(registry, profile, count=1)

    report = build_shadow_report(registry, replication_policy())

    assert report["recommendations"] == []
    decision = report["decisions"][0]
    assert decision["status"] == "observe"
    assert "demand_below_minimum" in decision["reason_codes"]
    assert "benefit_below_minimum" in decision["reason_codes"]


def test_shadow_counts_one_new_volume_cost_for_one_target_region(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    source = add_volume(registry, "source-provider-volume", "A")
    second_source = add_volume(registry, "second-source-provider-volume", "A")
    first_profile = fingerprint({"profile": "first"})
    second_profile = fingerprint({"profile": "second"})
    add_manifest(registry, source, first_profile, b"first")
    add_manifest(registry, second_source, second_profile, b"second")
    record_demand(registry, first_profile, region="B")
    for index in range(3):
        registry.record_regional_demand(
            job_id=f"job-B-second-{index}",
            profile_fingerprint=second_profile,
            provider="runpod",
            datacenter_id="B",
            prepared_volume_id=None,
            required_bytes=100,
            cached_bytes=0,
            missing_bytes=100,
            preparation_seconds=300,
            hourly_rate=1.2,
        )

    report = build_shadow_report(registry, replication_policy())

    assert len(report["recommendations"]) == 2
    assert sorted(
        item["incremental_monthly_storage_cost_usd"]
        for item in report["recommendations"]
    ) == [0.0, 7.0]
    assert report["projected_incremental_monthly_storage_cost_usd"] == 7.0
    assert all(
        "target_volume_missing" in item["automatic_blockers"]
        for item in report["recommendations"]
    )


def test_shadow_does_not_recommend_an_existing_compatible_replica(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    source = add_volume(registry, "source-provider-volume", "A")
    target = add_volume(registry, "target-provider-volume", "B")
    profile = fingerprint({"profile": "scene"})
    add_manifest(registry, source, profile)
    add_manifest(registry, target, profile)
    record_demand(registry, profile)

    report = build_shadow_report(registry, replication_policy())

    assert report["recommendations"] == []
    assert report["decisions"] == [
        {
            "profile_fingerprint": profile,
            "provider": "runpod",
            "target_region": "B",
            "status": "covered",
            "reason_codes": ["compatible_replica_already_present"],
            "expected_hits": 3,
        }
    ]


def test_paid_submit_hook_records_and_refreshes_shadow(tmp_path):
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=replication_policy(),
    )
    profile = fingerprint({"profile": "hook"})
    report = {"execution_plan": {"profile_fingerprint": profile}}
    candidate = {
        "provider": "runpod",
        "region": "B",
        "prepared_volume_id": None,
        "hourly_rate": 1.0,
        "preparation": {
            "required_bytes": 100,
            "cached_bytes": 0,
            "missing_bytes": 100,
        },
        "estimate": {"preparation_seconds": [30, 60]},
    }

    server._record_regional_demand(config, report, candidate, "queued-job")

    registry = CacheRegistry(config.queue_db_path)
    demand = registry.list_regional_demand()
    history = registry.list_shadow_evaluations()
    assert len(demand) == 1
    assert demand[0]["preparation_seconds"] == 30
    assert len(history) == 1
    assert history[0]["provider_mutation"] is False


def test_shadow_routes_record_and_return_safe_history(tmp_path, monkeypatch):
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=replication_policy(),
    )
    monkeypatch.setattr(server, "_config", lambda **kwargs: config)
    client = TestClient(server.app)

    evaluation = client.post("/api/cache/replication/shadow")
    history = client.get("/api/cache/replication/shadow?limit=1")

    assert evaluation.status_code == 200
    assert evaluation.json()["provider_mutation"] is False
    assert history.status_code == 200
    body = history.json()
    assert body["schema"] == "cloud-offload.replication-shadow-history.v1"
    assert body["provider_mutation"] is False
    assert len(body["evaluations"]) == 1
    assert "job_id" not in history.text
