import asyncio
import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.config import CloudConfig
from cloud_offload.dispatcher import Dispatcher
from cloud_offload.prepared_state import (
    ManifestSigner,
    blob_key,
    build_manifest,
    bundle_key,
    fingerprint,
)
from cloud_offload.providers.base import ProviderStorage
from cloud_offload.regional_replication import build_shadow_report, shadow_accuracy


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
        "shadow_required_recommendations": 10,
        "shadow_validation_hours": 24,
        "shadow_min_precision": 0.8,
        "controller_interval_seconds": 300,
        "copy_timeout_seconds": 21600,
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


def action_recommendation(source, target, manifest, **updates):
    return {
        "recommendation_id": "sha256:" + "9" * 64,
        "source_volume_id": source.id,
        "target_volume_id": target.id,
        "source_manifest_id": manifest["manifest_id"],
        "bytes": sum(item["size"] for item in manifest["artifacts"]),
        "incremental_monthly_storage_cost_usd": 0,
        "estimated_copy_cost_usd": 0,
        "reason_codes": ["measured_value"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(days=14))
        .isoformat()
        .replace("+00:00", "Z"),
        **updates,
    }


def test_replica_action_is_single_flight_budgeted_and_expirable(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    source = add_volume(registry, "source-provider-volume", "A")
    target = add_volume(registry, "target-provider-volume", "B")
    manifest = add_manifest(registry, source, fingerprint({"profile": "action"}))
    recommendation = action_recommendation(source, target, manifest)

    claimed = registry.claim_replica_action(
        recommendation, monthly_budget_usd=20, max_inflight=1
    )
    duplicate = registry.claim_replica_action(
        recommendation, monthly_budget_usd=20, max_inflight=1
    )

    assert claimed["status"] == "copying"
    assert claimed["duplicate_suppressed"] is False
    assert duplicate["id"] == claimed["id"]
    assert duplicate["duplicate_suppressed"] is True

    with pytest.raises(RuntimeError, match="concurrency"):
        registry.claim_replica_action(
            action_recommendation(
                source,
                target,
                manifest,
                recommendation_id="sha256:" + "8" * 64,
            ),
            monthly_budget_usd=20,
            max_inflight=1,
        )

    completed = registry.complete_replica_action(
        claimed["id"], target_manifest_id="sha256:" + "7" * 64
    )
    assert completed["status"] == "completed"
    assert registry.claim_replica_action(
        recommendation, monthly_budget_usd=20, max_inflight=1
    )["duplicate_suppressed"] is True
    assert registry.expire_replica_action(claimed["id"])["status"] == "expired"


def test_replica_action_rejects_unknown_cost_budget_and_expiry(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    source = add_volume(registry, "source-provider-volume", "A")
    target = add_volume(registry, "target-provider-volume", "B")
    manifest = add_manifest(registry, source, fingerprint({"profile": "bounds"}))

    with pytest.raises(ValueError, match="known copy cost"):
        registry.claim_replica_action(
            action_recommendation(
                source, target, manifest, estimated_copy_cost_usd=None
            ),
            monthly_budget_usd=20,
            max_inflight=1,
        )
    with pytest.raises(RuntimeError, match="budget"):
        registry.claim_replica_action(
            action_recommendation(
                source, target, manifest, incremental_monthly_storage_cost_usd=5
            ),
            monthly_budget_usd=4,
            max_inflight=1,
        )
    with pytest.raises(ValueError, match="expired"):
        registry.claim_replica_action(
            action_recommendation(
                source,
                target,
                manifest,
                expires_at="2020-01-01T00:00:00Z",
            ),
            monthly_budget_usd=20,
            max_inflight=1,
        )


def test_shadow_accuracy_requires_mature_repeated_demand(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    created = datetime(2026, 7, 1, tzinfo=timezone.utc)
    recommendations = []
    for index in range(3):
        recommendations.append(
            {
                "recommendation_id": "sha256:" + str(index + 1) * 64,
                "profile_fingerprint": "sha256:" + str(index + 4) * 64,
                "provider": "runpod",
                "target_region": "B",
                "expires_at": "2026-07-15T00:00:00Z",
            }
        )
    registry.record_shadow_evaluation(
        {
            "schema": "cloud-offload.replication-shadow.v1",
            "evaluation_id": "evaluation",
            "created_at": created.isoformat().replace("+00:00", "Z"),
            "recommendations": recommendations,
        }
    )
    for index, recommendation in enumerate(recommendations):
        registry.record_regional_demand(
            job_id=f"followup-{index}",
            profile_fingerprint=recommendation["profile_fingerprint"],
            provider="runpod",
            datacenter_id="B",
            prepared_volume_id=None,
            required_bytes=100,
            cached_bytes=0,
            missing_bytes=100,
            preparation_seconds=30,
            hourly_rate=1,
        )
    with registry._connect() as connection:
        connection.execute(
            "UPDATE regional_cache_demand SET created_at='2026-07-02T00:00:00Z'"
        )
    policy = replication_policy(
        replication={
            "shadow_required_recommendations": 3,
            "shadow_validation_hours": 24,
            "shadow_min_precision": 0.8,
        }
    )

    accuracy = shadow_accuracy(
        registry,
        policy,
        now=datetime(2026, 7, 3, tzinfo=timezone.utc),
    )

    assert accuracy["mature_recommendation_count"] == 3
    assert accuracy["validated_recommendation_count"] == 3
    assert accuracy["precision"] == 1
    assert accuracy["automation_gate_passed"] is True


def test_shadow_accuracy_accepts_real_followup_before_negative_window(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")
    profile = "sha256:" + "3" * 64
    registry.record_shadow_evaluation(
        {
            "schema": "cloud-offload.replication-shadow.v1",
            "evaluation_id": "immediate-followup",
            "created_at": "2026-07-30T00:00:00Z",
            "recommendations": [
                {
                    "recommendation_id": "sha256:" + "2" * 64,
                    "profile_fingerprint": profile,
                    "provider": "runpod",
                    "target_region": "B",
                    "expires_at": "2026-08-30T00:00:00Z",
                }
            ],
        }
    )
    registry.record_regional_demand(
        job_id="real-followup",
        profile_fingerprint=profile,
        provider="runpod",
        datacenter_id="B",
        prepared_volume_id=None,
        required_bytes=100,
        cached_bytes=0,
        missing_bytes=100,
        preparation_seconds=30,
        hourly_rate=1,
    )
    with registry._connect() as connection:
        connection.execute(
            "UPDATE regional_cache_demand SET created_at='2026-07-30T00:05:00Z'"
        )
    policy = replication_policy(
        replication={
            "shadow_required_recommendations": 3,
            "shadow_validation_hours": 24,
        }
    )

    accuracy = shadow_accuracy(
        registry,
        policy,
        now=datetime(2026, 7, 30, 0, 10, tzinfo=timezone.utc),
    )

    assert accuracy["mature_recommendation_count"] == 1
    assert accuracy["validated_recommendation_count"] == 1
    assert accuracy["precision"] == 1
    assert accuracy["automation_gate_passed"] is False


def test_execute_route_copies_once_and_suppresses_duplicate(tmp_path, monkeypatch):
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=replication_policy(),
    )
    registry = CacheRegistry(config.queue_db_path)
    source = add_volume(registry, "source-provider-volume", "A")
    add_volume(registry, "target-provider-volume", "B")
    profile = fingerprint({"profile": "route"})
    add_manifest(registry, source, profile)
    record_demand(registry, profile)
    report = build_shadow_report(registry, config.prepared_storage)
    recommendation = report["recommendations"][0]
    calls = []

    async def copy(config, registry, source, target, manifest_id):
        calls.append(manifest_id)
        return {
            "source_manifest_id": manifest_id,
            "target_manifest_id": "sha256:" + "6" * 64,
            "target_generation": "generation",
            "bytes": recommendation["bytes"],
            "artifact_count": 1,
        }

    monkeypatch.setattr(server, "_config", lambda **kwargs: config)
    monkeypatch.setattr(server, "_copy_cache_manifest", copy)
    client = TestClient(server.app)

    unconfirmed = client.post(
        "/api/cache/replication/execute",
        json={"recommendation_id": recommendation["recommendation_id"]},
    )
    first = client.post(
        "/api/cache/replication/execute",
        json={
            "recommendation_id": recommendation["recommendation_id"],
            "confirmed": True,
        },
    )
    duplicate = client.post(
        "/api/cache/replication/execute",
        json={
            "recommendation_id": recommendation["recommendation_id"],
            "confirmed": True,
        },
    )

    assert unconfirmed.status_code == 409
    assert first.status_code == 200
    assert first.json()["status"] == "completed"
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate_suppressed"] is True
    assert calls == [recommendation["source_manifest_id"]]


def test_manifest_copy_skips_complete_target_objects_and_releases_local_files(
    tmp_path, monkeypatch
):
    signer = ManifestSigner(b"r" * 32)
    first = artifact(b"already copied")
    second = artifact(b"copy this artifact")
    document = build_manifest(
        profile_fingerprint=fingerprint({"profile": "resume"}),
        producer={
            "image_digest": "sha256:" + "1" * 64,
            "cloud_offload_version": "test",
            "python_abi": "test",
            "platform": "portable",
            "torch": "",
            "cuda": "",
        },
        artifacts=[first, second],
        signer=signer,
    )
    source = SimpleNamespace(id="source", provider="runpod")
    target = SimpleNamespace(id="target", provider="runpod")

    class SourceStore:
        def __init__(self):
            self.downloads = []

        def load_index(self):
            return {
                "manifests": [
                    {
                        "manifest_id": document["manifest_id"],
                        "storage_key": "source-manifest.json",
                    }
                ]
            }

        def read_json(self, key):
            assert key == "source-manifest.json"
            return document

        def download_verified(self, key, digest, path):
            self.downloads.append(key)
            path.write_bytes(b"copy this artifact")

    class TargetStore:
        def __init__(self):
            self.exists_calls = []
            self.uploads = []
            self.upload_path = None
            self.published = None

        def exists(self, key, size):
            self.exists_calls.append((key, size))
            return key == first["storage_key"]

        def upload_verified(self, path, digest, *, storage_key):
            self.upload_path = path
            assert path.read_bytes() == b"copy this artifact"
            self.uploads.append(storage_key)

        def publish_manifest(self, manifest, manifest_signer):
            manifest_signer.verify(manifest)
            self.published = manifest

        def load_index(self):
            return {"generation": "target-generation", "manifests": []}

    source_store = SourceStore()
    target_store = TargetStore()
    registry = SimpleNamespace(reconcile_index=lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_cache_connector", lambda *args: object())
    monkeypatch.setattr(
        server,
        "_runpod_s3_store",
        lambda volume, connector: (
            source_store if volume.id == source.id else target_store
        ),
    )
    monkeypatch.setattr(server, "_prepared_manifest_signer", lambda config: signer)

    result = asyncio.run(
        server._copy_cache_manifest(
            SimpleNamespace(), registry, source, target, document["manifest_id"]
        )
    )

    assert source_store.downloads == [second["storage_key"]]
    assert target_store.uploads == [second["storage_key"]]
    assert target_store.upload_path is not None
    assert not target_store.upload_path.exists()
    assert target_store.published is not None
    assert result["artifact_count"] == 2


def test_manifest_copy_uses_configured_scratch_directory(tmp_path, monkeypatch):
    signer = ManifestSigner(b"s" * 32)
    copied = artifact(b"copy through configured scratch")
    document = build_manifest(
        profile_fingerprint=fingerprint({"profile": "scratch"}),
        producer={
            "image_digest": "sha256:" + "2" * 64,
            "cloud_offload_version": "test",
            "python_abi": "test",
            "platform": "portable",
            "torch": "",
            "cuda": "",
        },
        artifacts=[copied],
        signer=signer,
    )
    source = SimpleNamespace(id="source", provider="runpod")
    target = SimpleNamespace(id="target", provider="runpod")
    scratch = tmp_path / "scratch"

    class SourceStore:
        def load_index(self):
            return {
                "manifests": [
                    {
                        "manifest_id": document["manifest_id"],
                        "storage_key": "source-manifest.json",
                    }
                ]
            }

        def read_json(self, key):
            assert key == "source-manifest.json"
            return document

        def download_verified(self, key, digest, path):
            path.write_bytes(b"copy through configured scratch")

    class TargetStore:
        def __init__(self):
            self.upload_path = None

        def exists(self, key, size):
            return False

        def upload_verified(self, path, digest, *, storage_key):
            self.upload_path = path

        def publish_manifest(self, manifest, manifest_signer):
            manifest_signer.verify(manifest)

        def load_index(self):
            return {"generation": "target-generation", "manifests": []}

    source_store = SourceStore()
    target_store = TargetStore()
    store_calls = []
    registry = SimpleNamespace(reconcile_index=lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_cache_connector", lambda *args: object())

    def store_for(volume, connector, **kwargs):
        store_calls.append((volume.id, kwargs))
        return source_store if volume.id == source.id else target_store

    monkeypatch.setattr(
        server,
        "_runpod_s3_store",
        store_for,
    )
    monkeypatch.setattr(server, "_prepared_manifest_signer", lambda config: signer)

    asyncio.run(
        server._copy_cache_manifest(
            SimpleNamespace(scratch_dir=str(scratch)),
            registry,
            source,
            target,
            document["manifest_id"],
        )
    )

    assert target_store.upload_path is not None
    assert target_store.upload_path.is_relative_to(scratch)
    assert not target_store.upload_path.exists()
    assert store_calls == [
        ("source", {"scratch_dir": scratch}),
        ("target", {"scratch_dir": scratch}),
    ]


def test_runpod_s3_store_forwards_scratch_directory(tmp_path, monkeypatch):
    from cloud_offload import prepared_state

    scratch = tmp_path / "scratch"
    volume = SimpleNamespace(
        provider_volume_id="provider-volume",
        datacenter_id="EU-RO-1",
    )
    connector = SimpleNamespace(
        s3_endpoint=lambda region: "https://s3api-eu-ro-1.runpod.io/"
    )
    observed = {}
    expected = object()

    def from_environment(**kwargs):
        observed.update(kwargs)
        return expected

    monkeypatch.setattr(
        prepared_state.RunPodS3PreparedStore,
        "from_environment",
        from_environment,
    )

    result = server._runpod_s3_store(
        volume,
        connector,
        scratch_dir=scratch,
    )

    assert result is expected
    assert observed["scratch_dir"] == scratch


def test_manifest_refresh_reuses_only_exact_models_for_current_profile(
    tmp_path, monkeypatch
):
    signer = ManifestSigner(b"r" * 32)
    payload = b"current model bytes"
    model = artifact(payload)
    model["destination"] = {"category": "checkpoints", "filename": "model.bin"}
    old_runtime = {
        **artifact(b"old runtime"),
        "kind": "environment-bundle",
        "materialization": "extract",
        "portability": "runtime-bound",
        "requirements": {
            "image_digest": "sha256:" + "0" * 64,
            "dependency_lock": "sha256:" + "0" * 64,
            "platform": "linux-x86_64",
            "python_abi": "cp311",
        },
        "source": {"dependency_lock": "sha256:" + "0" * 64},
        "destination": {"dependency_lock": "sha256:" + "0" * 64},
    }
    old_runtime["storage_key"] = bundle_key(old_runtime["digest"])
    config = CloudConfig(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=replication_policy(),
        worker_profiles={
            "comfyui": {
                "image": "runner@sha256:" + "a" * 64,
                "models": ["comfyui-workflow"],
                "providers": ["runpod"],
                "custom_nodes": [
                    {
                        "id": "current-pack",
                        "git": "https://example.invalid/current-pack.git",
                        "commit": "b" * 40,
                    }
                ],
            }
        },
    )
    registry = CacheRegistry(config.queue_db_path)
    volume = add_volume(registry, "source-provider-volume", "A")
    source_manifest = build_manifest(
        profile_fingerprint=fingerprint({"profile": "old"}),
        producer={
            "image_digest": "sha256:" + "0" * 64,
            "cloud_offload_version": "old",
        },
        artifacts=[model, old_runtime],
        signer=signer,
        claims={"cache_volume_id": volume.id},
    )
    registry.reconcile_index(
        volume.id,
        {
            "schema": "cloud-offload.prepared-state.index.v1",
            "generation": "old-generation",
            "manifests": [source_manifest],
        },
        manifest_documents={source_manifest["manifest_id"]: source_manifest},
    )

    class RefreshStore:
        published = None

        def publish_manifest(self, manifest, manifest_signer):
            manifest_signer.verify(manifest)
            self.published = manifest

        def load_index(self):
            assert self.published is not None
            return {
                "schema": "cloud-offload.prepared-state.index.v1",
                "generation": "refreshed-generation",
                "manifests": [source_manifest, self.published],
            }

    store = RefreshStore()
    monkeypatch.setattr(server, "_config", lambda **kwargs: config)
    monkeypatch.setattr(server, "_cache_registry", lambda *args: registry)
    monkeypatch.setattr(server, "_cache_connector", lambda *args: object())
    monkeypatch.setattr(server, "_runpod_s3_store", lambda *args: store)
    monkeypatch.setattr(server, "_prepared_manifest_signer", lambda *args: signer)
    request = {
        "confirmed": True,
        "volume_id": volume.id,
        "source_manifest_id": source_manifest["manifest_id"],
        "profile": "comfyui",
        "requirement_artifacts": [
            {
                "sha256": model["digest"].removeprefix("sha256:"),
                "size": len(payload),
                "category": "checkpoints",
                "filename": "model.bin",
                "format": "other",
            }
        ],
    }
    client = TestClient(server.app)

    unconfirmed = client.post(
        "/api/cache/manifests/refresh", json={**request, "confirmed": False}
    )
    response = client.post("/api/cache/manifests/refresh", json=request)

    assert unconfirmed.status_code == 409
    assert response.status_code == 200
    body = response.json()
    assert body["manifest_id"] != source_manifest["manifest_id"]
    assert body["profile_fingerprint"] != source_manifest["profile_fingerprint"]
    assert body["artifact_count"] == 1
    assert body["cached_bytes"] == len(payload)
    assert body["complete"] is False
    assert body["runtime_requirements_pending"] is True
    assert body["provider_gpu_mutation"] is False
    assert [item["kind"] for item in store.published["artifacts"]] == [
        "model-weight"
    ]


def test_expiry_route_unpublishes_target_and_keeps_source(tmp_path, monkeypatch):
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=replication_policy(),
    )
    registry = CacheRegistry(config.queue_db_path)
    source = add_volume(registry, "source-provider-volume", "A")
    target = add_volume(registry, "target-provider-volume", "B")
    profile = fingerprint({"profile": "expiry"})
    source_manifest = add_manifest(registry, source, profile, b"source")
    target_manifest = add_manifest(registry, target, profile, b"replica")
    target_claim = registry.claim_replica_target(
        provider="runpod",
        datacenter_id="B",
        size_gb=100,
        monthly_cost_usd=7,
        monthly_budget_usd=20,
    )
    registry.complete_replica_target(
        target_claim["id"],
        provider_volume_id=target.provider_volume_id,
        cache_volume_id=target.id,
    )
    action = registry.claim_replica_action(
        action_recommendation(source, target, source_manifest),
        monthly_budget_usd=20,
        max_inflight=1,
    )
    registry.complete_replica_action(
        action["id"], target_manifest_id=target_manifest["manifest_id"]
    )
    with registry._connect() as connection:
        connection.execute(
            "UPDATE regional_replica_actions SET expires_at='2020-01-01T00:00:00Z'"
        )

    class ExpiryStore:
        def __init__(self):
            self.removed = []

        def remove_manifest(self, manifest_id, signer, *, manifest):
            self.removed.append(manifest_id)
            return {"manifests": 1, "objects": 1}

        def load_index(self):
            return {
                "schema": "cloud-offload.prepared-state.index.v1",
                "generation": "after-expiry",
                "manifests": [],
            }

    store = ExpiryStore()

    class Connector:
        def __init__(self):
            self.deleted = []

        def delete_storage(self, storage_id):
            self.deleted.append(storage_id)
            return True

    connector = Connector()
    monkeypatch.setattr(server, "_config", lambda **kwargs: config)
    monkeypatch.setattr(server, "_cache_connector", lambda *args: connector)
    monkeypatch.setattr(server, "_runpod_s3_store", lambda *args: store)
    monkeypatch.setattr(
        server, "_prepared_manifest_signer", lambda config: ManifestSigner(b"x" * 32)
    )
    client = TestClient(server.app)

    response = client.post("/api/cache/replication/expire")

    assert response.status_code == 200
    body = response.json()
    assert body["failures"] == []
    assert body["source_state_deleted"] is False
    assert body["provider_gpu_mutation"] is False
    assert len(body["expired"]) == 1
    assert body["expired"][0]["status"] == "expired"
    assert len(body["deleted_targets"]) == 1
    assert body["deleted_targets"][0]["status"] == "deleted"
    assert connector.deleted == [target.provider_volume_id]
    assert store.removed == [target_manifest["manifest_id"]]
    assert registry.get_manifest(source.id, source_manifest["manifest_id"])
    assert registry.get_manifest(target.id, target_manifest["manifest_id"]) is None
    assert registry.get_volume(target.id).status == "failed"


def test_manual_replication_failure_keeps_private_endpoint_out_of_api(
    tmp_path, monkeypatch
):
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=replication_policy(),
    )
    registry = CacheRegistry(config.queue_db_path)
    source = add_volume(registry, "source-provider-volume", "A")
    target = add_volume(registry, "target-provider-volume", "B")
    manifest = add_manifest(registry, source, fingerprint({"profile": "manual"}))

    async def fail_copy(*args, **kwargs):
        raise RuntimeError("Read timeout on https://private-storage.example.invalid")

    monkeypatch.setattr(server, "_config", lambda **kwargs: config)
    monkeypatch.setattr(server, "_cache_registry", lambda *args: registry)
    monkeypatch.setattr(server, "_copy_cache_manifest", fail_copy)

    response = TestClient(server.app).post(
        "/api/cache/replicate",
        json={
            "confirmed": True,
            "source_volume_id": source.id,
            "target_volume_id": target.id,
            "manifest_id": manifest["manifest_id"],
        },
    )

    assert response.status_code == 409
    payload = json.dumps(response.json(), sort_keys=True)
    assert "Replication failed: RuntimeError" in payload
    assert "private-storage" not in payload
    with sqlite3.connect(config.queue_db_path) as connection:
        stored_status = connection.execute(
            "SELECT status FROM cache_replications ORDER BY created_at DESC LIMIT 1"
        ).fetchone()[0]
    assert stored_status == "failed:RuntimeError:unknown:unknown"
    assert "private-storage" not in stored_status


def test_replication_failure_reason_keeps_only_safe_provider_routing_data():
    error = ClientError(
        {
            "Error": {
                "Code": "502",
                "Message": "failed at https://private-storage.example.invalid/secret-key",
            }
        },
        "UploadPart",
    )

    reason = server._safe_replication_failure_reason(error)

    assert reason == "ClientError:502:UploadPart"
    assert "private-storage" not in reason
    assert "secret-key" not in reason


def test_automatic_route_stays_locked_before_shadow_accuracy(tmp_path, monkeypatch):
    policy = replication_policy(replication={"mode": "automatic"})
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=policy,
    )
    registry = CacheRegistry(config.queue_db_path)
    source = add_volume(registry, "source-provider-volume", "A")
    add_volume(registry, "target-provider-volume", "B")
    profile = fingerprint({"profile": "automatic-gate"})
    add_manifest(registry, source, profile)
    record_demand(registry, profile)
    recommendation = build_shadow_report(registry, policy)["recommendations"][0]
    monkeypatch.setattr(server, "_config", lambda **kwargs: config)
    client = TestClient(server.app)

    response = client.post(
        "/api/cache/replication/execute",
        json={"recommendation_id": recommendation["recommendation_id"]},
    )

    assert response.status_code == 409
    detail = response.json()["error"]["details"]["detail"]
    assert detail["code"] == "cloud_offload.replication_shadow_gate"
    assert detail["accuracy"]["automation_gate_passed"] is False
    assert registry.list_replica_actions() == []


def test_automatic_route_creates_approved_target_then_copies(
    tmp_path, monkeypatch
):
    policy = replication_policy(replication={"mode": "automatic"})
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=policy,
    )
    registry = CacheRegistry(config.queue_db_path)
    source = add_volume(registry, "source-provider-volume", "A")
    profile = fingerprint({"profile": "automatic-create"})
    source_manifest = add_manifest(registry, source, profile)
    record_demand(registry, profile)
    recommendation = build_shadow_report(registry, policy)["recommendations"][0]

    class Connector:
        def __init__(self):
            self.created = []
            self.deleted = []

        def create_storage(self, *, name, size_gb, datacenter_id):
            self.created.append((name, size_gb, datacenter_id))
            return ProviderStorage(
                id="automatic-provider-volume",
                provider="runpod",
                name=name,
                size_gb=size_gb,
                datacenter_id=datacenter_id,
                s3_compatible=True,
            )

        def delete_storage(self, storage_id):
            self.deleted.append(storage_id)
            return True

    connector = Connector()
    copies = []

    async def copy(config, registry, source, target, manifest_id):
        copies.append((source.id, target.id, manifest_id))
        return {
            "source_manifest_id": manifest_id,
            "target_manifest_id": "sha256:" + "5" * 64,
            "target_generation": "automatic-generation",
            "bytes": recommendation["bytes"],
            "artifact_count": 1,
        }

    monkeypatch.setattr(server, "_config", lambda **kwargs: config)
    monkeypatch.setattr(server, "_cache_connector", lambda *args: connector)
    monkeypatch.setattr(server, "_copy_cache_manifest", copy)
    import cloud_offload.regional_replication as replication_module

    monkeypatch.setattr(
        replication_module,
        "shadow_accuracy",
        lambda *args, **kwargs: {"automation_gate_passed": True},
    )
    client = TestClient(server.app)

    response = client.post(
        "/api/cache/replication/execute",
        json={"recommendation_id": recommendation["recommendation_id"]},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert connector.created == [("cloud-offload-replica-b", 100, "B")]
    assert connector.deleted == []
    targets = registry.list_replica_targets(status="ready")
    assert len(targets) == 1
    target = registry.get_volume(targets[0]["cache_volume_id"])
    assert target.provider_volume_id == "automatic-provider-volume"
    assert copies == [(source.id, target.id, source_manifest["manifest_id"])]
    assert registry.list_replica_actions()[0]["target_volume_id"] == target.id


def test_replica_target_claim_is_single_flight_and_budgeted(tmp_path):
    registry = CacheRegistry(tmp_path / "queue.db")

    first = registry.claim_replica_target(
        provider="runpod",
        datacenter_id="B",
        size_gb=100,
        monthly_cost_usd=7,
        monthly_budget_usd=10,
    )
    duplicate = registry.claim_replica_target(
        provider="runpod",
        datacenter_id="B",
        size_gb=100,
        monthly_cost_usd=7,
        monthly_budget_usd=10,
    )

    assert first["duplicate_suppressed"] is False
    assert duplicate["id"] == first["id"]
    assert duplicate["duplicate_suppressed"] is True
    with pytest.raises(RuntimeError, match="budget"):
        registry.claim_replica_target(
            provider="runpod",
            datacenter_id="C",
            size_gb=100,
            monthly_cost_usd=7,
            monthly_budget_usd=10,
        )


def test_failed_target_creation_keeps_exact_provider_cleanup_ownership(
    tmp_path, monkeypatch
):
    policy = replication_policy(replication={"mode": "automatic"})
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=policy,
    )
    registry = CacheRegistry(config.queue_db_path)
    source = add_volume(registry, "source-provider-volume", "A")
    profile = fingerprint({"profile": "target-cleanup"})
    add_manifest(registry, source, profile)
    record_demand(registry, profile)
    recommendation = build_shadow_report(registry, policy)["recommendations"][0]

    class Connector:
        def __init__(self):
            self.cleanup_ready = False

        def create_storage(self, *, name, size_gb, datacenter_id):
            return ProviderStorage(
                id="cleanup-provider-volume",
                provider="runpod",
                name=name,
                size_gb=size_gb,
                datacenter_id=datacenter_id,
                s3_compatible=False,
            )

        def get_storage(self, storage_id):
            return ProviderStorage(
                id=storage_id,
                provider="runpod",
                name="cleanup",
                size_gb=100,
                datacenter_id="B",
            )

        def delete_storage(self, storage_id):
            return self.cleanup_ready

    connector = Connector()
    monkeypatch.setattr(server, "_cache_connector", lambda *args: connector)

    with pytest.raises(RuntimeError, match="does not support"):
        asyncio.run(
            server._ensure_automatic_replica_target(
                config, registry, recommendation
            )
        )

    pending = registry.list_replica_targets()[0]
    assert pending["status"] == "deleting"
    assert pending["provider_volume_id"] == "cleanup-provider-volume"

    connector.cleanup_ready = True
    recovery = asyncio.run(
        server._reconcile_automatic_replica_targets(config, registry)
    )

    assert recovery["cleaned"] == [pending["id"]]
    assert registry.list_replica_targets()[0]["status"] == "deleted"


def test_controller_marks_lost_target_out_of_placement(tmp_path, monkeypatch):
    policy = replication_policy(replication={"mode": "automatic"})
    config = SimpleNamespace(
        queue_db_path=tmp_path / "queue.db",
        prepared_storage=policy,
    )
    registry = CacheRegistry(config.queue_db_path)
    source_volume = add_volume(registry, "source-provider-volume", "A")
    target_volume = add_volume(registry, "lost-provider-volume", "B")
    manifest = add_manifest(
        registry, source_volume, fingerprint({"profile": "loss-recovery"})
    )
    claim = registry.claim_replica_target(
        provider="runpod",
        datacenter_id="B",
        size_gb=100,
        monthly_cost_usd=7,
        monthly_budget_usd=20,
    )
    registry.complete_replica_target(
        claim["id"],
        provider_volume_id=target_volume.provider_volume_id,
        cache_volume_id=target_volume.id,
    )
    recommendation = action_recommendation(source_volume, target_volume, manifest)
    action = registry.claim_replica_action(
        recommendation, monthly_budget_usd=20, max_inflight=1
    )
    registry.complete_replica_action(
        action["id"], target_manifest_id="sha256:" + "4" * 64
    )

    class MissingConnector:
        def get_storage(self, storage_id):
            return None

    monkeypatch.setattr(server, "_config", lambda **kwargs: config)
    monkeypatch.setattr(server, "_cache_connector", lambda *args: MissingConnector())
    client = TestClient(server.app)

    response = client.post("/api/cache/replication/controller/tick")

    assert response.status_code == 200
    body = response.json()
    assert body["target_recovery"]["lost"] == [claim["id"]]
    assert registry.list_replica_targets()[0]["status"] == "lost"
    assert registry.get_volume(target_volume.id).status == "failed"
    assert registry.list_replica_actions()[0]["status"] == "lost"
    assert body["provider_gpu_mutation"] is False

    replacement = add_volume(registry, "replacement-provider-volume", "B")
    retried = registry.claim_replica_action(
        {
            **recommendation,
            "target_volume_id": replacement.id,
        },
        monthly_budget_usd=20,
        max_inflight=1,
    )
    assert retried["status"] == "copying"
    assert retried["duplicate_suppressed"] is False


def test_dispatcher_starts_one_nonblocking_controller_thread(tmp_path, monkeypatch):
    policy = replication_policy(replication={"mode": "automatic"})
    config = CloudConfig(
        queue_db_path=tmp_path / "queue.db",
        worker_token="w" * 32,
        prepared_storage=policy,
    )
    config._source_path = tmp_path / "config.json"
    dispatcher = Dispatcher(config, connector=object())
    calls = []
    monkeypatch.setattr(
        dispatcher,
        "_run_replication_controller_tick",
        lambda: calls.append("tick"),
    )

    dispatcher._tick_regional_replication()
    dispatcher._replication_thread.join(timeout=2)
    dispatcher._tick_regional_replication()

    assert calls == ["tick"]
