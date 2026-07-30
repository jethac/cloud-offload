from datetime import datetime, timezone
import json

from fastapi.testclient import TestClient

from cloud_offload import preflight, server
from cloud_offload.cache_registry import CacheRegistry, CacheVolume
from cloud_offload.config import CloudConfig
from cloud_offload.preflight import build_partition_preflight, finite_report
from cloud_offload.preflight_store import PreflightStore
from cloud_offload.providers.base import ProviderStorage
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.storage import LocalStorage


class ReadOnlyConnector:
    def __init__(self):
        self.offer_reads = 0
        self.mutations = []
        self.storage = {}
        self.rate_multiplier = 1.0

    def list_available(
        self,
        gpu_type=None,
        min_gpu_ram=None,
        max_hourly_rate=None,
        placement=None,
    ):
        self.offer_reads += 1
        offers = [
            {
                "id": "gpu-l40",
                "provider": "runpod",
                "gpu_type": "L40",
                "gpu_count": 1,
                "gpu_ram_gb": 48,
                "hourly_rate": 0.75 * self.rate_multiplier,
                "raw": {"private_provider_payload": "must-not-leak"},
            },
            {
                "id": "gpu-a100",
                "provider": "runpod",
                "gpu_type": "A100 80 GB",
                "gpu_count": 1,
                "gpu_ram_gb": 80,
                "hourly_rate": 1.49 * self.rate_multiplier,
                "raw": {"private_provider_payload": "must-not-leak"},
            },
        ]
        return [
            offer
            for offer in offers
            if (min_gpu_ram is None or offer["gpu_ram_gb"] >= min_gpu_ram)
            and (max_hourly_rate is None or offer["hourly_rate"] <= max_hourly_rate)
        ]

    def get_storage(self, storage_id):
        return self.storage.get(storage_id)

    def launch(self, *args, **kwargs):
        self.mutations.append("launch")
        raise AssertionError("preflight must not launch")

    def create_storage(self, *args, **kwargs):
        self.mutations.append("create_storage")
        raise AssertionError("preflight must not create storage")

    def delete_storage(self, *args, **kwargs):
        self.mutations.append("delete_storage")
        raise AssertionError("preflight must not delete storage")


def config_for_preflight(tmp_path):
    return CloudConfig(
        enabled=True,
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="configured",
        max_hourly_rate=2.0,
        worker_token="worker-token",
        coordinator_url="https://coordinator.invalid",
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
                "gpu_type": "any",
                "min_gpu_ram_gb": 40,
                "image_size_gb": 16,
            }
        },
    )


def partition():
    return {
        "schema": "comfy.partition.job.v1",
        "partition_id": "part-1",
        "workflow": {"1": {"class_type": "CloudPartitionInput", "inputs": {}}},
        "runner": {"profile": "comfyui", "min_gpu_ram_gb": 40},
    }


def test_preflight_recommends_a_safe_offer_without_provider_mutation(tmp_path):
    config = config_for_preflight(tmp_path)
    connector = ReadOnlyConnector()

    report = build_partition_preflight(
        config=config,
        partition=partition(),
        input_artifacts={},
        provider="runpod",
        recommendation_policy="cheapest",
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda provider, config: connector,
        now=lambda: datetime(2026, 7, 29, tzinfo=timezone.utc),
    )

    assert report["schema"] == "cloud-offload.preflight.v1"
    assert report["status"] == "ready"
    assert report["blockers"] == []
    assert (
        report["recommendation"]["candidate_id"]
        == report["candidates"][0]["candidate_id"]
    )
    assert report["candidates"][0]["offer_id"] == "gpu-l40"
    assert report["execution_plan"]["offer_id"] == "gpu-l40"
    assert report["expires_at"] == "2026-07-29T00:01:00Z"
    assert report["confirmation"] == {
        "policy": "always",
        "required": True,
        "mandatory": False,
        "reason": "policy_always",
        "countdown_seconds": 10,
        "not_before": "2026-07-29T00:00:10Z",
    }
    assert connector.offer_reads == 1
    assert connector.mutations == []
    assert "private_provider_payload" not in str(report)
    assert finite_report(report)


def test_runpod_total_cost_includes_idle_compute_disk_and_zero_transfer(tmp_path):
    config = config_for_preflight(tmp_path)
    legacy_runner_value = partition()
    legacy_runner_value["runner"]["keep_warm"] = True

    report = build_partition_preflight(
        config=config,
        partition=legacy_runner_value,
        input_artifacts={},
        provider="runpod",
        recommendation_policy="cheapest",
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda provider, config: ReadOnlyConnector(),
    )

    estimate = report["estimate"]
    assert estimate["paid_idle_seconds"] == 300
    assert estimate["incremental_transfer_cost_usd"] == [0.0, 0.0]
    assert estimate["incremental_storage_cost_usd"][0] > 0
    assert estimate["cost_complete"] is True
    for index in (0, 1):
        expected = sum(
            component[index]
            for component in (
                estimate["compute_cost_usd"],
                estimate["incremental_transfer_cost_usd"],
                estimate["incremental_storage_cost_usd"],
            )
        )
        assert abs(estimate["total_job_cost_usd"][index] - expected) < 0.000002
    assert "incremental_costs_unmeasured" not in {
        item["code"] for item in report["unknowns"]
    }


def test_matched_history_changes_fastest_recommendation(tmp_path):
    config = config_for_preflight(tmp_path)

    def history(_workload, performance_class):
        execution = (
            [30.0, 40.0]
            if performance_class["gpu_type"] == "a10080gb"
            else [400.0, 500.0]
        )
        return {
            "sample_count": 2,
            "startup_seconds": [40.0, 60.0],
            "preparation_seconds": [10.0, 20.0],
            "execution_seconds": execution,
            "confidence": "medium",
        }

    report = build_partition_preflight(
        config=config,
        partition=partition(),
        input_artifacts={},
        provider="runpod",
        recommendation_policy="fastest",
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda provider, config: ReadOnlyConnector(),
        history_lookup=history,
    )

    assert report["execution_plan"]["offer_id"] == "gpu-a100"
    assert report["estimate"]["history_sample_count"] == 2
    assert report["estimate"]["confidence"] == "medium"
    assert "execution_history_unavailable" not in {
        item["code"] for item in report["unknowns"]
    }


def test_unknown_provider_costs_are_not_reported_as_zero():
    estimate = preflight._estimate(
        provider="plugin-provider",
        offer={"hourly_rate": 0.5},
        required_bytes=1024**3,
        cached_bytes=0,
        container_disk_gb=40,
        idle_shutdown_seconds=300,
        keep_warm=False,
        keep_warm_warning_seconds=3600,
    )

    assert estimate["compute_cost_usd"][0] > 0
    assert estimate["incremental_transfer_cost_usd"] is None
    assert estimate["incremental_storage_cost_usd"] is None
    assert estimate["total_job_cost_usd"] is None
    assert estimate["cost_complete"] is False


def test_one_history_sample_does_not_change_gpu_ranking(tmp_path):
    config = config_for_preflight(tmp_path)

    def one_sample(_workload, performance_class):
        return {
            "sample_count": 1,
            "startup_seconds": [1.0, 2.0],
            "preparation_seconds": [1.0, 2.0],
            "execution_seconds": (
                [1.0, 2.0]
                if performance_class["gpu_type"] == "a10080gb"
                else [500.0, 600.0]
            ),
            "confidence": "low",
        }

    report = build_partition_preflight(
        config=config,
        partition=partition(),
        input_artifacts={},
        provider="runpod",
        recommendation_policy="fastest",
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda provider, config: ReadOnlyConnector(),
        history_lookup=one_sample,
    )

    assert report["execution_plan"]["offer_id"] == "gpu-l40"
    assert report["estimate"]["history_sample_count"] == 1
    assert report["estimate"]["history_used"] is False
    assert report["estimate"]["execution_seconds"] == [120.0, 300.0]
    assert "execution_history_unavailable" in {
        item["code"] for item in report["unknowns"]
    }


def test_deterministic_blocker_stops_before_provider_read(tmp_path):
    config = config_for_preflight(tmp_path)
    connector = ReadOnlyConnector()
    invalid = {**partition(), "schema": "wrong", "workflow": {}}

    report = build_partition_preflight(
        config=config,
        partition=invalid,
        input_artifacts={},
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda provider, config: connector,
    )

    assert report["status"] == "blocked"
    assert {item["code"] for item in report["blockers"]} >= {
        "unsupported_partition_schema",
        "partition_workflow_required",
    }
    assert report["candidates"] == []
    assert connector.offer_reads == 0
    assert connector.mutations == []


def test_hard_total_cost_limit_filters_volatile_offers(tmp_path):
    config = config_for_preflight(tmp_path)
    connector = ReadOnlyConnector()

    report = build_partition_preflight(
        config=config,
        partition=partition(),
        input_artifacts={},
        max_total_job_cost=0.001,
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda provider, config: connector,
    )

    assert report["status"] == "uncertain"
    assert report["recommendation"] is None
    assert report["candidates"] == []
    assert any(item["code"] == "no_current_viable_offer" for item in report["unknowns"])
    assert connector.mutations == []


def test_prepared_region_wins_balanced_recommendation(monkeypatch, tmp_path):
    config = config_for_preflight(tmp_path)
    config.asset_sources = {
        "c" * 64: {
            "url": "https://models.invalid/model.safetensors",
        }
    }
    config.prepared_storage = {
        "enabled": True,
        "confirmed": True,
        "provider": "runpod",
        "policy": "smart",
        "region": "US-MD-1",
        "cold_fallback": "allow",
        "managed_size_gb": 250,
        "existing_volume_id": "provider-volume",
        "max_monthly_storage_cost": None,
        "tenant": "default",
        "cache_private_assets": False,
        "shadow_admission": True,
    }
    config.__post_init__()
    volume = CacheVolume(
        id="volume-1",
        provider="runpod",
        provider_volume_id="provider-volume",
        datacenter_id="US-MD-1",
        ownership="adopted",
        status="ready",
        capacity_bytes=250 * 1024**3,
        inventory_generation="generation-1",
        last_verified_at="2026-07-29T00:00:00Z",
        policy=config.prepared_storage,
        s3_compatible=True,
    )

    class Registry:
        def list_volumes(self, status=None):
            return [volume] if status in {None, "ready"} else []

        def volume_coverage(self, required, **kwargs):
            return [
                {
                    "volume": volume,
                    "cached_bytes": 1024**3,
                    "required_bytes": 1024**3,
                    "complete": True,
                    "manifest_ids": ["sha256:" + "d" * 64],
                }
            ]

    connector = ReadOnlyConnector()
    connector.storage["provider-volume"] = ProviderStorage(
        id="provider-volume",
        provider="runpod",
        name="prepared",
        size_gb=250,
        datacenter_id="US-MD-1",
        s3_compatible=True,
    )
    with_asset = {
        **partition(),
        "assets": [
            {
                "category": "checkpoints",
                "filename": "model.safetensors",
                "sha256": "c" * 64,
                "size": 1024**3,
                "format": "safetensors",
            }
        ],
    }
    monkeypatch.setattr(preflight, "_storage_credentials_configured", lambda: True)

    report = build_partition_preflight(
        config=config,
        partition=with_asset,
        input_artifacts={},
        storage=LocalStorage(config.storage_path),
        cache_registry=Registry(),
        connector_factory=lambda provider, config: connector,
    )

    assert report["status"] == "ready"
    assert report["execution_plan"]["prepared_volume_id"] == "volume-1"
    assert report["execution_plan"]["region"] == "US-MD-1"
    assert report["preparation"] == {
        "required_bytes": 1024**3,
        "cached_bytes": 1024**3,
        "missing_bytes": 0,
        "coverage_percent": 100.0,
        "complete": True,
    }
    assert connector.offer_reads == 2
    assert connector.mutations == []


def test_preflight_uses_two_compatible_replicas_and_keeps_cold_fallback(
    tmp_path, monkeypatch
):
    config = config_for_preflight(tmp_path)
    config.prepared_storage = {
        "enabled": True,
        "confirmed": True,
        "provider": "runpod",
        "policy": "smart",
        "region": "auto",
        "cold_fallback": "allow",
        "managed_size_gb": 100,
        "existing_volume_id": "provider-a",
        "max_monthly_storage_cost": 30,
        "tenant": "default",
        "cache_private_assets": False,
        "shadow_admission": True,
    }
    config.__post_init__()
    volumes = [
        CacheVolume(
            id=f"volume-{region.lower()}",
            provider="runpod",
            provider_volume_id=f"provider-{region.lower()}",
            datacenter_id=region,
            ownership="managed",
            status="ready",
            capacity_bytes=100 * 1024**3,
            inventory_generation="generation",
            last_verified_at="2026-07-30T00:00:00Z",
            policy=config.prepared_storage,
            s3_compatible=True,
        )
        for region in ("A", "B")
    ]

    class Registry:
        def list_volumes(self, status=None):
            assert status == "ready"
            return volumes

        def volume_coverage(self, required, **kwargs):
            return [
                {
                    "volume": volume,
                    "cached_bytes": 0,
                    "required_bytes": 0,
                    "complete": True,
                    "manifest_ids": ["sha256:" + region * 64],
                }
                for volume, region in zip(volumes, ("a", "b"), strict=True)
            ]

    connector = ReadOnlyConnector()
    for volume in volumes:
        connector.storage[volume.provider_volume_id] = ProviderStorage(
            id=volume.provider_volume_id,
            provider="runpod",
            name=volume.provider_volume_id,
            size_gb=100,
            datacenter_id=volume.datacenter_id,
            s3_compatible=True,
        )
    monkeypatch.setattr(preflight, "_storage_credentials_configured", lambda: True)

    report = build_partition_preflight(
        config=config,
        partition=partition(),
        input_artifacts={},
        storage=LocalStorage(config.storage_path),
        cache_registry=Registry(),
        connector_factory=lambda provider, config: connector,
    )

    prepared = [
        item for item in report["candidates"] if item["prepared_volume_id"]
    ]
    cold = [
        item for item in report["candidates"] if not item["prepared_volume_id"]
    ]
    assert {item["region"] for item in prepared} == {"A", "B"}
    assert all(item["preparation"]["complete"] for item in prepared)
    assert cold
    assert report["recommendation"]["candidate_id"] in {
        item["candidate_id"] for item in prepared
    }
    assert connector.mutations == []


def test_manual_policy_returns_ranked_choices_without_auto_selection(tmp_path):
    config = config_for_preflight(tmp_path)

    report = build_partition_preflight(
        config=config,
        partition=partition(),
        input_artifacts={},
        recommendation_policy="manual",
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda provider, config: ReadOnlyConnector(),
    )

    assert report["status"] == "ready"
    assert report["recommendation"] is None
    assert [item["rank"] for item in report["candidates"]] == [1, 2]
    assert report["execution_plan"]["offer_id"] is None


def test_preflight_store_persists_only_the_safe_report(tmp_path):
    config = config_for_preflight(tmp_path)
    report = build_partition_preflight(
        config=config,
        partition=partition(),
        input_artifacts={},
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda provider, config: ReadOnlyConnector(),
    )
    store = PreflightStore(config.queue_db_path)

    store.put(report)
    restored = store.get(report["preflight_id"])

    assert restored == report
    assert "CloudPartitionInput" not in str(restored)
    assert store.get("missing") is None


def test_paid_partition_requires_preflight_identity(monkeypatch, tmp_path):
    config = config_for_preflight(tmp_path)
    queue = JobQueue(config.queue_db_path)
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)

    response = TestClient(server.app).post(
        "/api/partitions",
        json={"partition": partition(), "provider": "runpod"},
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "cloud_offload.preflight_required"
    assert queue.count_by_status(*list(JobStatus)) == 0


def test_preflight_identity_is_revalidated_and_bound_to_job(monkeypatch, tmp_path):
    config = config_for_preflight(tmp_path)
    queue = JobQueue(config.queue_db_path)
    connector = ReadOnlyConnector()
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(
        preflight,
        "create_connector",
        lambda provider, config: connector,
    )
    client = TestClient(server.app)

    checked = client.post(
        "/api/preflight",
        json={"partition": partition(), "provider": "runpod"},
    )
    assert checked.status_code == 200
    report = checked.json()
    candidate_id = report["recommendation"]["candidate_id"]

    created = client.post(
        "/api/partitions",
        json={
            "partition": partition(),
            "provider": "runpod",
            "preflight_id": report["preflight_id"],
            "manifest_digest": report["manifest_digest"],
            "candidate_id": candidate_id,
            "confirmation_action": "start_now",
        },
    )

    assert created.status_code == 202
    job = queue.get(created.json()["job_id"])
    assert job.params["preflight"]["candidate_id"] == candidate_id
    assert job.params["preflight"]["offer_id"] == "gpu-l40"
    assert job.params["preflight"]["confirmation"]["action"] == "start_now"
    assert job.provider == "runpod"
    assert connector.offer_reads == 2
    assert connector.mutations == []


def test_preflight_accepts_dispatcher_managed_worker_auth(monkeypatch, tmp_path):
    config = config_for_preflight(tmp_path)
    config.worker_token = ""
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("dispatcher-managed-token")
    connector = ReadOnlyConnector()
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(
        preflight,
        "create_connector",
        lambda provider, config: connector,
    )

    report = (
        TestClient(server.app)
        .post(
            "/api/preflight",
            json={"partition": partition(), "provider": "runpod"},
        )
        .json()
    )

    assert report["status"] == "ready"
    assert "worker_token_missing" not in {item["code"] for item in report["blockers"]}


def test_request_cannot_loosen_configured_cost_or_region_limits(tmp_path):
    config = config_for_preflight(tmp_path)
    config.max_hourly_rate = 0.8
    config.max_total_job_cost = 0.2
    config.allowed_regions = ["US-MD-1"]
    config.__post_init__()
    connector = ReadOnlyConnector()

    conflict = build_partition_preflight(
        config=config,
        partition=partition(),
        input_artifacts={},
        max_hourly_rate=10,
        max_total_job_cost=1,
        allowed_regions=["EU-RO-1"],
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda *args: connector,
    )

    assert conflict["status"] == "blocked"
    assert conflict["request_policy"]["max_hourly_rate"] == 0.8
    assert conflict["request_policy"]["max_total_job_cost"] == 0.2
    assert any(
        item["code"] == "region_policy_conflict" for item in conflict["blockers"]
    )
    assert connector.offer_reads == 0


def test_paid_partition_waits_for_confirmation_without_creating_a_job(
    monkeypatch, tmp_path
):
    config = config_for_preflight(tmp_path)
    queue = JobQueue(config.queue_db_path)
    connector = ReadOnlyConnector()
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(preflight, "create_connector", lambda *args: connector)
    client = TestClient(server.app)
    report = client.post(
        "/api/preflight", json={"partition": partition(), "provider": "runpod"}
    ).json()
    identity = {
        "partition": partition(),
        "provider": "runpod",
        "preflight_id": report["preflight_id"],
        "manifest_digest": report["manifest_digest"],
        "candidate_id": report["recommendation"]["candidate_id"],
    }

    missing = client.post("/api/partitions", json=identity)
    early = client.post(
        "/api/partitions",
        json={**identity, "confirmation_action": "countdown_elapsed"},
    )

    assert missing.status_code == 409
    assert missing.json()["error"]["code"] == "cloud_offload.confirmation_required"
    assert early.status_code == 409
    assert early.json()["error"]["code"] == (
        "cloud_offload.confirmation_countdown_active"
    )
    assert early.json()["error"]["details"]["remaining_seconds"] >= 1
    assert queue.count_by_status(*list(JobStatus)) == 0
    assert connector.mutations == []


def test_elapsed_countdown_and_never_policy_can_start_without_manual_action(
    monkeypatch, tmp_path
):
    config = config_for_preflight(tmp_path)
    queue = JobQueue(config.queue_db_path)
    connector = ReadOnlyConnector()
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(preflight, "create_connector", lambda *args: connector)
    client = TestClient(server.app)
    elapsed = client.post(
        "/api/preflight", json={"partition": partition(), "provider": "runpod"}
    ).json()
    elapsed["confirmation"]["not_before"] = "2000-01-01T00:00:00Z"
    PreflightStore(config.queue_db_path).put(elapsed)

    started = client.post(
        "/api/partitions",
        json={
            "partition": partition(),
            "provider": "runpod",
            "preflight_id": elapsed["preflight_id"],
            "manifest_digest": elapsed["manifest_digest"],
            "candidate_id": elapsed["recommendation"]["candidate_id"],
            "confirmation_action": "countdown_elapsed",
        },
    )
    assert started.status_code == 202
    assert started.json()["confirmation_action"] == "countdown_elapsed"

    config.rental_confirmation = "never"
    config.__post_init__()
    skipped = client.post(
        "/api/preflight", json={"partition": partition(), "provider": "runpod"}
    ).json()
    assert skipped["confirmation"]["required"] is False
    accepted = client.post(
        "/api/partitions",
        json={
            "partition": partition(),
            "provider": "runpod",
            "preflight_id": skipped["preflight_id"],
            "manifest_digest": skipped["manifest_digest"],
            "candidate_id": skipped["recommendation"]["candidate_id"],
            "force_execution": True,
        },
    )
    assert accepted.status_code == 202
    assert accepted.json()["confirmation_action"] == "policy_skip"


def test_price_change_returns_revised_preflight_without_queueing(monkeypatch, tmp_path):
    config = config_for_preflight(tmp_path)
    queue = JobQueue(config.queue_db_path)
    connector = ReadOnlyConnector()
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(
        preflight,
        "create_connector",
        lambda provider, config: connector,
    )
    client = TestClient(server.app)
    report = client.post(
        "/api/preflight",
        json={"partition": partition(), "provider": "runpod"},
    ).json()
    connector.rate_multiplier = 1.1

    response = client.post(
        "/api/partitions",
        json={
            "partition": partition(),
            "provider": "runpod",
            "preflight_id": report["preflight_id"],
            "manifest_digest": report["manifest_digest"],
            "candidate_id": report["recommendation"]["candidate_id"],
            "confirmation_action": "start_now",
        },
    )

    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "cloud_offload.preflight_changed"
    assert "hourly_rate" in error["details"]["changes"]
    assert error["details"]["revised_preflight"]["schema"] == (
        "cloud-offload.preflight.v1"
    )
    assert error["details"]["revised_preflight"]["confirmation"]["required"] is True
    assert error["details"]["revised_preflight"]["confirmation"]["mandatory"] is True
    assert queue.count_by_status(*list(JobStatus)) == 0
    assert connector.mutations == []


def test_price_change_within_tolerance_uses_current_price_without_reconfirmation(
    monkeypatch, tmp_path
):
    config = config_for_preflight(tmp_path)
    queue = JobQueue(config.queue_db_path)
    connector = ReadOnlyConnector()
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(preflight, "create_connector", lambda *args: connector)
    client = TestClient(server.app)
    report = client.post(
        "/api/preflight", json={"partition": partition(), "provider": "runpod"}
    ).json()
    connector.rate_multiplier = 1.04

    response = client.post(
        "/api/partitions",
        json={
            "partition": partition(),
            "provider": "runpod",
            "preflight_id": report["preflight_id"],
            "manifest_digest": report["manifest_digest"],
            "candidate_id": report["recommendation"]["candidate_id"],
            "confirmation_action": "start_now",
        },
    )

    assert response.status_code == 202
    job = queue.get(response.json()["job_id"])
    assert job.params["preflight"]["hourly_rate"] == 0.78
    assert connector.mutations == []


def test_material_change_forces_confirmation_when_normal_policy_is_never(
    monkeypatch, tmp_path
):
    config = config_for_preflight(tmp_path)
    config.rental_confirmation = "never"
    config.__post_init__()
    queue = JobQueue(config.queue_db_path)
    connector = ReadOnlyConnector()
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(preflight, "create_connector", lambda *args: connector)
    client = TestClient(server.app)
    report = client.post(
        "/api/preflight", json={"partition": partition(), "provider": "runpod"}
    ).json()
    connector.rate_multiplier = 1.1

    changed = client.post(
        "/api/partitions",
        json={
            "partition": partition(),
            "provider": "runpod",
            "preflight_id": report["preflight_id"],
            "manifest_digest": report["manifest_digest"],
            "candidate_id": report["recommendation"]["candidate_id"],
        },
    )
    revised = changed.json()["error"]["details"]["revised_preflight"]
    refused = client.post(
        "/api/partitions",
        json={
            "partition": partition(),
            "provider": "runpod",
            "preflight_id": revised["preflight_id"],
            "manifest_digest": revised["manifest_digest"],
            "candidate_id": report["recommendation"]["candidate_id"],
        },
    )

    assert changed.status_code == 409
    assert revised["confirmation"]["mandatory"] is True
    assert refused.status_code == 409
    assert refused.json()["error"]["code"] == "cloud_offload.confirmation_required"
    assert queue.count_by_status(*list(JobStatus)) == 0


def test_preflight_endpoint_returns_report(monkeypatch, tmp_path):
    config = config_for_preflight(tmp_path)
    expected = {
        "schema": "cloud-offload.preflight.v1",
        "status": "blocked",
        "preflight_id": "preflight-1",
        "manifest_digest": "sha256:" + "b" * 64,
        "created_at": "2026-07-29T00:00:00Z",
        "expires_at": "2026-07-29T00:01:00Z",
        "blockers": [{"code": "test", "message": "Test blocker"}],
        "warnings": [],
        "unknowns": [],
    }
    calls = []

    def fake_build(**kwargs):
        calls.append(kwargs)
        return expected

    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(preflight, "build_partition_preflight", fake_build)

    response = TestClient(server.app).post(
        "/api/preflight",
        json={"partition": partition(), "recommendation_policy": "balanced"},
    )

    assert response.status_code == 200
    assert response.json() == expected
    assert len(calls) == 1
    assert calls[0]["partition"]["partition_id"] == "part-1"


def test_confirmation_policy_settings_are_validated_and_persisted(
    monkeypatch, tmp_path
):
    from cloud_offload import config as config_module

    monkeypatch.setattr(config_module, "CONFIG_DIR", tmp_path)
    client = TestClient(server.app)
    response = client.post(
        "/api/config",
        json={
            "rental_confirmation": "material_changes",
            "confirmation_countdown_seconds": 15,
            "recommendation_policy": "fastest",
            "max_total_job_cost": 0.75,
            "allowed_regions": ["US-MD-1", "EU-RO-1", "US-MD-1"],
            "material_price_change_percent": 7.5,
            "material_cost_change_percent": 12,
        },
    )

    assert response.status_code == 200
    public = response.json()["config"]
    assert public["rental_confirmation"] == "material_changes"
    assert public["confirmation_countdown_seconds"] == 15
    assert public["recommendation_policy"] == "fastest"
    assert public["max_total_job_cost"] == 0.75
    assert public["allowed_regions"] == ["US-MD-1", "EU-RO-1"]
    persisted = json.loads((tmp_path / "config.json").read_text())
    assert persisted["rental_confirmation"] == "material_changes"

    invalid = client.post("/api/config", json={"confirmation_countdown_seconds": 61})
    assert invalid.status_code == 400
    assert (
        json.loads((tmp_path / "config.json").read_text())[
            "confirmation_countdown_seconds"
        ]
        == 15
    )
