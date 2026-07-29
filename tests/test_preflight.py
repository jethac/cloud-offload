from datetime import datetime, timezone

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
    assert report["recommendation"]["candidate_id"] == report["candidates"][0][
        "candidate_id"
    ]
    assert report["candidates"][0]["offer_id"] == "gpu-l40"
    assert report["execution_plan"]["offer_id"] == "gpu-l40"
    assert report["expires_at"] == "2026-07-29T00:01:00Z"
    assert connector.offer_reads == 1
    assert connector.mutations == []
    assert "private_provider_payload" not in str(report)
    assert finite_report(report)


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


def test_prepared_region_wins_balanced_recommendation(
    monkeypatch, tmp_path
):
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


def test_preflight_identity_is_revalidated_and_bound_to_job(
    monkeypatch, tmp_path
):
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
        },
    )

    assert created.status_code == 202
    job = queue.get(created.json()["job_id"])
    assert job.params["preflight"]["candidate_id"] == candidate_id
    assert job.params["preflight"]["offer_id"] == "gpu-l40"
    assert job.provider == "runpod"
    assert connector.offer_reads == 2
    assert connector.mutations == []


def test_price_change_returns_revised_preflight_without_queueing(
    monkeypatch, tmp_path
):
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
        },
    )

    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "cloud_offload.preflight_changed"
    assert "hourly_rate" in error["details"]["changes"]
    assert error["details"]["revised_preflight"]["schema"] == (
        "cloud-offload.preflight.v1"
    )
    assert queue.count_by_status(*list(JobStatus)) == 0
    assert connector.mutations == []


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
