"""Storage planning: sizing a pod's disk before the meter starts.

The failure being prevented is specific and was real: a worker was rented with a
fixed 20 GB container disk, the runner image took 14.6 GB of it, and the
partition then staged a 19.6 GB model onto what was left. The pod died out of
space *after* it started charging — the exact class of failure every other
pre-provision check in this coordinator exists to move to submission time, where
a refusal costs nothing.

So the tests below care about direction as much as arithmetic. A component whose
size cannot be determined must push the total *up* and say so; it must never be
quietly treated as zero, because a confident under-estimate is what buys a dead
pod.
"""

import json

import pytest
from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.dispatcher import Dispatcher
from cloud_offload.profiles import (
    configured_worker_profiles,
    normalized_profile_disk_gb,
)
from cloud_offload.providers.base import CloudProvider
from cloud_offload.queue import JobQueue
from cloud_offload.storage_plan import (
    GIB,
    HEADROOM_FLOOR_BYTES,
    MINIMUM_DISK_GB,
    PACK_ALLOWANCE_BYTES,
    UNKNOWN_IMAGE_BYTES,
    UNKNOWN_SNAPSHOT_BYTES,
    UNKNOWN_WEIGHT_FILE_BYTES,
    plan_disk_gb,
    plan_storage,
    weight_key,
)
from tests.preflight_helpers import accept_test_preflight
from cloud_offload.weight_sizes import (
    cached_weight_sizes,
    huggingface_file_sizes,
    load_size_cache,
    refresh_weight_sizes,
    weight_size_cache_path,
)

# The image and model that actually filled a pod, to the byte.
IMAGE_BYTES = 14_600_000_000
MODEL_BYTES = 19_600_000_000

SDXL_WEIGHTS = [
    {
        "repo_id": "stabilityai/sdxl-base",
        "revision": "462165984030d82259a11f4367a4eed129e94a7b",
        "files": ["sd_xl_base_1.0.safetensors"],
        "dest": "checkpoints",
        "gated": False,
    }
]
SDXL_KEY = weight_key(
    "stabilityai/sdxl-base",
    "462165984030d82259a11f4367a4eed129e94a7b",
    "sd_xl_base_1.0.safetensors",
)
SNAPSHOT_WEIGHTS = [
    {
        "repo_id": "org/vae",
        "revision": "def456",
        "files": None,
        "dest": "vae",
        "gated": False,
    }
]
SNAPSHOT_KEY = weight_key("org/vae", "def456")


def asset(size, filename="model.safetensors"):
    return {
        "category": "checkpoints",
        "filename": filename,
        "sha256": "a" * 64,
        "size": size,
        "format": "safetensors",
    }


def profile(**overrides):
    """A profile whose image size is known, so tests isolate one unknown at a time."""
    return {
        "name": "comfyui",
        "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
        "models": ["comfyui-partition-v1"],
        "providers": ["runpod"],
        "weights": [],
        "custom_nodes": [],
        "extra_disk_gb": 0,
        "image_size_gb": IMAGE_BYTES / GIB,
        **overrides,
    }


def component(plan, name):
    return next(item for item in plan["components"] if item["name"] == name)


# ---------------------------------------------------------------------------
# Planner arithmetic
# ---------------------------------------------------------------------------


def test_declared_assets_are_counted_to_the_byte():
    plan = plan_storage([asset(MODEL_BYTES), asset(1024, "vae.pt")], profile(), image_bytes=IMAGE_BYTES)

    assert plan["assets"] == MODEL_BYTES + 1024
    assert plan["weights"] == 0
    assert plan["packs"] == 0
    assert plan["reserve"] == 0
    assert plan["unknown"] == []


def test_a_plan_totals_every_component():
    plan = plan_storage(
        [asset(MODEL_BYTES)],
        profile(weights=SDXL_WEIGHTS, custom_nodes=[{"registry_id": "pack", "version": "1"}]),
        image_bytes=IMAGE_BYTES,
        weight_bytes={SDXL_KEY: 6_938_040_714},
    )

    assert plan["image"] == IMAGE_BYTES
    assert plan["assets"] == MODEL_BYTES
    assert plan["weights"] == 6_938_040_714
    assert plan["packs"] == PACK_ALLOWANCE_BYTES
    assert plan["total"] == (
        plan["image"]
        + plan["assets"]
        + plan["weights"]
        + plan["packs"]
        + plan["reserve"]
        + plan["headroom"]
    )
    assert plan["unknown"] == []


def test_the_rent_that_failed_is_now_planned_for():
    """The original incident: a 14.6 GB image plus a 19.6 GB model on a 20 GB disk."""
    plan = plan_storage([asset(MODEL_BYTES)], profile(), image_bytes=IMAGE_BYTES)

    assert plan_disk_gb(plan) > 20


def test_each_declared_pack_gets_a_flat_allowance():
    packs = [{"registry_id": "one", "version": "1"}, {"registry_id": "two", "version": "1"}]

    plan = plan_storage([], profile(custom_nodes=packs), image_bytes=IMAGE_BYTES)

    assert plan["packs"] == 2 * PACK_ALLOWANCE_BYTES
    assert "allowance, not a measurement" in component(plan, "packs")["detail"]


def test_extra_disk_gb_becomes_the_reserve():
    plan = plan_storage([], profile(extra_disk_gb=60), image_bytes=IMAGE_BYTES)

    assert plan["reserve"] == 60 * GIB
    assert "extra_disk_gb" in component(plan, "reserve")["detail"]


def test_a_profile_declaring_no_reserve_says_what_it_cannot_see():
    # The real case behind the field: a node whose from_pretrained call pulls
    # 53.8 GB that no manifest mentions.
    plan = plan_storage([], profile(), image_bytes=IMAGE_BYTES)

    assert plan["reserve"] == 0
    assert "downloads its own weights at runtime" in component(plan, "reserve")["detail"]


def test_headroom_falls_back_to_its_floor_for_a_small_plan():
    plan = plan_storage([asset(1024)], profile(), image_bytes=1024)

    assert plan["headroom"] == HEADROOM_FLOOR_BYTES


def test_headroom_scales_with_a_large_plan():
    plan = plan_storage([asset(400 * GIB)], profile(), image_bytes=IMAGE_BYTES)

    subtotal = plan["image"] + plan["assets"]
    assert plan["headroom"] == int(subtotal * 0.20)
    assert plan["headroom"] > HEADROOM_FLOOR_BYTES


# ---------------------------------------------------------------------------
# Unknowns: named, and charged upward
# ---------------------------------------------------------------------------


def test_an_undeclared_image_size_is_named_and_charged():
    known = plan_storage([], profile(), image_bytes=IMAGE_BYTES)
    unknown = plan_storage([], profile(), image_bytes=None)

    assert unknown["image"] == UNKNOWN_IMAGE_BYTES
    assert unknown["total"] > known["total"]
    assert any("runner image" in item for item in unknown["unknown"])
    assert "image_size_gb" in unknown["unknown"][0]


def test_an_unresolved_weight_file_is_named_and_charged():
    resolved = plan_storage(
        [], profile(weights=SDXL_WEIGHTS), image_bytes=IMAGE_BYTES,
        weight_bytes={SDXL_KEY: 1024},
    )
    unresolved = plan_storage(
        [], profile(weights=SDXL_WEIGHTS), image_bytes=IMAGE_BYTES, weight_bytes={}
    )

    assert unresolved["weights"] == UNKNOWN_WEIGHT_FILE_BYTES
    assert unresolved["total"] > resolved["total"]
    assert unresolved["unknown"] == [
        f"the pinned weights file {SDXL_KEY}, charged 8.0 GiB"
    ]


def test_an_unresolved_snapshot_is_charged_more_than_a_single_file():
    plan = plan_storage([], profile(weights=SNAPSHOT_WEIGHTS), image_bytes=IMAGE_BYTES)

    assert plan["weights"] == UNKNOWN_SNAPSHOT_BYTES
    assert UNKNOWN_SNAPSHOT_BYTES > UNKNOWN_WEIGHT_FILE_BYTES
    assert SNAPSHOT_KEY in plan["unknown"][0]


def test_a_partial_weight_mapping_charges_only_what_is_missing():
    weights = SDXL_WEIGHTS + SNAPSHOT_WEIGHTS

    plan = plan_storage(
        [], profile(weights=weights), image_bytes=IMAGE_BYTES,
        weight_bytes={SDXL_KEY: 1024},
    )

    assert plan["weights"] == 1024 + UNKNOWN_SNAPSHOT_BYTES
    assert len(plan["unknown"]) == 1
    assert "2 pinned profile weights entries, 1 of unknown size" == component(
        plan, "weights"
    )["detail"]


def test_nothing_known_still_plans_a_usable_pod():
    plan = plan_storage(None, None)

    assert plan["image"] == UNKNOWN_IMAGE_BYTES
    assert plan["total"] > UNKNOWN_IMAGE_BYTES
    assert plan_disk_gb(plan) >= MINIMUM_DISK_GB


# ---------------------------------------------------------------------------
# plan_disk_gb
# ---------------------------------------------------------------------------


def test_the_requested_disk_rounds_up_to_a_whole_gibibyte():
    assert plan_disk_gb({"total": 200 * GIB + 1}) == 201
    assert plan_disk_gb({"total": 200 * GIB}) == 200


def test_the_requested_disk_never_falls_below_the_minimum():
    assert plan_disk_gb({"total": 1}) == MINIMUM_DISK_GB
    assert plan_disk_gb({"total": 0}) == MINIMUM_DISK_GB
    assert plan_disk_gb({}) == MINIMUM_DISK_GB


# ---------------------------------------------------------------------------
# Profile fields
# ---------------------------------------------------------------------------


def storage_config(tmp_path, **profile_fields):
    return CloudConfig(
        enabled=True,
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="secret",
        coordinator_url="https://coordinator.invalid",
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
                **profile_fields,
            }
        },
    )


def test_the_storage_fields_surface_through_configured_profiles(tmp_path):
    config = storage_config(tmp_path, extra_disk_gb=60, image_size_gb=14.6)

    resolved = configured_worker_profiles(config)["comfyui"]

    assert resolved["extra_disk_gb"] == 60.0
    assert resolved["image_size_gb"] == 14.6


def test_the_storage_fields_default_to_zero(tmp_path):
    resolved = configured_worker_profiles(storage_config(tmp_path))["comfyui"]

    assert resolved["extra_disk_gb"] == 0.0
    assert resolved["image_size_gb"] == 0.0


@pytest.mark.parametrize("field", ["extra_disk_gb", "image_size_gb"])
def test_a_negative_storage_field_is_refused_by_name(field):
    with pytest.raises(ValueError, match=f"{field} cannot be negative"):
        normalized_profile_disk_gb("comfyui", field, -1)


@pytest.mark.parametrize("field", ["extra_disk_gb", "image_size_gb"])
def test_a_non_numeric_storage_field_is_refused_by_name(field):
    with pytest.raises(ValueError, match=f"{field} must be a number"):
        normalized_profile_disk_gb("comfyui", field, "lots")


def test_an_invalid_storage_field_fails_at_config_load(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "cloud": {
                    "worker_profiles": {
                        "comfyui": {
                            "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                            "models": ["comfyui-workflow"],
                            "extra_disk_gb": -5,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="extra_disk_gb cannot be negative"):
        CloudConfig.load(config_path)


def test_the_container_disk_ceiling_defaults_and_validates():
    assert CloudConfig().max_container_disk_gb == 500
    with pytest.raises(ValueError, match="max_container_disk_gb must be at least 1"):
        CloudConfig(max_container_disk_gb=0)


# ---------------------------------------------------------------------------
# Size discovery
# ---------------------------------------------------------------------------


class FakeHub:
    """Answers the revision listing; records every URL it was asked for."""

    def __init__(self, payload=None, status_code=200):
        self.payload = payload if payload is not None else {
            "siblings": [
                {"rfilename": "sd_xl_base_1.0.safetensors", "size": 6_938_040_714},
                {"rfilename": "config.json", "size": 512},
            ]
        }
        self.status_code = status_code
        self.urls = []

    def __call__(self, url, headers):
        self.urls.append(url)
        return self

    def json(self):
        return self.payload


def test_discovery_resolves_a_pinned_file_size():
    hub = FakeHub()

    sizes = huggingface_file_sizes(SDXL_WEIGHTS, fetch=hub)

    assert sizes == {SDXL_KEY: 6_938_040_714}
    assert hub.urls[0].startswith(
        "https://huggingface.co/api/models/stabilityai/sdxl-base/revision/"
    )


def test_discovery_sums_a_whole_snapshot():
    hub = FakeHub()

    sizes = huggingface_file_sizes(SNAPSHOT_WEIGHTS, fetch=hub)

    assert sizes == {SNAPSHOT_KEY: 6_938_040_714 + 512}


def test_a_snapshot_with_an_unsized_file_stays_unknown():
    # A partial sum presented as a fact would under-provision the pod.
    hub = FakeHub({"siblings": [{"rfilename": "a.bin", "size": 10}, {"rfilename": "b.bin"}]})

    assert huggingface_file_sizes(SNAPSHOT_WEIGHTS, fetch=hub) == {}


def test_an_http_failure_degrades_to_unknown():
    hub = FakeHub(status_code=404)

    assert huggingface_file_sizes(SDXL_WEIGHTS, fetch=hub) == {}


def test_a_raising_fetch_degrades_to_unknown():
    def explode(url, headers):
        raise ConnectionError("no route to host")

    assert huggingface_file_sizes(SDXL_WEIGHTS, fetch=explode) == {}


def test_a_cache_hit_avoids_a_second_fetch():
    hub = FakeHub()
    cache = {}

    first = huggingface_file_sizes(SDXL_WEIGHTS, fetch=hub, cache=cache)
    second = huggingface_file_sizes(SDXL_WEIGHTS, fetch=hub, cache=cache)

    assert first == second == {SDXL_KEY: 6_938_040_714}
    assert len(hub.urls) == 1


def test_one_request_answers_for_every_file_in_a_revision():
    hub = FakeHub()
    weights = [{**SDXL_WEIGHTS[0], "files": ["sd_xl_base_1.0.safetensors", "config.json"]}]

    sizes = huggingface_file_sizes(weights, fetch=hub)

    assert len(sizes) == 2
    assert len(hub.urls) == 1


def test_resolved_sizes_persist_beside_the_queue_database(tmp_path):
    config = storage_config(tmp_path)
    resolved = configured_worker_profiles(config)["comfyui"]
    resolved["weights"] = SDXL_WEIGHTS
    hub = FakeHub()

    refresh_weight_sizes(config, resolved, fetch=hub)

    assert weight_size_cache_path(config).parent == (tmp_path / "queue.db").parent
    assert load_size_cache(weight_size_cache_path(config))[SDXL_KEY] == 6_938_040_714
    assert cached_weight_sizes(config, resolved) == {SDXL_KEY: 6_938_040_714}


def test_a_cold_cache_reports_nothing_rather_than_guessing(tmp_path):
    config = storage_config(tmp_path)
    resolved = configured_worker_profiles(config)["comfyui"]
    resolved["weights"] = SDXL_WEIGHTS

    assert cached_weight_sizes(config, resolved) == {}


# ---------------------------------------------------------------------------
# Dispatcher: renting the planned disk
# ---------------------------------------------------------------------------


class LaunchProvider(CloudProvider):
    """Captures the launch kwargs, so the requested disk can be asserted."""

    def __init__(self):
        self.kwargs = None

    @property
    def name(self) -> str:
        return "runpod"

    def list_available(self, *args, **kwargs):
        return []

    def find_cheapest(self, **kwargs):
        return {"id": "offer-1", "gpu_type": "RTX 4090", "hourly_rate": 0.34}

    def launch(self, *args, **kwargs):
        self.kwargs = kwargs
        from types import SimpleNamespace

        return SimpleNamespace(
            id="pod-1",
            provider="runpod",
            gpu_type="RTX 4090",
            hourly_rate=0.34,
            status="running",
        )

    def get_instance(self, instance_id):
        return None

    def terminate(self, instance_id):
        return True

    def list_instances(self):
        return []


def queued_job(queue, disk_gb=None):
    params = {"runtime_profile": "comfyui"}
    if disk_gb is not None:
        params["container_disk_gb"] = disk_gb
    return queue.create("comfyui-partition-v1", "artifacts://partition", params=params)


def test_the_dispatcher_rents_the_planned_disk_when_it_is_larger(tmp_path):
    config = storage_config(tmp_path)
    provider = LaunchProvider()
    dispatcher = Dispatcher(config, provider=provider)
    job = queued_job(dispatcher.queue, disk_gb=140)

    dispatcher._launch_worker("runpod", "comfyui", [job])

    assert provider.kwargs["disk_gb"] == 140


def test_the_dispatcher_keeps_the_configured_disk_when_the_plan_is_smaller(tmp_path):
    config = storage_config(tmp_path)
    config.runpod_container_disk_gb = 80
    provider = LaunchProvider()
    dispatcher = Dispatcher(config, provider=provider)
    job = queued_job(dispatcher.queue, disk_gb=40)

    dispatcher._launch_worker("runpod", "comfyui", [job])

    assert provider.kwargs["disk_gb"] == 80


def test_a_job_queued_before_planning_existed_gets_the_configured_disk(tmp_path):
    config = storage_config(tmp_path)
    provider = LaunchProvider()
    dispatcher = Dispatcher(config, provider=provider)
    job = queued_job(dispatcher.queue)

    dispatcher._launch_worker("runpod", "comfyui", [job])

    assert provider.kwargs["disk_gb"] == config.runpod_container_disk_gb


def test_the_largest_queued_plan_wins(tmp_path):
    config = storage_config(tmp_path)
    provider = LaunchProvider()
    dispatcher = Dispatcher(config, provider=provider)
    jobs = [queued_job(dispatcher.queue, disk_gb=size) for size in (40, 260, 90)]

    dispatcher._launch_worker("runpod", "comfyui", jobs)

    assert provider.kwargs["disk_gb"] == 260


def test_runpod_uses_the_planned_disk_for_its_pod():
    from cloud_offload.providers.runpod import RunPodConnector

    captured = {}

    class FakeHttp:
        def request(self, method, url, **kwargs):
            captured.update(kwargs.get("json") or {})
            raise RuntimeError("stop before the pod is created")

    connector = RunPodConnector(api_key="k", container_disk_gb=20, http_client=FakeHttp())

    with pytest.raises(RuntimeError, match="stop before"):
        connector.launch(offer_id="gpu", docker_image="example/runner", disk_gb=140)

    assert captured["variables"]["input"]["containerDiskInGb"] == 140


def test_runpod_falls_back_to_its_configured_disk():
    from cloud_offload.providers.runpod import RunPodConnector

    captured = {}

    class FakeHttp:
        def request(self, method, url, **kwargs):
            captured.update(kwargs.get("json") or {})
            raise RuntimeError("stop before the pod is created")

    connector = RunPodConnector(api_key="k", container_disk_gb=20, http_client=FakeHttp())

    with pytest.raises(RuntimeError, match="stop before"):
        connector.launch(offer_id="gpu", docker_image="example/runner")

    assert captured["variables"]["input"]["containerDiskInGb"] == 20


# ---------------------------------------------------------------------------
# Submission
# ---------------------------------------------------------------------------


def storage_client(monkeypatch, config):
    queue = JobQueue(config.queue_db_path)
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    accept_test_preflight(monkeypatch, server, config)
    return TestClient(server.app), queue


def partition_request(assets=None):
    partition = {
        "schema": "comfy.partition.job.v1",
        "partition_id": "part-1",
        "workflow": {"1": {"class_type": "CloudPartitionInput", "inputs": {}}},
        "inputs": [],
        "outputs": [],
        "runner": {"profile": "comfyui"},
    }
    if assets is not None:
        partition["assets"] = assets
    return {"partition": partition, "input_artifacts": {}, "provider": "auto"}


def test_the_accepted_response_carries_the_storage_plan(monkeypatch, tmp_path):
    config = storage_config(tmp_path, image_size_gb=14.6)
    config.asset_sources = {"a" * 64: {"url": "https://example.invalid/model.safetensors"}}
    client, _ = storage_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([asset(MODEL_BYTES)]))

    assert response.status_code == 202
    storage = response.json()["storage"]
    assert storage["total_gb"] > 20
    assert storage["total_bytes"] > MODEL_BYTES
    assert [item["name"] for item in storage["components"]] == [
        "image",
        "assets",
        "weights",
        "packs",
        "reserve",
        "headroom",
    ]
    assert storage["unknown"] == []


def test_an_undeclared_image_size_is_reported_to_the_submitter(monkeypatch, tmp_path):
    client, _ = storage_client(monkeypatch, storage_config(tmp_path))

    response = client.post("/api/partitions", json=partition_request())

    assert response.status_code == 202
    assert any(
        "image_size_gb" in item for item in response.json()["storage"]["unknown"]
    )


def test_the_planned_disk_reaches_the_job(monkeypatch, tmp_path):
    config = storage_config(tmp_path, image_size_gb=14.6)
    config.asset_sources = {"a" * 64: {"url": "https://example.invalid/model.safetensors"}}
    client, queue = storage_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([asset(MODEL_BYTES)]))

    job = queue.get(response.json()["job_id"])
    assert job.params["container_disk_gb"] == response.json()["storage"]["total_gb"]


def test_a_plan_over_the_ceiling_is_refused_before_provisioning(monkeypatch, tmp_path):
    config = storage_config(tmp_path, image_size_gb=14.6, extra_disk_gb=600)
    client, queue = storage_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request())

    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "cloud_offload.storage_plan_exceeds_ceiling"
    assert "above the configured ceiling of 500 GB" in error["message"]
    assert "reserve 600.0 GiB" in error["message"]
    assert "max_container_disk_gb" in error["message"]
    assert error["details"]["storage"]["total_gb"] > 500
    # Refused before anything was queued, let alone rented.
    assert queue.list_by_status() == []


def test_raising_the_ceiling_accepts_the_same_partition(monkeypatch, tmp_path):
    config = storage_config(tmp_path, image_size_gb=14.6, extra_disk_gb=600)
    config.max_container_disk_gb = 2000
    client, _ = storage_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request())

    assert response.status_code == 202
