"""Tests for declared partition assets.

The chain under test: a compiled partition names the model files it references
by content digest; the coordinator proves it can supply each one *before* it
routes or provisions; the worker stages them digest-verified at its first job.

The refusal is the point. A partition whose weights nobody can supply must cost
a 409, not a rented GPU that fails on its first prompt — and a file that already
sits on the runner under the right name but with the wrong bytes must be
quarantined loudly rather than used.
"""

import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from cloud_offload import config as config_module
from cloud_offload import router as router_module
from cloud_offload import server
from cloud_offload.assets import (
    normalized_asset_sources,
    normalized_partition_assets,
    resolve_partition_assets,
)
from cloud_offload.config import CloudConfig
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.storage import LocalStorage, partition_artifact_key


CHECKPOINT_BYTES = b"checkpoint weights"
CHECKPOINT_SHA = hashlib.sha256(CHECKPOINT_BYTES).hexdigest()
OTHER_SHA = hashlib.sha256(b"different weights").hexdigest()

CHECKPOINT_ASSET = {
    "category": "checkpoints",
    "filename": "sd_xl_base_1.0.safetensors",
    "sha256": CHECKPOINT_SHA,
    "size": len(CHECKPOINT_BYTES),
    "format": "safetensors",
}

HF_SOURCE = {
    "repo_id": "stabilityai/sdxl-base",
    "revision": "462165984030d82259a11f4367a4eed129e94a7b",
    "filename": "sd_xl_base_1.0.safetensors",
}

PROFILE_WEIGHTS = [
    {
        **HF_SOURCE,
        "files": ["sd_xl_base_1.0.safetensors"],
        "dest": "checkpoints",
        "filename": None,
    }
]


def assets_config(tmp_path, *, asset_sources=None, weights=None):
    profile = {
        "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
        "models": ["comfyui-partition-v1"],
        "providers": ["runpod"],
    }
    if weights is not None:
        profile["weights"] = weights
    return CloudConfig(
        enabled=True,
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="secret",
        coordinator_url="https://coordinator.invalid",
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        asset_sources=asset_sources or {},
        worker_profiles={"comfyui": profile},
    )


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


@pytest.fixture
def watch_routing(monkeypatch):
    """Record every routing call, which is the step just before provisioning."""
    calls = []
    original = router_module.select_profile_provider

    def recording(*args, **kwargs):
        calls.append(args[1:])
        return original(*args, **kwargs)

    monkeypatch.setattr(router_module, "select_profile_provider", recording)
    return calls


def assets_client(monkeypatch, config):
    queue = JobQueue(config.queue_db_path)
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    return TestClient(server.app), queue


# ---------------------------------------------------------------------------
# Configuration: the asset_sources registry
# ---------------------------------------------------------------------------


def test_asset_sources_default_to_an_empty_registry():
    config = CloudConfig()

    assert config.asset_sources == {}
    assert config.to_dict()["asset_sources"] == {}


def test_asset_sources_round_trip_through_the_config_file(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"cloud": {"asset_sources": {CHECKPOINT_SHA.upper(): HF_SOURCE}}}),
        encoding="utf-8",
    )

    config = CloudConfig.load(config_path, resolve_secrets=False)

    # Digests are folded to lowercase hex at load, so lookup is by content only.
    assert config.asset_sources == {CHECKPOINT_SHA: HF_SOURCE}
    assert config.to_dict()["asset_sources"] == {CHECKPOINT_SHA: HF_SOURCE}


def test_asset_sources_round_trip_through_the_config_routes(monkeypatch, tmp_path):
    home = tmp_path / "cloud-offload"
    home.mkdir()
    monkeypatch.setenv("CLOUD_OFFLOAD_HOME", str(home))
    monkeypatch.setattr(config_module, "CONFIG_DIR", home)
    client = TestClient(server.app)

    assert client.get("/api/config").json()["asset_sources"] == {}

    updated = client.post(
        "/api/config", json={"asset_sources": {CHECKPOINT_SHA: {"url": "https://cdn.invalid/x"}}}
    )

    assert updated.status_code == 200
    assert updated.json()["config"]["asset_sources"] == {
        CHECKPOINT_SHA: {"url": "https://cdn.invalid/x"}
    }
    assert client.get("/api/config").json()["asset_sources"] == {
        CHECKPOINT_SHA: {"url": "https://cdn.invalid/x"}
    }


@pytest.mark.parametrize(
    "entry, match",
    [
        ({"revision": "abc", "filename": "x.safetensors"}, "repo_id is required"),
        ({"repo_id": "org/repo", "filename": "x.safetensors"}, "revision is required"),
        ({"repo_id": "org/repo", "revision": "abc"}, "filename is required"),
        (
            {"repo_id": "org/repo", "revision": "abc", "filename": "../escape.safetensors"},
            "must not traverse upward",
        ),
        ({"url": "ftp://cdn.invalid/x"}, "url must be http or https"),
        ({"url": "https://cdn.invalid/x", "repo_id": "org/repo"}, "not both"),
        ("not-an-object", "must be an object"),
    ],
)
def test_invalid_asset_sources_name_the_digest(entry, match):
    with pytest.raises(ValueError, match=match) as failure:
        normalized_asset_sources({CHECKPOINT_SHA: entry})

    assert CHECKPOINT_SHA in str(failure.value)


def test_asset_source_keys_must_be_sha256_digests():
    with pytest.raises(ValueError, match="sha256"):
        normalized_asset_sources({"not-a-digest": HF_SOURCE})


def test_invalid_asset_sources_fail_at_config_load(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"cloud": {"asset_sources": {CHECKPOINT_SHA: {"repo_id": "org/repo"}}}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="revision is required"):
        CloudConfig.load(config_path)


# ---------------------------------------------------------------------------
# Declared asset validation
# ---------------------------------------------------------------------------


def test_declared_assets_are_normalized():
    normalized = normalized_partition_assets(
        [{**CHECKPOINT_ASSET, "sha256": CHECKPOINT_SHA.upper(), "extra": "ignored"}]
    )

    assert normalized == [CHECKPOINT_ASSET]


@pytest.mark.parametrize(
    "asset, match",
    [
        ({**CHECKPOINT_ASSET, "sha256": "abc"}, "sha256 must be"),
        ({**CHECKPOINT_ASSET, "category": ""}, "category is required"),
        ({**CHECKPOINT_ASSET, "filename": ""}, "filename is required"),
        ({**CHECKPOINT_ASSET, "filename": "../../etc/passwd"}, "must not traverse upward"),
        ({**CHECKPOINT_ASSET, "category": "/abs"}, "must be a relative path"),
        ({**CHECKPOINT_ASSET, "size": "big"}, "size must be an integer"),
        ({**CHECKPOINT_ASSET, "size": -1}, "cannot be negative"),
        ({**CHECKPOINT_ASSET, "format": "pytorch"}, "format must be one of"),
        ("not-an-object", "must be an object"),
    ],
)
def test_malformed_declared_assets_are_rejected(asset, match):
    with pytest.raises(ValueError, match=match):
        normalized_partition_assets([asset])


# ---------------------------------------------------------------------------
# Resolution order
# ---------------------------------------------------------------------------


def test_a_registered_source_resolves_by_digest(tmp_path):
    config = assets_config(tmp_path, asset_sources={CHECKPOINT_SHA: HF_SOURCE})

    resolved, unresolved = resolve_partition_assets(config, [CHECKPOINT_ASSET], None)

    assert unresolved == []
    assert resolved[0]["origin"] == "source"
    assert resolved[0]["source"] == HF_SOURCE
    assert "warning" not in resolved[0]


def test_the_artifact_store_resolves_by_digest(tmp_path):
    config = assets_config(tmp_path)
    storage = LocalStorage(config.storage_path)
    staged = tmp_path / "staged.part"
    staged.write_bytes(CHECKPOINT_BYTES)
    storage.upload(staged, partition_artifact_key(CHECKPOINT_SHA))

    resolved, unresolved = resolve_partition_assets(
        config, [CHECKPOINT_ASSET], None, storage
    )

    assert unresolved == []
    assert resolved[0]["origin"] == "store"
    assert resolved[0]["source"] == {"artifact_id": CHECKPOINT_SHA}


def test_a_registered_source_wins_over_the_store(tmp_path):
    config = assets_config(tmp_path, asset_sources={CHECKPOINT_SHA: HF_SOURCE})
    storage = LocalStorage(config.storage_path)
    staged = tmp_path / "staged.part"
    staged.write_bytes(CHECKPOINT_BYTES)
    storage.upload(staged, partition_artifact_key(CHECKPOINT_SHA))

    resolved, _ = resolve_partition_assets(config, [CHECKPOINT_ASSET], None, storage)

    assert resolved[0]["origin"] == "source"


def test_profile_weights_resolve_by_name_and_say_so(tmp_path):
    from cloud_offload.profiles import configured_worker_profiles

    config = assets_config(tmp_path, weights=[dict(PROFILE_WEIGHTS[0])])
    profile = configured_worker_profiles(config)["comfyui"]

    resolved, unresolved = resolve_partition_assets(config, [CHECKPOINT_ASSET], profile)

    assert unresolved == []
    assert resolved[0]["origin"] == "profile"
    assert "not by digest" in resolved[0]["warning"]


def test_an_unknown_digest_with_no_name_match_is_unresolved(tmp_path):
    from cloud_offload.profiles import configured_worker_profiles

    config = assets_config(tmp_path, weights=[dict(PROFILE_WEIGHTS[0])])
    profile = configured_worker_profiles(config)["comfyui"]
    asset = {**CHECKPOINT_ASSET, "filename": "hero.safetensors", "sha256": OTHER_SHA}

    resolved, unresolved = resolve_partition_assets(config, [asset], profile)

    assert resolved == []
    assert unresolved == [asset]


# ---------------------------------------------------------------------------
# Submission: POST /api/partitions
# ---------------------------------------------------------------------------


def test_submission_threads_resolutions_into_the_job(monkeypatch, tmp_path):
    config = assets_config(tmp_path, asset_sources={CHECKPOINT_SHA: HF_SOURCE})
    client, queue = assets_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([CHECKPOINT_ASSET]))

    assert response.status_code == 202
    assert "asset_warnings" not in response.json()
    job = queue.get(response.json()["job_id"])
    assert job.request["assets"] == [
        {**CHECKPOINT_ASSET, "origin": "source", "source": HF_SOURCE}
    ]


def test_submission_warns_when_an_asset_is_only_name_matched(monkeypatch, tmp_path):
    config = assets_config(tmp_path, weights=[dict(PROFILE_WEIGHTS[0])])
    client, queue = assets_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([CHECKPOINT_ASSET]))

    assert response.status_code == 202
    warning = response.json()["asset_warnings"][0]
    assert warning["filename"] == CHECKPOINT_ASSET["filename"]
    assert "not by digest" in warning["warning"]
    assert queue.get(response.json()["job_id"]).request["assets"][0]["origin"] == "profile"


def test_an_unresolvable_asset_is_refused_before_routing(
    monkeypatch, tmp_path, watch_routing
):
    config = assets_config(tmp_path)
    client, queue = assets_client(monkeypatch, config)
    big = {**CHECKPOINT_ASSET, "filename": "hero.safetensors", "size": 6938040714}

    response = client.post("/api/partitions", json=partition_request([big]))

    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "cloud_offload.unresolved_assets"
    assert error["message"] == (
        "Cloud Offload cannot obtain 1 model file declared by this partition: "
        f"hero.safetensors (checkpoints, sha256 {CHECKPOINT_SHA[:12]}, 6616.6 MiB). "
        "Register a source for it in asset_sources, or upload the file to the "
        "coordinator's artifact store."
    )
    # Machine-readable, so a client can render the table instead of parsing prose.
    assert error["details"]["unresolved"] == [big]
    # Nothing was routed, so nothing was provisioned, and no job exists to bill.
    assert watch_routing == []
    assert queue.list_by_status(*JobStatus) == []


def test_the_refusal_names_every_unresolvable_file(monkeypatch, tmp_path):
    config = assets_config(tmp_path, asset_sources={CHECKPOINT_SHA: HF_SOURCE})
    client, _ = assets_client(monkeypatch, config)
    missing = [
        {**CHECKPOINT_ASSET, "filename": "hero.safetensors", "sha256": OTHER_SHA},
        {**CHECKPOINT_ASSET, "filename": "villain.pth", "sha256": "b" * 64, "format": "pickle"},
    ]

    response = client.post(
        "/api/partitions", json=partition_request([CHECKPOINT_ASSET, *missing])
    )

    assert response.status_code == 409
    message = response.json()["error"]["message"]
    assert "2 model files" in message
    assert "hero.safetensors" in message and "villain.pth" in message
    # The resolvable one is not blamed.
    assert CHECKPOINT_ASSET["filename"] not in message


@pytest.mark.parametrize(
    "assets",
    [
        [{**CHECKPOINT_ASSET, "sha256": "abc"}],
        [{**CHECKPOINT_ASSET, "filename": "../../escape.safetensors"}],
        [{**CHECKPOINT_ASSET, "format": "pytorch"}],
        "not-a-list",
    ],
)
def test_malformed_assets_are_rejected_with_400(monkeypatch, tmp_path, assets, watch_routing):
    client, queue = assets_client(monkeypatch, assets_config(tmp_path))

    response = client.post("/api/partitions", json=partition_request(assets))

    assert response.status_code == 400
    assert watch_routing == []
    assert queue.list_by_status(*JobStatus) == []


def test_a_partition_without_assets_behaves_exactly_as_before(monkeypatch, tmp_path):
    """Regression guard for every workflow compiled by an older node pack."""
    client, queue = assets_client(monkeypatch, assets_config(tmp_path))

    response = client.post("/api/partitions", json=partition_request())

    assert response.status_code == 202
    assert response.json().keys() == {"job_id", "status", "status_url"}
    job = queue.get(response.json()["job_id"])
    assert "assets" not in job.request
    assert job.provider == "runpod"


def test_an_empty_asset_list_stays_out_of_the_job_request(monkeypatch, tmp_path):
    client, queue = assets_client(monkeypatch, assets_config(tmp_path))

    response = client.post("/api/partitions", json=partition_request([]))

    assert response.status_code == 202
    assert "assets" not in queue.get(response.json()["job_id"]).request
