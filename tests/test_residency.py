"""Tests for on-prem asset residency.

Three layers, matching the design: the ``on_prem_assets`` policy list round
trips through configuration so the queue-time compiler can read it; connectors
carry a ``residency_class`` so routing knows which backends are rented
hardware; and a partition job that declares ``residency: "on-prem"`` may only
route to on-prem connectors — with none registered, submission is refused
outright rather than quietly running on a cloud pod.
"""

import copy
import json

import pytest
from fastapi.testclient import TestClient

from cloud_offload import config as config_module
from cloud_offload import providers as providers_module
from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.providers import connector_metadata, register_connector
from cloud_offload.providers.base import CloudConnector
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.router import select_profile_provider


NO_ON_PREM_BACKEND = (
    "Partition requires on-prem execution (on-prem-only assets) "
    "but no on-prem backend is registered"
)


class StubConnector(CloudConnector):
    name = "stub"

    def list_available(self, **kwargs):
        return []

    def launch(self, *args, **kwargs):
        raise NotImplementedError

    def get_instance(self, instance_id):
        return None

    def terminate(self, instance_id):
        return True

    def list_instances(self):
        return []


@pytest.fixture(autouse=True)
def restore_registry():
    """Registering a connector is process-wide state; undo it after each test."""
    saved = {
        attribute: copy.deepcopy(getattr(providers_module, attribute))
        for attribute in ("_CONNECTORS", "_CANONICAL_NAMES", "_METADATA")
    }
    yield
    for attribute, snapshot in saved.items():
        live = getattr(providers_module, attribute)
        live.clear()
        live.update(snapshot)


def partition_config(tmp_path, providers=("runpod",)):
    return CloudConfig(
        enabled=True,
        provider="runpod",
        provider_order=list(providers),
        runpod_api_key="secret",
        coordinator_url="https://coordinator.invalid",
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": list(providers),
            }
        },
    )


def partition_request(**partition_fields):
    return {
        "partition": {
            "schema": "comfy.partition.job.v1",
            "partition_id": "part-1",
            "workflow": {"1": {"class_type": "CloudPartitionInput", "inputs": {}}},
            "inputs": [],
            "outputs": [],
            "runner": {"profile": "comfyui"},
            **partition_fields,
        },
        "input_artifacts": {},
        "provider": "auto",
    }


def partition_client(monkeypatch, config):
    queue = JobQueue(config.queue_db_path)
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    return TestClient(server.app), queue


# ---------------------------------------------------------------------------
# Configuration: the on_prem_assets policy list
# ---------------------------------------------------------------------------


def test_on_prem_assets_default_to_an_empty_list():
    config = CloudConfig()

    assert config.on_prem_assets == []
    assert config.to_dict()["on_prem_assets"] == []


def test_on_prem_assets_round_trip_through_the_config_file(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {"cloud": {"on_prem_assets": ["studiox_*.safetensors", "  ", ""]}}
        ),
        encoding="utf-8",
    )

    config = CloudConfig.load(config_path, resolve_secrets=False)

    # Blank entries are dropped at load, not at match time.
    assert config.on_prem_assets == ["studiox_*.safetensors"]
    assert config.to_dict()["on_prem_assets"] == ["studiox_*.safetensors"]


def test_on_prem_assets_environment_overrides_the_file(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        '{"cloud": {"on_prem_assets": ["from_file_*"]}}', encoding="utf-8"
    )
    monkeypatch.setenv(
        "CLOUD_OFFLOAD_ON_PREM_ASSETS", "hero_*.safetensors, nda_?_mesh.glb"
    )

    config = CloudConfig.load(config_path, resolve_secrets=False)

    assert config.on_prem_assets == ["hero_*.safetensors", "nda_?_mesh.glb"]


def test_on_prem_assets_round_trip_through_the_config_routes(monkeypatch, tmp_path):
    """The node pack reads and writes the list through GET/POST /api/config."""
    home = tmp_path / "cloud-offload"
    home.mkdir()
    monkeypatch.setenv("CLOUD_OFFLOAD_HOME", str(home))
    monkeypatch.setattr(config_module, "CONFIG_DIR", home)
    monkeypatch.delenv("CLOUD_OFFLOAD_ON_PREM_ASSETS", raising=False)
    client = TestClient(server.app)

    assert client.get("/api/config").json()["on_prem_assets"] == []

    updated = client.post(
        "/api/config", json={"on_prem_assets": ["studiox_*.safetensors"]}
    )

    assert updated.status_code == 200
    assert updated.json()["config"]["on_prem_assets"] == ["studiox_*.safetensors"]
    assert client.get("/api/config").json()["on_prem_assets"] == [
        "studiox_*.safetensors"
    ]
    persisted = json.loads((home / "config.json").read_text(encoding="utf-8"))
    assert persisted["on_prem_assets"] == ["studiox_*.safetensors"]


# ---------------------------------------------------------------------------
# Connector registry: residency_class
# ---------------------------------------------------------------------------


def test_registered_connectors_default_to_the_cloud_residency_class():
    # The bundled connectors are rented hardware, so they stay cloud-class.
    assert connector_metadata("runpod")["residency_class"] == "cloud"
    assert connector_metadata("vast.ai")["residency_class"] == "cloud"

    register_connector("stub-default", lambda config: StubConnector())

    assert connector_metadata("stub-default")["residency_class"] == "cloud"
    # An unregistered name is treated as cloud too: unknown never means trusted.
    assert connector_metadata("never-registered")["residency_class"] == "cloud"


def test_connector_can_register_as_on_prem():
    register_connector(
        "workroom", lambda config: StubConnector(), residency_class="on-prem"
    )

    assert connector_metadata("workroom")["residency_class"] == "on-prem"


def test_register_connector_rejects_unknown_residency_classes():
    with pytest.raises(ValueError, match="residency_class"):
        register_connector(
            "orbital", lambda config: StubConnector(), residency_class="orbit"
        )


def test_providers_route_reports_residency_class(monkeypatch, tmp_path):
    register_connector(
        "workroom", lambda config: StubConnector(), residency_class="on-prem"
    )
    config = CloudConfig(queue_db_path=str(tmp_path / "queue.db"))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)

    providers = TestClient(server.app).get("/api/providers").json()["providers"]
    by_name = {entry["provider"]: entry for entry in providers}

    assert by_name["runpod"]["residency_class"] == "cloud"
    assert by_name["workroom"]["residency_class"] == "on-prem"


# ---------------------------------------------------------------------------
# Routing and submission: the residency constraint
# ---------------------------------------------------------------------------


def test_select_profile_provider_threads_the_residency_requirement(tmp_path):
    register_connector(
        "workroom", lambda config: StubConnector(), residency_class="on-prem"
    )
    config = partition_config(tmp_path, providers=("runpod", "workroom"))
    config.provider_credentials = {"workroom": "local-token"}

    assert select_profile_provider(config, "comfyui").provider == "runpod"
    assert (
        select_profile_provider(config, "comfyui", residency="on-prem").provider
        == "workroom"
    )
    # Even asking for a cloud provider by name cannot override the constraint.
    with pytest.raises(ValueError, match="not configured for profile"):
        select_profile_provider(config, "comfyui", "runpod", residency="on-prem")


def test_on_prem_partition_is_refused_without_an_on_prem_backend(
    monkeypatch, tmp_path
):
    client, queue = partition_client(monkeypatch, partition_config(tmp_path))

    response = client.post(
        "/api/partitions", json=partition_request(residency="on-prem")
    )

    assert response.status_code == 409
    assert response.json()["error"]["message"] == NO_ON_PREM_BACKEND
    assert queue.list_by_status(*JobStatus) == []


def test_partition_with_invalid_residency_is_rejected(monkeypatch, tmp_path):
    client, queue = partition_client(monkeypatch, partition_config(tmp_path))

    response = client.post(
        "/api/partitions", json=partition_request(residency="orbital")
    )

    assert response.status_code == 400
    assert "residency" in response.json()["error"]["message"]
    assert queue.list_by_status(*JobStatus) == []


@pytest.mark.parametrize("partition_fields", [{}, {"residency": "cloud"}])
def test_cloud_and_unspecified_residency_route_as_before(
    monkeypatch, tmp_path, partition_fields
):
    client, queue = partition_client(monkeypatch, partition_config(tmp_path))

    response = client.post(
        "/api/partitions", json=partition_request(**partition_fields)
    )

    assert response.status_code == 202
    assert queue.get(response.json()["job_id"]).provider == "runpod"


def test_on_prem_partition_routes_to_a_registered_on_prem_backend(
    monkeypatch, tmp_path
):
    register_connector(
        "workroom", lambda config: StubConnector(), residency_class="on-prem"
    )
    config = partition_config(tmp_path, providers=("runpod", "workroom"))
    config.provider_credentials = {"workroom": "local-token"}
    client, queue = partition_client(monkeypatch, config)

    response = client.post(
        "/api/partitions", json=partition_request(residency="on-prem")
    )

    assert response.status_code == 202
    job = queue.get(response.json()["job_id"])
    assert job.provider == "workroom"
    assert job.request["partition"]["residency"] == "on-prem"
