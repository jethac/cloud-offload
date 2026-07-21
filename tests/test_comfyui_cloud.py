import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.comfyui import ComfyUIWorkflowError, ComfyUIWorkflowExecutor
from cloud_offload.config import CloudConfig
from cloud_offload.queue import JobQueue
from cloud_offload.router import select_profile_provider
from cloud_offload.worker import Worker
from cloud_offload.profiles import configured_worker_profiles, load_worker_manifest


class Response:
    def __init__(self, payload=None, *, content=b"", headers=None, status_code=200):
        self.payload = payload
        self.content = content
        self.headers = headers or {}
        self.status_code = status_code
        self.text = "" if payload is None else str(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
        return None

    def json(self):
        return self.payload


class HTTP:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        return next(self.responses)


def profile_config(tmp_path):
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
                "models": ["comfyui-workflow"],
                "providers": ["runpod"],
                "gpu_type": "any",
                "min_gpu_ram_gb": 16,
            }
        },
    )


def test_comfyui_profile_accepts_workflow_capability(tmp_path):
    config = profile_config(tmp_path)

    profile = configured_worker_profiles(config)["comfyui"]
    route = select_profile_provider(config, "comfyui", "runpod")

    assert profile["models"] == ["comfyui-workflow"]
    assert route.provider == "runpod"
    assert route.profile["name"] == "comfyui"


def test_omni_profile_accepts_partition_capability(tmp_path):
    config = profile_config(tmp_path)
    config.worker_profiles["comfyui-omni"] = {
        "image": "ghcr.io/example/comfyui-omni@sha256:" + "b" * 64,
        "models": ["comfyui-workflow", "comfyui-partition-v1"],
        "providers": ["runpod"],
        "gpu_type": "any",
        "min_gpu_ram_gb": 40,
    }

    profile = configured_worker_profiles(config)["comfyui-omni"]
    route = select_profile_provider(config, "comfyui-omni", "runpod")

    assert profile["models"] == ["comfyui-workflow", "comfyui-partition-v1"]
    assert route.profile["name"] == "comfyui-omni"
    assert route.profile["min_gpu_ram_gb"] == 40


def test_comfyui_manifest_and_worker_capability(tmp_path):
    manifest = tmp_path / "runtime-profile.json"
    manifest.write_text(
        '{"profile":"comfyui","models":["comfyui-workflow"]}',
        encoding="utf-8",
    )
    worker = Worker.__new__(Worker)
    worker.runtime_profile = "comfyui"
    worker.declared_capabilities = ["comfyui-workflow"]

    assert load_worker_manifest(manifest)["models"] == ["comfyui-workflow"]
    assert worker._validated_capabilities() == ["comfyui-workflow"]

    manifest.write_text(
        '{"profile":"comfyui","models":["comfyui-workflow",'
        '"comfyui-partition-v1"],"partition_protocol":"comfy.partition.bundle.v1"}',
        encoding="utf-8",
    )
    worker.config = SimpleNamespace(worker_manifest_path=str(manifest))
    worker._apply_image_manifest()
    assert worker.declared_capabilities == [
        "comfyui-workflow",
        "comfyui-partition-v1",
    ]
    assert load_worker_manifest(manifest)["partition_protocol"] == "comfy.partition.bundle.v1"


def test_executor_uploads_inputs_runs_prompt_and_returns_images():
    http = HTTP(
        [
            Response({"name": "input.png", "subfolder": "", "type": "input"}),
            Response({"prompt_id": "prompt-1"}),
            Response(
                {
                    "prompt-1": {
                        "status": {"status_str": "success", "completed": True},
                        "outputs": {
                            "9": {
                                "images": [
                                    {
                                        "filename": "result.png",
                                        "subfolder": "",
                                        "type": "output",
                                    }
                                ]
                            }
                        },
                    }
                }
            ),
            Response(
                content=b"image-bytes",
                headers={"Content-Type": "image/png"},
            ),
        ]
    )
    executor = ComfyUIWorkflowExecutor("http://comfy.invalid", http)

    result = executor.execute(
        {"1": {"class_type": "LoadImage", "inputs": {"image": "input.png"}}},
        inputs={"input.png": base64.b64encode(b"input-bytes").decode("ascii")},
    )

    assert result["prompt_id"] == "prompt-1"
    assert base64.b64decode(result["images"][0]["data"]) == b"image-bytes"
    assert [request[0] for request in http.requests] == ["POST", "POST", "GET", "GET"]


def test_executor_rejects_input_paths():
    executor = ComfyUIWorkflowExecutor("http://comfy.invalid", HTTP([]))

    with pytest.raises(ComfyUIWorkflowError, match="filename"):
        executor.execute(
            {"1": {"class_type": "LoadImage", "inputs": {}}},
            inputs={"../secret.png": base64.b64encode(b"x").decode("ascii")},
        )


def test_executor_preserves_comfy_prompt_validation_error():
    http = HTTP(
        [
            Response(
                {
                    "error": {
                        "type": "prompt_outputs_failed_validation",
                        "message": "Cannot execute because node MissingCustomNode does not exist.",
                    }
                },
                status_code=400,
            )
        ]
    )
    executor = ComfyUIWorkflowExecutor("http://comfy.invalid", http)

    with pytest.raises(ComfyUIWorkflowError, match="MissingCustomNode does not exist"):
        executor.execute({"1": {"class_type": "MissingCustomNode", "inputs": {}}})


def test_workflow_endpoint_queues_dedicated_profile(monkeypatch, tmp_path):
    config = profile_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))

    response = TestClient(server.app).post(
        "/api/workflows",
        json={
            "workflow": {"1": {"class_type": "LoadImage", "inputs": {}}},
            "provider": "runpod",
        },
    )

    assert response.status_code == 202
    job = queue.get(response.json()["job_id"])
    assert job.model == "comfyui-workflow"
    assert job.params["runtime_profile"] == "comfyui"
    assert job.request["workflow"]["1"]["class_type"] == "LoadImage"
    assert response.json()["status_url"] == f"/api/jobs/{job.id}"


def test_partition_artifact_and_job_endpoints(monkeypatch, tmp_path):
    config = profile_config(tmp_path)
    config.worker_profiles["comfyui-omni"] = {
        "image": "ghcr.io/example/comfyui-omni@sha256:" + "b" * 64,
        "models": ["comfyui-partition-v1"],
        "providers": ["runpod"],
        "min_gpu_ram_gb": 40,
    }
    queue = JobQueue(config.queue_db_path)
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    client = TestClient(server.app)
    content = b"safe-partition-bundle"
    digest = hashlib.sha256(content).hexdigest()

    uploaded = client.post(
        "/api/artifacts",
        data={"sha256": digest},
        files={"file": ("input.part", content, "application/vnd.comfy.partition+zip")},
    )
    assert uploaded.status_code == 200
    assert uploaded.json()["artifact_id"] == digest
    assert client.get(f"/api/artifacts/{digest}").content == content

    partition_request = {
        "partition": {
            "schema": "comfy.partition.job.v1",
            "partition_id": "part-1",
            "workflow": {"1": {"class_type": "CloudPartitionInput", "inputs": {}}},
            "inputs": [{"key": "input_0000"}],
            "outputs": [],
            "runner": {"profile": "comfyui-omni"},
        },
        "input_artifacts": {"input_0000": digest},
        "provider": "runpod",
    }
    created = client.post("/api/partitions", json=partition_request)
    assert created.status_code == 202
    job = queue.get(created.json()["job_id"])
    assert job.model == "comfyui-partition-v1"
    assert job.params["runtime_profile"] == "comfyui-omni"
    assert job.request["kind"] == "comfyui-partition"
    assert job.request["input_artifacts"] == {"input_0000": digest}

    cached_result = {
        "schema": "comfy.partition.result.v1",
        "partition_id": "part-1",
        "output_artifacts": {"output_0000": digest},
    }
    queue.complete_job(job.id, cached_result)
    cache_hit = client.post("/api/partitions", json=partition_request)
    assert cache_hit.status_code == 202
    assert cache_hit.json()["cache_hit"] is True
    cached_job = queue.get(cache_hit.json()["job_id"])
    assert cached_job.status.value == "completed"
    assert cached_job.result == cached_result

    queue.set_worker_token("worker-secret")
    event = client.post(
        f"/api/workers/jobs/{job.id}/events",
        headers={"Authorization": "Bearer worker-secret"},
        json={
            "event": {
                "type": "progress",
                "node_id": "1",
                "data": {"value": 2, "max": 10},
            }
        },
    )
    assert event.status_code == 200
    page = client.get(f"/api/jobs/{job.id}/events").json()
    assert page["events"][0]["event"]["node_id"] == "1"
    resumed = client.get(
        f"/api/jobs/{job.id}/events?after={page['next_after']}"
    ).json()
    assert resumed["events"] == []


def test_worker_partition_stages_bridges_and_publishes_outputs(tmp_path):
    config = profile_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    worker = Worker.__new__(Worker)
    worker.queue = queue
    from cloud_offload.storage import create_storage

    worker.storage = create_storage(config)
    input_content = b"input-bundle"
    input_digest = hashlib.sha256(input_content).hexdigest()
    input_source = tmp_path / "input.part"
    input_source.write_bytes(input_content)
    worker.storage.upload(input_source, worker._partition_artifact_key(input_digest))

    job = queue.create(
        model="comfyui-partition-v1",
        input_path="artifacts://partition",
        request={
            "kind": "comfyui-partition",
            "partition": {
                "schema": "comfy.partition.job.v1",
                "partition_id": "part-1",
                "workflow": {
                    "in": {
                        "class_type": "CloudPartitionInput",
                        "inputs": {"boundary_key": "input_0000", "artifact_path": "", "type_name": "IMAGE"},
                    },
                    "out": {
                        "class_type": "CloudPartitionOutput",
                        "inputs": {"value": ["in", 0], "boundary_key": "output_0000", "output_path": "", "type_name": "IMAGE"},
                    },
                },
                "inputs": [{"key": "input_0000"}],
                "outputs": [{"key": "output_0000"}],
            },
            "input_artifacts": {"input_0000": input_digest},
            "timeout_seconds": 30,
        },
    )

    class Executor:
        def execute(
            self,
            workflow,
            timeout_seconds,
            event_callback=None,
            cancel_check=None,
        ):
            staged_input = Path(workflow["in"]["inputs"]["artifact_path"])
            staged_output = Path(workflow["out"]["inputs"]["output_path"])
            assert staged_input.read_bytes() == input_content
            assert cancel_check() is False
            event_callback(
                {
                    "type": "executing",
                    "node_id": "in",
                    "data": {"node": "in", "prompt_id": "remote-1"},
                }
            )
            event_callback(
                {
                    "type": "progress",
                    "node_id": "in",
                    "data": {"node": "in", "value": 1, "max": 2},
                }
            )
            staged_output.write_bytes(b"output-bundle")
            event_callback(
                {
                    "type": "executed",
                    "node_id": "out",
                    "data": {"node": "out", "prompt_id": "remote-1"},
                }
            )
            return {"prompt_id": "remote-1", "outputs": {}}

    import os

    previous = os.environ.get("COMFY_PARTITION_ROOT")
    os.environ["COMFY_PARTITION_ROOT"] = str(tmp_path / "partitions")
    try:
        result = worker._run_comfyui_partition(job, Executor())
    finally:
        if previous is None:
            os.environ.pop("COMFY_PARTITION_ROOT", None)
        else:
            os.environ["COMFY_PARTITION_ROOT"] = previous

    output_id = result["output_artifacts"]["output_0000"]
    assert result["schema"] == "comfy.partition.result.v1"
    assert output_id == hashlib.sha256(b"output-bundle").hexdigest()
    assert worker.storage.exists(worker._partition_artifact_key(output_id))
    events = queue.list_events(job.id)
    assert [item["event"]["type"] for item in events] == [
        "partition_staging",
        "executing",
        "progress",
        "executed",
        "partition_uploading",
    ]
    assert queue.get(job.id).progress > 10


# === Auth: tunneled loopback must be able to require a bearer token ===

def test_loopback_bind_does_not_require_auth_by_default(monkeypatch):
    monkeypatch.delenv("CLOUD_OFFLOAD_REQUIRE_AUTH", raising=False)
    assert server._resolve_auth_required("127.0.0.1", require_auth=False) is False


def test_require_auth_flag_forces_bearer_on_loopback(monkeypatch):
    monkeypatch.delenv("CLOUD_OFFLOAD_REQUIRE_AUTH", raising=False)
    # A tunneled loopback service is publicly reachable; the flag closes the gap.
    assert server._resolve_auth_required("127.0.0.1", require_auth=True) is True


def test_require_auth_env_forces_bearer_on_loopback(monkeypatch):
    monkeypatch.setenv("CLOUD_OFFLOAD_REQUIRE_AUTH", "true")
    assert server._resolve_auth_required("127.0.0.1", require_auth=False) is True


def test_non_loopback_bind_always_requires_auth(monkeypatch):
    monkeypatch.delenv("CLOUD_OFFLOAD_REQUIRE_AUTH", raising=False)
    assert server._resolve_auth_required("0.0.0.0", require_auth=False) is True


def test_bearer_middleware_rejects_and_admits_and_exempts_worker(monkeypatch):
    monkeypatch.setattr(server, "auth_required", True)
    monkeypatch.setattr(server, "auth_token", "sekret")
    client = TestClient(server.app)
    # No credential -> rejected on a normal route.
    assert client.get("/api/status").status_code == 401
    # Correct bearer -> admitted (health needs no queue).
    assert client.get("/api/health", headers={"Authorization": "Bearer sekret"}).status_code == 200
    # Worker channel carries its own token and is exempt from the global bearer.
    worker = client.get(f"{server.WORKER_PATH_PREFIX}/policy")
    assert worker.status_code != 401


# === Output collection: 3D/mesh outputs must not be dropped ===

def test_executor_returns_mesh_outputs_under_files():
    http = HTTP(
        [
            Response({"prompt_id": "prompt-9"}),
            Response(
                {
                    "prompt-9": {
                        "status": {"status_str": "success", "completed": True},
                        "outputs": {
                            "12": {
                                "3d": [
                                    {
                                        "filename": "mesh_00001_.glb",
                                        "subfolder": "",
                                        "type": "output",
                                    }
                                ]
                            }
                        },
                    }
                }
            ),
            Response(
                content=b"glb-bytes",
                headers={"Content-Type": "model/gltf-binary"},
            ),
        ]
    )
    executor = ComfyUIWorkflowExecutor("http://comfy.invalid", http)

    result = executor.execute(
        {"1": {"class_type": "SaveGLB", "inputs": {"mesh": ["0", 0]}}},
    )

    assert result["images"] == []
    assert len(result["files"]) == 1
    entry = result["files"][0]
    assert entry["filename"] == "mesh_00001_.glb"
    assert entry["output_kind"] == "3d"
    assert base64.b64decode(entry["data"]) == b"glb-bytes"


# === Pluggable provider discovery and credentials ===

def test_providers_endpoint_lists_registered_connectors(monkeypatch, tmp_path):
    from cloud_offload.providers import register_connector
    from cloud_offload.providers.base import CloudConnector, Instance

    class PluginConnector(CloudConnector):
        @property
        def name(self):
            return "acme"

        def list_available(self, **kwargs):
            return []

        def launch(self, *args, **kwargs):
            return Instance("i-1", "acme", "L4", 1, 0.5, "running")

        def get_instance(self, instance_id):
            return None

        def terminate(self, instance_id):
            return True

        def list_instances(self):
            return []

    # Restore the registry afterwards: a leaked global registration changes what
    # every later test sees from connector_names().
    import cloud_offload.providers as providers_module

    for attribute in ("_CONNECTORS", "_CANONICAL_NAMES", "_METADATA"):
        snapshot = dict(getattr(providers_module, attribute))
        monkeypatch.setattr(providers_module, attribute, snapshot)

    register_connector(
        "acme",
        lambda config: PluginConnector(),
        replace=True,
        display_name="Acme GPU",
        kind="plugin",
        settings_schema=[{"key": "region", "type": "string"}],
    )

    config = profile_config(tmp_path)
    # The user's order does not mention the plugin at all.
    config.provider_order = ["runpod"]
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)

    payload = TestClient(server.app).get("/api/providers").json()
    names = {entry["provider"] for entry in payload["providers"]}

    assert payload["default_provider"] == "runpod"
    assert {"runpod", "vast.ai", "acme"} <= names
    acme = next(entry for entry in payload["providers"] if entry["provider"] == "acme")
    assert acme["kind"] == "plugin"
    assert acme["display_name"] == "Acme GPU"
    assert acme["in_provider_order"] is False
    assert acme["settings_schema"] == [{"key": "region", "type": "string"}]


def test_generic_credential_resolution_for_plugin_provider(monkeypatch, tmp_path):
    from cloud_offload.config import CloudConfig, provider_env_var

    assert provider_env_var("acme") == "CLOUD_OFFLOAD_ACME_API_KEY"
    assert provider_env_var("vast.ai") == "CLOUD_OFFLOAD_VAST_AI_API_KEY"

    config = CloudConfig(queue_db_path=str(tmp_path / "q.db"))
    config.provider_credentials = {}
    monkeypatch.delenv("CLOUD_OFFLOAD_ACME_API_KEY", raising=False)
    assert config.api_key_for("acme") == ""

    # Env var wins.
    monkeypatch.setenv("CLOUD_OFFLOAD_ACME_API_KEY", "env-key")
    assert config.api_key_for("acme") == "env-key"

    # Credential file is the fallback.
    monkeypatch.delenv("CLOUD_OFFLOAD_ACME_API_KEY", raising=False)
    config.provider_credentials = {"acme": "file-key"}
    assert config.api_key_for("acme") == "file-key"


def test_credentials_route_stores_secret_outside_config(monkeypatch, tmp_path):
    from cloud_offload import config as config_module

    monkeypatch.setattr(config_module, "CREDENTIALS_FILE", tmp_path / "credentials.json")
    monkeypatch.delenv("CLOUD_OFFLOAD_RUNPOD_API_KEY", raising=False)
    client = TestClient(server.app)

    response = client.post("/api/providers/runpod/credentials", json={"api_key": "secret-key"})
    assert response.status_code == 200
    assert response.json() == {"provider": "runpod", "configured": True}
    # Stored in the credential file, never echoed back.
    assert config_module.load_provider_credentials()["runpod"] == "secret-key"
    assert "secret-key" not in response.text

    # Unknown connectors are rejected rather than silently stored.
    assert client.post("/api/providers/nope/credentials", json={"api_key": "x"}).status_code == 404


def test_config_route_still_refuses_provider_credentials():
    response = TestClient(server.app).post(
        "/api/config", json={"provider_credentials": {"runpod": "leak"}}
    )
    assert response.status_code == 400
    assert "provider_credentials" in response.json()["error"]["message"]


def test_provider_settings_route_persists_and_rejects_secrets(monkeypatch, tmp_path):
    from cloud_offload import config as config_module

    monkeypatch.setattr(config_module, "CONFIG_DIR", tmp_path)
    client = TestClient(server.app)

    ok = client.post("/api/providers/runpod/settings", json={"settings": {"cloud_type": "COMMUNITY"}})
    assert ok.status_code == 200
    assert ok.json()["settings"]["cloud_type"] == "COMMUNITY"
    saved = json.loads((tmp_path / "config.json").read_text())
    assert saved["cloud"]["connector_options"]["runpod"]["cloud_type"] == "COMMUNITY"

    # Secrets must go through the credentials route, not the settings blob.
    leak = client.post("/api/providers/runpod/settings", json={"settings": {"runpod_api_key": "x"}})
    assert leak.status_code == 400


def test_provider_test_route_reports_failure_without_credentials(monkeypatch, tmp_path):
    config = profile_config(tmp_path)
    config.runpod_api_key = ""
    config.provider_credentials = {}
    monkeypatch.delenv("CLOUD_OFFLOAD_RUNPOD_API_KEY", raising=False)
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)

    payload = TestClient(server.app).post("/api/providers/runpod/test").json()

    assert payload == {"provider": "runpod", "ok": False, "error": "No credentials configured"}
