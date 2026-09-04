import base64
import hashlib

from fastapi.testclient import TestClient

from cloud_offload import preflight, server
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.comfyui import ComfyUIWorkflowExecutor
from cloud_offload.preflight import build_workflow_preflight
from cloud_offload.queue import JobQueue
from cloud_offload.storage import LocalStorage, partition_artifact_key
from cloud_offload.worker import Worker
from cloud_offload.workflow_capsule import (
    normalize_workflow_capsule,
    workflow_capsule_digest,
)
from tests.test_preflight import ReadOnlyConnector, config_for_preflight


def capsule(*, inputs=None, outputs=None, dynamic=True):
    return {
        "schema": "comfy.workflow.capsule.v1",
        "workflow": {
            "1": {
                "class_type": "LoadImage",
                "inputs": {"image": "reference.png"},
            },
            "9": {"class_type": "SaveGLB", "inputs": {"mesh": ["1", 0]}},
        },
        "runner": {
            "profile": "comfyui",
            "gpu_type": "any",
            "min_gpu_ram_gb": 40,
        },
        "residency": "cloud",
        "assets": [],
        "node_packs": [],
        "inputs": inputs or [],
        "outputs": outputs or [],
        "dynamic_behavior": {
            "declared": dynamic,
            "requirements": [],
        },
    }


def workflow_config(tmp_path):
    config = config_for_preflight(tmp_path)
    config.worker_profiles["comfyui"]["models"].append("comfyui-workflow")
    config.__post_init__()
    return config


def test_capsule_digest_is_stable_for_the_same_normalized_closure():
    first = capsule(
        inputs=[
            {"name": "z.png", "kind": "image"},
            {"name": "a.png", "kind": "image"},
        ]
    )
    second = capsule(
        inputs=[
            {"kind": "image", "name": "a.png"},
            {"kind": "image", "name": "z.png"},
        ]
    )

    assert workflow_capsule_digest(first) == workflow_capsule_digest(second)
    assert normalize_workflow_capsule(first)["inputs"][0]["name"] == "a.png"


def test_workflow_preflight_blocks_a_missing_input_and_shows_dynamic_uncertainty(
    tmp_path,
):
    config = workflow_config(tmp_path)
    report = build_workflow_preflight(
        config=config,
        capsule=capsule(
            inputs=[{"name": "reference.png", "kind": "image"}], dynamic=False
        ),
        input_artifacts={},
        provider="runpod",
        storage=LocalStorage(config.storage_path),
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda *args: ReadOnlyConnector(),
    )

    assert report["workload_type"] == "workflow_capsule"
    assert report["status"] == "blocked"
    assert report["capsule_digest"].startswith("sha256:")
    assert "workflow_input_missing" in {item["code"] for item in report["blockers"]}
    assert "workflow_dynamic_behavior_undeclared" in {
        item["code"] for item in report["unknowns"]
    }
    assert report["confirmation"]["required"] is False


def test_workflow_preflight_and_confirmed_submission_bind_the_capsule(
    monkeypatch, tmp_path
):
    config = workflow_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    connector = ReadOnlyConnector()
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    monkeypatch.setattr(preflight, "create_connector", lambda *args: connector)
    client = TestClient(server.app)
    workflow_capsule = capsule()

    checked = client.post(
        "/api/preflight",
        json={"capsule": workflow_capsule, "provider": "runpod"},
    )
    assert checked.status_code == 200
    report = checked.json()
    assert report["status"] == "ready"

    created = client.post(
        "/api/workflows",
        json={
            "capsule": workflow_capsule,
            "provider": "runpod",
            "preflight_id": report["preflight_id"],
            "manifest_digest": report["manifest_digest"],
            "candidate_id": report["recommendation"]["candidate_id"],
            "confirmation_action": "start_now",
        },
    )

    assert created.status_code == 202
    job = queue.get(created.json()["job_id"])
    assert job.request["kind"] == "comfyui-workflow-capsule"
    assert job.params["preflight"]["capsule_digest"] == report["capsule_digest"]
    assert job.params["container_disk_gb"] >= 1
    assert connector.mutations == []


def test_workflow_preflight_accepts_canonical_sha256_input_binding(tmp_path):
    """Workflow boundary digests use the protocol prefix at storage lookup."""
    config = workflow_config(tmp_path)
    storage = LocalStorage(config.storage_path)
    digest = "a" * 64
    source = tmp_path / "reference.png"
    source.write_bytes(b"workflow-input")
    storage.upload(source, partition_artifact_key(digest))

    report = build_workflow_preflight(
        config=config,
        capsule=capsule(inputs=[{"name": "reference.png", "kind": "image"}]),
        input_artifacts={"reference.png": "sha256:" + digest},
        provider="runpod",
        storage=storage,
        cache_registry=CacheRegistry(config.queue_db_path),
        connector_factory=lambda *args: ReadOnlyConnector(),
    )

    assert report["status"] == "ready"
    assert report["blockers"] == []


def test_capsule_worker_uses_cooperative_cancel_and_publishes_artifacts(
    monkeypatch, tmp_path
):
    queue = JobQueue(tmp_path / "queue.db")
    worker = Worker.__new__(Worker)
    worker.queue = queue
    output = b"glb-output"
    uploaded = []

    def execute(self, workflow, **kwargs):
        assert kwargs["cancel_check"]() is False
        return {
            "prompt_id": "prompt-1",
            "uploaded_inputs": {},
            "outputs": {"9": {"3d": [{"filename": "mesh.glb"}]}},
            "images": [],
            "files": [
                {
                    "node_id": "9",
                    "filename": "mesh.glb",
                    "subfolder": "",
                    "mime_type": "model/gltf-binary",
                    "output_kind": "3d",
                    "data": base64.b64encode(output).decode("ascii"),
                }
            ],
        }

    def upload(path):
        content = path.read_bytes()
        uploaded.append(content)
        digest = hashlib.sha256(content).hexdigest()
        return {"artifact_id": digest, "sha256": digest, "size": len(content)}

    monkeypatch.setattr(ComfyUIWorkflowExecutor, "execute", execute)
    worker._upload_partition_artifact = upload
    job = queue.create(
        "comfyui-workflow",
        "artifacts://workflow",
        request={
            "kind": "comfyui-workflow-capsule",
            "capsule": capsule(
                outputs=[{"node_id": "9", "kind": "3d", "required": True}]
            ),
            "input_artifacts": {},
        },
    )

    result = worker._run_comfyui_workflow(job)

    assert uploaded == [output]
    assert result["schema"] == "comfy.workflow.result.v1"
    assert result["artifacts"][0]["output_kind"] == "3d"
    assert "data" not in str(result)
