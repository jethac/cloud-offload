import json

from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.support_bundle import build_support_bundle


def test_support_bundle_keeps_evidence_and_removes_payloads_and_secrets(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create(
        "comfyui-partition-v1",
        "inline://private",
        provider="runpod",
        params={
            "runtime_profile": "comfyui-omni",
            "api_token": "hf_private-token",
        },
        request={
            "kind": "comfyui-partition",
            "partition": {
                "schema": "comfy.partition.job.v1",
                "partition_id": "part-1",
                "runner": {"profile": "comfyui-omni"},
                "workflow": {
                    "1": {
                        "class_type": "LoadImage",
                        "inputs": {"image": "private-family-photo.png"},
                    },
                    "2": {
                        "class_type": "KSampler",
                        "inputs": {"prompt": "private medical prompt"},
                    },
                },
                "inputs": [{"key": "input_0000", "value": "private-value"}],
                "outputs": [{"key": "output_0000"}],
            },
            "input_artifacts": {"input_0000": "a" * 64},
            "assets": [
                {
                    "filename": "model.safetensors",
                    "sha256": "b" * 64,
                    "size": 1234,
                    "download_url": "https://user:password@example.invalid/model?sig=secret",
                }
            ],
            "timeout_seconds": 90,
        },
        status=JobStatus.QUEUED,
    )
    queue.append_event(
        job.id,
        {
            "type": "weight_download_progress",
            "phase": "dependency_preparation",
            "bytes": 512,
            "total_bytes": 1234,
            "authorization": "Bearer worker-secret",
            "data_base64": "cHJpdmF0ZS1pbWFnZS1ieXRlcw==",
            "source_url": "https://user:password@example.invalid/model?sig=secret#private",
            "message": "using hf_inline-secret",
        },
    )

    bundle = build_support_bundle(queue, queue.get(job.id))
    encoded = json.dumps(bundle, sort_keys=True)

    assert bundle["schema"] == "cloud-offload.support-bundle.v1"
    assert bundle["job"]["request"]["partition"]["node_types"] == {
        "KSampler": 1,
        "LoadImage": 1,
    }
    assert bundle["job"]["request"]["assets"] == [
        {"filename": "model.safetensors", "sha256": "b" * 64, "size": 1234}
    ]
    transfer = next(
        item for item in bundle["events"] if item["type"] == "weight_download_progress"
    )
    assert transfer["metrics"] == {"bytes": 512, "total_bytes": 1234}
    for private_value in (
        "hf_private-token",
        "worker-secret",
        "hf_inline-secret",
        "private-family-photo.png",
        "private medical prompt",
        "private-value",
        "cHJpdmF0ZS1pbWFnZS1ieXRlcw==",
        "password",
        "sig=secret",
    ):
        assert private_value not in encoded


def test_support_bundle_marks_a_bounded_event_history_as_truncated(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    job = queue.create("comfyui-workflow", "inline://request")
    for value in range(4):
        queue.append_event(job.id, {"type": "progress", "value": value})

    from cloud_offload import support_bundle

    events, truncated = support_bundle._all_events(queue, job.id, maximum=3)

    assert len(events) == 3
    assert truncated is True
