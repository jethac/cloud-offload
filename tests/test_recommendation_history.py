import sqlite3
from datetime import datetime, timedelta

from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.recommendation_history import (
    RecommendationHistory,
    candidate_class,
    workload_digest,
)


def workflow_partition():
    return {
        "schema": "comfy.partition.job.v1",
        "partition_id": "partition-a",
        "workflow": {
            "10": {
                "class_type": "CloudPartitionInput",
                "inputs": {"artifact_id": "artifact-a"},
            },
            "20": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": "private prompt", "clip": ["10", 0]},
            },
            "30": {
                "class_type": "KSampler",
                "inputs": {
                    "model": ["10", 1],
                    "positive": ["20", 0],
                    "seed": 123,
                    "steps": 20,
                    "cfg": 7.0,
                    "sampler_name": "euler",
                    "scheduler": "normal",
                },
            },
        },
        "runner": {"profile": "comfyui", "min_gpu_ram_gb": 40},
        "assets": [{"sha256": "a" * 64}],
        "node_packs": [{"digest": "sha256:" + "b" * 64}],
    }


def test_workload_digest_ignores_private_and_run_specific_values():
    first = workflow_partition()
    second = workflow_partition()
    second["partition_id"] = "partition-b"
    second["workflow"] = {
        "70": {
            "class_type": "CloudPartitionInput",
            "inputs": {"artifact_id": "artifact-b"},
        },
        "80": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": "a different private prompt", "clip": ["70", 0]},
        },
        "90": {
            "class_type": "KSampler",
            "inputs": {
                "model": ["70", 1],
                "positive": ["80", 0],
                "seed": 987654,
                "steps": 20,
                "cfg": 7.0,
                "sampler_name": "euler",
                "scheduler": "normal",
            },
        },
    }

    assert workload_digest(first) == workload_digest(second)


def test_workload_digest_changes_for_performance_work():
    original = workflow_partition()
    more_steps = workflow_partition()
    more_steps["workflow"]["30"]["inputs"]["steps"] = 40
    different_asset = workflow_partition()
    different_asset["assets"][0]["sha256"] = "c" * 64

    assert workload_digest(original) != workload_digest(more_steps)
    assert workload_digest(original) != workload_digest(different_asset)


def _completed_sample(
    queue: JobQueue,
    *,
    workload: str,
    startup: float,
    preparation: float,
    execution: float,
):
    job = queue.create(
        "comfyui-partition-v1",
        "artifacts://partition",
        params={
            "runtime_profile": "comfyui",
            "min_gpu_ram_gb": 40,
            "gpu_type": "A100 80 GB",
            "preflight": {
                "workload_digest": workload,
                "provider": "runpod",
                "gpu_type": "A100 80 GB",
                "region": "US-MD-1",
                "prepared_volume_id": "volume-a",
            },
        },
        request={"partition": workflow_partition()},
        provider="runpod",
        status=JobStatus.QUEUED,
    )
    created = datetime.fromisoformat(job.created_at)
    monotonic = 1_000_000.0
    events = []
    for phase, offset, observed_offset in (
        ("staging_started", 0.0, startup),
        ("execution_started", preparation * 1000, startup + preparation),
        (
            "result_available",
            (preparation + execution) * 1000,
            startup + preparation + execution,
        ),
    ):
        events.append(
            (
                queue.append_event(
                    job.id,
                    {
                        "type": "phase_timing",
                        "phase": phase,
                        "monotonic_ms": monotonic + offset,
                    },
                )["sequence"],
                created + timedelta(seconds=observed_offset),
            )
        )
    with sqlite3.connect(queue.db_path) as connection:
        for sequence, observed in events:
            connection.execute(
                "UPDATE job_events SET observed_at = ? WHERE sequence = ?",
                (observed.isoformat(), sequence),
            )
    queue.complete_job(job.id, {"outputs": {}})


def test_history_returns_only_safe_matched_aggregates(tmp_path):
    queue = JobQueue(tmp_path / "queue.db")
    workload = workload_digest(workflow_partition())
    _completed_sample(
        queue,
        workload=workload,
        startup=10,
        preparation=100,
        execution=200,
    )
    _completed_sample(
        queue,
        workload=workload,
        startup=20,
        preparation=120,
        execution=180,
    )
    history = RecommendationHistory(queue.db_path)
    matched = history.lookup(
        workload,
        candidate_class(
            provider="runpod",
            gpu_type="A100 80 GB",
            region="US-MD-1",
            prepared=True,
        ),
    )

    assert matched == {
        "schema": "cloud-offload.recommendation-history.v1",
        "sample_count": 2,
        "candidate_class_digest": matched["candidate_class_digest"],
        "startup_seconds": [9.0, 22.0],
        "preparation_seconds": [90.0, 132.0],
        "execution_seconds": [162.0, 220.0],
        "confidence": "medium",
        "basis": "matched_completed_jobs",
    }
    assert "private prompt" not in str(matched)
    assert (
        history.lookup(
            workload,
            candidate_class(
                provider="runpod",
                gpu_type="L40",
                region="US-MD-1",
                prepared=True,
            ),
        )
        is None
    )
