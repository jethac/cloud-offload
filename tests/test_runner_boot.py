"""Tests for a runner's startup: what it says, and what it says when it fails.

The failure these are written against cost three pods for one job. A runner that
could not bring ComfyUI up inside a fixed 180-second window raised SystemExit
into ``set -euo pipefail``; the container died with its logs, the coordinator
learned nothing, the job stayed queued, and the dispatcher rented the next pod
into the same wall.

So: a runner announces itself before it is ready, waits on whether ComfyUI is
alive rather than on a clock, and reports home before it dies. What it must not
do is claim a job it cannot execute — that would convert a clean pre-execution
failure into a paid one that also spends a retry.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.runner import (
    DEFAULT_READY_TIMEOUT_SECONDS,
    RunnerStartupError,
    log_tail,
    ready_timeout_seconds,
    report_startup_failure,
    run_boot,
    run_ready,
    wait_for_comfyui,
)
from cloud_offload.worker import MAX_CAPTURED_OUTPUT, Worker, resolve_worker_id


# The window this replaces. Anything at or below it is a regression.
OLD_FIXED_WINDOW_SECONDS = 180


def runner_config(tmp_path, **overrides):
    return CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        worker_profile="comfyui",
        **overrides,
    )


def runner_worker(tmp_path, custom_nodes=None, worker_id="worker-boot"):
    """A worker assembled the way the staging tests assemble one: no cloud, no
    GPU probe, no signal handlers — only the state a boot phase actually uses."""
    config = runner_config(tmp_path)
    worker = Worker.__new__(Worker)
    worker.config = config
    worker.worker_id = worker_id
    worker.queue = JobQueue(config.queue_db_path)
    worker.runtime_profile = "comfyui"
    worker.capabilities = ["comfyui-partition-v1"]
    worker.custom_nodes = custom_nodes or []
    worker._custom_nodes_staged = False
    return worker


class Clock:
    """A monotonic clock that only moves when something sleeps on it."""

    def __init__(self):
        self.now = 0.0
        self.slept = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


# ---------------------------------------------------------------------------
# Waiting: liveness, not a clock
# ---------------------------------------------------------------------------


def test_a_comfyui_that_exits_immediately_fails_without_waiting():
    clock = Clock()

    with pytest.raises(RunnerStartupError, match="exited before it became ready"):
        wait_for_comfyui(
            is_alive=lambda: False,
            probe=lambda: False,
            clock=clock,
            sleep=clock.sleep,
            timeout_seconds=DEFAULT_READY_TIMEOUT_SECONDS,
        )

    # A process that has exited is not slow. Nothing is gained by outliving it.
    assert clock.slept == []


def test_a_slow_but_living_comfyui_is_waited_out_past_the_old_window():
    clock = Clock()
    ready_at = 600.0

    wait_for_comfyui(
        is_alive=lambda: True,
        probe=lambda: clock.now >= ready_at,
        clock=clock,
        sleep=clock.sleep,
        timeout_seconds=DEFAULT_READY_TIMEOUT_SECONDS,
    )

    assert clock.now >= ready_at
    assert clock.now > OLD_FIXED_WINDOW_SECONDS


def test_a_living_but_wedged_comfyui_gives_up_at_the_cap():
    clock = Clock()

    with pytest.raises(RunnerStartupError, match="had not become ready after 300 seconds"):
        wait_for_comfyui(
            is_alive=lambda: True,
            probe=lambda: False,
            clock=clock,
            sleep=clock.sleep,
            timeout_seconds=300,
        )

    assert clock.now >= 300


def test_a_ready_comfyui_is_not_made_to_wait():
    clock = Clock()

    wait_for_comfyui(
        is_alive=lambda: True,
        probe=lambda: True,
        clock=clock,
        sleep=clock.sleep,
        timeout_seconds=DEFAULT_READY_TIMEOUT_SECONDS,
    )

    assert clock.slept == []


def test_the_runner_keeps_saying_it_is_starting_while_it_waits():
    """Registration ages out after ninety seconds. A runner that registered once
    and then waited twenty minutes would look, to the dispatcher, exactly like a
    pod that never existed — and a second pod would be rented for the same job."""
    clock = Clock()
    beats = []

    wait_for_comfyui(
        is_alive=lambda: True,
        probe=lambda: clock.now >= 400,
        heartbeat=lambda: beats.append(clock.now),
        heartbeat_seconds=30,
        clock=clock,
        sleep=clock.sleep,
        timeout_seconds=DEFAULT_READY_TIMEOUT_SECONDS,
    )

    assert len(beats) >= 400 // 90


def test_the_default_cap_is_far_above_the_window_it_replaces():
    assert DEFAULT_READY_TIMEOUT_SECONDS > OLD_FIXED_WINDOW_SECONDS * 5
    assert ready_timeout_seconds() == DEFAULT_READY_TIMEOUT_SECONDS


def test_the_cap_is_configurable_and_refuses_nonsense(monkeypatch):
    monkeypatch.setenv("CLOUD_OFFLOAD_COMFYUI_READY_TIMEOUT", "45")
    assert ready_timeout_seconds() == 45

    monkeypatch.setenv("CLOUD_OFFLOAD_COMFYUI_READY_TIMEOUT", "soon")
    with pytest.raises(RuntimeError, match="not a number of seconds"):
        ready_timeout_seconds()

    monkeypatch.setenv("CLOUD_OFFLOAD_COMFYUI_READY_TIMEOUT", "0")
    with pytest.raises(RuntimeError, match="must be positive"):
        ready_timeout_seconds()


# ---------------------------------------------------------------------------
# Reporting home
# ---------------------------------------------------------------------------


def test_the_log_tail_is_the_end_of_the_file_and_is_capped(tmp_path):
    log = tmp_path / "comfyui.log"
    log.write_bytes(b"x" * 10_000 + b"\nTraceback: the part that matters\n")

    tail = log_tail(log)

    assert len(tail) == MAX_CAPTURED_OUTPUT
    assert tail.endswith("Traceback: the part that matters\n")


def test_a_missing_log_costs_nothing(tmp_path):
    assert log_tail(tmp_path / "never-written.log") == ""
    assert log_tail(None) == ""


def test_a_startup_failure_lands_on_the_worker_with_the_log_tail(tmp_path):
    config = runner_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    log = tmp_path / "comfyui.log"
    log.write_text("y" * 9_000 + "ImportError: No module named 'diffusers'\n", encoding="utf-8")

    report_startup_failure(
        config, "worker-boot", "ComfyUI exited before it became ready", log, queue=queue
    )

    worker = queue.list_recent_workers()[0]
    assert worker["worker_id"] == "worker-boot"
    assert worker["status"] == "failed"
    assert worker["runtime_profile"] == "comfyui"
    assert "ComfyUI exited before it became ready" in worker["detail"]
    assert "ImportError: No module named 'diffusers'" in worker["detail"]
    assert len(worker["detail"]) < len(log.read_text(encoding="utf-8"))
    # A failed runner is not a worker the dispatcher should expect work from.
    assert queue.list_active_workers() == []


def test_a_coordinator_that_cannot_be_reached_does_not_swallow_the_reason(tmp_path, caplog):
    class DeadQueue:
        def record_worker(self, *args, **kwargs):
            raise ConnectionError("tunnel closed")

    report_startup_failure(
        runner_config(tmp_path), "worker-boot", "ComfyUI never answered", None, DeadQueue()
    )

    assert "ComfyUI never answered" in caplog.text
    assert "tunnel closed" in caplog.text


# ---------------------------------------------------------------------------
# The two boot phases
# ---------------------------------------------------------------------------


def test_the_boot_registers_as_starting_and_claims_nothing(tmp_path):
    worker = runner_worker(tmp_path)
    queued = worker.queue.create(
        "comfyui-partition-v1",
        "input.part",
        provider="runpod",
        params={"runtime_profile": "comfyui"},
        status=JobStatus.QUEUED,
    )

    assert run_boot(worker.config, worker) == 0

    registered = worker.queue.list_active_workers()
    assert [item["status"] for item in registered] == ["starting"]
    assert registered[0]["runtime_profile"] == "comfyui"
    # Readiness is proved by ComfyUI answering, never by a worker asserting it.
    assert worker.queue.get(queued.id).status == JobStatus.QUEUED


def test_a_boot_that_cannot_stage_its_packs_reports_instead_of_vanishing(tmp_path, monkeypatch):
    worker = runner_worker(tmp_path)
    monkeypatch.setattr(
        Worker,
        "stage_node_packs",
        lambda self: (_ for _ in ()).throw(RuntimeError("registry 503")),
    )

    assert run_boot(worker.config, worker) == 1

    failed = worker.queue.list_recent_workers()[0]
    assert failed["status"] == "failed"
    assert "Node pack staging failed: registry 503" in failed["detail"]


def test_a_runner_that_cannot_even_be_configured_still_reports_why(tmp_path, monkeypatch):
    """The launch environment is the one thing that can break before a worker
    exists to report with, so the report is built from configuration instead."""
    monkeypatch.setattr(Worker, "_detect_gpu", staticmethod(lambda: ("", 0.0)))
    monkeypatch.setenv("CLOUD_OFFLOAD_CUSTOM_NODES", '[{"git": "https://x/y.git"')
    config = runner_config(tmp_path)

    assert run_boot(config) == 1

    failed = JobQueue(config.queue_db_path).list_recent_workers()[0]
    assert failed["status"] == "failed"
    assert "Runner configuration failed" in failed["detail"]
    assert "CLOUD_OFFLOAD_CUSTOM_NODES" in failed["detail"]


def test_a_runner_whose_comfyui_dies_reports_and_exits_non_zero(tmp_path):
    worker = runner_worker(tmp_path)
    log = tmp_path / "comfyui.log"
    log.write_text("RuntimeError: CUDA driver initialisation failed\n", encoding="utf-8")
    clock = Clock()

    exit_code = run_ready(
        comfyui_pid=4321,
        log_path=log,
        config=worker.config,
        worker=worker,
        is_alive=lambda: False,
        probe=lambda: False,
        clock=clock,
        sleep=clock.sleep,
        timeout_seconds=DEFAULT_READY_TIMEOUT_SECONDS,
    )

    assert exit_code == 1
    failed = worker.queue.list_recent_workers()[0]
    assert failed["worker_id"] == worker.worker_id
    assert failed["status"] == "failed"
    assert "ComfyUI exited before it became ready" in failed["detail"]
    assert "CUDA driver initialisation failed" in failed["detail"]


def test_a_runner_that_never_answers_reports_the_cap_it_exceeded(tmp_path):
    worker = runner_worker(tmp_path)
    log = tmp_path / "comfyui.log"
    log.write_text("still importing custom nodes\n", encoding="utf-8")
    clock = Clock()

    exit_code = run_ready(
        comfyui_pid=4321,
        log_path=log,
        config=worker.config,
        worker=worker,
        is_alive=lambda: True,
        probe=lambda: False,
        clock=clock,
        sleep=clock.sleep,
        timeout_seconds=600,
    )

    assert exit_code == 1
    failed = worker.queue.list_recent_workers()[0]
    assert "had not become ready after 600 seconds" in failed["detail"]
    assert "still importing custom nodes" in failed["detail"]


def test_a_ready_runner_hands_over_with_nothing_to_report(tmp_path):
    worker = runner_worker(tmp_path)
    clock = Clock()

    exit_code = run_ready(
        comfyui_pid=4321,
        log_path=None,
        config=worker.config,
        worker=worker,
        is_alive=lambda: True,
        probe=lambda: True,
        clock=clock,
        sleep=clock.sleep,
        timeout_seconds=DEFAULT_READY_TIMEOUT_SECONDS,
    )

    assert exit_code == 0
    # Still only "starting": the worker loop takes it to "active" by claiming,
    # which is the one thing readiness is allowed to be inferred from.
    assert [item["status"] for item in worker.queue.list_recent_workers()] == ["starting"]


# ---------------------------------------------------------------------------
# The coordinator's side of the channel
# ---------------------------------------------------------------------------


WORKER_AUTH = {"Authorization": "Bearer worker-secret"}


def worker_client(monkeypatch, tmp_path):
    config = runner_config(tmp_path)
    queue = JobQueue(config.queue_db_path)
    queue.set_worker_token("worker-secret")
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    return TestClient(server.app), queue


def test_the_status_route_records_a_startup_failure_against_the_worker(
    monkeypatch, tmp_path
):
    client, queue = worker_client(monkeypatch, tmp_path)

    response = client.post(
        "/api/workers/status",
        headers=WORKER_AUTH,
        json={
            "worker_id": "worker-boot",
            "provider": "runpod",
            "status": "failed",
            "runtime_profile": "comfyui",
            "detail": "ComfyUI exited before it became ready\n\ntraceback",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"worker_id": "worker-boot", "status": "failed"}
    assert queue.list_recent_workers()[0]["detail"].endswith("traceback")


def test_the_status_route_bounds_what_a_worker_may_post(monkeypatch, tmp_path):
    client, queue = worker_client(monkeypatch, tmp_path)

    client.post(
        "/api/workers/status",
        headers=WORKER_AUTH,
        json={
            "worker_id": "worker-boot",
            "provider": "runpod",
            "status": "failed",
            "detail": "z" * 100_000,
        },
    )

    assert len(queue.list_recent_workers()[0]["detail"]) == server.MAX_WORKER_DETAIL_CHARS


def test_the_status_route_refuses_a_status_it_does_not_know(monkeypatch, tmp_path):
    client, _ = worker_client(monkeypatch, tmp_path)

    response = client.post(
        "/api/workers/status",
        headers=WORKER_AUTH,
        json={"worker_id": "worker-boot", "provider": "runpod", "status": "ready"},
    )

    assert response.status_code == 400
    assert "Unknown worker status" in response.json()["error"]["message"]


def test_the_status_route_needs_the_worker_credential(monkeypatch, tmp_path):
    client, _ = worker_client(monkeypatch, tmp_path)
    body = {"worker_id": "worker-boot", "provider": "runpod", "status": "starting"}

    refused = client.post("/api/workers/status", json=body)
    wrong = client.post(
        "/api/workers/status", headers={"Authorization": "Bearer nope"}, json=body
    )
    accepted = client.post("/api/workers/status", headers=WORKER_AUTH, json=body)

    assert refused.status_code == 401
    assert wrong.status_code == 401
    assert accepted.status_code == 200


def test_a_failed_runner_is_reported_in_the_service_status(monkeypatch, tmp_path):
    client, queue = worker_client(monkeypatch, tmp_path)
    queue.record_worker(
        "worker-boot", "runpod", status="failed", detail="ComfyUI never answered"
    )

    payload = client.get("/api/status").json()

    assert payload["active_workers"] == 0
    assert payload["failed_workers"][0]["detail"] == "ComfyUI never answered"


# ---------------------------------------------------------------------------
# The entrypoint's ordering, which is the whole of defect one
# ---------------------------------------------------------------------------


def test_the_entrypoint_stages_node_packs_before_it_starts_comfyui():
    """ComfyUI builds its node registry while it imports. A pack installed after
    the server is up is invisible to it, which is how a runner that had staged
    both declared packs still answered "Node 'LayerScope Decompose' not found"."""
    script = (
        Path(__file__).resolve().parents[1]
        / "deploy"
        / "runtime-profiles"
        / "comfyui"
        / "entrypoint.sh"
    ).read_text(encoding="utf-8")

    order = [
        script.index("cloud-offload runner-boot"),
        script.index("python /opt/ComfyUI/main.py"),
        script.index("cloud-offload runner-ready"),
        script.index("cloud-offload worker"),
    ]

    assert order == sorted(order)
    # The behaviour that must survive the rewrite.
    assert "--disable-auto-launch" in script and "--disable-metadata" in script
    assert 'trap cleanup EXIT INT TERM' in script
    assert "CLOUD_OFFLOAD_WORKER_ID" in script


def test_the_entrypoint_uses_the_environment_prefix_site_packages():
    script = (
        Path(__file__).resolve().parents[1]
        / "deploy"
        / "runtime-profiles"
        / "comfyui"
        / "entrypoint.sh"
    ).read_text(encoding="utf-8")

    assert (
        'export PYTHONPATH="${CLOUD_OFFLOAD_ENV_ROOT:-/opt/cloud-offload/environment}'
        '/lib/python3.11/site-packages${PYTHONPATH:+:${PYTHONPATH}}"'
    ) in script


def test_the_runner_image_keeps_large_cuda_payloads_in_parallel_pull_layers():
    dockerfile = (
        Path(__file__).resolve().parents[1]
        / "deploy"
        / "runtime-profiles"
        / "comfyui"
        / "Dockerfile"
    ).read_text(encoding="utf-8")

    assert dockerfile.startswith("# syntax=docker/dockerfile:1.19")
    assert " AS pytorch-runtime" in dockerfile
    assert "FROM ubuntu:22.04@sha256:" in dockerfile
    assert "--exclude=lib/python3.11/site-packages/torch" in dockerfile
    assert "--exclude=lib/python3.11/site-packages/nvidia" in dockerfile

    # These are the large payloads in the pinned PyTorch runtime. Each COPY is
    # a separate registry blob, so a worker can download them in parallel.
    payloads = (
        "torch",
        "nvidia/cublas",
        "nvidia/cudnn",
        "nvidia/cufft",
        "nvidia/curand",
        "nvidia/cusolver",
        "nvidia/cusparse",
        "nvidia/nccl",
    )
    for payload in payloads:
        source = f"/opt/conda/lib/python3.11/site-packages/{payload}/"
        assert f"COPY --from=pytorch-runtime {source} {source}" in dockerfile


def test_both_boot_phases_answer_to_the_same_worker_id(monkeypatch):
    monkeypatch.setenv("CLOUD_OFFLOAD_WORKER_ID", "worker-abc123")

    assert resolve_worker_id() == "worker-abc123"
    assert resolve_worker_id("worker-explicit") == "worker-explicit"

    monkeypatch.delenv("CLOUD_OFFLOAD_WORKER_ID")
    assert resolve_worker_id().startswith("worker-")
