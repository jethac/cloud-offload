import ctypes
import json
from types import SimpleNamespace

import pytest

import cloud_offload.benchmark_faults as benchmark_faults
from cloud_offload.benchmark_faults import (
    _benchmark_context,
    _process_exists,
    _windows_process_exists,
    cleanup_corruption,
    inject_storage_unavailable,
    observe_corruption,
    prepare_corruption,
    restart_coordinator,
)


class FakeFaultClient:
    def __init__(self):
        self.prepared = {
            "enabled": True,
            "confirmed": True,
            "provider": "runpod",
            "policy": "strict",
            "region": "EU-RO-1",
            "cold_fallback": "deny",
            "managed_size_gb": 250,
            "existing_volume_id": "provider-volume",
            "max_monthly_storage_cost": None,
            "tenant": "default",
            "cache_private_assets": False,
            "shadow_admission": True,
        }
        self.posts = []
        self.event_callback = None

    def prepared_storage(self):
        return json.loads(json.dumps(self.prepared))

    def set_prepared_storage(self, prepared):
        self.prepared = json.loads(json.dumps(prepared))
        self.posts.append(json.loads(json.dumps(prepared)))

    def events(self, job_id, after):
        if self.event_callback:
            self.event_callback()
        if str(self.prepared.get("existing_volume_id")).startswith(
            "benchmark-missing-"
        ):
            return [{"sequence": 1, "type": "provisioning_failed"}]
        return [{"sequence": 1, "type": "cache_artifact_quarantined"}]

    def job(self, job_id):
        return {
            "id": job_id,
            "status": "queued",
            "request": {
                "partition": {
                    "assets": [
                        {"sha256": "a" * 64},
                        {"sha256": "b" * 64},
                    ]
                }
            },
        }

    def get(self, path):
        if path == "/api/cache/status":
            return {
                "volumes": [
                    {
                        "id": "registry-volume",
                        "provider_volume_id": "provider-volume",
                        "datacenter_id": "EU-RO-1",
                        "s3_compatible": True,
                    }
                ]
            }
        if path == "/api/cache/manifests":
            return {
                "manifests": [
                    {
                        "volume_id": "registry-volume",
                        "artifacts": [
                            {
                                "digest": "sha256:" + "a" * 64,
                                "kind": "model-weight",
                                "size": 200,
                                "storage_key": "blobs/a",
                            },
                            {
                                "digest": "sha256:" + "b" * 64,
                                "kind": "model-weight",
                                "size": 100,
                                "storage_key": "blobs/b",
                            },
                            {
                                "digest": "sha256:" + "c" * 64,
                                "kind": "model-weight",
                                "size": 1,
                                "storage_key": "blobs/unrelated",
                            },
                        ],
                    }
                ]
            }
        raise AssertionError(path)


class FakeS3Client:
    def __init__(self):
        self.objects = {"blobs/b": 100}
        self.deleted = []

    def copy_object(self, *, Bucket, CopySource, Key):
        self.objects[Key] = self.objects[CopySource["Key"]]

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"ContentLength": self.objects[Key]}

    def put_object(self, *, Bucket, Key, Body):
        self.objects[Key] = len(Body)

    def delete_object(self, *, Bucket, Key):
        self.deleted.append(Key)
        self.objects.pop(Key, None)


class FakeKernel32:
    def __init__(self, *, handle=42, exit_code=259, query_succeeds=True):
        self.handle = handle
        self.exit_code = exit_code
        self.query_succeeds = query_succeeds
        self.closed = []

    def OpenProcess(self, access, inherit, pid):
        return self.handle

    def GetExitCodeProcess(self, handle, output):
        if not self.query_succeeds:
            return 0
        ctypes.cast(output, ctypes.POINTER(ctypes.c_ulong))[0] = self.exit_code
        return 1

    def CloseHandle(self, handle):
        self.closed.append(handle)
        return 1


class FakeRestartClient:
    def __init__(self, pid, status="queued"):
        self.pid = pid
        self.status = status
        self.posts = []

    def get(self, path):
        assert path == "/api/health"
        return {"pid": self.pid}

    def job(self, job_id):
        return {"id": job_id, "status": self.status}

    def post(self, path, body):
        self.posts.append((path, body))
        return {"status": "failed"}


class FakeReplacementProcess:
    pid = 222

    @staticmethod
    def poll():
        return None


def test_fault_hook_refuses_direct_invocation(monkeypatch):
    monkeypatch.delenv("CLOUD_OFFLOAD_BENCHMARK_FAILURE_KIND", raising=False)
    monkeypatch.delenv("CLOUD_OFFLOAD_BENCHMARK_JOB_ID", raising=False)
    monkeypatch.delenv("CLOUD_OFFLOAD_BENCHMARK_SCENARIO", raising=False)

    with pytest.raises(RuntimeError, match="only inside"):
        _benchmark_context("storage")


def test_windows_process_probe_reports_only_live_processes_and_closes_handles():
    live = FakeKernel32(exit_code=259)
    exited = FakeKernel32(exit_code=0)

    assert _windows_process_exists(123, kernel32=live) is True
    assert _windows_process_exists(123, kernel32=exited) is False
    assert live.closed == [42]
    assert exited.closed == [42]


def test_windows_process_probe_handles_absent_and_failed_queries():
    assert _windows_process_exists(123, kernel32=FakeKernel32(handle=0)) is False
    failed = FakeKernel32(query_succeeds=False)
    assert _windows_process_exists(123, kernel32=failed) is False
    assert failed.closed == [42]


def test_process_exists_uses_native_windows_probe(monkeypatch):
    monkeypatch.setattr(benchmark_faults.os, "name", "nt")
    monkeypatch.setattr(
        benchmark_faults, "_windows_process_exists", lambda pid: pid == 123
    )

    assert _process_exists(123) is True
    assert _process_exists(456) is False


def test_restart_replays_and_cancels_through_replacement_coordinator(
    monkeypatch, tmp_path
):
    old = FakeRestartClient(111)
    replacement = FakeRestartClient(222, status="running")
    service_reads = iter(
        [
            {
                "url": "http://127.0.0.1:11435",
                "host": "127.0.0.1",
                "port": 11435,
                "pid": 111,
            },
            {
                "url": "http://127.0.0.1:11435",
                "host": "127.0.0.1",
                "port": 11435,
                "pid": 222,
            },
        ]
    )
    process_states = iter([True, False, False])
    launches = []
    monkeypatch.setattr(
        benchmark_faults,
        "read_service_info",
        lambda require_healthy=True: next(service_reads),
    )
    monkeypatch.setattr(
        benchmark_faults, "_process_exists", lambda pid: next(process_states)
    )
    monkeypatch.setattr(benchmark_faults.os, "kill", lambda pid, signal: None)
    monkeypatch.setattr(benchmark_faults, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(
        benchmark_faults,
        "CoordinatorFaultClient",
        lambda service: replacement,
    )

    def launch(*args, **kwargs):
        launches.append((args, kwargs))
        return FakeReplacementProcess()

    monkeypatch.setattr(benchmark_faults.subprocess, "Popen", launch)

    receipt = restart_coordinator(old, "job-restart")

    assert receipt == {
        "kind": "restart",
        "old_process_stopped": True,
        "replacement_healthy": True,
        "job_replay_available": True,
        "replayed_status": "running",
        "cancellation_recorded": True,
    }
    assert replacement.posts == [("/api/jobs/job-restart/cancel", {})]
    assert len(launches) == 1


def test_storage_fault_is_observed_without_mutating_provider_storage():
    client = FakeFaultClient()
    original = client.prepared_storage()

    receipt = inject_storage_unavailable(client, "job-storage")

    assert receipt == {
        "kind": "storage",
        "observed_event": "provisioning_failed",
        "provider_storage_mutated": False,
        "config_restored": True,
    }
    assert client.prepared == original
    assert client.posts[0]["existing_volume_id"].startswith("benchmark-missing-")
    assert client.posts[-1] == original


def test_corruption_fault_chooses_smallest_required_object_and_restores_backup():
    client = FakeFaultClient()
    client.prepared["policy"] = "smart"
    s3 = FakeS3Client()
    store = SimpleNamespace(volume_id="provider-volume", client=s3)

    declared = {"a" * 64, "b" * 64}
    prepared = prepare_corruption(
        client,
        "corruption-scenario",
        declared,
        store_factory=lambda volume: store,
        settle_seconds=0,
    )

    assert prepared == {
        "kind": "corruption",
        "stage": "prepare",
        "backup_verified": True,
        "canary_verified": True,
        "artifact_size": 100,
    }
    assert s3.objects["blobs/b"] != 100

    observed = observe_corruption(
        client,
        "job-corruption",
        "corruption-scenario",
        declared,
        store_factory=lambda volume: store,
    )

    assert observed == {
        "kind": "corruption",
        "stage": "observe",
        "quarantine_observed": True,
        "canonical_size_restored": True,
        "backup_deleted": True,
        "artifact_size": 100,
    }
    assert s3.objects["blobs/b"] == 100
    assert len(s3.deleted) == 1
    assert s3.deleted[0].startswith("staging/benchmark-corruption/")

    assert (
        cleanup_corruption(
            client,
            "corruption-scenario",
            declared,
            store_factory=lambda volume: store,
        )["changed"]
        is False
    )
