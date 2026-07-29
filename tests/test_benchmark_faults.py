import ctypes
import io
import json

import pytest

import cloud_offload.benchmark_faults as benchmark_faults
from cloud_offload.benchmark_faults import (
    _benchmark_context,
    _corruption_profile_fingerprint,
    _process_exists,
    _windows_process_exists,
    cleanup_corruption,
    corruption_canary_asset,
    inject_storage_unavailable,
    observe_corruption,
    prepare_corruption,
    restart_coordinator,
    run_fault,
)
from cloud_offload.prepared_state import ManifestSigner, blob_key


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
                        "manifest_id": "sha256:" + "f" * 64,
                        "profile_fingerprint": "sha256:" + "d" * 64,
                        "created_at": "2026-01-01T00:00:00Z",
                        "producer": {"image_digest": "sha256:" + "e" * 64},
                        "artifacts": [
                            {
                                "digest": "sha256:" + "a" * 64,
                                "kind": "model-weight",
                                "size": 200,
                                "storage_key": blob_key("a" * 64),
                                "portability": "portable",
                                "requirements": {},
                                "policy": {
                                    "tenant": "default",
                                    "cacheable": True,
                                    "private": False,
                                },
                            },
                            {
                                "digest": "sha256:" + "b" * 64,
                                "kind": "model-weight",
                                "size": 100,
                                "storage_key": blob_key("b" * 64),
                                "portability": "portable",
                                "requirements": {},
                                "policy": {
                                    "tenant": "default",
                                    "cacheable": True,
                                    "private": False,
                                },
                            },
                        ],
                    }
                ]
            }
        raise AssertionError(path)


class FakeS3Client:
    def __init__(self):
        self.objects = {"indexes/latest": b"original-generation"}
        self.deleted = []
        self.puts = []

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"ContentLength": len(self.objects[Key])}

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, *, Bucket, Key, Body):
        payload = bytes(Body)
        self.puts.append((Key, payload))
        self.objects[Key] = payload

    def delete_object(self, *, Bucket, Key):
        self.deleted.append(Key)
        self.objects.pop(Key, None)

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):
        return {
            "Contents": [
                {"Key": key} for key in sorted(self.objects) if key.startswith(Prefix)
            ],
            "IsTruncated": False,
        }


class FakePreparedStore:
    volume_id = "provider-volume"

    def __init__(self, client):
        self.client = client

    def load_index(self):
        return {
            "schema": "cloud-offload.prepared-state.index.v1",
            "generation": "original-generation",
            "created_at": "2026-01-01T00:00:00Z",
            "manifests": [],
        }


class FakeRegistry:
    def __init__(self):
        self.announced = []
        self.removed = []

    def announce_manifest(self, volume_id, generation, manifest):
        self.announced.append((volume_id, generation, manifest["manifest_id"]))
        return {"artifacts": len(manifest["artifacts"]), "drifted": 0}

    def remove_manifest(self, volume_id, manifest_id, *, inventory_generation=None):
        self.removed.append((volume_id, manifest_id, inventory_generation))
        return {"manifests": 1, "artifacts_restored": 2, "artifacts_removed": 1}


class FakeCoordinatorStorage:
    def __init__(self):
        self.objects = {}

    def exists(self, key):
        return key in self.objects

    def upload(self, path, key):
        self.objects[key] = path.read_bytes()
        return key

    def delete(self, key):
        return self.objects.pop(key, None) is not None


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


def test_corruption_fault_uses_fresh_object_and_restores_inventory():
    client = FakeFaultClient()
    client.prepared["policy"] = "smart"
    s3 = FakeS3Client()
    store = FakePreparedStore(s3)
    registry = FakeRegistry()
    coordinator_storage = FakeCoordinatorStorage()
    scenario = "corruption-scenario"
    canary_nonce = "campaign-123"
    canary = corruption_canary_asset(scenario, nonce=canary_nonce)

    declared = {"a" * 64, "b" * 64, canary["sha256"]}
    prepared = prepare_corruption(
        client,
        scenario,
        declared,
        store_factory=lambda volume: store,
        signer_factory=lambda: ManifestSigner(b"x" * 32),
        registry_factory=lambda: registry,
        coordinator_storage_factory=lambda: coordinator_storage,
        canary_nonce=canary_nonce,
        settle_seconds=0,
    )

    assert prepared == {
        "kind": "corruption",
        "stage": "prepare",
        "fresh_object": True,
        "canary_manifest_published": True,
        "canary_verified": True,
        "artifact_size": canary["size"],
    }
    canary_key = blob_key(canary["sha256"])
    assert s3.objects[canary_key] == b"cloud-offload-benchmark-corrupt"
    assert [body for key, body in s3.puts if key == canary_key] == [
        b"cloud-offload-benchmark-corrupt"
    ]
    coordinator_payload = next(iter(coordinator_storage.objects.values()))
    assert len(coordinator_payload) == canary["size"]
    assert coordinator_payload != b"cloud-offload-benchmark-corrupt"
    assert s3.objects["indexes/latest"] != b"original-generation"
    assert len(registry.announced) == 1

    observed = observe_corruption(
        client,
        "job-corruption",
        scenario,
        declared,
        store_factory=lambda volume: store,
        canary_nonce=canary_nonce,
    )

    assert observed == {
        "kind": "corruption",
        "stage": "observe",
        "quarantine_observed": True,
        "valid_retry_object_restored": True,
        "artifact_size": canary["size"],
    }
    assert len(s3.objects[canary_key]) == canary["size"]

    cleaned = cleanup_corruption(
        client,
        scenario,
        declared,
        store_factory=lambda volume: store,
        registry_factory=lambda: registry,
        coordinator_storage_factory=lambda: coordinator_storage,
        canary_nonce=canary_nonce,
    )
    assert cleaned["changed"] is True
    assert cleaned["original_generation_restored"] is True
    assert cleaned["canary_deleted"] is True
    assert s3.objects["indexes/latest"] == b"original-generation"
    assert canary_key not in s3.objects
    assert len(registry.removed) == 1
    assert coordinator_storage.objects == {}

    assert (
        cleanup_corruption(
            client,
            scenario,
            declared,
            store_factory=lambda volume: store,
            registry_factory=lambda: registry,
            coordinator_storage_factory=lambda: coordinator_storage,
            canary_nonce=canary_nonce,
        )["changed"]
        is False
    )


def test_corruption_prepare_uses_injected_requirement_profile(monkeypatch):
    declared = {"a" * 64, "b" * 64}
    calls = {}
    client = FakeFaultClient()
    monkeypatch.setenv("CLOUD_OFFLOAD_BENCHMARK_FAILURE_KIND", "corruption")
    monkeypatch.setenv("CLOUD_OFFLOAD_BENCHMARK_SCENARIO", "profile-canary")
    monkeypatch.setenv("CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE", "prepare")
    monkeypatch.setenv("CLOUD_OFFLOAD_BENCHMARK_ASSET_DIGESTS", ",".join(declared))
    monkeypatch.setenv("CLOUD_OFFLOAD_BENCHMARK_PROFILE", "profile-v2")
    monkeypatch.setenv("CLOUD_OFFLOAD_BENCHMARK_CANARY_NONCE", "run-123")
    monkeypatch.setattr(benchmark_faults, "CoordinatorFaultClient", lambda: client)

    def fingerprint(profile_name, digests):
        calls["fingerprint"] = (profile_name, digests)
        return "sha256:" + "c" * 64

    def prepare(
        received_client,
        scenario,
        digests,
        *,
        profile_fingerprint=None,
        canary_nonce=None,
    ):
        calls["prepare"] = (
            received_client,
            scenario,
            digests,
            profile_fingerprint,
            canary_nonce,
        )
        return {"stage": "prepare"}

    monkeypatch.setattr(
        benchmark_faults, "_corruption_profile_fingerprint", fingerprint
    )
    monkeypatch.setattr(benchmark_faults, "prepare_corruption", prepare)

    assert run_fault("corruption") == {"stage": "prepare"}
    assert calls["fingerprint"] == ("profile-v2", declared)
    assert calls["prepare"] == (
        client,
        "profile-canary",
        declared,
        "sha256:" + "c" * 64,
        "run-123",
    )


def test_corruption_profile_uses_dispatcher_launch_profile_name(monkeypatch):
    from types import SimpleNamespace

    from cloud_offload.cache_scheduler import resolve_prepared_requirements
    from cloud_offload.profiles import configured_worker_profiles, profile_providing

    config = SimpleNamespace(
        worker_profiles={
            "operator-comfy": {
                "image": "ghcr.io/example/worker@sha256:" + "d" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
            }
        }
    )
    monkeypatch.setattr(
        benchmark_faults.CloudConfig,
        "load",
        lambda resolve_secrets=False: config,
    )
    declared = {"a" * 64, "b" * 64}
    profiles = configured_worker_profiles(config)
    profile = profile_providing(profiles, "comfyui-partition-v1")
    jobs = [
        SimpleNamespace(
            request={
                "assets": [
                    {"sha256": digest, "size": 123} for digest in sorted(declared)
                ]
            }
        )
    ]
    expected = resolve_prepared_requirements("operator-comfy", profile, jobs)[
        "profile_fingerprint"
    ]
    capability_named = resolve_prepared_requirements(
        "comfyui-partition-v1", profile, jobs
    )["profile_fingerprint"]

    actual = _corruption_profile_fingerprint("comfyui-partition-v1", declared)

    assert actual == expected
    assert actual != capability_named
