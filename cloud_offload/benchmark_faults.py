"""Reviewed, reversible production canaries for the benchmark harness.

These commands are deliberately unavailable as ordinary product operations.
They require the environment injected by ``BenchmarkRunner.run_hook`` and are
still gated by the campaign's explicit ``--allow-hooks`` acknowledgement.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import requests

from cloud_offload.config import CONFIG_DIR, CloudConfig
from cloud_offload.prepared_state import RunPodS3PreparedStore, normalize_digest
from cloud_offload.providers import create_connector
from cloud_offload.service_config import (
    ServiceConfigError,
    discover_service_info,
    is_local_host,
    read_service_info,
)


FAULT_KINDS = {"storage", "corruption", "restart"}


def _benchmark_context(expected_kind: str) -> tuple[str, str, str]:
    kind = os.environ.get("CLOUD_OFFLOAD_BENCHMARK_FAILURE_KIND", "").strip()
    job_id = os.environ.get("CLOUD_OFFLOAD_BENCHMARK_JOB_ID", "").strip()
    scenario = os.environ.get("CLOUD_OFFLOAD_BENCHMARK_SCENARIO", "").strip()
    stage = (
        os.environ.get("CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE", "observe").strip().lower()
    )
    if (
        kind != expected_kind
        or not scenario
        or stage not in {"prepare", "observe", "cleanup"}
        or (stage == "observe" and not job_id)
    ):
        raise RuntimeError(
            "Benchmark fault canaries run only inside a matching benchmark hook"
        )
    return job_id, scenario, stage


class CoordinatorFaultClient:
    def __init__(self, service: dict[str, Any] | None = None):
        self.service = service or discover_service_info(require_healthy=True)
        self.base_url = str(self.service["url"]).rstrip("/")
        self.session = requests.Session()
        if self.service.get("token"):
            self.session.headers["Authorization"] = f"Bearer {self.service['token']}"

    def get(self, path: str) -> Any:
        response = self.session.get(f"{self.base_url}{path}", timeout=30)
        response.raise_for_status()
        return response.json()

    def post(self, path: str, body: dict[str, Any]) -> Any:
        response = self.session.post(f"{self.base_url}{path}", json=body, timeout=30)
        response.raise_for_status()
        return response.json()

    def prepared_storage(self) -> dict[str, Any]:
        prepared = self.get("/api/config").get("prepared_storage")
        if not isinstance(prepared, dict):
            raise RuntimeError("Coordinator returned no prepared-storage config")
        return json.loads(json.dumps(prepared, allow_nan=False))

    def set_prepared_storage(self, prepared: dict[str, Any]) -> None:
        self.post("/api/config", {"prepared_storage": prepared})

    def job(self, job_id: str) -> dict[str, Any]:
        value = self.get(f"/api/jobs/{job_id}")
        if not isinstance(value, dict):
            raise RuntimeError("Coordinator returned no benchmark job")
        return value

    def events(self, job_id: str, after: int) -> list[dict[str, Any]]:
        payload = self.get(f"/api/jobs/{job_id}/events?after={after}&limit=1000")
        return list(payload.get("events") or [])


def _wait_for_event(
    client: CoordinatorFaultClient,
    job_id: str,
    wanted: set[str],
    *,
    timeout_seconds: float,
    forbidden: set[str] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    deadline = time.monotonic() + timeout_seconds
    cursor = 0
    forbidden = forbidden or set()
    while time.monotonic() < deadline:
        events = client.events(job_id, cursor)
        if events:
            cursor = max(int(item.get("sequence") or 0) for item in events)
        event_types = {str(item.get("type") or "") for item in events}
        unexpected = sorted(event_types & forbidden)
        if unexpected:
            raise RuntimeError(
                "Fault canary lost its pre-mutation race: " + ", ".join(unexpected)
            )
        matched = sorted(event_types & wanted)
        if matched:
            return matched[0]
        status = str(client.job(job_id).get("status") or "")
        if status in {"completed", "failed", "dead_letter"}:
            raise RuntimeError(
                f"Benchmark job became {status} before the fault was observed"
            )
        sleep(1)
    raise TimeoutError("Timed out waiting for benchmark fault evidence")


def inject_storage_unavailable(
    client: CoordinatorFaultClient, job_id: str
) -> dict[str, Any]:
    """Temporarily point strict placement at a nonexistent volume.

    No provider storage is created, detached, or deleted. The full scenario
    config is restored before this hook exits, and a concurrent edit is never
    overwritten.
    """

    original = client.prepared_storage()
    if not original.get("enabled") or original.get("policy") not in {
        "strict",
        "pinned",
    }:
        raise RuntimeError(
            "Storage-unavailable canary requires strict prepared storage"
        )
    if not original.get("existing_volume_id"):
        raise RuntimeError("Storage-unavailable canary requires a bound volume")
    target = json.loads(json.dumps(original, allow_nan=False))
    target["existing_volume_id"] = f"benchmark-missing-{job_id[:12]}"
    applied = False
    observed = None
    try:
        client.set_prepared_storage(target)
        applied = True
        if client.prepared_storage() != target:
            raise RuntimeError("Coordinator did not apply storage fault config")
        observed = _wait_for_event(
            client,
            job_id,
            {"provisioning_failed"},
            forbidden={"provisioning_started", "runner_starting"},
            timeout_seconds=90,
        )
    finally:
        if applied:
            current = client.prepared_storage()
            if current == target:
                client.set_prepared_storage(original)
            elif current != original:
                raise RuntimeError(
                    "Prepared-storage config changed during fault; refusing to overwrite it"
                )
            if client.prepared_storage() != original:
                raise RuntimeError("Storage fault did not restore scenario config")
    return {
        "kind": "storage",
        "observed_event": observed,
        "provider_storage_mutated": False,
        "config_restored": True,
    }


def _corruption_target(
    client: CoordinatorFaultClient,
    job_id: str | None = None,
    *,
    declared_digests: set[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    prepared = client.prepared_storage()
    provider_volume_id = str(prepared.get("existing_volume_id") or "")
    if not prepared.get("enabled") or not provider_volume_id:
        raise RuntimeError("Corruption canary requires enabled, bound prepared storage")
    volumes = list(client.get("/api/cache/status").get("volumes") or [])
    volume = next(
        (
            item
            for item in volumes
            if str(item.get("provider_volume_id") or "") == provider_volume_id
        ),
        None,
    )
    if not volume or not volume.get("s3_compatible"):
        raise RuntimeError("Bound prepared volume has no S3 canary path")

    declared = {
        "sha256:" + normalize_digest(item) for item in (declared_digests or set())
    }
    if not declared and job_id:
        job = client.job(job_id)
        declared = {
            "sha256:" + normalize_digest(str(item.get("sha256") or ""))
            for item in ((job.get("request") or {}).get("partition") or {}).get(
                "assets", []
            )
            if item.get("sha256")
        }
    if not declared:
        raise RuntimeError("Benchmark job declares no digest-addressed assets")
    manifests = list(client.get("/api/cache/manifests").get("manifests") or [])
    candidates: dict[str, dict[str, Any]] = {}
    for manifest in manifests:
        if str(manifest.get("volume_id") or "") != str(volume.get("id") or ""):
            continue
        for artifact in manifest.get("artifacts") or []:
            digest = str(artifact.get("digest") or "")
            if digest in declared and artifact.get("kind") == "model-weight":
                candidates[digest] = artifact
    if not candidates:
        raise RuntimeError("No prepared model artifact matches the benchmark job")
    artifact = min(
        candidates.values(),
        key=lambda item: (int(item.get("size") or 0), str(item.get("digest") or "")),
    )
    return volume, artifact


def _prepared_store(volume: dict[str, Any]) -> RunPodS3PreparedStore:
    connector = create_connector("runpod", CloudConfig.load())
    endpoint = connector.s3_endpoint(str(volume["datacenter_id"]))
    if not endpoint:
        raise RuntimeError("RunPod volume datacenter has no S3 endpoint")
    return RunPodS3PreparedStore.from_environment(
        volume_id=str(volume["provider_volume_id"]),
        datacenter_id=str(volume["datacenter_id"]),
        endpoint_url=endpoint,
    )


def _corruption_canary(
    client: CoordinatorFaultClient,
    scenario: str,
    declared_digests: set[str],
    *,
    store_factory: Callable[[dict[str, Any]], RunPodS3PreparedStore],
) -> tuple[RunPodS3PreparedStore, dict[str, Any], str]:
    volume, artifact = _corruption_target(client, declared_digests=declared_digests)
    store = store_factory(volume)
    digest = normalize_digest(str(artifact["digest"]))
    scenario_tag = hashlib.sha256(scenario.encode("utf-8")).hexdigest()[:16]
    backup = f"staging/benchmark-corruption/{scenario_tag}/{digest}.backup"
    return store, artifact, backup


def _object_size(store: RunPodS3PreparedStore, key: str) -> int | None:
    try:
        head = store.client.head_object(Bucket=store.volume_id, Key=key)
    except Exception:
        return None
    return int(head.get("ContentLength") or -1)


def prepare_corruption(
    client: CoordinatorFaultClient,
    scenario: str,
    declared_digests: set[str],
    *,
    store_factory: Callable[[dict[str, Any]], RunPodS3PreparedStore] = _prepared_store,
    settle_seconds: float = 5,
) -> dict[str, Any]:
    """Back up and corrupt a required object before a Pod can be submitted."""

    store, artifact, backup = _corruption_canary(
        client, scenario, declared_digests, store_factory=store_factory
    )
    key = str(artifact["storage_key"])
    expected_size = int(artifact["size"])
    if _object_size(store, key) != expected_size:
        raise RuntimeError("Corruption target is not valid before canary preparation")
    if _object_size(store, backup) is not None:
        raise RuntimeError("A stale corruption canary backup already exists")
    copied = False
    try:
        store.client.copy_object(
            Bucket=store.volume_id,
            CopySource={"Bucket": store.volume_id, "Key": key},
            Key=backup,
        )
        if _object_size(store, backup) != expected_size:
            raise RuntimeError("Corruption canary backup has the wrong size")
        copied = True
        store.client.put_object(
            Bucket=store.volume_id,
            Key=key,
            Body=b"cloud-offload-benchmark-corruption-canary",
        )
        canary_size = _object_size(store, key)
        if canary_size is None or canary_size == expected_size:
            raise RuntimeError("Corruption canary did not replace the canonical object")
        # Give the provider gateway a bounded propagation window before the Pod
        # and its fresh volume mount are created.
        time.sleep(max(0.0, settle_seconds))
        if _object_size(store, key) != canary_size:
            raise RuntimeError("Corruption canary was not stable before submission")
    except Exception:
        if copied:
            store.client.copy_object(
                Bucket=store.volume_id,
                CopySource={"Bucket": store.volume_id, "Key": backup},
                Key=key,
            )
            store.client.delete_object(Bucket=store.volume_id, Key=backup)
        raise
    return {
        "kind": "corruption",
        "stage": "prepare",
        "backup_verified": True,
        "canary_verified": True,
        "artifact_size": expected_size,
    }


def cleanup_corruption(
    client: CoordinatorFaultClient,
    scenario: str,
    declared_digests: set[str],
    *,
    store_factory: Callable[[dict[str, Any]], RunPodS3PreparedStore] = _prepared_store,
) -> dict[str, Any]:
    """Idempotently restore the canonical object and remove the canary backup."""

    store, artifact, backup = _corruption_canary(
        client, scenario, declared_digests, store_factory=store_factory
    )
    key = str(artifact["storage_key"])
    expected_size = int(artifact["size"])
    backup_size = _object_size(store, backup)
    changed = False
    if backup_size is None:
        if _object_size(store, key) != expected_size:
            raise RuntimeError("Corruption backup is absent and canonical is invalid")
        return {
            "kind": "corruption",
            "stage": "cleanup",
            "canonical_size_restored": True,
            "backup_deleted": True,
            "changed": False,
        }
    if backup_size != expected_size:
        raise RuntimeError("Corruption backup has the wrong size during cleanup")
    if _object_size(store, key) != expected_size:
        store.client.copy_object(
            Bucket=store.volume_id,
            CopySource={"Bucket": store.volume_id, "Key": backup},
            Key=key,
        )
        changed = True
    if _object_size(store, key) != expected_size:
        raise RuntimeError("Corruption cleanup did not restore canonical size")
    store.client.delete_object(Bucket=store.volume_id, Key=backup)
    return {
        "kind": "corruption",
        "stage": "cleanup",
        "canonical_size_restored": True,
        "backup_deleted": True,
        "changed": changed,
    }


def observe_corruption(
    client: CoordinatorFaultClient,
    job_id: str,
    scenario: str,
    declared_digests: set[str],
    *,
    store_factory: Callable[[dict[str, Any]], RunPodS3PreparedStore] = _prepared_store,
) -> dict[str, Any]:
    """Require worker quarantine, then recover the pre-submission canary.

    The cleanup path is unconditional, so a timeout, terminal job, or callback
    error cannot strand either corrupted canonical bytes or the backup.
    """

    store, artifact, backup = _corruption_canary(
        client, scenario, declared_digests, store_factory=store_factory
    )
    key = str(artifact["storage_key"])
    expected_size = int(artifact["size"])
    quarantine_observed = False
    if _object_size(store, backup) != expected_size:
        raise RuntimeError("Prepared corruption backup is missing")
    if _object_size(store, key) == expected_size:
        raise RuntimeError("Prepared corruption canary is no longer present")
    try:
        observed = _wait_for_event(
            client,
            job_id,
            {"cache_artifact_quarantined"},
            timeout_seconds=105,
        )
        quarantine_observed = observed == "cache_artifact_quarantined"
    finally:
        cleanup = cleanup_corruption(
            client,
            scenario,
            declared_digests,
            store_factory=store_factory,
        )
    if not quarantine_observed or not cleanup.get("canonical_size_restored"):
        raise RuntimeError("Corruption canary did not quarantine and recover cleanly")
    return {
        "kind": "corruption",
        "stage": "observe",
        "quarantine_observed": True,
        "canonical_size_restored": True,
        "backup_deleted": True,
        "artifact_size": expected_size,
    }


def _windows_process_exists(pid: int, *, kernel32: Any | None = None) -> bool:
    """Query a Windows process without using ``os.kill(pid, 0)``.

    CPython implements ``os.kill`` through Windows termination APIs rather than
    the POSIX existence probe. During process exit, signal 0 can surface as a
    ``SystemError`` and strand the restart canary after it has stopped the old
    coordinator. ``OpenProcess`` plus ``GetExitCodeProcess`` gives the state we
    actually need and never signals the target.
    """

    if pid <= 0:
        return False
    configure = kernel32 is None
    if kernel32 is None:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    if configure:
        kernel32.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        kernel32.OpenProcess.restype = ctypes.c_void_p
        kernel32.GetExitCodeProcess.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_ulong),
        ]
        kernel32.GetExitCodeProcess.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int
    process_query_limited_information = 0x1000
    still_active = 259
    handle = kernel32.OpenProcess(process_query_limited_information, 0, pid)
    if not handle:
        return False
    exit_code = ctypes.c_ulong()
    try:
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        return exit_code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def _process_exists(pid: int) -> bool:
    if os.name == "nt":
        return _windows_process_exists(pid)
    try:
        os.kill(pid, 0)
    except (OSError, SystemError):
        return False
    return True


def restart_coordinator(client: CoordinatorFaultClient, job_id: str) -> dict[str, Any]:
    """Restart the exact discovered local coordinator and wait for health."""

    info = read_service_info(require_healthy=True)
    if not info:
        raise ServiceConfigError("No healthy file-discovered coordinator to restart")
    parsed = urlparse(str(info.get("url") or ""))
    if parsed.scheme != "http" or not is_local_host(str(info.get("host") or "")):
        raise RuntimeError("Restart canary supports only a local HTTP coordinator")
    old_pid = int(info.get("pid") or 0)
    if old_pid <= 0 or not _process_exists(old_pid):
        raise RuntimeError("Discovered coordinator PID is not running")
    health_pid = int(client.get("/api/health").get("pid") or 0)
    if health_pid != old_pid:
        raise RuntimeError("Service file PID does not own the healthy coordinator")
    if str(client.job(job_id).get("status") or "") not in {
        "queued",
        "dispatched",
        "running",
    }:
        raise RuntimeError("Restart canary requires a live benchmark job")

    os.kill(old_pid, signal.SIGTERM)
    deadline = time.monotonic() + 15
    while _process_exists(old_pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    if _process_exists(old_pid):
        raise RuntimeError("Coordinator did not stop for restart canary")

    job_tag = job_id[:12]
    stdout_path = CONFIG_DIR / f"benchmark-restart-{job_tag}.out.log"
    stderr_path = CONFIG_DIR / f"benchmark-restart-{job_tag}.err.log"
    command = [
        sys.executable,
        "-m",
        "cloud_offload",
        "serve",
        "--host",
        str(info["host"]),
        "--port",
        str(info["port"]),
    ]
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        process = subprocess.Popen(
            command,
            cwd=Path.cwd(),
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            creationflags=creationflags,
        )

    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        current = read_service_info(require_healthy=True)
        if current and int(current.get("pid") or 0) == process.pid:
            replacement = CoordinatorFaultClient(current)
            if int(replacement.get("/api/health").get("pid") or 0) == process.pid:
                replayed = replacement.job(job_id)
                replayed_status = str(replayed.get("status") or "")
                if replayed_status not in {"queued", "dispatched", "running"}:
                    raise RuntimeError(
                        "Replacement coordinator did not replay the live job"
                    )
                cancelled = replacement.post(f"/api/jobs/{job_id}/cancel", {})
                if str(cancelled.get("status") or "") != "failed":
                    raise RuntimeError(
                        "Replacement coordinator did not persist canary cancellation"
                    )
                return {
                    "kind": "restart",
                    "old_process_stopped": True,
                    "replacement_healthy": True,
                    "job_replay_available": True,
                    "replayed_status": replayed_status,
                    "cancellation_recorded": True,
                }
        if process.poll() is not None:
            raise RuntimeError("Replacement coordinator exited during restart canary")
        time.sleep(0.25)
    raise TimeoutError("Replacement coordinator did not become healthy")


def run_fault(kind: str) -> dict[str, Any]:
    normalized = str(kind).strip().lower()
    if normalized not in FAULT_KINDS:
        raise ValueError("Unknown benchmark fault kind")
    job_id, scenario, stage = _benchmark_context(normalized)
    client = CoordinatorFaultClient()
    if normalized == "storage":
        if stage != "observe":
            raise RuntimeError("Storage canary supports only the observe stage")
        return inject_storage_unavailable(client, job_id)
    if normalized == "corruption":
        declared = {
            normalize_digest(item)
            for item in os.environ.get(
                "CLOUD_OFFLOAD_BENCHMARK_ASSET_DIGESTS", ""
            ).split(",")
            if item.strip()
        }
        if not declared:
            raise RuntimeError("Corruption canary received no declared asset digests")
        if stage == "prepare":
            return prepare_corruption(client, scenario, declared)
        if stage == "cleanup":
            return cleanup_corruption(client, scenario, declared)
        return observe_corruption(client, job_id, scenario, declared)
    if stage != "observe":
        raise RuntimeError("Restart canary supports only the observe stage")
    return restart_coordinator(client, job_id)
