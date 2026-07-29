import json
import os
import tarfile
from types import SimpleNamespace

import pytest

from cloud_offload import server
from cloud_offload.cache_scheduler import (
    resolve_prepared_requirements,
    scheduler_runtime,
)
from cloud_offload.config import CloudConfig
from cloud_offload.prepared_state import (
    ManifestSigner,
    PreparedStateCAS,
    bundle_key,
    fingerprint,
)
from cloud_offload.runtime_bundles import build_reproducible_bundle
from cloud_offload.router import resolve_worker_profile
from tests.test_prepared_storage import cache_worker, policy


PACK = {
    "id": "example-pack",
    "git": "https://example.invalid/example-pack.git",
    "commit": "1" * 40,
    "install_requirements": True,
}


def test_runtime_bundle_is_reproducible_and_excludes_transient_state(tmp_path):
    source = tmp_path / "source"
    (source / "package").mkdir(parents=True)
    (source / ".git").mkdir()
    (source / "__pycache__").mkdir()
    (source / "package" / "b.py").write_text("b = 2\n", encoding="utf-8")
    (source / "package" / "a.py").write_text("a = 1\n", encoding="utf-8")
    (source / ".git" / "config").write_text("private", encoding="utf-8")
    (source / "__pycache__" / "a.pyc").write_bytes(b"transient")
    first = tmp_path / "first.tar"
    second = tmp_path / "second.tar"

    result1 = build_reproducible_bundle(source, first)
    os.utime(source / "package" / "a.py", (2_000_000_000, 2_000_000_000))
    result2 = build_reproducible_bundle(source, second)

    assert result1["sha256"] == result2["sha256"]
    assert first.read_bytes() == second.read_bytes()
    with tarfile.open(first, "r:*") as archive:
        assert archive.getnames() == ["package", "package/a.py", "package/b.py"]


def test_runtime_requirements_include_code_and_environment_closure():
    profile = {
        "image": "runner@sha256:" + "a" * 64,
        "custom_nodes": [PACK],
        "weights": [],
        "wheelhouse_sha256": "sha256:" + "b" * 64,
    }

    requirement = resolve_prepared_requirements("comfyui", profile, [])

    assert "custom-node:example-pack" in requirement["logical_required"]
    assert any(
        item.startswith("environment:sha256:")
        for item in requirement["logical_required"]
    )


def test_worker_builds_and_restores_signed_runtime_bundles(monkeypatch, tmp_path):
    signer = ManifestSigner(b"r" * 32)
    cas = PreparedStateCAS(tmp_path / "volume", signer)
    profile = fingerprint({"profile": "runtime"})
    worker = cache_worker(cas, profile, "runtime-writer")
    worker.custom_nodes = [PACK]
    worker._pending_prepared_artifacts = []
    worker._verified_prepared_digests = set()
    events = []
    worker.queue = SimpleNamespace(
        append_event=lambda job_id, event: events.append((job_id, event))
    )
    worker._raise_if_cancelled = lambda active_job: None
    job = SimpleNamespace(id="job-runtime")
    pack = tmp_path / "custom_nodes" / "example-pack"
    pack.mkdir(parents=True)
    (pack / "__init__.py").write_text("NODE_CLASS_MAPPINGS = {}", encoding="utf-8")
    environment = tmp_path / "environment"
    monkeypatch.setenv("CLOUD_OFFLOAD_ENV_ROOT", str(environment))
    worker._mark_environment_ready()
    (environment / "dependency.py").write_text("ready = True", encoding="utf-8")

    worker._populate_custom_node_bundle("example-pack", PACK, pack, job)
    worker._populate_environment_bundle(job)

    artifacts = worker._pending_prepared_artifacts
    assert {item["kind"] for item in artifacts} == {
        "custom-node-bundle",
        "environment-bundle",
    }
    worker._flush_prepared_manifest(job)
    completed = [
        event
        for _, event in events
        if event["type"] == "cache_population_completed"
    ]
    assert {event["kind"] for event in completed} == {
        "custom-node-bundle",
        "environment-bundle",
    }

    restored_root = tmp_path / "restored-environment"
    monkeypatch.setenv("CLOUD_OFFLOAD_ENV_ROOT", str(restored_root))
    reader = cache_worker(cas, profile, "runtime-reader")
    reader.custom_nodes = [PACK]

    assert reader._restore_environment_bundle(None) is True
    assert (restored_root / "dependency.py").read_text(encoding="utf-8") == (
        "ready = True"
    )


def _bundle_artifact(data: bytes, *, kind: str, **fields):
    import hashlib

    digest = hashlib.sha256(data).hexdigest()
    return {
        "digest": "sha256:" + digest,
        "kind": kind,
        "size": len(data),
        "storage_key": bundle_key(digest),
        "materialization": "extract",
        "policy": {"tenant": "default", "cacheable": True, "private": False},
        **fields,
    }


def test_coordinator_authorizes_only_profile_bound_runtime_bundles(tmp_path):
    config = CloudConfig(
        prepared_storage=policy(),
        queue_db_path=str(tmp_path / "queue.db"),
        worker_profiles={
            "comfyui": {
                "image": "runner@sha256:" + "a" * 64,
                "models": ["comfyui-workflow"],
                "providers": ["runpod"],
                "custom_nodes": [PACK],
            }
        },
    )
    profile = resolve_worker_profile(config, "comfyui")
    requirement = resolve_prepared_requirements("comfyui", profile, [])
    runtime = scheduler_runtime(requirement)
    runtime.update({"platform": "linux-x86_64", "python_abi": "cp311"})
    job = SimpleNamespace(
        params={
            "runtime_profile": "comfyui",
            "cache_volume_id": "volume-1",
            "prepared_requirement": requirement,
        }
    )
    custom = _bundle_artifact(
        b"custom",
        kind="custom-node-bundle",
        portability="portable",
        requirements={},
        source={"pack_id": "example-pack", **PACK},
        destination={"pack_id": "example-pack"},
    )
    environment = _bundle_artifact(
        b"environment",
        kind="environment-bundle",
        portability="runtime-bound",
        requirements=runtime,
        source={"dependency_lock": runtime["dependency_lock"]},
        destination={"dependency_lock": runtime["dependency_lock"]},
    )
    proposal = {
        "schema": "cloud-offload.prepared-state.v1",
        "profile_fingerprint": requirement["profile_fingerprint"],
        "created_at": "2000-01-01T00:00:00Z",
        "producer": {},
        "artifacts": [custom, environment],
    }

    server._validate_manifest_proposal(
        config, proposal, job=job, volume_id="volume-1"
    )
    assert proposal["cache_volume_id"] == "volume-1"

    spoofed = json.loads(json.dumps(proposal))
    spoofed.pop("cache_volume_id", None)
    spoofed.pop("cache_provider_volume_id", None)
    spoofed["artifacts"][0]["source"]["commit"] = "2" * 40
    with pytest.raises(ValueError, match="profile pin"):
        server._validate_manifest_proposal(
            config, spoofed, job=job, volume_id="volume-1"
        )

    spoofed_environment = json.loads(json.dumps(proposal))
    spoofed_environment.pop("cache_volume_id", None)
    spoofed_environment.pop("cache_provider_volume_id", None)
    spoofed_environment["artifacts"][1]["source"]["dependency_lock"] = (
        "sha256:" + "f" * 64
    )
    with pytest.raises(ValueError, match="source does not match"):
        server._validate_manifest_proposal(
            config, spoofed_environment, job=job, volume_id="volume-1"
        )

    copied_environment = json.loads(json.dumps(proposal))
    copied_environment.pop("cache_volume_id", None)
    copied_environment.pop("cache_provider_volume_id", None)
    copied_environment["artifacts"][1]["materialization"] = "copy"
    with pytest.raises(ValueError, match="safe extraction"):
        server._validate_manifest_proposal(
            config, copied_environment, job=job, volume_id="volume-1"
        )
