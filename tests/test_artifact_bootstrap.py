from __future__ import annotations

import hashlib
import json
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

from cloud_offload.artifact_bootstrap import (
    ArtifactBootstrapError,
    DeclaredArtifact,
    declared_input_artifacts,
    import_declared_artifacts,
)
from cloud_offload.cache_registry import CacheRegistry
from cloud_offload.config import CloudConfig
from cloud_offload.preflight import build_partition_preflight
from cloud_offload.storage import LocalStorage, partition_artifact_key


def _bundle(root: Path, content: bytes) -> str:
    digest = hashlib.sha256(content).hexdigest()
    source = root / partition_artifact_key(digest)
    source.parent.mkdir(parents=True)
    source.write_bytes(content)
    return digest


def _declared(first: str, second: str, *, first_size: int | None = None):
    return [
        DeclaredArtifact(
            digest=first,
            expected_size=first_size,
            roles=("input_0000:image", "input_0001:images"),
        ),
        DeclaredArtifact(
            digest=second,
            expected_size=None,
            roles=("input_0002:on_false", "input_0003:on_false", "input_0004:image"),
        ),
    ]


def test_imports_declared_artifacts_atomically_and_is_idempotent(tmp_path):
    source_root = tmp_path / "source"
    destination_root = tmp_path / "isolated" / "job_files"
    first = _bundle(source_root, b"first-bundle")
    second = _bundle(source_root, b"second-bundle")
    unrelated = _bundle(source_root, b"not-declared")

    imported = import_declared_artifacts(
        source_root, destination_root, _declared(first, second)
    )
    assert [(item.digest, item.size, item.roles, item.already_present) for item in imported] == [
        (first, 12, ("input_0000:image", "input_0001:images"), False),
        (second, 13, ("input_0002:on_false", "input_0003:on_false", "input_0004:image"), False),
    ]
    for digest, content in ((first, b"first-bundle"), (second, b"second-bundle")):
        target = destination_root / partition_artifact_key(digest)
        assert target.read_bytes() == content
    assert not (destination_root / partition_artifact_key(unrelated)).exists()

    repeated = import_declared_artifacts(
        source_root, destination_root, _declared(first, second)
    )
    assert all(item.already_present for item in repeated)
    assert list(destination_root.rglob("*.tmp")) == []


def test_import_refuses_missing_source(tmp_path):
    digest = "a" * 64
    with pytest.raises(ArtifactBootstrapError, match="source artifact is missing"):
        import_declared_artifacts(
            tmp_path / "source",
            tmp_path / "destination",
            [DeclaredArtifact(digest=digest, roles=("input_0000:image",))],
        )


def test_import_refuses_source_digest_mismatch(tmp_path):
    source_root = tmp_path / "source"
    expected = "b" * 64
    source = source_root / partition_artifact_key(expected)
    source.parent.mkdir(parents=True)
    source.write_bytes(b"actual")
    with pytest.raises(ArtifactBootstrapError, match="source digest mismatch"):
        import_declared_artifacts(
            source_root,
            tmp_path / "destination",
            [DeclaredArtifact(digest=expected, roles=("input_0000:image",))],
        )
    assert not (tmp_path / "destination" / partition_artifact_key(expected)).exists()


def test_import_refuses_wrong_expected_size(tmp_path):
    source_root = tmp_path / "source"
    digest = _bundle(source_root, b"sized")
    with pytest.raises(ArtifactBootstrapError, match="source size mismatch"):
        import_declared_artifacts(
            source_root,
            tmp_path / "destination",
            [DeclaredArtifact(digest=digest, expected_size=99, roles=("input_0000:image",))],
        )


def test_import_removes_partial_destination_on_copy_failure(tmp_path, monkeypatch):
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    digest = _bundle(source_root, b"complete-bundle")

    def partial_copy(source, destination):
        Path(destination).write_bytes(b"partial")
        raise OSError("simulated copy interruption")

    monkeypatch.setattr("cloud_offload.artifact_bootstrap.shutil.copyfile", partial_copy)
    with pytest.raises(ArtifactBootstrapError, match="copy failed"):
        import_declared_artifacts(
            source_root,
            destination_root,
            [DeclaredArtifact(digest=digest, roles=("input_0000:image",))],
        )
    assert not (destination_root / partition_artifact_key(digest)).exists()
    assert list(destination_root.rglob("*.tmp")) == [] if destination_root.exists() else True


def test_import_refuses_conflicting_destination_bytes(tmp_path):
    source_root = tmp_path / "source"
    destination_root = tmp_path / "destination"
    digest = _bundle(source_root, b"source-bundle")
    target = destination_root / partition_artifact_key(digest)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"different")
    with pytest.raises(ArtifactBootstrapError, match="destination artifact mismatch"):
        import_declared_artifacts(
            source_root,
            destination_root,
            [DeclaredArtifact(digest=digest, roles=("input_0000:image",))],
        )
    assert target.read_bytes() == b"different"


def test_declared_input_artifacts_aggregate_roles_from_benchmark_plans(tmp_path):
    first = "a" * 64
    second = "b" * 64
    plan = tmp_path / "benchmark.json"
    plan.write_text(
        json.dumps(
            {
                "scenarios": [
                    {
                        "request": {
                            "input_artifacts": {"input_0000": first, "input_0001": first, "input_0002": second},
                            "partition": {
                                "inputs": [
                                    {"key": "input_0000", "target_input": "image"},
                                    {"key": "input_0001", "target_input": "images"},
                                    {"key": "input_0002", "target_input": "on_false"},
                                ]
                            },
                        }
                    }
                ]
            }
        )
    )
    declarations = declared_input_artifacts([plan])
    assert declarations == [
        DeclaredArtifact(digest=first, expected_size=None, roles=("input_0000:image", "input_0001:images")),
        DeclaredArtifact(digest=second, expected_size=None, roles=("input_0002:on_false",)),
    ]


def test_imported_artifacts_clear_preflight_input_blockers_without_real_provider(tmp_path, monkeypatch):
    source_root = tmp_path / "source"
    isolated_root = tmp_path / "isolated" / "job_files"
    first = _bundle(source_root, b"first-bundle")
    second = _bundle(source_root, b"second-bundle")
    declarations = _declared(first, second)
    import_declared_artifacts(source_root, isolated_root, declarations)

    calls = []

    class FakeConnector:
        def list_available(self, **kwargs):
            calls.append(kwargs)
            return []

    config = CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="test-only",
        worker_token="test-only",
        coordinator_url="http://127.0.0.1:1",
        gpu_type="any",
        max_hourly_rate=2.2,
        storage_type="local",
        storage_path=str(isolated_root),
        queue_db_path=str(tmp_path / "queue.db"),
        worker_profiles={
            "comfyui": {
                "image": "ghcr.io/example/worker@sha256:" + "c" * 64,
                "providers": ["runpod"],
                "models": ["comfyui-partition-v1"],
                "min_gpu_ram_gb": 16,
            }
        },
    )
    partition = {
        "schema": "comfy.partition.job.v1",
        "partition_id": "local-bootstrap",
        "inputs": [
            {"key": "input_0000", "target_input": "image", "type": "IMAGE"},
            {"key": "input_0001", "target_input": "images", "type": "IMAGE"},
            {"key": "input_0002", "target_input": "on_false", "type": "IMAGE"},
            {"key": "input_0003", "target_input": "on_false", "type": "IMAGE"},
            {"key": "input_0004", "target_input": "image", "type": "IMAGE"},
        ],
        "outputs": [],
        "runner": {"profile": "comfyui"},
        "workflow": {"node": {"class_type": "CloudPartitionInput", "inputs": {}}},
        "assets": [],
    }
    with monkeypatch.context() as patch:
        patch.setattr("cloud_offload.preflight._storage_credentials_configured", lambda: False)
        report = build_partition_preflight(
            config=config,
            partition=partition,
            input_artifacts={
                "input_0000": first,
                "input_0001": first,
                "input_0002": second,
                "input_0003": second,
                "input_0004": second,
            },
            provider="runpod",
            allowed_regions=[],
            storage=LocalStorage(isolated_root),
            cache_registry=CacheRegistry(str(tmp_path / "preflight.db")),
            worker_auth_configured=True,
            connector_factory=lambda *_: FakeConnector(),
        )
    assert not [item for item in report["blockers"] if item["code"] == "input_artifact_not_found"]
    assert len(calls) == 1


def test_release_bootstrap_cli_imports_only_plan_inputs(tmp_path, monkeypatch, capsys):
    from cloud_offload import __main__

    source_root = tmp_path / "source"
    destination_root = tmp_path / "isolated" / "job_files"
    first = _bundle(source_root, b"first-bundle")
    second = _bundle(source_root, b"second-bundle")
    benchmark = tmp_path / "benchmark.json"
    benchmark.write_text(
        json.dumps(
            {
                "schema": "cloud-offload.benchmark-plan.v1",
                "providers": ["runpod"],
                "exclusive": True,
                "limits": {
                    "max_total_cost_usd": 1,
                    "max_scenario_cost_usd": 1,
                    "max_campaign_seconds": 10,
                    "poll_seconds": 1,
                    "cleanup_timeout_seconds": 1,
                },
                "scenarios": [
                    {
                        "name": "cold",
                        "cache_state": "cold",
                        "endpoint": "/api/partitions",
                        "fresh_instance": True,
                        "prepared_storage_policy": "off",
                        "timeout_seconds": 1,
                        "expected_statuses": ["completed"],
                        "request": {
                            "force_execution": True,
                            "input_artifacts": {"input_0000": first, "input_0001": second},
                            "partition": {
                                "inputs": [
                                    {"key": "input_0000", "target_input": "image"},
                                    {"key": "input_0001", "target_input": "images"},
                                ]
                            },
                        },
                    }
                ],
            }
        )
    )
    release = tmp_path / "release.json"
    release.write_text("{}")
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps({"cloud": {"storage_type": "local", "storage_path": str(destination_root)}})
    )
    monkeypatch.setattr(
        "cloud_offload.release_gate.ReleasePlan.load",
        lambda _: SimpleNamespace(
            cases=[SimpleNamespace(benchmark_plan_path=benchmark)]
        ),
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cloud-offload",
            "release",
            "bootstrap-artifacts",
            "--plan",
            str(release),
            "--source-root",
            str(source_root),
            "--config",
            str(config),
        ],
    )
    with pytest.raises(SystemExit) as exit_info:
        __main__.main()
    assert exit_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_count"] == 2
    assert {item["digest"] for item in payload["artifacts"]} == {first, second}
    assert all("path" not in item for item in payload["artifacts"])
