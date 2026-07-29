import json
from types import SimpleNamespace

import pytest

from cloud_offload.benchmark_faults import (
    _benchmark_context,
    inject_corruption,
    inject_storage_unavailable,
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


def test_fault_hook_refuses_direct_invocation(monkeypatch):
    monkeypatch.delenv("CLOUD_OFFLOAD_BENCHMARK_FAILURE_KIND", raising=False)
    monkeypatch.delenv("CLOUD_OFFLOAD_BENCHMARK_JOB_ID", raising=False)
    monkeypatch.delenv("CLOUD_OFFLOAD_BENCHMARK_SCENARIO", raising=False)

    with pytest.raises(RuntimeError, match="only inside"):
        _benchmark_context("storage")


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

    receipt = inject_corruption(
        client,
        "job-corruption",
        store_factory=lambda volume: store,
    )

    assert receipt == {
        "kind": "corruption",
        "quarantine_observed": True,
        "canonical_size_restored": True,
        "backup_deleted": True,
        "artifact_size": 100,
    }
    assert s3.objects["blobs/b"] == 100
    assert len(s3.deleted) == 1
    assert s3.deleted[0].startswith("staging/benchmark-corruption/")
