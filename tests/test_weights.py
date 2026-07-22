"""Profile weight pinning: schema validation, dispatch env, worker staging."""

import json
import sys
from types import SimpleNamespace

import pytest

from cloud_offload.config import CloudConfig
from cloud_offload.dispatcher import Dispatcher
from cloud_offload.profiles import configured_worker_profiles, normalized_profile_weights
from cloud_offload.providers.base import CloudProvider
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.worker import Worker

SDXL_WEIGHTS = [
    {
        "repo_id": "stabilityai/sdxl-base",
        "revision": "462165984030d82259a11f4367a4eed129e94a7b",
        "files": ["sd_xl_base_1.0.safetensors"],
        "dest": "checkpoints",
    }
]


def weights_config(tmp_path, weights=SDXL_WEIGHTS):
    profile = {
        "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
        "models": ["comfyui-partition-v1"],
        "providers": ["runpod"],
    }
    if weights is not None:
        profile["weights"] = weights
    return CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        coordinator_url="https://coordinator.invalid",
        worker_profiles={"comfyui": profile},
    )


# === Schema validation ===

def test_weights_are_normalized_into_the_profile(tmp_path):
    config = weights_config(tmp_path)

    profile = configured_worker_profiles(config)["comfyui"]

    assert profile["weights"] == SDXL_WEIGHTS


def test_profile_without_weights_gets_an_empty_list(tmp_path):
    config = weights_config(tmp_path, weights=None)

    assert configured_worker_profiles(config)["comfyui"]["weights"] == []


def test_snapshot_entry_keeps_files_null():
    normalized = normalized_profile_weights(
        "comfyui",
        [{"repo_id": "org/repo", "revision": "abc123", "files": None, "dest": "vae"}],
    )

    assert normalized[0]["files"] is None
    assert normalized[0]["dest"] == "vae"


@pytest.mark.parametrize(
    "entry, match",
    [
        ({"revision": "abc", "dest": "checkpoints"}, "repo_id is required"),
        ({"repo_id": "org/repo", "dest": "checkpoints"}, "revision is required"),
        ({"repo_id": "org/repo", "revision": "abc"}, "dest is required"),
        (
            {"repo_id": "org/repo", "revision": "abc", "dest": "/etc"},
            "must be a relative path",
        ),
        (
            {"repo_id": "org/repo", "revision": "abc", "dest": "C:\\weights"},
            "must be a relative path",
        ),
        (
            {"repo_id": "org/repo", "revision": "abc", "dest": "../outside"},
            "must not traverse upward",
        ),
        (
            {"repo_id": "org/repo", "revision": "abc", "dest": "vae", "files": []},
            "files must be null",
        ),
        (
            {"repo_id": "org/repo", "revision": "abc", "dest": "vae", "files": "x.pt"},
            "files must be null",
        ),
        (
            {
                "repo_id": "org/repo",
                "revision": "abc",
                "dest": "vae",
                "files": ["../../escape.pt"],
            },
            "must not traverse upward",
        ),
    ],
)
def test_invalid_weights_entries_fail_loudly(entry, match):
    with pytest.raises(ValueError, match=match):
        normalized_profile_weights("comfyui", [entry])


def test_invalid_weights_fail_at_config_load(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "cloud": {
                    "worker_profiles": {
                        "comfyui": {
                            "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
                            "models": ["comfyui-workflow"],
                            "weights": [{"repo_id": "org/repo", "dest": "checkpoints"}],
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="revision is required"):
        CloudConfig.load(config_path)


# === Dispatcher: worker environment ===

class LaunchProvider(CloudProvider):
    """Captures the env_vars a launch would hand to the cloud instance."""

    def __init__(self):
        self.env_vars = None

    @property
    def name(self) -> str:
        return "runpod"

    def list_available(self, *args, **kwargs):
        return []

    def find_cheapest(self, **kwargs):
        return {"id": "offer-1", "gpu_type": "RTX 4090", "hourly_rate": 0.34}

    def launch(self, *args, **kwargs):
        self.env_vars = kwargs["env_vars"]
        return SimpleNamespace(
            id="pod-1",
            provider="runpod",
            gpu_type="RTX 4090",
            hourly_rate=0.34,
            status="running",
        )

    def get_instance(self, instance_id):
        return None

    def terminate(self, instance_id):
        return True

    def list_instances(self):
        return []


def test_dispatcher_passes_weights_and_token_to_the_worker(isolate_credentials, tmp_path):
    isolate_credentials.store["huggingface"] = "hf-secret"
    provider = LaunchProvider()
    dispatcher = Dispatcher(weights_config(tmp_path), provider=provider)

    dispatcher._launch_worker("runpod", "comfyui")

    assert json.loads(provider.env_vars["CLOUD_OFFLOAD_WEIGHTS"]) == SDXL_WEIGHTS
    assert provider.env_vars["HF_TOKEN"] == "hf-secret"


def test_dispatcher_omits_the_token_when_none_resolves(tmp_path):
    provider = LaunchProvider()
    dispatcher = Dispatcher(weights_config(tmp_path), provider=provider)

    dispatcher._launch_worker("runpod", "comfyui")

    assert "CLOUD_OFFLOAD_WEIGHTS" in provider.env_vars
    assert "HF_TOKEN" not in provider.env_vars


def test_dispatcher_sets_neither_var_without_weights(isolate_credentials, tmp_path):
    # Even with a stored token: no weights means no secret in the pod env.
    isolate_credentials.store["huggingface"] = "hf-secret"
    provider = LaunchProvider()
    dispatcher = Dispatcher(weights_config(tmp_path, weights=None), provider=provider)

    dispatcher._launch_worker("runpod", "comfyui")

    assert "CLOUD_OFFLOAD_WEIGHTS" not in provider.env_vars
    assert "HF_TOKEN" not in provider.env_vars


# === Worker: staging at the first claimed job ===

class FakeHub:
    """Stands in for huggingface_hub; records calls, writes the target file."""

    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def hf_hub_download(self, *, repo_id, filename, revision, local_dir, token):
        self.calls.append(("file", repo_id, filename, revision, local_dir, token))
        if self.fail_on == filename:
            raise RuntimeError("401 gated repo")
        from pathlib import Path

        target = Path(local_dir) / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"weights")
        return str(target)

    def snapshot_download(self, *, repo_id, revision, local_dir, token):
        self.calls.append(("snapshot", repo_id, None, revision, local_dir, token))
        if self.fail_on == repo_id:
            raise RuntimeError("401 gated repo")
        return str(local_dir)


def staging_worker(tmp_path, monkeypatch, weights, hub):
    monkeypatch.setitem(sys.modules, "huggingface_hub", hub)
    monkeypatch.setenv("CLOUD_OFFLOAD_COMFYUI_ROOT", str(tmp_path / "ComfyUI"))
    worker = Worker.__new__(Worker)
    worker.queue = JobQueue(tmp_path / "queue.db")
    worker.weights = weights
    worker._weights_staged = False
    return worker


def test_staging_downloads_listed_files_into_the_models_dir(tmp_path, monkeypatch):
    hub = FakeHub()
    worker = staging_worker(tmp_path, monkeypatch, SDXL_WEIGHTS, hub)
    job = worker.queue.create("comfyui-workflow", "inline://request")

    worker._stage_profile_weights(job)

    kind, repo_id, filename, revision, local_dir, token = hub.calls[0]
    assert (kind, repo_id, filename) == (
        "file",
        "stabilityai/sdxl-base",
        "sd_xl_base_1.0.safetensors",
    )
    assert revision == SDXL_WEIGHTS[0]["revision"]
    assert local_dir.endswith("checkpoints")
    assert token is None
    assert worker._weights_staged is True


def test_staging_uses_snapshot_download_when_files_is_null(tmp_path, monkeypatch):
    hub = FakeHub()
    weights = [{"repo_id": "org/vae", "revision": "def456", "files": None, "dest": "vae"}]
    worker = staging_worker(tmp_path, monkeypatch, weights, hub)
    job = worker.queue.create("comfyui-workflow", "inline://request")

    worker._stage_profile_weights(job)

    assert [call[0] for call in hub.calls] == ["snapshot"]
    assert hub.calls[0][1] == "org/vae"
    assert hub.calls[0][4].endswith("vae")


def test_staging_passes_the_hub_token_from_the_environment(tmp_path, monkeypatch):
    hub = FakeHub()
    worker = staging_worker(tmp_path, monkeypatch, SDXL_WEIGHTS, hub)
    monkeypatch.setenv("HF_TOKEN", "hf-worker-token")
    job = worker.queue.create("comfyui-workflow", "inline://request")

    worker._stage_profile_weights(job)

    assert hub.calls[0][5] == "hf-worker-token"


def test_staging_skips_files_that_already_exist(tmp_path, monkeypatch):
    hub = FakeHub()
    worker = staging_worker(tmp_path, monkeypatch, SDXL_WEIGHTS, hub)
    staged = tmp_path / "ComfyUI" / "models" / "checkpoints" / "sd_xl_base_1.0.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"already here")
    job = worker.queue.create("comfyui-workflow", "inline://request")

    worker._stage_profile_weights(job)

    assert hub.calls == []
    assert worker._weights_staged is True


def test_staging_emits_ordered_weights_staging_events(tmp_path, monkeypatch):
    hub = FakeHub()
    weights = [
        {
            "repo_id": "org/checkpoints",
            "revision": "abc123",
            "files": ["base.safetensors", "refiner.safetensors"],
            "dest": "checkpoints",
        },
        {"repo_id": "org/vae", "revision": "def456", "files": None, "dest": "vae"},
    ]
    worker = staging_worker(tmp_path, monkeypatch, weights, hub)
    job = worker.queue.create("comfyui-workflow", "inline://request")

    worker._stage_profile_weights(job)

    events = [item["event"] for item in worker.queue.list_events(job.id)]
    assert [event["type"] for event in events] == ["weights_staging"] * 4
    assert [(event["repo_id"], event["file"]) for event in events] == [
        ("org/checkpoints", "base.safetensors"),
        ("org/checkpoints", "refiner.safetensors"),
        ("org/vae", None),
        (None, None),  # completion marker
    ]
    assert [event["downloaded_files"] for event in events] == [0, 1, 2, 3]
    assert all(event["total_files"] == 3 for event in events)
    # Staging progress stays between runner_starting (2) and running (10).
    progresses = [event["overall_progress"] for event in events]
    assert progresses == sorted(progresses)
    assert progresses[0] == 3 and progresses[-1] == 9


def test_staging_failure_names_the_repo_and_file(tmp_path, monkeypatch):
    hub = FakeHub(fail_on="sd_xl_base_1.0.safetensors")
    worker = staging_worker(tmp_path, monkeypatch, SDXL_WEIGHTS, hub)
    job = worker.queue.create("comfyui-workflow", "inline://request")

    with pytest.raises(RuntimeError, match=r"stabilityai/sdxl-base \(sd_xl_base_1\.0"):
        worker._stage_profile_weights(job)

    # Not marked staged: the next claimed job retries the download.
    assert worker._weights_staged is False


def test_first_job_stages_before_executing_and_later_jobs_skip(tmp_path, monkeypatch):
    hub = FakeHub()
    worker = staging_worker(tmp_path, monkeypatch, SDXL_WEIGHTS, hub)
    order = []
    original = FakeHub.hf_hub_download

    def tracking_download(self, **kwargs):
        order.append("download")
        return original(self, **kwargs)

    monkeypatch.setattr(FakeHub, "hf_hub_download", tracking_download)
    worker._run_comfyui_workflow = lambda job: order.append("execute") or {"outputs": {}}

    first = worker.queue.create("comfyui-workflow", "inline://request")
    second = worker.queue.create("comfyui-workflow", "inline://request")
    worker._process_job(first)
    worker._process_job(second)

    assert order == ["download", "execute", "execute"]
    assert worker.queue.get(first.id).status == JobStatus.COMPLETED
    assert worker.queue.get(second.id).status == JobStatus.COMPLETED


def test_worker_rejects_malformed_weights_env(monkeypatch):
    monkeypatch.setenv("CLOUD_OFFLOAD_WEIGHTS", "{not json")

    with pytest.raises(RuntimeError, match="not valid JSON"):
        Worker._load_weights_env()

    monkeypatch.setenv("CLOUD_OFFLOAD_WEIGHTS", '{"repo_id": "org/repo"}')
    with pytest.raises(RuntimeError, match="JSON list"):
        Worker._load_weights_env()

    monkeypatch.delenv("CLOUD_OFFLOAD_WEIGHTS")
    assert Worker._load_weights_env() == []
