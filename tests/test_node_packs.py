"""Tests for required custom node packs.

The chain under test: a compiled partition names the node packs it needs by
registry id and content digest; the coordinator proves the target worker profile
was told to install each one *before* it routes or provisions; the worker
installs them from a pinned registry release or a pinned commit at its first job.

The refusal is the point. A partition whose node types will not exist on the
runner must cost a 409, not a rented GPU that fails on its first prompt. So is
what is *not* refused: a version disagreement is a warning, because the version
a pack declares is not evidence about the code it contains — the pack this
feature was built around ships a security fix under the version number of the
unpatched release.
"""

import hashlib
import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from cloud_offload import router as router_module
from cloud_offload import server
from cloud_offload.config import CloudConfig
from cloud_offload.node_packs import (
    missing_node_packs,
    missing_node_packs_message,
    normalized_partition_node_packs,
)
from cloud_offload.profiles import (
    configured_worker_profiles,
    normalized_profile_custom_nodes,
    profile_pack_identifier,
)
from cloud_offload.providers.base import CloudProvider
from cloud_offload.queue import JobQueue, JobStatus
from cloud_offload.worker import Worker


PACK_SOURCE = b"NODE_CLASS_MAPPINGS = {}\n"
PACK_DIGEST = hashlib.sha256(PACK_SOURCE).hexdigest()
COMMIT = "2be3bd3" + "0" * 33

QWEN_PACK = {
    "id": "eric-qwen-layer",
    "directory": "eric-qwen-layer",
    "version": "0.1.0",
    "digest": PACK_DIGEST,
}

REGISTRY_ENTRY = {"registry_id": "eric-qwen-layer", "version": "0.1.0"}
GIT_ENTRY = {"git": "https://github.com/EricRollei/eric-qwen-layer.git", "commit": COMMIT}


def packs_config(tmp_path, *, custom_nodes=None):
    profile = {
        "image": "ghcr.io/example/comfyui@sha256:" + "a" * 64,
        "models": ["comfyui-partition-v1"],
        "providers": ["runpod"],
    }
    if custom_nodes is not None:
        profile["custom_nodes"] = custom_nodes
    return CloudConfig(
        enabled=True,
        provider="runpod",
        provider_order=["runpod"],
        runpod_api_key="secret",
        coordinator_url="https://coordinator.invalid",
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
        worker_profiles={"comfyui": profile},
    )


def partition_request(node_packs=None):
    partition = {
        "schema": "comfy.partition.job.v1",
        "partition_id": "part-1",
        "workflow": {"1": {"class_type": "CloudPartitionInput", "inputs": {}}},
        "inputs": [],
        "outputs": [],
        "runner": {"profile": "comfyui"},
    }
    if node_packs is not None:
        partition["node_packs"] = node_packs
    return {"partition": partition, "input_artifacts": {}, "provider": "auto"}


@pytest.fixture
def watch_routing(monkeypatch):
    """Record every routing call, which is the step just before provisioning."""
    calls = []
    original = router_module.select_profile_provider

    def recording(*args, **kwargs):
        calls.append(args[1:])
        return original(*args, **kwargs)

    monkeypatch.setattr(router_module, "select_profile_provider", recording)
    return calls


def packs_client(monkeypatch, config):
    queue = JobQueue(config.queue_db_path)
    monkeypatch.setattr(server, "_queue", lambda: (config, queue))
    monkeypatch.setattr(server, "_config", lambda resolve_secrets=True: config)
    return TestClient(server.app), queue


# ---------------------------------------------------------------------------
# Configuration: a profile's custom_nodes
# ---------------------------------------------------------------------------


def test_both_source_kinds_normalize_with_install_requirements_defaulted():
    normalized = normalized_profile_custom_nodes("comfyui", [REGISTRY_ENTRY, GIT_ENTRY])

    assert normalized == [
        {"registry_id": "eric-qwen-layer", "version": "0.1.0", "install_requirements": True},
        {"git": GIT_ENTRY["git"], "commit": COMMIT, "install_requirements": True},
    ]


def test_install_requirements_can_be_switched_off():
    normalized = normalized_profile_custom_nodes(
        "comfyui", [{**REGISTRY_ENTRY, "install_requirements": False}]
    )

    assert normalized[0]["install_requirements"] is False


def test_profiles_default_to_declaring_no_packs(tmp_path):
    profile = configured_worker_profiles(packs_config(tmp_path))["comfyui"]

    assert profile["custom_nodes"] == []


@pytest.mark.parametrize(
    "entry, match",
    [
        ({}, "registry_id or git is required"),
        ({**REGISTRY_ENTRY, **GIT_ENTRY}, "not both"),
        ({"registry_id": "eric-qwen-layer"}, "version is required"),
        ({"git": GIT_ENTRY["git"]}, "commit is required"),
        ({"git": GIT_ENTRY["git"], "commit": "main"}, "full 40-character sha"),
        ({"git": GIT_ENTRY["git"], "commit": "2be3bd3"}, "full 40-character sha"),
        ({"git": "git@github.com:owner/pack.git", "commit": COMMIT}, "http or https"),
        ("not-an-object", "must be an object"),
    ],
)
def test_invalid_custom_nodes_name_the_entry(entry, match):
    with pytest.raises(ValueError, match=match) as failure:
        normalized_profile_custom_nodes("comfyui", [entry])

    assert "custom_nodes[0]" in str(failure.value)


def test_custom_nodes_must_be_a_list():
    with pytest.raises(ValueError, match="must be a list"):
        normalized_profile_custom_nodes("comfyui", {"registry_id": "x"})


def test_invalid_custom_nodes_fail_at_config_load(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "cloud": {
                    "worker_profiles": {
                        "comfyui": {"custom_nodes": [{"git": GIT_ENTRY["git"], "commit": "main"}]}
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="full 40-character sha"):
        CloudConfig.load(config_path)


def test_a_git_entry_answers_to_its_repository_name():
    assert profile_pack_identifier(GIT_ENTRY) == "eric-qwen-layer"
    assert profile_pack_identifier({"git": "https://host/owner/Pack/"}) == "Pack"
    assert profile_pack_identifier(REGISTRY_ENTRY) == "eric-qwen-layer"


# ---------------------------------------------------------------------------
# Declared node pack validation
# ---------------------------------------------------------------------------


def test_declared_node_packs_are_normalized():
    normalized = normalized_partition_node_packs(
        [{**QWEN_PACK, "digest": PACK_DIGEST.upper(), "declared": {"id": True}}]
    )

    assert normalized == [QWEN_PACK]


def test_a_pack_that_declares_no_version_is_still_valid():
    normalized = normalized_partition_node_packs([{**QWEN_PACK, "version": ""}])

    assert normalized[0]["version"] == ""


@pytest.mark.parametrize(
    "pack, match",
    [
        ({**QWEN_PACK, "id": ""}, "id is required"),
        ({**QWEN_PACK, "directory": ""}, "directory is required"),
        ({**QWEN_PACK, "directory": "../escape"}, "must not traverse upward"),
        ({**QWEN_PACK, "directory": "/abs"}, "must be a relative path"),
        ({**QWEN_PACK, "digest": "abc"}, "64-character sha256"),
        ({**QWEN_PACK, "digest": None}, "64-character sha256"),
        ("not-an-object", "must be an object"),
    ],
)
def test_malformed_declared_node_packs_are_rejected(pack, match):
    with pytest.raises(ValueError, match=match):
        normalized_partition_node_packs([pack])


# ---------------------------------------------------------------------------
# Submission: POST /api/partitions
# ---------------------------------------------------------------------------


def test_a_declared_pack_routes(monkeypatch, tmp_path, watch_routing):
    config = packs_config(tmp_path, custom_nodes=[REGISTRY_ENTRY])
    client, queue = packs_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([QWEN_PACK]))

    assert response.status_code == 202
    assert "node_pack_warnings" not in response.json()
    assert len(watch_routing) == 1
    assert queue.get(response.json()["job_id"]).provider == "runpod"


def test_a_git_declaration_covers_the_pack_it_clones(monkeypatch, tmp_path):
    # The requirement names the registry id; the profile pins a commit. The
    # repository name is the directory git would create, so the two match.
    config = packs_config(tmp_path, custom_nodes=[GIT_ENTRY])
    client, _ = packs_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([QWEN_PACK]))

    assert response.status_code == 202


def test_matching_ignores_case(monkeypatch, tmp_path):
    config = packs_config(tmp_path, custom_nodes=[{**REGISTRY_ENTRY, "registry_id": "Eric-Qwen-Layer"}])
    client, _ = packs_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([QWEN_PACK]))

    assert response.status_code == 202


def test_an_undeclared_pack_is_refused_before_routing(monkeypatch, tmp_path, watch_routing):
    config = packs_config(tmp_path, custom_nodes=[])
    client, queue = packs_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([QWEN_PACK]))

    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "cloud_offload.missing_node_packs"
    assert error["message"] == (
        "Cloud Offload cannot provide 1 custom node pack required by this "
        "partition: eric-qwen-layer. Add it to the worker profile's custom_nodes, "
        "or remove those nodes from the box."
    )
    # Machine-readable, so a client can offer to add the pin instead of parsing prose.
    assert error["details"]["missing"] == [QWEN_PACK]
    # Nothing was routed, so nothing was provisioned, and no job exists to bill.
    assert watch_routing == []
    assert queue.list_by_status(*JobStatus) == []


def test_the_refusal_names_every_undeclared_pack(monkeypatch, tmp_path):
    config = packs_config(tmp_path, custom_nodes=[REGISTRY_ENTRY])
    client, _ = packs_client(monkeypatch, config)
    missing = [
        {**QWEN_PACK, "id": "ComfyUI-Grounding", "directory": "ComfyUI-Grounding"},
        {**QWEN_PACK, "id": "ComfyUI-See-through", "directory": "ComfyUI-See-through"},
    ]

    response = client.post("/api/partitions", json=partition_request([QWEN_PACK, *missing]))

    assert response.status_code == 409
    message = response.json()["error"]["message"]
    assert "2 custom node packs" in message
    assert "ComfyUI-Grounding, ComfyUI-See-through" in message
    assert "Add them to" in message
    # The declared one is not blamed.
    assert QWEN_PACK["id"] not in message


@pytest.mark.parametrize(
    "packs",
    [
        [{**QWEN_PACK, "digest": "abc"}],
        [{**QWEN_PACK, "directory": "../../escape"}],
        [{**QWEN_PACK, "id": ""}],
        "not-a-list",
    ],
)
def test_malformed_node_packs_are_rejected_with_400(monkeypatch, tmp_path, packs, watch_routing):
    config = packs_config(tmp_path, custom_nodes=[REGISTRY_ENTRY])
    client, queue = packs_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request(packs))

    assert response.status_code == 400
    assert watch_routing == []
    assert queue.list_by_status(*JobStatus) == []


def test_a_version_disagreement_warns_and_still_routes(monkeypatch, tmp_path):
    config = packs_config(tmp_path, custom_nodes=[{**REGISTRY_ENTRY, "version": "0.2.0"}])
    client, queue = packs_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([QWEN_PACK]))

    assert response.status_code == 202
    warning = response.json()["node_pack_warnings"][0]
    assert warning["id"] == "eric-qwen-layer"
    assert (warning["declared_version"], warning["profile_version"]) == ("0.1.0", "0.2.0")
    assert "would not have proven a code match" in warning["warning"]
    assert queue.get(response.json()["job_id"]).status == JobStatus.QUEUED


def test_a_git_pin_never_warns_about_versions(monkeypatch, tmp_path):
    # A commit is not a version, so there is nothing to disagree about.
    config = packs_config(tmp_path, custom_nodes=[GIT_ENTRY])
    client, _ = packs_client(monkeypatch, config)

    response = client.post("/api/partitions", json=partition_request([QWEN_PACK]))

    assert "node_pack_warnings" not in response.json()


def test_a_partition_without_node_packs_behaves_exactly_as_before(monkeypatch, tmp_path):
    """Regression guard for every workflow compiled by an older node pack."""
    client, queue = packs_client(monkeypatch, packs_config(tmp_path))

    response = client.post("/api/partitions", json=partition_request())

    assert response.status_code == 202
    assert response.json().keys() == {"job_id", "status", "status_url", "storage"}
    job = queue.get(response.json()["job_id"])
    assert "node_packs" not in job.request
    assert job.provider == "runpod"


def test_an_empty_node_pack_list_routes_without_a_profile_declaration(monkeypatch, tmp_path):
    client, _ = packs_client(monkeypatch, packs_config(tmp_path))

    response = client.post("/api/partitions", json=partition_request([]))

    assert response.status_code == 202


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


def test_the_dispatcher_passes_declared_packs_to_the_runner(tmp_path):
    from cloud_offload.dispatcher import Dispatcher

    provider = LaunchProvider()
    config = packs_config(tmp_path, custom_nodes=[REGISTRY_ENTRY])

    Dispatcher(config, provider=provider)._launch_worker("runpod", "comfyui")

    assert json.loads(provider.env_vars["CLOUD_OFFLOAD_CUSTOM_NODES"]) == [
        {"registry_id": "eric-qwen-layer", "version": "0.1.0", "install_requirements": True}
    ]


def test_the_dispatcher_sets_no_variable_without_declared_packs(tmp_path):
    from cloud_offload.dispatcher import Dispatcher

    provider = LaunchProvider()

    Dispatcher(packs_config(tmp_path), provider=provider)._launch_worker("runpod", "comfyui")

    assert "CLOUD_OFFLOAD_CUSTOM_NODES" not in provider.env_vars


# ---------------------------------------------------------------------------
# Worker: staging at the first claimed job
# ---------------------------------------------------------------------------


def pack_zip(members: dict[str, bytes], symlinks: tuple[str, ...] = ()) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        for name, body in members.items():
            bundle.writestr(name, body)
        for name in symlinks:
            info = zipfile.ZipInfo(name)
            # 0o120000 is the symlink file type in the high half of external_attr.
            info.external_attr = (0o120777 << 16)
            bundle.writestr(info, "../../../etc/passwd")
    return buffer.getvalue()


class FakeRequests:
    """Stands in for ``requests``; serves a versions list and one archive."""

    def __init__(self, versions=None, archive=None):
        self.versions = versions if versions is not None else [
            {"version": "0.0.9", "downloadUrl": "https://cdn.invalid/old.zip"},
            {"version": "0.1.0", "downloadUrl": "https://cdn.invalid/node.zip"},
        ]
        self.archive = archive if archive is not None else pack_zip(
            {"eric-qwen-layer/__init__.py": PACK_SOURCE}
        )
        self.urls = []

    class Response:
        def __init__(self, payload=None, body=b""):
            self.payload = payload
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

        def iter_content(self, chunk_size):
            yield self.body

    def get(self, url, timeout=None, stream=False):
        self.urls.append(url)
        if url.endswith("/versions"):
            return self.Response(payload={"versions": self.versions})
        return self.Response(body=self.archive)


def staging_worker(tmp_path, monkeypatch, custom_nodes, requests_module=None):
    monkeypatch.setitem(sys.modules, "requests", requests_module or FakeRequests())
    monkeypatch.setenv("CLOUD_OFFLOAD_COMFYUI_ROOT", str(tmp_path / "ComfyUI"))
    worker = Worker.__new__(Worker)
    worker.config = CloudConfig(
        provider="runpod",
        queue_db_path=str(tmp_path / "queue.db"),
        storage_path=str(tmp_path / "storage"),
    )
    worker.worker_id = "worker-staging"
    worker.runtime_profile = "comfyui"
    worker.capabilities = ["comfyui-partition-v1"]
    worker.queue = JobQueue(tmp_path / "queue.db")
    worker.custom_nodes = custom_nodes
    worker._custom_nodes_staged = False
    return worker


def staging_job(worker):
    return worker.queue.create("comfyui-partition-v1", "artifacts://comfyui-partition")


def pack_path(tmp_path, *parts):
    return tmp_path.joinpath("ComfyUI", "custom_nodes", *parts)


def test_a_registry_pack_is_resolved_by_version_and_extracted(tmp_path, monkeypatch):
    fake = FakeRequests()
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)

    worker._stage_custom_nodes(staging_job(worker))

    assert fake.urls == [
        "https://api.comfy.org/nodes/eric-qwen-layer/versions",
        "https://cdn.invalid/node.zip",
    ]
    installed = pack_path(tmp_path, "eric-qwen-layer", "eric-qwen-layer", "__init__.py")
    assert installed.read_bytes() == PACK_SOURCE
    assert worker._custom_nodes_staged is True


def test_a_version_the_registry_does_not_publish_fails_clearly(tmp_path, monkeypatch):
    fake = FakeRequests(versions=[{"version": "0.9.0", "downloadUrl": "https://cdn.invalid/x.zip"}])
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)

    with pytest.raises(RuntimeError, match="no registry version 0.1.0"):
        worker._stage_custom_nodes(staging_job(worker))


@pytest.mark.parametrize(
    "members, symlinks, match",
    [
        ({"../../evil.py": b"x"}, (), "traverses upward and was refused: ../../evil.py"),
        ({"pack/../../evil.py": b"x"}, (), "traverses upward and was refused"),
        ({"/etc/cron.d/evil": b"x"}, (), "absolute path and was refused: /etc/cron.d/evil"),
        ({"C:/Windows/evil.py": b"x"}, (), "absolute path and was refused"),
        ({"pack/__init__.py": PACK_SOURCE}, ("pack/link.py",), "symlink and was refused: pack/link.py"),
    ],
)
def test_a_traversal_bearing_archive_is_refused_by_member(
    tmp_path, monkeypatch, members, symlinks, match
):
    fake = FakeRequests(archive=pack_zip(members, symlinks))
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)

    with pytest.raises(RuntimeError, match=match):
        worker._stage_custom_nodes(staging_job(worker))

    # Refused in full: not one member of a hostile archive reached the disk.
    assert not pack_path(tmp_path, "eric-qwen-layer").exists()
    assert not (tmp_path / "evil.py").exists()


def test_a_git_pack_is_cloned_and_pinned(tmp_path, monkeypatch):
    calls = []

    def fake_run(arguments, **kwargs):
        calls.append(arguments)
        if arguments[-1] == "HEAD":
            return subprocess.CompletedProcess(arguments, 0, stdout=f"{COMMIT}\n", stderr="")
        if arguments[1] == "clone":
            Path(arguments[-1]).mkdir(parents=True)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    worker = staging_worker(tmp_path, monkeypatch, [dict(GIT_ENTRY)])

    worker._stage_custom_nodes(staging_job(worker))

    target = str(pack_path(tmp_path, "eric-qwen-layer"))
    assert calls == [
        ["git", "clone", "--filter=blob:none", "--no-checkout", GIT_ENTRY["git"], target],
        ["git", "-C", target, "checkout", "--detach", COMMIT],
        ["git", "-C", target, "rev-parse", "HEAD"],
    ]


def test_a_checkout_that_lands_off_the_pin_fails_loudly(tmp_path, monkeypatch):
    other = "f" * 40

    def fake_run(arguments, **kwargs):
        if arguments[-1] == "HEAD":
            return subprocess.CompletedProcess(arguments, 0, stdout=f"{other}\n", stderr="")
        if arguments[1] == "clone":
            Path(arguments[-1]).mkdir(parents=True)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    worker = staging_worker(tmp_path, monkeypatch, [dict(GIT_ENTRY)])

    with pytest.raises(RuntimeError, match=f"checked out {other} but the worker profile pins"):
        worker._stage_custom_nodes(staging_job(worker))


def test_a_failing_git_command_reports_its_stderr(tmp_path, monkeypatch):
    def fake_run(arguments, **kwargs):
        return subprocess.CompletedProcess(arguments, 128, stdout="", stderr="fatal: not found\n")

    monkeypatch.setattr(subprocess, "run", fake_run)
    worker = staging_worker(tmp_path, monkeypatch, [dict(GIT_ENTRY)])

    with pytest.raises(RuntimeError, match="fatal: not found"):
        worker._stage_custom_nodes(staging_job(worker))


def test_an_already_present_pack_is_skipped(tmp_path, monkeypatch):
    fake = FakeRequests()
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)
    existing = pack_path(tmp_path, "eric-qwen-layer")
    existing.mkdir(parents=True)
    (existing / "__init__.py").write_bytes(b"# installed by hand\n")

    worker._stage_custom_nodes(staging_job(worker))

    assert fake.urls == []
    assert (existing / "__init__.py").read_bytes() == b"# installed by hand\n"


def test_staging_runs_once_across_jobs(tmp_path, monkeypatch):
    fake = FakeRequests()
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)

    worker._stage_custom_nodes(staging_job(worker))
    worker._stage_custom_nodes(staging_job(worker))

    assert len(fake.urls) == 2


def test_staging_emits_events_in_the_weight_staging_band(tmp_path, monkeypatch):
    fake = FakeRequests()
    worker = staging_worker(
        tmp_path,
        monkeypatch,
        [dict(REGISTRY_ENTRY), {**REGISTRY_ENTRY, "registry_id": "second-pack"}],
        fake,
    )
    job = staging_job(worker)

    worker._stage_custom_nodes(job)

    events = [item["event"] for item in worker.queue.list_events(job.id)]
    assert [event["type"] for event in events] == ["node_pack_staging"] * 3
    assert [event["pack_id"] for event in events] == [
        "eric-qwen-layer",
        "second-pack",
        None,
    ]
    assert [event["source"] for event in events] == ["registry", "registry", None]
    assert [event["downloaded_packs"] for event in events] == [0, 1, 2]
    assert all(event["total_packs"] == 2 for event in events)
    # Between runner_starting (2) and running (10), like weight staging.
    progresses = [event["overall_progress"] for event in events]
    assert progresses == sorted(progresses)
    assert progresses[0] == 3 and progresses[-1] == 9


def test_requirements_are_installed_and_their_output_captured(tmp_path, monkeypatch):
    fake = FakeRequests(
        archive=pack_zip(
            {"__init__.py": PACK_SOURCE, "requirements.txt": b"psd-tools>=1.9.0\n"}
        )
    )
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)
    commands = []

    def fake_run(arguments, **kwargs):
        commands.append(arguments)
        return subprocess.CompletedProcess(
            arguments, 0, stdout="Successfully installed psd-tools-1.9.0\n", stderr=""
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    job = staging_job(worker)

    worker._stage_custom_nodes(job)

    assert commands[0][1:4] == ["-m", "pip", "install"]
    installed = [item["event"] for item in worker.queue.list_events(job.id)]
    requirements_event = next(
        event for event in installed if event["type"] == "node_pack_requirements"
    )
    assert requirements_event["pack_id"] == "eric-qwen-layer"
    assert "Successfully installed psd-tools" in requirements_event["output"]


def test_a_failed_requirements_install_fails_the_job(tmp_path, monkeypatch):
    fake = FakeRequests(
        archive=pack_zip({"__init__.py": PACK_SOURCE, "requirements.txt": b"nope\n"})
    )
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda arguments, **kwargs: subprocess.CompletedProcess(
            arguments, 1, stdout="", stderr="ERROR: No matching distribution\n"
        ),
    )

    with pytest.raises(RuntimeError, match="No matching distribution"):
        worker._stage_custom_nodes(staging_job(worker))

    assert worker._custom_nodes_staged is False


def test_requirements_are_skipped_when_the_profile_says_so(tmp_path, monkeypatch):
    fake = FakeRequests(
        archive=pack_zip({"__init__.py": PACK_SOURCE, "requirements.txt": b"torch\n"})
    )
    worker = staging_worker(
        tmp_path, monkeypatch, [{**REGISTRY_ENTRY, "install_requirements": False}], fake
    )
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("pip must not run when disabled"),
    )

    worker._stage_custom_nodes(staging_job(worker))

    assert pack_path(tmp_path, "eric-qwen-layer", "requirements.txt").is_file()


def test_a_worker_with_no_declared_packs_does_nothing(tmp_path, monkeypatch):
    worker = staging_worker(tmp_path, monkeypatch, [])

    worker._stage_custom_nodes(staging_job(worker))

    assert worker._custom_nodes_staged is True
    assert not pack_path(tmp_path).exists()


def test_the_worker_rejects_a_malformed_custom_nodes_env(monkeypatch):
    monkeypatch.setenv("CLOUD_OFFLOAD_CUSTOM_NODES", "{not json")
    with pytest.raises(RuntimeError, match="not valid JSON"):
        Worker._load_custom_nodes_env()

    monkeypatch.setenv("CLOUD_OFFLOAD_CUSTOM_NODES", '{"registry_id": "pack"}')
    with pytest.raises(RuntimeError, match="JSON list"):
        Worker._load_custom_nodes_env()

    monkeypatch.delenv("CLOUD_OFFLOAD_CUSTOM_NODES")
    assert Worker._load_custom_nodes_env() == []


@pytest.mark.parametrize(
    "value, match",
    [
        ("", "is set but empty"),
        ("   ", "is set but empty"),
        ('[{"registry_id": "pack", "version": "0.1.0"}', "not valid JSON"),
        ('{"registry_id": "pack"}', "must be a JSON list"),
        ('["eric-qwen-layer"]', r"\[0\] must be a JSON object"),
    ],
)
def test_an_unusable_packs_variable_names_the_value_it_refused(monkeypatch, value, match):
    """A variable that cannot be read is a launch that thinks it configured a
    runner and did not. Yielding an empty list there is indistinguishable from a
    profile that declared no packs at all, which is precisely the silence that
    let a runner claim a job whose node types could never exist."""
    monkeypatch.setenv("CLOUD_OFFLOAD_CUSTOM_NODES", value)

    with pytest.raises(RuntimeError, match=match) as failure:
        Worker._load_custom_nodes_env()

    assert "CLOUD_OFFLOAD_CUSTOM_NODES" in str(failure.value)


def test_what_the_dispatcher_serializes_is_what_the_worker_reads(tmp_path, monkeypatch):
    """The whole channel, end to end, on the exact profile that failed in
    production: two git-pinned packs with explicit ids, serialized by the
    dispatcher's separators and parsed back by the worker."""
    from cloud_offload.dispatcher import Dispatcher

    declared = [
        {
            "id": "eric-qwen-layer",
            "git": "https://github.com/EricRollei/Qwen_Layers_Diffuser_Pipeline_Comfyui.git",
            "commit": "2be3bd30449771364af9a38d6ee55c6fa3d74724",
            "install_requirements": True,
        },
        {
            "id": "layerscope",
            "git": "https://github.com/jethac/layerscope.git",
            "commit": "9adb37b7c08b1c891900c4445be827740936e895",
            "install_requirements": True,
        },
    ]
    provider = LaunchProvider()
    config = packs_config(tmp_path, custom_nodes=declared)

    Dispatcher(config, provider=provider)._launch_worker("runpod", "comfyui")
    monkeypatch.setenv(
        "CLOUD_OFFLOAD_CUSTOM_NODES", provider.env_vars["CLOUD_OFFLOAD_CUSTOM_NODES"]
    )

    assert Worker._load_custom_nodes_env() == declared
    assert [profile_pack_identifier(entry) for entry in Worker._load_custom_nodes_env()] == [
        "eric-qwen-layer",
        "layerscope",
    ]


def test_a_skipped_staging_says_so_and_says_why(tmp_path, monkeypatch):
    """Silence is the bug. A second job on a runner that already staged its
    packs emitted nothing at all, which reads exactly like a runner that was
    never told to stage anything — the two are only distinguishable if the
    skip is stated."""
    fake = FakeRequests()
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)

    worker._stage_custom_nodes(staging_job(worker))
    second = staging_job(worker)
    worker._stage_custom_nodes(second)

    events = [item["event"] for item in worker.queue.list_events(second.id)]
    assert [event["type"] for event in events] == ["node_pack_staging"]
    assert events[0]["skipped"] == "already_staged"
    assert events[0]["total_packs"] == 1


def test_declaring_no_packs_is_stated_rather_than_assumed(tmp_path, monkeypatch):
    worker = staging_worker(tmp_path, monkeypatch, [])
    job = staging_job(worker)

    worker._stage_custom_nodes(job)

    events = [item["event"] for item in worker.queue.list_events(job.id)]
    assert [event["skipped"] for event in events] == ["none_declared"]
    assert events[0]["total_packs"] == 0


def test_a_pack_already_on_disk_is_reported_as_present(tmp_path, monkeypatch):
    """What the boot phase leaves behind: the first claimed job still stages,
    finds the directories there, and puts that in the job's own events."""
    fake = FakeRequests()
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)
    pack_path(tmp_path, "eric-qwen-layer").mkdir(parents=True)
    job = staging_job(worker)

    worker._stage_custom_nodes(job)

    events = [item["event"] for item in worker.queue.list_events(job.id)]
    assert events[0]["pack_id"] == "eric-qwen-layer"
    assert events[0]["present"] is True
    assert fake.urls == []


def test_staging_before_comfyui_exists_needs_no_job(tmp_path, monkeypatch):
    """The runner boot stages with nothing to attach events to, because ComfyUI
    has not started and no job has been claimed. It must still install."""
    fake = FakeRequests()
    worker = staging_worker(tmp_path, monkeypatch, [dict(REGISTRY_ENTRY)], fake)

    worker.stage_node_packs()

    assert pack_path(
        tmp_path, "eric-qwen-layer", "eric-qwen-layer", "__init__.py"
    ).read_bytes() == PACK_SOURCE
    assert worker._custom_nodes_staged is True
    # Nothing to attach progress to, so the worker record carries the only
    # signal there is: this pod is alive and still coming up.
    assert [item["status"] for item in worker.queue.list_active_workers()] == ["starting"]


def test_the_missing_message_reads_the_same_way_the_asset_one_does():
    assert missing_node_packs_message([QWEN_PACK]).startswith(
        "Cloud Offload cannot provide 1 custom node pack required by this partition:"
    )


# === A pack's name, its repository and its directory may all differ ===

def test_an_entry_may_state_which_pack_it_provides():
    # eric-qwen-layer ships from a repository called
    # Qwen_Layers_Diffuser_Pipeline_Comfyui, so a git entry that could only
    # answer to its URL would never match what ComfyUI reports.
    entries = normalized_profile_custom_nodes(
        "comfyui",
        [
            {
                "id": "eric-qwen-layer",
                "git": "https://github.com/EricRollei/Qwen_Layers_Diffuser_Pipeline_Comfyui.git",
                "commit": "2be3bd30449771364af9a38d6ee55c6fa3d74724",
            }
        ],
    )

    assert entries[0]["id"] == "eric-qwen-layer"
    assert profile_pack_identifier(entries[0]) == "eric-qwen-layer"


def test_an_explicit_id_satisfies_the_preflight_check():
    profile = {
        "custom_nodes": normalized_profile_custom_nodes(
            "comfyui",
            [
                {
                    "id": "eric-qwen-layer",
                    "git": "https://github.com/EricRollei/Qwen_Layers_Diffuser_Pipeline_Comfyui.git",
                    "commit": "2be3bd30449771364af9a38d6ee55c6fa3d74724",
                }
            ],
        )
    }
    required = [
        {"id": "eric-qwen-layer", "directory": "eric-qwen-layer", "version": "0.1.0", "digest": "d" * 64}
    ]

    assert missing_node_packs(required, profile) == []


def test_a_url_derived_name_still_works_without_an_explicit_id():
    entries = normalized_profile_custom_nodes(
        "comfyui",
        [{"git": "https://github.com/acme/ComfyUI-Widgets.git", "commit": "b" * 40}],
    )

    assert profile_pack_identifier(entries[0]) == "ComfyUI-Widgets"


def test_preflight_resolves_the_profile_by_capability(tmp_path, monkeypatch):
    """A client stamps the capability, not the operator's profile name.

    Reading the profile by the raw name returned None, so every declared pack
    read as missing and a correctly configured worker was refused.
    """
    from cloud_offload.router import resolve_worker_profile

    config = CloudConfig(
        provider="runpod",
        provider_order=["runpod"],
        queue_db_path=str(tmp_path / "queue.db"),
        worker_profiles={
            "comfyui": {
                "image": "registry.invalid/comfyui@sha256:" + "a" * 64,
                "models": ["comfyui-partition-v1"],
                "providers": ["runpod"],
                "custom_nodes": [
                    {"id": "layerscope", "git": "https://example.invalid/x.git", "commit": "c" * 40}
                ],
            }
        },
    )

    profile = resolve_worker_profile(config, "comfyui-partition-v1")

    assert profile is not None and profile["name"] == "comfyui"
    required = [
        {"id": "layerscope", "directory": "layerscope", "version": "0.1.0", "digest": "d" * 64}
    ]
    assert missing_node_packs(required, profile) == []
