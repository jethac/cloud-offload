import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "deploy" / "runtime-profiles" / "comfyui" / "Dockerfile.overlay"
ENTRYPOINT = ROOT / "deploy" / "runtime-profiles" / "comfyui" / "entrypoint.sh"
IMAGE = os.environ.get("CLOUD_OFFLOAD_TEST_IMAGE")


def _git(repo: Path, *args: str, text: bool = True):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=text)


def test_overlay_copies_current_entrypoint_and_makes_it_executable():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "COPY deploy/runtime-profiles/comfyui/entrypoint.sh "
        "/opt/cloud-offload/entrypoint.sh" in dockerfile
    )
    assert "chmod +x /opt/cloud-offload/entrypoint.sh" in dockerfile


def test_context_contains_exact_git_blobs_modes_and_symlinks(tmp_path):
    from scripts.build_worker_image import prepare_context, tree_entries

    repo = tmp_path / "source"
    repo.mkdir()
    _git(repo, "init", "--quiet")
    _git(repo, "config", "core.autocrlf", "true")
    (repo / "entrypoint.sh").write_bytes(b"#!/bin/bash\nexit 0\n")
    (repo / "entrypoint.sh").chmod(0o755)
    try:
        os.symlink("entrypoint.sh", repo / "current-entrypoint.sh")
    except OSError as exc:
        pytest.fail(f"symlink support is required for clean image contexts: {exc}")
    _git(repo, "add", ".")
    env = os.environ | {
        "GIT_AUTHOR_NAME": "image-test",
        "GIT_AUTHOR_EMAIL": "image-test@example.invalid",
        "GIT_COMMITTER_NAME": "image-test",
        "GIT_COMMITTER_EMAIL": "image-test@example.invalid",
    }
    subprocess.run(["git", "-C", str(repo), "commit", "--quiet", "-m", "fixture"], check=True, env=env)

    revision = _git(repo, "rev-parse", "HEAD").strip()
    destination = tmp_path / "context"
    assert prepare_context(repo, revision, destination) == (
        "current-entrypoint.sh",
        "entrypoint.sh",
    )
    entries = {entry.path: entry for entry in tree_entries(repo, revision)}
    for path, entry in entries.items():
        target = destination / path
        blob = bytes(_git(repo, "cat-file", "blob", entry.oid, text=False))
        if stat.S_ISLNK(entry.mode):
            assert target.is_symlink()
            assert os.fsencode(os.readlink(target)) == blob
        else:
            assert target.read_bytes() == blob
            # NTFS does not expose POSIX execute bits, but the builder still
            # applies the commit mode (and Dockerfile chmod handles entrypoint).
            if os.name != "nt":
                assert stat.S_IMODE(target.stat().st_mode) == stat.S_IMODE(entry.mode)
    assert (destination / "entrypoint.sh").read_bytes().startswith(b"#!/bin/bash\n")


def test_context_rejects_unsafe_git_tree_paths(tmp_path, monkeypatch):
    from scripts import build_worker_image

    monkeypatch.setattr(
        build_worker_image,
        "tree_entries",
        lambda *_args: (build_worker_image.TreeEntry("../escape", 0o100644, "blob", "0" * 40),),
    )
    with pytest.raises(ValueError, match="unsafe tracked path"):
        build_worker_image.prepare_context(ROOT, "HEAD", tmp_path / "context")


def test_context_is_contained_and_disjoint_from_source(tmp_path):
    from scripts.build_worker_image import prepare_context

    with pytest.raises(ValueError, match="disjoint"):
        prepare_context(ROOT, "HEAD", ROOT / ".context")
    with pytest.raises(ValueError, match="disjoint"):
        prepare_context(ROOT, "HEAD", ROOT.parent)


def _docker_create() -> str:
    return subprocess.check_output(["docker", "create", IMAGE], text=True).strip()


def _docker_cp_source(tmp_path: Path) -> Path:
    container = _docker_create()
    try:
        subprocess.run(["docker", "cp", f"{container}:/opt/cloud-offload/source", str(tmp_path)], check=True)
    finally:
        subprocess.run(["docker", "rm", "-f", container], check=True, stdout=subprocess.DEVNULL)
    return tmp_path / "source"


@pytest.mark.skipif(not IMAGE, reason="set CLOUD_OFFLOAD_TEST_IMAGE for image inspection")
def test_local_image_has_revision_label_and_exact_entrypoint_bytes(tmp_path):
    from scripts.build_worker_image import _blob_bytes, tree_entries

    revision = _git(ROOT, "rev-parse", "HEAD").strip()
    label = subprocess.check_output(
        ["docker", "image", "inspect", IMAGE, "--format", "{{index .Config.Labels \"org.opencontainers.image.revision\"}}"],
        text=True,
    ).strip()
    assert label == revision
    source = _docker_cp_source(tmp_path)
    entry = next(item for item in tree_entries(ROOT, revision) if item.path == ENTRYPOINT.relative_to(ROOT).as_posix())
    assert (source / entry.path).read_bytes() == _blob_bytes(ROOT, entry.oid)
    assert hashlib.sha256((source / entry.path).read_bytes()).hexdigest() == hashlib.sha256(_blob_bytes(ROOT, entry.oid)).hexdigest()
    assert (source / entry.path).read_bytes().startswith(b"#!/bin/bash\n")


@pytest.mark.skipif(not IMAGE, reason="set CLOUD_OFFLOAD_TEST_IMAGE for image inspection")
def test_local_image_source_tree_matches_exact_blobs_and_intended_omissions(tmp_path):
    from scripts.build_worker_image import _blob_bytes, tree_entries

    revision = _git(ROOT, "rev-parse", "HEAD").strip()
    entries = {entry.path: entry for entry in tree_entries(ROOT, revision)}
    source = _docker_cp_source(tmp_path)
    actual = tuple(sorted(path.relative_to(source).as_posix() for path in source.rglob("*") if path.is_file() or path.is_symlink()))
    ignored = {
        path for path in entries
        if path == ".github" or path.startswith(".github/")
        or path == "tests" or path.startswith("tests/")
        or path.startswith((".git/", ".runlogs/", ".worktrees/", ".pytest_cache/", ".venv/"))
        or "/__pycache__/" in f"/{path}/" or path.endswith(".pyc")
        or path.endswith(".egg-info") or ".egg-info/" in path
        or path.startswith(("build/", "dist/", "htmlcov/"))
    }
    assert set(actual) == set(entries) - ignored
    assert set(actual) >= {"cloud_offload/__init__.py", "cloud_offload/worker.py", "pyproject.toml"}
    for path in actual:
        target = source / path
        entry = entries[path]
        blob = _blob_bytes(ROOT, entry.oid)
        if target.is_symlink():
            assert os.fsencode(os.readlink(target)) == blob
        else:
            assert target.read_bytes() == blob


@pytest.mark.skipif(not IMAGE, reason="set CLOUD_OFFLOAD_TEST_IMAGE for image inspection")
def test_local_image_runtime_profile_is_the_declared_profile(tmp_path):
    container = _docker_create()
    try:
        destination = tmp_path
        output = subprocess.check_output(
            ["docker", "cp", f"{container}:/opt/cloud-offload/runtime-profile.json", str(destination)],
            text=True,
        )
    finally:
        subprocess.run(["docker", "rm", "-f", container], check=True, stdout=subprocess.DEVNULL)
    profile = json.loads((destination / "runtime-profile.json").read_text(encoding="utf-8"))
    expected = json.loads((ROOT / "deploy/runtime-profiles/comfyui/runtime-profile.json").read_text(encoding="utf-8"))
    assert profile == expected


@pytest.mark.skipif(not IMAGE, reason="set CLOUD_OFFLOAD_TEST_IMAGE for image smoke")
def test_local_image_starts_through_its_oci_entrypoint(tmp_path):
    """Run the actual entrypoint with supported local queue/root configuration."""

    name = "cloud-offload-image-smoke"
    comfyui_root = tmp_path / "ComfyUI"
    comfyui_root.mkdir()
    (comfyui_root / "main.py").write_text(
        "from http.server import BaseHTTPRequestHandler, HTTPServer\n"
        "class Handler(BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        self.send_response(200)\n"
        "        self.end_headers()\n"
        "        self.wfile.write(b'{}')\n"
        "    def log_message(self, *_args):\n"
        "        pass\n"
        "HTTPServer(('127.0.0.1', 8188), Handler).serve_forever()\n",
        encoding="utf-8",
    )
    command = [
        "docker", "run", "--name", name,
        "--mount", f"type=bind,source={comfyui_root},target=/tmp/smoke-comfyui,readonly",
        "-e", "CLOUD_OFFLOAD_QUEUE_DB=/tmp/cloud-offload-smoke.db",
        "-e", "CLOUD_OFFLOAD_COMFYUI_ROOT=/tmp/smoke-comfyui",
        "-e", "CLOUD_OFFLOAD_COMFYUI_READY_TIMEOUT=15", "-e", "CLOUD_OFFLOAD_POLL_INTERVAL=1",
        "-e", "CLOUD_OFFLOAD_WORKER_MODELS=comfyui-workflow", IMAGE,
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        try:
            process.wait(timeout=30)
            output = process.stdout.read() if process.stdout else ""
        except subprocess.TimeoutExpired:
            subprocess.run(["docker", "rm", "-f", name], check=False, stdout=subprocess.DEVNULL)
            output, _ = process.communicate(timeout=10)
        assert "ComfyUI is ready; handing over to the worker loop" in output
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
