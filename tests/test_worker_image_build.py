import hashlib
import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "deploy" / "runtime-profiles" / "comfyui" / "Dockerfile.overlay"
ENTRYPOINT = ROOT / "deploy" / "runtime-profiles" / "comfyui" / "entrypoint.sh"
IMAGE = os.environ.get("CLOUD_OFFLOAD_TEST_IMAGE")


def test_overlay_copies_current_entrypoint_and_makes_it_executable():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "COPY deploy/runtime-profiles/comfyui/entrypoint.sh "
        "/opt/cloud-offload/entrypoint.sh" in dockerfile
    )
    assert "chmod +x /opt/cloud-offload/entrypoint.sh" in dockerfile


def test_archived_context_contains_exactly_the_tracked_source_tree(tmp_path):
    from scripts.build_worker_image import prepare_context

    manifest = prepare_context(ROOT, "HEAD", tmp_path / "context")
    expected = subprocess.check_output(
        ["git", "-C", str(ROOT), "ls-tree", "-r", "--name-only", "HEAD"],
        text=True,
    ).splitlines()

    assert manifest == tuple(expected)
    actual = tuple(
        sorted(
            path.relative_to(tmp_path / "context").as_posix()
            for path in (tmp_path / "context").rglob("*")
            if path.is_file()
        )
    )
    assert actual == manifest
    assert not any(
        path.startswith((".superpowers/", ".ruff_cache/", "build/"))
        or path.endswith(".egg-info")
        or "/.egg-info/" in path
        for path in actual
    )


def test_archived_context_rejects_unsafe_archive_members(tmp_path, monkeypatch):
    from scripts import build_worker_image

    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as tar:
        info = tarfile.TarInfo("../escape.txt")
        info.size = 0
        tar.addfile(info)

    monkeypatch.setattr(
        build_worker_image,
        "_archive_bytes",
        lambda *_args: archive.getvalue(),
    )

    with pytest.raises(ValueError, match="unsafe archive path"):
        build_worker_image.prepare_context(ROOT, "HEAD", tmp_path / "context")


@pytest.mark.skipif(not IMAGE, reason="set CLOUD_OFFLOAD_TEST_IMAGE for image inspection")
def test_local_image_uses_the_current_entrypoint_content():
    expected = ENTRYPOINT.read_bytes()
    expected_hash = hashlib.sha256(expected).hexdigest()
    result = subprocess.check_output(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "/bin/sh",
            IMAGE,
            "-c",
            "sha256sum /opt/cloud-offload/entrypoint.sh && cat /opt/cloud-offload/entrypoint.sh",
        ],
        text=True,
    )

    assert result.splitlines()[0].split()[0] == expected_hash
    assert "cloud-offload runner-boot" in result
    assert "PYTHONPATH" in result


@pytest.mark.skipif(not IMAGE, reason="set CLOUD_OFFLOAD_TEST_IMAGE for image inspection")
def test_local_image_source_tree_matches_the_clean_context_manifest():
    from scripts.build_worker_image import tracked_manifest

    result = subprocess.check_output(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "/bin/sh",
            IMAGE,
            "-c",
            "find /opt/cloud-offload/source -type f -printf '%P\\n' | sort",
        ],
        text=True,
    )
    image_files = tuple(line for line in result.splitlines() if line)

    assert image_files == tracked_manifest(ROOT, "HEAD")
    assert not any(
        path.startswith((".superpowers/", ".ruff_cache/", "build/"))
        or ".egg-info/" in path
        or path.endswith(".egg-info")
        for path in image_files
    )


@pytest.mark.skipif(not IMAGE, reason="set CLOUD_OFFLOAD_TEST_IMAGE for image inspection")
def test_local_image_runtime_profile_is_the_declared_profile():
    result = subprocess.check_output(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "/bin/sh",
            IMAGE,
            "-c",
            "cat /opt/cloud-offload/runtime-profile.json",
        ],
        text=True,
    )
    profile = json.loads(result)
    expected = json.loads(
        (ROOT / "deploy/runtime-profiles/comfyui/runtime-profile.json").read_text(
            encoding="utf-8"
        )
    )

    assert profile == expected
