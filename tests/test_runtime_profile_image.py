"""Drift checks for the reviewed M7 worker image pin."""

import json
import os
import re
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PIN_PATH = ROOT / "deploy" / "runtime-profiles" / "comfyui" / "image-pin.json"
PROFILE_PATH = ROOT / "deploy" / "runtime-profiles" / "comfyui" / "runtime-profile.json"
IMAGE_REPOSITORY = "ghcr.io/jethac/cloud-offload-worker-comfyui"
SOURCE_REVISION = "06a54909f7aeb430ffe59542deb3d4c29fc7f973"
IMAGE_TAG = "m7-06a5490"
IMAGE_DIGEST = "sha256:1039f1e218587b4a08eb6dabd8d4e47e722c0b808d6457fd8922072dfe9c24b1"


def _pin():
    return json.loads(PIN_PATH.read_text(encoding="utf-8"))


def test_m7_worker_image_pin_is_immutable_and_built_from_merged_main():
    pin = _pin()

    assert pin["image"] == f"{IMAGE_REPOSITORY}@{IMAGE_DIGEST}"
    assert pin["tag"] == IMAGE_TAG
    assert pin["source_revision"] == SOURCE_REVISION
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", IMAGE_DIGEST)
    assert "239446" not in PIN_PATH.read_text(encoding="utf-8")


def test_m7_worker_image_pin_matches_baked_runtime_profile_contract():
    pin = _pin()
    profile = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))

    assert pin["runtime_profile"] == profile["profile"]
    assert pin["platform"] == profile["platform"]
    assert pin["python_abi"] == profile["python_abi"]
    assert pin["capabilities"] == profile["models"]
    assert pin["partition_protocol"] == profile["partition_protocol"]


@pytest.mark.skipif(
    not os.environ.get("CLOUD_OFFLOAD_REGISTRY_IMAGE"),
    reason="set CLOUD_OFFLOAD_REGISTRY_IMAGE for live registry drift inspection",
)
def test_m7_worker_registry_tag_resolves_to_pinned_digest():
    image = os.environ["CLOUD_OFFLOAD_REGISTRY_IMAGE"]
    assert image == f"{IMAGE_REPOSITORY}:{IMAGE_TAG}"

    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", image],
        check=True,
        capture_output=True,
        text=True,
    )
    digest_lines = [
        line.strip().split(None, 1)[1]
        for line in result.stdout.splitlines()
        if line.strip().startswith("Digest:")
    ]
    assert digest_lines == [IMAGE_DIGEST]
