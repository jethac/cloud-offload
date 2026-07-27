"""Worker runtime-profile helpers.

Cloud Offload is model-agnostic: the coordinator never loads a 3D model.
Generation rides inside the submitted subgraph, so the only job "models" a
worker claims are the ComfyUI workflow capabilities below. Profiles are
declared in configuration (``worker_profiles``) and baked into runner images as
``runtime-profile.json`` manifests.
"""

from __future__ import annotations

import json
import math
from pathlib import Path, PureWindowsPath
from typing import Any

# The capabilities a ComfyUI runner can claim. ``comfyui-workflow`` runs an
# arbitrary API-format workflow; ``comfyui-partition-v1`` runs a compiled
# subgraph with typed boundary artifacts.
WORKFLOW_CAPABILITIES = frozenset({"comfyui-workflow", "comfyui-partition-v1"})


def configured_worker_profiles(config: Any) -> dict[str, dict[str, Any]]:
    """Return normalized, explicitly configured cloud worker profiles.

    A profile is only usable when it pins its image by digest (``@sha256:``) and
    declares at least one known workflow capability.
    """
    result: dict[str, dict[str, Any]] = {}
    for name, value in (getattr(config, "worker_profiles", {}) or {}).items():
        if not isinstance(value, dict):
            continue
        image = str(value.get("image") or "").strip()
        models = [
            str(item)
            for item in value.get("models", [])
            if str(item) in WORKFLOW_CAPABILITIES
        ]
        providers = [
            "vast.ai" if str(item).lower() == "vast" else str(item).lower()
            for item in value.get("providers", ["runpod", "vast.ai"])
        ]
        if not image or "@sha256:" not in image or not models:
            continue
        result[str(name)] = {
            "name": str(name),
            "image": image,
            "models": models,
            "providers": list(dict.fromkeys(providers)),
            "gpu_type": str(value.get("gpu_type") or "").strip(),
            "min_gpu_ram_gb": float(value.get("min_gpu_ram_gb") or 0),
            "wheelhouse_url": str(value.get("wheelhouse_url") or ""),
            "wheelhouse_sha256": str(value.get("wheelhouse_sha256") or ""),
            "weights": normalized_profile_weights(str(name), value.get("weights")),
        }
    return result


def require_models_relative(label: str, field: str, value: str) -> None:
    """Reject absolute paths and upward traversal in a models-relative path."""
    # PureWindowsPath also parses forward slashes and catches drive letters, so
    # one check covers both separator styles regardless of the host OS.
    pure = PureWindowsPath(value)
    if pure.is_absolute() or pure.drive or value.startswith(("/", "\\")):
        raise ValueError(f"{label}: {field} must be a relative path, got {value!r}")
    if ".." in pure.parts:
        raise ValueError(f"{label}: {field} must not traverse upward, got {value!r}")


def normalized_profile_weights(name: str, entries: Any) -> list[dict[str, Any]]:
    """Validate and normalize a profile's optional pinned ``weights`` list.

    Every entry names a Hugging Face repo at a pinned revision and a destination
    under the runner's ComfyUI models directory; ``files: null`` means the whole
    snapshot. Invalid entries raise instead of being dropped: a profile that
    believes it stages weights but silently does not would fail every job at
    runtime with a far less useful error.
    """
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise ValueError(f"Worker profile {name!r}: weights must be a list")
    normalized: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        label = f"Worker profile {name!r} weights[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{label} must be an object")
        repo_id = str(entry.get("repo_id") or "").strip()
        if not repo_id:
            raise ValueError(f"{label}: repo_id is required")
        revision = str(entry.get("revision") or "").strip()
        if not revision:
            raise ValueError(
                f"{label}: revision is required; pin a commit hash, not a branch"
            )
        files = entry.get("files")
        if files is not None:
            if (
                not isinstance(files, list)
                or not files
                or not all(isinstance(item, str) and item.strip() for item in files)
            ):
                raise ValueError(
                    f"{label}: files must be null (whole snapshot) or a list of file paths"
                )
            files = [item.strip() for item in files]
            for item in files:
                require_models_relative(label, "files entry", item)
        dest = str(entry.get("dest") or "").strip()
        if not dest:
            raise ValueError(
                f"{label}: dest is required (a subdirectory of the ComfyUI models dir)"
            )
        require_models_relative(label, "dest", dest)
        normalized.append(
            {
                "repo_id": repo_id,
                "revision": revision,
                "files": files,
                "dest": dest,
                "gated": bool(entry.get("gated", False)),
            }
        )
    return normalized


def worker_profile_gpu_type(profile: dict[str, Any], default: str | None = None) -> str | None:
    """Return the GPU type filter for a worker profile."""
    gpu_type = str(profile.get("gpu_type") or default or "").strip()
    if not gpu_type:
        return None
    if gpu_type.lower() == "any":
        return None
    if gpu_type.lower() == "vast":
        return "vast.ai"
    return gpu_type


def worker_profile_min_gpu_ram(profile: dict[str, Any]) -> int:
    """Return the minimum GPU RAM a profile should be scheduled against."""
    return int(math.ceil(float(profile.get("min_gpu_ram_gb") or 0)))


def cloud_profiles_for_model(
    config: Any, model_name: str, provider: str | None = None
) -> list[dict[str, Any]]:
    """Return configured profiles that can run ``model_name`` on ``provider``."""
    profiles = []
    normalized_provider = "vast.ai" if provider == "vast" else provider
    for profile in configured_worker_profiles(config).values():
        if model_name not in profile["models"]:
            continue
        if normalized_provider and normalized_provider not in profile["providers"]:
            continue
        profiles.append(profile)
    return profiles


def load_worker_manifest(path: str | Path) -> dict[str, Any]:
    """Load and minimally validate a capability manifest baked into an image."""
    manifest_path = Path(path)
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    profile = str(data.get("profile") or "").strip()
    models = [
        str(item)
        for item in data.get("models", [])
        if str(item) in WORKFLOW_CAPABILITIES
    ]
    if not profile or not models:
        raise ValueError(f"Invalid worker capability manifest: {manifest_path}")
    return {
        "profile": profile,
        "models": list(dict.fromkeys(models)),
        "version": str(data.get("version") or ""),
        "partition_protocol": str(data.get("partition_protocol") or ""),
        "source": str(manifest_path),
    }
