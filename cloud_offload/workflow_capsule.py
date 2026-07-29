"""Canonical whole-workflow contract for paid ComfyUI execution.

A workflow capsule is the immutable closure that Cloud Offload can inspect
before it rents a GPU.  It names the graph, its runner limits, every known
model and custom-node dependency, its input and output contract, and any
dynamic behavior that cannot be proved from the graph.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import PurePosixPath
from typing import Any

from cloud_offload.assets import normalized_partition_assets
from cloud_offload.node_packs import normalized_partition_node_packs


WORKFLOW_CAPSULE_SCHEMA = "comfy.workflow.capsule.v1"
_INPUT_KINDS = frozenset({"image", "file", "binary"})
_OUTPUT_KINDS = frozenset({"image", "3d", "file", "any"})


def _safe_filename(value: Any, label: str) -> str:
    filename = str(value or "").strip()
    path = PurePosixPath(filename.replace("\\", "/"))
    if not filename or path.is_absolute() or len(path.parts) != 1 or ".." in path.parts:
        raise ValueError(f"{label} must be one safe file name")
    return filename


def _sha256(value: Any, label: str) -> str:
    digest = str(value or "").strip().lower()
    if digest.startswith("sha256:"):
        digest = digest[7:]
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"{label} must be a sha256 digest")
    return "sha256:" + digest


def _normalized_inputs(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Workflow capsule inputs must be a list")
    entries: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, raw in enumerate(value):
        label = f"Workflow capsule inputs[{index}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{label} must be an object")
        name = _safe_filename(raw.get("name"), f"{label}.name")
        if name in names:
            raise ValueError(f"Workflow capsule input name is not unique: {name}")
        names.add(name)
        kind = str(raw.get("kind") or "file").strip().lower()
        if kind not in _INPUT_KINDS:
            raise ValueError(
                f"{label}.kind must be one of {', '.join(sorted(_INPUT_KINDS))}"
            )
        entries.append(
            {
                "name": name,
                "kind": kind,
                "required": bool(raw.get("required", True)),
                **(
                    {"media_type": str(raw["media_type"]).strip()}
                    if str(raw.get("media_type") or "").strip()
                    else {}
                ),
            }
        )
    return sorted(entries, key=lambda item: item["name"])


def _normalized_outputs(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Workflow capsule outputs must be a list")
    entries: list[dict[str, Any]] = []
    for index, raw in enumerate(value):
        label = f"Workflow capsule outputs[{index}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{label} must be an object")
        node_id = str(raw.get("node_id") or "").strip()
        if not node_id:
            raise ValueError(f"{label}.node_id is required")
        kind = str(raw.get("kind") or "any").strip().lower()
        if kind not in _OUTPUT_KINDS:
            raise ValueError(
                f"{label}.kind must be one of {', '.join(sorted(_OUTPUT_KINDS))}"
            )
        name = str(raw.get("name") or "").strip()
        entries.append(
            {
                "node_id": node_id,
                "kind": kind,
                "required": bool(raw.get("required", True)),
                **({"name": name} if name else {}),
            }
        )
    return sorted(
        entries,
        key=lambda item: (item["node_id"], item["kind"], item.get("name", "")),
    )


def _normalized_dynamic_behavior(value: Any) -> dict[str, Any]:
    if value is None:
        return {"declared": False, "requirements": []}
    if not isinstance(value, dict):
        raise ValueError("Workflow capsule dynamic_behavior must be an object")
    requirements = value.get("requirements") or []
    if not isinstance(requirements, list):
        raise ValueError("Workflow capsule dynamic requirements must be a list")
    normalized: list[dict[str, str]] = []
    for index, raw in enumerate(requirements):
        label = f"Workflow capsule dynamic_behavior.requirements[{index}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{label} must be an object")
        description = str(raw.get("description") or "").strip()
        if not description:
            raise ValueError(f"{label}.description is required")
        node_id = str(raw.get("node_id") or "").strip()
        normalized.append(
            {
                **({"node_id": node_id} if node_id else {}),
                "description": description,
            }
        )
    return {
        "declared": bool(value.get("declared", True)),
        "requirements": sorted(
            normalized,
            key=lambda item: (item.get("node_id", ""), item["description"]),
        ),
    }


def _normalized_environment(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Workflow capsule environment must be an object")
    allowed = {"object_info_digest", "dependency_lock_digest"}
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(
            "Workflow capsule environment has unsupported fields: " + ", ".join(unknown)
        )
    normalized: dict[str, Any] = {}
    for field in sorted(allowed):
        if value.get(field):
            normalized[field] = _sha256(value[field], f"Workflow capsule environment.{field}")
    return normalized


def normalize_workflow_capsule(capsule: Any) -> dict[str, Any]:
    """Validate a capsule and return its deterministic execution closure."""

    if not isinstance(capsule, dict):
        raise ValueError("Workflow capsule must be an object")
    if capsule.get("schema") != WORKFLOW_CAPSULE_SCHEMA:
        raise ValueError(f"Workflow capsule schema must be {WORKFLOW_CAPSULE_SCHEMA}")
    allowed = {
        "schema",
        "workflow",
        "runner",
        "residency",
        "assets",
        "node_packs",
        "inputs",
        "outputs",
        "environment",
        "dynamic_behavior",
    }
    unknown = sorted(set(capsule) - allowed)
    if unknown:
        raise ValueError("Workflow capsule has unsupported fields: " + ", ".join(unknown))

    workflow = capsule.get("workflow")
    if not isinstance(workflow, dict) or not workflow:
        raise ValueError("Workflow capsule workflow must be a non-empty object")
    for node_id, node in workflow.items():
        if not str(node_id).strip() or not isinstance(node, dict):
            raise ValueError("Workflow capsule contains an invalid node")
        if not str(node.get("class_type") or "").strip():
            raise ValueError(f"Workflow capsule node {node_id} has no class_type")
        if not isinstance(node.get("inputs", {}), dict):
            raise ValueError(f"Workflow capsule node {node_id} inputs must be an object")

    runner = capsule.get("runner") or {}
    if not isinstance(runner, dict):
        raise ValueError("Workflow capsule runner must be an object")
    profile = str(runner.get("profile") or "comfyui").strip()[:100]
    if not profile.startswith("comfyui"):
        raise ValueError("Workflow capsule runner profile must be a ComfyUI profile")
    try:
        min_gpu_ram_gb = int(runner.get("min_gpu_ram_gb") or 16)
    except (TypeError, ValueError) as exc:
        raise ValueError("Workflow capsule GPU memory must be an integer") from exc
    if not 1 <= min_gpu_ram_gb <= 256:
        raise ValueError("Workflow capsule GPU memory must be from 1 through 256 GiB")
    gpu_type = str(runner.get("gpu_type") or "any").strip()[:100] or "any"

    residency = str(capsule.get("residency") or "cloud").strip().lower()
    if residency not in {"cloud", "on-prem"}:
        raise ValueError("Workflow capsule residency must be cloud or on-prem")

    assets = normalized_partition_assets(capsule.get("assets"))
    packs = normalized_partition_node_packs(capsule.get("node_packs"))
    return {
        "schema": WORKFLOW_CAPSULE_SCHEMA,
        "workflow": workflow,
        "runner": {
            "profile": profile,
            "gpu_type": gpu_type,
            "min_gpu_ram_gb": min_gpu_ram_gb,
        },
        "residency": residency,
        "assets": sorted(assets, key=lambda item: (item["sha256"], item["filename"])),
        "node_packs": sorted(packs, key=lambda item: (item["digest"], item["id"])),
        "inputs": _normalized_inputs(capsule.get("inputs")),
        "outputs": _normalized_outputs(capsule.get("outputs")),
        "environment": _normalized_environment(capsule.get("environment")),
        "dynamic_behavior": _normalized_dynamic_behavior(
            capsule.get("dynamic_behavior")
        ),
    }


def workflow_capsule_digest(capsule: Any) -> str:
    """Return the stable identity of one normalized workflow closure."""

    normalized = normalize_workflow_capsule(capsule)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def capsule_partition(capsule: Any) -> dict[str, Any]:
    """Adapt a capsule to the common readiness and placement engine."""

    normalized = normalize_workflow_capsule(capsule)
    digest = workflow_capsule_digest(normalized)
    return {
        "schema": "comfy.partition.job.v1",
        "partition_id": "workflow-" + digest.removeprefix("sha256:")[:24],
        "workflow": normalized["workflow"],
        "runner": normalized["runner"],
        "residency": normalized["residency"],
        "assets": normalized["assets"],
        "node_packs": normalized["node_packs"],
    }
