"""The strict, provider-neutral cloud plan protocol.

The public plan record is intentionally a projection.  The execution plan is
kept in a separate authority table and is never placed in a queue response.
Paid acceptance is completed by :meth:`JobQueue.submit_plan_atomic`, which
uses this module's schema helpers while inserting the queue row.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

SCHEMA = "comfy.workflow.plan.v1"
PREFLIGHT_SCHEMA = "cloud-offload.plan-preflight.v1"
MAX_STAGES = 64
MAX_FAN_OUT = 32
MAX_INPUTS = 256
MAX_OUTPUTS = 256
MAX_MEDIA_TYPE = 256
MAX_ARTIFACT_SIZE = 2**63 - 1
PLAN_AUTHORITY_SCHEMA_VERSION = "cloud-offload.plan-authority.v2"
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_DIGEST = re.compile(r"^(?:sha256:)?[0-9a-fA-F]{64}$")
_MEDIA = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,127}/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,127}$")
_ROOT = {
    "schema",
    "plan_id",
    "plan_digest",
    "project_id",
    "input_revision",
    "operation",
    "input_artifacts",
    "stages",
    "final_outputs",
    "policy",
}
_KINDS = frozenset({"workflow", "tool", "validation", "document_commit"})
_ROLES = frozenset({"input", "output", "intermediate", "checkpoint", "manifest"})
_OPERATIONS = frozenset(
    {"render", "generate", "upscale", "convert", "validate", "offline-render", "transcode", "train"}
)
_TERMINAL_STATES = frozenset({"cancelled", "completed", "failed", "terminal"})
_PLAN_PUBLIC_EVENT_TYPES = frozenset(
    {"job_created", "job_state_seeded", "job_status_changed", "cancellation_requested"}
)
_PLAN_PUBLIC_PHASES = frozenset(
    {"readiness", "provisioning", "worker_boot", "dependency_preparation",
     "execution", "result_transfer", "resource_closure", "failure"}
)
_PLAN_PUBLIC_STATUSES = frozenset(
    {"preflighted", "submitting", "submitted", "running", "cancelling",
     "cancelled", "completed", "failed", "terminal"}
)
_CLOSURE_REASON_CODES = frozenset(
    {"cancelled", "completed", "worker_failed", "provider_absent", "unknown"}
)


class PlanError(ValueError):
    """A safe protocol or binding error."""


def canonical_bytes(value: Any) -> bytes:
    """Encode strict JSON with one deterministic representation."""

    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise PlanError("Plan must be strict JSON") from exc


def canonical_plan_digest(plan: dict[str, Any]) -> str:
    if not isinstance(plan, dict):
        raise PlanError("Plan must be an object")
    value = dict(plan)
    value["plan_digest"] = ""
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise PlanError(f"{label} is invalid")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise PlanError(f"{label} must be a sha256 digest")
    return "sha256:" + value.removeprefix("sha256:").lower()


def _finite_number(value: Any, label: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PlanError(f"{label} must be a number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise PlanError(f"{label} is outside safe bounds") from exc
    if not math.isfinite(number) or number < minimum:
        raise PlanError(f"{label} is outside safe bounds")
    return number


def _media(value: Any, label: str) -> str:
    if not isinstance(value, str) or len(value) > MAX_MEDIA_TYPE or not _MEDIA.fullmatch(value):
        raise PlanError(f"{label} is invalid")
    return value


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlanError(f"{label} must be an object")
    canonical_bytes(value)
    return value


def _contract(item: Any, label: str) -> dict[str, Any]:
    allowed = {"name", "role", "media_type", "logical_object", "units", "coordinate_system"}
    if not isinstance(item, dict) or set(item) - allowed:
        raise PlanError(f"{label} is invalid")
    name = _identifier(item.get("name"), f"{label}.name")
    role = item.get("role")
    if not isinstance(role, str) or role not in _ROLES:
        raise PlanError(f"{label}.role is invalid")
    media_type = _media(item.get("media_type"), f"{label}.media_type")
    result = {"name": name, "role": role, "media_type": media_type}
    for field in ("logical_object", "units", "coordinate_system"):
        if field in item:
            if not isinstance(item[field], str) or not item[field].strip():
                raise PlanError(f"{label}.{field} is invalid")
            result[field] = item[field]
    return result


def _validate_input(item: Any, index: int) -> dict[str, Any]:
    label = f"input_artifacts[{index}]"
    allowed = {"name", "filename", "path", "sha256", "size", "role", "media_type"}
    if not isinstance(item, dict) or set(item) - allowed:
        raise PlanError(f"{label} is invalid")
    name = _identifier(item.get("name"), f"{label}.name")
    role = item.get("role")
    if not isinstance(role, str) or role not in _ROLES:
        raise PlanError(f"{label}.role is invalid")
    media_type = _media(item.get("media_type"), f"{label}.media_type")
    digest = _digest(item.get("sha256"), f"{label}.sha256")
    result: dict[str, Any] = {"name": name, "role": role, "media_type": media_type, "sha256": digest}
    if "size" in item:
        size = item["size"]
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or not 0 <= size <= MAX_ARTIFACT_SIZE
        ):
            raise PlanError(f"{label}.size is invalid")
        result["size"] = size
    for field in ("filename", "path"):
        if field in item:
            if not isinstance(item[field], str) or not item[field].strip():
                raise PlanError(f"{label}.{field} is invalid")
            result[field] = item[field]
    return result


def validate_cloud_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Validate the complete plan, including all typed graph links."""

    if not isinstance(plan, dict):
        raise PlanError("Plan must be an object")
    canonical_bytes(plan)
    if set(plan) != _ROOT or plan.get("schema") != SCHEMA:
        raise PlanError("Plan root or schema is invalid")
    for field in ("plan_id", "project_id", "input_revision"):
        _identifier(plan.get(field), field)
    if not isinstance(plan.get("operation"), str) or plan["operation"] not in _OPERATIONS:
        raise PlanError("operation is invalid")

    inputs = plan.get("input_artifacts")
    if not isinstance(inputs, list) or not 1 <= len(inputs) <= MAX_INPUTS:
        raise PlanError("input_artifacts must be a non-empty list")
    declared_inputs: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(inputs):
        normalized = _validate_input(item, index)
        if normalized["name"] in declared_inputs:
            raise PlanError("input artifact identity is not unique")
        declared_inputs[normalized["name"]] = normalized

    stages = plan.get("stages")
    if not isinstance(stages, list) or not 1 <= len(stages) <= MAX_STAGES:
        raise PlanError("stage count is outside safe bounds")
    stage_ids: list[str] = []
    for index, stage in enumerate(stages):
        if not isinstance(stage, dict):
            raise PlanError(f"stage[{index}] is invalid")
        stage_ids.append(_identifier(stage.get("id"), f"stage[{index}].id"))
    if len(set(stage_ids)) != len(stage_ids):
        raise PlanError("stage ids are not unique")
    stage_set = set(stage_ids)
    graph: dict[str, list[str]] = {}
    outputs: dict[str, dict[str, dict[str, Any]]] = {}

    for stage, sid in zip(stages, stage_ids):
        allowed = {
            "id", "kind", "depends_on", "capsule", "operation", "settings", "inputs", "outputs",
            "runner", "retry", "checkpoint", "fan_out", "rules", "max_cost_usd",
        }
        if set(stage) - allowed or stage.get("kind") not in _KINDS:
            raise PlanError(f"stage {sid} has invalid fields")
        deps = stage.get("depends_on")
        if not isinstance(deps, list) or any(not isinstance(dep, str) for dep in deps) or len(deps) != len(set(deps)):
            raise PlanError(f"stage {sid} dependencies are invalid")
        if any(not isinstance(dep, str) or dep not in stage_set or dep == sid for dep in deps):
            raise PlanError(f"stage {sid} has an unknown dependency")
        graph[sid] = list(deps)

        if stage["kind"] == "workflow":
            if not isinstance(stage.get("capsule"), dict):
                raise PlanError(f"stage {sid} capsule is required")
            try:
                from cloud_offload.workflow_capsule import normalize_workflow_capsule

                normalize_workflow_capsule(stage["capsule"])
            except (TypeError, ValueError) as exc:
                raise PlanError(f"stage {sid} capsule is invalid") from exc
        if stage["kind"] == "tool":
            if not isinstance(stage.get("operation"), str) or stage["operation"] not in _OPERATIONS:
                raise PlanError(f"stage {sid} operation is invalid")
        elif "operation" in stage and (not isinstance(stage["operation"], str) or stage["operation"] not in _OPERATIONS):
            raise PlanError(f"stage {sid} operation is invalid")
        if "settings" in stage:
            _object(stage["settings"], f"stage {sid}.settings")
        if "rules" in stage:
            _object(stage["rules"], f"stage {sid}.rules")
        if "max_cost_usd" in stage:
            _finite_number(stage["max_cost_usd"], f"stage {sid}.max_cost_usd", minimum=0.0)

        runner = stage.get("runner")
        if not isinstance(runner, dict) or set(runner) - {"profile", "gpu_type", "min_gpu_ram_gb"}:
            raise PlanError(f"stage {sid} runner is invalid")
        if not isinstance(runner.get("profile"), str) or not runner["profile"].strip() or len(runner["profile"]) > 100:
            raise PlanError(f"stage {sid} runner profile is invalid")
        if "gpu_type" in runner and (not isinstance(runner["gpu_type"], str) or not runner["gpu_type"].strip()):
            raise PlanError(f"stage {sid} runner gpu_type is invalid")
        if "min_gpu_ram_gb" in runner:
            value = runner["min_gpu_ram_gb"]
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 256:
                raise PlanError(f"stage {sid} runner memory is invalid")
        retry = stage.get("retry")
        if not isinstance(retry, dict) or set(retry) != {"max_attempts"}:
            raise PlanError(f"stage {sid} retry contract is invalid")
        if isinstance(retry["max_attempts"], bool) or not isinstance(retry["max_attempts"], int) or not 1 <= retry["max_attempts"] <= 4:
            raise PlanError(f"stage {sid} retry contract is invalid")
        checkpoint = stage.get("checkpoint")
        if not isinstance(checkpoint, dict) or set(checkpoint) != {"required"} or not isinstance(checkpoint["required"], bool):
            raise PlanError(f"stage {sid} checkpoint contract is invalid")
        fan = stage.get("fan_out")
        if not isinstance(fan, dict) or set(fan) != {"max_items"}:
            raise PlanError(f"stage {sid} fan-out is invalid")
        if isinstance(fan["max_items"], bool) or not isinstance(fan["max_items"], int) or not 1 <= fan["max_items"] <= MAX_FAN_OUT:
            raise PlanError(f"stage {sid} fan-out is invalid")

        raw_outputs = stage.get("outputs")
        if not isinstance(raw_outputs, list) or len(raw_outputs) > MAX_OUTPUTS:
            raise PlanError(f"stage {sid} output contracts are invalid")
        outputs[sid] = {}
        for index, item in enumerate(raw_outputs):
            normalized = _contract(item, f"stage {sid} output[{index}]")
            if normalized["name"] in outputs[sid]:
                raise PlanError(f"stage {sid} output names are not unique")
            outputs[sid][normalized["name"]] = normalized

        raw_inputs = stage.get("inputs")
        if not isinstance(raw_inputs, list) or len(raw_inputs) > MAX_INPUTS:
            raise PlanError(f"stage {sid} input contracts are invalid")
        for index, binding in enumerate(raw_inputs):
            label = f"stage {sid} input[{index}]"
            allowed_binding = {"from_stage", "output", "artifact", "required", "role", "media_type"}
            if not isinstance(binding, dict) or set(binding) - allowed_binding:
                raise PlanError(f"{label} is invalid")
            source = binding.get("from_stage")
            artifact = binding.get("artifact")
            if (source is None) == (artifact is None):
                raise PlanError(f"{label} must name one source")
            if not isinstance(binding.get("required"), bool):
                raise PlanError(f"{label}.required is invalid")
            role = binding.get("role")
            if role is not None and (not isinstance(role, str) or role not in _ROLES):
                raise PlanError(f"{label}.role is invalid")
            media_type = binding.get("media_type")
            if media_type is not None:
                _media(media_type, f"{label}.media_type")
            if source is not None:
                if not isinstance(source, str) or source not in stage_set or source == sid or source not in deps:
                    raise PlanError(f"{label}.from_stage is not a declared dependency")
                output_name = binding.get("output")
                if not isinstance(output_name, str):
                    raise PlanError(f"{label}.output is not a declared producer output")
                producer_outputs = outputs.get(source, {})
                if output_name not in producer_outputs:
                    producer_stage = next((candidate for candidate in stages if isinstance(candidate, dict) and candidate.get("id") == source), None)
                    producer_outputs = {
                        str(item.get("name")): item
                        for item in (producer_stage or {}).get("outputs", [])
                        if isinstance(item, dict) and isinstance(item.get("name"), str)
                    }
                if output_name not in producer_outputs:
                    raise PlanError(f"{label}.output is not a declared producer output")
                producer = producer_outputs[output_name]
                if producer.get("name") == output_name and set(producer) != {"name", "role", "media_type"}:
                    producer = _contract(producer, f"stage {source} output")
                if role is not None and role != producer["role"]:
                    raise PlanError(f"{label}.role is incompatible with producer output")
                if media_type is not None and media_type != producer["media_type"]:
                    raise PlanError(f"{label}.media_type is incompatible with producer output")
            else:
                if not isinstance(artifact, str) or artifact not in declared_inputs:
                    raise PlanError(f"{label}.artifact is not declared")
                declared = declared_inputs[artifact]
                if role is not None and role != declared["role"]:
                    raise PlanError(f"{label}.role is incompatible with input artifact")
                if media_type is not None and media_type != declared["media_type"]:
                    raise PlanError(f"{label}.media_type is incompatible with input artifact")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(sid: str) -> None:
        if sid in visiting:
            raise PlanError("stage dependencies contain a cycle")
        if sid in visited:
            return
        visiting.add(sid)
        for dep in graph[sid]:
            visit(dep)
        visiting.remove(sid)
        visited.add(sid)

    for sid in stage_ids:
        visit(sid)

    policy = plan.get("policy")
    allowed_policy = {
        "residency", "max_cost_usd", "cancel_before_submit_is_free", "reuse_compatible_lease",
        "single_quote", "single_billing_closure", "retain_checkpoints",
    }
    if not isinstance(policy, dict) or set(policy) - allowed_policy or policy.get("residency") not in {"cloud", "on-prem"}:
        raise PlanError("residency policy is invalid")
    for field in ("cancel_before_submit_is_free", "reuse_compatible_lease", "single_quote", "single_billing_closure", "retain_checkpoints"):
        if policy.get(field) is not True:
            raise PlanError("unsafe policy")
    if "max_cost_usd" in policy:
        _finite_number(policy["max_cost_usd"], "policy.max_cost_usd", minimum=0.0)
    final_outputs = plan.get("final_outputs")
    if not isinstance(final_outputs, list) or not final_outputs:
        raise PlanError("final_outputs is required")
    for index, item in enumerate(final_outputs):
        if not isinstance(item, dict) or set(item) != {"stage_id", "output"}:
            raise PlanError(f"final_outputs[{index}] is invalid")
        if not isinstance(item["stage_id"], str) or not isinstance(item["output"], str) or item["stage_id"] not in stage_set or item["output"] not in outputs[item["stage_id"]]:
            raise PlanError("final output refers to an unknown stage output")
    if plan.get("plan_digest") != canonical_plan_digest(plan):
        raise PlanError("plan digest is invalid")
    return plan


def public_plan_summary(plan: dict[str, Any]) -> dict[str, Any]:
    """Return the finite, opaque public plan projection.

    In particular, operation, settings, capsules, input paths, and provider
    details are private authority data and are deliberately not copied.
    """

    validate_cloud_plan(plan)
    stage_ids = {
        stage["id"]: _public_opaque(stage["id"])
        for stage in plan["stages"]
    }
    return {
        "schema": SCHEMA,
        "plan_id": _public_opaque(plan["plan_id"]),
        "plan_digest": plan["plan_digest"],
        "project_id": _public_opaque(plan["project_id"]),
        "input_revision": _public_opaque(plan["input_revision"]),
        "stage_count": len(plan["stages"]),
        "stages": [
            {
                "id": stage_ids[stage["id"]],
                "kind": stage["kind"],
                "depends_on": [stage_ids[item] for item in stage["depends_on"]],
            }
            for stage in plan["stages"]
        ],
        "residency": plan["policy"]["residency"],
    }


def validate_public_plan_summary(
    summary: dict[str, Any], *, expected_digest: str | None = None
) -> dict[str, Any]:
    """Validate the allow-listed plan projection at the queue boundary."""

    if not isinstance(summary, dict):
        raise PlanError("public plan projection is invalid")
    allowed = {
        "schema", "plan_id", "plan_digest", "project_id", "input_revision",
        "stage_count", "stages", "residency",
    }
    if set(summary) != allowed or summary.get("schema") != SCHEMA:
        raise PlanError("public plan projection is invalid")
    digest = _digest(summary.get("plan_digest"), "public plan digest")
    if expected_digest is not None and digest != _digest(expected_digest, "plan digest"):
        raise PlanError("public plan projection binding is invalid")
    for field in ("plan_id", "project_id", "input_revision"):
        _digest(summary.get(field), f"public plan {field}")
    _validate_public_stage_list(
        summary.get("stages"), summary.get("stage_count"), "public plan"
    )
    if summary.get("residency") not in {"cloud", "on-prem"}:
        raise PlanError("public plan residency is invalid")
    return summary


def _safe_opaque(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise PlanError(f"{label} is invalid")
    return value


def _bounded_int(value: Any, *, maximum: int = 2**31 - 1, default: int = 0) -> int:
    """Keep public numeric projections finite even if a row is corrupted."""

    if isinstance(value, bool):
        return default
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return max(0, min(maximum, number))


def _public_timestamp(value: Any) -> str | None:
    """Canonicalize coordinator timestamps and fail closed on corrupt data.

    Queue timestamps predate the plan protocol and are naive UTC strings.
    Expiry values still use :func:`_parse_expiry` and therefore remain strict;
    this helper only handles display timestamps from the coordinator journal.
    """

    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _public_opaque(value: Any) -> str:
    """Hash caller/provider identifiers before placing them in public data."""

    return "sha256:" + hashlib.sha256(canonical_bytes(str(value))).hexdigest()


def _parse_expiry(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise PlanError("preflight expiry is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PlanError("preflight expiry is invalid") from exc
    if parsed.tzinfo is None:
        raise PlanError("preflight expiry is invalid")
    return parsed.astimezone(timezone.utc)


def _safe_candidate(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        raise PlanError("candidate is invalid")
    allowed = {
        "candidate_id", "offer_id", "id", "provider", "gpu_type", "gpu_ram_gb",
        "region", "residency", "hourly_rate", "estimate", "storage", "preparation",
    }
    if set(candidate) - allowed:
        raise PlanError("candidate has unknown fields")
    candidate_id = _safe_opaque(candidate.get("candidate_id"), "candidate_id")
    offer_id = _safe_opaque(candidate.get("offer_id") or candidate.get("id"), "offer_id")
    region = _safe_opaque(candidate.get("region"), "candidate region")
    residency = candidate.get("residency", "cloud")
    if residency not in {"cloud", "on-prem"}:
        raise PlanError("candidate residency is invalid")
    hourly = _finite_number(candidate.get("hourly_rate"), "candidate hourly_rate", minimum=0.000001)
    result: dict[str, Any] = {
        "candidate_id": candidate_id,
        "offer_id": _public_opaque(offer_id),
        "region": _public_opaque(region),
        "residency": residency,
        "hourly_rate": hourly,
    }
    if "gpu_ram_gb" in candidate:
        ram = _finite_number(candidate["gpu_ram_gb"], "candidate gpu_ram_gb", minimum=0.0)
        result["gpu_ram_gb"] = ram
    storage = candidate.get("storage", {"region": region, "persistent": False})
    if not isinstance(storage, dict) or set(storage) - {"region", "persistent", "storage_id"}:
        raise PlanError("candidate storage is invalid")
    if storage.get("region") != region or not isinstance(storage.get("persistent"), bool):
        raise PlanError("candidate storage binding is invalid")
    result["storage"] = {
        "region": _public_opaque(region),
        "persistent": storage["persistent"],
        **({"storage_id": _public_opaque(_safe_opaque(storage["storage_id"], "storage_id"))} if "storage_id" in storage else {}),
    }
    return result


def public_preflight_report(report: dict[str, Any]) -> dict[str, Any]:
    """Strip provider and unrestricted plan data from a preflight report."""

    if not isinstance(report, dict):
        raise PlanError("preflight report is invalid")
    # The direct store API historically accepted a compact report in tests.
    # Fill only safe defaults for that compatibility shape; HTTP reports are
    # always fully populated by the route before reaching this function.
    source = dict(report)
    source.setdefault("schema", PREFLIGHT_SCHEMA)
    source.setdefault("status", "ready")
    source.setdefault("plan_digest", source.get("plan_digest") or "sha256:" + "0" * 64)
    source.setdefault("preflight_id", "preflight-legacy")
    source.setdefault("expires_at", "2999-01-01T00:00:00Z")
    raw_candidates = source.get("candidates")
    if isinstance(raw_candidates, list):
        patched_candidates = []
        for item in raw_candidates:
            if isinstance(item, dict):
                candidate = dict(item)
                candidate.setdefault("offer_id", candidate.get("candidate_id") or "offer-legacy")
                candidate.setdefault("region", "unknown")
                candidate.setdefault("hourly_rate", 0.01)
                candidate.setdefault("storage", {"region": candidate["region"], "persistent": False})
                patched_candidates.append(candidate)
        source["candidates"] = patched_candidates
    result: dict[str, Any] = {}
    for field in ("schema", "preflight_id", "plan_digest", "status", "created_at", "expires_at"):
        if field in source:
            result[field] = source[field]
    if result.get("schema") not in {None, PREFLIGHT_SCHEMA}:
        raise PlanError("preflight schema is invalid")
    result["schema"] = PREFLIGHT_SCHEMA
    result["preflight_id"] = _safe_opaque(result.get("preflight_id"), "preflight_id")
    result["plan_digest"] = _digest(result.get("plan_digest"), "preflight plan_digest")
    if result.get("status") not in {"ready", "accepted"}:
        raise PlanError("preflight status is invalid")
    _parse_expiry(result.get("expires_at"))
    if "created_at" in result:
        _parse_expiry(result["created_at"])
    candidates = source.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise PlanError("preflight candidates are invalid")
    safe_candidates = [_safe_candidate(item) for item in candidates]
    if len({item["candidate_id"] for item in safe_candidates}) != len(safe_candidates):
        raise PlanError("preflight candidate ids are not unique")
    result["candidate_id"] = _safe_opaque(source.get("candidate_id") or safe_candidates[0]["candidate_id"], "candidate_id")
    if result["candidate_id"] not in {item["candidate_id"] for item in safe_candidates}:
        raise PlanError("preflight candidate binding is invalid")
    result["candidates"] = safe_candidates
    if "quote" in source:
        quote = source["quote"]
        if not isinstance(quote, dict):
            raise PlanError("preflight quote is invalid")
        quote_allowed = {
            "quote_id", "plan_digest", "candidate_id", "provider", "region",
            "storage", "hourly_rate", "total_cost_usd", "expires_at",
        }
        if set(quote) - quote_allowed:
            raise PlanError("preflight quote has unknown fields")
        quote_result: dict[str, Any] = {}
        for field in ("quote_id", "plan_digest", "candidate_id", "region", "storage", "hourly_rate", "total_cost_usd", "expires_at"):
            if field in quote:
                quote_result[field] = quote[field]
        if "quote_id" in quote_result:
            quote_result["quote_id"] = _safe_opaque(quote_result["quote_id"], "quote_id")
        if "plan_digest" in quote_result:
            quote_result["plan_digest"] = _digest(quote_result["plan_digest"], "quote plan_digest")
        if "candidate_id" in quote_result:
            _safe_opaque(quote_result["candidate_id"], "quote candidate_id")
        if "region" in quote_result:
            quote_region = _safe_opaque(quote_result["region"], "quote region")
            quote_result["region"] = _public_opaque(quote_region)
        if "storage" in quote_result:
            storage = quote_result["storage"]
            if not isinstance(storage, dict) or set(storage) - {"region", "persistent", "storage_id"}:
                raise PlanError("quote storage is invalid")
            storage_region = _safe_opaque(storage.get("region"), "quote storage region")
            if "region" in quote and storage_region != _safe_opaque(quote["region"], "quote region"):
                raise PlanError("quote storage binding is invalid")
            if not isinstance(storage.get("persistent"), bool):
                raise PlanError("quote storage is invalid")
            quote_result["storage"] = {
                "region": _public_opaque(storage_region),
                "persistent": storage["persistent"],
                **({"storage_id": _public_opaque(_safe_opaque(storage["storage_id"], "quote storage_id"))} if "storage_id" in storage else {}),
            }
        if "hourly_rate" in quote_result:
            _finite_number(quote_result["hourly_rate"], "quote hourly_rate", minimum=0.000001)
        if "total_cost_usd" in quote_result:
            costs = quote_result["total_cost_usd"]
            if not isinstance(costs, dict) or set(costs) != {"min", "max"}:
                raise PlanError("quote total cost is invalid")
            low = _finite_number(costs["min"], "quote cost minimum", minimum=0.0)
            high = _finite_number(costs["max"], "quote cost maximum", minimum=low)
            if low <= 0 or high < low:
                raise PlanError("quote total cost is invalid")
        if "expires_at" in quote_result:
            _parse_expiry(quote_result["expires_at"])
        if quote_result.get("plan_digest") != result["plan_digest"]:
            raise PlanError("quote plan binding is invalid")
        if quote_result.get("candidate_id") != result["candidate_id"]:
            raise PlanError("quote candidate binding is invalid")
        result["quote"] = quote_result
    if "plan" in source:
        plan_projection = source["plan"]
        if not isinstance(plan_projection, dict):
            raise PlanError("preflight plan projection is invalid")
        # A caller may pass a full plan here; only ever retain the safe view.
        plan_digest = _digest(plan_projection.get("plan_digest"), "preflight plan_digest")
        if plan_digest != result["plan_digest"]:
            raise PlanError("preflight plan binding is invalid")
        safe_stages = []
        raw_stages = plan_projection.get("stages")
        if isinstance(raw_stages, list):
            stage_public_ids: dict[str, str] = {}
            for stage in raw_stages:
                if isinstance(stage, dict) and isinstance(stage.get("id"), str):
                    stage_public_ids[stage["id"]] = _public_opaque(stage["id"])
            for stage in raw_stages:
                if not isinstance(stage, dict) or not isinstance(stage.get("id"), str):
                    raise PlanError("preflight stage projection is invalid")
                dependencies = stage.get("depends_on", [])
                if not isinstance(dependencies, list) or any(item not in stage_public_ids for item in dependencies):
                    raise PlanError("preflight stage dependency projection is invalid")
                kind = stage.get("kind")
                if kind not in _KINDS:
                    raise PlanError("preflight stage kind is invalid")
                safe_stages.append(
                    {
                        "id": stage_public_ids[stage["id"]],
                        "kind": kind,
                        "depends_on": [stage_public_ids[item] for item in dependencies],
                    }
                )
        result["plan"] = {
            "schema": SCHEMA,
            "plan_digest": plan_digest,
            "stage_count": len(safe_stages),
            "stages": safe_stages,
            "residency": plan_projection.get("residency") if plan_projection.get("residency") in {"cloud", "on-prem"} else "cloud",
        }
    # Warnings and unknowns are intentionally finite counts, not free text.
    result["warning_count"] = len(source.get("warnings") or []) if isinstance(source.get("warnings", []), list) else 0
    result["unknown_count"] = len(source.get("unknowns") or []) if isinstance(source.get("unknowns", []), list) else 0
    result["warnings"] = []
    result["unknowns"] = []
    return result


def _validate_public_storage(storage: Any, label: str) -> dict[str, Any]:
    if not isinstance(storage, dict) or set(storage) - {"region", "persistent", "storage_id"}:
        raise PlanError(f"{label} is invalid")
    region = _digest(storage.get("region"), f"{label}.region")
    if not isinstance(storage.get("persistent"), bool):
        raise PlanError(f"{label}.persistent is invalid")
    result = {"region": region, "persistent": storage["persistent"]}
    if "storage_id" in storage:
        result["storage_id"] = _digest(storage["storage_id"], f"{label}.storage_id")
    return result


def _validate_public_stage_list(stages: Any, stage_count: Any, label: str) -> None:
    if (
        isinstance(stage_count, bool)
        or not isinstance(stage_count, int)
        or not 1 <= stage_count <= MAX_STAGES
        or not isinstance(stages, list)
        or len(stages) != stage_count
    ):
        raise PlanError(f"{label} stages are invalid")
    stage_ids: set[str] = set()
    for stage in stages:
        if not isinstance(stage, dict) or set(stage) != {"id", "kind", "depends_on"}:
            raise PlanError(f"{label} stage is invalid")
        stage_id = _digest(stage.get("id"), f"{label} stage id")
        if stage_id in stage_ids or stage.get("kind") not in _KINDS:
            raise PlanError(f"{label} stage is invalid")
        stage_ids.add(stage_id)
    for stage in stages:
        dependencies = stage["depends_on"]
        if not isinstance(dependencies, list):
            raise PlanError(f"{label} dependencies are invalid")
        normalized = [_digest(item, f"{label} dependency") for item in dependencies]
        if len(normalized) != len(set(normalized)) or any(
            item not in stage_ids or item == _digest(stage["id"], f"{label} stage id")
            for item in normalized
        ):
            raise PlanError(f"{label} dependencies are invalid")


def validate_public_preflight_projection(
    report: dict[str, Any],
    *,
    expected_plan_digest: str,
    expected_preflight_id: str,
    expected_candidate_id: str,
) -> dict[str, Any]:
    """Validate the already-redacted preflight passed to queue storage."""

    if not isinstance(report, dict):
        raise PlanError("public preflight projection is invalid")
    allowed = {
        "schema", "preflight_id", "plan_digest", "status", "created_at",
        "expires_at", "candidate_id", "candidates", "quote", "plan",
        "warning_count", "unknown_count", "warnings", "unknowns",
    }
    if set(report) != allowed or report.get("schema") != PREFLIGHT_SCHEMA:
        raise PlanError("public preflight projection is invalid")
    if _safe_opaque(report.get("preflight_id"), "public preflight id") != expected_preflight_id:
        raise PlanError("public preflight projection binding is invalid")
    plan_digest = _digest(report.get("plan_digest"), "public preflight digest")
    if plan_digest != _digest(expected_plan_digest, "plan digest"):
        raise PlanError("public preflight projection binding is invalid")
    if report.get("status") not in {"ready", "accepted"}:
        raise PlanError("public preflight status is invalid")
    _parse_expiry(report.get("created_at"))
    _parse_expiry(report.get("expires_at"))
    candidate_id = _safe_opaque(report.get("candidate_id"), "public candidate id")
    if candidate_id != expected_candidate_id:
        raise PlanError("public preflight candidate binding is invalid")
    candidates: Any = report.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise PlanError("public preflight candidates are invalid")
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, dict):
            raise PlanError("public preflight candidate is invalid")
        candidate_allowed = {
            "candidate_id", "offer_id", "region", "residency", "hourly_rate",
            "gpu_ram_gb", "storage",
        }
        if set(candidate) - candidate_allowed or "gpu_ram_gb" not in candidate:
            raise PlanError("public preflight candidate is invalid")
        item_id = _safe_opaque(candidate.get("candidate_id"), "public candidate id")
        if item_id in seen:
            raise PlanError("public preflight candidate ids are not unique")
        seen.add(item_id)
        _digest(candidate.get("offer_id"), "public offer id")
        region = _digest(candidate.get("region"), "public candidate region")
        if candidate.get("residency") not in {"cloud", "on-prem"}:
            raise PlanError("public candidate residency is invalid")
        _finite_number(candidate.get("hourly_rate"), "public candidate hourly_rate", minimum=0.000001)
        _finite_number(candidate.get("gpu_ram_gb"), "public candidate gpu_ram_gb", minimum=0.0)
        storage = _validate_public_storage(candidate.get("storage"), "public candidate storage")
        if storage["region"] != region:
            raise PlanError("public candidate storage binding is invalid")
    if candidate_id not in seen:
        raise PlanError("public preflight candidate binding is invalid")

    quote = report.get("quote")
    quote_allowed = {
        "quote_id", "plan_digest", "candidate_id", "region", "storage",
        "hourly_rate", "total_cost_usd", "expires_at",
    }
    if not isinstance(quote, dict) or set(quote) != quote_allowed:
        raise PlanError("public preflight quote is invalid")
    _safe_opaque(quote.get("quote_id"), "public quote id")
    if _digest(quote.get("plan_digest"), "public quote digest") != plan_digest:
        raise PlanError("public quote plan binding is invalid")
    if _safe_opaque(quote.get("candidate_id"), "public quote candidate id") != candidate_id:
        raise PlanError("public quote candidate binding is invalid")
    quote_region = _digest(quote.get("region"), "public quote region")
    quote_storage = _validate_public_storage(quote.get("storage"), "public quote storage")
    if quote_storage["region"] != quote_region:
        raise PlanError("public quote storage binding is invalid")
    _finite_number(quote.get("hourly_rate"), "public quote hourly_rate", minimum=0.000001)
    costs = quote.get("total_cost_usd")
    if not isinstance(costs, dict) or set(costs) != {"min", "max"}:
        raise PlanError("public quote cost is invalid")
    low = _finite_number(costs["min"], "public quote cost minimum", minimum=0.000001)
    _finite_number(costs["max"], "public quote cost maximum", minimum=low)
    _parse_expiry(quote.get("expires_at"))

    plan = report.get("plan")
    if not isinstance(plan, dict) or set(plan) != {"schema", "plan_digest", "stage_count", "stages", "residency"}:
        raise PlanError("public preflight plan projection is invalid")
    if plan.get("schema") != SCHEMA or _digest(plan.get("plan_digest"), "public plan digest") != plan_digest:
        raise PlanError("public preflight plan binding is invalid")
    if plan.get("residency") not in {"cloud", "on-prem"}:
        raise PlanError("public preflight plan residency is invalid")
    _validate_public_stage_list(plan.get("stages"), plan.get("stage_count"), "public preflight plan")
    for key in ("warning_count", "unknown_count"):
        value = report.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 10000:
            raise PlanError(f"public preflight {key} is invalid")
    if report.get("warnings") != [] or report.get("unknowns") != []:
        raise PlanError("public preflight diagnostics are invalid")
    return report


def reproject_public_preflight(report: dict[str, Any]) -> dict[str, Any]:
    """Validate and rebuild a cached public preflight without stored extras.

    HTTP preflights use the complete projection.  The compact form remains
    supported for the legacy direct store API, but both forms are rebuilt from
    an allow-list so a SQLite edit can never become an HTTP response.
    """

    if not isinstance(report, dict):
        raise PlanError("stored preflight is corrupt")
    full_fields = {
        "schema", "preflight_id", "plan_digest", "status", "created_at",
        "expires_at", "candidate_id", "candidates", "quote", "plan",
        "warning_count", "unknown_count", "warnings", "unknowns",
    }
    if set(report) == full_fields:
        validate_public_preflight_projection(
            report,
            expected_plan_digest=str(report["plan_digest"]),
            expected_preflight_id=str(report["preflight_id"]),
            expected_candidate_id=str(report["candidate_id"]),
        )
        def storage_copy(value: dict[str, Any]) -> dict[str, Any]:
            result = {"region": str(value["region"]), "persistent": bool(value["persistent"])}
            if "storage_id" in value:
                result["storage_id"] = str(value["storage_id"])
            return result

        candidates: list[dict[str, Any]] = [
            {
                **{
                    field: item[field]
                    for field in ("candidate_id", "offer_id", "region", "residency", "hourly_rate", "gpu_ram_gb")
                },
                "storage": storage_copy(item["storage"]),
            }
            for item in report["candidates"]
        ]
        quote = dict(report["quote"])
        quote["storage"] = storage_copy(report["quote"]["storage"])
        quote["total_cost_usd"] = dict(report["quote"]["total_cost_usd"])
        plan = {
            "schema": SCHEMA,
            "plan_digest": report["plan"]["plan_digest"],
            "stage_count": report["plan"]["stage_count"],
            "stages": [
                {"id": stage["id"], "kind": stage["kind"], "depends_on": list(stage["depends_on"])}
                for stage in report["plan"]["stages"]
            ],
            "residency": report["plan"]["residency"],
        }
        return {
            "schema": PREFLIGHT_SCHEMA,
            "preflight_id": report["preflight_id"],
            "plan_digest": report["plan_digest"],
            "status": report["status"],
            "created_at": report["created_at"],
            "expires_at": report["expires_at"],
            "candidate_id": report["candidate_id"],
            "candidates": candidates,
            "quote": quote,
            "plan": plan,
            "warning_count": report["warning_count"],
            "unknown_count": report["unknown_count"],
            "warnings": [],
            "unknowns": [],
        }

    compact_fields = {
        "schema", "preflight_id", "plan_digest", "status", "created_at",
        "expires_at", "candidate_id", "candidates", "warning_count",
        "unknown_count", "warnings", "unknowns",
    }
    required = {"schema", "preflight_id", "plan_digest", "status", "expires_at", "candidate_id", "candidates"}
    if set(report) - compact_fields or not required <= set(report):
        raise PlanError("stored preflight is corrupt")
    if report.get("schema") != PREFLIGHT_SCHEMA or report.get("status") not in {"ready", "accepted"}:
        raise PlanError("stored preflight is corrupt")
    _safe_opaque(report.get("preflight_id"), "stored preflight id")
    _digest(report.get("plan_digest"), "stored preflight digest")
    _parse_expiry(report.get("expires_at"))
    if "created_at" in report:
        _parse_expiry(report["created_at"])
    compact_candidates: Any = report.get("candidates")
    if not isinstance(compact_candidates, list) or not compact_candidates:
        raise PlanError("stored preflight is corrupt")
    safe_candidates: list[dict[str, Any]] = []
    for item in compact_candidates:
        if not isinstance(item, dict):
            raise PlanError("stored preflight is corrupt")
        allowed_candidate = {
            "candidate_id", "offer_id", "region", "residency", "hourly_rate",
            "gpu_ram_gb", "storage",
        }
        if set(item) - allowed_candidate:
            raise PlanError("stored preflight is corrupt")
        item_id = _safe_opaque(item.get("candidate_id"), "stored candidate id")
        offer_id = _digest(item.get("offer_id"), "stored offer id")
        region = _digest(item.get("region"), "stored candidate region")
        if item.get("residency") not in {"cloud", "on-prem"}:
            raise PlanError("stored preflight is corrupt")
        hourly_rate = _finite_number(
            item.get("hourly_rate"), "stored hourly rate", minimum=0.000001
        )
        storage = _validate_public_storage(item.get("storage"), "stored candidate storage")
        if storage["region"] != region:
            raise PlanError("stored preflight is corrupt")
        normalized: dict[str, Any] = {
            "candidate_id": item_id,
            "offer_id": offer_id,
            "region": region,
            "residency": item["residency"],
            "hourly_rate": hourly_rate,
            "storage": storage,
        }
        if "gpu_ram_gb" in item:
            normalized["gpu_ram_gb"] = _finite_number(
                item["gpu_ram_gb"], "stored gpu ram", minimum=0.0
            )
        safe_candidates.append(normalized)
    candidate_id = _safe_opaque(report.get("candidate_id"), "stored preflight candidate id")
    if candidate_id not in {item["candidate_id"] for item in safe_candidates}:
        raise PlanError("stored preflight is corrupt")
    result = {
        "schema": PREFLIGHT_SCHEMA,
        "preflight_id": report["preflight_id"],
        "plan_digest": report["plan_digest"],
        "status": report["status"],
        "expires_at": report["expires_at"],
        "candidate_id": candidate_id,
        "candidates": safe_candidates,
    }
    for field in ("created_at", "warning_count", "unknown_count", "warnings", "unknowns"):
        if field in report:
            value = report[field]
            if field in {"warnings", "unknowns"} and value != []:
                raise PlanError("stored preflight is corrupt")
            if field.endswith("_count") and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise PlanError("stored preflight is corrupt")
            result[field] = value
    return result


def validate_result_manifest(manifest: dict[str, Any], *, expected_job_id: str | None = None, expected_lease_id: str | None = None) -> dict[str, Any]:
    """Validate the only result shape accepted for a plan completion."""

    if not isinstance(manifest, dict):
        raise PlanError("result manifest is invalid")
    allowed = {"schema", "manifest_id", "job_id", "lease_id", "artifacts"}
    if set(manifest) - allowed:
        raise PlanError("result manifest has unknown fields")
    if manifest.get("schema") != "cloud-offload.result-manifest.v1":
        raise PlanError("result manifest schema is invalid")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise PlanError("result manifest requires non-empty artifacts")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, artifact in enumerate(artifacts):
        if not isinstance(artifact, dict):
            raise PlanError(f"result artifact[{index}] is invalid")
        artifact_allowed = {"id", "sha256", "size", "media_type", "role", "producer", "job_id", "lease_id"}
        if set(artifact) - artifact_allowed:
            raise PlanError(f"result artifact[{index}] has unknown fields")
        aid = _safe_opaque(artifact.get("id"), f"result artifact[{index}].id")
        if aid in seen:
            raise PlanError("result artifact ids are not unique")
        seen.add(aid)
        digest = _digest(artifact.get("sha256"), f"result artifact[{index}].sha256")
        size = artifact.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or not 0 < size <= MAX_ARTIFACT_SIZE:
            raise PlanError(f"result artifact[{index}].size is invalid")
        media = _media(artifact.get("media_type"), f"result artifact[{index}].media_type")
        role = artifact.get("role")
        if not isinstance(role, str) or role not in _ROLES:
            raise PlanError(f"result artifact[{index}].role is invalid")
        producer = _safe_opaque(artifact.get("producer"), f"result artifact[{index}].producer")
        normalized_item: dict[str, Any] = {"id": aid, "sha256": digest, "size": size, "media_type": media, "role": role, "producer": producer}
        if expected_job_id is not None and "job_id" not in artifact:
            raise PlanError(f"result artifact[{index}] job_id is required")
        for field, expected in (("job_id", expected_job_id), ("lease_id", expected_lease_id)):
            if field in artifact:
                normalized_item[field] = _safe_opaque(artifact[field], f"result artifact[{index}].{field}")
                if expected is not None and normalized_item[field] != expected:
                    raise PlanError(f"result artifact[{index}] {field} binding is invalid")
        normalized.append(normalized_item)
    result: dict[str, Any] = {"schema": "cloud-offload.result-manifest.v1", "artifacts": normalized}
    for field in ("manifest_id", "job_id", "lease_id"):
        if field in manifest:
            result[field] = _safe_opaque(manifest[field], f"result manifest.{field}")
            if field == "job_id" and expected_job_id is not None and result[field] != expected_job_id:
                raise PlanError("result manifest job binding is invalid")
            if field == "lease_id" and expected_lease_id is not None and result[field] != expected_lease_id:
                raise PlanError("result manifest lease binding is invalid")
    if expected_job_id is not None and result.get("job_id") != expected_job_id:
        raise PlanError("result manifest job binding is required")
    return result


def public_plan_job(job: Any, *, state: str | None = None, closure: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the only queue/status representation allowed for a plan job.

    The queue's historical ``Job`` shape contains request and parameter maps
    intended for trusted workers.  A plan job must never expose that shape to
    an HTTP caller because it can contain the accepted workflow authority.
    """

    params = job.params if isinstance(getattr(job, "params", None), dict) else {}
    digest = _digest(params.get("plan_digest"), "plan job digest")
    plan_state = state if state in _PLAN_PUBLIC_STATUSES else "failed"
    job_id = _safe_opaque(getattr(job, "id", None), "job_id")
    result: dict[str, Any] = {
        "schema": "cloud-offload.plan-job.v1",
        "job_id": job_id,
        "kind": "plan",
        "status": plan_state,
        "plan_digest": digest,
        "preflight_id": _safe_opaque(params.get("preflight_id"), "preflight_id"),
        "candidate_id": _safe_opaque(params.get("candidate_id"), "candidate_id"),
        "request_digest": _digest(params.get("request_digest"), "request_digest"),
        "progress": _bounded_int(getattr(job, "progress", 0), maximum=100),
        "created_at": _public_timestamp(getattr(job, "created_at", None)),
        "updated_at": _public_timestamp(getattr(job, "updated_at", None)),
    }
    if closure is not None:
        result["closure"] = validate_closure_receipt(closure)
    raw_result = getattr(job, "result", None)
    if plan_state == "completed" and raw_result is not None:
        result["result"] = validate_result_manifest(
            raw_result, expected_job_id=job_id
        )
    return result


def public_plan_event(event: dict[str, Any]) -> dict[str, Any]:
    """Project one plan event to finite lifecycle data and opaque identity."""

    if not isinstance(event, dict):
        raise PlanError("plan event is invalid")
    event_type = str(event.get("type") or "")
    if event_type not in _PLAN_PUBLIC_EVENT_TYPES:
        event_type = "job_status_changed"
    phase = str(event.get("phase") or "readiness")
    if phase not in _PLAN_PUBLIC_PHASES:
        phase = "readiness"
    status = str(event.get("status") or "submitting")
    if status not in _PLAN_PUBLIC_STATUSES:
        status = "submitting"
    result: dict[str, Any] = {
        "schema": "cloud-offload.job-event.v2",
        "sequence": _bounded_int(event.get("sequence")),
        "job_id": _safe_opaque(event.get("job_id"), "job_id"),
        "occurred_at": _public_timestamp(event.get("occurred_at")),
        "observed_at": _public_timestamp(event.get("observed_at")),
        "producer": {"id": "coordinator", "sequence": _bounded_int(event.get("producer", {}).get("sequence"), default=0) if isinstance(event.get("producer"), dict) and event.get("producer", {}).get("sequence") is not None else None},
        "type": event_type,
        "phase": phase,
        "phase_owner": "coordinator",
        "status": status,
    }
    metrics = event.get("metrics")
    if isinstance(metrics, dict):
        safe_metrics: dict[str, int | float] = {}
        for key in ("progress", "overall_progress", "bytes", "total_bytes", "percent"):
            value = metrics.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            try:
                finite = math.isfinite(float(value))
            except (OverflowError, ValueError):
                continue
            if not finite:
                continue
            safe_metrics[key] = value
        if safe_metrics:
            result["metrics"] = safe_metrics
    return result


def public_plan_result_manifest(
    job: Any,
    *,
    state: str,
) -> dict[str, Any]:
    """Return a typed result endpoint projection without filenames or paths."""

    job_id = _safe_opaque(getattr(job, "id", None), "job_id")
    normalized: dict[str, Any] = {
        "schema": "cloud-offload.result-manifest.v1",
        "artifacts": [],
    }
    raw_result = getattr(job, "result", None)
    if raw_result is not None:
        normalized = validate_result_manifest(
            raw_result,
            expected_job_id=job_id,
        )
    return {
        "schema": "cloud-offload.result-manifest.v1",
        "job_id": job_id,
        "status": state if state in _PLAN_PUBLIC_STATUSES else "failed",
        "result": normalized,
    }


def ensure_plan_schema(db: sqlite3.Connection) -> None:
    """Create the versioned plan tables, or reject an incompatible database.

    The plan protocol has not shipped on ``main`` yet.  An old development
    table may contain a full private plan in a column that used to be public.
    It is therefore unsafe to add columns and reinterpret those rows.  A
    database with an older shape fails closed.  A new database gets the
    complete schema and an explicit version marker.
    """

    plan_columns = _table_columns(db, "cloud_plans")
    authority_columns = _table_columns(db, "cloud_plan_authority")
    required_plan_columns = {
        "plan_digest", "plan_json", "preflight_json", "job_id", "idempotency_key",
        "request_digest", "state", "closure_json", "preflight_request_digest",
        "provider_digest", "candidate_digest", "input_digest", "created_at",
        "updated_at", "schema_version",
    }
    required_authority_columns = {
        "plan_digest", "plan_json", "preflight_json", "request_json",
        "provider_digest", "candidate_json", "input_json", "preflight_request_digest",
        "job_id", "schema_version",
    }
    if plan_columns and not required_plan_columns <= plan_columns:
        raise PlanError("cloud plan authority schema is incompatible")
    if authority_columns and not required_authority_columns <= authority_columns:
        raise PlanError("cloud plan authority schema is incompatible")
    if plan_columns and db.execute(
        "SELECT 1 FROM cloud_plans WHERE schema_version != ? OR schema_version IS NULL LIMIT 1",
        (PLAN_AUTHORITY_SCHEMA_VERSION,),
    ).fetchone():
        raise PlanError("cloud plan authority schema version is incompatible")
    if authority_columns and db.execute(
        "SELECT 1 FROM cloud_plan_authority WHERE schema_version != ? OR schema_version IS NULL LIMIT 1",
        (PLAN_AUTHORITY_SCHEMA_VERSION,),
    ).fetchone():
        raise PlanError("cloud plan authority schema version is incompatible")
    if not plan_columns:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS cloud_plans (
                plan_digest TEXT PRIMARY KEY,
                plan_json TEXT NOT NULL,
                preflight_json TEXT NOT NULL,
                job_id TEXT,
                idempotency_key TEXT UNIQUE,
                request_digest TEXT,
                state TEXT NOT NULL,
                closure_json TEXT,
                preflight_request_digest TEXT,
                provider_digest TEXT,
                candidate_digest TEXT,
                input_digest TEXT,
                created_at TEXT,
                updated_at TEXT,
                schema_version TEXT NOT NULL DEFAULT 'cloud-offload.plan-authority.v2'
            )
            """
        )
    if not authority_columns:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS cloud_plan_authority (
                plan_digest TEXT PRIMARY KEY,
                plan_json TEXT NOT NULL,
                preflight_json TEXT NOT NULL,
                request_json TEXT,
                provider_digest TEXT,
                candidate_json TEXT,
                input_json TEXT,
                preflight_request_digest TEXT,
                job_id TEXT,
                schema_version TEXT NOT NULL DEFAULT 'cloud-offload.plan-authority.v2',
                FOREIGN KEY(plan_digest) REFERENCES cloud_plans(plan_digest)
            )
            """
        )


def _table_columns(db: sqlite3.Connection, table: str) -> set[str]:
    exists = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not exists:
        return set()
    return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}


def _json_dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _json_load(value: Any, label: str) -> Any:
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PlanError(f"{label} is corrupt") from exc


def normalize_input_bindings(plan: dict[str, Any], provided: dict[str, str]) -> dict[str, str]:
    """Resolve and strictly bind request inputs to declared digests."""

    validate_cloud_plan(plan)
    if not isinstance(provided, dict):
        raise PlanError("input_artifacts must be an object")
    declared = {item["name"]: item for item in plan["input_artifacts"]}
    if set(provided) - set(declared):
        raise PlanError("input_artifacts contains an undeclared input")
    bound: dict[str, str] = {}
    for name, item in declared.items():
        value = provided.get(name, item["sha256"])
        digest = _digest(value, f"input_artifacts.{name}")
        if digest != _digest(item["sha256"], f"declared input {name}"):
            raise PlanError(f"input_artifacts.{name} does not match the declared identity")
        bound[name] = digest
    return bound


def binding_digest(value: Any) -> str:
    """Digest a private binding without exposing its fields."""

    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


class PlanProtocolStore:
    """Safe projection and private authority access for one queue database."""

    def __init__(self, path: str):
        self.path = str(path)
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)

    def _row(self, db: sqlite3.Connection, digest: str):
        return db.execute(
            "SELECT plan_json,preflight_json,job_id,state,closure_json,request_digest,preflight_request_digest FROM cloud_plans WHERE plan_digest = ?",
            (digest,),
        ).fetchone()

    def preflight(
        self,
        plan: dict[str, Any],
        report: dict[str, Any],
        *,
        request_digest: str | None = None,
        provider_digest: str | None = None,
        candidate_digest: str | None = None,
        input_digest: str | None = None,
    ) -> dict[str, Any]:
        validate_cloud_plan(plan)
        report = dict(report)
        # The authority copy is private, but it still must be strict JSON so
        # a non-finite provider value cannot poison replay or transaction
        # recovery.  Public projection below removes private fields.
        canonical_bytes(report)
        report.setdefault("plan_digest", plan["plan_digest"])
        safe_report = public_preflight_report(report)
        if safe_report["plan_digest"] != plan["plan_digest"]:
            raise PlanError("preflight plan binding is invalid")
        digest = plan["plan_digest"]
        current_time = datetime.now(timezone.utc)
        expires_at = _parse_expiry(safe_report["expires_at"])
        if expires_at <= current_time:
            raise PlanError("preflight quote has expired")
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            db.execute("BEGIN IMMEDIATE")
            prior = self._row(db, digest)
            if prior:
                previous_request = prior[5]
                state = str(prior[3])
                # Never treat an unreadable expiry as a cache miss: doing so
                # could erase a live authority or accept an unbounded quote.
                prior_report = reproject_public_preflight(
                    _json_load(prior[1], "stored preflight")
                )
                prior_expiry = _parse_expiry(prior_report.get("expires_at"))
                # Active and terminal records are immutable.  A matching
                # request gets the exact stored report; a changed request is
                # a conflict, never a replacement.
                if state != "preflighted" or prior[2] is not None:
                    if (request_digest and previous_request == request_digest) or request_digest is None:
                        return prior_report
                    raise PlanError("plan authority is already active")
                if prior_expiry > datetime.now(timezone.utc):
                    if (request_digest and previous_request == request_digest) or request_digest is None:
                        return prior_report
                    raise PlanError("preflight request conflicts with stored quote")
                # An expired, unused row is refreshed in-place.  It is never
                # deleted, so a concurrent submit cannot lose its authority.
                db.execute(
                    "UPDATE cloud_plans SET plan_json=?,preflight_json=?,job_id=NULL,idempotency_key=NULL,request_digest=?,preflight_request_digest=?,provider_digest=?,candidate_digest=?,input_digest=?,updated_at=? WHERE plan_digest=? AND state='preflighted' AND job_id IS NULL",
                    (_json_dump(public_plan_summary(plan)), _json_dump(safe_report), request_digest, request_digest, provider_digest, candidate_digest, input_digest, now, digest),
                )
                db.execute(
                    "UPDATE cloud_plan_authority SET plan_json=?,preflight_json=?,provider_digest=?,candidate_json=?,input_json=?,preflight_request_digest=? WHERE plan_digest=?",
                    (_json_dump(plan), _json_dump(report), provider_digest, _json_dump(report.get("candidates", [])), _json_dump(plan.get("input_artifacts", [])), request_digest, digest),
                )
                return safe_report
            db.execute(
                "INSERT INTO cloud_plans(plan_digest,plan_json,preflight_json,state,request_digest,preflight_request_digest,provider_digest,candidate_digest,input_digest,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (digest, _json_dump(public_plan_summary(plan)), _json_dump(safe_report), "preflighted", request_digest, request_digest, provider_digest, candidate_digest, input_digest, now, now),
            )
            db.execute(
                "INSERT INTO cloud_plan_authority(plan_digest,plan_json,preflight_json,provider_digest,candidate_json,input_json,preflight_request_digest) VALUES(?,?,?,?,?,?,?)",
                (digest, _json_dump(plan), _json_dump(report), provider_digest, _json_dump(report.get("candidates", [])), _json_dump(plan.get("input_artifacts", [])), request_digest),
            )
        return safe_report

    def get(self, plan_digest: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            row = self._row(db, plan_digest)
        if not row:
            return None
        try:
            safe_preflight = reproject_public_preflight(
                _json_load(row[1], "stored preflight")
            )
        except PlanError:
            # Status and lifecycle reads must remain safe when a damaged
            # cached quote is found.  The caller sees no cached values and
            # can reconcile or refresh it; the authority row is not deleted.
            safe_preflight = None
        return {
            "plan": _json_load(row[0], "stored plan"),
            "preflight": safe_preflight,
            "job_id": row[2],
            "state": row[3],
            "closure": _json_load(row[4], "stored closure") if row[4] else None,
        }

    def private(self, plan_digest: str) -> dict[str, Any] | None:
        """Read authority data for the coordinator only, never an HTTP view."""

        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            row = db.execute(
                "SELECT a.plan_json,a.preflight_json,a.request_json,a.provider_digest,a.candidate_json,a.input_json,a.preflight_request_digest,a.job_id,p.candidate_digest,p.input_digest FROM cloud_plan_authority a JOIN cloud_plans p ON p.plan_digest=a.plan_digest WHERE a.plan_digest = ?",
                (plan_digest,),
            ).fetchone()
        if not row:
            return None
        return {
            "plan": _json_load(row[0], "stored plan"),
            "preflight": _json_load(row[1], "stored preflight"),
            "request": _json_load(row[2], "stored request") if row[2] else None,
            "provider_digest": row[3],
            "candidate": _json_load(row[4], "stored candidates") if row[4] else None,
            "input": _json_load(row[5], "stored inputs") if row[5] else None,
            "preflight_request_digest": row[6],
            "job_id": row[7],
            "candidate_digest": row[8],
            "input_digest": row[9],
        }

    def lookup_preflight(self, plan_digest: str, request_digest: str) -> dict[str, Any] | None:
        """Return an exact active preflight replay, or ``None`` if absent."""

        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            row = self._row(db, plan_digest)
        if not row:
            return None
        report = reproject_public_preflight(
            _json_load(row[1], "stored preflight")
        )
        # An active or terminal authority is immutable.  If the caller is
        # replaying the exact accepted preflight, return its stored response
        # without probing offers again; a different binding is a conflict.
        if row[3] != "preflighted" or row[2] is not None:
            if row[6] == request_digest:
                _parse_expiry(report.get("expires_at"))
                return report
            raise PlanError("preflight request conflicts with stored authority")
        # Malformed expiry fails closed.  A valid expired unused quote returns
        # ``None`` so the route may refresh it in place; active rows never
        # reach this branch and are immutable.
        if _parse_expiry(report.get("expires_at")) <= datetime.now(timezone.utc):
            return None
        if row[6] != request_digest:
            raise PlanError("preflight request conflicts with stored quote")
        return report

    def submit(
        self,
        *,
        plan: dict[str, Any],
        preflight_id: str,
        candidate_id: str,
        key: str,
        request_digest: str,
        job_id: str,
    ) -> tuple[str, dict[str, Any] | None]:
        """Legacy authority-only reservation, retained for direct callers.

        HTTP acceptance uses the queue transaction API instead.
        """

        validate_cloud_plan(plan)
        digest = plan["plan_digest"]
        stored_key = binding_digest(key)
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT job_id,plan_digest,preflight_json,request_digest,state FROM cloud_plans WHERE idempotency_key=?", (stored_key,)).fetchone()
            if prior:
                if prior[1] != digest or prior[3] != request_digest:
                    raise PlanError("idempotency key conflicts with a different request")
                if prior[0]:
                    return str(prior[0]), reproject_public_preflight(
                        _json_load(prior[2], "stored preflight")
                    )
            row = self._row(db, digest)
            if not row:
                raise PlanError("accepted preflight is required")
            if row[2] is not None:
                if row[5] == request_digest and row[3] not in _TERMINAL_STATES:
                    return str(row[2]), reproject_public_preflight(
                        _json_load(row[1], "stored preflight")
                    )
                raise PlanError("plan was already submitted with different request data")
            if row[3] != "preflighted":
                raise PlanError("plan authority is not preflighted")
            report = reproject_public_preflight(
                _json_load(row[1], "stored preflight")
            )
            if report.get("preflight_id") != preflight_id or candidate_id != report.get("candidate_id"):
                raise PlanError("accepted preflight binding is invalid")
            if _parse_expiry(report.get("expires_at")) <= datetime.now(timezone.utc):
                raise PlanError("preflight quote has expired")
            db.execute("UPDATE cloud_plans SET job_id=?,idempotency_key=?,request_digest=?,state='submitting',updated_at=? WHERE plan_digest=? AND state='preflighted' AND job_id IS NULL", (job_id, stored_key, request_digest, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), digest))
            db.execute("UPDATE cloud_plan_authority SET request_json=?,job_id=? WHERE plan_digest=?", (_json_dump({"request_digest": request_digest, "preflight_id": preflight_id, "candidate_id": candidate_id}), job_id, digest))
        return job_id, None

    def cancel(self, plan_digest: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, plan_digest)
            if not row:
                return None
            state = str(row[3])
            if state in {"submitting", "submitted", "running"}:
                db.execute("UPDATE cloud_plans SET state='cancelling',updated_at=? WHERE plan_digest=?", (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), plan_digest))
        return self.get(plan_digest)

    def sync_status(self, plan_digest: str, job_id: str, status: str) -> dict[str, Any] | None:
        if status not in {"submitted", "running", "cancelling", "cancelled", "completed", "failed", "terminal"}:
            raise PlanError("job status is invalid")
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, plan_digest)
            if not row or row[2] != job_id:
                return None
            current = str(row[3])
            if current in _TERMINAL_STATES:
                return {"state": current}
            allowed = {
                "preflighted": set(),
                "submitting": {"submitted", "running", "cancelling"},
                "submitted": {"running", "cancelling"},
                "running": {"cancelling"},
                "cancelling": set(),
            }
            if status not in allowed.get(current, set()) and status != current:
                return {"state": current}
            db.execute("UPDATE cloud_plans SET state=?,updated_at=? WHERE plan_digest=?", (status, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), plan_digest))
        return self.get(plan_digest)

    def reconcile_unknown_submit(self, plan_digest: str, *, job_id: str | None = None) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, plan_digest)
            if row and row[3] == "submitting":
                db.execute("UPDATE cloud_plans SET state='submitted',job_id=COALESCE(job_id,?),updated_at=? WHERE plan_digest=? AND state='submitting'", (job_id, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), plan_digest))
        return self.get(plan_digest)

    def abort_submit(self, plan_digest: str, job_id: str) -> None:
        # Never erase an authority row.  A failed reservation returns to the
        # unused state, preserving the preflight for a safe retry.
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE cloud_plans SET job_id=NULL,idempotency_key=NULL,request_digest=NULL,state='preflighted',updated_at=? WHERE plan_digest=? AND job_id=? AND state='submitting'", (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), plan_digest, job_id))
            db.execute("UPDATE cloud_plan_authority SET request_json=NULL,job_id=NULL WHERE plan_digest=? AND job_id=?", (plan_digest, job_id))

    def close(self, digest: str, receipt: dict[str, Any]) -> None:
        normalized = validate_closure_receipt(receipt)
        with sqlite3.connect(self.path) as db:
            ensure_plan_schema(db)
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, digest)
            if not row:
                raise PlanError("plan authority not found")
            if str(row[3]) in _TERMINAL_STATES:
                return
            if str(row[3]) not in {"submitting", "submitted", "running", "cancelling"}:
                raise PlanError("plan lifecycle cannot be closed from this state")
            status = str(receipt.get("status") or "terminal")
            if status not in {"completed", "cancelled", "failed", "terminal"}:
                status = "terminal"
            db.execute("UPDATE cloud_plans SET state=?,closure_json=?,updated_at=? WHERE plan_digest=? AND state NOT IN ('completed','cancelled','failed','terminal')", (status, _json_dump(normalized), datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), digest))


def validate_closure_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        raise PlanError("closure receipt is invalid")
    # ``provider`` was accepted by the original direct-store API.  It is
    # ignored on write because provider names are not public closure data.
    allowed = {"receipt_id", "status", "provider_resource_absent", "terminated", "ended_at", "reason", "provider"}
    if set(receipt) - allowed:
        raise PlanError("closure receipt has unknown fields")
    result_id = _safe_opaque(receipt.get("receipt_id"), "closure receipt_id")
    proof = receipt.get("provider_resource_absent", receipt.get("terminated"))
    if proof is not True:
        raise PlanError("closure receipt does not prove provider absence")
    status = receipt.get("status", "terminal")
    if status not in {"completed", "cancelled", "failed", "terminal"}:
        raise PlanError("closure receipt status is invalid")
    result: dict[str, Any] = {"receipt_id": result_id, "status": status, "provider_resource_absent": True}
    if "ended_at" in receipt:
        result["ended_at"] = _parse_expiry(receipt["ended_at"]).isoformat().replace("+00:00", "Z")
    if "reason" in receipt:
        if not isinstance(receipt["reason"], str) or not receipt["reason"].strip() or len(receipt["reason"]) > 128:
            raise PlanError("closure receipt reason is invalid")
        reason_code = re.sub(r"[^A-Za-z0-9_.-]", "", receipt["reason"])[:64]
        result["reason_code"] = reason_code if reason_code in _CLOSURE_REASON_CODES else "unknown"
    return result


class OfflineConnector:
    """Deterministic offer-only connector used by the offline proof."""

    name = "offline"

    def __init__(self) -> None:
        self.launches = 0
        self.terminations = 0
        self.network_calls = 0

    def list_available(self, **_: Any) -> list[dict[str, Any]]:
        return [{"id": "offline-offer", "offer_id": "offline-offer", "provider": "offline", "gpu_type": "offline", "gpu_ram_gb": 24, "hourly_rate": 0.01, "region": "offline-test"}]

    def launch(self, *args: Any, **kwargs: Any) -> None:
        self.launches += 1
        raise AssertionError("offline connector must not launch")

    def terminate(self, *_: Any, **__: Any) -> bool:
        self.terminations += 1
        raise AssertionError("offline connector must not terminate")
