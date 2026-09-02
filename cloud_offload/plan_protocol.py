"""Strict, provider-neutral protocol for multi-stage cloud plans.

This module is deliberately small. It is the authority for plan identity and
paid-submit idempotency. Provider connectors are passed in by the caller.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

SCHEMA = "comfy.workflow.plan.v1"
MAX_STAGES = 64
MAX_FAN_OUT = 32
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_ROOT = {"schema", "plan_id", "plan_digest", "project_id", "input_revision", "operation", "input_artifacts", "stages", "final_outputs", "policy"}


class PlanError(ValueError):
    """Safe plan validation error."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise PlanError("Plan must be strict JSON") from exc


def canonical_plan_digest(plan: dict[str, Any]) -> str:
    value = dict(plan)
    value["plan_digest"] = ""
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value.strip()):
        raise PlanError(f"{label} is invalid")
    return value.strip()


def _contract(item: Any, label: str) -> None:
    if not isinstance(item, dict) or set(item) - {"name", "role", "media_type", "logical_object", "units", "coordinate_system"}:
        raise PlanError(f"{label} is invalid")
    if any(not isinstance(item.get(k), str) or not item[k].strip() for k in ("name", "role", "media_type")):
        raise PlanError(f"{label} is incomplete")


def validate_cloud_plan(plan: dict[str, Any]) -> dict[str, Any]:
    canonical_bytes(plan)
    if not isinstance(plan, dict) or set(plan) != _ROOT or plan.get("schema") != SCHEMA:
        raise PlanError("Plan root or schema is invalid")
    for field in ("plan_id", "project_id", "input_revision"):
        _identifier(plan.get(field), field)
    if not isinstance(plan.get("operation"), str) or not plan["operation"].strip():
        raise PlanError("operation is invalid")
    inputs = plan.get("input_artifacts")
    if not isinstance(inputs, list) or not inputs:
        raise PlanError("input_artifacts must be a non-empty list")
    input_names: set[str] = set()
    for item in inputs:
        if not isinstance(item, dict) or set(item) - {"name", "filename", "path", "sha256", "size", "role", "media_type"}:
            raise PlanError("input artifact contract is invalid")
        name = _identifier(item.get("name"), "input artifact name")
        if name in input_names or not isinstance(item.get("role"), str) or not item["role"] or not isinstance(item.get("media_type"), str) or not item["media_type"]:
            raise PlanError("input artifact identity or type is invalid")
        input_names.add(name)
        if item.get("sha256") is not None and (not isinstance(item["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])):
            raise PlanError("input artifact digest is invalid")
        if item.get("size") is not None and (isinstance(item["size"], bool) or not isinstance(item["size"], int) or item["size"] < 0):
            raise PlanError("input artifact size is invalid")
    stages = plan.get("stages")
    if not isinstance(stages, list) or not 1 <= len(stages) <= MAX_STAGES:
        raise PlanError("stage count is outside safe bounds")
    ids = [_identifier(s.get("id"), "stage id") if isinstance(s, dict) else "" for s in stages]
    if len(set(ids)) != len(ids):
        raise PlanError("stage ids are not unique")
    stage_ids = set(ids)
    graph: dict[str, list[str]] = {}
    outputs: dict[str, set[str]] = {}
    for stage, sid in zip(stages, ids):
        allowed = {"id", "kind", "depends_on", "capsule", "operation", "settings", "inputs", "outputs", "runner", "retry", "checkpoint", "fan_out", "rules"}
        if set(stage) - allowed or stage.get("kind") not in {"workflow", "tool", "validation", "document_commit"}:
            raise PlanError(f"stage {sid} has invalid fields")
        deps = stage.get("depends_on")
        if not isinstance(deps, list) or len(deps) != len(set(deps)) or any(d not in stage_ids or d == sid for d in deps):
            raise PlanError(f"stage {sid} has an unknown dependency")
        graph[sid] = deps
        if stage["kind"] == "workflow" and not isinstance(stage.get("capsule"), dict):
            raise PlanError(f"stage {sid} capsule is required")
        if stage["kind"] == "tool" and not isinstance(stage.get("operation"), str):
            raise PlanError(f"stage {sid} operation is required")
        if not isinstance(stage.get("inputs"), list) or not isinstance(stage.get("outputs"), list):
            raise PlanError(f"stage {sid} input/output contracts are invalid")
        outputs[sid] = set()
        for item in stage["outputs"]:
            _contract(item, f"stage {sid} output")
            if item["name"] in outputs[sid]:
                raise PlanError(f"stage {sid} output names are not unique")
            outputs[sid].add(item["name"])
        for binding in stage["inputs"]:
            if not isinstance(binding, dict) or set(binding) - {"from_stage", "output", "artifact", "required", "role", "media_type"}:
                raise PlanError(f"stage {sid} input contract is invalid")
            source = binding.get("from_stage")
            artifact = binding.get("artifact")
            if bool(source) == bool(artifact):
                raise PlanError(f"stage {sid} input must name one source")
            if source and (source not in stage_ids or source == sid):
                raise PlanError(f"stage {sid} input dependency is invalid")
            if artifact and artifact not in input_names:
                raise PlanError(f"stage {sid} input refers to an unknown artifact")
            if not isinstance(binding.get("required"), bool):
                raise PlanError(f"stage {sid} input required flag is invalid")
        runner = stage.get("runner")
        if not isinstance(runner, dict) or not isinstance(runner.get("profile"), str):
            raise PlanError(f"stage {sid} runner is invalid")
        retry = stage.get("retry")
        if not isinstance(retry, dict) or isinstance(retry.get("max_attempts"), bool) or not isinstance(retry.get("max_attempts"), int) or not 1 <= retry["max_attempts"] <= 4:
            raise PlanError(f"stage {sid} retry contract is invalid")
        checkpoint = stage.get("checkpoint")
        if not isinstance(checkpoint, dict) or not isinstance(checkpoint.get("required"), bool):
            raise PlanError(f"stage {sid} checkpoint contract is invalid")
        fan = stage.get("fan_out")
        if not isinstance(fan, dict) or isinstance(fan.get("max_items"), bool) or not isinstance(fan.get("max_items"), int) or not 1 <= fan["max_items"] <= MAX_FAN_OUT:
            raise PlanError(f"stage {sid} fan-out is invalid")
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
    for sid in ids:
        visit(sid)
    policy = plan.get("policy")
    if not isinstance(policy, dict) or set(policy) - {"residency", "max_cost_usd", "cancel_before_submit_is_free", "reuse_compatible_lease", "single_quote", "single_billing_closure", "retain_checkpoints"} or policy.get("residency") not in {"cloud", "on-prem"}:
        raise PlanError("residency policy is invalid")
    if any(policy.get(k) is not True for k in ("cancel_before_submit_is_free", "reuse_compatible_lease", "single_quote", "single_billing_closure", "retain_checkpoints")):
        raise PlanError("unsafe policy")
    if not isinstance(plan.get("final_outputs"), list) or not plan["final_outputs"]:
        raise PlanError("final_outputs is required")
    for item in plan["final_outputs"]:
        if not isinstance(item, dict) or item.get("stage_id") not in stage_ids or item.get("output") not in outputs[item["stage_id"]]:
            raise PlanError("final output refers to an unknown stage output")
    if plan.get("plan_digest") != canonical_plan_digest(plan):
        raise PlanError("plan digest is invalid")
    return plan


def public_plan_summary(plan: dict[str, Any]) -> dict[str, Any]:
    validate_cloud_plan(plan)
    return {"schema": SCHEMA, "plan_id": plan["plan_id"], "plan_digest": plan["plan_digest"], "project_id": plan["project_id"], "input_revision": plan["input_revision"], "operation": plan["operation"], "stage_count": len(plan["stages"]), "stages": [{"id": s["id"], "kind": s["kind"], "depends_on": s["depends_on"]} for s in plan["stages"]], "residency": plan["policy"]["residency"]}


class PlanProtocolStore:
    """Coordinator SQLite authority for plan bindings and lifecycle."""
    def __init__(self, path: str):
        self.path = str(path)
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TABLE IF NOT EXISTS cloud_plans (plan_digest TEXT PRIMARY KEY, plan_json TEXT NOT NULL, preflight_json TEXT NOT NULL, job_id TEXT, idempotency_key TEXT UNIQUE, request_digest TEXT, state TEXT NOT NULL, closure_json TEXT)")
            columns = {str(row[1]) for row in db.execute("PRAGMA table_info(cloud_plans)")}
            if "request_digest" not in columns:
                db.execute("ALTER TABLE cloud_plans ADD COLUMN request_digest TEXT")

    def preflight(self, plan: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
        validate_cloud_plan(plan)
        with sqlite3.connect(self.path) as db:
            prior = db.execute("SELECT preflight_json FROM cloud_plans WHERE plan_digest=?", (plan["plan_digest"],)).fetchone()
            if prior:
                cached = json.loads(prior[0])
                try:
                    expiry = datetime.fromisoformat(str(cached.get("expires_at") or "").replace("Z", "+00:00"))
                    if expiry > datetime.now(timezone.utc):
                        return cached
                except ValueError:
                    pass
                db.execute("DELETE FROM cloud_plans WHERE plan_digest=?", (plan["plan_digest"],))
            db.execute("INSERT OR REPLACE INTO cloud_plans(plan_digest,plan_json,preflight_json,state) VALUES(?,?,?,?)", (plan["plan_digest"], json.dumps(public_plan_summary(plan), sort_keys=True), json.dumps(report, sort_keys=True), "preflighted"))
        return report

    def get(self, plan_digest: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as db:
            row = db.execute("SELECT plan_json,preflight_json,job_id,state,closure_json FROM cloud_plans WHERE plan_digest=?", (plan_digest,)).fetchone()
        if not row:
            return None
        return {"plan": json.loads(row[0]), "preflight": json.loads(row[1]), "job_id": row[2], "state": row[3], "closure": json.loads(row[4]) if row[4] else None}

    def cancel(self, plan_digest: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE cloud_plans SET state=CASE WHEN state IN ('terminal','cancelled') THEN state ELSE 'cancelling' END WHERE plan_digest=?", (plan_digest,))
        return self.get(plan_digest)

    def reconcile_unknown_submit(self, plan_digest: str, *, job_id: str | None = None) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE cloud_plans SET state='submitted',job_id=COALESCE(job_id,?) WHERE plan_digest=? AND state='submitting'", (job_id, plan_digest))
        return self.get(plan_digest)

    def submit(self, *, plan: dict[str, Any], preflight_id: str, candidate_id: str, key: str, request_digest: str, job_id: str) -> tuple[str, dict[str, Any] | None]:
        digest = plan["plan_digest"]
        with sqlite3.connect(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT job_id,plan_digest,preflight_json,request_digest FROM cloud_plans WHERE idempotency_key=?", (key,)).fetchone()
            if prior and prior[1] != digest:
                raise PlanError("idempotency key conflicts with a different plan")
            if prior and prior[0]:
                if not prior[3] or prior[3] != request_digest:
                    raise PlanError("idempotency key conflicts with a different request")
                return prior[0], json.loads(prior[2])
            row = db.execute("SELECT plan_digest,job_id,idempotency_key,preflight_json,state,request_digest FROM cloud_plans WHERE plan_digest=?", (digest,)).fetchone()
            if row and row[2] and row[2] != key:
                raise PlanError("idempotency key conflicts with this plan")
            if row and row[1]:
                if row[2] == key and row[5] == request_digest:
                    return row[1], json.loads(row[3])
                raise PlanError("plan was already submitted with different request data")
            if not row:
                raise PlanError("accepted preflight is required")
            report = json.loads(row[3])
            if report.get("preflight_id") != preflight_id or report.get("status") not in {"ready", "accepted"}:
                raise PlanError("accepted preflight binding is invalid")
            expires = str(report.get("expires_at") or "").replace("Z", "+00:00")
            try:
                if datetime.fromisoformat(expires) <= datetime.now(timezone.utc):
                    raise PlanError("preflight quote has expired")
            except ValueError as exc:
                raise PlanError("preflight quote expiry is invalid") from exc
            if candidate_id not in {str(c.get("candidate_id")) for c in report.get("candidates", [])} and report.get("candidate_id") != candidate_id:
                raise PlanError("candidate binding is invalid")
            db.execute("UPDATE cloud_plans SET job_id=?,idempotency_key=?,request_digest=?,state=? WHERE plan_digest=?", (job_id, key, request_digest, "submitting", digest))
            return job_id, None

    def abort_submit(self, plan_digest: str, job_id: str) -> None:
        with sqlite3.connect(self.path) as db:
            db.execute("DELETE FROM cloud_plans WHERE plan_digest=? AND job_id=? AND state='submitting'", (plan_digest, job_id))

    def close(self, digest: str, receipt: dict[str, Any]) -> None:
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE cloud_plans SET state='terminal',closure_json=? WHERE plan_digest=?", (json.dumps(receipt, sort_keys=True), digest))


class OfflineConnector:
    """Deterministic test connector. It never contacts or mutates a provider."""
    name = "offline"
    launches = 0
    terminations = 0
    network_calls = 0
    def list_available(self, **_: Any) -> list[dict[str, Any]]:
        return [{"id": "offline-offer", "offer_id": "offline-offer", "provider": "offline", "gpu_type": "offline", "gpu_ram_gb": 24, "hourly_rate": 0.01, "region": "offline-test"}]
    def launch(self, *args: Any, **kwargs: Any) -> None:
        self.launches += 1
        raise AssertionError("offline connector must not launch")
    def terminate(self, *_: Any, **__: Any) -> bool:
        self.terminations += 1
        raise AssertionError("offline connector must not terminate")
