import copy
import pytest

from cloud_offload.plan_protocol import PlanError, canonical_plan_digest, public_plan_summary, validate_cloud_plan


def plan():
    value = {
        "schema": "comfy.workflow.plan.v1", "plan_id": "p-1", "plan_digest": "", "project_id": "project-1", "input_revision": "rev-1", "operation": "render",
        "input_artifacts": [{"name": "source", "role": "input", "media_type": "application/octet-stream", "sha256": "a" * 64, "size": 1}],
        "stages": [{"id": "render", "kind": "tool", "depends_on": [], "operation": "offline-render", "settings": {}, "inputs": [], "outputs": [{"name": "result", "role": "output", "media_type": "image/png"}], "runner": {"profile": "offline"}, "retry": {"max_attempts": 1}, "checkpoint": {"required": True}, "fan_out": {"max_items": 1}}],
        "final_outputs": [{"stage_id": "render", "output": "result"}],
        "policy": {"residency": "cloud", "cancel_before_submit_is_free": True, "reuse_compatible_lease": True, "single_quote": True, "single_billing_closure": True, "retain_checkpoints": True},
    }
    value["plan_digest"] = canonical_plan_digest(value)
    return value


def test_valid_plan_has_stable_digest_and_safe_projection():
    value = plan()
    assert validate_cloud_plan(value) is value
    projection = public_plan_summary(value)
    assert projection["plan_digest"] == value["plan_digest"]
    assert "stages" in projection and "operation" not in projection["stages"][0]


@pytest.mark.parametrize("change, message", [(lambda p: p["stages"][0].update({"depends_on": ["missing"]}), "unknown dependency"), (lambda p: p["stages"].append(copy.deepcopy(p["stages"][0])), "stage ids"), (lambda p: p["policy"].update({"residency": "mars"}), "residency")])
def test_plan_rejects_unsafe_contract(change, message):
    value = plan()
    change(value)
    value["plan_digest"] = canonical_plan_digest(value)
    with pytest.raises(PlanError, match=message):
        validate_cloud_plan(value)


def test_digest_changes_when_plan_changes():
    value = plan()
    changed = copy.deepcopy(value)
    changed["operation"] = "other"
    assert canonical_plan_digest(value) != canonical_plan_digest(changed)
