import json
import sys
from dataclasses import dataclass

import pytest

from cloud_offload.benchmark import (
    PLAN_SCHEMA,
    BenchmarkPlan,
    BenchmarkRunner,
    InstanceObservation,
    write_scorecard,
)


def event(
    sequence,
    event_type,
    phase,
    seconds,
    *,
    instance_id="pod-1",
    hourly_rate=0.36,
):
    resources = {"provider": "runpod", "hourly_rate": hourly_rate}
    if instance_id:
        resources["worker_instance_id"] = instance_id
    return {
        "schema": "cloud-offload.job-event.v2",
        "sequence": sequence,
        "type": event_type,
        "phase": phase,
        "observed_at": f"2026-07-29T00:00:{seconds:02d}+00:00",
        "resources": resources,
    }


@dataclass
class Script:
    steps: list[dict]
    terminate_succeeds: bool = True


class FakeDriver:
    def __init__(self, scripts):
        self.clock = 0.0
        self.scripts = scripts
        self.jobs = {}
        self.current_job = None
        self.cancelled = set()
        self.terminated = set()
        self.termination_attempts = []
        self.hooks = []
        self.active_until = 0.0

    def monotonic(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += seconds

    def inventory(self, providers):
        result = {provider: {} for provider in providers}
        if self.current_job is None:
            return result
        state = self.jobs[self.current_job]
        step = state["script"].steps[state["index"]]
        for item in step.get("instances", []):
            key = (item.get("provider", "runpod"), item["id"])
            if key in self.terminated:
                continue
            observation = InstanceObservation(
                id=item["id"],
                provider=key[0],
                hourly_rate=float(item.get("hourly_rate", 0.36)),
                status=item.get("status", "running"),
                managed=item.get("managed", True),
                name=item.get("name", "cloud-offload-worker-test"),
            )
            result[key[0]][item["id"]] = observation
        return result

    def active_workers(self, providers):
        return (
            [{"provider": "runpod", "worker_id": "stale-worker"}]
            if self.clock < self.active_until
            else []
        )

    def submit(self, scenario):
        job_id = f"job-{len(self.jobs) + 1}"
        self.jobs[job_id] = {
            "script": self.scripts[scenario.name],
            "index": 0,
            "scenario": scenario,
        }
        self.current_job = job_id
        return job_id

    def snapshot(self, job_id):
        if job_id in self.cancelled:
            return {"status": "failed", "lifecycle_phase": "execution"}
        state = self.jobs[job_id]
        step = state["script"].steps[state["index"]]
        snapshot = dict(step["snapshot"])
        if state["index"] + 1 < len(state["script"].steps):
            state["index"] += 1
        return snapshot

    def events(self, job_id, after):
        state = self.jobs[job_id]
        available = [
            item
            for step in state["script"].steps[: state["index"] + 1]
            for item in step.get("events", [])
        ]
        return [item for item in available if item["sequence"] > after]

    def cancel(self, job_id):
        self.cancelled.add(job_id)
        return {"accepted": True, "status_code": 200}

    def terminate(self, provider, instance_id):
        self.termination_attempts.append((provider, instance_id))
        state = self.jobs[self.current_job]
        if state["script"].terminate_succeeds:
            self.terminated.add((provider, instance_id))
            return True
        return False

    def support_bundle(self, job_id):
        return {
            "schema": "cloud-offload.support-bundle.v1",
            "job": {"id": job_id},
        }

    def run_hook(self, injection, context):
        self.hooks.append((injection.kind, context))
        return {"exit_code": 0, "duration_seconds": 0.01, "output_omitted": True}


def plan_dict(scenarios, **limit_overrides):
    limits = {
        "max_total_cost_usd": 1.0,
        "max_scenario_cost_usd": 0.5,
        "max_campaign_seconds": 120,
        "poll_seconds": 1,
        "cleanup_timeout_seconds": 3,
    }
    limits.update(limit_overrides)
    return {
        "schema": PLAN_SCHEMA,
        "providers": ["runpod"],
        "exclusive": True,
        "limits": limits,
        "scenarios": scenarios,
    }


def scenario(name, cache_state, **overrides):
    value = {
        "name": name,
        "cache_state": cache_state,
        "endpoint": "/api/partitions",
        "request": {
            "partition": {"partition_id": name},
            "private_prompt": f"private-{name}",
        },
        "timeout_seconds": 20,
        "expected_statuses": ["completed"],
        "fresh_instance": True,
    }
    value.update(overrides)
    return value


def successful_script(pod_id, *, rate=0.36):
    return Script(
        [
            {
                "snapshot": {"status": "running", "lifecycle_phase": "worker_boot"},
                "events": [
                    event(
                        1,
                        "provisioning_started",
                        "provisioning",
                        0,
                        instance_id=None,
                        hourly_rate=rate,
                    ),
                    event(
                        2,
                        "runner_starting",
                        "worker_boot",
                        1,
                        instance_id=pod_id,
                        hourly_rate=rate,
                    ),
                ],
                "instances": [{"id": pod_id, "hourly_rate": rate}],
            },
            {
                "snapshot": {
                    "status": "completed",
                    "lifecycle_phase": "result_transfer",
                },
                "events": [
                    event(
                        3,
                        "executed",
                        "execution",
                        2,
                        instance_id=pod_id,
                        hourly_rate=rate,
                    )
                ],
                "instances": [{"id": pod_id, "hourly_rate": rate}],
            },
        ]
    )


def test_plan_requires_alternating_cold_hot_and_explicit_failure_hooks():
    with pytest.raises(ValueError, match="must alternate"):
        BenchmarkPlan.from_dict(
            plan_dict([scenario("cold-1", "cold"), scenario("cold-2", "cold")])
        )

    restart = scenario(
        "restart",
        "failure",
        failure={"kind": "restart", "trigger_phase": "execution"},
    )
    with pytest.raises(ValueError, match="requires hook_argv"):
        BenchmarkPlan.from_dict(plan_dict([restart]))


def test_cold_hot_campaign_records_comparable_json_and_removes_exact_pods(tmp_path):
    plan = BenchmarkPlan.from_dict(
        plan_dict([scenario("cold", "cold"), scenario("hot", "hot")])
    )
    driver = FakeDriver(
        {
            "cold": successful_script("pod-cold"),
            "hot": successful_script("pod-hot"),
        }
    )

    scorecard = BenchmarkRunner(driver).run(plan)
    destination = write_scorecard(tmp_path / "scorecard.json", scorecard)
    encoded = destination.read_text(encoding="utf-8")

    assert scorecard["passed"] is True
    assert [item["cache_state"] for item in scorecard["results"]] == ["cold", "hot"]
    assert all(item["fresh_instance_observed"] for item in scorecard["results"])
    assert scorecard["distributions"]["cold"]["duration_seconds"]["count"] == 1
    assert scorecard["distributions"]["hot"]["duration_seconds"]["count"] == 1
    assert scorecard["distributions"]["resource_closure_seconds"]["count"] == 2
    assert driver.termination_attempts == [
        ("runpod", "pod-cold"),
        ("runpod", "pod-hot"),
    ]
    assert scorecard["orphaned_resources"] == []
    assert "private-cold" not in encoded
    assert "private-hot" not in encoded
    assert json.loads(encoded)["schema"] == "cloud-offload.benchmark-scorecard.v1"


def test_scenario_cost_limit_cancels_and_still_removes_the_pod():
    expensive = successful_script("pod-expensive", rate=3600)
    expensive.steps[1]["snapshot"] = {
        "status": "running",
        "lifecycle_phase": "execution",
    }
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [scenario("cold", "cold")],
            max_scenario_cost_usd=0.5,
            max_total_cost_usd=0.75,
        )
    )
    driver = FakeDriver({"cold": expensive})

    scorecard = BenchmarkRunner(driver).run(plan)
    result = scorecard["results"][0]

    assert result["limit_triggered"] == "scenario_cost_limit"
    assert result["passed"] is False
    assert "job-1" in driver.cancelled
    assert ("runpod", "pod-expensive") in driver.terminated
    assert scorecard["orphaned_resources"] == []


def test_cancellation_and_hook_failures_are_injected_at_the_requested_phase():
    scripts = {
        "cancel": successful_script("pod-cancel"),
        "restart": successful_script("pod-restart"),
    }
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [
                scenario(
                    "cancel",
                    "failure",
                    expected_statuses=["failed"],
                    failure={"kind": "cancellation", "trigger_phase": "worker_boot"},
                ),
                scenario(
                    "restart",
                    "failure",
                    failure={
                        "kind": "restart",
                        "trigger_phase": "worker_boot",
                        "hook_argv": ["restart-coordinator"],
                    },
                ),
            ]
        )
    )
    driver = FakeDriver(scripts)

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is True
    assert scorecard["results"][0]["failure_injection"]["kind"] == "cancellation"
    assert scorecard["results"][1]["failure_injection"]["hook"]["exit_code"] == 0
    assert driver.hooks[0][0] == "restart"
    assert driver.hooks[0][1]["CLOUD_OFFLOAD_BENCHMARK_JOB_ID"] == "job-2"


def test_provider_failure_terminates_only_the_observed_instance():
    plan = BenchmarkPlan.from_dict(
        plan_dict(
            [
                scenario(
                    "provider",
                    "failure",
                    failure={"kind": "provider", "trigger_phase": "worker_boot"},
                )
            ]
        )
    )
    driver = FakeDriver({"provider": successful_script("pod-provider")})

    scorecard = BenchmarkRunner(driver).run(plan)
    injection = scorecard["results"][0]["failure_injection"]

    assert scorecard["passed"] is True
    assert injection["receipts"] == [
        {
            "provider": "runpod",
            "instance_id": "pod-provider",
            "termination_requested": True,
        }
    ]
    assert driver.termination_attempts == [("runpod", "pod-provider")]


def test_cleanup_failure_is_a_hard_campaign_failure_and_reports_the_orphan():
    driver = FakeDriver(
        {"cold": Script(successful_script("pod-stuck").steps, terminate_succeeds=False)}
    )
    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is False
    assert scorecard["campaign_abort"] == "orphan_cleanup_failed"
    assert scorecard["results"][0]["orphaned_resources"]
    assert scorecard["orphaned_resources"] == [
        {"provider": "runpod", "instance_id": "pod-stuck"}
    ]


def test_fresh_pod_waits_for_stale_worker_heartbeat_without_charging_scenario_time():
    driver = FakeDriver({"cold": successful_script("pod-cold")})
    driver.active_until = 3
    plan = BenchmarkPlan.from_dict(plan_dict([scenario("cold", "cold")]))

    scorecard = BenchmarkRunner(driver).run(plan)

    assert scorecard["passed"] is True
    # The result includes one polling second plus two cleanup-verification
    # seconds, but excludes the three seconds spent aging out the stale worker.
    assert scorecard["results"][0]["duration_seconds"] == 3
    assert driver.clock >= 6


def test_cli_validation_redacts_request_and_run_requires_spend_confirmation(
    monkeypatch, capsys, tmp_path
):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(plan_dict([scenario("cold", "cold")])), encoding="utf-8"
    )
    from cloud_offload.__main__ import main

    monkeypatch.setattr(
        sys,
        "argv",
        ["cloud-offload", "benchmark", "validate", "--plan", str(plan_path)],
    )
    main()
    validated = capsys.readouterr().out

    assert "private-cold" not in validated
    assert "request_digest" in validated

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cloud-offload",
            "benchmark",
            "run",
            "--plan",
            str(plan_path),
            "--output",
            str(tmp_path / "scorecard.json"),
        ],
    )
    with pytest.raises(SystemExit) as stopped:
        main()

    assert stopped.value.code == 2
    assert "--confirm-spend" in capsys.readouterr().err

    hooked_path = tmp_path / "hooked.json"
    hooked_path.write_text(
        json.dumps(
            plan_dict(
                [
                    scenario(
                        "restart",
                        "failure",
                        failure={
                            "kind": "restart",
                            "hook_argv": ["restart-coordinator"],
                        },
                    )
                ]
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cloud-offload",
            "benchmark",
            "run",
            "--plan",
            str(hooked_path),
            "--output",
            str(tmp_path / "scorecard.json"),
            "--confirm-spend",
        ],
    )
    with pytest.raises(SystemExit) as hook_stopped:
        main()

    assert hook_stopped.value.code == 2
    assert "--allow-hooks" in capsys.readouterr().err
