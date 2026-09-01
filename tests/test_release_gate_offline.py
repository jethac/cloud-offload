"""Offline end-to-end exercise of the M7 release controller.

These tests drive ``ReleaseExecutor`` against a fake benchmark runner, fake
coordinator driver, and real temporary git repositories, so the whole
plan -> matrix -> ledger -> gate loop runs without any provider access.
"""

import json
import subprocess
import pytest

import cloud_offload.release_gate as release_gate
from cloud_offload.benchmark import InstanceObservation, write_scorecard
from cloud_offload.config import CloudConfig
from cloud_offload.release_gate import (
    RELEASE_PLAN_SCHEMA,
    ReleaseExecutor,
    ReleasePlan,
    _scorecard_receipt,
    load_ledger,
    new_ledger,
    update_ledger,
    write_release_projection,
)

from tests.test_release_gate import (
    IMAGE_DIGEST,
    benchmark_plan,
    matrix_receipt,
    release_scorecard,
)


SERVICE_TOKEN = "service-token-must-stay-private"
SERVICE_URL = "http://127.0.0.1:59999"


def _git(repo, *argv):
    return subprocess.run(
        ["git", "-C", str(repo), *argv],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def make_repo(tmp_path, name):
    repo = tmp_path / name
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    _git(repo, "config", "user.email", "release@example.com")
    _git(repo, "config", "user.name", "Release Gate")
    (repo / "README.md").write_text("release fixture\n", encoding="utf-8")
    (repo / "test_contract_ok.py").write_text(
        "def test_ok():\n    assert True\n\n"
        "def test_also_ok():\n    assert True\n",
        encoding="utf-8",
    )
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "fixture")
    return repo, _git(repo, "rev-parse", "HEAD").lower()


def offline_release_plan(tmp_path, *, regions=("US-MD-1", "EU-RO-1"), required=30):
    backend, backend_rev = make_repo(tmp_path, "backend-repo")
    extension, extension_rev = make_repo(tmp_path, "extension-repo")
    cases = []
    for region in regions:
        benchmark_path = tmp_path / f"benchmark-{region}.json"
        benchmark_path.write_text(
            json.dumps(benchmark_plan(region)), encoding="utf-8"
        )
        cases.append(
            {
                "name": f"comfyui-{region.lower()}",
                "profile": "comfyui",
                "region": region,
                "benchmark_plan": str(benchmark_path),
            }
        )
    value = {
        "schema": RELEASE_PLAN_SCHEMA,
        "required_consecutive_matrices": required,
        "repositories": [
            {"name": "backend", "path": str(backend), "revision": backend_rev},
            {
                "name": "extension",
                "path": str(extension),
                "revision": extension_rev,
            },
        ],
        "profiles": [{"name": "comfyui", "image_digest": IMAGE_DIGEST}],
        "regions": list(regions),
        "cases": cases,
        "limits": {
            "max_total_cost_usd": 100,
            "max_matrix_cost_usd": 0.5,
            "max_total_seconds": 1000000,
            "max_matrix_seconds": 2200,
            "contract_test_timeout_seconds": 300,
            "cancellation_slo_seconds": 90,
            "provider_closure_slo_seconds": 90,
            "reload_slo_seconds": 2,
            "hot_preparation_ratio_max": 0.25,
            "max_monthly_storage_cost_usd": 10,
        },
    }
    path = tmp_path / "release.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


class FakeWorld:
    """Shared state between the fake driver and the fake runner."""

    def __init__(self, plan: ReleasePlan):
        self.plan = plan
        self.outcomes: list[str] = []
        self.ran_cases: list[str] = []
        self.jobs: dict[str, dict] = {}
        self.volumes = [
            {
                "provider": "runpod",
                "status": "ready",
                "capacity_bytes": 50 * 1024**3,
            }
        ]
        self.storage_policy = {"max_monthly_storage_cost": 10}

    def case_for(self, benchmark) -> "release_gate.ReleaseCase":
        region = benchmark.scenarios[0].allowed_regions[0]
        return next(item for item in self.plan.cases if item.region == region)

    def scorecard(self, benchmark) -> dict:
        case = self.case_for(benchmark)
        outcome = self.outcomes.pop(0) if self.outcomes else "pass"
        self.ran_cases.append(case.name)
        scorecard = release_scorecard(case)
        for index, result in enumerate(scorecard["results"]):
            job_id = f"job-{case.name}-{len(self.ran_cases)}-{index}"
            result["job_id"] = job_id
            result["status"] = "completed"
            self.jobs[job_id] = {
                "status": "completed",
                "events": [
                    {"sequence": sequence, "type": "job_status_changed"}
                    for sequence in (1, 2, 3)
                ],
            }
        if outcome == "fail":
            scorecard["passed"] = False
            scorecard["orphaned_resources"] = [
                {"provider": "runpod", "instance_id": "orphan-1"}
            ]
        elif outcome == "interrupt":
            scorecard["passed"] = False
            scorecard["campaign_abort"] = "operator_interrupt:KeyboardInterrupt"
            scorecard["results"] = scorecard["results"][:1]
            scorecard["results"][0]["passed"] = False
            scorecard["results"][0]["abort_reason"] = (
                "operator_interrupt:KeyboardInterrupt"
            )
        return scorecard


class FakeDriver:
    def __init__(self, world: FakeWorld, url, token, config, providers, *, allow_hooks):
        assert url == SERVICE_URL
        self.world = world

    def snapshot(self, job_id):
        return {"status": self.world.jobs[job_id]["status"]}

    def events(self, job_id, after):
        return [
            item
            for item in self.world.jobs[job_id]["events"]
            if item["sequence"] > after
        ]

    def cache_status(self):
        return {
            "volumes": self.world.volumes,
            "policy": self.world.storage_policy,
        }

    def inventory(self, providers):
        return {provider: {} for provider in providers}

    def terminate(self, provider, instance_id):
        return True

    @staticmethod
    def monotonic():
        return 0.0

    @staticmethod
    def sleep(seconds):
        return None


class FakeRunner:
    def __init__(self, driver: FakeDriver):
        self.driver = driver

    def run(self, benchmark):
        return self.driver.world.scorecard(benchmark)


@pytest.fixture
def offline(tmp_path, monkeypatch):
    plan_path = offline_release_plan(tmp_path)
    plan = ReleasePlan.load(plan_path)
    world = FakeWorld(plan)
    monkeypatch.setattr(
        release_gate,
        "CoordinatorBenchmarkDriver",
        lambda *args, **kwargs: FakeDriver(world, *args, **kwargs),
    )
    monkeypatch.setattr(
        release_gate, "BenchmarkRunner", lambda driver: FakeRunner(driver)
    )
    monkeypatch.setattr(
        release_gate,
        "CONTRACT_TEST_GROUPS",
        {"offline_contract": ("test_contract_ok.py",)},
    )
    config = CloudConfig(
        worker_profiles={
            "comfyui": {
                "image": f"ghcr.io/example/worker@{IMAGE_DIGEST}",
                "models": ["comfyui-partition-v1"],
            }
        }
    )
    ledger_path = tmp_path / "state" / "release-ledger.json"
    output_dir = tmp_path / "runlogs"

    def executor():
        return ReleaseExecutor(
            plan,
            ledger_path,
            output_dir,
            config,
            {"url": SERVICE_URL, "token": SERVICE_TOKEN},
            allow_hooks=True,
        )

    return plan, world, executor, ledger_path, output_dir


def test_thirty_passing_matrices_satisfy_the_gate(offline):
    plan, world, executor, ledger_path, output_dir = offline

    ledger = executor().run(max_matrices=29)
    assert ledger["passed"] is False
    assert ledger["consecutive_passes"] == 29
    assert ledger["last_stop_reason"] == "requested_matrix_limit"

    ledger = executor().run(max_matrices=1)
    assert ledger["passed"] is True
    assert ledger["consecutive_passes"] == 30
    assert ledger["last_stop_reason"] == "release_passed"
    assert ledger["coverage"]["cases"] == sorted(
        item.name for item in plan.cases
    )
    assert ledger["coverage"]["regions"] == ["EU-RO-1", "US-MD-1"]
    assert ledger["coverage"]["profiles"] == ["comfyui"]
    for receipt in ledger["matrices"]:
        assert receipt["passed"] is True
        assert receipt["contract_tests"]["passed"] is True
        assert receipt["contract_tests"]["test_count"] == 2
        assert receipt["replay_probe"]["passed"] is True
        assert receipt["storage_budget"]["passed"] is True
    matrix_dirs = sorted(output_dir.glob("matrix-*"))
    assert len(matrix_dirs) == 30
    for matrix_dir in matrix_dirs:
        assert (matrix_dir / "benchmark-scorecard.json").exists()
        assert (matrix_dir / "contract-tests.log").exists()

    resumed = executor().run()
    assert resumed["last_stop_reason"] == "release_already_passed"
    assert len(resumed["matrices"]) == 30


def test_case_rotation_is_stable_across_resume_and_failure(offline):
    plan, world, executor, ledger_path, _ = offline
    world.outcomes = ["pass", "pass", "pass", "fail"]

    executor().run(max_matrices=3)
    ledger = executor().run(max_matrices=3)
    assert ledger["last_stop_reason"] == "matrix_failed"
    assert ledger["consecutive_passes"] == 0

    executor().run(max_matrices=4)
    expected = [
        plan.cases[index % len(plan.cases)].name for index in range(8)
    ]
    assert world.ran_cases == expected


def test_single_failure_resets_the_consecutive_count(offline):
    plan, world, executor, ledger_path, _ = offline
    world.outcomes = ["pass"] * 5 + ["fail"]

    ledger = executor().run(max_matrices=10)
    assert ledger["last_stop_reason"] == "matrix_failed"
    assert ledger["consecutive_passes"] == 0
    assert len(ledger["matrices"]) == 6
    assert ledger["matrices"][-1]["passed"] is False
    assert "orphan_or_audit_failure" in ledger["matrices"][-1]["failure_codes"]

    ledger = executor().run(max_matrices=30)
    assert ledger["passed"] is True
    assert ledger["consecutive_passes"] == 30
    assert len(ledger["matrices"]) == 36


def test_ledger_writes_are_atomic_and_reload_correctly(offline):
    plan, world, executor, ledger_path, _ = offline

    executor().run(max_matrices=3)
    assert not list(ledger_path.parent.glob("*.tmp"))
    on_disk = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert on_disk["release_plan_digest"] == plan.digest
    assert len(on_disk["matrices"]) == 3

    reloaded = load_ledger(ledger_path, plan)
    update_ledger(reloaded, plan)
    assert reloaded["consecutive_passes"] == 3

    on_disk["release_plan_digest"] = "sha256:" + "0" * 64
    ledger_path.write_text(json.dumps(on_disk), encoding="utf-8")
    with pytest.raises(ValueError, match="different release plan"):
        load_ledger(ledger_path, plan)


def test_ledger_projection_is_finite_and_redacted(offline, tmp_path):
    plan, world, executor, ledger_path, output_dir = offline

    executor().run(max_matrices=4)
    projection = write_release_projection(
        tmp_path / "projection.json", json.loads(ledger_path.read_text())
    )
    for text in (
        ledger_path.read_text(encoding="utf-8"),
        projection.read_text(encoding="utf-8"),
    ):
        assert "must-not-enter-safe-summary" not in text
        assert SERVICE_TOKEN not in text
        assert SERVICE_URL not in text
        assert str(output_dir) not in text
        assert str(plan.plan_path) not in text
        assert "hook_argv" not in text
        assert "workflow" not in text
        parsed = json.loads(text)
        for receipt in parsed["matrices"]:
            assert "results" not in receipt
            assert receipt["benchmark_scorecard_digest"].startswith("sha256:")


def test_trailing_window_must_cover_every_case(offline):
    plan, world, executor, ledger_path, _ = offline

    ledger = new_ledger(plan)
    for index in range(1, 31):
        ledger["matrices"].append(matrix_receipt(index, plan.cases[0]))
    update_ledger(ledger, plan)
    assert ledger["consecutive_passes"] == 30
    assert ledger["passed"] is False

    ledger = new_ledger(plan)
    ledger["matrices"].append(matrix_receipt(1, plan.cases[1]))
    for index in range(2, 32):
        ledger["matrices"].append(matrix_receipt(index, plan.cases[0]))
    update_ledger(ledger, plan)
    assert ledger["consecutive_passes"] == 31
    assert ledger["passed"] is False


def test_gate_requires_total_budget_and_stops_spending_at_the_ceiling(offline):
    plan, world, executor, ledger_path, _ = offline

    ledger = new_ledger(plan)
    for index in range(1, 31):
        receipt = matrix_receipt(index, plan.cases[(index - 1) % len(plan.cases)])
        receipt["estimated_compute_cost_upper_usd"] = 4.0
        ledger["matrices"].append(receipt)
    update_ledger(ledger, plan)
    assert ledger["consecutive_passes"] == 30
    assert ledger["passed"] is False

    release_gate._atomic_json(ledger_path, ledger)
    before = len(world.ran_cases)
    resumed = executor().run(max_matrices=5)
    assert resumed["last_stop_reason"] == "total_cost_limit"
    assert len(world.ran_cases) == before


def test_hook_scenarios_require_reviewed_hooks(offline):
    plan, world, executor, ledger_path, output_dir = offline
    config = CloudConfig(
        worker_profiles={
            "comfyui": {
                "image": f"ghcr.io/example/worker@{IMAGE_DIGEST}",
                "models": ["comfyui-partition-v1"],
            }
        }
    )
    guarded = ReleaseExecutor(
        plan,
        ledger_path,
        output_dir,
        config,
        {"url": SERVICE_URL, "token": SERVICE_TOKEN},
        allow_hooks=False,
    )
    with pytest.raises(RuntimeError, match="reviewed benchmark hooks"):
        guarded.run(max_matrices=1)


def test_dirty_or_wrong_revision_worktree_fails_the_precheck(offline):
    plan, world, executor, ledger_path, _ = offline
    backend = next(
        item for item in plan.repositories if item.name == "backend"
    )
    (backend.path / "README.md").write_text("drift\n", encoding="utf-8")

    ledger = executor().run(max_matrices=1)
    receipt = ledger["matrices"][-1]
    assert receipt["passed"] is False
    assert receipt["failure_codes"] == ["release_precheck_failed"]
    assert receipt["provider_mutation"] is False
    assert world.ran_cases == []


def test_release_plan_rejects_more_cases_than_the_window_can_cover(tmp_path):
    regions = [f"R{index:02d}-X" for index in range(31)]
    path = offline_release_plan(tmp_path, regions=regions, required=30)
    with pytest.raises(ValueError, match="trailing window"):
        ReleasePlan.load(path)


def test_missing_provider_closure_measurement_fails_the_matrix(tmp_path):
    path = offline_release_plan(tmp_path, regions=("US-MD-1",))
    plan = ReleasePlan.load(path)
    case = plan.cases[0]
    scorecard = release_scorecard(case)
    del scorecard["results"][0]["resource_closure_seconds"]
    receipt = _scorecard_receipt(
        scorecard, case, plan.limits, {"passed": True}, {"passed": True}
    )
    assert receipt["passed"] is False
    assert "provider_closure_measurement_missing" in receipt["failure_codes"]


def test_replay_probe_rejects_jobs_with_no_events(offline):
    plan, world, executor, ledger_path, _ = offline

    original = world.scorecard

    def eventless(benchmark):
        scorecard = original(benchmark)
        for job in world.jobs.values():
            job["events"] = []
        return scorecard

    world.scorecard = eventless
    ledger = executor().run(max_matrices=1)
    receipt = ledger["matrices"][-1]
    assert receipt["passed"] is False
    assert "reload_reconnect_event_order" in receipt["failure_codes"]
    assert receipt["replay_probe"]["events_strictly_ordered"] is False


def test_harness_crash_is_recorded_as_a_failed_matrix(offline):
    plan, world, executor, ledger_path, _ = offline

    executor().run(max_matrices=2)

    original = world.scorecard

    def crashing(benchmark):
        raise ConnectionError("coordinator died mid-matrix")

    world.scorecard = crashing
    ledger = executor().run(max_matrices=5)
    world.scorecard = original

    assert ledger["last_stop_reason"] == "matrix_failed"
    assert ledger["consecutive_passes"] == 0
    assert len(ledger["matrices"]) == 3
    receipt = ledger["matrices"][-1]
    assert receipt["passed"] is False
    assert receipt["failure_codes"] == ["release_harness_error:ConnectionError"]
    assert "coordinator died mid-matrix" not in json.dumps(receipt)

    reloaded = load_ledger(ledger_path, plan)
    assert reloaded["consecutive_passes"] == 0
    assert len(reloaded["matrices"]) == 3


def test_operator_interrupt_atomically_publishes_aborted_scorecard_and_ledger(offline):
    plan, world, executor, ledger_path, output_dir = offline
    world.outcomes = ["interrupt"]

    ledger = executor().run(max_matrices=2)

    assert ledger["last_stop_reason"] == "operator_interrupt:KeyboardInterrupt"
    assert ledger["last_run_matrix_count"] == 1
    assert len(ledger["matrices"]) == 1
    receipt = ledger["matrices"][0]
    assert receipt["passed"] is False
    assert receipt["matrix_stop_reason"] == "operator_interrupt:KeyboardInterrupt"
    scorecard_path = next(output_dir.glob("matrix-*/benchmark-scorecard.json"))
    scorecard = json.loads(scorecard_path.read_text(encoding="utf-8"))
    assert scorecard["campaign_abort"] == "operator_interrupt:KeyboardInterrupt"
    assert scorecard["results"][0]["abort_reason"] == (
        "operator_interrupt:KeyboardInterrupt"
    )
    assert not list(output_dir.rglob("*.tmp"))
    encoded_ledger = ledger_path.read_text(encoding="utf-8")
    assert SERVICE_TOKEN not in encoded_ledger
    assert SERVICE_URL not in encoded_ledger
    assert str(output_dir) not in encoded_ledger


def test_release_layer_interrupt_still_writes_failed_cleanup_unknown_receipts(offline):
    plan, world, executor, ledger_path, output_dir = offline

    def interrupted(benchmark):
        raise KeyboardInterrupt

    world.scorecard = interrupted

    ledger = executor().run(max_matrices=2)

    assert ledger["last_stop_reason"] == "operator_interrupt:KeyboardInterrupt"
    assert len(ledger["matrices"]) == 1
    receipt = ledger["matrices"][0]
    assert receipt["cleanup_proof"] == {"state": "confirmed"}
    assert receipt["provider_mutation"] == "unknown"
    assert receipt["estimated_compute_cost_upper_usd"] == (
        plan.limits.max_matrix_cost_usd
    )
    assert receipt["duration_seconds"] == plan.limits.max_matrix_seconds
    assert receipt["duration_basis"] == "matrix_limit_conservative"
    assert receipt["failure_codes"] == ["operator_interrupt:KeyboardInterrupt"]
    scorecard_path = next(output_dir.glob("matrix-*/benchmark-scorecard.json"))
    scorecard = json.loads(scorecard_path.read_text(encoding="utf-8"))
    assert scorecard["campaign_abort"] == "operator_interrupt:KeyboardInterrupt"
    assert scorecard["cleanup_proof"] == {"state": "confirmed"}
    assert not list(output_dir.rglob("*.tmp"))
    encoded = ledger_path.read_text(encoding="utf-8")
    assert SERVICE_TOKEN not in encoded
    assert SERVICE_URL not in encoded
    assert str(output_dir) not in encoded


def test_interrupt_fallback_never_trusts_an_older_lower_scorecard_or_clock(offline, monkeypatch):
    plan, world, executor, _, output_dir = offline
    release = executor()
    case = plan.cases[0]
    matrix_dir = output_dir / f"matrix-0001-{case.name}"
    stale = world.scorecard(case.benchmark_plan)
    stale["estimated_compute_cost_upper_usd"] = 0.01
    for result in stale["results"]:
        result["duration_seconds"] = 0.01
    write_scorecard(matrix_dir / "benchmark-scorecard.json", stale)
    clock = iter([25.0, 40.0])
    monkeypatch.setattr(release_gate.time, "monotonic", lambda: next(clock, 40.0))

    receipt = release._release_interrupt_receipt(
        1, case, KeyboardInterrupt(), matrix_started=10.0
    )

    assert receipt["estimated_compute_cost_upper_usd"] >= plan.limits.max_matrix_cost_usd
    assert receipt["duration_seconds"] >= plan.limits.max_matrix_seconds
    published = json.loads((matrix_dir / "benchmark-scorecard.json").read_text())
    assert published["estimated_compute_cost_upper_usd"] >= plan.limits.max_matrix_cost_usd
    assert published["campaign_abort"] == "operator_interrupt:KeyboardInterrupt"


@pytest.mark.parametrize("failure_site", ["replay", "storage"])
def test_paid_matrix_postcheck_exception_uses_durable_conservative_audit(
    offline, monkeypatch, failure_site
):
    plan, world, executor, _, output_dir = offline

    def fail(*args, **kwargs):
        raise RuntimeError(
            "AKIAABCDEFGHIJKLMNOP https://user:token@example.invalid/ "
            r"\\server\private\artifact"
        )

    target = "_replay_probe" if failure_site == "replay" else "_storage_budget_receipt"
    monkeypatch.setattr(release_gate, target, fail)

    ledger = executor().run(max_matrices=1)
    receipt = ledger["matrices"][0]

    assert receipt["passed"] is False
    assert receipt["provider_mutation"] == "unknown"
    assert receipt["estimated_compute_cost_upper_usd"] >= plan.limits.max_matrix_cost_usd
    assert receipt["duration_seconds"] >= plan.limits.max_matrix_seconds
    assert receipt["failure_codes"] == ["release_harness_error:RuntimeError"]
    encoded = json.dumps(ledger) + next(
        output_dir.glob("matrix-*/benchmark-scorecard.json")
    ).read_text()
    for forbidden in ("AKIA", "example.invalid", "server", "token"):
        assert forbidden not in encoded


def test_release_fallback_does_not_delete_unattributed_managed_resource(
    offline, monkeypatch
):
    plan, world, executor, _, _ = offline
    termination_calls = []

    class ActiveFallbackDriver(FakeDriver):
        def inventory(self, providers):
            return {
                "runpod": {
                    "pod-unattributed": InstanceObservation(
                        id="pod-unattributed",
                        provider="runpod",
                        hourly_rate=0.5,
                        status="running",
                        managed=True,
                    )
                }
            }

        def terminate(self, provider, instance_id):
            termination_calls.append((provider, instance_id))
            return True

    monkeypatch.setattr(
        release_gate,
        "CoordinatorBenchmarkDriver",
        lambda *args, **kwargs: ActiveFallbackDriver(world, *args, **kwargs),
    )

    def interrupted(benchmark):
        raise KeyboardInterrupt

    world.scorecard = interrupted
    ledger = executor().run(max_matrices=1)

    receipt = ledger["matrices"][0]
    assert termination_calls == []
    assert receipt["cleanup_proof"] == {"state": "failed"}
    assert "cleanup_unverified" in receipt["failure_codes"]
    assert receipt["estimated_compute_cost_upper_usd"] == plan.limits.max_total_cost_usd
    assert receipt["duration_seconds"] == plan.limits.max_total_seconds
    assert receipt["ongoing_orphan_cost"] is True
