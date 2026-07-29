"""Spend-capped production benchmarking for Cloud Offload.

The harness is deliberately control-plane-first: it never launches a provider
resource itself. It submits an ordinary job, observes the durable JobEventV2
journal, accounts conservatively from the quoted hourly rate, and terminates only
the exact Cloud Offload resources attributable to the campaign.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import requests

from cloud_offload.config import CloudConfig
from cloud_offload.providers import create_connector


PLAN_SCHEMA = "cloud-offload.benchmark-plan.v1"
SCORECARD_SCHEMA = "cloud-offload.benchmark-scorecard.v1"
SCENARIO_SCHEMA = "cloud-offload.benchmark-scenario-result.v1"
TERMINAL_STATUSES = {"completed", "failed", "dead_letter"}
FAILURE_KINDS = {"cancellation", "provider", "storage", "corruption", "restart"}
CACHE_STATES = {"cold", "hot", "failure"}
PREPARED_STORAGE_POLICIES = {"off", "smart", "strict", "pinned"}
SUBMISSION_ENDPOINTS = {"/api/partitions", "/api/workflows"}
MANAGED_INSTANCE_PREFIX = "cloud-offload-worker-"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _bounded_positive(value: Any, label: str, *, allow_zero: bool = False) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a number") from exc
    minimum = 0 if allow_zero else 0.000001
    if not math.isfinite(number) or number < minimum:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{label} must be a finite {qualifier} number")
    return number


@dataclass(frozen=True)
class BenchmarkLimits:
    max_total_cost_usd: float
    max_scenario_cost_usd: float
    max_campaign_seconds: float
    poll_seconds: float = 2.0
    cleanup_timeout_seconds: float = 90.0
    fresh_worker_timeout_seconds: float = 120.0

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BenchmarkLimits":
        return cls(
            max_total_cost_usd=_bounded_positive(
                value.get("max_total_cost_usd"), "limits.max_total_cost_usd"
            ),
            max_scenario_cost_usd=_bounded_positive(
                value.get("max_scenario_cost_usd"),
                "limits.max_scenario_cost_usd",
            ),
            max_campaign_seconds=_bounded_positive(
                value.get("max_campaign_seconds"), "limits.max_campaign_seconds"
            ),
            poll_seconds=_bounded_positive(
                value.get("poll_seconds", 2), "limits.poll_seconds"
            ),
            cleanup_timeout_seconds=_bounded_positive(
                value.get("cleanup_timeout_seconds", 90),
                "limits.cleanup_timeout_seconds",
            ),
            fresh_worker_timeout_seconds=_bounded_positive(
                value.get("fresh_worker_timeout_seconds", 120),
                "limits.fresh_worker_timeout_seconds",
            ),
        )


@dataclass(frozen=True)
class FailureInjection:
    kind: str
    trigger_phase: str | None = None
    trigger_event: str | None = None
    after_seconds: float = 0.0
    hook_argv: tuple[str, ...] = ()
    before_submit: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "FailureInjection":
        kind = str(value.get("kind") or "").strip().lower()
        if kind not in FAILURE_KINDS:
            raise ValueError(
                f"failure.kind must be one of {', '.join(sorted(FAILURE_KINDS))}"
            )
        hook = value.get("hook_argv") or []
        if not isinstance(hook, list) or any(not str(item).strip() for item in hook):
            raise ValueError("failure.hook_argv must be a list of non-empty strings")
        if kind in {"storage", "corruption", "restart"} and not hook:
            raise ValueError(f"failure kind {kind!r} requires hook_argv")
        if kind in {"cancellation", "provider"} and hook:
            raise ValueError(
                f"failure kind {kind!r} uses a built-in action, not a hook"
            )
        before_submit = bool(value.get("before_submit", False))
        if before_submit and kind not in {"storage", "corruption", "restart"}:
            raise ValueError("failure.before_submit requires an external hook kind")
        return cls(
            kind=kind,
            trigger_phase=(
                str(value["trigger_phase"]) if value.get("trigger_phase") else None
            ),
            trigger_event=(
                str(value["trigger_event"]) if value.get("trigger_event") else None
            ),
            after_seconds=_bounded_positive(
                value.get("after_seconds", 0),
                "failure.after_seconds",
                allow_zero=True,
            ),
            hook_argv=tuple(str(item) for item in hook),
            before_submit=before_submit,
        )


@dataclass(frozen=True)
class BenchmarkScenario:
    name: str
    cache_state: str
    endpoint: str
    request: dict[str, Any]
    timeout_seconds: float
    expected_statuses: tuple[str, ...]
    fresh_instance: bool = True
    prepared_storage_policy: str | None = None
    failure: FailureInjection | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any], index: int) -> "BenchmarkScenario":
        name = str(value.get("name") or "").strip()
        if not name:
            raise ValueError(f"scenarios[{index}].name is required")
        cache_state = str(value.get("cache_state") or "").strip().lower()
        if cache_state not in CACHE_STATES:
            raise ValueError(
                f"scenarios[{index}].cache_state must be cold, hot, or failure"
            )
        endpoint = str(value.get("endpoint") or "").strip()
        if endpoint not in SUBMISSION_ENDPOINTS:
            raise ValueError(
                f"scenarios[{index}].endpoint must be /api/partitions or /api/workflows"
            )
        request = value.get("request")
        if not isinstance(request, dict):
            raise ValueError(f"scenarios[{index}].request must be an object")
        if (
            endpoint == "/api/partitions"
            and bool(value.get("fresh_instance", True))
            and request.get("force_execution") is not True
        ):
            raise ValueError(
                f"scenarios[{index}] requires request.force_execution=true "
                "to prove a fresh-Pod run"
            )
        try:
            _canonical_digest(request)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"scenarios[{index}].request must contain finite JSON"
            ) from exc
        statuses = value.get("expected_statuses") or ["completed"]
        if not isinstance(statuses, list) or not statuses:
            raise ValueError(f"scenarios[{index}].expected_statuses must be a list")
        normalized_statuses = tuple(str(item) for item in statuses)
        unknown = set(normalized_statuses) - TERMINAL_STATUSES
        if unknown:
            raise ValueError(
                f"scenarios[{index}].expected_statuses contains non-terminal values: "
                + ", ".join(sorted(unknown))
            )
        failure = value.get("failure")
        if failure is not None and not isinstance(failure, dict):
            raise ValueError(f"scenarios[{index}].failure must be an object")
        raw_storage_policy = value.get("prepared_storage_policy")
        storage_policy = (
            str(raw_storage_policy).strip().lower()
            if raw_storage_policy is not None
            else None
        )
        if (
            storage_policy is not None
            and storage_policy not in PREPARED_STORAGE_POLICIES
        ):
            raise ValueError(
                f"scenarios[{index}].prepared_storage_policy must be one of "
                + ", ".join(sorted(PREPARED_STORAGE_POLICIES))
            )
        if cache_state == "cold" and storage_policy != "off":
            raise ValueError(
                f"scenarios[{index}] cold runs require prepared_storage_policy='off'"
            )
        if cache_state == "hot" and storage_policy not in {
            "smart",
            "strict",
            "pinned",
        }:
            raise ValueError(
                f"scenarios[{index}] hot runs require an enabled "
                "prepared_storage_policy"
            )
        return cls(
            name=name,
            cache_state=cache_state,
            endpoint=endpoint,
            request=request,
            timeout_seconds=_bounded_positive(
                value.get("timeout_seconds"), f"scenarios[{index}].timeout_seconds"
            ),
            expected_statuses=normalized_statuses,
            fresh_instance=bool(value.get("fresh_instance", True)),
            prepared_storage_policy=storage_policy,
            failure=FailureInjection.from_dict(failure) if failure else None,
        )


@dataclass(frozen=True)
class BenchmarkPlan:
    providers: tuple[str, ...]
    scenarios: tuple[BenchmarkScenario, ...]
    limits: BenchmarkLimits
    exclusive: bool = True

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BenchmarkPlan":
        if value.get("schema") != PLAN_SCHEMA:
            raise ValueError(f"benchmark plan schema must be {PLAN_SCHEMA!r}")
        providers = value.get("providers") or []
        if not isinstance(providers, list) or not providers:
            raise ValueError("providers must be a non-empty list")
        normalized_providers = tuple(str(item).strip() for item in providers)
        if any(not item for item in normalized_providers):
            raise ValueError("providers cannot contain empty names")
        if len(set(normalized_providers)) != len(normalized_providers):
            raise ValueError("providers must be unique")
        raw_scenarios = value.get("scenarios") or []
        if not isinstance(raw_scenarios, list) or not raw_scenarios:
            raise ValueError("scenarios must be a non-empty list")
        scenarios = tuple(
            BenchmarkScenario.from_dict(item, index)
            for index, item in enumerate(raw_scenarios)
            if isinstance(item, dict)
        )
        if len(scenarios) != len(raw_scenarios):
            raise ValueError("every scenario must be an object")
        names = [item.name for item in scenarios]
        if len(set(names)) != len(names):
            raise ValueError("scenario names must be unique")
        alternating = [
            scenario.cache_state
            for scenario in scenarios
            if scenario.cache_state in {"cold", "hot"}
        ]
        if alternating:
            if alternating[0] != "cold":
                raise ValueError("cold/hot benchmark scenarios must begin with cold")
            if any(left == right for left, right in zip(alternating, alternating[1:])):
                raise ValueError("cold/hot benchmark scenarios must alternate")
        limits = value.get("limits")
        if not isinstance(limits, dict):
            raise ValueError("limits must be an object")
        return cls(
            providers=normalized_providers,
            scenarios=scenarios,
            limits=BenchmarkLimits.from_dict(limits),
            exclusive=bool(value.get("exclusive", True)),
        )

    @classmethod
    def load(cls, path: str | Path) -> "BenchmarkPlan":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("benchmark plan must contain a JSON object")
        return cls.from_dict(payload)

    def safe_summary(self) -> dict[str, Any]:
        return {
            "schema": PLAN_SCHEMA,
            "providers": list(self.providers),
            "exclusive": self.exclusive,
            "limits": {
                "max_total_cost_usd": self.limits.max_total_cost_usd,
                "max_scenario_cost_usd": self.limits.max_scenario_cost_usd,
                "max_campaign_seconds": self.limits.max_campaign_seconds,
                "fresh_worker_timeout_seconds": self.limits.fresh_worker_timeout_seconds,
            },
            "scenarios": [
                {
                    "name": item.name,
                    "cache_state": item.cache_state,
                    "endpoint": item.endpoint,
                    "request_digest": _canonical_digest(item.request),
                    "timeout_seconds": item.timeout_seconds,
                    "fresh_instance": item.fresh_instance,
                    "prepared_storage_policy": item.prepared_storage_policy,
                    "failure_kind": item.failure.kind if item.failure else None,
                    "failure_before_submit": (
                        item.failure.before_submit if item.failure else False
                    ),
                }
                for item in self.scenarios
            ],
        }


@dataclass(frozen=True)
class InstanceObservation:
    id: str
    provider: str
    hourly_rate: float
    status: str
    managed: bool
    name: str | None = None


class BenchmarkDriver(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...

    def inventory(
        self, providers: tuple[str, ...]
    ) -> dict[str, dict[str, InstanceObservation]]: ...

    def active_workers(self, providers: tuple[str, ...]) -> list[dict[str, Any]]: ...

    def prepare_scenario(self, scenario: BenchmarkScenario) -> dict[str, Any]: ...

    def restore_scenario(self, scenario: BenchmarkScenario) -> dict[str, Any]: ...

    def submit(self, scenario: BenchmarkScenario) -> str: ...

    def snapshot(self, job_id: str) -> dict[str, Any]: ...

    def events(self, job_id: str, after: int) -> list[dict[str, Any]]: ...

    def cancel(self, job_id: str) -> dict[str, Any]: ...

    def terminate(self, provider: str, instance_id: str) -> bool: ...

    def support_bundle(self, job_id: str) -> dict[str, Any] | None: ...

    def run_hook(
        self, injection: FailureInjection, context: dict[str, str]
    ) -> dict[str, Any]: ...


@dataclass
class _ResourceMeter:
    id: str
    provider: str
    hourly_rate: float
    first_seen: float
    last_seen: float
    source: str


def _managed_ids(
    inventory: dict[str, dict[str, InstanceObservation]],
) -> set[tuple[str, str]]:
    return {
        (provider, instance.id)
        for provider, instances in inventory.items()
        for instance in instances.values()
        if instance.managed
    }


def _distribution(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    ordered = sorted(values)

    def percentile(fraction: float) -> float:
        index = max(0, math.ceil(fraction * len(ordered)) - 1)
        return ordered[index]

    return {
        "count": len(ordered),
        "min": round(ordered[0], 6),
        "mean": round(statistics.fmean(ordered), 6),
        "p50": round(percentile(0.50), 6),
        "p95": round(percentile(0.95), 6),
        "max": round(ordered[-1], 6),
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)
    except ValueError:
        return None


def _phase_durations(events: list[dict[str, Any]]) -> dict[str, float]:
    first_seen: dict[str, datetime] = {}
    final_time: datetime | None = None
    for item in events:
        observed = _parse_timestamp(item.get("observed_at"))
        if observed is None:
            continue
        final_time = max(final_time, observed) if final_time else observed
        phase = item.get("phase")
        if phase and phase not in first_seen:
            first_seen[str(phase)] = observed
    ordered = sorted(first_seen.items(), key=lambda item: item[1])
    durations: dict[str, float] = {}
    for index, (phase, started) in enumerate(ordered):
        ended = ordered[index + 1][1] if index + 1 < len(ordered) else final_time
        durations[phase] = round(max(0.0, (ended - started).total_seconds()), 6)
    return durations


class BenchmarkRunner:
    def __init__(self, driver: BenchmarkDriver):
        self.driver = driver

    def run(self, plan: BenchmarkPlan) -> dict[str, Any]:
        campaign_started_at = _utc_now()
        campaign_started = self.driver.monotonic()
        baseline = self.driver.inventory(plan.providers)
        baseline_managed = _managed_ids(baseline)
        if plan.exclusive and baseline_managed:
            names = ", ".join(
                f"{provider}:{instance}"
                for provider, instance in sorted(baseline_managed)
            )
            raise RuntimeError(
                "Exclusive benchmark refused to start with active Cloud Offload "
                f"instances: {names}"
            )

        results: list[dict[str, Any]] = []
        estimated_total = 0.0
        campaign_abort: str | None = None
        for scenario in plan.scenarios:
            elapsed = self.driver.monotonic() - campaign_started
            if elapsed >= plan.limits.max_campaign_seconds:
                campaign_abort = "campaign_runtime_limit"
                break
            if estimated_total >= plan.limits.max_total_cost_usd:
                campaign_abort = "campaign_cost_limit"
                break
            if scenario.fresh_instance and not self._wait_for_worker_quiescence(
                plan, campaign_started
            ):
                campaign_abort = "fresh_worker_quiescence_timeout"
                break
            result = self._run_scenario(
                plan,
                scenario,
                baseline,
                campaign_started,
                estimated_total,
            )
            results.append(result)
            estimated_total += float(result["estimated_compute_cost_upper_usd"])
            if result.get("orphaned_resources"):
                campaign_abort = "orphan_cleanup_failed"
                break
            if result.get("limit_triggered") == "campaign_cost_limit":
                campaign_abort = "campaign_cost_limit"
                break

        final_cleanup = (
            self._cleanup_new_managed(
                plan.providers,
                baseline,
                plan.limits.cleanup_timeout_seconds,
            )
            if plan.exclusive
            else []
        )
        final_audit_error = None
        try:
            final_inventory = self.driver.inventory(plan.providers)
            orphaned = sorted(_managed_ids(final_inventory) - baseline_managed)
            if orphaned and plan.exclusive:
                final_cleanup.extend(
                    self._cleanup_new_managed(
                        plan.providers,
                        baseline,
                        plan.limits.cleanup_timeout_seconds,
                    )
                )
                final_inventory = self.driver.inventory(plan.providers)
                orphaned = sorted(_managed_ids(final_inventory) - baseline_managed)
        except Exception as exc:  # noqa: BLE001 - a missing audit is a failed audit
            final_audit_error = type(exc).__name__
            orphaned = []
        passed = (
            campaign_abort is None
            and not orphaned
            and final_audit_error is None
            and len(results) == len(plan.scenarios)
            and all(result["passed"] for result in results)
        )
        scorecard = {
            "schema": SCORECARD_SCHEMA,
            "started_at": campaign_started_at,
            "completed_at": _utc_now(),
            "plan": plan.safe_summary(),
            "passed": passed,
            "campaign_abort": campaign_abort,
            "estimated_compute_cost_upper_usd": round(estimated_total, 6),
            "limits": {
                "max_total_cost_usd": plan.limits.max_total_cost_usd,
                "max_scenario_cost_usd": plan.limits.max_scenario_cost_usd,
                "max_campaign_seconds": plan.limits.max_campaign_seconds,
                "fresh_worker_timeout_seconds": plan.limits.fresh_worker_timeout_seconds,
            },
            "results": results,
            "distributions": self._score_distributions(results),
            "final_cleanup": final_cleanup,
            "final_audit_error": final_audit_error,
            "orphaned_resources": [
                {"provider": provider, "instance_id": instance}
                for provider, instance in orphaned
            ],
        }
        return scorecard

    def _wait_for_worker_quiescence(
        self, plan: BenchmarkPlan, campaign_started: float
    ) -> bool:
        deadline = min(
            campaign_started + plan.limits.max_campaign_seconds,
            self.driver.monotonic() + plan.limits.fresh_worker_timeout_seconds,
        )
        while self.driver.active_workers(plan.providers):
            if self.driver.monotonic() >= deadline:
                return False
            self.driver.sleep(plan.limits.poll_seconds)
        return True

    def _run_scenario(
        self,
        plan: BenchmarkPlan,
        scenario: BenchmarkScenario,
        campaign_baseline: dict[str, dict[str, InstanceObservation]],
        campaign_started: float,
        cost_before: float,
    ) -> dict[str, Any]:
        started_at = _utc_now()
        started = self.driver.monotonic()
        scenario_baseline = self.driver.inventory(plan.providers)
        events: list[dict[str, Any]] = []
        cursor = 0
        resources: dict[tuple[str, str], _ResourceMeter] = {}
        last_provider = plan.providers[0]
        last_rate = 0.0
        job_id: str | None = None
        status: str | None = None
        failure_result: dict[str, Any] | None = None
        limit_triggered: str | None = None
        harness_error: str | None = None
        fatal_error: BaseException | None = None
        support_bundle: dict[str, Any] | None = None
        preparation: dict[str, Any] | None = None
        restoration: dict[str, Any] | None = None
        failure_preparation: dict[str, Any] | None = None
        failure_cleanup: dict[str, Any] | None = None

        try:
            preparation = self.driver.prepare_scenario(scenario)
            if scenario.failure and scenario.failure.before_submit:
                failure_preparation = self.driver.run_hook(
                    scenario.failure,
                    self._hook_context(
                        scenario,
                        stage="prepare",
                        job_id=None,
                        resources=resources,
                    ),
                )
                if failure_preparation.get("exit_code") != 0:
                    raise RuntimeError("Pre-submit failure hook did not succeed")
            job_id = self.driver.submit(scenario)
            while True:
                now = self.driver.monotonic()
                elapsed = now - started
                new_events = self.driver.events(job_id, cursor)
                if new_events:
                    events.extend(new_events)
                    cursor = max(int(item.get("sequence") or 0) for item in events)
                for item in new_events:
                    event_resources = item.get("resources") or {}
                    provider = str(event_resources.get("provider") or last_provider)
                    rate_value = event_resources.get("hourly_rate")
                    if rate_value is not None:
                        last_rate = max(0.0, float(rate_value))
                    last_provider = provider
                    instance_id = event_resources.get(
                        "worker_instance_id"
                    ) or event_resources.get("pod_id")
                    if instance_id:
                        key = (provider, str(instance_id))
                        meter = resources.get(key)
                        if meter is None:
                            resources[key] = _ResourceMeter(
                                id=str(instance_id),
                                provider=provider,
                                hourly_rate=last_rate,
                                first_seen=started,
                                last_seen=now,
                                source="journal",
                            )
                        else:
                            meter.last_seen = now
                            meter.hourly_rate = max(meter.hourly_rate, last_rate)

                if plan.exclusive:
                    current_inventory = self.driver.inventory(plan.providers)
                    scenario_new = _managed_ids(current_inventory) - _managed_ids(
                        scenario_baseline
                    )
                    for provider, instance_id in scenario_new:
                        observation = current_inventory[provider][instance_id]
                        key = (provider, instance_id)
                        if key not in resources:
                            resources[key] = _ResourceMeter(
                                id=instance_id,
                                provider=provider,
                                hourly_rate=max(last_rate, observation.hourly_rate),
                                first_seen=started,
                                last_seen=now,
                                source="provider_inventory",
                            )

                estimated = self._estimated_cost(resources, now)
                if not resources and last_rate:
                    estimated = last_rate * elapsed / 3600
                if estimated >= plan.limits.max_scenario_cost_usd:
                    limit_triggered = "scenario_cost_limit"
                    self.driver.cancel(job_id)
                    break
                if cost_before + estimated >= plan.limits.max_total_cost_usd:
                    limit_triggered = "campaign_cost_limit"
                    self.driver.cancel(job_id)
                    break
                if now - campaign_started >= plan.limits.max_campaign_seconds:
                    limit_triggered = "campaign_runtime_limit"
                    self.driver.cancel(job_id)
                    break
                if elapsed >= scenario.timeout_seconds:
                    limit_triggered = "scenario_timeout"
                    self.driver.cancel(job_id)
                    break

                snapshot = self.driver.snapshot(job_id)
                status = str(snapshot.get("status") or "")
                if scenario.failure and failure_result is None:
                    failure_result = self._maybe_inject(
                        scenario,
                        snapshot,
                        events,
                        resources,
                        elapsed,
                        job_id,
                        failure_preparation,
                    )
                if status in TERMINAL_STATUSES:
                    break
                self.driver.sleep(plan.limits.poll_seconds)
        except (KeyboardInterrupt, SystemExit) as exc:
            # Paid resource cleanup and config restoration still run before the
            # operator interrupt is allowed to escape.
            fatal_error = exc
        except Exception as exc:  # noqa: BLE001 - preserve cleanup on harness faults
            harness_error = f"{type(exc).__name__}: {exc}"
        finally:
            if job_id:
                try:
                    support_bundle = self.driver.support_bundle(job_id)
                except Exception as exc:  # noqa: BLE001
                    support_bundle = {
                        "schema": "cloud-offload.support-bundle-error.v1",
                        "error": f"{type(exc).__name__}: {exc}",
                    }

        if scenario.failure and scenario.failure.before_submit and failure_preparation:
            try:
                failure_cleanup = self.driver.run_hook(
                    scenario.failure,
                    self._hook_context(
                        scenario,
                        stage="cleanup",
                        job_id=job_id,
                        resources=resources,
                    ),
                )
            except Exception as exc:  # noqa: BLE001 - cleanup must remain visible
                failure_cleanup = {
                    "exit_code": None,
                    "error_type": type(exc).__name__,
                    "output_omitted": True,
                }
            if failure_result is None:
                failure_result = {
                    "kind": scenario.failure.kind,
                    "triggered": False,
                    "preparation_hook": failure_preparation,
                }
            failure_result["cleanup_hook"] = failure_cleanup

        cleanup_started = self.driver.monotonic()
        cleanup = self._cleanup_resources(
            plan.providers,
            campaign_baseline,
            resources,
            plan.limits.cleanup_timeout_seconds,
            include_untracked=plan.exclusive,
        )
        try:
            restoration = self.driver.restore_scenario(scenario)
        except Exception as exc:  # noqa: BLE001 - report without leaking config
            restoration = {
                "required": scenario.prepared_storage_policy is not None,
                "restored": False,
                "error_type": type(exc).__name__,
            }
        completed = self.driver.monotonic()
        cleanup_duration = max(0.0, completed - cleanup_started)
        estimated_cost = self._estimated_cost(resources, completed)
        orphaned_resources = [
            item for item in cleanup if not item.get("provider_absent")
        ]
        if job_id and status not in TERMINAL_STATUSES:
            try:
                status = str(self.driver.snapshot(job_id).get("status") or status or "")
            except Exception:  # noqa: BLE001
                pass
        failure_expected = scenario.failure is not None
        failure_triggered = failure_result is not None and failure_result.get(
            "triggered"
        )
        failure_succeeded = self._failure_injection_succeeded(failure_result)
        passed = (
            harness_error is None
            and limit_triggered is None
            and status in scenario.expected_statuses
            and not orphaned_resources
            and bool((preparation or {}).get("prepared"))
            and bool((restoration or {}).get("restored"))
            and (not failure_expected or (failure_triggered and failure_succeeded))
            and (not scenario.fresh_instance or bool(resources))
        )
        if fatal_error is not None:
            raise fatal_error
        return {
            "schema": SCENARIO_SCHEMA,
            "name": scenario.name,
            "cache_state": scenario.cache_state,
            "request_digest": _canonical_digest(scenario.request),
            "started_at": started_at,
            "completed_at": _utc_now(),
            "duration_seconds": round(max(0.0, completed - started), 6),
            "resource_closure_seconds": round(cleanup_duration, 6),
            "job_id": job_id,
            "status": status,
            "expected_statuses": list(scenario.expected_statuses),
            "passed": passed,
            "fresh_instance_required": scenario.fresh_instance,
            "fresh_instance_observed": bool(resources),
            "scenario_preparation": preparation,
            "scenario_restoration": restoration,
            "event_count": len(events),
            "event_cursor": cursor,
            "phase_durations_seconds": _phase_durations(events),
            "estimated_compute_cost_upper_usd": round(estimated_cost, 6),
            "resources": [
                {
                    "provider": meter.provider,
                    "instance_id": meter.id,
                    "hourly_rate": meter.hourly_rate,
                    "source": meter.source,
                }
                for meter in sorted(
                    resources.values(), key=lambda item: (item.provider, item.id)
                )
            ],
            "failure_injection": failure_result,
            "limit_triggered": limit_triggered,
            "harness_error": harness_error,
            "cleanup": cleanup,
            "orphaned_resources": orphaned_resources,
            "support_bundle": support_bundle,
        }

    @staticmethod
    def _failure_injection_succeeded(result: dict[str, Any] | None) -> bool:
        if not result or not result.get("triggered"):
            return False
        if result.get("kind") == "cancellation":
            return bool((result.get("response") or {}).get("accepted"))
        if result.get("kind") == "provider":
            receipts = result.get("receipts") or []
            return bool(receipts) and all(
                item.get("termination_requested") for item in receipts
            )
        hook_succeeded = (result.get("hook") or {}).get("exit_code") == 0
        preparation_succeeded = (result.get("preparation_hook") or {}).get(
            "exit_code", 0
        ) == 0
        cleanup_succeeded = (result.get("cleanup_hook") or {}).get("exit_code", 0) == 0
        return hook_succeeded and preparation_succeeded and cleanup_succeeded

    def _maybe_inject(
        self,
        scenario: BenchmarkScenario,
        snapshot: dict[str, Any],
        events: list[dict[str, Any]],
        resources: dict[tuple[str, str], _ResourceMeter],
        elapsed: float,
        job_id: str,
        failure_preparation: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        injection = scenario.failure
        if injection is None or elapsed < injection.after_seconds:
            return None
        phases = {str(item.get("phase")) for item in events if item.get("phase")}
        phases.add(str(snapshot.get("lifecycle_phase") or ""))
        event_types = {str(item.get("type")) for item in events if item.get("type")}
        if injection.trigger_phase and injection.trigger_phase not in phases:
            return None
        if injection.trigger_event and injection.trigger_event not in event_types:
            return None
        if injection.kind == "cancellation":
            response = self.driver.cancel(job_id)
            return {"kind": injection.kind, "triggered": True, "response": response}
        if injection.kind == "provider":
            if not resources:
                return None
            receipts = [
                {
                    "provider": meter.provider,
                    "instance_id": meter.id,
                    "termination_requested": self.driver.terminate(
                        meter.provider, meter.id
                    ),
                }
                for meter in resources.values()
            ]
            return {"kind": injection.kind, "triggered": True, "receipts": receipts}
        result = {
            "kind": injection.kind,
            "triggered": True,
            "hook": self.driver.run_hook(
                injection,
                self._hook_context(
                    scenario,
                    stage="observe",
                    job_id=job_id,
                    resources=resources,
                ),
            ),
        }
        if failure_preparation is not None:
            result["preparation_hook"] = failure_preparation
        return result

    @staticmethod
    def _hook_context(
        scenario: BenchmarkScenario,
        *,
        stage: str,
        job_id: str | None,
        resources: dict[tuple[str, str], _ResourceMeter],
    ) -> dict[str, str]:
        injection = scenario.failure
        assets = (scenario.request.get("partition") or {}).get("assets") or []
        digests = sorted(
            {
                str(item.get("sha256") or "").lower()
                for item in assets
                if isinstance(item, dict)
                and len(str(item.get("sha256") or "")) == 64
                and all(
                    character in "0123456789abcdefABCDEF"
                    for character in str(item.get("sha256") or "")
                )
            }
        )
        return {
            "CLOUD_OFFLOAD_BENCHMARK_JOB_ID": job_id or "",
            "CLOUD_OFFLOAD_BENCHMARK_SCENARIO": scenario.name,
            "CLOUD_OFFLOAD_BENCHMARK_FAILURE_KIND": (
                injection.kind if injection else ""
            ),
            "CLOUD_OFFLOAD_BENCHMARK_HOOK_STAGE": stage,
            "CLOUD_OFFLOAD_BENCHMARK_REQUEST_DIGEST": _canonical_digest(
                scenario.request
            ),
            "CLOUD_OFFLOAD_BENCHMARK_ASSET_DIGESTS": ",".join(digests),
            "CLOUD_OFFLOAD_BENCHMARK_INSTANCE_IDS": ",".join(
                meter.id for meter in resources.values()
            ),
        }

    @staticmethod
    def _estimated_cost(
        resources: dict[tuple[str, str], _ResourceMeter], now: float
    ) -> float:
        return sum(
            max(0.0, now - meter.first_seen) * meter.hourly_rate / 3600
            for meter in resources.values()
        )

    def _cleanup_resources(
        self,
        providers: tuple[str, ...],
        baseline: dict[str, dict[str, InstanceObservation]],
        resources: dict[tuple[str, str], _ResourceMeter],
        timeout_seconds: float,
        *,
        include_untracked: bool = True,
    ) -> list[dict[str, Any]]:
        inventory_error = None
        try:
            current = self.driver.inventory(providers)
        except Exception as exc:  # noqa: BLE001
            current = {provider: {} for provider in providers}
            inventory_error = type(exc).__name__
        baseline_ids = _managed_ids(baseline)
        candidates = set(resources)
        if include_untracked:
            candidates |= _managed_ids(current) - baseline_ids
        receipts = {
            key: {
                "provider": key[0],
                "instance_id": key[1],
                "termination_attempts": 0,
                "provider_absent": False,
            }
            for key in candidates
            if key not in baseline_ids
        }
        deadline = self.driver.monotonic() + timeout_seconds
        while receipts and self.driver.monotonic() <= deadline:
            try:
                inventory = self.driver.inventory(providers)
                inventory_error = None
            except Exception as exc:  # noqa: BLE001
                inventory = {provider: {} for provider in providers}
                inventory_error = type(exc).__name__
            pending = []
            for key, receipt in receipts.items():
                if inventory_error is None and key[1] not in inventory.get(key[0], {}):
                    receipt["provider_absent"] = True
                    continue
                pending.append(key)
                receipt["termination_attempts"] += 1
                try:
                    receipt["termination_requested"] = self.driver.terminate(*key)
                except Exception as exc:  # noqa: BLE001
                    receipt["termination_requested"] = False
                    receipt["termination_error"] = type(exc).__name__
            if not pending:
                break
            self.driver.sleep(min(2.0, timeout_seconds))
        try:
            inventory = self.driver.inventory(providers)
            inventory_error = None
        except Exception as exc:  # noqa: BLE001
            inventory = {provider: {} for provider in providers}
            inventory_error = type(exc).__name__
        for key, receipt in receipts.items():
            receipt["provider_absent"] = inventory_error is None and key[
                1
            ] not in inventory.get(key[0], {})
            if inventory_error:
                receipt["inventory_error"] = inventory_error
        return [receipts[key] for key in sorted(receipts)]

    def _cleanup_new_managed(
        self,
        providers: tuple[str, ...],
        baseline: dict[str, dict[str, InstanceObservation]],
        timeout_seconds: float,
    ) -> list[dict[str, Any]]:
        return self._cleanup_resources(
            providers, baseline, {}, timeout_seconds, include_untracked=True
        )

    @staticmethod
    def _score_distributions(results: list[dict[str, Any]]) -> dict[str, Any]:
        distributions: dict[str, Any] = {}
        for cache_state in ("cold", "hot", "failure"):
            selected = [item for item in results if item["cache_state"] == cache_state]
            distributions[cache_state] = {
                "duration_seconds": _distribution(
                    [float(item["duration_seconds"]) for item in selected]
                ),
                "estimated_compute_cost_upper_usd": _distribution(
                    [
                        float(item["estimated_compute_cost_upper_usd"])
                        for item in selected
                    ]
                ),
            }
        phases = sorted(
            {
                phase
                for item in results
                for phase in item.get("phase_durations_seconds", {})
            }
        )
        distributions["phases_seconds"] = {
            phase: _distribution(
                [
                    float(item["phase_durations_seconds"][phase])
                    for item in results
                    if phase in item.get("phase_durations_seconds", {})
                ]
            )
            for phase in phases
        }
        distributions["resource_closure_seconds"] = _distribution(
            [float(item["resource_closure_seconds"]) for item in results]
        )
        return distributions


class CoordinatorBenchmarkDriver:
    """Production driver backed by the coordinator API and provider connectors."""

    def __init__(
        self,
        base_url: str,
        token: str | None,
        config: CloudConfig,
        providers: tuple[str, ...],
        *,
        allow_hooks: bool = False,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        self.connectors = {
            provider: create_connector(provider, config) for provider in providers
        }
        self.allow_hooks = allow_hooks
        self._prepared_storage_restore: dict[str, dict[str, dict[str, Any]]] = {}

    def _request(
        self,
        method: str,
        path: str,
        *,
        retry_safe: bool = False,
        timeout: float = 30,
        **kwargs,
    ) -> requests.Response:
        deadline = time.monotonic() + 30
        while True:
            try:
                response = self.session.request(
                    method,
                    f"{self.base_url}{path}",
                    timeout=timeout,
                    **kwargs,
                )
                if not retry_safe or response.status_code not in {502, 503, 504}:
                    return response
            except requests.RequestException:
                if not retry_safe or time.monotonic() >= deadline:
                    raise
            if time.monotonic() >= deadline:
                return response
            time.sleep(1)

    @staticmethod
    def monotonic() -> float:
        return time.monotonic()

    @staticmethod
    def sleep(seconds: float) -> None:
        time.sleep(max(0.0, seconds))

    def inventory(
        self, providers: tuple[str, ...]
    ) -> dict[str, dict[str, InstanceObservation]]:
        result: dict[str, dict[str, InstanceObservation]] = {}
        for provider in providers:
            deadline = time.monotonic() + 30
            while True:
                try:
                    instances = self.connectors[provider].list_instances()
                    break
                except Exception:  # noqa: BLE001 - bounded provider retry
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(1)
            result[provider] = {
                instance.id: InstanceObservation(
                    id=instance.id,
                    provider=provider,
                    hourly_rate=max(0.0, float(instance.hourly_rate or 0)),
                    status=instance.status,
                    managed=str(instance.metadata.get("name") or "").startswith(
                        MANAGED_INSTANCE_PREFIX
                    ),
                    name=instance.metadata.get("name"),
                )
                for instance in instances
            }
        return result

    def active_workers(self, providers: tuple[str, ...]) -> list[dict[str, Any]]:
        response = self._request(
            "GET", "/api/active-workers", retry_safe=True, timeout=30
        )
        response.raise_for_status()
        allowed = set(providers)
        return [
            item
            for item in response.json().get("workers") or []
            if str(item.get("provider") or "") in allowed
        ]

    def _prepared_storage_config(self) -> dict[str, Any]:
        response = self._request("GET", "/api/config", retry_safe=True, timeout=30)
        response.raise_for_status()
        prepared = response.json().get("prepared_storage")
        if not isinstance(prepared, dict):
            raise RuntimeError("Coordinator returned no prepared-storage config")
        # The coordinator contract is finite JSON. Round-tripping makes the
        # private restoration copy independent from the response object.
        return json.loads(json.dumps(prepared, allow_nan=False))

    def _set_prepared_storage_config(self, prepared: dict[str, Any]) -> None:
        response = self._request(
            "POST",
            "/api/config",
            retry_safe=True,
            timeout=30,
            json={"prepared_storage": prepared},
        )
        response.raise_for_status()

    def prepare_scenario(self, scenario: BenchmarkScenario) -> dict[str, Any]:
        requested = scenario.prepared_storage_policy
        if requested is None:
            return {"required": False, "prepared": True}
        if scenario.name in self._prepared_storage_restore:
            raise RuntimeError("Scenario prepared-storage policy is already active")

        previous = self._prepared_storage_config()
        target = json.loads(json.dumps(previous, allow_nan=False))
        if requested == "off":
            target["policy"] = "off"
            target["enabled"] = False
        else:
            if not previous.get("confirmed"):
                raise RuntimeError(
                    "Hot benchmark requires previously confirmed prepared storage"
                )
            if not previous.get("existing_volume_id"):
                raise RuntimeError(
                    "Hot benchmark requires an already-bound prepared volume"
                )
            target["policy"] = requested
            target["enabled"] = True

        # Store the complete original before the idempotent POST. If the response
        # is lost after the coordinator applies it, restoration still knows both
        # legitimate states and can safely recover.
        self._prepared_storage_restore[scenario.name] = {
            "previous": previous,
            "target": target,
        }
        changed = target != previous
        if changed:
            self._set_prepared_storage_config(target)
        observed = self._prepared_storage_config()
        if observed != target:
            raise RuntimeError("Coordinator did not apply benchmark storage policy")
        return {
            "required": True,
            "prepared": True,
            "requested_policy": requested,
            "previous_policy": str(previous.get("policy") or ""),
            "changed": changed,
        }

    def restore_scenario(self, scenario: BenchmarkScenario) -> dict[str, Any]:
        saved = self._prepared_storage_restore.get(scenario.name)
        if saved is None:
            return {"required": False, "restored": True, "changed": False}
        previous = saved["previous"]
        target = saved["target"]
        current = self._prepared_storage_config()
        if current == previous:
            changed = False
        elif current == target:
            self._set_prepared_storage_config(previous)
            changed = True
        else:
            raise RuntimeError(
                "Prepared-storage config changed during benchmark; refusing to overwrite it"
            )
        observed = self._prepared_storage_config()
        if observed != previous:
            raise RuntimeError("Coordinator did not restore prepared-storage config")
        self._prepared_storage_restore.pop(scenario.name, None)
        return {
            "required": True,
            "restored": True,
            "restored_policy": str(previous.get("policy") or ""),
            "changed": changed,
        }

    def submit(self, scenario: BenchmarkScenario) -> str:
        # Submission is not transport-idempotent yet, so it is deliberately not
        # retried after an ambiguous connection failure.
        response = self._request(
            "POST",
            scenario.endpoint,
            json=scenario.request,
            timeout=120,
        )
        response.raise_for_status()
        payload = response.json()
        job_id = payload.get("job_id")
        if not job_id:
            raise RuntimeError("Coordinator submission returned no job_id")
        return str(job_id)

    def snapshot(self, job_id: str) -> dict[str, Any]:
        response = self._request(
            "GET",
            f"/api/jobs/{job_id}/snapshot",
            retry_safe=True,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def events(self, job_id: str, after: int) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/api/jobs/{job_id}/events",
            retry_safe=True,
            params={"after": max(0, int(after)), "limit": 1000},
            timeout=30,
        )
        response.raise_for_status()
        return list(response.json().get("events") or [])

    def cancel(self, job_id: str) -> dict[str, Any]:
        response = self._request(
            "POST",
            f"/api/jobs/{job_id}/cancel",
            retry_safe=True,
            timeout=30,
        )
        if response.status_code not in {200, 409}:
            response.raise_for_status()
        return {
            "status_code": response.status_code,
            "accepted": response.status_code == 200,
        }

    def terminate(self, provider: str, instance_id: str) -> bool:
        return bool(self.connectors[provider].terminate(instance_id))

    def support_bundle(self, job_id: str) -> dict[str, Any] | None:
        response = self._request(
            "GET",
            f"/api/jobs/{job_id}/support-bundle",
            retry_safe=True,
            timeout=60,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def run_hook(
        self, injection: FailureInjection, context: dict[str, str]
    ) -> dict[str, Any]:
        if not self.allow_hooks:
            raise RuntimeError(
                f"{injection.kind} failure injection requires --allow-hooks"
            )
        started = time.monotonic()
        environment = os.environ.copy()
        environment.update(context)
        completed = subprocess.run(
            list(injection.hook_argv),
            env=environment,
            capture_output=True,
            check=False,
            timeout=120,
        )
        return {
            "exit_code": completed.returncode,
            "duration_seconds": round(time.monotonic() - started, 6),
            # Hook output is deliberately excluded: failure tools often print
            # provider URLs, object keys, or process environments.
            "output_omitted": True,
        }


def write_scorecard(path: str | Path, scorecard: dict[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, destination)
    return destination
