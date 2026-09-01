"""
Cloud Offload CLI entry point.

Usage:
    cloud-offload serve       # Start the coordinator HTTP service
    cloud-offload dispatch    # Run the queue-driven provisioning dispatcher
    cloud-offload worker      # Run as a cloud worker (inside a runner image)
    cloud-offload queue ...   # Inspect / manage the local job queue
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from cloud_offload.config import CONFIG_DIR


def setup_logging(
    name: str = "cloud-offload", level=logging.INFO, *, home: str | Path | None = None
):
    """Configure logging to both console and a dated log file."""
    log_dir = Path(home).resolve() / "logs" if home else CONFIG_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    return log_file


def load_plugins(logger: logging.Logger | None = None) -> dict:
    """Discover third-party connectors before anything reads the registry.

    Never fatal: a broken plugin (or a broken plugin *system*) must not stop the
    coordinator from starting, so failures are logged and execution continues.
    """
    logger = logger or logging.getLogger("cloud-offload")
    try:
        from cloud_offload.plugins import load_connector_plugins

        summary = load_connector_plugins()
    except Exception as exc:  # noqa: BLE001 - discovery is best-effort
        logger.warning(f"Connector plugin discovery failed: {exc}")
        return {"loaded": [], "failed": [{"source": "discovery", "error": str(exc)}]}

    # Individual failures are already logged as warnings by the loader.
    loaded = summary.get("loaded", [])
    failed = summary.get("failed", [])
    logger.info(f"connector plugins: loaded={len(loaded)} failed={len(failed)}")
    return summary


def _build_parser() -> tuple[
    argparse.ArgumentParser,
    argparse.ArgumentParser,
    argparse.ArgumentParser,
    argparse.ArgumentParser,
]:
    parser = argparse.ArgumentParser(
        prog="cloud-offload", description="Provider-neutral cloud offload coordinator"
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    serve_parser = subparsers.add_parser("serve", help="Start the coordinator service")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument(
        "--port", type=int, help="Port to bind. Omit or pass 0 to auto-select."
    )
    serve_parser.add_argument(
        "--allow-lan",
        action="store_true",
        help="Allow binding to a non-localhost address",
    )
    serve_parser.add_argument(
        "--require-auth",
        action="store_true",
        help="Force a bearer token (already the default; kept for explicitness)",
    )
    serve_parser.add_argument(
        "--allow-anonymous-loopback",
        action="store_true",
        help="Serve loopback without a bearer token (single-user desktop only)",
    )
    serve_parser.add_argument("--tls-cert", help="TLS certificate for HTTPS")
    serve_parser.add_argument("--tls-key", help="TLS private key for HTTPS")
    serve_parser.add_argument("--config", help="Path to the coordinator config")
    serve_parser.add_argument(
        "--home", help="Explicit isolated coordinator home for M7 startup"
    )
    serve_parser.add_argument(
        "--release-plan", help="Validated release plan required for isolated M7 startup"
    )

    worker_parser = subparsers.add_parser("worker", help="Run as a cloud worker")
    worker_parser.add_argument("--config", help="Path to config file")
    worker_parser.add_argument(
        "--poll", type=int, default=10, help="Poll interval in seconds"
    )
    worker_parser.add_argument("--max-jobs", type=int, help="Max jobs before exit")

    boot_parser = subparsers.add_parser(
        "runner-boot",
        help="Register this runner and stage its node packs, before ComfyUI starts",
    )
    boot_parser.add_argument("--config", help="Path to config file")

    ready_parser = subparsers.add_parser(
        "runner-ready",
        help="Wait for the colocated ComfyUI, or report home why it never answered",
    )
    ready_parser.add_argument("--config", help="Path to config file")
    ready_parser.add_argument(
        "--comfyui-pid", type=int, required=True, help="PID of the ComfyUI process"
    )
    ready_parser.add_argument(
        "--log-file", help="Runner log whose tail is reported on failure"
    )
    ready_parser.add_argument(
        "--timeout",
        type=float,
        help="Seconds to wait on a living but unready ComfyUI "
        "(default: CLOUD_OFFLOAD_COMFYUI_READY_TIMEOUT, else 1200)",
    )

    dispatch_parser = subparsers.add_parser("dispatch", help="Run the job dispatcher")
    dispatch_parser.add_argument("--config", help="Path to config file")
    dispatch_parser.add_argument(
        "--once", action="store_true", help="Run once and exit"
    )

    storage_parser = subparsers.add_parser(
        "storage-plan", help="Size the container disk a worker profile needs"
    )
    storage_parser.add_argument("profile", help="Configured worker profile name")
    storage_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Resolve unknown weight sizes from Hugging Face and cache them",
    )

    benchmark_parser = subparsers.add_parser(
        "benchmark", help="Run or validate a spend-capped production benchmark"
    )
    benchmark_sub = benchmark_parser.add_subparsers(dest="benchmark_command")
    benchmark_validate = benchmark_sub.add_parser(
        "validate", help="Validate a plan without submitting work"
    )
    benchmark_validate.add_argument("--plan", required=True, help="Benchmark plan JSON")
    benchmark_run = benchmark_sub.add_parser(
        "run", help="Execute a benchmark campaign and write its scorecard"
    )
    benchmark_run.add_argument("--plan", required=True, help="Benchmark plan JSON")
    benchmark_run.add_argument("--output", required=True, help="Scorecard JSON path")
    benchmark_run.add_argument("--config", help="Path to Cloud Offload config")
    benchmark_run.add_argument(
        "--confirm-spend",
        action="store_true",
        help="Acknowledge the plan's provider spend and runtime ceilings",
    )
    benchmark_run.add_argument(
        "--allow-hooks",
        action="store_true",
        help="Allow explicit storage/corruption/restart hook commands from the plan",
    )

    release_parser = subparsers.add_parser(
        "release", help="Validate or run the continuous M7 production gate"
    )
    release_sub = release_parser.add_subparsers(dest="release_command")
    release_validate = release_sub.add_parser(
        "validate", help="Validate a release plan without provider work"
    )
    release_validate.add_argument("--plan", required=True, help="Release plan JSON")
    release_status = release_sub.add_parser(
        "status", help="Show the safe release ledger"
    )
    release_status.add_argument("--plan", required=True, help="Release plan JSON")
    release_status.add_argument("--ledger", required=True, help="Release ledger JSON")
    release_run = release_sub.add_parser(
        "run", help="Run release matrices and update the atomic ledger"
    )
    release_run.add_argument("--plan", required=True, help="Release plan JSON")
    release_run.add_argument("--ledger", required=True, help="Release ledger JSON")
    release_run.add_argument(
        "--output-dir", required=True, help="Private scorecard and test-log directory"
    )
    release_run.add_argument("--config", help="Path to Cloud Offload config")
    release_run.add_argument(
        "--home", help="Explicit isolated coordinator home; requires a bootstrap receipt"
    )
    release_run.add_argument(
        "--max-matrices", type=int, help="Stop after this many new matrices"
    )
    release_run.add_argument(
        "--confirm-spend",
        action="store_true",
        help="Acknowledge the release plan's provider spend and runtime ceilings",
    )
    release_run.add_argument(
        "--allow-hooks",
        action="store_true",
        help="Allow the reviewed storage, corruption, and restart canaries",
    )
    release_bootstrap = release_sub.add_parser(
        "bootstrap-artifacts",
        help="Verify and import private benchmark input artifacts before startup",
    )
    release_bootstrap.add_argument("--plan", required=True, help="Release plan JSON")
    release_bootstrap.add_argument(
        "--source-root", required=True, help="Read-only content-addressed source root"
    )
    release_bootstrap.add_argument("--config", help="Isolated Cloud Offload config")
    release_bootstrap.add_argument(
        "--home", required=True, help="Explicit isolated coordinator home"
    )

    benchmark_hook = subparsers.add_parser(
        "benchmark-hook",
        help="Run a reviewed fault canary inside an authorized benchmark",
    )
    benchmark_hook.add_argument("kind", choices=("storage", "corruption", "restart"))

    queue_parser = subparsers.add_parser("queue", help="Manage the local job queue")
    queue_sub = queue_parser.add_subparsers(dest="queue_command")
    queue_sub.add_parser("status", help="Show queue status")
    queue_sub.add_parser("list", help="List active jobs")
    queue_cancel = queue_sub.add_parser("cancel", help="Cancel a job")
    queue_cancel.add_argument("job_id", help="Job ID to cancel")
    queue_clean = queue_sub.add_parser("clean", help="Clean old terminal jobs")
    queue_clean.add_argument(
        "--days", type=int, default=7, help="Delete jobs older than N days"
    )
    return parser, queue_parser, benchmark_parser, release_parser


def main():
    parser, queue_parser, benchmark_parser, release_parser = _build_parser()
    args = parser.parse_args()

    if args.command == "serve":
        isolated_config = None
        isolated_plan = None
        if any((args.config, args.home, args.release_plan)):
            if not all((args.config, args.home, args.release_plan)):
                print(
                    "Isolated M7 serve requires --config, --home, and --release-plan.",
                    file=sys.stderr,
                )
                raise SystemExit(2)
            from cloud_offload.artifact_bootstrap import (
                ArtifactBootstrapError,
                config_artifact_store,
                config_digest,
                declared_input_artifacts,
                file_digest,
                verify_bootstrap_receipt,
            )
            from cloud_offload.config import CloudConfig
            from cloud_offload.release_gate import ReleasePlan

            try:
                isolated_config = CloudConfig.from_file(args.config, home=args.home)
                isolated_plan = ReleasePlan.load(args.release_plan)
                declarations = declared_input_artifacts(
                    (case.benchmark_plan_digest, case.benchmark_plan)
                    for case in isolated_plan.cases
                )
                destination = config_artifact_store(isolated_config, args.home)
                verify_bootstrap_receipt(
                    destination,
                    declarations,
                    release_plan_digest=file_digest(args.release_plan),
                    config_digest=config_digest(isolated_config, destination),
                )
            except (ArtifactBootstrapError, OSError, ValueError, json.JSONDecodeError) as exc:
                print(f"Isolated M7 startup refused: {exc}", file=sys.stderr)
                raise SystemExit(2) from exc
        log_file = setup_logging("coordinator", home=args.home)
        logger = logging.getLogger("cloud-offload")
        port_label = args.port if args.port not in {None, 0} else "auto"
        logger.info(f"Starting Cloud Offload coordinator on {args.host}:{port_label}")
        logger.info(f"Log file: {log_file}")
        load_plugins(logger)
        from cloud_offload.service_config import ServiceConfigError
        from cloud_offload.server import serve

        try:
            if args.allow_anonymous_loopback:
                os.environ["CLOUD_OFFLOAD_ALLOW_ANONYMOUS_LOOPBACK"] = "true"
            serve(
                args.host,
                args.port,
                allow_lan=args.allow_lan,
                require_auth=args.require_auth,
                tls_cert=args.tls_cert,
                tls_key=args.tls_key,
                config=isolated_config,
            )
        except ServiceConfigError as exc:
            logger.error(str(exc))
            raise SystemExit(2) from exc

    elif args.command == "worker":
        setup_logging("worker")
        load_plugins()
        from cloud_offload.config import CloudConfig
        from cloud_offload.worker import Worker

        config = (
            CloudConfig.from_file(args.config) if args.config else CloudConfig.load()
        )
        worker = Worker(config)
        worker.run(poll_interval=args.poll, max_jobs=args.max_jobs)

    elif args.command in {"runner-boot", "runner-ready"}:
        setup_logging("runner")
        load_plugins()
        from cloud_offload.config import CloudConfig
        from cloud_offload.runner import DEFAULT_COMFYUI_LOG, run_boot, run_ready

        config = (
            CloudConfig.from_file(args.config) if args.config else CloudConfig.load()
        )
        if args.command == "runner-boot":
            raise SystemExit(run_boot(config))
        raise SystemExit(
            run_ready(
                args.comfyui_pid,
                log_path=args.log_file or DEFAULT_COMFYUI_LOG,
                timeout_seconds=args.timeout,
                config=config,
            )
        )

    elif args.command == "dispatch":
        setup_logging("dispatcher")
        load_plugins()
        from cloud_offload.config import CloudConfig
        from cloud_offload.dispatcher import Dispatcher

        config = (
            CloudConfig.from_file(args.config) if args.config else CloudConfig.load()
        )
        dispatcher = Dispatcher(config)
        try:
            dispatcher.run(once=args.once)
        except KeyboardInterrupt:
            dispatcher.shutdown()

    elif args.command == "storage-plan":
        load_plugins()
        from cloud_offload.config import CloudConfig
        from cloud_offload.profiles import configured_worker_profiles
        from cloud_offload.storage_plan import GIB, plan_disk_gb, plan_storage
        from cloud_offload.weight_sizes import cached_weight_sizes, refresh_weight_sizes

        config = CloudConfig.load()
        profiles = configured_worker_profiles(config)
        profile = profiles.get(args.profile)
        if not profile:
            known = ", ".join(sorted(profiles)) or "none"
            print(f"No worker profile named {args.profile!r} (configured: {known})")
            sys.exit(1)

        # Resolving is explicit: the submission path only ever reads the cache,
        # so a coordinator never blocks a job on the Hugging Face API. This is
        # where an operator chooses to go and ask.
        if args.refresh:
            refresh_weight_sizes(config, profile)
        weight_bytes = cached_weight_sizes(config, profile)
        image_bytes = int(float(profile.get("image_size_gb") or 0) * GIB) or None
        plan = plan_storage(
            [], profile, image_bytes=image_bytes, weight_bytes=weight_bytes
        )
        print(
            f"Storage plan for worker profile {args.profile!r}, with no partition assets:"
        )
        for component in plan["components"]:
            print(
                f"  {component['name']:<9} {component['bytes'] / GIB:>9.1f} GiB  "
                f"{component['detail']}"
            )
        print(f"  {'total':<9} {plan['total'] / GIB:>9.1f} GiB")
        print(f"Container disk to request: {plan_disk_gb(plan)} GB")
        if plan["unknown"]:
            print("Unknown, charged a conservative default:")
            for item in plan["unknown"]:
                print(f"  - {item}")

    elif args.command == "benchmark-hook":
        from cloud_offload.benchmark_faults import run_fault

        try:
            receipt = run_fault(args.kind)
        except Exception as exc:  # noqa: BLE001 - hook returns a safe failure code
            print(
                f"Benchmark {args.kind} canary failed: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            raise SystemExit(1) from exc
        print(json.dumps(receipt, sort_keys=True))

    elif args.command == "benchmark":
        from cloud_offload.benchmark import (
            BenchmarkPlan,
            BenchmarkRunner,
            CoordinatorBenchmarkDriver,
            write_scorecard,
        )

        if not args.benchmark_command:
            benchmark_parser.print_help()
            raise SystemExit(2)

        try:
            plan = BenchmarkPlan.load(args.plan)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            print(f"Invalid benchmark plan: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc

        if args.benchmark_command == "validate":
            print(json.dumps(plan.safe_summary(), indent=2, sort_keys=True))
        elif args.benchmark_command == "run":
            if not args.confirm_spend:
                print(
                    "Benchmark run not started. Review the validated plan, then pass "
                    "--confirm-spend to acknowledge ceilings of "
                    f"${plan.limits.max_total_cost_usd:.2f} and "
                    f"{plan.limits.max_campaign_seconds:.0f}s.",
                    file=sys.stderr,
                )
                raise SystemExit(2)
            hook_scenarios = [
                scenario.name
                for scenario in plan.scenarios
                if scenario.failure and scenario.failure.hook_argv
            ]
            if hook_scenarios and not args.allow_hooks:
                print(
                    "Benchmark run not started. These scenarios contain external "
                    "failure hooks and require --allow-hooks: "
                    + ", ".join(hook_scenarios),
                    file=sys.stderr,
                )
                raise SystemExit(2)
            setup_logging("benchmark")
            load_plugins()
            from cloud_offload.config import CloudConfig
            from cloud_offload.service_config import discover_service_info

            config = (
                CloudConfig.from_file(args.config, home=args.home)
                if args.config
                else CloudConfig.load()
            )
            try:
                if args.home:
                    if not args.config:
                        raise ValueError("isolated M7 release run requires --config")
                    from cloud_offload.artifact_bootstrap import (
                        config_artifact_store,
                        config_digest,
                        declared_input_artifacts,
                        file_digest,
                        verify_bootstrap_receipt,
                    )

                    destination = config_artifact_store(config, args.home)
                    declarations = declared_input_artifacts(
                        (case.benchmark_plan_digest, case.benchmark_plan)
                        for case in plan.cases
                    )
                    verify_bootstrap_receipt(
                        destination,
                        declarations,
                        release_plan_digest=file_digest(args.plan),
                        config_digest=config_digest(config, destination),
                    )
                service = discover_service_info(require_healthy=True)
                driver = CoordinatorBenchmarkDriver(
                    service["url"],
                    service.get("token"),
                    config,
                    plan.providers,
                    allow_hooks=args.allow_hooks,
                )
                scorecard = BenchmarkRunner(driver).run(plan)
                output = write_scorecard(args.output, scorecard)
            except Exception as exc:  # noqa: BLE001 - CLI must report a safe failure
                print(
                    f"Benchmark failed before scorecard completion: {exc}",
                    file=sys.stderr,
                )
                raise SystemExit(1) from exc
            print(
                f"Benchmark {'passed' if scorecard['passed'] else 'failed'}: {output} "
                f"(estimated upper compute cost "
                f"${scorecard['estimated_compute_cost_upper_usd']:.4f})"
            )
            raise SystemExit(0 if scorecard["passed"] else 1)
        else:
            benchmark_parser.print_help()

    elif args.command == "release":
        from cloud_offload.release_gate import (
            ReleaseExecutor,
            ReleasePlan,
            load_ledger,
            update_ledger,
        )

        if not args.release_command:
            release_parser.print_help()
            raise SystemExit(2)
        try:
            plan = ReleasePlan.load(args.plan)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            print(f"Invalid release plan: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc
        if args.release_command == "validate":
            print(json.dumps(plan.safe_summary(), indent=2, sort_keys=True))
        elif args.release_command == "status":
            try:
                ledger = load_ledger(Path(args.ledger), plan)
                update_ledger(ledger, plan)
            except (OSError, json.JSONDecodeError, ValueError) as exc:
                print(f"Invalid release ledger: {exc}", file=sys.stderr)
                raise SystemExit(2) from exc
            print(json.dumps(ledger, indent=2, sort_keys=True))
        elif args.release_command == "bootstrap-artifacts":
            from cloud_offload.artifact_bootstrap import (
                ArtifactBootstrapError,
                config_artifact_store,
                config_digest,
                declared_input_artifacts,
                file_digest,
                import_declared_artifacts,
            )
            from cloud_offload.config import CloudConfig
            try:
                config = CloudConfig.from_file(args.config, home=args.home) if args.config else CloudConfig.load(resolve_secrets=False)
                destination = config_artifact_store(config, args.home)
                declarations = declared_input_artifacts(
                    (case.benchmark_plan_digest, case.benchmark_plan)
                    for case in plan.cases
                )
                records = import_declared_artifacts(
                    args.source_root,
                    destination,
                    declarations,
                    release_plan_digest=file_digest(args.plan),
                    config_digest=config_digest(config, destination),
                )
            except (OSError, ValueError, ArtifactBootstrapError) as exc:
                print(f"Artifact bootstrap stopped safely: {exc}", file=sys.stderr)
                raise SystemExit(2) from exc
            print(
                json.dumps(
                    {
                        "artifact_count": len(records),
                        "artifacts": [
                            {
                                "digest": item.digest,
                                "size": item.size,
                                "roles": list(item.roles),
                                "already_present": item.already_present,
                            }
                            for item in records
                        ],
                    },
                    sort_keys=True,
                )
            )
            raise SystemExit(0)
        elif args.release_command == "run":
            if not args.confirm_spend:
                print(
                    "Release run not started. Review the safe plan, then pass "
                    "--confirm-spend to acknowledge ceilings of "
                    f"${plan.limits.max_total_cost_usd:.2f} and "
                    f"{plan.limits.max_total_seconds:.0f}s.",
                    file=sys.stderr,
                )
                raise SystemExit(2)
            if args.max_matrices is not None and args.max_matrices <= 0:
                print("--max-matrices must be positive", file=sys.stderr)
                raise SystemExit(2)
            setup_logging("release", home=args.home)
            load_plugins()
            from cloud_offload.config import CloudConfig
            from cloud_offload.service_config import discover_service_info

            config = (
                CloudConfig.from_file(args.config)
                if args.config
                else CloudConfig.load()
            )
            try:
                service = discover_service_info(require_healthy=True)
                ledger = ReleaseExecutor(
                    plan,
                    args.ledger,
                    args.output_dir,
                    config,
                    service,
                    allow_hooks=args.allow_hooks,
                ).run(max_matrices=args.max_matrices)
            except Exception as exc:  # noqa: BLE001 - keep paid cleanup visible
                print(
                    "Release matrix stopped safely: "
                    f"{type(exc).__name__}. See the private output directory.",
                    file=sys.stderr,
                )
                raise SystemExit(1) from exc
            print(
                f"Release {'passed' if ledger['passed'] else 'in progress'}: "
                f"{ledger['consecutive_passes']}/"
                f"{plan.required_consecutive_matrices} consecutive matrices, "
                f"${ledger['total_estimated_compute_cost_upper_usd']:.4f} "
                "estimated upper compute cost."
            )
            successful_stops = {
                "release_already_passed",
                "release_passed",
                "requested_matrix_limit",
            }
            raise SystemExit(
                0 if ledger.get("last_stop_reason") in successful_stops else 1
            )
        else:
            release_parser.print_help()

    elif args.command == "queue":
        load_plugins()
        from cloud_offload.config import CloudConfig
        from cloud_offload.queue import JobQueue, JobStatus

        config = CloudConfig.load()
        queue = JobQueue(config.queue_db_path)

        if args.queue_command == "status":
            print("Queue status:")
            print(f"  Pending:   {queue.count_by_status(JobStatus.PENDING)}")
            print(f"  Queued:    {queue.count_by_status(JobStatus.QUEUED)}")
            print(
                f"  Running:   {queue.count_by_status(JobStatus.RUNNING, JobStatus.DISPATCHED)}"
            )
            print(f"  Completed: {queue.count_by_status(JobStatus.COMPLETED)}")
            print(f"  Failed:    {queue.count_by_status(JobStatus.FAILED)}")
            print(f"  Dead:      {queue.count_by_status(JobStatus.DEAD_LETTER)}")

        elif args.queue_command == "list":
            jobs = queue.list_by_status(
                JobStatus.PENDING,
                JobStatus.QUEUED,
                JobStatus.DISPATCHED,
                JobStatus.RUNNING,
                JobStatus.DEAD_LETTER,
            )
            if not jobs:
                print("No active jobs")
            else:
                print(f"{'ID':<36} {'Model':<25} {'Status':<12} {'Created'}")
                print("-" * 90)
                for job in jobs:
                    created = job.created_at[:19] if job.created_at else "N/A"
                    print(
                        f"{job.id:<36} {job.model:<25} {job.status.value:<12} {created}"
                    )

        elif args.queue_command == "cancel":
            job = queue.get(args.job_id)
            if not job:
                print(f"Job {args.job_id} not found")
                sys.exit(1)
            queue.update_status(
                args.job_id, JobStatus.FAILED, error="Cancelled by user"
            )
            print(f"Cancelled job {args.job_id}")

        elif args.queue_command == "clean":
            count = queue.cleanup_old(args.days)
            print(f"Cleaned {count} old jobs")

        else:
            queue_parser.print_help()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
