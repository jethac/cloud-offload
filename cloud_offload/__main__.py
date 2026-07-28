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


def setup_logging(name: str = "cloud-offload", level=logging.INFO):
    """Configure logging to both console and a dated log file."""
    log_dir = CONFIG_DIR / "logs"
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


def _build_parser() -> argparse.ArgumentParser:
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
        "--allow-lan", action="store_true", help="Allow binding to a non-localhost address"
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

    worker_parser = subparsers.add_parser("worker", help="Run as a cloud worker")
    worker_parser.add_argument("--config", help="Path to config file")
    worker_parser.add_argument("--poll", type=int, default=10, help="Poll interval in seconds")
    worker_parser.add_argument("--max-jobs", type=int, help="Max jobs before exit")

    dispatch_parser = subparsers.add_parser("dispatch", help="Run the job dispatcher")
    dispatch_parser.add_argument("--config", help="Path to config file")
    dispatch_parser.add_argument("--once", action="store_true", help="Run once and exit")

    storage_parser = subparsers.add_parser(
        "storage-plan", help="Size the container disk a worker profile needs"
    )
    storage_parser.add_argument("profile", help="Configured worker profile name")
    storage_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Resolve unknown weight sizes from Hugging Face and cache them",
    )

    queue_parser = subparsers.add_parser("queue", help="Manage the local job queue")
    queue_sub = queue_parser.add_subparsers(dest="queue_command")
    queue_sub.add_parser("status", help="Show queue status")
    queue_sub.add_parser("list", help="List active jobs")
    queue_cancel = queue_sub.add_parser("cancel", help="Cancel a job")
    queue_cancel.add_argument("job_id", help="Job ID to cancel")
    queue_clean = queue_sub.add_parser("clean", help="Clean old terminal jobs")
    queue_clean.add_argument("--days", type=int, default=7, help="Delete jobs older than N days")
    return parser, queue_parser


def main():
    parser, queue_parser = _build_parser()
    args = parser.parse_args()

    if args.command == "serve":
        log_file = setup_logging("coordinator")
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
            )
        except ServiceConfigError as exc:
            logger.error(str(exc))
            raise SystemExit(2) from exc

    elif args.command == "worker":
        setup_logging("worker")
        load_plugins()
        from cloud_offload.config import CloudConfig
        from cloud_offload.worker import Worker

        config = CloudConfig.from_file(args.config) if args.config else CloudConfig.load()
        worker = Worker(config)
        worker.run(poll_interval=args.poll, max_jobs=args.max_jobs)

    elif args.command == "dispatch":
        setup_logging("dispatcher")
        load_plugins()
        from cloud_offload.config import CloudConfig
        from cloud_offload.dispatcher import Dispatcher

        config = CloudConfig.from_file(args.config) if args.config else CloudConfig.load()
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
        print(f"Storage plan for worker profile {args.profile!r}, with no partition assets:")
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
                    print(f"{job.id:<36} {job.model:<25} {job.status.value:<12} {created}")

        elif args.queue_command == "cancel":
            job = queue.get(args.job_id)
            if not job:
                print(f"Job {args.job_id} not found")
                sys.exit(1)
            queue.update_status(args.job_id, JobStatus.FAILED, error="Cancelled by user")
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
