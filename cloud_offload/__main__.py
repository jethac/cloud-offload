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

    worker_parser = subparsers.add_parser("worker", help="Run as a cloud worker")
    worker_parser.add_argument("--config", help="Path to config file")
    worker_parser.add_argument("--poll", type=int, default=10, help="Poll interval in seconds")
    worker_parser.add_argument("--max-jobs", type=int, help="Max jobs before exit")

    dispatch_parser = subparsers.add_parser("dispatch", help="Run the job dispatcher")
    dispatch_parser.add_argument("--config", help="Path to config file")
    dispatch_parser.add_argument("--once", action="store_true", help="Run once and exit")

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
        from cloud_offload.service_config import ServiceConfigError
        from cloud_offload.server import serve

        try:
            serve(args.host, args.port, allow_lan=args.allow_lan)
        except ServiceConfigError as exc:
            logger.error(str(exc))
            raise SystemExit(2) from exc

    elif args.command == "worker":
        setup_logging("worker")
        from cloud_offload.config import CloudConfig
        from cloud_offload.worker import Worker

        config = CloudConfig.from_file(args.config) if args.config else CloudConfig.load()
        worker = Worker(config)
        worker.run(poll_interval=args.poll, max_jobs=args.max_jobs)

    elif args.command == "dispatch":
        setup_logging("dispatcher")
        from cloud_offload.config import CloudConfig
        from cloud_offload.dispatcher import Dispatcher

        config = CloudConfig.from_file(args.config) if args.config else CloudConfig.load()
        dispatcher = Dispatcher(config)
        try:
            dispatcher.run(once=args.once)
        except KeyboardInterrupt:
            dispatcher.shutdown()

    elif args.command == "queue":
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
