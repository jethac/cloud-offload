"""
Worker - runs on cloud instances, processes jobs from the queue.

Claims ComfyUI jobs, runs the colocated headless ComfyUI executor, and
publishes results/artifacts back to the coordinator. The worker never loads a
3D model: generation rides inside the submitted subgraph.
"""

import logging
import signal
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path

from cloud_offload.config import CloudConfig
from cloud_offload.queue import Job, JobQueue, JobStatus
from cloud_offload.storage import Storage, create_storage
from cloud_offload.profiles import WORKFLOW_CAPABILITIES, load_worker_manifest

logger = logging.getLogger(__name__)

DEFAULT_PARTITION_ROOT = "/opt/cloud-offload/partitions"


class Worker:
    """
    Cloud worker that processes jobs from the queue.

    Lifecycle:
    1. Poll queue for jobs
    2. Claim batch of jobs
    3. Run the colocated ComfyUI executor
    4. Publish results/artifacts
    5. Mark complete
    6. If idle too long, self-terminate
    """

    def __init__(
        self,
        config: CloudConfig,
        queue: JobQueue | None = None,
        storage: Storage | None = None,
        worker_id: str | None = None,
    ):
        self.config = config
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        if queue is not None:
            self.queue = queue
        elif config.coordinator_url:
            from cloud_offload.coordinator import CoordinatorQueue

            if not config.worker_token:
                raise ValueError(
                    "CLOUD_OFFLOAD_WORKER_TOKEN is required with CLOUD_OFFLOAD_COORDINATOR_URL"
                )
            self.queue = CoordinatorQueue(
                config.coordinator_url,
                config.worker_token,
                config.provider,
                self.worker_id,
            )
        else:
            self.queue = JobQueue(config.queue_db_path)
        self.storage = storage or create_storage(config)

        self.running = False
        self.last_job_time = datetime.utcnow()
        self.runtime_profile = config.worker_profile
        self.declared_capabilities = list(dict.fromkeys(config.worker_models))
        self._apply_image_manifest()
        self.capabilities = self._validated_capabilities()
        self.gpu_name, self.gpu_vram_gb = self._detect_gpu()

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _apply_image_manifest(self) -> None:
        manifest_path = Path(self.config.worker_manifest_path)
        if not manifest_path.is_file():
            logger.warning("Worker capability manifest not found: %s", manifest_path)
            return
        manifest = load_worker_manifest(manifest_path)
        if self.runtime_profile and manifest["profile"] != self.runtime_profile:
            raise RuntimeError(
                f"Worker image profile {manifest['profile']} does not match "
                f"requested profile {self.runtime_profile}"
            )
        self.runtime_profile = manifest["profile"]
        manifest_models = set(manifest["models"])
        self.declared_capabilities = [
            model for model in self.declared_capabilities if model in manifest_models
        ]
        if (
            self.runtime_profile.startswith("comfyui")
            and "comfyui-workflow" in self.declared_capabilities
            and "comfyui-partition-v1" in manifest_models
        ):
            self.declared_capabilities.append("comfyui-partition-v1")

    def _validated_capabilities(self) -> list[str]:
        """Only claim workflow capabilities this ComfyUI runner actually offers."""
        return list(
            dict.fromkeys(
                model
                for model in self.declared_capabilities
                if model in WORKFLOW_CAPABILITIES
            )
        )

    def _handle_signal(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    @staticmethod
    def _detect_gpu() -> tuple[str, float]:
        """Return the worker's real GPU identity for claim-time constraints."""
        try:
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=name,memory.total",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=True,
            )
            first = result.stdout.strip().splitlines()[0]
            name, memory_mib = first.rsplit(",", 1)
            return name.strip(), float(memory_mib.strip()) / 1024.0
        except (FileNotFoundError, IndexError, OSError, ValueError, subprocess.SubprocessError):
            logger.warning("Unable to detect an NVIDIA GPU; constrained jobs will not be claimed")
            return "", 0.0

    def run(
        self,
        poll_interval: int | None = None,
        max_jobs: int | None = None,
        once: bool = False,
    ):
        """
        Main worker loop.

        Args:
            poll_interval: Seconds between queue polls
            max_jobs: Maximum jobs to process before exiting (None = unlimited)
            once: Process one batch and exit
        """
        poll_interval = poll_interval or self.config.poll_interval_seconds
        jobs_processed = 0

        logger.info(
            "Worker %s starting (profile=%s, capabilities=%s)",
            self.worker_id,
            self.runtime_profile or "unconfigured",
            ",".join(self.capabilities) or "none",
        )
        if not self.capabilities:
            raise RuntimeError(
                "Worker has no validated capabilities; configure "
                "CLOUD_OFFLOAD_WORKER_PROFILE and CLOUD_OFFLOAD_WORKER_MODELS in a "
                "compatible image"
            )
        self.running = True
        self.last_job_time = datetime.utcnow()

        while self.running:
            try:
                # Check idle timeout
                if self._should_shutdown():
                    logger.info("Idle timeout reached, shutting down")
                    break

                # Claim jobs
                jobs = self.queue.claim_jobs(
                    self.worker_id,
                    limit=5,
                    token=self.config.worker_token or None,
                    provider=self.config.provider,
                    models=self.capabilities,
                    runtime_profile=self.runtime_profile,
                    gpu_vram_gb=self.gpu_vram_gb,
                    gpu_name=self.gpu_name,
                )

                if jobs:
                    self.last_job_time = datetime.utcnow()

                    for job in jobs:
                        if not self.running:
                            break

                        try:
                            self._process_job(job)
                            jobs_processed += 1

                            if max_jobs and jobs_processed >= max_jobs:
                                logger.info(f"Processed {max_jobs} jobs, exiting")
                                self.running = False
                                break

                        except Exception as e:
                            logger.error(f"Job {job.id} failed: {e}")
                            self.queue.fail_job(job.id, error=str(e))
                else:
                    logger.debug("No jobs available")

            except Exception as e:
                logger.error(f"Worker error: {e}")

            if once:
                break

            if self.running:
                time.sleep(poll_interval)

        logger.info(f"Worker {self.worker_id} shutting down (processed {jobs_processed} jobs)")

    def _should_shutdown(self) -> bool:
        """Check if worker should shut down due to idle timeout."""
        keep_warm = self.config.keep_warm
        idle_shutdown_seconds = self.config.idle_shutdown_seconds
        policy_reader = getattr(self.queue, "worker_policy", None)
        if callable(policy_reader):
            try:
                policy = policy_reader()
                keep_warm = bool(policy.get("keep_warm", keep_warm))
                idle_shutdown_seconds = int(
                    policy.get("idle_shutdown_seconds", idle_shutdown_seconds)
                )
            except Exception as exc:
                logger.warning("Could not refresh worker lifetime policy: %s", exc)
        if keep_warm:
            return False
        idle_time = datetime.utcnow() - self.last_job_time
        return idle_time.total_seconds() > idle_shutdown_seconds

    def _process_job(self, job: Job):
        """Process a single job."""
        logger.info(f"Processing job {job.id} (model={job.model})")

        current = self.queue.get(job.id)
        if current and current.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}:
            logger.info("Skipping terminal job %s (%s)", job.id, current.status.value)
            return

        if job.model not in WORKFLOW_CAPABILITIES:
            raise RuntimeError(f"Unsupported job model: {job.model}")

        # Mark as running
        self.queue.update_status(job.id, JobStatus.RUNNING)

        result = self._run_comfyui_workflow(job)
        self.queue.update_status(job.id, JobStatus.COMPLETED, result=result)
        logger.info(f"Job {job.id} completed")

    def _run_comfyui_workflow(self, job: Job) -> dict:
        """Execute an arbitrary API-format workflow in the colocated ComfyUI."""
        from cloud_offload.comfyui import ComfyUIWorkflowExecutor

        request = job.request
        if request.get("kind") == "comfyui-partition":
            return self._run_comfyui_partition(job, ComfyUIWorkflowExecutor())
        return ComfyUIWorkflowExecutor().execute(
            request.get("workflow") or {},
            inputs=request.get("inputs") or {},
            timeout_seconds=int(request.get("timeout_seconds", 3600)),
        )

    @staticmethod
    def _partition_artifact_key(artifact_id: str) -> str:
        if len(artifact_id) != 64 or any(c not in "0123456789abcdef" for c in artifact_id):
            raise ValueError(f"Invalid partition artifact ID: {artifact_id}")
        return f"partition-artifacts/{artifact_id[:2]}/{artifact_id}.part"

    def _download_partition_artifact(self, artifact_id: str, destination: Path) -> Path:
        downloader = getattr(self.queue, "download_artifact", None)
        if callable(downloader):
            return downloader(artifact_id, destination)
        return self.storage.download(self._partition_artifact_key(artifact_id), destination)

    def _upload_partition_artifact(self, source: Path) -> dict:
        uploader = getattr(self.queue, "upload_artifact", None)
        if callable(uploader):
            return uploader(source)
        import hashlib

        digest = hashlib.sha256()
        with source.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        artifact_id = digest.hexdigest()
        self.storage.upload(source, self._partition_artifact_key(artifact_id))
        return {"artifact_id": artifact_id, "sha256": artifact_id, "size": source.stat().st_size}

    def _run_comfyui_partition(self, job: Job, executor) -> dict:
        """Stage typed boundaries, execute the extracted prompt, and publish outputs."""
        import copy
        import os

        request = job.request
        partition = request.get("partition") or {}
        if partition.get("schema") != "comfy.partition.job.v1":
            raise ValueError("Unsupported ComfyUI partition schema")
        workflow = copy.deepcopy(partition.get("workflow") or {})
        root = Path(
            os.environ.get("COMFY_PARTITION_ROOT", DEFAULT_PARTITION_ROOT)
        ).resolve()
        job_root = (root / job.id).resolve()
        job_root.relative_to(root)
        input_root = job_root / "inputs"
        output_root = job_root / "outputs"
        input_root.mkdir(parents=True, exist_ok=True)
        output_root.mkdir(parents=True, exist_ok=True)

        event_writer = getattr(self.queue, "append_event", None)

        def publish(event: dict) -> None:
            if callable(event_writer):
                event_writer(job.id, event)

        publish(
            {
                "type": "partition_staging",
                "partition_id": partition.get("partition_id"),
            }
        )

        input_artifacts = request.get("input_artifacts") or {}
        expected_inputs = {item["key"] for item in partition.get("inputs") or []}
        if set(input_artifacts) != expected_inputs:
            raise ValueError("Partition input artifact keys do not match the compiled boundary")
        input_paths = {}
        for boundary_key, artifact_id in input_artifacts.items():
            path = input_root / f"{boundary_key}.part"
            self._download_partition_artifact(str(artifact_id), path)
            input_paths[boundary_key] = path

        expected_outputs = {item["key"] for item in partition.get("outputs") or []}
        seen_inputs = set()
        seen_outputs = set()
        output_paths = {}
        for node in workflow.values():
            if node.get("class_type") == "CloudPartitionInput":
                key = str(node["inputs"]["boundary_key"])
                if key not in input_paths:
                    raise ValueError(f"Compiled partition input has no artifact: {key}")
                node["inputs"]["artifact_path"] = str(input_paths[key])
                seen_inputs.add(key)
            elif node.get("class_type") == "CloudPartitionOutput":
                key = str(node["inputs"]["boundary_key"])
                if key not in expected_outputs:
                    raise ValueError(f"Unexpected compiled partition output: {key}")
                path = output_root / f"{key}.part"
                node["inputs"]["output_path"] = str(path)
                output_paths[key] = path
                seen_outputs.add(key)
        if seen_inputs != expected_inputs or seen_outputs != expected_outputs:
            raise ValueError("Compiled partition bridge nodes do not match its boundary manifest")

        total_nodes = max(1, len(workflow))
        finished_nodes: set[str] = set()
        last_progress_sent = 0.0
        last_cancel_check = 0.0
        cancel_requested = False

        def should_cancel() -> bool:
            nonlocal last_cancel_check, cancel_requested
            if cancel_requested:
                return True
            now = time.monotonic()
            if now - last_cancel_check < 1.0:
                return False
            last_cancel_check = now
            current = self.queue.get(job.id)
            cancel_requested = bool(
                current
                and current.status in {JobStatus.FAILED, JobStatus.DEAD_LETTER}
                and str(current.error or "").lower().startswith("cancel")
            )
            return cancel_requested

        def relay(event: dict) -> None:
            nonlocal last_progress_sent
            event_type = str(event.get("type") or "")
            node_id = event.get("node_id")
            if event_type == "executed" and node_id is not None:
                finished_nodes.add(str(node_id))
            elif event_type == "execution_cached":
                for cached in (event.get("data") or {}).get("nodes") or []:
                    finished_nodes.add(str(cached))
            now = time.monotonic()
            if event_type == "progress":
                data = event.get("data") or {}
                maximum = max(1, int(data.get("max") or 1))
                fraction = max(0.0, min(1.0, float(data.get("value") or 0) / maximum))
                overall = (len(finished_nodes) + fraction) / total_nodes
                if now - last_progress_sent < 0.25 and fraction < 1.0:
                    return
                last_progress_sent = now
            else:
                overall = len(finished_nodes) / total_nodes
            progress = max(10, min(95, 10 + round(overall * 85)))
            setter = getattr(self.queue, "set_progress", None)
            if callable(setter):
                setter(job.id, progress)
            publish(
                {
                    **event,
                    "partition_id": partition.get("partition_id"),
                    "overall_progress": progress,
                }
            )

        result = executor.execute(
            workflow,
            timeout_seconds=int(request.get("timeout_seconds", 3600)),
            event_callback=relay,
            cancel_check=should_cancel,
        )
        publish(
            {
                "type": "partition_uploading",
                "partition_id": partition.get("partition_id"),
            }
        )
        output_artifacts = {}
        for boundary_key, path in output_paths.items():
            if not path.is_file():
                raise RuntimeError(f"ComfyUI produced no partition output: {boundary_key}")
            output_artifacts[boundary_key] = self._upload_partition_artifact(path)["artifact_id"]
        return {
            "schema": "comfy.partition.result.v1",
            "partition_id": partition.get("partition_id"),
            "prompt_id": result.get("prompt_id"),
            "output_artifacts": output_artifacts,
            "outputs": result.get("outputs") or {},
        }


def run_worker(
    config_path: str | None = None,
    poll_interval: int = 10,
    max_jobs: int | None = None,
):
    """Entry point for running a worker."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if config_path:
        config = CloudConfig.from_file(config_path)
    else:
        config = CloudConfig.load()

    worker = Worker(config)
    worker.run(poll_interval=poll_interval, max_jobs=max_jobs)
