"""
Worker - runs on cloud instances, processes jobs from the queue.

Claims ComfyUI jobs, runs the colocated headless ComfyUI executor, and
publishes results/artifacts back to the coordinator. The worker never loads a
3D model: generation rides inside the submitted subgraph.
"""

import hashlib
import logging
import shutil
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
# Where a model file whose bytes contradict the job's manifest is set aside.
# Quarantine rather than overwrite: it preserves the evidence, and it makes
# "same filename, different weights" a loud event instead of a silent one.
QUARANTINE_DIRNAME = ".cloud-offload-quarantine"

# The Comfy Registry, which resolves a pack id and version to a download URL.
DEFAULT_REGISTRY_URL = "https://api.comfy.org"
REGISTRY_TIMEOUT_SECONDS = 60
GIT_TIMEOUT_SECONDS = 600
PIP_TIMEOUT_SECONDS = 1800
# Enough to see what pip actually did without turning one failed build into an
# event nobody can page through.
MAX_CAPTURED_OUTPUT = 4000


def sha256_file(path: Path) -> str:
    """Digest a file in bounded memory. Never opens it as anything but bytes."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        # Pinned weights and node packs the launching profile wants on disk.
        # Staged lazily at the first claimed job so the progress is visible in
        # that job's events.
        self.weights = self._load_weights_env()
        self._weights_staged = False
        self.custom_nodes = self._load_custom_nodes_env()
        self._custom_nodes_staged = False

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

    @staticmethod
    def _load_json_list_env(name: str) -> list[dict]:
        """Read one of the launching profile's JSON list environment variables."""
        import json
        import os

        raw = os.environ.get(name, "").strip()
        if not raw:
            return []
        try:
            entries = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{name} is not valid JSON: {exc}") from exc
        if not isinstance(entries, list):
            raise RuntimeError(f"{name} must be a JSON list")
        return entries

    @staticmethod
    def _load_weights_env() -> list[dict]:
        """Weight downloads requested by the launching profile, if any."""
        return Worker._load_json_list_env("CLOUD_OFFLOAD_WEIGHTS")

    @staticmethod
    def _load_custom_nodes_env() -> list[dict]:
        """Custom node packs requested by the launching profile, if any."""
        return Worker._load_json_list_env("CLOUD_OFFLOAD_CUSTOM_NODES")

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

        # The first claimed job pays the node pack install and the weight
        # download; later jobs skip both. Packs come first: they are what makes
        # the graph's node types exist at all, and they are far the smaller
        # download, so a profile that is wrong about them fails fast.
        self._stage_custom_nodes(job)
        self._stage_profile_weights(job)

        # Mark as running
        self.queue.update_status(job.id, JobStatus.RUNNING)

        result = self._run_comfyui_workflow(job)
        self.queue.update_status(job.id, JobStatus.COMPLETED, result=result)
        logger.info(f"Job {job.id} completed")

    def _stage_custom_nodes(self, job: Job) -> None:
        """Install the profile's declared node packs, once, at the first job.

        The coordinator has already refused any partition whose required packs
        this profile does not declare, so by the time a job is claimed the list
        here is the answer to "what does the graph need". What is left is putting
        the code on disk, pinned: a registry release by version, or a git
        checkout at an exact commit.

        Progress rides the job's event stream as ``node_pack_staging`` events in
        the same 3..9 band weight staging uses, after the dispatcher's
        ``runner_starting`` (2) and under the 10 that marks the job running.
        Sharing one band keeps both phases of "preparing the runner" out of the
        range that means "executing". A failed install raises, which fails the
        job through the normal path in ``run``.
        """
        if self._custom_nodes_staged or not self.custom_nodes:
            self._custom_nodes_staged = True
            return

        from cloud_offload.comfyui import comfyui_custom_nodes_dir
        from cloud_offload.profiles import profile_pack_identifier

        root = comfyui_custom_nodes_dir().resolve()
        root.mkdir(parents=True, exist_ok=True)
        event_writer = getattr(self.queue, "append_event", None)
        progress_setter = getattr(self.queue, "set_progress", None)
        total_packs = len(self.custom_nodes)
        downloaded = 0

        def publish(pack_id: str | None, source: str | None) -> None:
            progress = 3 + round(6 * downloaded / max(1, total_packs))
            if callable(progress_setter):
                progress_setter(job.id, progress)
            if callable(event_writer):
                event_writer(
                    job.id,
                    {
                        "type": "node_pack_staging",
                        "pack_id": pack_id,
                        "source": source,
                        "downloaded_packs": downloaded,
                        "total_packs": total_packs,
                        "overall_progress": progress,
                    },
                )

        for entry in self.custom_nodes:
            pack_id = profile_pack_identifier(entry)
            source = "registry" if entry.get("registry_id") else "git"
            if not pack_id:
                raise RuntimeError(f"Custom node pack entry names no pack: {entry!r}")
            # The pack directory is named for the pin, which is also the name
            # ComfyUI will report its nodes under and the name the coordinator
            # matched the partition's requirement against.
            target = (root / pack_id).resolve()
            try:
                target.relative_to(root)
            except ValueError:
                raise RuntimeError(
                    f"Custom node pack escapes the custom_nodes directory: {pack_id!r}"
                )
            publish(pack_id, source)
            if target.exists():
                logger.info(
                    "Custom node pack already present, skipping %s (%s)", pack_id, target
                )
                downloaded += 1
                continue
            if source == "registry":
                self._install_registry_pack(entry, target)
            else:
                self._install_git_pack(entry, target)
            if entry.get("install_requirements", True):
                self._install_pack_requirements(job, pack_id, target)
            downloaded += 1

        publish(None, None)
        self._custom_nodes_staged = True
        logger.info("Staged %d custom node pack(s) into %s", total_packs, root)

    def _install_registry_pack(self, entry: dict, target: Path) -> None:
        """Install one Comfy Registry release, pinned by version.

        The registry's version list is the only place a release's artifact URL
        is published, so the version is resolved to a ``downloadUrl`` and the
        archive is unpacked through the traversal guard below.
        """
        import os
        import tempfile

        import requests

        registry_id = str(entry.get("registry_id") or "")
        version = str(entry.get("version") or "")
        base = os.environ.get("CLOUD_OFFLOAD_REGISTRY_URL", DEFAULT_REGISTRY_URL).rstrip("/")
        response = requests.get(
            f"{base}/nodes/{registry_id}/versions", timeout=REGISTRY_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        payload = response.json()
        versions = payload.get("versions") if isinstance(payload, dict) else payload
        download_url = ""
        for item in versions or ():
            if isinstance(item, dict) and str(item.get("version") or "") == version:
                download_url = str(item.get("downloadUrl") or "")
                break
        if not download_url:
            raise RuntimeError(
                f"Custom node pack {registry_id} has no registry version {version}"
            )

        handle = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        archive = Path(handle.name)
        handle.close()
        try:
            with requests.get(
                download_url, stream=True, timeout=REGISTRY_TIMEOUT_SECONDS
            ) as download:
                download.raise_for_status()
                with archive.open("wb") as sink:
                    for chunk in download.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            sink.write(chunk)
            self._extract_node_pack(archive, target, f"{registry_id}@{version}")
        finally:
            archive.unlink(missing_ok=True)

    @staticmethod
    def _extract_node_pack(archive: Path, target: Path, label: str) -> None:
        """Unpack a node pack archive, refusing anything that escapes ``target``.

        Every member is checked before a single one is written: an absolute path,
        a ``..`` component or a symlink entry aborts the whole install, naming the
        member. This is not a hypothetical class of bug — the pack this feature
        was designed around shipped a path-traversal fix — and the blast radius
        here is a runner's filesystem, so the archive is validated in full rather
        than sanitized member by member.
        """
        import stat
        import zipfile

        from pathlib import PureWindowsPath

        with zipfile.ZipFile(archive) as bundle:
            for info in bundle.infolist():
                name = info.filename
                # PureWindowsPath parses both separator styles and catches drive
                # letters, so one check covers archives from either platform.
                pure = PureWindowsPath(name)
                if pure.is_absolute() or pure.drive or name.startswith(("/", "\\")):
                    raise RuntimeError(
                        f"Custom node pack {label} archive member has an absolute "
                        f"path and was refused: {name}"
                    )
                if ".." in pure.parts:
                    raise RuntimeError(
                        f"Custom node pack {label} archive member traverses upward "
                        f"and was refused: {name}"
                    )
                if stat.S_ISLNK(info.external_attr >> 16):
                    raise RuntimeError(
                        f"Custom node pack {label} archive member is a symlink and "
                        f"was refused: {name}"
                    )
            target.mkdir(parents=True, exist_ok=True)
            bundle.extractall(target)

    def _install_git_pack(self, entry: dict, target: Path) -> None:
        """Clone one pack and check out the pinned commit, verifying HEAD.

        A blobless clone rather than ``--depth 1``: a depth-1 clone contains only
        the tip of the default branch, which need not include the pinned commit,
        while ``--filter=blob:none`` fetches the history cheaply and can reach any
        commit in it.

        HEAD is re-read afterwards because ``checkout`` succeeding is not proof of
        landing on the pin — an ambiguous ref or a rewritten remote would both
        leave the runner quietly executing code nobody pinned.
        """
        url = str(entry.get("git") or "")
        commit = str(entry.get("commit") or "").lower()
        self._run_git(
            ["clone", "--filter=blob:none", "--no-checkout", url, str(target)],
            f"cloning {url}",
        )
        self._run_git(
            ["-C", str(target), "checkout", "--detach", commit],
            f"checking out {commit[:12]} of {url}",
        )
        head = self._run_git(
            ["-C", str(target), "rev-parse", "HEAD"], f"reading HEAD of {url}"
        ).strip().lower()
        if head != commit:
            raise RuntimeError(
                f"Custom node pack {url} checked out {head} but the worker profile "
                f"pins {commit}"
            )

    @staticmethod
    def _run_git(arguments: list[str], description: str) -> str:
        """Run one git command, failing loudly with its stderr."""
        result = subprocess.run(
            ["git", *arguments],
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Custom node pack staging failed while {description}: "
                f"{(result.stderr or result.stdout or '').strip()[:MAX_CAPTURED_OUTPUT]}"
            )
        return result.stdout or ""

    def _install_pack_requirements(self, job: Job, pack_id: str, target: Path) -> None:
        """Install a pack's requirements.txt, with pip's output in the events.

        A pack whose dependencies are missing imports at ComfyUI startup and
        vanishes from the node registry, which surfaces later as "unknown node
        type" on a runner that is already rented. The output goes into the job's
        event stream because that is the only place an operator can read it: the
        runner is gone by the time anyone asks what happened.
        """
        import sys

        requirements = target / "requirements.txt"
        if not requirements.is_file():
            return
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", str(requirements)],
            capture_output=True,
            text=True,
            timeout=PIP_TIMEOUT_SECONDS,
        )
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        event_writer = getattr(self.queue, "append_event", None)
        if callable(event_writer):
            event_writer(
                job.id,
                {
                    "type": "node_pack_requirements",
                    "pack_id": pack_id,
                    "returncode": result.returncode,
                    "output": output[-MAX_CAPTURED_OUTPUT:],
                },
            )
        if result.returncode != 0:
            raise RuntimeError(
                f"Custom node pack {pack_id} requirements install failed: "
                f"{output[-MAX_CAPTURED_OUTPUT:]}"
            )

    def _stage_profile_weights(self, job: Job) -> None:
        """Stage the profile's pinned weights and the job's declared assets.

        Two lists with different lifetimes meet here. The profile's ``weights``
        are what the operator pinned to the runner image and are staged once,
        at the first claimed job. A job's ``assets`` are what its graph actually
        declares, digest by digest, so they are checked on every job — cheaply,
        because a file whose digest already matches is left alone.

        Progress rides the job's event stream as ``weights_staging`` events in
        the 3..9 band, after the dispatcher's ``runner_starting`` (2) and under
        the 10 that marks the job running. A failed download raises, which
        fails the job through the normal path in ``run``.
        """
        assets = [
            asset
            for asset in ((job.request or {}).get("assets") or [])
            if isinstance(asset, dict)
        ]
        pending_weights = [] if self._weights_staged else list(self.weights)
        if not pending_weights and not assets:
            self._weights_staged = True
            return

        # Imported here, not at module top: huggingface_hub is a runner-side
        # extra ("cloud"), like aiohttp in the streaming executor.
        import huggingface_hub

        from cloud_offload.comfyui import comfyui_models_dir
        from cloud_offload.credentials import huggingface_token

        models_dir = comfyui_models_dir().resolve()
        token = huggingface_token() or None
        event_writer = getattr(self.queue, "append_event", None)
        progress_setter = getattr(self.queue, "set_progress", None)
        total_files = sum(
            len(entry["files"]) if entry.get("files") else 1 for entry in pending_weights
        ) + len(assets)
        downloaded = 0

        def publish(
            repo_id: str | None,
            filename: str | None,
            category: str | None = None,
        ) -> None:
            progress = 3 + round(6 * downloaded / max(1, total_files))
            if callable(progress_setter):
                progress_setter(job.id, progress)
            if callable(event_writer):
                event_writer(
                    job.id,
                    {
                        "type": "weights_staging",
                        "repo_id": repo_id,
                        "file": filename,
                        "category": category,
                        "downloaded_files": downloaded,
                        "total_files": total_files,
                        "overall_progress": progress,
                    },
                )

        for entry in pending_weights:
            repo_id = str(entry.get("repo_id") or "")
            revision = str(entry.get("revision") or "")
            target_dir = (models_dir / str(entry.get("dest") or "")).resolve()
            try:
                target_dir.relative_to(models_dir)
            except ValueError:
                # The dispatcher validated dest at config load; re-check anyway.
                raise RuntimeError(
                    f"Weights dest escapes the models directory: {entry.get('dest')!r}"
                )
            files = entry.get("files")
            if files:
                for filename in files:
                    target = target_dir / filename
                    if target.is_file() and target.stat().st_size > 0:
                        logger.info(
                            "Weights already staged, skipping %s (%s)", filename, repo_id
                        )
                        downloaded += 1
                        continue
                    publish(repo_id, filename)
                    try:
                        huggingface_hub.hf_hub_download(
                            repo_id=repo_id,
                            filename=filename,
                            revision=revision,
                            local_dir=str(target_dir),
                            token=token,
                        )
                    except Exception as exc:
                        raise RuntimeError(
                            f"Weights staging failed for {repo_id} ({filename}@{revision}): {exc}"
                        ) from exc
                    downloaded += 1
            else:
                publish(repo_id, None)
                try:
                    huggingface_hub.snapshot_download(
                        repo_id=repo_id,
                        revision=revision,
                        local_dir=str(target_dir),
                        token=token,
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"Weights staging failed for {repo_id}@{revision}: {exc}"
                    ) from exc
                downloaded += 1

        for asset in assets:
            publish(
                (asset.get("source") or {}).get("repo_id"),
                str(asset.get("filename") or ""),
                str(asset.get("category") or ""),
            )
            self._stage_declared_asset(asset, models_dir, token)
            downloaded += 1

        publish(None, None)
        self._weights_staged = True
        logger.info("Staged %d weight file(s) into %s", total_files, models_dir)

    def _stage_declared_asset(
        self, asset: dict, models_dir: Path, token: str | None
    ) -> None:
        """Place one job-declared model file, verified by content digest.

        A file already at the target path is trusted only when its digest
        matches the manifest. A mismatch is quarantined, never overwritten in
        silence: two checkpoints sharing a filename is exactly the failure that
        otherwise produces confident, wrong output.

        Nothing here loads or executes what it fetches — staging moves bytes.
        """
        category = str(asset.get("category") or "")
        filename = str(asset.get("filename") or "")
        expected = str(asset.get("sha256") or "").lower()
        target = (models_dir / category / filename).resolve()
        try:
            target.relative_to(models_dir)
        except ValueError:
            raise RuntimeError(
                f"Declared asset escapes the models directory: {category}/{filename}"
            )

        if target.is_file():
            present = sha256_file(target)
            if present == expected:
                logger.info("Declared asset already staged, skipping %s", target.name)
                return
            quarantine = models_dir / QUARANTINE_DIRNAME / present / filename
            quarantine.parent.mkdir(parents=True, exist_ok=True)
            logger.warning(
                "Declared asset %s/%s holds sha256 %s but this job requires %s; "
                "quarantining it at %s and fetching the declared bytes",
                category,
                filename,
                present,
                expected,
                quarantine,
            )
            shutil.move(str(target), str(quarantine))

        target.parent.mkdir(parents=True, exist_ok=True)
        self._fetch_declared_asset(asset, target, token)
        written = sha256_file(target)
        if written != expected:
            raise RuntimeError(
                f"Declared asset {category}/{filename} hashed {written} after "
                f"staging but the job declared {expected}"
            )

    def _fetch_declared_asset(self, asset: dict, target: Path, token: str | None) -> None:
        """Fetch one declared asset from the origin the coordinator resolved."""
        source = asset.get("source") or {}
        label = f"{asset.get('category')}/{asset.get('filename')}"
        try:
            if source.get("artifact_id"):
                self._download_partition_artifact(str(source["artifact_id"]), target)
            elif source.get("url"):
                self._download_asset_url(str(source["url"]), target)
            elif source.get("repo_id"):
                import huggingface_hub

                # hf_hub_download names the file after its path in the repo,
                # which need not match the runner's filename, so it lands in a
                # scratch directory and is moved into place.
                staging = target.parent / ".cloud-offload-fetch"
                staging.mkdir(parents=True, exist_ok=True)
                try:
                    fetched = huggingface_hub.hf_hub_download(
                        repo_id=str(source["repo_id"]),
                        filename=str(source.get("filename") or asset.get("filename")),
                        revision=str(source.get("revision") or ""),
                        local_dir=str(staging),
                        token=token,
                    )
                    shutil.move(str(fetched), str(target))
                finally:
                    shutil.rmtree(staging, ignore_errors=True)
            else:
                raise RuntimeError("no source was resolved for it")
        except Exception as exc:
            raise RuntimeError(f"Declared asset staging failed for {label}: {exc}") from exc

    @staticmethod
    def _download_asset_url(url: str, target: Path) -> None:
        """Stream a declared asset from a plain URL."""
        import requests

        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with target.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)

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
        artifact_id = sha256_file(source)
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
