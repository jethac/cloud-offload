"""Runner startup, lifted out of the image's entrypoint script.

Two phases, both invoked by ``entrypoint.sh``:

``runner-boot`` runs before ComfyUI. It registers the worker as ``starting`` so
the dispatcher knows a pod is already coming up for this profile, and it stages
the profile's node packs, which have to be on disk before ComfyUI imports its
node registry.

``runner-ready`` runs after ComfyUI has been launched into the background. It
waits for the server to answer, and if it never does it says why — with the tail
of ComfyUI's own log — over the worker channel, before the container exits and
takes the evidence with it. Both live here rather than in bash because a startup
path nobody can test is a startup path that fails once, expensively, in a pod.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Callable

from cloud_offload.config import CloudConfig
from cloud_offload.worker import (
    MAX_CAPTURED_OUTPUT,
    Worker,
    resolve_worker_id,
    worker_queue,
)

logger = logging.getLogger(__name__)

DEFAULT_COMFYUI_LOG = "/tmp/comfyui.log"

# How long a ComfyUI that is alive but has not answered yet is given.
#
# This is a backstop, not the mechanism: a ComfyUI that has exited fails on the
# spot, so the cap only decides how long to wait on a process that is up and
# still working. The 180 seconds it replaces was neither — it killed pods that
# were making progress. A cold runner walks a large models directory, imports
# torch, and imports every staged custom node pack behind it (two of which pull
# in diffusers), and it does all of that on a machine whose disk it is sharing.
# Twenty minutes covers that with room to spare, and costs about nine cents of
# an RTX A5000 if it is ever spent in full — far less than the pod that would
# otherwise be rented, killed, and rented again.
DEFAULT_READY_TIMEOUT_SECONDS = 1200.0
READY_TIMEOUT_ENV = "CLOUD_OFFLOAD_COMFYUI_READY_TIMEOUT"

READY_POLL_SECONDS = 2.0
# Registration ages out of the coordinator's active list after 90 seconds, so a
# runner that is still coming up has to keep saying so or the dispatcher will
# decide nothing is on its way and rent a second pod.
HEARTBEAT_SECONDS = 30.0
READY_PROBE_TIMEOUT_SECONDS = 5.0


class RunnerStartupError(RuntimeError):
    """Raised when a runner cannot reach a state where it could claim a job."""


def ready_timeout_seconds() -> float:
    """The absolute cap on waiting for ComfyUI, in seconds."""
    raw = os.environ.get(READY_TIMEOUT_ENV, "").strip()
    if not raw:
        return DEFAULT_READY_TIMEOUT_SECONDS
    try:
        seconds = float(raw)
    except ValueError:
        raise RuntimeError(f"{READY_TIMEOUT_ENV} is not a number of seconds: {raw!r}")
    if seconds <= 0:
        raise RuntimeError(f"{READY_TIMEOUT_ENV} must be positive, got {raw!r}")
    return seconds


def log_tail(path: str | Path | None, limit: int = MAX_CAPTURED_OUTPUT) -> str:
    """The last ``limit`` characters of a runner log.

    The reason a runner failed is almost never in the failure message; it is in
    whatever ComfyUI printed just before it stopped, in a file that dies with the
    container. Only the tail is read, and it is read from the end, so a process
    that spent its last minutes writing gigabytes cannot turn its own failure
    report into a second failure.
    """
    if not path:
        return ""
    try:
        with Path(path).open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            # UTF-8 needs at most four bytes per character; read that much and
            # trim precisely after decoding.
            handle.seek(max(0, size - limit * 4))
            raw = handle.read()
    except OSError:
        return ""
    return raw.decode("utf-8", errors="replace")[-limit:]


def process_is_alive(pid: int) -> bool:
    """Whether a process is still running. A zombie is dead, not slow.

    ComfyUI is started by the entrypoint, so this process is not its parent and
    cannot ``wait`` on it. A ComfyUI that crashed lingers as a zombie until the
    shell reaps it, and a zombie answers ``kill(pid, 0)`` exactly as a healthy
    process would, so ``/proc`` decides wherever it exists.
    """
    try:
        status = Path(f"/proc/{pid}/status").read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass
    else:
        for line in status.splitlines():
            if line.startswith("State:"):
                return not line.split(":", 1)[1].split()[0].startswith("Z")
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        # Running, but owned by somebody this process may not signal.
        return True
    return True


def comfyui_is_ready(timeout: float = READY_PROBE_TIMEOUT_SECONDS) -> bool:
    """Whether the colocated ComfyUI answers, which is the only proof of readiness."""
    import requests

    base = os.environ.get("CLOUD_OFFLOAD_COMFYUI_URL", "http://127.0.0.1:8188").rstrip("/")
    try:
        response = requests.get(f"{base}/system_stats", timeout=timeout)
    except requests.RequestException:
        return False
    return bool(response.ok)


def wait_for_comfyui(
    *,
    is_alive: Callable[[], bool],
    probe: Callable[[], bool],
    heartbeat: Callable[[], None] | None = None,
    timeout_seconds: float | None = None,
    poll_seconds: float = READY_POLL_SECONDS,
    heartbeat_seconds: float = HEARTBEAT_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Wait for ComfyUI to answer: patiently while it lives, not against a clock.

    Three outcomes, and the old fixed window could only express one of them. A
    process that has exited is not slow, so it fails immediately with whatever it
    logged rather than burning the rest of the budget waiting for a corpse. A
    process that is alive is making progress, and a cold pod is entitled to take
    its time. The cap is only for the third case: alive and wedged.
    """
    if timeout_seconds is None:
        timeout_seconds = ready_timeout_seconds()
    deadline = clock() + timeout_seconds
    last_heartbeat = clock()
    while True:
        if probe():
            return
        if not is_alive():
            raise RunnerStartupError(
                "ComfyUI exited before it became ready"
            )
        now = clock()
        if now >= deadline:
            raise RunnerStartupError(
                f"ComfyUI was still alive but had not become ready after "
                f"{timeout_seconds:.0f} seconds"
            )
        if heartbeat is not None and now - last_heartbeat >= heartbeat_seconds:
            last_heartbeat = now
            heartbeat()
        sleep(poll_seconds)


def report_startup_failure(
    config: CloudConfig,
    worker_id: str,
    reason: str,
    log_path: str | Path | None = None,
    queue: Any | None = None,
) -> None:
    """Tell the coordinator why this runner never started, then let it die.

    Built from configuration rather than from a :class:`Worker`, because the
    failure this most needs to survive is one where no worker could be
    constructed at all. Best effort in both directions: a coordinator that
    cannot be reached must not replace the reason with a stack trace about
    reaching it, so the reason is logged either way.
    """
    detail = reason
    tail = log_tail(log_path)
    if tail:
        detail = f"{reason}\n\n--- runner log (last {len(tail)} characters) ---\n{tail}"
    logger.error("Runner startup failed: %s", detail)
    if queue is None:
        try:
            queue = worker_queue(config, worker_id)
        except Exception as exc:
            logger.error("No coordinator channel to report the failure on: %s", exc)
            return
    recorder = getattr(queue, "record_worker", None)
    if not callable(recorder):
        return
    try:
        recorder(
            worker_id,
            config.provider,
            status="failed",
            runtime_profile=config.worker_profile or "",
            detail=detail,
        )
    except Exception as exc:
        logger.error("Could not report the runner startup failure: %s", exc)


def run_boot(config: CloudConfig | None = None, worker: Worker | None = None) -> int:
    """Register this runner and stage its node packs, before ComfyUI starts."""
    config = config or CloudConfig.load()
    worker_id = resolve_worker_id()
    try:
        worker = worker or Worker(config, worker_id=worker_id)
    except Exception as exc:
        report_startup_failure(config, worker_id, f"Runner configuration failed: {exc}")
        return 1
    worker.register("starting")
    try:
        worker.stage_node_packs()
    except Exception as exc:
        report_startup_failure(
            config,
            worker.worker_id,
            f"Node pack staging failed: {exc}",
            queue=worker.queue,
        )
        return 1
    worker.register("starting")
    return 0


def run_ready(
    comfyui_pid: int,
    log_path: str | Path | None = DEFAULT_COMFYUI_LOG,
    timeout_seconds: float | None = None,
    config: CloudConfig | None = None,
    worker: Worker | None = None,
    **wait_arguments,
) -> int:
    """Wait for the colocated ComfyUI, or report home why it never answered."""
    config = config or CloudConfig.load()
    worker_id = resolve_worker_id()
    try:
        worker = worker or Worker(config, worker_id=worker_id)
    except Exception as exc:
        report_startup_failure(
            config, worker_id, f"Runner configuration failed: {exc}", log_path
        )
        return 1
    worker.register("starting")
    wait_arguments.setdefault("is_alive", lambda: process_is_alive(comfyui_pid))
    wait_arguments.setdefault("probe", comfyui_is_ready)
    wait_arguments.setdefault("heartbeat", lambda: worker.register("starting"))
    try:
        # RunnerStartupError is a RuntimeError; so is a nonsensical cap.
        wait_for_comfyui(timeout_seconds=timeout_seconds, **wait_arguments)
    except RuntimeError as exc:
        report_startup_failure(
            config, worker.worker_id, str(exc), log_path, queue=worker.queue
        )
        return 1
    logger.info("ComfyUI is ready; handing over to the worker loop")
    return 0
