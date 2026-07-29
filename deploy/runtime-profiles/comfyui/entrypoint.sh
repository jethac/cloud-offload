#!/bin/bash
set -euo pipefail

# One identity for every phase of the boot. The registration a starting runner
# makes, the reason it reports if it never starts, and the worker that will
# claim jobs are all the same worker to the coordinator.
export CLOUD_OFFLOAD_WORKER_ID="${CLOUD_OFFLOAD_WORKER_ID:-worker-$(python -c 'import uuid; print(uuid.uuid4().hex[:8])')}"

# Registers this runner as starting, and stages the profile's node packs. Both
# happen before ComfyUI: it builds its node registry while it imports, so a pack
# installed after the server is up is a pack the server will never see.
cloud-offload runner-boot

# Custom-node Python packages are installed or restored into one immutable
# profile environment before ComfyUI imports its node registry.
export PYTHONPATH="${CLOUD_OFFLOAD_ENV_ROOT:-/opt/cloud-offload/environment}${PYTHONPATH:+:${PYTHONPATH}}"

python /opt/ComfyUI/main.py \
  --listen 127.0.0.1 \
  --port 8188 \
  --disable-auto-launch \
  --disable-metadata \
  >/tmp/comfyui.log 2>&1 &
comfy_pid=$!

cleanup() {
  kill "${comfy_pid}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Waits while ComfyUI is alive, fails the moment it is not, and reports either
# outcome home with the tail of /tmp/comfyui.log before this container exits.
cloud-offload runner-ready --comfyui-pid "${comfy_pid}" --log-file /tmp/comfyui.log

cloud-offload worker --poll "${CLOUD_OFFLOAD_POLL_INTERVAL:-10}"
