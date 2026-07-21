#!/bin/bash
set -euo pipefail

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

python - <<'PY'
import time
import requests

deadline = time.monotonic() + 180
while time.monotonic() < deadline:
    try:
        response = requests.get("http://127.0.0.1:8188/system_stats", timeout=2)
        if response.ok:
            break
    except requests.RequestException:
        pass
    time.sleep(2)
else:
    raise SystemExit("ComfyUI did not become ready within 180 seconds")
PY

cloud-offload worker --poll "${CLOUD_OFFLOAD_POLL_INTERVAL:-10}"
