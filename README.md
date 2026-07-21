# Cloud Offload

A standalone, provider-neutral **cloud offload coordinator** for ComfyUI. It
takes ComfyUI workflows (or compiled subgraph *partitions*) submitted over HTTP,
provisions cloud GPU workers on demand, runs the graph on a headless ComfyUI
inside the worker, and streams resumable execution events and content-addressed
result artifacts back to the caller.

The coordinator **never loads a model**. Generation rides inside the submitted
ComfyUI subgraph, so the runner image only needs a ComfyUI that has the nodes
your graph uses. This makes the service model-agnostic and reusable for any
ComfyUI workload.

It pairs with the separately built [`ComfyUI-Cloud-Offload`](https://github.com/jethac/ComfyUI-Cloud-Offload)
node pack (the selection-box UI, queue-time partition compiler, and thin HTTP
client). Both agree on the wire contract: neutral routes, the
`comfy.partition.bundle.v1` bundle format (`.part` files), and the
`CLOUD_OFFLOAD_URL` / `CLOUD_OFFLOAD_TOKEN` / `COMFY_PARTITION_ROOT` env vars.

## Architecture

```
ComfyUI node pack ──HTTP──▶ Coordinator (FastAPI + SQLite queue)
                                │  queue-depth-driven provisioning
                                ▼
                           Dispatcher ──▶ Provider (RunPod default / Vast.ai)
                                │              rents a GPU pod running the
                                ▼              runner image
                    Worker (inside runner) ──▶ headless ComfyUI
                       claims jobs, stages typed boundary artifacts,
                       relays websocket events, uploads .part outputs
```

- **Coordinator** (`cloud_offload.server`): FastAPI service exposing neutral
  routes with a stable error envelope and a LAN bearer-token middleware. A
  separate `Bearer <worker_token>` channel (`/api/workers/*`) is exempt from the
  LAN token.
- **JobQueue** (`cloud_offload.queue`): SQLite store for jobs, resumable
  `job_events`, `partition_cache`, and worker tokens/heartbeats.
- **Dispatcher** (`cloud_offload.dispatcher`): watches queue depth per
  provider/profile, provisions workers past a threshold, emits provisioning
  events, and enforces idle-shutdown / keep-warm.
- **Worker** (`cloud_offload.worker`): claims jobs and runs the
  `ComfyUIWorkflowExecutor` for `comfyui-workflow` and `comfyui-partition-v1`.
- **Providers** (`cloud_offload.providers`): pluggable connector registry;
  RunPod (default) and Vast.ai ship built in.
- **Storage** (`cloud_offload.storage`): Local / GCS / S3 for content-addressed
  `.part` artifacts.
- **Partition protocol** (`cloud_offload.partition_protocol`): the safe,
  pickle-free `comfy.partition.bundle.v1` bundle (ZIP of `manifest.json` +
  `tensors.safetensors` + `blobs/`).

## Install

```bash
pip install -e .            # coordinator (core)
pip install -e ".[cloud]"   # add aiohttp + safetensors (runner side)
pip install -e ".[gcs]"     # GCS storage backend
pip install -e ".[s3]"      # S3 storage backend
```

Requires Python 3.10+.

## Run the coordinator

```bash
cloud-offload serve                 # binds 127.0.0.1, auto-selects a port
cloud-offload serve --port 11435    # explicit port (11434 is refused: Ollama)
cloud-offload serve --allow-lan --host 0.0.0.0   # LAN bind requires a bearer token
```

On startup it writes a discovery file to `~/.cloud-offload/service.json` (the
port scan skips Ollama's 11434). When bound to a LAN address it requires
`Authorization: Bearer <token>` on every non-worker route; the token is created
under `~/.cloud-offload/token`.

Other commands:

```bash
cloud-offload dispatch      # run the provisioning dispatcher (needs provider creds)
cloud-offload worker        # run a worker (normally the runner image's entrypoint)
cloud-offload queue status  # inspect the local job queue
```

## Provider setup

RunPod is the **default** provider (`provider="runpod"`, order
`["runpod","vast.ai"]`). Credentials come from the environment only — they are
never written to `config.json` and never returned by the API.

### RunPod (default)

```bash
export RUNPOD_API_KEY=...             # required
export RUNPOD_CLOUD_TYPE=SECURE       # or COMMUNITY
export RUNPOD_REGISTRY_AUTH_ID=...    # required to pull a private GHCR runner image
```

### Vast.ai (the "add a provider" example)

Vast.ai is the worked example of adding an alternative provider. Set its key and
put it in the routing order:

```bash
export VAST_API_KEY=...
export CLOUD_OFFLOAD_PROVIDERS="runpod,vast.ai"
```

The connector registry is pluggable (`register_connector(name, factory,
aliases=...)`), so Comfy Cloud or any other backend can be added the same way
without touching the coordinator.

### Worker profiles

A profile pins a runner image **by digest** and declares which providers can run
it. Persist non-secret config via `POST /api/config` or `~/.cloud-offload/config.json`:

```json
{
  "enabled": true,
  "worker_profiles": {
    "comfyui": {
      "image": "ghcr.io/jethac/cloud-offload-runner-comfyui@sha256:<digest>",
      "models": ["comfyui-workflow", "comfyui-partition-v1"],
      "providers": ["runpod", "vast.ai"],
      "gpu_type": "any",
      "min_gpu_ram_gb": 16
    }
  }
}
```

See [`deploy/runtime-profiles/`](deploy/runtime-profiles/) for the model-agnostic
runner image (plain ComfyUI + the `CloudPartition{Input,Output}` bridge nodes +
the baked capability manifest).

## HTTP routes

Client (node-pack) routes:

| Method | Path | Purpose |
|---|---|---|
| GET  | `/api/health` | `{"name":"cloud-offload","status":"ok","version":…}` |
| GET  | `/api/status` | queue counts + active workers + providers |
| GET/POST | `/api/config` | read / update non-secret config (POST rejects secrets) |
| GET  | `/api/providers` | credential presence + live balances |
| POST | `/api/partitions` | submit a compiled `comfy.partition.job.v1` job (content-hash cache) |
| POST | `/api/workflows` | submit a whole API-format workflow to a runner |
| POST | `/api/artifacts` | upload a `.part` bundle (sha256 content-addressed) |
| GET  | `/api/artifacts/{id}` | download a bundle (digest-verified) |
| GET  | `/api/jobs/{id}` | job status (+ `result` when completed) |
| POST | `/api/jobs/{id}/cancel` | request cancellation |
| GET  | `/api/jobs/{id}/events?after=N&limit=M` | resumable event page (`next_after`) |

Worker channel (separate `Bearer <worker_token>`, exempt from the LAN token):
`POST /api/workers/claim`, `GET /api/workers/policy`,
`GET|POST /api/workers/artifacts[/{id}]`, `GET /api/workers/jobs/{id}`,
`.../running`, `.../progress`, `.../events`, `.../complete`, `.../fail`.

Every error uses the stable envelope `{"error":{"code","message","details"}}`.

## Tests

```bash
python -m pytest
```

## License

MIT
