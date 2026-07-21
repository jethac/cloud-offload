# Cloud Offload runner profiles

A runner image is an immutable ComfyUI worker. Every image bakes
`/opt/cloud-offload/runtime-profile.json`; the worker intersects that manifest
with the capabilities the coordinator requests and probes ComfyUI readiness
before claiming work.

## `comfyui` (default, model-agnostic)

The default runner is **model-agnostic**: plain pinned ComfyUI plus the
`CloudPartition{Input,Output}` bridge nodes and a baked manifest declaring
`["comfyui-workflow", "comfyui-partition-v1"]` with
`partition_protocol: comfy.partition.bundle.v1`. It sets `COMFY_PARTITION_ROOT`
and does **not** embed any 3D model runtime. Generation nodes ride inside the
submitted subgraph, so a graph that uses Hunyuan/TRELLIS/etc. just needs a
ComfyUI that has those nodes installed — add them as an extra build layer if you
want a "batteries included" variant.

Build from the repository root and pin the image by digest (never a mutable
tag):

```bash
docker build -f deploy/runtime-profiles/comfyui/Dockerfile \
  --build-arg CLOUD_OFFLOAD_SOURCE_REVISION=$(git rev-parse HEAD) \
  -t ghcr.io/jethac/cloud-offload-runner-comfyui:0.1.0 .
```

Push the image, resolve its registry digest, and configure a worker profile with
the digest:

```json
{
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

`POST /api/workflows` targets this `comfyui` profile; `POST /api/partitions`
targets whichever `comfyui*` profile the compiled job requests via
`partition.runner.profile`.

## Adding native model nodes ("batteries included")

To offload graphs that need model nodes, extend the default Dockerfile with the
custom-node repositories your workflows import (pin each by revision) before the
`pip install` layer, then register the resulting image under a distinct profile
name (still starting with `comfyui`, e.g. `comfyui-omni`). Nothing else in the
coordinator changes — the runner still only claims the two workflow
capabilities.
