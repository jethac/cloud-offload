# Model-agnostic Cloud Offload ComfyUI runner.
#
# Plain pinned ComfyUI + the CloudPartition{Input,Output} bridge nodes + a baked
# capability manifest. It does NOT embed any 3D model runtime: generation nodes
# ride inside the submitted subgraph (install whatever custom nodes your graphs
# need as an additional build layer, or bake a "batteries included" variant).
FROM pytorch/pytorch@sha256:0cf3402e946b7c384ba943ee05c90b4c5a4a05227923921f2b0918c011cfaf56

ARG CLOUD_OFFLOAD_SOURCE_REVISION=working-tree
ARG COMFYUI_REVISION=03978e1e81475f19eebd7edc065cc55cb4e15e10

LABEL org.opencontainers.image.title="Cloud Offload ComfyUI runner"
LABEL org.opencontainers.image.revision="${CLOUD_OFFLOAD_SOURCE_REVISION}"
LABEL ai.cloud-offload.comfyui-source-revision="${COMFYUI_REVISION}"

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    CLOUD_OFFLOAD_WORKER_MANIFEST=/opt/cloud-offload/runtime-profile.json \
    CLOUD_OFFLOAD_COMFYUI_URL=http://127.0.0.1:8188 \
    CLOUD_OFFLOAD_COMFYUI_ROOT=/opt/ComfyUI \
    COMFY_PARTITION_ROOT=/opt/cloud-offload/partitions \
    HF_HUB_ENABLE_HF_TRANSFER=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git libgl1 libglib2.0-0 libopengl0 \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --filter=blob:none https://github.com/comfyanonymous/ComfyUI.git /opt/ComfyUI \
    && git -C /opt/ComfyUI checkout --detach "${COMFYUI_REVISION}"

RUN python -m pip install --no-cache-dir -r /opt/ComfyUI/requirements.txt

COPY . /opt/cloud-offload/source
COPY deploy/runtime-profiles/comfyui/runtime-profile.json /opt/cloud-offload/runtime-profile.json
COPY deploy/runtime-profiles/comfyui/entrypoint.sh /opt/cloud-offload/entrypoint.sh
COPY deploy/runtime-profiles/comfyui/ComfyUI-Cloud-Offload-Runtime /opt/ComfyUI/custom_nodes/ComfyUI-Cloud-Offload-Runtime

RUN python -m pip install --no-cache-dir "/opt/cloud-offload/source[cloud]" \
    && mkdir -p /opt/cloud-offload/partitions \
    && chmod +x /opt/cloud-offload/entrypoint.sh \
    && python -c "import cloud_offload; print('Cloud Offload runner import OK')"

WORKDIR /opt/ComfyUI
ENTRYPOINT ["/opt/cloud-offload/entrypoint.sh"]
