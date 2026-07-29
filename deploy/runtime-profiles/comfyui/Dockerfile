# Model-agnostic Cloud Offload ComfyUI runner.
#
# Plain pinned ComfyUI + the CloudPartition{Input,Output} bridge nodes + a baked
# capability manifest. It does NOT embed any 3D model runtime: generation nodes
# ride inside the submitted subgraph (install whatever custom nodes your graphs
# need as an additional build layer, or bake a "batteries included" variant).
FROM pytorch/pytorch@sha256:77f17f843507062875ce8be2a6f76aa6aa3df7f9ef1e31d9d7432f4b0f563dee

ARG CLOUD_OFFLOAD_SOURCE_REVISION=working-tree
ARG COMFYUI_REVISION=03978e1e81475f19eebd7edc065cc55cb4e15e10

LABEL org.opencontainers.image.title="Cloud Offload ComfyUI runner"
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

# Triton compiles its small CUDA driver helper on first use, so the otherwise
# slim runtime image still needs a host C toolchain when ComfyUI starts.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY . /opt/cloud-offload/source
COPY deploy/runtime-profiles/comfyui/runtime-profile.json /opt/cloud-offload/runtime-profile.json
COPY deploy/runtime-profiles/comfyui/entrypoint.sh /opt/cloud-offload/entrypoint.sh
COPY deploy/runtime-profiles/comfyui/ComfyUI-Cloud-Offload-Runtime /opt/ComfyUI/custom_nodes/ComfyUI-Cloud-Offload-Runtime

RUN python -m pip install --no-cache-dir "/opt/cloud-offload/source[cloud]" \
    && mkdir -p /opt/cloud-offload/partitions \
    && sed -i 's/\r$//' /opt/cloud-offload/entrypoint.sh \
    && chmod +x /opt/cloud-offload/entrypoint.sh \
    && python -c "import cloud_offload; print('Cloud Offload runner import OK')"

# Revision metadata belongs after the expensive immutable runtime layers. A new
# source revision must not force apt, ComfyUI, and its Python dependencies to be
# rebuilt from scratch.
LABEL org.opencontainers.image.revision="${CLOUD_OFFLOAD_SOURCE_REVISION}"

WORKDIR /opt/ComfyUI
ENTRYPOINT ["/opt/cloud-offload/entrypoint.sh"]
