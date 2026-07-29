# syntax=docker/dockerfile:1.19

# Model-agnostic Cloud Offload ComfyUI runner.
#
# Plain pinned ComfyUI + the CloudPartition{Input,Output} bridge nodes + a baked
# capability manifest. It does NOT embed any 3D model runtime: generation nodes
# ride inside the submitted subgraph (install whatever custom nodes your graphs
# need as an additional build layer, or bake a "batteries included" variant).
FROM pytorch/pytorch@sha256:77f17f843507062875ce8be2a6f76aa6aa3df7f9ef1e31d9d7432f4b0f563dee AS pytorch-runtime

# The pinned PyTorch image stores its complete 5.95 GB Conda tree in one base
# layer. Its registry blob is 3.28 GB, so a rented worker cannot start until one
# long serial download completes. Repack the same files into independent layers.
# Container runtimes can pull these layers in parallel.
FROM ubuntu:22.04@sha256:0e0a0fc6d18feda9db1590da249ac93e8d5abfea8f4c3c0c849ce512b5ef8982

COPY --from=pytorch-runtime \
    --exclude=lib/python3.11/site-packages/torch \
    --exclude=lib/python3.11/site-packages/nvidia \
    /opt/conda/ /opt/conda/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/torch/ /opt/conda/lib/python3.11/site-packages/torch/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/__init__.py /opt/conda/lib/python3.11/site-packages/nvidia/__init__.py
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cublas/ /opt/conda/lib/python3.11/site-packages/nvidia/cublas/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cuda_cupti/ /opt/conda/lib/python3.11/site-packages/nvidia/cuda_cupti/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cuda_nvrtc/ /opt/conda/lib/python3.11/site-packages/nvidia/cuda_nvrtc/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cuda_runtime/ /opt/conda/lib/python3.11/site-packages/nvidia/cuda_runtime/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cudnn/ /opt/conda/lib/python3.11/site-packages/nvidia/cudnn/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cufft/ /opt/conda/lib/python3.11/site-packages/nvidia/cufft/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/curand/ /opt/conda/lib/python3.11/site-packages/nvidia/curand/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cusolver/ /opt/conda/lib/python3.11/site-packages/nvidia/cusolver/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/cusparse/ /opt/conda/lib/python3.11/site-packages/nvidia/cusparse/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/nccl/ /opt/conda/lib/python3.11/site-packages/nvidia/nccl/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/nvjitlink/ /opt/conda/lib/python3.11/site-packages/nvidia/nvjitlink/
COPY --from=pytorch-runtime /opt/conda/lib/python3.11/site-packages/nvidia/nvtx/ /opt/conda/lib/python3.11/site-packages/nvidia/nvtx/

ARG COMFYUI_REVISION=03978e1e81475f19eebd7edc065cc55cb4e15e10

LABEL org.opencontainers.image.title="Cloud Offload ComfyUI runner"
LABEL ai.cloud-offload.comfyui-source-revision="${COMFYUI_REVISION}"

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PATH=/opt/conda/bin:/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    LD_LIBRARY_PATH=/usr/local/nvidia/lib:/usr/local/nvidia/lib64 \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility \
    NVIDIA_VISIBLE_DEVICES=all \
    PYTORCH_VERSION=2.6.0 \
    CLOUD_OFFLOAD_WORKER_MANIFEST=/opt/cloud-offload/runtime-profile.json \
    CLOUD_OFFLOAD_COMFYUI_URL=http://127.0.0.1:8188 \
    CLOUD_OFFLOAD_COMFYUI_ROOT=/opt/ComfyUI \
    COMFY_PARTITION_ROOT=/opt/cloud-offload/partitions \
    HF_HUB_ENABLE_HF_TRANSFER=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        git \
        libgl1 \
        libglib2.0-0 \
        libjpeg-dev \
        libopengl0 \
        libpng-dev \
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
    && sed -i 's/\r$//' /opt/cloud-offload/entrypoint.sh \
    && chmod +x /opt/cloud-offload/entrypoint.sh \
    && python -c "import cloud_offload; print('Cloud Offload runner import OK')"

# Revision metadata belongs after the expensive immutable runtime layers. A new
# source revision must not force apt, ComfyUI, and its Python dependencies to be
# rebuilt from scratch.
ARG CLOUD_OFFLOAD_SOURCE_REVISION=working-tree
LABEL org.opencontainers.image.revision="${CLOUD_OFFLOAD_SOURCE_REVISION}"

WORKDIR /opt/ComfyUI
ENTRYPOINT ["/opt/cloud-offload/entrypoint.sh"]
