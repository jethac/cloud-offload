"""Runner-only bridge nodes for Cloud Offload graph partitions.

These nodes are compiler-generated and dev-only: users never hand-place them.
The queue-time compiler in the ComfyUI-Cloud-Offload node pack emits
``CloudPartitionInput`` / ``CloudPartitionOutput`` class_types, and the worker
stages their ``artifact_path`` / ``output_path`` inputs before execution. They
transport typed boundary values across a cloud partition using the
``comfy.partition.bundle.v1`` safe bundle format.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from comfy_api.latest import ComfyExtension, io
from cloud_offload.partition_protocol import dump_bundle, load_bundle, validate_boundary_type


def _partition_path(value: str, *, must_exist: bool) -> Path:
    root_value = os.environ.get("COMFY_PARTITION_ROOT")
    if not root_value:
        raise ValueError("COMFY_PARTITION_ROOT is not configured")
    root = Path(root_value).resolve()
    path = Path(value).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError("Partition artifact path escapes COMFY_PARTITION_ROOT") from exc
    if must_exist and not path.is_file():
        raise ValueError(f"Partition input artifact does not exist: {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


class CloudPartitionInput(io.ComfyNode):
    @classmethod
    def define_schema(cls) -> io.Schema:
        return io.Schema(
            node_id="CloudPartitionInput",
            display_name="Cloud Partition Input",
            category="Cloud Offload/Internal",
            inputs=[
                io.String.Input("boundary_key"),
                io.String.Input("artifact_path"),
                io.String.Input("type_name"),
            ],
            outputs=[io.AnyType.Output(display_name="value")],
            is_dev_only=True,
        )

    @classmethod
    def execute(cls, boundary_key: str, artifact_path: str, type_name: str) -> io.NodeOutput:
        validate_boundary_type(type_name)
        return io.NodeOutput(load_bundle(_partition_path(artifact_path, must_exist=True)))


class CloudPartitionOutput(io.ComfyNode):
    @classmethod
    def define_schema(cls) -> io.Schema:
        return io.Schema(
            node_id="CloudPartitionOutput",
            display_name="Cloud Partition Output",
            category="Cloud Offload/Internal",
            inputs=[
                io.AnyType.Input("value"),
                io.String.Input("boundary_key"),
                io.String.Input("output_path"),
                io.String.Input("type_name"),
            ],
            outputs=[],
            is_output_node=True,
            is_dev_only=True,
        )

    @classmethod
    def execute(
        cls, value: Any, boundary_key: str, output_path: str, type_name: str
    ) -> io.NodeOutput:
        validate_boundary_type(type_name)
        path = _partition_path(output_path, must_exist=False)
        metadata = dump_bundle(value, path)
        return io.NodeOutput(
            ui={
                "comfy_partition_artifacts": [
                    {"boundary_key": boundary_key, "path": str(path), **metadata}
                ]
            }
        )


class CloudPartitionRuntimeExtension(ComfyExtension):
    async def get_node_list(self) -> list[type[io.ComfyNode]]:
        return [CloudPartitionInput, CloudPartitionOutput]


async def comfy_entrypoint() -> CloudPartitionRuntimeExtension:
    return CloudPartitionRuntimeExtension()
