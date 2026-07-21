"""Headless ComfyUI API client used inside Cloud Offload workflow runners."""

from __future__ import annotations

import base64
import asyncio
import json
import mimetypes
import os
import struct
import time
import uuid
from typing import Any


# ComfyUI UI output keys that carry retrievable file artifacts other than
# images. Core's 3D save nodes emit meshes under "3d"; extend as needed.
FILE_OUTPUT_KEYS = ("3d",)


class ComfyUIWorkflowError(RuntimeError):
    """Raised when the internal ComfyUI service rejects or fails a workflow."""


class ComfyUIWorkflowExecutor:
    """Execute API-format workflows against a colocated ComfyUI server."""

    def __init__(self, base_url: str | None = None, http_client: Any | None = None):
        if http_client is None:
            import requests

            http_client = requests.Session()
        self.http = http_client
        self.base_url = (
            base_url
            or os.environ.get("CLOUD_OFFLOAD_COMFYUI_URL", "http://127.0.0.1:8188")
        ).rstrip("/")

    def _request(self, method: str, path: str, **kwargs):
        response = self.http.request(
            method,
            f"{self.base_url}{path}",
            timeout=kwargs.pop("timeout", 120),
            **kwargs,
        )
        try:
            response.raise_for_status()
        except Exception as exc:
            status = getattr(response, "status_code", "unknown")
            detail: Any = None
            try:
                payload = response.json()
                detail = payload.get("error") or payload.get("node_errors") or payload
            except Exception:
                detail = getattr(response, "text", "")
            if isinstance(detail, (dict, list)):
                detail = json.dumps(detail, ensure_ascii=False, sort_keys=True)
            detail = str(detail or "no response body")
            raise ComfyUIWorkflowError(
                f"ComfyUI rejected {method} {path} with HTTP {status}: {detail}"
            ) from exc
        return response

    def _upload_inputs(self, inputs: dict[str, str]) -> dict[str, dict[str, str]]:
        uploaded = {}
        for filename, encoded in inputs.items():
            if not filename or "/" in filename or "\\" in filename:
                raise ComfyUIWorkflowError(f"Invalid ComfyUI input filename: {filename!r}")
            try:
                content = base64.b64decode(encoded, validate=True)
            except ValueError as exc:
                raise ComfyUIWorkflowError(f"Invalid base64 input: {filename}") from exc
            mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            response = self._request(
                "POST",
                "/upload/image",
                files={"image": (filename, content, mime)},
                data={"type": "input", "overwrite": "true"},
            ).json()
            uploaded[filename] = {
                "name": str(response.get("name") or filename),
                "subfolder": str(response.get("subfolder") or ""),
                "type": str(response.get("type") or "input"),
            }
        return uploaded

    def execute(
        self,
        workflow: dict[str, Any],
        *,
        inputs: dict[str, str] | None = None,
        timeout_seconds: int = 3600,
        event_callback: Any | None = None,
        cancel_check: Any | None = None,
    ) -> dict[str, Any]:
        if not isinstance(workflow, dict) or not workflow:
            raise ComfyUIWorkflowError("workflow must be a non-empty API-format object")
        uploaded = self._upload_inputs(inputs or {})
        client_id = str(uuid.uuid4())
        if event_callback is not None:
            return asyncio.run(
                self._execute_streaming(
                    workflow,
                    uploaded,
                    client_id,
                    timeout_seconds,
                    event_callback,
                    cancel_check,
                )
            )
        submitted = self._request(
            "POST",
            "/prompt",
            json={"prompt": workflow, "client_id": client_id},
        ).json()
        prompt_id = str(submitted.get("prompt_id") or "")
        if not prompt_id:
            raise ComfyUIWorkflowError(
                f"ComfyUI returned no prompt_id: {submitted.get('error') or submitted}"
            )

        history = self._wait_for_history(prompt_id, timeout_seconds)
        return self._format_result(prompt_id, uploaded, history)

    def _wait_for_history(self, prompt_id: str, timeout_seconds: int) -> dict[str, Any]:
        deadline = time.monotonic() + max(1, int(timeout_seconds))
        history = None
        while time.monotonic() < deadline:
            payload = self._request("GET", f"/history/{prompt_id}", timeout=30).json()
            if prompt_id in payload:
                history = payload[prompt_id]
                break
            time.sleep(1)
        if history is None:
            raise ComfyUIWorkflowError(
                f"ComfyUI workflow {prompt_id} exceeded {timeout_seconds}s"
            )

        return history

    def _format_result(
        self,
        prompt_id: str,
        uploaded: dict[str, dict[str, str]],
        history: dict[str, Any],
    ) -> dict[str, Any]:
        status = history.get("status") or {}
        if status.get("status_str") == "error" or status.get("completed") is False:
            messages = status.get("messages") or []
            raise ComfyUIWorkflowError(f"ComfyUI workflow failed: {messages}")

        outputs = history.get("outputs") or {}
        images = []
        files = []
        for node_id, node_output in outputs.items():
            for descriptor in node_output.get("images") or []:
                images.append(self._fetch_output_file(str(node_id), descriptor, "output"))
            # Non-image file outputs live under other UI keys. Core's 3D save
            # nodes (SaveGLB, etc.) emit their meshes under "3d", so a mesh
            # workflow would return nothing if we only read "images".
            for key in FILE_OUTPUT_KEYS:
                for descriptor in node_output.get(key) or []:
                    entry = self._fetch_output_file(str(node_id), descriptor, "output")
                    entry["output_kind"] = key
                    files.append(entry)
        return {
            "prompt_id": prompt_id,
            "uploaded_inputs": uploaded,
            "outputs": outputs,
            "images": images,
            "files": files,
        }

    def _fetch_output_file(
        self, node_id: str, descriptor: dict[str, Any], default_type: str
    ) -> dict[str, Any]:
        params = {
            "filename": descriptor["filename"],
            "subfolder": descriptor.get("subfolder", ""),
            "type": descriptor.get("type", default_type),
        }
        response = self._request("GET", "/view", params=params, timeout=120)
        return {
            "node_id": node_id,
            **params,
            "mime_type": response.headers.get("Content-Type", "application/octet-stream"),
            "data": base64.b64encode(response.content).decode("ascii"),
        }

    @staticmethod
    def _emit(callback: Any, event: dict[str, Any]) -> None:
        """Report progress without allowing telemetry failure to kill inference."""
        try:
            callback(event)
        except Exception:
            # The final job status remains authoritative if a transient coordinator
            # request fails while inference itself is healthy.
            return

    async def _execute_streaming(
        self,
        workflow: dict[str, Any],
        uploaded: dict[str, dict[str, str]],
        client_id: str,
        timeout_seconds: int,
        event_callback: Any,
        cancel_check: Any | None,
    ) -> dict[str, Any]:
        """Execute while relaying ComfyUI's websocket events node by node."""
        import aiohttp

        ws_base = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        timeout = aiohttp.ClientTimeout(total=None, connect=30)
        prompt_id = ""
        deadline = time.monotonic() + max(1, int(timeout_seconds))
        history = None
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(
                f"{ws_base}/ws", params={"clientId": client_id}, heartbeat=30
            ) as websocket:
                submitted = self._request(
                    "POST",
                    "/prompt",
                    json={"prompt": workflow, "client_id": client_id},
                ).json()
                prompt_id = str(submitted.get("prompt_id") or "")
                if not prompt_id:
                    raise ComfyUIWorkflowError(
                        f"ComfyUI returned no prompt_id: {submitted.get('error') or submitted}"
                    )
                self._emit(
                    event_callback,
                    {"type": "execution_submitted", "prompt_id": prompt_id},
                )
                last_node = None
                while time.monotonic() < deadline:
                    if cancel_check is not None and cancel_check():
                        self._request("POST", "/interrupt", timeout=30)
                        self._emit(
                            event_callback,
                            {
                                "type": "execution_cancelled",
                                "prompt_id": prompt_id,
                                "node_id": last_node,
                            },
                        )
                        raise ComfyUIWorkflowError("Cloud ComfyUI execution was cancelled")
                    remaining = max(0.1, deadline - time.monotonic())
                    try:
                        message = await websocket.receive(timeout=min(5.0, remaining))
                    except asyncio.TimeoutError:
                        payload = self._request(
                            "GET", f"/history/{prompt_id}", timeout=30
                        ).json()
                        if prompt_id in payload:
                            history = payload[prompt_id]
                            break
                        continue
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            envelope = json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        data = envelope.get("data") or {}
                        if data.get("prompt_id") not in {None, prompt_id}:
                            continue
                        event_type = str(envelope.get("type") or "")
                        if event_type == "status":
                            continue
                        if event_type == "executing":
                            last_node = data.get("display_node") or data.get("node")
                        self._emit(
                            event_callback,
                            {
                                "type": event_type,
                                "prompt_id": prompt_id,
                                "node_id": data.get("display_node")
                                or data.get("node")
                                or last_node,
                                "data": data,
                            },
                        )
                        if event_type == "executed":
                            for descriptor in (data.get("output") or {}).get("images") or []:
                                params = {
                                    "filename": descriptor.get("filename"),
                                    "subfolder": descriptor.get("subfolder", ""),
                                    "type": descriptor.get("type", "temp"),
                                }
                                if not params["filename"]:
                                    continue
                                response = self._request(
                                    "GET", "/view", params=params, timeout=120
                                )
                                if len(response.content) <= 2 * 1024 * 1024:
                                    self._emit(
                                        event_callback,
                                        {
                                            "type": "preview",
                                            "prompt_id": prompt_id,
                                            "node_id": data.get("display_node")
                                            or data.get("node")
                                            or last_node,
                                            "mime_type": response.headers.get(
                                                "Content-Type", "image/jpeg"
                                            ),
                                            "metadata": params,
                                            "data_base64": base64.b64encode(
                                                response.content
                                            ).decode("ascii"),
                                        },
                                    )
                        if event_type in {"execution_success", "execution_error"}:
                            payload = self._request(
                                "GET", f"/history/{prompt_id}", timeout=30
                            ).json()
                            history = payload.get(prompt_id)
                            if history is not None:
                                break
                    elif message.type == aiohttp.WSMsgType.BINARY:
                        raw = bytes(message.data)
                        if len(raw) < 8:
                            continue
                        binary_type = struct.unpack(">I", raw[:4])[0]
                        mime_type = "application/octet-stream"
                        image = b""
                        metadata: dict[str, Any] = {}
                        if binary_type == 1:
                            image_type = struct.unpack(">I", raw[4:8])[0]
                            mime_type = "image/png" if image_type == 2 else "image/jpeg"
                            image = raw[8:]
                        elif binary_type == 4:
                            metadata_size = struct.unpack(">I", raw[4:8])[0]
                            if metadata_size > len(raw) - 8:
                                continue
                            try:
                                metadata = json.loads(raw[8 : 8 + metadata_size])
                            except (UnicodeDecodeError, json.JSONDecodeError):
                                metadata = {}
                            mime_type = str(metadata.get("image_type") or "image/jpeg")
                            image = raw[8 + metadata_size :]
                        if image and len(image) <= 2 * 1024 * 1024:
                            self._emit(
                                event_callback,
                                {
                                    "type": "preview",
                                    "prompt_id": prompt_id,
                                    "node_id": last_node,
                                    "mime_type": mime_type,
                                    "metadata": metadata,
                                    "data_base64": base64.b64encode(image).decode("ascii"),
                                },
                            )
                    elif message.type in {
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    }:
                        break
        if history is None:
            remaining = max(1, int(deadline - time.monotonic()))
            history = self._wait_for_history(prompt_id, remaining)
        return self._format_result(prompt_id, uploaded, history)
