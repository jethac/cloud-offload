"""Vast.ai cloud connector.

Vast.ai is the worked example of "adding a provider" alongside the default
RunPod connector.
"""

import json
import os
import time

from cloud_offload.providers.base import CloudConnector, Instance


class VastConnector(CloudConnector):
    """
    Vast.ai cloud provider.

    Requires VAST_API_KEY environment variable.
    Install vastai CLI: pip install vastai
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://console.vast.ai/api/v0",
    ):
        self.api_key = api_key or os.environ.get("VAST_API_KEY")
        if not self.api_key:
            raise ValueError("VAST_API_KEY required")

        # Import here to avoid hard dependency
        try:
            import requests

            self.requests = requests
        except ImportError:
            raise ImportError("requests required: pip install requests")

        self.base_url = base_url.rstrip("/")

    @property
    def name(self) -> str:
        return "vast.ai"

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make authenticated API request."""
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.api_key}"

        url = f"{self.base_url}/{endpoint}"
        response = self.requests.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()

        if response.text:
            return response.json()
        return {}

    def list_available(
        self,
        gpu_type: str | None = None,
        min_gpu_ram: int | None = None,
        max_hourly_rate: float | None = None,
    ) -> list[dict]:
        """List available GPU offers."""
        query = {
            "verified": {"eq": True},
            "rentable": {"eq": True},
            "rented": {"eq": False},
            "type": "on-demand",  # or "interruptible" for spot
        }

        if gpu_type:
            query["gpu_name"] = {"eq": gpu_type}

        if min_gpu_ram:
            query["gpu_ram"] = {"gte": min_gpu_ram * 1024}  # Convert GB to MB

        if max_hourly_rate:
            query["dph_total"] = {"lte": max_hourly_rate}

        try:
            response = self._request(
                "GET",
                "bundles",
                params={"q": json.dumps(query)},
            )
        except Exception as e:
            print(f"Vast.ai API error: {e}")
            return []

        offers = response.get("offers", [])

        return [
            {
                "id": str(offer["id"]),
                "provider": self.name,
                "gpu_type": offer.get("gpu_name", "unknown"),
                "gpu_count": offer.get("num_gpus", 1),
                "gpu_ram_gb": offer.get("gpu_ram", 0) / 1024,
                "hourly_rate": offer.get("dph_total", 0),
                "cpu_cores": offer.get("cpu_cores", 0),
                "ram_gb": offer.get("cpu_ram", 0) / 1024,
                "disk_gb": offer.get("disk_space", 0),
                "reliability": offer.get("reliability", 0),
                "location": offer.get("geolocation", "unknown"),
                "raw": offer,
            }
            for offer in offers
        ]

    def launch(
        self,
        offer_id: str,
        docker_image: str,
        env_vars: dict | None = None,
        startup_script: str | None = None,
    ) -> Instance:
        """Launch a Vast.ai instance."""
        config = {
            "client_id": "me",
            "image": docker_image,
            "disk": 20,  # GB
            "runtype": "ssh",  # or "jupyter", "args"
        }

        if env_vars:
            config["env"] = env_vars

        if startup_script:
            config["onstart"] = startup_script

        response = self._request(
            "PUT",
            f"asks/{offer_id}/",
            json=config,
        )

        instance_id = str(response.get("new_contract"))

        instance = self._wait_for_ready(instance_id, timeout=300)
        return instance

    def _wait_for_ready(self, instance_id: str, timeout: int = 300) -> Instance:
        """Wait for instance to be running."""
        start = time.time()
        while time.time() - start < timeout:
            instance = self.get_instance(instance_id)
            if instance and instance.status == "running":
                return instance
            time.sleep(5)
        raise TimeoutError(f"Instance {instance_id} did not start within {timeout}s")

    def get_instance(self, instance_id: str) -> Instance | None:
        """Get instance details."""
        try:
            response = self._request("GET", "instances", params={"owner": "me"})
        except Exception:
            return None

        instances = response.get("instances", [])
        for inst in instances:
            if str(inst["id"]) == instance_id:
                return self._parse_instance(inst)
        return None

    def _parse_instance(self, data: dict) -> Instance:
        """Parse Vast.ai instance data to Instance object."""
        status_map = {
            "running": "running",
            "loading": "pending",
            "exited": "stopped",
        }

        return Instance(
            id=str(data["id"]),
            provider=self.name,
            gpu_type=data.get("gpu_name", "unknown"),
            gpu_count=data.get("num_gpus", 1),
            hourly_rate=data.get("dph_total", 0),
            status=status_map.get(data.get("actual_status", ""), "unknown"),
            ip_address=data.get("public_ipaddr"),
            ssh_port=data.get("ssh_port"),
            metadata={
                "ssh_host": data.get("ssh_host"),
                "machine_id": data.get("machine_id"),
                "start_date": data.get("start_date"),
            },
        )

    def terminate(self, instance_id: str) -> bool:
        """Terminate an instance."""
        try:
            self._request("DELETE", f"instances/{instance_id}/")
            return True
        except Exception as e:
            print(f"Failed to terminate {instance_id}: {e}")
            return False

    def list_instances(self) -> list[Instance]:
        """List all active instances."""
        try:
            response = self._request("GET", "instances", params={"owner": "me"})
        except Exception:
            return []

        return [
            self._parse_instance(inst)
            for inst in response.get("instances", [])
            if inst.get("actual_status") in ("running", "loading")
        ]

    def account_balance(self) -> dict:
        """Return Vast.ai balance and credit fields for the current user."""
        user = self._request("GET", "users/current/")
        return {
            "available": True,
            "currency": "USD",
            "balance": float(user.get("balance") or 0),
            "credit": float(user.get("credit") or 0),
        }


# Compatibility with the original concrete class name.
VastProvider = VastConnector
