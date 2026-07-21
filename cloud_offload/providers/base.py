"""Base interface for pluggable cloud compute connectors."""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Instance:
    """A cloud GPU instance."""

    id: str
    provider: str
    gpu_type: str
    gpu_count: int
    hourly_rate: float
    status: str  # pending, running, stopped, terminated
    ip_address: str | None = None
    ssh_port: int | None = None
    metadata: dict = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class CloudConnector(ABC):
    """Abstract connector for a cloud GPU compute service."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name."""

    @abstractmethod
    def list_available(
        self,
        gpu_type: str | None = None,
        min_gpu_ram: int | None = None,
        max_hourly_rate: float | None = None,
    ) -> list[dict]:
        """List available instances/offers. Returns list of offer dicts with pricing info."""

    @abstractmethod
    def launch(
        self,
        offer_id: str,
        docker_image: str,
        env_vars: dict | None = None,
        startup_script: str | None = None,
    ) -> Instance:
        """Launch an instance with the given offer. Returns Instance object."""

    @abstractmethod
    def get_instance(self, instance_id: str) -> Instance | None:
        """Get instance by ID."""

    @abstractmethod
    def terminate(self, instance_id: str) -> bool:
        """Terminate an instance."""

    @abstractmethod
    def list_instances(self) -> list[Instance]:
        """List all active instances for this provider."""

    def find_cheapest(
        self,
        gpu_type: str | None = None,
        min_gpu_ram: int = 24,
        max_hourly_rate: float = 1.0,
    ) -> dict | None:
        """Find the cheapest available offer matching criteria."""
        offers = self.list_available(
            gpu_type=gpu_type,
            min_gpu_ram=min_gpu_ram,
            max_hourly_rate=max_hourly_rate,
        )
        if not offers:
            return None
        return min(offers, key=lambda x: x.get("hourly_rate", float("inf")))

    def account_balance(self) -> dict:
        """Return normalized account credit data when supported."""
        return {"available": False, "currency": "USD"}


# Backwards compatibility for code that implemented the original provider API.
CloudProvider = CloudConnector
