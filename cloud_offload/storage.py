"""Storage abstraction for job inputs/outputs and partition artifacts."""

import shutil
from abc import ABC, abstractmethod
from pathlib import Path


class Storage(ABC):
    """Abstract storage interface."""

    @abstractmethod
    def upload(self, local_path: str | Path, remote_key: str) -> str:
        """Upload file to storage. Returns the URI/path to access it."""

    @abstractmethod
    def download(self, remote_key: str, local_path: str | Path) -> Path:
        """Download file from storage. Returns local path."""

    @abstractmethod
    def exists(self, remote_key: str) -> bool:
        """Check if file exists."""

    @abstractmethod
    def delete(self, remote_key: str) -> bool:
        """Delete file from storage."""

    @abstractmethod
    def get_uri(self, remote_key: str) -> str:
        """Get URI for a stored file."""


class LocalStorage(Storage):
    """Local filesystem storage (for development/single-machine use)."""

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _resolve(self, remote_key: str) -> Path:
        return self.base_path / remote_key

    def upload(self, local_path: str | Path, remote_key: str) -> str:
        local_path = Path(local_path)
        dest = self._resolve(remote_key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)
        return self.get_uri(remote_key)

    def download(self, remote_key: str, local_path: str | Path) -> Path:
        local_path = Path(local_path)
        src = self._resolve(remote_key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, local_path)
        return local_path

    def exists(self, remote_key: str) -> bool:
        return self._resolve(remote_key).exists()

    def delete(self, remote_key: str) -> bool:
        path = self._resolve(remote_key)
        if path.exists():
            path.unlink()
            return True
        return False

    def get_uri(self, remote_key: str) -> str:
        return f"file://{self._resolve(remote_key).absolute()}"


class GCSStorage(Storage):
    """Google Cloud Storage backend."""

    def __init__(self, bucket_name: str, prefix: str = "cloud-offload"):
        try:
            from google.cloud import storage
        except ImportError:
            raise ImportError("google-cloud-storage required: pip install google-cloud-storage")

        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.prefix = prefix

    def _key(self, remote_key: str) -> str:
        return f"{self.prefix}/{remote_key}" if self.prefix else remote_key

    def upload(self, local_path: str | Path, remote_key: str) -> str:
        local_path = Path(local_path)
        blob = self.bucket.blob(self._key(remote_key))
        blob.upload_from_filename(str(local_path))
        return self.get_uri(remote_key)

    def download(self, remote_key: str, local_path: str | Path) -> Path:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob = self.bucket.blob(self._key(remote_key))
        blob.download_to_filename(str(local_path))
        return local_path

    def exists(self, remote_key: str) -> bool:
        blob = self.bucket.blob(self._key(remote_key))
        return blob.exists()

    def delete(self, remote_key: str) -> bool:
        blob = self.bucket.blob(self._key(remote_key))
        if blob.exists():
            blob.delete()
            return True
        return False

    def get_uri(self, remote_key: str) -> str:
        return f"gs://{self.bucket.name}/{self._key(remote_key)}"


class S3Storage(Storage):
    """AWS S3 storage backend."""

    def __init__(self, bucket_name: str, prefix: str = "cloud-offload", region: str = "us-east-1"):
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required: pip install boto3")

        self.s3 = boto3.client("s3", region_name=region)
        self.bucket_name = bucket_name
        self.prefix = prefix

    def _key(self, remote_key: str) -> str:
        return f"{self.prefix}/{remote_key}" if self.prefix else remote_key

    def upload(self, local_path: str | Path, remote_key: str) -> str:
        local_path = Path(local_path)
        self.s3.upload_file(str(local_path), self.bucket_name, self._key(remote_key))
        return self.get_uri(remote_key)

    def download(self, remote_key: str, local_path: str | Path) -> Path:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(self.bucket_name, self._key(remote_key), str(local_path))
        return local_path

    def exists(self, remote_key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=self._key(remote_key))
            return True
        except Exception:
            return False

    def delete(self, remote_key: str) -> bool:
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=self._key(remote_key))
            return True
        except Exception:
            return False

    def get_uri(self, remote_key: str) -> str:
        return f"s3://{self.bucket_name}/{self._key(remote_key)}"


def create_storage(config) -> Storage:
    """Factory function to create storage from config."""
    if config.storage_type == "local":
        return LocalStorage(config.storage_path)
    elif config.storage_type == "gcs":
        return GCSStorage(config.storage_path)
    elif config.storage_type == "s3":
        return S3Storage(config.storage_path)
    else:
        raise ValueError(f"Unknown storage type: {config.storage_type}")
