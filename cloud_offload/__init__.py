"""Cloud Offload - provider-neutral cloud offload coordinator for ComfyUI.

A standalone FastAPI coordinator + SQLite queue + dispatcher + worker + provider
connectors + storage + partition protocol + headless-ComfyUI executor. It never
loads a model: generation rides inside the submitted ComfyUI subgraph.
"""

__version__ = "0.1.0"
