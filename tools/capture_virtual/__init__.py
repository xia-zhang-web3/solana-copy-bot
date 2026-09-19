"""Observation-only capture admission for a separately owned virtual ledger."""

from .adapter import CaptureVirtualAdapter
from .control import CaptureControl

__all__ = ["CaptureControl", "CaptureVirtualAdapter"]
