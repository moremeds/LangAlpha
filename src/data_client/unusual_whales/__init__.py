"""Unusual Whales: options chains and short data for a self-hosted stack."""

from __future__ import annotations

import os

from .market_intel_source import UnusualWhalesIntelSource

_client: UnusualWhalesIntelSource | None = None


def get_uw_client() -> UnusualWhalesIntelSource | None:
    """The process's client when ``UW_API_KEY`` is set, else None.

    For the sandbox MCP servers, one process each; the backend builds its own
    through the registry.
    """
    global _client
    key = os.getenv("UW_API_KEY", "")
    # An unset ${UW_API_KEY} in mcp.json can arrive unexpanded.
    if _client is None and key and not key.startswith("${"):
        _client = UnusualWhalesIntelSource(key)
    return _client


async def close_uw_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None
