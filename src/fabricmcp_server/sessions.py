# In src/fabricmcp_server/session.py (NEW FILE)

from __future__ import annotations

import asyncio
from typing import Dict

from .fabric_api_client import FabricApiClient, FabricApiException
from fastmcp import Context

# In-memory store for long-running operation status URLs
# Key: job_id (str), Value: status_url (str)
job_status_store: Dict[str, str] = {}

# Cache for active Fabric API clients, keyed by session object ID
_active_clients: Dict[str, FabricApiClient] = {}
_client_creation_locks: Dict[str, asyncio.Lock] = {}
_global_lock = asyncio.Lock()

async def _get_lock(session_id: str) -> asyncio.Lock:
    async with _global_lock:
        return _client_creation_locks.setdefault(session_id, asyncio.Lock())

async def get_session_fabric_client(ctx: Context) -> FabricApiClient:
    session_id = str(id(ctx.session))
    
    if client := _active_clients.get(session_id):
        return client

    creation_lock = await _get_lock(session_id)
    async with creation_lock:
        if client := _active_clients.get(session_id):
            return client

        # Creating the client must happen here, inside the lock
        base_url = "https://api.fabric.microsoft.com"
        client = await FabricApiClient.create(base_url)
        _active_clients[session_id] = client
        return client