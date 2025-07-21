from __future__ import annotations

import asyncio
import logging
import os
import sys
import argparse
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict

import dotenv
import uvicorn
from azure.identity.aio import DefaultAzureCredential
from cachetools import TTLCache
from fastmcp import Context, FastMCP

from .fabric_api_client import FabricApiClient, FabricApiException, FabricAuthException

dotenv.load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
log_format = '%(asctime)s %(levelname)-8s %(name)s | %(message)s'
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format=log_format, stream=sys.stderr, force=True)
logger = logging.getLogger("fabricmcp_server.app")
# In-memory store for long-running operation status URLs
# Key: job_id (str), Value: status_url (str)
job_status_store: Dict[str, str] = {}

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

        logger.info(f"Creating new FabricApiClient for session {session_id}.")
        base_url = os.getenv("FABRIC_API_BASE_URL", "https://api.fabric.microsoft.com")
        try:
            client = await FabricApiClient.create(base_url)
            _active_clients[session_id] = client
            return client
        except (FabricAuthException, FabricApiException) as e:
            logger.error(f"Failed to create FabricApiClient for session {session_id}: {e}")
            raise

@asynccontextmanager
async def app_lifespan(app: FastMCP) -> AsyncIterator[None]:
    logger.info("FabricMCP Server starting up.")
    yield
    logger.info(f"FabricMCP Server shutting down. Closing {_active_clients.__len__()} clients.")
    await asyncio.gather(*(client.close() for client in _active_clients.values()), return_exceptions=True)
    _active_clients.clear()
    logger.info("All active Fabric API clients closed.")

mcp_app = FastMCP(
    name="FabricMCP Server",
    instructions="This server exposes tools for Microsoft Fabric.",
    dependencies=["httpx", "azure-identity"],
    lifespan=app_lifespan,
)

def register_tools() -> None:
    logger.info("Registering tools...")
    try:
        from .tools import items
        items.register_item_tools(mcp_app)
        logger.info("Successfully registered 'items' tools.")
    except Exception as exc:
        logger.exception(f"Error during tool registration: {exc}")

if __name__ == "__main__":
    register_tools()
    parser = argparse.ArgumentParser(description="Run FabricMCP Server with Streamable HTTP transport.")
    parser.add_argument("--port", type=int, default=8081, help="Port to listen on.")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to.")
    args = parser.parse_args()

    logger.info(f"Starting MCP server via HTTP on {args.host}:{args.port}...")
    uvicorn.run(mcp_app.streamable_http_app, host=args.host, port=args.port)