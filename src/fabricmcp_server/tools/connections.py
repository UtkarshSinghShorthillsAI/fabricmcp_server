# This is a new file: src/fabricmcp_server/tools/connections.py

import logging
from typing import List, Optional

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from pydantic import Field

from ..fabric_models import ConnectionDetails, FabricApiException, FabricAuthException
from ..app import get_session_fabric_client

logger = logging.getLogger(__name__)

async def list_connections_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace to provide context. Connections are listed tenant-wide.")
) -> List[ConnectionDetails]:
    """
    Lists all connections accessible by the authenticated principal, providing comprehensive details for each.
    The list is tenant-wide but automatically scoped by the principal's permissions.
    """
    logger.info(f"Tool 'list_connections' called for workspace context {workspace_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        
        connections_data = await client.list_connections()
        
        if not connections_data:
            return []

        processed_connections = []
        for conn_data in connections_data:
            connection_id = conn_data.get("id")
            if not connection_id:
                logger.warning(f"Skipping a connection entry due to missing ID: {conn_data}")
                continue

            # This logic now correctly maps the raw API response to our snake_case model
            details = conn_data.get("connectionDetails", {}) or {}
            credentials = conn_data.get("credentialDetails", {}) or {}

            model_data = {
                "id": connection_id,
                "display_name": conn_data.get("displayName"),
                "connection_type": details.get("type"),
                "connectivity_type": conn_data.get("connectivityType"),
                "credential_type": credentials.get("credentialType"),
                "connection_path": details.get("path"),
                "privacy_level": conn_data.get("privacyLevel"),
                "allow_gateway_usage": conn_data.get("allowConnectionUsageInGateway"),
                "gateway_id": conn_data.get("gatewayId")
            }
            
            try:
                # The comprehensive model now validates the fully parsed data
                connection_model = ConnectionDetails.model_validate(model_data)
                processed_connections.append(connection_model)
            except Exception as e:
                logger.error(f"Pydantic validation failed for connection {connection_id}: {e}")
                continue
            
        return processed_connections

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to list connections: {e.response_text or str(e)}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in list_connections_impl: {e}")
        raise ToolError(f"An unexpected server error occurred: {e}")

def register_connection_tools(app: FastMCP):
    """Registers connection-related tools with the MCP app."""
    logger.info("Registering Fabric Connection tools...")
    app.tool(name="list_connections")(list_connections_impl)
    logger.info("Fabric Connection tools registration complete.")