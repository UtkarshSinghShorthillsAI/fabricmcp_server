"""
Microsoft Fabric Connection Manager Tool - Simplified Version

This module provides a basic tool for listing existing connections in Microsoft Fabric workspaces.
Starting simple and focusing on one working function before adding complexity.

Author: AI Assistant
Date: 2025-08-01
"""

import logging
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

from fastmcp import FastMCP, Context
from ..sessions import get_session_fabric_client

logger = logging.getLogger(__name__)

# =============================================================================
# SIMPLE CONNECTION MODELS
# =============================================================================

class ConnectionSummary(BaseModel):
    """Basic connection information from the API"""
    id: str = Field(..., description="Connection ID")
    display_name: str = Field(..., description="Connection display name") 
    connection_type: str = Field(..., description="Type of connection")
    created_date: Optional[str] = Field(None, description="Creation date")
    gateway_id: Optional[str] = Field(None, description="Gateway ID if using on-premises gateway")

# =============================================================================
# MAIN CONNECTION LISTING FUNCTION
# =============================================================================

async def list_workspace_connections_impl(
    ctx: Context,
    workspace_id: str,
    include_details: bool = False
) -> Dict[str, Any]:
    """
    List all connections accessible from a workspace
    
    Args:
        workspace_id: Target workspace ID  
        include_details: Whether to include detailed connection information
    
    Returns:
        Dictionary containing list of connections and metadata
    """
    
    try:
        client = await get_session_fabric_client(ctx)
        logger.info(f"Listing connections for workspace: {workspace_id}")
        
        # Try the tenant-level connections API first (as per the blog post)
        logger.info("Attempting to get tenant-level connections...")
        connections_response = await client.get_connections()
        
        if not connections_response:
            logger.info("No tenant connections found, trying workspace-specific endpoint...")
            # Try workspace-specific endpoint if available
            connections_response = await client.get_workspace_connections(workspace_id)
        
        if not connections_response:
            return {
                "workspace_id": workspace_id,
                "connections": [],
                "total_count": 0,
                "message": "No connections found or API returned no data",
                "api_endpoints_tried": ["/v1/connections", f"/v1/workspaces/{workspace_id}/connections"]
            }
        
        # Handle different response structures
        connections_data = []
        if isinstance(connections_response, dict):
            if "value" in connections_response:
                connections_data = connections_response["value"]
            elif "connections" in connections_response:
                connections_data = connections_response["connections"]
            elif isinstance(connections_response.get("data"), list):
                connections_data = connections_response["data"]
            else:
                # Response might be a direct list or single connection
                if isinstance(connections_response, list):
                    connections_data = connections_response
                else:
                    connections_data = [connections_response]
        
        logger.info(f"Found {len(connections_data)} connections in API response")
        
        # Process connections data
        processed_connections = []
        
        for conn_data in connections_data:
            try:
                # Handle the actual API response structure based on what we discovered
                connection_id = conn_data.get("id")
                display_name = conn_data.get("displayName", "Unknown")
                
                # Connection type is in connectionDetails.type
                connection_details = conn_data.get("connectionDetails", {})
                connection_type = connection_details.get("type", "Unknown")
                
                # Additional useful fields from the actual API
                connectivity_type = conn_data.get("connectivityType", "Unknown")
                credential_type = conn_data.get("credentialDetails", {}).get("credentialType", "Unknown")
                connection_path = connection_details.get("path", "")
                
                if not connection_id:
                    logger.warning(f"Skipping connection with no ID: {conn_data}")
                    continue
                
                if include_details:
                    # Include all available fields when details requested
                    processed_connection = {
                        "id": connection_id,
                        "display_name": display_name,
                        "connection_type": connection_type,
                        "connectivity_type": connectivity_type,
                        "credential_type": credential_type,
                        "connection_path": connection_path,
                        "privacy_level": conn_data.get("privacyLevel", "Unknown"),
                        "allow_gateway_usage": conn_data.get("allowConnectionUsageInGateway", False),
                        "raw_data": conn_data  # Include raw data for debugging
                    }
                else:
                    # Basic summary only
                    processed_connection = ConnectionSummary(
                        id=connection_id,
                        display_name=display_name,
                        connection_type=connection_type,
                        created_date=None,  # API doesn't provide dates
                        gateway_id=None  # This might be in a different endpoint
                    ).model_dump()
                
                processed_connections.append(processed_connection)
                
            except Exception as e:
                logger.error(f"Error processing connection data {conn_data}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(processed_connections)} connections")
        
        return {
            "workspace_id": workspace_id,
            "connections": processed_connections,
            "total_count": len(processed_connections),
            "include_details": include_details,
            "raw_response_structure": {
                "response_type": type(connections_response).__name__,
                "has_value_key": "value" in connections_response if isinstance(connections_response, dict) else False,
                "top_level_keys": list(connections_response.keys()) if isinstance(connections_response, dict) else "Not a dict"
            }
        }
        
    except Exception as e:
        logger.error(f"Error listing workspace connections: {str(e)}")
        return {
            "error": f"Failed to list connections: {str(e)}",
            "workspace_id": workspace_id,
            "connections": [],
            "error_type": type(e).__name__
        }

# =============================================================================
# TOOL REGISTRATION
# =============================================================================

def register_connection_manager_tools(app: FastMCP):
    """Register connection manager tools with the MCP app"""
    logger.info("Registering Connection Manager tools...")
    
    # Register only the basic list connections tool for now
    app.tool(name="list_workspace_connections")(list_workspace_connections_impl)
    
    logger.info("Connection Manager tools registration complete.") 