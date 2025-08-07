import logging
from typing import List, Dict, Any

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from pydantic import Field

from ..fabric_models import FabricApiException, FabricAuthException
from ..app import get_session_fabric_client

logger = logging.getLogger(__name__)

async def list_datasets_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace to search for datasets.")
) -> List[Dict[str, Any]]:
    """
    Lists all datasets (Semantic Models) in a specified Fabric workspace.
    This tool returns the raw API response for initial inspection.
    """
    logger.info(f"Tool 'list_datasets' called for workspace {workspace_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        
        # Call the new client method and return the raw list of dictionaries
        datasets_data = await client.list_datasets(workspace_id=workspace_id)
        
        if datasets_data is None:
            return []
            
        return datasets_data

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to list datasets: {e.response_text or str(e)}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in list_datasets_impl: {e}")
        raise ToolError(f"An unexpected server error occurred: {e}")


def register_dataset_tools(app: FastMCP):
    """Registers dataset-related tools with the MCP app."""
    logger.info("Registering Fabric Dataset tools...")
    app.tool(name="list_datasets")(list_datasets_impl)
    logger.info("Fabric Dataset tools registration complete.")