import logging
import uuid
import httpx
from typing import Optional, List, Dict, Any

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from pydantic import Field

from ..fabric_models import ItemEntity, CreateItemRequest, FabricApiException, FabricAuthException
from ..app import get_session_fabric_client, job_status_store

logger = logging.getLogger(__name__)

def _process_fabric_response(resp: httpx.Response):
    """
    Return the raw body (or headers if body is empty) for any 2xx response.
    For 4xx/5xx, raise ToolError with the raw body so the caller sees exactly
    what Fabric returned.
    """
    if resp.status_code >= 400:
        raise ToolError(resp.text or f"HTTP {resp.status_code}")
    return resp.text if resp.text else dict(resp.headers)


async def list_fabric_items_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace.")
) -> Optional[List[ItemEntity]]:
    """Lists all items (e.g., 'Lakehouse', 'Warehouse', 'DataPipeline', 'Notebook', 'Dataflow', any other Fabric item) in a specified Fabric workspace."""
    logger.info(f"Tool 'list_fabric_items' called for workspace {workspace_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        items = await client.list_items(workspace_id=workspace_id)
        logger.info(f"Found {len(items) if items else 0} items in workspace {workspace_id}.")
        return items
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to list Fabric items: {e}") from e
    except Exception as e:
        raise ToolError(f"An unexpected error occurred: {str(e)}") from e

async def get_fabric_item_details_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace."),
    item_id: str = Field(..., description="The ID of the Fabric item to retrieve.")
) -> Optional[ItemEntity]:
    """Retrieves the details of a specific Fabric item by its ID."""
    logger.info(f"Tool 'get_fabric_item_details' called for item {item_id} in workspace {workspace_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        item = await client.get_item(workspace_id=workspace_id, item_id=item_id)
        if not item:
            logger.warning(f"Item {item_id} not found in workspace {workspace_id}.")
        return item
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to get Fabric item details: {e}") from e

async def create_fabric_item_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace where the item will be created."),
    display_name: str = Field(..., description="The display name for the new item."),
    item_type: str = Field(..., description="The type of simple item to create (e.g., 'Lakehouse', 'Warehouse', 'DataPipeline', 'Notebook', 'Dataflow', any other Fabric item). For complex items like Notebooks, use the dedicated tool."),
    description: Optional[str] = Field(None, description="An optional description for the new item."),
) -> ItemEntity:
    """Creates a simple new item in a specified Fabric workspace. Does not support complex definitions."""
    logger.info(f"Tool 'create_fabric_item' called to create '{display_name}' ({item_type}) in workspace {workspace_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        payload = CreateItemRequest(displayName=display_name, type=item_type, description=description)
        response = await client.create_item(workspace_id=workspace_id, payload=payload)
        
        if isinstance(response, ItemEntity):
            logger.info(f"Successfully created item with ID: {response.id}")
            return response.model_dump(by_alias=True)

        if isinstance(response, httpx.Response):
            return _process_fabric_response(response)

        raise ToolError(str(response))

        
        raise ToolError(f"Unexpected response type from API: {type(response)}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to create Fabric item: {e}") from e

async def delete_fabric_item_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace."),
    item_id: str = Field(..., description="The ID of the Fabric item to delete.")
) -> Dict[str, Any]:
    """Deletes a specific Fabric item by its ID. This can be a long-running operation."""
    logger.info(f"Tool 'delete_fabric_item' called for item {item_id} in workspace {workspace_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        response = await client.delete_item(workspace_id=workspace_id, item_id=item_id)

        if isinstance(response, httpx.Response):
            if response.status_code == 202:
                operation_url = response.headers.get("Operation-Location")
                if not operation_url:
                     return { "status": "Accepted (Untrackable)", "message": "Deletion initiated."}
                
                job_id = str(uuid.uuid4())
                job_status_store[job_id] = operation_url
                return { "status": "Accepted", "job_id": job_id, "message": "Deletion initiated. Use 'get_operation_status' to check progress."}
            
            elif response.status_code in (200, 204):
                return {"status": "Succeeded", "message": f"Successfully deleted item {item_id}."}
        
        raise ToolError(f"Unexpected response from delete operation: {type(response)}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to delete Fabric item: {e}") from e

async def get_operation_status_impl(
    ctx: Context,
    job_id: str = Field(..., description="The job ID returned from a long-running operation.")
) -> Dict[str, Any]:
    """Checks the status of a long-running operation (like item creation, deletion, or a job run)."""
    logger.info(f"Tool 'get_operation_status' called for job ID {job_id}.")
    operation_url = job_status_store.get(job_id)
    if not operation_url:
        raise ToolError(f"Job ID '{job_id}' not found or has expired.")
    try:
        client = await get_session_fabric_client(ctx)
        response = await client.poll_lro_status(operation_url)
        poll_data = response.json()
        status = poll_data.get("status")

        if status in ("Succeeded", "Failed", "Canceled"):
            job_status_store.pop(job_id, None)
        
        return poll_data

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to get operation status for job {job_id}: {e}") from e

def register_item_tools(app: FastMCP):
    logger.info("Registering Fabric Item tools...")
    app.tool(name="list_fabric_items")(list_fabric_items_impl)
    app.tool(name="get_fabric_item_details")(get_fabric_item_details_impl)
    app.tool(name="create_fabric_item")(create_fabric_item_impl)
    app.tool(name="delete_fabric_item")(delete_fabric_item_impl)
    app.tool(name="get_operation_status")(get_operation_status_impl)
    logger.info("Fabric Item tools registration complete.")