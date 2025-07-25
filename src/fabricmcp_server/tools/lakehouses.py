import logging
import uuid
import httpx
from typing import Optional, List, Dict, Any

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from pydantic import Field

from ..fabric_models import LoadTableRequest, FabricApiException, FabricAuthException
from ..app import get_session_fabric_client, job_status_store

logger = logging.getLogger(__name__)

async def upload_file_to_lakehouse_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace."),
    lakehouse_id: str = Field(..., description="The ID of the target Lakehouse."),
    local_file_path: str = Field(..., description="The local path to the file to upload (e.g., './data/sales.csv')."),
    target_path_in_lakehouse: str = Field(..., description="The target path within the Lakehouse, including the filename (e.g., 'Files/raw_data/sales.csv').")
) -> Dict[str, Any]:
    """Uploads a local file from the server's machine to a specified path in a Fabric Lakehouse."""
    logger.info(f"Tool 'upload_file_to_lakehouse' called for '{local_file_path}'.")
    try:
        client = await get_session_fabric_client(ctx)
        success = await client.upload_file_chunked(
            workspace_id, lakehouse_id, local_file_path, target_path_in_lakehouse
        )
        if success:
            return {"status": "Succeeded", "message": f"File '{local_file_path}' uploaded to '{target_path_in_lakehouse}'."}
        else:
            raise ToolError("File upload failed for an unknown reason.")
            
    except FileNotFoundError:
        raise ToolError(f"Local file not found at path: {local_file_path}")
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to upload file: {e.response_text or str(e)}") from e

async def create_table_from_file_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace."),
    lakehouse_id: str = Field(..., description="The ID of the Lakehouse containing the file."),
    table_name: str = Field(..., description="The name of the new Delta table to be created."),
    file_path_in_lakehouse: str = Field(..., description="The path to the source file within the Lakehouse (e.g., 'Files/raw_data/sales.csv').")
) -> Dict[str, Any]:
    """Creates a Delta table from a file that already exists inside a Lakehouse. This is a long-running operation."""
    logger.info(f"Tool 'create_table_from_file' called for table '{table_name}'.")
    try:
        client = await get_session_fabric_client(ctx)
        payload = LoadTableRequest(relativePath=file_path_in_lakehouse)
        
        response = await client.load_table(workspace_id, lakehouse_id, table_name, payload)

        if isinstance(response, httpx.Response) and response.status_code == 202:
            operation_url = response.headers.get("Location")
            if not operation_url:
                return {"status": "Accepted (Untrackable)", "message": "Table load operation initiated."}
            
            job_id = str(uuid.uuid4())
            job_status_store[job_id] = operation_url
            return {"status": "Accepted", "job_id": job_id, "message": "Table load in progress. Use 'get_operation_status' to check."}

        raise ToolError(f"Unexpected API response. Status: {getattr(response, 'status_code', 'N/A')}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to create table: {e.response_text or str(e)}") from e

def register_lakehouse_tools(app: FastMCP):
    """Registers all tools related to Fabric Lakehouse operations."""
    logger.info("Registering Fabric Lakehouse tools...")
    app.tool(name="upload_file_to_lakehouse")(upload_file_to_lakehouse_impl)
    app.tool(name="create_table_from_file")(create_table_from_file_impl)
    logger.info("Fabric Lakehouse tools registration complete.")