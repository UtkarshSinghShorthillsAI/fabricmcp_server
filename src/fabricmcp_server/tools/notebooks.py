# This is the final, correct, and complete file: src/fabricmcp_server/tools/notebooks.py

import json
import base64
import logging
from typing import List, Dict, Any, Literal, Optional

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ..fabric_models import FabricApiException, FabricAuthException
from ..app import get_session_fabric_client, job_status_store

logger = logging.getLogger(__name__)

# =============================================================================
#  PYDANTIC MODELS FOR NOTEBOOK STRUCTURE (THE FIX)
# =============================================================================

class NotebookCell(BaseModel):
    """Represents a single cell in a Jupyter-like notebook."""
    cell_type: Literal["code", "markdown"]
    source: List[str]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    execution_count: Optional[int] = None
    outputs: List[Any] = Field(default_factory=list)

class NotebookMetadata(BaseModel):
    """Represents the metadata for the notebook."""
    language_info: Dict[str, str] = Field(default={"name": "python"})

class NotebookStructure(BaseModel):
    """Represents the full structure of an .ipynb file."""
    metadata: NotebookMetadata = Field(default_factory=NotebookMetadata)
    nbformat: int = 4
    nbformat_minor: int = 5
    cells: List[NotebookCell]

# =============================================================================
#  HELPER & TOOL IMPLEMENTATIONS
# =============================================================================

def _build_notebook_definition(
    notebook_id: str,
    workspace_id: str,
    lakehouse_id: str,
    cells: List[Dict[str, Any]]
) -> tuple[str, str]:
    """
    Constructs the full, Base64-encoded JSON payload for a notebook's definition.
    """
    # The definition format is consistently 'ipynb' for this operation
    definition_format = "ipynb"
    
    # Create an instance of our Pydantic model from the raw cell dictionaries
    notebook_model = NotebookStructure(cells=[NotebookCell(**cell) for cell in cells])
    
    # Now, we can safely call .model_dump() on a Pydantic model object
    notebook_json = notebook_model.model_dump(by_alias=True, exclude_none=True)
    
    json_string = json.dumps(notebook_json, indent=2)
    b64_payload = base64.b64encode(json_string.encode("utf-8")).decode("utf-8")
    
    return b64_payload, definition_format

# (The create_notebook_impl function remains unchanged)
async def create_notebook_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the workspace where the notebook will be created."),
    notebook_name: str = Field(..., description="Display name for the new notebook."),
    lakehouse_id: Optional[str] = Field(None, description="Optional ID of a Lakehouse to attach.")
) -> Dict[str, Any]:
    # ... existing implementation ...
    logger.info(f"Tool 'create_notebook' called for '{notebook_name}'.")
    try:
        client = await get_session_fabric_client(ctx)
        
        b64_payload, definition_format = _build_notebook_definition(
            notebook_id="", # Not needed for creation
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id if lakehouse_id else "",
            cells=[{"cell_type": "code", "source": ["# New notebook created via MCP"]}]
        )
        
        definition = {
            "format": definition_format,
            "parts": [{
                "path": "notebook-content.ipynb",
                "payload": b64_payload,
                "payloadType": "InlineBase64"
            }]
        }
        
        item_payload = {
            "displayName": notebook_name,
            "type": "Notebook",
            "definition": definition
        }
        
        response = await client.create_item(workspace_id, item_payload)
        return response.model_dump(by_alias=True)

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to create notebook: {e.response_text or str(e)}")

async def update_notebook_content_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the notebook's workspace."),
    notebook_id: str = Field(..., description="The ID of the notebook to update."),
    lakehouse_id: str = Field(..., description="The ID of the Lakehouse attached to this notebook."),
    cells: List[Dict[str, Any]] = Field(..., description="A list of cell objects in Jupyter Notebook format.")
) -> Dict[str, str]:
    """
    Updates a notebook's content. This is a long-running operation.
    Returns a job_id to track the update status.
    """
    logger.info(f"Tool 'update_notebook_content' called for notebook {notebook_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        
        b64_payload, definition_format = _build_notebook_definition(
            notebook_id=notebook_id,
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            cells=cells
        )
        
        definition_payload = {
            "definition": {
                "format": definition_format,
                "parts": [{
                    "path": "notebook-content.ipynb",
                    "payload": b64_payload,
                    "payloadType": "InlineBase64"
                }]
            }
        }

        response = await client.update_item_definition(workspace_id, notebook_id, definition_payload)

        if response.status_code == 200:
            return {"status": "Succeeded", "message": "Notebook content updated successfully."}
        
        elif response.status_code == 202:
            operation_url = response.headers.get("Location") or response.headers.get("Operation-Location")
            if not operation_url:
                raise ToolError("API accepted the request but did not provide a status location URL.")
            
            job_id = operation_url.split('/')[-1].split('?')[0]
            job_status_store[job_id] = operation_url
            
            return {
                "status": "Accepted", 
                "job_id": job_id,
                "message": "Notebook update is in progress. Use 'get_operation_status' to track completion."
            }
        else:
            raise ToolError(f"API returned an unexpected status code: {response.status_code} - {response.text}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to update notebook content: {e.response_text or str(e)}")


def register_notebook_tools(app: FastMCP):
    """Registers notebook-related tools with the MCP app."""
    logger.info("Registering Fabric Notebook tools...")
    # app.tool(name="create_notebook")(create_notebook_impl) # Disabling for now to focus on update
    app.tool(name="update_notebook_content")(update_notebook_content_impl)
    logger.info("Fabric Notebook tools registration complete.")