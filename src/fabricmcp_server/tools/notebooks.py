import logging
import json
import base64
import httpx
import uuid
from typing import Optional, List, Dict, Any

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from pydantic import Field

from ..fabric_models import (
    ItemEntity, CreateItemRequest, DefinitionPart, ItemDefinitionForCreate,
    UpdateItemDefinitionRequest, NotebookCell, FabricApiException, FabricAuthException
)
from ..app import get_session_fabric_client, job_status_store

logger = logging.getLogger(__name__)

def _build_notebook_definition(cells: List[NotebookCell], lakehouse_id: str, workspace_id: str) -> str:
    """Helper to construct and encode the notebook's JSON definition."""
    notebook_struct = {
        "nbformat": 4, "nbformat_minor": 5,
        "cells": [cell.model_dump(by_alias=True, exclude_none=True) for cell in cells],
        "metadata": {
            "language_info": {"name": "python"},
            "dependencies": {
                "lakehouse": {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_workspace_id": workspace_id
                }
            }
        }
    }
    notebook_json = json.dumps(notebook_struct)
    return base64.b64encode(notebook_json.encode('utf-8')).decode('utf-8')

async def create_notebook_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the workspace where the notebook will be created."),
    notebook_name: str = Field(..., description="The desired display name for the new notebook."),
    lakehouse_id: str = Field(..., description="The ID of the Lakehouse to attach to this notebook by default."),
    description: Optional[str] = Field(None, description="An optional description for the new notebook.")
) -> Dict[str, Any]:
    """Creates a new notebook. Handles both immediate and long-running creation."""
    # This implementation is correct and remains unchanged.
    logger.info(f"Tool 'create_notebook' called for '{notebook_name}'.")
    try:
        client = await get_session_fabric_client(ctx)
        initial_cells = [NotebookCell(cell_type="markdown", source=[f"# {notebook_name}"])]
        b64_payload = _build_notebook_definition(initial_cells, lakehouse_id, workspace_id)
        definition = ItemDefinitionForCreate(format="ipynb", parts=[DefinitionPart(path="notebook-content.ipynb", payload=b64_payload, payloadType="InlineBase64")])
        create_payload = CreateItemRequest(displayName=notebook_name, type="Notebook", description=description, definition=definition)
        response = await client.create_item(workspace_id, create_payload)
        if isinstance(response, ItemEntity):
            return {"status": "Succeeded", "message": "Notebook created immediately.", "result": response.model_dump(by_alias=True)}
        if isinstance(response, httpx.Response) and response.status_code == 202:
            operation_url = response.headers.get("Operation-Location")
            if not operation_url: return {"status": "Accepted (Untrackable)"}
            job_id = str(uuid.uuid4())
            job_status_store[job_id] = operation_url
            return {"status": "Accepted", "job_id": job_id, "message": "Notebook creation in progress."}
        raise ToolError(f"Unexpected API response. Status: {getattr(response, 'status_code', 'N/A')}")
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to create notebook: {e.response_text or str(e)}") from e

async def update_notebook_content_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the workspace containing the notebook."),
    notebook_id: str = Field(..., description="The ID of the notebook to update."),
    lakehouse_id: str = Field(..., description="The ID of the default Lakehouse currently attached to the notebook."),
    cells: List[NotebookCell] = Field(..., description="A list of cell objects defining the new content of the notebook.")
) -> Dict[str, Any]:
    """Updates a notebook's content. Requires the attached Lakehouse ID. This is a long-running operation."""
    # This implementation is correct and remains unchanged.
    logger.info(f"Tool 'update_notebook_content' called for notebook '{notebook_id}'.")
    try:
        client = await get_session_fabric_client(ctx)
        b64_payload = _build_notebook_definition(cells, lakehouse_id, workspace_id)
        definition = ItemDefinitionForCreate(format="ipynb", parts=[DefinitionPart(path="notebook-content.ipynb", payload=b64_payload, payloadType="InlineBase64")])
        update_payload = UpdateItemDefinitionRequest(definition=definition)
        response = await client.update_item_definition(workspace_id, notebook_id, update_payload)
        if isinstance(response, httpx.Response) and response.status_code == 202:
            operation_url = response.headers.get("Operation-Location")
            if not operation_url: return {"status": "Accepted (Untrackable)"}
            job_id = str(uuid.uuid4())
            job_status_store[job_id] = operation_url
            return {"status": "Accepted", "job_id": job_id, "message": "Notebook update in progress."}
        raise ToolError(f"Unexpected response. Status: {getattr(response, 'status_code', 'N/A')}")
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to update notebook: {e.response_text or str(e)}") from e
        
async def run_notebook_impl(
    ctx: Context, 
    workspace_id: str = Field(..., description="The ID of the workspace containing the notebook."),
    notebook_id: str = Field(..., description="The ID of the notebook to execute.")
) -> Dict[str, Any]:
    """Triggers the execution of a Fabric notebook. This is a long-running operation."""
    logger.info(f"Tool 'run_notebook' called for notebook '{notebook_id}'.")
    try:
        client = await get_session_fabric_client(ctx)
        response = await client.run_item(workspace_id, notebook_id, "RunNotebook")

        if isinstance(response, httpx.Response) and response.status_code == 202:
            operation_url = response.headers.get("Location")
            if not operation_url: raise ToolError("API did not provide a status location.")
            
            # --- THIS IS THE FIX ---
            run_id = operation_url.split('/')[-1] # The run_id is the job instance ID
            job_status_store[run_id] = operation_url # Store the full URL for polling
            return {"status": "Accepted", "run_id": run_id, "message": "Notebook execution started. Use 'get_job_run_status' to monitor."}

        raise ToolError(f"Unexpected response from API. Status: {getattr(response, 'status_code', 'N/A')}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to run notebook: {e.response_text or str(e)}") from e
    
async def delete_notebook_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace."),
    notebook_id: str = Field(..., description="The ID of the Notebook to delete.")
) -> Dict[str, Any]:
    """Deletes a specific Notebook by its ID. This is a long-running operation."""
    # This implementation is correct and remains unchanged.
    logger.info(f"Tool 'delete_notebook' called for notebook {notebook_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        response = await client.delete_item(workspace_id=workspace_id, item_id=notebook_id)
        if isinstance(response, httpx.Response):
            if response.status_code == 202:
                operation_url = response.headers.get("Operation-Location")
                if not operation_url: return { "status": "Accepted (Untrackable)"}
                job_id = str(uuid.uuid4())
                job_status_store[job_id] = operation_url
                return { "status": "Accepted", "job_id": job_id, "message": "Deletion initiated."}
            elif response.status_code in (200, 204):
                return {"status": "Succeeded", "message": f"Successfully deleted notebook {notebook_id}."}
        raise ToolError(f"Unexpected response from delete operation: {type(response)}")
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to delete notebook: {e.response_text or str(e)}") from e

# --- NEW FUNCTION ---
async def get_job_run_status_impl(
    ctx: Context,
    run_id: str = Field(..., description="The 'run_id' returned from a 'run_notebook' or 'run_pipeline' tool.")
) -> Dict[str, Any]:
    """Checks the status of a specific job run (e.g., a notebook or pipeline execution), including detailed results."""
    logger.info(f"Tool 'get_job_run_status' called for run ID {run_id}.")
    run_url = job_status_store.get(run_id)
    if not run_url:
        raise ToolError(f"Run ID '{run_id}' not found. It may have completed, expired, or was from a different server session.")
    try:
        client = await get_session_fabric_client(ctx)
        status_data = await client.get_job_instance_status(run_url)
        status = status_data.get("status")
        # Once a job is finished, we can remove it from the in-memory store
        if status in ("Succeeded", "Failed", "Cancelled", "Canceled"):
            job_status_store.pop(run_id, None)
        return status_data
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to get run status for '{run_id}': {e.response_text or str(e)}") from e

def register_notebook_tools(app: FastMCP):
    logger.info("Registering Fabric Notebook tools...")
    app.tool(name="create_notebook")(create_notebook_impl)
    app.tool(name="update_notebook_content")(update_notebook_content_impl)
    app.tool(name="run_notebook")(run_notebook_impl)
    app.tool(name="delete_notebook")(delete_notebook_impl)
    # --- ADD THIS LINE ---
    app.tool(name="get_job_run_status")(get_job_run_status_impl)
    logger.info("Fabric Notebook tools registration complete.")