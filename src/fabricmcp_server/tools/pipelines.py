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
    PipelineActivity, FabricApiException, FabricAuthException
)
from ..app import get_session_fabric_client, job_status_store

logger = logging.getLogger(__name__)

def _build_pipeline_definition(pipeline_name: str, activities: List[PipelineActivity], workspace_id: str) -> str:
    pipeline_activities = []
    for act in activities:
        activity_def = {
            "name": act.name, "type": "TridentNotebook",
            "typeProperties": { "notebookId": act.notebook_id, "workspaceId": workspace_id, "parameters": {} },
            "dependsOn": [{"activity": dep, "dependencyConditions": ["Succeeded"]} for dep in act.depends_on] if act.depends_on else []
        }
        pipeline_activities.append(activity_def)

    pipeline_struct = {"name": pipeline_name, "properties": {"activities": pipeline_activities}}
    pipeline_json = json.dumps(pipeline_struct)
    return base64.b64encode(pipeline_json.encode('utf-8')).decode('utf-8')

async def create_pipeline_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the workspace where the pipeline will be created."),
    pipeline_name: str = Field(..., description="The desired display name for the new data pipeline."),
    activities: List[PipelineActivity] = Field(..., description="A list of notebook activities that define the pipeline's workflow."),
    description: Optional[str] = Field(None, description="An optional description for the new pipeline.")
) -> Dict[str, Any]:
    """Creates a new Data Pipeline by orchestrating notebooks. Handles both immediate and long-running creation."""
    logger.info(f"Tool 'create_pipeline' called for '{pipeline_name}'.")
    try:
        client = await get_session_fabric_client(ctx)
        b64_payload = _build_pipeline_definition(pipeline_name, activities, workspace_id)
        
        definition = ItemDefinitionForCreate(
            format="Trident.DataPipeline",
            parts=[DefinitionPart(path="pipeline-content.json", payload=b64_payload, payloadType="InlineBase64")]
        )
        create_payload = CreateItemRequest(displayName=pipeline_name, type="DataPipeline", description=description, definition=definition)
        
        response = await client.create_item(workspace_id, create_payload)
        
        if isinstance(response, ItemEntity):
            return {"status": "Succeeded", "message": "Pipeline created immediately.", "result": response.model_dump(by_alias=True)}

        if isinstance(response, httpx.Response) and response.status_code == 202:
            operation_url = response.headers.get("Operation-Location")
            if not operation_url:
                return {"status": "Accepted (Untrackable)", "message": "Pipeline creation initiated."}
            
            job_id = str(uuid.uuid4())
            job_status_store[job_id] = operation_url
            return {"status": "Accepted", "job_id": job_id, "message": "Pipeline creation is in progress."}
        
        raise ToolError(f"Unexpected response. Status: {getattr(response, 'status_code', 'N/A')}")
        
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to create pipeline: {e.response_text or str(e)}") from e

async def run_pipeline_impl(
    ctx: Context, 
    workspace_id: str = Field(..., description="The ID of the workspace containing the pipeline."),
    pipeline_id: str = Field(..., description="The ID of the Data Pipeline to execute.")
) -> Dict[str, Any]:
    """Triggers the execution of a Fabric Data Pipeline. This is a long-running operation."""
    logger.info(f"Tool 'run_pipeline' called for pipeline '{pipeline_id}'.")
    try:
        client = await get_session_fabric_client(ctx)
        response = await client.run_item(workspace_id, pipeline_id, "Pipeline")

        if isinstance(response, httpx.Response) and response.status_code == 202:
            operation_url = response.headers.get("Location")
            if not operation_url: raise ToolError("API did not provide a status location.")
            
            job_id = operation_url.split('/')[-1]
            job_status_store[job_id] = operation_url
            
            return {"status": "Accepted", "job_id": job_id, "message": "Pipeline execution started."}

        raise ToolError(f"Unexpected response from API. Status: {getattr(response, 'status_code', 'N/A')}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to run pipeline: {e.response_text or str(e)}") from e

def register_pipeline_tools(app: FastMCP):
    logger.info("Registering Fabric Pipeline tools...")
    app.tool(name="create_pipeline")(create_pipeline_impl)
    app.tool(name="run_pipeline")(run_pipeline_impl)
    logger.info("Fabric Pipeline tools registration complete.")