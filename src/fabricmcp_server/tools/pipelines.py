# This is the final, definitive, and correct file: src/fabricmcp_server/tools/pipelines.py

import logging
import json
import base64
import httpx
import uuid
from typing import Optional, List, Dict, Any

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from pydantic import Field

# Correctly import all necessary components
from ..fabric_models import (
    CreateItemRequest, DefinitionPart, ItemDefinitionForCreate, 
    FabricApiException, FabricAuthException, ItemEntity
)
from ..app import get_session_fabric_client, job_status_store
from ..activity_types import Activity, CopyActivity
from ..copy_activity_schemas import build_source_payload, build_sink_payload

logger = logging.getLogger(__name__)

def _encode_b64(obj: dict) -> str:
    """Encodes a dictionary to a Base64 string."""
    return base64.b64encode(json.dumps(obj).encode("utf-8")).decode("utf-8")

def _build_pipeline_definition_payload(
    pipeline_name: str,
    activities: List[Activity]
) -> str:
    """
    Builds the final pipeline JSON definition payload.
    It now intelligently transforms Copy activities using the payload builders.
    """
    final_activities_json = []
    for act in activities:
        # This is the core logic that makes the tool robust
        if isinstance(act, CopyActivity):
            # Create a deep copy to avoid modifying the input model
            activity_dict = act.model_dump(by_alias=True, exclude_none=True)
            
            # Use the dedicated builders to generate the final API-compliant JSON
            source_payload = build_source_payload(act.typeProperties.source)
            sink_payload = build_sink_payload(act.typeProperties.sink)
            
            # Inject the generated payloads into the correct location
            activity_dict["typeProperties"]["source"] = source_payload
            activity_dict["typeProperties"]["sink"] = sink_payload
            
            final_activities_json.append(activity_dict)
        else:
            # Use default Pydantic serialization for all other activities
            final_activities_json.append(act.model_dump(by_alias=True, exclude_none=True))

    pipeline_struct = {"name": pipeline_name, "properties": {"activities": final_activities_json}}
    return _encode_b64(pipeline_struct)

async def create_pipeline_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the workspace where the pipeline will be created."),
    pipeline_name: str = Field(..., description="Display name for the new data pipeline."),
    activities: List[Activity] = Field(default_factory=list, description="A list of activities to include in the pipeline."),
    description: Optional[str] = Field(None, description="Optional description for the pipeline.")
) -> Dict[str, Any]:
    """Creates a new Data Pipeline with a specified list of activities."""
    logger.info(f"Tool 'create_pipeline' called for '{pipeline_name}'.")
    try:
        client = await get_session_fabric_client(ctx)

        b64_payload = _build_pipeline_definition_payload(
            pipeline_name=pipeline_name,
            activities=activities
        )

        definition = ItemDefinitionForCreate(
            format="Trident.DataPipeline",
            parts=[DefinitionPart(path="pipeline-content.json", payload=b64_payload, payloadType="InlineBase64")]
        )
        create_payload = CreateItemRequest(displayName=pipeline_name, type="DataPipeline", description=description, definition=definition)
        
        response = await client.create_item(workspace_id, create_payload)
        
        if isinstance(response, ItemEntity):
            return response.model_dump(by_alias=True)
        if isinstance(response, httpx.Response):
             return {"status_code": response.status_code, "headers": dict(response.headers), "text": response.text}
        
        raise ToolError(f"Unexpected response type: {type(response)}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to create pipeline: {e.response_text or str(e)}")

async def update_pipeline_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="ID of the pipeline's workspace."),
    pipeline_id: str = Field(..., description="ID of the pipeline to update."),
    pipeline_name: str = Field(..., description="The current or new name of the pipeline."),
    activities: List[Activity] = Field(..., description="The complete, final list of activities for the pipeline.")
) -> Dict[str, Any]:
    """
    Updates a Data Pipeline's definition with a new list of activities.
    This replaces all existing activities with the provided list.
    """
    logger.info(f"Tool 'update_pipeline' called for pipeline '{pipeline_id}'.")
    try:
        client = await get_session_fabric_client(ctx)

        b64_payload = _build_pipeline_definition_payload(
            pipeline_name=pipeline_name,
            activities=activities
        )
        
        parts = [{"path": "pipeline-content.json", "payload": b64_payload, "payloadType": "InlineBase64"}]
        definition = {"parts": parts}
        
        response = await client.update_pipeline_definition(
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            definition=definition
        )
        
        if response.status_code in [200, 202]:
            return {"status": "Succeeded", "message": "Pipeline definition update accepted."}
        else:
            raise ToolError(f"Failed to update pipeline. Status: {response.status_code}, Body: {response.text}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to update pipeline: {e.response_text or str(e)}")

# (The run_pipeline_impl tool remains unchanged)
async def run_pipeline_impl(
    ctx: Context, 
    workspace_id: str = Field(..., description="The ID of the workspace containing the pipeline."),
    pipeline_id: str = Field(..., description="The ID of the Data Pipeline to execute.")
) -> Dict[str, Any]:
    # ... existing implementation is correct ...
    logger.info(f"Tool 'run_pipeline' called for pipeline '{pipeline_id}'.")
    try:
        client = await get_session_fabric_client(ctx)
        response = await client.run_item(workspace_id, pipeline_id, "Pipeline")

        if isinstance(response, httpx.Response):
            if response.status_code == 202:
                operation_url = response.headers.get("Location")
                if not operation_url:
                    raise ToolError("API did not provide a status location.")
                job_id = operation_url.split('/')[-1]
                job_status_store[job_id] = operation_url
                return {"status": "Accepted", "job_id": job_id, "message": "Pipeline execution started."}
        
        raise ToolError(f"Unexpected response from API: {response}")

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to run pipeline: {e.response_text or str(e)}")

async def get_pipeline_definition_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the Fabric workspace containing the pipeline."),
    pipeline_id: str = Field(..., description="The ID of the pipeline whose definition you want to retrieve."),
    decode_payload: bool = Field(True, description="If true, decodes the Base64 payload of the pipeline content into JSON.")
) -> Dict[str, Any]:
    """
    Retrieves the full JSON definition of a Data Pipeline.
    By default, it decodes the Base64 payload to show the structured activities.
    """
    logger.info(f"Tool 'get_pipeline_definition' called for pipeline {pipeline_id}.")
    try:
        client = await get_session_fabric_client(ctx)
        
        definition = await client.get_item_definition(
            workspace_id=workspace_id, 
            item_id=pipeline_id
        )
        
        if not definition:
            raise ToolError("Pipeline definition not found or the API returned an empty response.")

        if decode_payload and 'definition' in definition and 'parts' in definition['definition']:
            try:
                # Find the specific part for the pipeline content
                content_part = next(
                    part for part in definition['definition']['parts'] 
                    if part.get("path") == "pipeline-content.json"
                )
                
                # Decode the payload from Base64 to a JSON object
                base64_payload = content_part['payload']
                decoded_json_string = base64.b64decode(base64_payload).decode('utf-8')
                decoded_payload_obj = json.loads(decoded_json_string)
                
                # Replace the opaque string with the rich JSON object
                content_part['payload'] = decoded_payload_obj
                logger.info(f"Successfully decoded pipeline content for pipeline {pipeline_id}.")

            except (StopIteration, KeyError, Exception) as e:
                logger.warning(f"Could not decode pipeline payload for {pipeline_id}: {e}")
                # If decoding fails, we still return the raw definition to the user
                pass
            
        return definition

    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to get pipeline definition: {e.response_text or str(e)}")
    
def register_pipeline_tools(app: FastMCP):
    logger.info("Registering Fabric Pipeline tools...")
    app.tool(name="create_pipeline")(create_pipeline_impl)
    app.tool(name="update_pipeline")(update_pipeline_impl)
    app.tool(name="run_pipeline")(run_pipeline_impl)
    app.tool(name="get_pipeline_definition")(get_pipeline_definition_impl)
    logger.info("Fabric Pipeline tools registration complete.")