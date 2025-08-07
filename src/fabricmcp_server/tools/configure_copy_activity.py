import base64
import json
import logging
from typing import Dict, Any, Optional

from fastmcp import FastMCP, Context
from pydantic import Field

from ..sessions import get_session_fabric_client
from ..fabric_models import DefinitionPart
from ..activity_types import SourceConfig, SinkConfig      # ← NEW import

logger = logging.getLogger(__name__)

async def configure_copy_activity_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="Workspace ID"),
    pipeline_id: str = Field(..., description="Pipeline ID"),
    activity_name: str = Field(..., description="Exact name of the Copy activity to patch"),
    source: Optional[SourceConfig] = None,                 # ← typed
    sink:   Optional[SinkConfig] = None,                   # ← typed
    translator: Optional[Dict[str, Any]] = None,
    extra_type_properties: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Patch the given Copy activity.
    Only the dictionaries you pass are merged; everything else is untouched.
    Enum-based models guarantee that 'type' contains a Fabric-valid string.
    """

    client = await get_session_fabric_client(ctx)

    # ------------------------------------------------------------------ pull
    definition = await client.get_pipeline_definition(workspace_id, pipeline_id)
    raw_payload = next(
        p["payload"] for p in definition["parts"] if p["path"] == "pipeline-content.json"
    )
    pipeline_json = json.loads(base64.b64decode(raw_payload).decode("utf-8"))

    # ------------------------------------------------------------------ patch
    for act in pipeline_json["properties"]["activities"]:
        if act["name"] == activity_name and act["type"] == "Copy":
            tp = act.setdefault("typeProperties", {})
            if source is not None:
                tp["source"] = {"type": source.type.value, **source.details}
            if sink is not None:
                tp["sink"] = {"type": sink.type.value, **sink.details}
            if translator is not None:
                tp["translator"] = translator
            if extra_type_properties:
                tp.update(extra_type_properties)
            break
    else:
        return {"error": f"Copy activity '{activity_name}' not found in pipeline."}

    # ------------------------------------------------------------------ push
    new_payload = base64.b64encode(json.dumps(pipeline_json).encode()).decode()
    parts = [
        DefinitionPart(
            path="pipeline-content.json",
            payload=new_payload,
            payloadType="InlineBase64"
        )
    ]
    await client.update_pipeline_definition(
        workspace_id,
        pipeline_id,
        {"parts": [p.model_dump(by_alias=True) for p in parts]},
    )

    return {"status": "Succeeded", "message": f"{activity_name} updated."}

# ------------------------------------------------------------------ registry
def register_copy_tools(app: FastMCP):
    logger.info("Registering configure_copy_activity tool")
    app.tool(name="configure_copy_activity")(configure_copy_activity_impl)
