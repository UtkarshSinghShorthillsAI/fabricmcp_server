"""
Microsoft Fabric Copy Data Activity Tool
Based on official Fabric Copy Job Definition and real JSON examples
Modular design for future extensibility
"""

import json
import uuid
import base64
import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime

from fastmcp import FastMCP, Context
from pydantic import Field, BaseModel

from ..fabric_api_client import FabricApiClient
from ..fabric_models import (
    CreateItemRequest, 
    ItemDefinitionForCreate, 
    DefinitionPart,
    ItemEntity,
    FabricAuthException,
    FabricApiException
)
from ..sessions import get_session_fabric_client
from fastmcp.exceptions import ToolError

logger = logging.getLogger(__name__)

class CopyDataActivityTemplate(BaseModel):
    """Template for Copy Data Activity based on Fabric official structure"""
    name: str = Field(..., description="Activity name")
    description: Optional[str] = Field(None, description="Activity description")
    type: str = Field(default="Copy", description="Activity type (always 'Copy')")
    dependsOn: List[Dict[str, Any]] = Field(default_factory=list, description="Activity dependencies")
    policy: Dict[str, Any] = Field(default_factory=dict, description="Activity policy settings")
    typeProperties: Dict[str, Any] = Field(..., description="Copy activity type properties containing source and sink")

class LinkedServiceTemplate(BaseModel):
    """Template for LinkedService structure in datasetSettings"""
    name: str = Field(..., description="Linked service name")
    properties: Dict[str, Any] = Field(..., description="Linked service properties")

class DatasetSettingsTemplate(BaseModel):
    """Template for dataset settings structure"""
    annotations: List[Any] = Field(default_factory=list, description="Dataset annotations")
    linkedService: Dict[str, Any] = Field(..., description="Linked service configuration")
    type: str = Field(..., description="Dataset type (e.g., 'LakehouseTable', 'Json')")
    schema: List[Any] = Field(default_factory=list, description="Dataset schema")
    typeProperties: Optional[Dict[str, Any]] = Field(None, description="Type-specific properties")

def create_lakehouse_linked_service(lakehouse_name: str, workspace_id: str, artifact_id: str, root_folder: str = "Tables") -> Dict[str, Any]:
    """Create linkedService structure for Lakehouse connections
    
    Args:
        lakehouse_name: Display name of the lakehouse
        workspace_id: Workspace ID containing the lakehouse
        artifact_id: Lakehouse artifact ID
        root_folder: "Tables" or "Files"
    
    Returns:
        Dict containing the linkedService structure
    """
    return {
        "name": lakehouse_name,
        "properties": {
            "annotations": [],
            "type": "Lakehouse",
            "typeProperties": {
                "workspaceId": workspace_id,
                "artifactId": artifact_id,
                "rootFolder": root_folder
            }
        }
    }

def create_lakehouse_table_dataset_settings(linked_service: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """Create dataset settings for Lakehouse Table
    
    Args:
        linked_service: LinkedService configuration
        table_name: Name of the table
    
    Returns:
        Dict containing datasetSettings for LakehouseTable
    """
    return {
        "annotations": [],
        "linkedService": linked_service,
        "type": "LakehouseTable",
        "schema": [],
        "typeProperties": {
            "table": table_name
        }
    }

def create_lakehouse_file_dataset_settings(linked_service: Dict[str, Any], file_name: str, 
                                          folder_path: str = "", compression: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create dataset settings for Lakehouse Files
    
    Args:
        linked_service: LinkedService configuration
        file_name: Name of the file
        folder_path: Path to the folder
        compression: Compression settings
    
    Returns:
        Dict containing datasetSettings for files
    """
    location = {
        "type": "LakehouseLocation",
        "fileName": file_name
    }
    
    if folder_path:
        location["folderPath"] = folder_path
    
    type_properties = {"location": location}
    
    if compression:
        type_properties["compression"] = compression
    
    return {
        "annotations": [],
        "linkedService": linked_service,
        "type": "Json",  # Can be extended for other file types
        "typeProperties": type_properties,
        "schema": {}
    }

def create_lakehouse_table_source(lakehouse_name: str, workspace_id: str, artifact_id: str, 
                                 table_name: str, timestamp_as_of: Optional[str] = None, 
                                 version_as_of: Optional[int] = None) -> Dict[str, Any]:
    """Create source configuration for Lakehouse Table
    
    Args:
        lakehouse_name: Display name of the lakehouse
        workspace_id: Workspace ID
        artifact_id: Lakehouse artifact ID
        table_name: Source table name
        timestamp_as_of: Optional timestamp for time travel
        version_as_of: Optional version for time travel
    
    Returns:
        Dict containing source configuration
    """
    linked_service = create_lakehouse_linked_service(lakehouse_name, workspace_id, artifact_id, "Tables")
    dataset_settings = create_lakehouse_table_dataset_settings(linked_service, table_name)
    
    source = {
        "type": "LakehouseTableSource",
        "datasetSettings": dataset_settings
    }
    
    # Add time travel options if provided
    if timestamp_as_of:
        source["timestampAsOf"] = timestamp_as_of
    if version_as_of:
        source["versionAsOf"] = version_as_of
    
    return source

def create_lakehouse_table_sink(lakehouse_name: str, workspace_id: str, artifact_id: str, 
                                table_name: str, table_action: str = "Append") -> Dict[str, Any]:
    """Create sink configuration for Lakehouse Table
    
    Args:
        lakehouse_name: Display name of the lakehouse
        workspace_id: Workspace ID
        artifact_id: Lakehouse artifact ID
        table_name: Destination table name
        table_action: "Append", "Overwrite", etc.
    
    Returns:
        Dict containing sink configuration
    """
    linked_service = create_lakehouse_linked_service(lakehouse_name, workspace_id, artifact_id, "Tables")
    dataset_settings = create_lakehouse_table_dataset_settings(linked_service, table_name)
    
    return {
        "type": "LakehouseTableSink",
        "tableActionOption": table_action,
        "datasetSettings": dataset_settings
    }

def create_lakehouse_file_sink(lakehouse_name: str, workspace_id: str, artifact_id: str, 
                               file_name: str, folder_path: str = "", file_format: str = "Json",
                               store_settings: Optional[Dict[str, Any]] = None,
                               format_settings: Optional[Dict[str, Any]] = None,
                               compression: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create sink configuration for Lakehouse Files
    
    Args:
        lakehouse_name: Display name of the lakehouse
        workspace_id: Workspace ID
        artifact_id: Lakehouse artifact ID
        file_name: Destination file name
        folder_path: Destination folder path
        file_format: File format (Json, Csv, Parquet, etc.)
        store_settings: Store settings for file operations
        format_settings: Format-specific settings
        compression: Compression configuration
    
    Returns:
        Dict containing sink configuration
    """
    linked_service = create_lakehouse_linked_service(lakehouse_name, workspace_id, artifact_id, "Files")
    dataset_settings = create_lakehouse_file_dataset_settings(linked_service, file_name, folder_path, compression)
    
    sink = {
        "type": f"{file_format}Sink",
        "datasetSettings": dataset_settings
    }
    
    # Add store settings if provided
    if store_settings:
        sink["storeSettings"] = store_settings
    else:
        # Default store settings for Lakehouse files
        sink["storeSettings"] = {
            "type": "LakehouseWriteSettings",
            "maxConcurrentConnections": 15,
            "copyBehavior": "PreserveHierarchy",
            "blockSizeInMB": 50
        }
    
    # Add format settings if provided
    if format_settings:
        sink["formatSettings"] = format_settings
    else:
        # Default format settings based on file format
        if file_format == "Json":
            sink["formatSettings"] = {
                "type": "JsonWriteSettings",
                "filePattern": "setOfObjects"
            }
    
    return sink

def build_copy_activity_from_template(template: Dict[str, Any]) -> Dict[str, Any]:
    """Build a complete copy activity from template ensuring all required fields
    
    Args:
        template: Copy activity template dictionary
    
    Returns:
        Complete copy activity with generated ID and validated structure
    """
    activity = template.copy()
    
    # Ensure required fields
    if "id" not in activity:
        activity["id"] = str(uuid.uuid4())
    
    # Set default policy if not provided
    if not activity.get("policy"):
        activity["policy"] = {
            "timeout": "0.12:00:00",
            "retry": 0,
            "retryIntervalInSeconds": 30,
            "secureOutput": False,
            "secureInput": False
        }
    
    # Set default translator if not in typeProperties
    if "translator" not in activity.get("typeProperties", {}):
        activity["typeProperties"]["translator"] = {
            "type": "TabularTranslator",
            "typeConversion": True,
            "typeConversionSettings": {
                "allowDataTruncation": True,
                "treatBooleanAsNumber": False
            }
        }
    
    # Set default enableStaging if not provided
    if "enableStaging" not in activity.get("typeProperties", {}):
        activity["typeProperties"]["enableStaging"] = False
    
    return activity

def _build_pipeline_definition_with_copy_activity(pipeline_name: str, copy_activity: Dict[str, Any], 
                                                 description: Optional[str] = None,
                                                 parameters: Optional[Dict[str, Any]] = None,
                                                 variables: Optional[Dict[str, Any]] = None,
                                                 annotations: Optional[List[str]] = None,
                                                 concurrency: int = 1,
                                                 folder: Optional[Dict[str, str]] = None) -> str:
    """Build pipeline definition containing the copy activity"""
    
    pipeline_structure = {
        "name": pipeline_name,
        "objectId": str(uuid.uuid4()),
        "properties": {
            "description": description or f"Copy Data Pipeline created at {datetime.now().isoformat()}",
            "activities": [copy_activity],
            "parameters": parameters or {},
            "variables": variables or {},
            "annotations": annotations or ["CopyData", "Modular", "Production"],
            "concurrency": concurrency,
            "folder": folder or {"name": "CopyDataPipelines"}
        }
    }
    
    pipeline_json = json.dumps(pipeline_structure, indent=2)
    logger.info(f"Generated copy data pipeline JSON size: {len(pipeline_json)} characters")
    
    return base64.b64encode(pipeline_json.encode('utf-8')).decode('utf-8')

# MCP Tool Functions
async def create_copy_data_activity_from_json_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the workspace where the pipeline will be created."),
    pipeline_name: str = Field(..., description="The name for the pipeline containing the copy activity."),
    copy_activity_json: Dict[str, Any] = Field(..., description="Complete copy activity JSON following Fabric Copy Job Definition structure."),
    pipeline_parameters: Optional[Dict[str, Dict[str, Any]]] = Field(None, description="Pipeline parameters"),
    pipeline_variables: Optional[Dict[str, Dict[str, Any]]] = Field(None, description="Pipeline variables"),
    annotations: Optional[List[str]] = Field(None, description="Pipeline annotations"),
    concurrency: Optional[int] = Field(None, description="Pipeline concurrency level"),
    folder: Optional[Dict[str, str]] = Field(None, description="Pipeline folder organization"),
    description: Optional[str] = Field(None, description="Pipeline description")
) -> Dict[str, Any]:
    """
    Creates a data pipeline with a copy activity using the exact JSON structure expected by Microsoft Fabric.
    
    This tool accepts the complete copy activity JSON as shown in Fabric UI exports, ensuring maximum
    compatibility and supporting all current and future connection types without code changes.
    """
    logger.info(f"Tool 'create_copy_data_activity_from_json' called for '{pipeline_name}'.")
    
    try:
        client = await get_session_fabric_client(ctx)
        
        # Validate and build the copy activity
        copy_activity = build_copy_activity_from_template(copy_activity_json)
        
        # Convert parameters/variables if provided
        converted_parameters = None
        if pipeline_parameters:
            converted_parameters = {}
            for key, param_dict in pipeline_parameters.items():
                converted_parameters[key] = param_dict
        
        converted_variables = None
        if pipeline_variables:
            converted_variables = {}
            for key, var_dict in pipeline_variables.items():
                converted_variables[key] = var_dict
        
        # Build pipeline definition
        b64_payload = _build_pipeline_definition_with_copy_activity(
            pipeline_name=pipeline_name,
            copy_activity=copy_activity,
            description=description,
            parameters=converted_parameters,
            variables=converted_variables,
            annotations=annotations,
            concurrency=concurrency or 1,
            folder=folder
        )
        
        # Create the pipeline
        definition = ItemDefinitionForCreate(
            parts=[DefinitionPart(path="pipeline-content.json", payload=b64_payload, payloadType="InlineBase64")]
        )
        
        create_payload = CreateItemRequest(
            displayName=pipeline_name, 
            type="DataPipeline", 
            description=description, 
            definition=definition
        )
        
        response = await client.create_item(workspace_id, create_payload)
        
        if isinstance(response, ItemEntity):
            # Extract connection info from the copy activity for response
            source_type = copy_activity.get("typeProperties", {}).get("source", {}).get("type", "Unknown")
            sink_type = copy_activity.get("typeProperties", {}).get("sink", {}).get("type", "Unknown")
            
            return {
                "status": "Succeeded",
                "message": "Copy data pipeline created successfully using provided JSON structure.",
                "result": response.model_dump(by_alias=True),
                "copy_activity": {
                    "name": copy_activity.get("name"),
                    "source_type": source_type,
                    "sink_type": sink_type,
                    "id": copy_activity.get("id")
                }
            }
        
        raise ToolError(f"Unexpected response. Status: {getattr(response, 'status_code', 'N/A')}")
        
    except (FabricAuthException, FabricApiException) as e:
        raise ToolError(f"Failed to create copy data pipeline: {e.response_text or str(e)}") from e
    except Exception as e:
        raise ToolError(f"Error processing copy activity JSON: {str(e)}") from e

async def create_lakehouse_copy_activity_impl(
    ctx: Context,
    workspace_id: str = Field(..., description="The ID of the workspace where the pipeline will be created."),
    pipeline_name: str = Field(..., description="The name for the pipeline containing the copy activity."),
    source_lakehouse_name: str = Field(..., description="Source lakehouse display name"),
    source_lakehouse_id: str = Field(..., description="Source lakehouse artifact ID"),
    source_table_name: str = Field(..., description="Source table name"),
    sink_lakehouse_name: str = Field(..., description="Sink lakehouse display name"),
    sink_lakehouse_id: str = Field(..., description="Sink lakehouse artifact ID"),
    sink_table_name: str = Field(..., description="Sink table name"),
    sink_mode: str = Field("table", description="Sink mode: 'table' or 'file'"),
    table_action: str = Field("Append", description="Table action for table mode: Append, Overwrite"),
    file_name: Optional[str] = Field(None, description="File name for file mode"),
    folder_path: Optional[str] = Field(None, description="Folder path for file mode"),
    file_format: str = Field("Json", description="File format for file mode: Json, Csv, Parquet"),
    timestamp_as_of: Optional[str] = Field(None, description="Source timestamp for time travel"),
    version_as_of: Optional[int] = Field(None, description="Source version for time travel"),
    description: Optional[str] = Field(None, description="Pipeline description")
) -> Dict[str, Any]:
    """
    Creates a lakehouse-to-lakehouse copy activity with proper structure.
    Supports both table-to-table and table-to-file scenarios.
    """
    logger.info(f"Tool 'create_lakehouse_copy_activity' called for '{pipeline_name}'.")
    
    try:
        # Create source (always table for this tool)
        source = create_lakehouse_table_source(
            lakehouse_name=source_lakehouse_name,
            workspace_id=workspace_id,
            artifact_id=source_lakehouse_id,
            table_name=source_table_name,
            timestamp_as_of=timestamp_as_of,
            version_as_of=version_as_of
        )
        
        # Create sink based on mode
        if sink_mode == "table":
            sink = create_lakehouse_table_sink(
                lakehouse_name=sink_lakehouse_name,
                workspace_id=workspace_id,
                artifact_id=sink_lakehouse_id,
                table_name=sink_table_name,
                table_action=table_action
            )
        else:  # file mode
            sink = create_lakehouse_file_sink(
                lakehouse_name=sink_lakehouse_name,
                workspace_id=workspace_id,
                artifact_id=sink_lakehouse_id,
                file_name=file_name or f"{sink_table_name}.{file_format.lower()}",
                folder_path=folder_path or "processed_data",
                file_format=file_format
            )
        
        # Build copy activity
        copy_activity = {
            "name": f"Copy_{source_table_name}_to_{sink_table_name}",
            "description": f"Copy from {source_lakehouse_name}.{source_table_name} to {sink_lakehouse_name}.{sink_table_name}",
            "type": "Copy",
            "dependsOn": [],
            "typeProperties": {
                "source": source,
                "sink": sink,
                "enableStaging": False,
                "translator": {
                    "type": "TabularTranslator",
                    "typeConversion": True,
                    "typeConversionSettings": {
                        "allowDataTruncation": True,
                        "treatBooleanAsNumber": False
                    }
                }
            }
        }
        
        # Use the JSON-based implementation
        return await create_copy_data_activity_from_json_impl(
            ctx=ctx,
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            copy_activity_json=copy_activity,
            description=description
        )
        
    except Exception as e:
        raise ToolError(f"Failed to create lakehouse copy activity: {str(e)}") from e

# Template examples for common scenarios
COPY_ACTIVITY_TEMPLATES = {
    "lakehouse_table_to_table": {
        "name": "LakehouseTableToTable",
        "type": "Copy",
        "typeProperties": {
            "source": {
                "type": "LakehouseTableSource",
                "datasetSettings": {
                    "annotations": [],
                    "linkedService": {
                        "name": "SOURCE_LAKEHOUSE_NAME",
                        "properties": {
                            "annotations": [],
                            "type": "Lakehouse",
                            "typeProperties": {
                                "workspaceId": "WORKSPACE_ID",
                                "artifactId": "SOURCE_ARTIFACT_ID",
                                "rootFolder": "Tables"
                            }
                        }
                    },
                    "type": "LakehouseTable",
                    "schema": [],
                    "typeProperties": {
                        "table": "SOURCE_TABLE_NAME"
                    }
                }
            },
            "sink": {
                "type": "LakehouseTableSink",
                "tableActionOption": "Append",
                "datasetSettings": {
                    "annotations": [],
                    "linkedService": {
                        "name": "SINK_LAKEHOUSE_NAME",
                        "properties": {
                            "annotations": [],
                            "type": "Lakehouse",
                            "typeProperties": {
                                "workspaceId": "WORKSPACE_ID",
                                "artifactId": "SINK_ARTIFACT_ID",
                                "rootFolder": "Tables"
                            }
                        }
                    },
                    "type": "LakehouseTable",
                    "schema": [],
                    "typeProperties": {
                        "table": "SINK_TABLE_NAME"
                    }
                }
            }
        }
    },
    "lakehouse_table_to_files": {
        "name": "LakehouseTableToFiles",
        "type": "Copy",
        "typeProperties": {
            "source": {
                "type": "LakehouseTableSource",
                "datasetSettings": {
                    "annotations": [],
                    "linkedService": {
                        "name": "SOURCE_LAKEHOUSE_NAME",
                        "properties": {
                            "annotations": [],
                            "type": "Lakehouse",
                            "typeProperties": {
                                "workspaceId": "WORKSPACE_ID",
                                "artifactId": "SOURCE_ARTIFACT_ID",
                                "rootFolder": "Tables"
                            }
                        }
                    },
                    "type": "LakehouseTable",
                    "schema": [],
                    "typeProperties": {
                        "table": "SOURCE_TABLE_NAME"
                    }
                }
            },
            "sink": {
                "type": "JsonSink",
                "storeSettings": {
                    "type": "LakehouseWriteSettings",
                    "maxConcurrentConnections": 15,
                    "copyBehavior": "PreserveHierarchy",
                    "blockSizeInMB": 50
                },
                "formatSettings": {
                    "type": "JsonWriteSettings",
                    "filePattern": "setOfObjects"
                },
                "datasetSettings": {
                    "annotations": [],
                    "linkedService": {
                        "name": "SINK_LAKEHOUSE_NAME",
                        "properties": {
                            "annotations": [],
                            "type": "Lakehouse",
                            "typeProperties": {
                                "workspaceId": "WORKSPACE_ID",
                                "artifactId": "SINK_ARTIFACT_ID",
                                "rootFolder": "Files"
                            }
                        }
                    },
                    "type": "Json",
                    "typeProperties": {
                        "location": {
                            "type": "LakehouseLocation",
                            "fileName": "OUTPUT_FILE_NAME",
                            "folderPath": "FOLDER_PATH"
                        }
                    },
                    "schema": {}
                }
            }
        }
    }
} 

def register_copy_data_activity_tools(app: FastMCP):
    """Register copy data activity tools with the MCP app"""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("Registering Copy Data Activity tools...")
    app.tool(name="create_copy_data_activity_from_json")(create_copy_data_activity_from_json_impl)
    app.tool(name="create_lakehouse_copy_activity")(create_lakehouse_copy_activity_impl)
    logger.info("Copy Data Activity tools registration complete.") 