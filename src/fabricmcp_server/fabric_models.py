from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

# --- Custom Exceptions ---

class FabricAuthException(Exception):
    pass

class FabricApiException(Exception):
    def __init__(self, status_code: int, message: str, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.response_text = response_text

    def __str__(self):
        return f"Fabric API Error {self.status_code}: {self.message}"

# --- Core Fabric API Models ---

class DefinitionPart(BaseModel):
    path: str
    payload: str
    payload_type: str = Field(alias="payloadType")
    model_config = {"populate_by_name": True}

# Corrected Definition model for creation
class ItemDefinitionForCreate(BaseModel):
    format: Optional[str] = Field(None, description="The format of the definition, e.g., 'ipynb' for notebooks or 'pbidataset' for Power BI datasets.")
    parts: List[DefinitionPart]

class ItemDefinitionForGet(BaseModel):
    parts: List[DefinitionPart]

class ItemEntity(BaseModel):
    id: Optional[str] = Field(None, description="The item ID")
    workspace_id: Optional[str] = Field(None, alias="workspaceId", description="The workspace ID")
    type: Optional[str] = Field(None, description="The type of the item (e.g., 'Lakehouse', 'Notebook')")
    display_name: Optional[str] = Field(None, alias="displayName", description="The display name of the item")
    description: Optional[str] = Field(None, description="The description of the item")
    definition: Optional[ItemDefinitionForGet] = None
    model_config = {"populate_by_name": True}

class CreateItemRequest(BaseModel):
    display_name: str = Field(..., alias="displayName")
    type: str
    description: Optional[str] = None
    definition: Optional[ItemDefinitionForCreate] = None
    model_config = {"populate_by_name": True}

class UpdateItemDefinitionRequest(BaseModel):
    definition: ItemDefinitionForCreate # Update also requires the format

# --- Enhanced Pipeline Models ---

class PipelineParameter(BaseModel):
    type: str = Field(..., description="Parameter type (string, bool, int, float, array, object)")
    default_value: Optional[Any] = Field(None, alias="defaultValue", description="Default value for the parameter")
    description: Optional[str] = Field(None, description="Parameter description")
    model_config = {"populate_by_name": True}

class PipelineVariable(BaseModel):
    type: str = Field(..., description="Variable type (string, bool, int, float, array, object)")
    default_value: Optional[Any] = Field(None, alias="defaultValue", description="Default value for the variable")
    description: Optional[str] = Field(None, description="Variable description")
    model_config = {"populate_by_name": True}

class ActivityPolicy(BaseModel):
    timeout: Optional[str] = Field("7.00:00:00", description="Activity timeout in HH:MM:SS format")
    retry: Optional[int] = Field(0, description="Number of retry attempts")
    retry_interval_in_seconds: Optional[int] = Field(30, alias="retryIntervalInSeconds", description="Retry interval in seconds")
    secure_input: Optional[bool] = Field(False, alias="secureInput", description="Secure input flag")
    secure_output: Optional[bool] = Field(False, alias="secureOutput", description="Secure output flag")
    model_config = {"populate_by_name": True}

class ActivityDependency(BaseModel):
    activity: str = Field(..., description="Name of the activity this depends on")
    dependency_conditions: List[str] = Field(..., alias="dependencyConditions", description="Dependency conditions (Succeeded, Failed, Completed, Skipped)")
    model_config = {"populate_by_name": True}

# --- Base Activity Models ---

class BaseActivity(BaseModel):
    name: str = Field(..., description="Unique activity name within the pipeline")
    type: str = Field(..., description="Activity type")
    description: Optional[str] = Field(None, description="Activity description")
    depends_on: Optional[List[ActivityDependency]] = Field(None, alias="dependsOn", description="Activity dependencies")
    policy: Optional[ActivityPolicy] = Field(None, description="Activity execution policy")
    user_properties: Optional[List[Dict[str, Any]]] = Field(None, alias="userProperties", description="User-defined properties")
    model_config = {"populate_by_name": True}

# --- Specific Activity Types ---

class CopyDataActivity(BaseActivity):
    type: str = Field("Copy", description="Copy Data activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Copy activity specific properties")
    inputs: Optional[List[Dict[str, Any]]] = Field(None, description="Input datasets")
    outputs: Optional[List[Dict[str, Any]]] = Field(None, description="Output datasets")
    model_config = {"populate_by_name": True}

class NotebookActivity(BaseActivity):
    type: str = Field("TridentNotebook", description="Notebook activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Notebook activity properties including notebookId, workspaceId, parameters")
    model_config = {"populate_by_name": True}

class SetVariableActivity(BaseActivity):
    type: str = Field("SetVariable", description="Set Variable activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Set variable properties including variableName and value")
    model_config = {"populate_by_name": True}

class ForEachActivity(BaseActivity):
    type: str = Field("ForEach", description="ForEach activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="ForEach properties including items, activities, isSequential, batchCount")
    model_config = {"populate_by_name": True}

class IfConditionActivity(BaseActivity):
    type: str = Field("IfCondition", description="If Condition activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="If condition properties including expression, ifTrueActivities, ifFalseActivities")
    model_config = {"populate_by_name": True}

class UntilActivity(BaseActivity):
    type: str = Field("Until", description="Until activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Until properties including expression and activities")
    model_config = {"populate_by_name": True}

class WaitActivity(BaseActivity):
    type: str = Field("Wait", description="Wait activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Wait properties including waitTimeInSeconds")
    model_config = {"populate_by_name": True}

class LookupActivity(BaseActivity):
    type: str = Field("Lookup", description="Lookup activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Lookup properties including source, dataset, firstRowOnly")
    model_config = {"populate_by_name": True}

class ExecutePipelineActivity(BaseActivity):
    type: str = Field("ExecutePipeline", description="Execute Pipeline activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Execute pipeline properties including pipeline, parameters, waitOnCompletion")
    model_config = {"populate_by_name": True}

class ScriptActivity(BaseActivity):
    type: str = Field("Script", description="Script activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Script activity properties including linkedService, scripts, scriptBlockExecutionTimeout")
    model_config = {"populate_by_name": True}

class StoredProcedureActivity(BaseActivity):
    type: str = Field("SqlServerStoredProcedure", description="Stored Procedure activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Stored procedure properties including storedProcedureName, storedProcedureParameters")
    model_config = {"populate_by_name": True}

class WebHookActivity(BaseActivity):
    type: str = Field("WebHook", description="WebHook activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="WebHook properties including url, method, headers, body, timeout")
    model_config = {"populate_by_name": True}

class WebActivity(BaseActivity):
    type: str = Field("WebActivity", description="Web activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Web activity properties including url, method, headers, body")
    model_config = {"populate_by_name": True}

class TeamsNotificationActivity(BaseActivity):
    type: str = Field("TeamsNotification", description="Teams Notification activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Teams notification properties including webhookUrl, message")
    model_config = {"populate_by_name": True}

class SwitchActivity(BaseActivity):
    type: str = Field("Switch", description="Switch activity type")
    type_properties: Dict[str, Any] = Field(..., alias="typeProperties", description="Switch properties including on, cases, defaultActivities")
    model_config = {"populate_by_name": True}

# --- Comprehensive Pipeline Definition ---

class ComprehensivePipelineDefinition(BaseModel):
    name: str = Field(..., description="Pipeline name")
    object_id: Optional[str] = Field(None, alias="objectId", description="Pipeline object ID")
    description: Optional[str] = Field(None, description="Pipeline description")
    parameters: Optional[Dict[str, PipelineParameter]] = Field(None, description="Pipeline parameters")
    variables: Optional[Dict[str, PipelineVariable]] = Field(None, description="Pipeline variables")
    activities: List[Union[BaseActivity, Dict[str, Any]]] = Field(..., description="Pipeline activities")
    annotations: Optional[List[str]] = Field(None, description="Pipeline annotations")
    concurrency: Optional[int] = Field(None, description="Pipeline concurrency level")
    folder: Optional[Dict[str, str]] = Field(None, description="Pipeline folder organization")
    model_config = {"populate_by_name": True}

# --- Activity Builder Helper Models ---

class ActivityBuilder(BaseModel):
    """Helper class for building specific activity configurations"""
    
    @staticmethod
    def create_copy_data_activity(
        name: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        description: Optional[str] = None,
        depends_on: Optional[List[ActivityDependency]] = None,
        policy: Optional[ActivityPolicy] = None
    ) -> CopyDataActivity:
        """Create a Copy Data activity with proper configuration"""
        type_properties = {
            "source": source_config,
            "sink": sink_config,
            "enableStaging": False,
            "enableSkipIncompatibleRow": False
        }
        
        return CopyDataActivity(
            name=name,
            description=description,
            dependsOn=depends_on,
            policy=policy,
            typeProperties=type_properties
        )
    
    @staticmethod
    def create_notebook_activity(
        name: str,
        notebook_id: str,
        workspace_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        depends_on: Optional[List[ActivityDependency]] = None,
        policy: Optional[ActivityPolicy] = None
    ) -> NotebookActivity:
        """Create a Notebook activity with proper configuration"""
        type_properties = {
            "notebookId": notebook_id,
            "workspaceId": workspace_id,
            "parameters": parameters or {}
        }
        
        return NotebookActivity(
            name=name,
            description=description,
            dependsOn=depends_on,
            policy=policy,
            typeProperties=type_properties
        )
    
    @staticmethod
    def create_foreach_activity(
        name: str,
        items: str,  # Expression for items to iterate over
        activities: List[BaseActivity],
        is_sequential: bool = False,
        batch_count: Optional[int] = None,
        description: Optional[str] = None,
        depends_on: Optional[List[ActivityDependency]] = None,
        policy: Optional[ActivityPolicy] = None
    ) -> ForEachActivity:
        """Create a ForEach activity with proper configuration"""
        type_properties = {
            "items": {"value": items, "type": "Expression"},
            "activities": [activity.model_dump(by_alias=True, exclude_none=True) for activity in activities],
            "isSequential": is_sequential
        }
        
        if batch_count:
            type_properties["batchCount"] = batch_count
            
        return ForEachActivity(
            name=name,
            description=description,
            dependsOn=depends_on,
            policy=policy,
            typeProperties=type_properties
        )
    
    @staticmethod
    def create_if_condition_activity(
        name: str,
        expression: str,
        if_true_activities: List[BaseActivity],
        if_false_activities: Optional[List[BaseActivity]] = None,
        description: Optional[str] = None,
        depends_on: Optional[List[ActivityDependency]] = None,
        policy: Optional[ActivityPolicy] = None
    ) -> IfConditionActivity:
        """Create an If Condition activity with proper configuration"""
        type_properties = {
            "expression": {"value": expression, "type": "Expression"},
            "ifTrueActivities": [activity.model_dump(by_alias=True, exclude_none=True) for activity in if_true_activities]
        }
        
        if if_false_activities:
            type_properties["ifFalseActivities"] = [activity.model_dump(by_alias=True, exclude_none=True) for activity in if_false_activities]
            
        return IfConditionActivity(
            name=name,
            description=description,
            dependsOn=depends_on,
            policy=policy,
            typeProperties=type_properties
        )

# --- Models for Complex Tool Arguments ---

class NotebookCell(BaseModel):
    cell_type: str = Field(..., description="Type of the cell, e.g., 'code' or 'markdown'.")
    source: List[str] = Field(..., description="A list of strings representing the lines of code/text in the cell.")
    metadata: Dict[str, Any] = Field(default_factory=dict)

class PipelineActivity(BaseModel):
    name: str = Field(..., description="A unique name for the activity within the pipeline.")
    notebook_id: str = Field(..., description="The ID of the notebook to be executed in this activity.")
    depends_on: Optional[List[str]] = Field(None, description="A list of names of other activities that must succeed before this one runs.")

# --- Models for Lakehouse Operations ---

class FormatOptions(BaseModel):
    format: str = "Csv"
    header: bool = True
    delimiter: str = ","

class LoadTableRequest(BaseModel):
    relative_path: str = Field(..., alias="relativePath")
    path_type: str = Field("File", alias="pathType")
    mode: str = "Overwrite"
    recursive: bool = False
    format_options: FormatOptions = Field(FormatOptions(), alias="formatOptions")
    model_config = {"populate_by_name": True}

# --- Global Parameters and Configuration Models ---

class GlobalParameter(BaseModel):
    type: str = Field(..., description="Parameter type")
    value: Any = Field(..., description="Parameter value")
    description: Optional[str] = Field(None, description="Parameter description")

class PipelineConfiguration(BaseModel):
    """Model for metadata-driven pipeline configuration"""
    source_table: str = Field(..., description="Source table name")
    destination_table: str = Field(..., description="Destination table name")
    load_type: str = Field(..., description="Load type: full or incremental")
    batch_number: Optional[int] = Field(None, description="Batch execution order")
    active: bool = Field(True, description="Whether this configuration is active")
    watermark_column: Optional[str] = Field(None, description="Column used for incremental loading")
    last_watermark: Optional[str] = Field(None, description="Last processed watermark value")
    
class MetadataFramework(BaseModel):
    """Model for metadata-driven pipeline framework"""
    configurations: List[PipelineConfiguration] = Field(..., description="List of pipeline configurations")
    global_parameters: Optional[Dict[str, GlobalParameter]] = Field(None, description="Global parameters")
    environment: str = Field("dev", description="Environment (dev, test, prod)")
