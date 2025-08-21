# fabric_mcp_server/activity_types.py
from __future__ import annotations
from typing import List, Optional, Literal, Dict, Any, Union, Annotated
from pydantic import BaseModel, Field, model_validator
# Removed overfitted copy schemas - using flexible models
from .common_schemas import Expression, PipelineReference, ExternalReferences, LinkedServiceReference, DatasetReference, TabularTranslator
from .connection_types import (
    DatabaseConnectionRef, 
    StorageConnectionRef, 
    ServiceConnectionRef,
    OtherConnectionRef,
    ConnectionRef,  # Union of all connection types
    FabricLinkedService
)

# ---------------- Common pieces ----------------

class DependencyCondition(BaseModel):
    activity: str
    dependencyConditions: List[Literal["Succeeded", "Failed", "Completed", "Skipped"]]

class Policy(BaseModel):
    timeout: Optional[str] = None                 # e.g. "0.12:00:00"
    retry: Optional[int] = 0
    retryIntervalInSeconds: Optional[int] = 30
    secureOutput: Optional[bool] = False
    secureInput: Optional[bool] = False

class BaseActivity(BaseModel):
    name: str
    type: str
    dependsOn: Optional[List[DependencyCondition]] = None
    policy: Optional[Policy] = None
    state: Optional[Literal["Active", "Inactive"]] = None
    onInactiveMarkAs: Optional[Literal["Succeeded", "Failed", "Skipped"]] = None
    externalReferences: Optional[Dict[str, Any]] = None

    model_config = {"extra": "ignore"}  # ignore unknown top-level fields to be safe

# ---------------- TridentNotebook ----------------

class NotebookProperties(BaseModel):
    notebookId: str
    workspaceId: str
    parameters: Optional[Dict[str, Any]] = None

class TridentNotebookActivity(BaseActivity):
    type: Literal["TridentNotebook"]
    typeProperties: NotebookProperties

# ---------------- Teams ----------------

class TeamsInputs(BaseModel):
    method: str
    path: str
    body: Optional[Dict[str, Any]] = None

class TeamsProperties(BaseModel):
    inputs: TeamsInputs

class TeamsActivity(BaseActivity):
    type: Literal["Teams"]
    typeProperties: TeamsProperties

# ---------------- Copy ----------------

from .flexible_copy_schemas import FlexibleCopyProperties

class CopyActivity(BaseActivity):
    """Flexible Copy Activity model - matches real Fabric API patterns.
    
    Example usage for copying from SqlServer to Lakehouse:
    {
        "name": "Copy_SqlServer_to_Lakehouse",
        "type": "Copy",
        "typeProperties": {
            "source": {
                "type": "SqlServerSource",
                "datasetSettings": {
                    "type": "SqlServerTable",
                    "externalReferences": {"connection": "your_sqlserver_connection_id"}
                }
            },
            "sink": {
                "type": "LakehouseTableSink",
                "datasetSettings": {
                    "type": "LakehouseTable",
                    "typeProperties": {"table": "target_table_name"},
                    "linkedService": {
                        "name": "lakehouse_name",
                        "properties": {
                            "type": "Lakehouse",
                            "typeProperties": {
                                "artifactId": "lakehouse_id",
                                "workspaceId": "workspace_id"
                            }
                        }
                    }
                }
            }
        }
    }
    
    Pattern for source types: {ConnectionType}Source (e.g., OracleSource, MySqlSource)
    Pattern for sink types: LakehouseTableSink or DataWarehouseSink for Fabric targets
    """
    type: Literal["Copy"]
    typeProperties: FlexibleCopyProperties = Field(..., description="Copy activity configuration with source and sink")
    inputs: Optional[List[DatasetReference]] = Field(None, description="Optional dataset references for inputs")
    outputs: Optional[List[DatasetReference]] = Field(None, description="Optional dataset references for outputs")


# ---------------- RefreshDataflow ----------------

class RefreshDataflowProperties(BaseModel):
    workspaceId: str
    notifyOption: Optional[str] = "NoNotification"
    dataflowId: Optional[str] = None
    dataflowType: Optional[str] = None

class RefreshDataflowActivity(BaseActivity):
    type: Literal["RefreshDataflow"]
    typeProperties: RefreshDataflowProperties

# ---------------- GetMetadata ----------------

class GetMetadataProperties(BaseModel):
    """Defines the high-level configuration for a GetMetadata activity."""
    datasetSettings: Dict[str, Any]  # Flexible dataset settings 
    fieldList: List[str]

class GetMetadataActivity(BaseActivity):
    type: Literal["GetMetadata"]
    typeProperties: GetMetadataProperties

class LookupProperties(BaseModel):
    """Defines the high-level configuration for a Lookup activity."""
    source: Dict[str, Any]  # Flexible source settings
    datasetSettings: Dict[str, Any]  # Flexible dataset settings

class LookupActivity(BaseActivity):
    type: Literal["Lookup"]
    typeProperties: LookupProperties
# ---------------- SqlServerStoredProcedure ----------------

class StoredProcedureParameter(BaseModel):
    value: Any
    type: Optional[str] = Field(None, description="e.g., 'String', 'Int32', etc.")

class SqlServerStoredProcedureProperties(BaseModel):
    storedProcedureName: str
    storedProcedureParameters: Optional[Dict[str, StoredProcedureParameter]] = None

class SqlServerStoredProcedureActivity(BaseActivity):
    type: Literal["SqlServerStoredProcedure"]
    typeProperties: SqlServerStoredProcedureProperties
    linkedServiceName: LinkedServiceReference # <-- The crucial connection reference

class LinkedServiceTypeProperties(BaseModel):
    artifactId: Optional[str] = None
    workspaceId: Optional[str] = None

class LinkedServiceProperties(BaseModel):
    annotations: Optional[List[Any]] = None
    type: Optional[str] = None
    typeProperties: Optional[LinkedServiceTypeProperties] = None

class LinkedService(BaseModel):
    name: str
    properties: LinkedServiceProperties

class SqlServerStoredProcedureActivity(BaseActivity):
    type: Literal["SqlServerStoredProcedure"]
    typeProperties: Optional[SqlServerStoredProcedureProperties] = None
    linkedService: Optional[LinkedService] = None


# ---------------- SetVariable ----------------

class KVValue(BaseModel):
    type: Optional[str] = None       # "String", "Boolean", etc.
    content: Optional[Any] = None

class KVPair(BaseModel):
    key: str
    value: KVValue

ValueUnion = Union[str, List[KVPair]]

class SetVariableProperties(BaseModel):
    variableName: str
    value: Optional[ValueUnion] = None
    setSystemVariable: Optional[bool] = None

class SetVariableActivity(BaseActivity):
    type: Literal["SetVariable"]
    typeProperties: SetVariableProperties

# ---------------- Flow Control ----------------

# IfCondition
class Expression(BaseModel):
    value: Optional[str] = None
    type: Optional[Literal["Expression"]] = None

class IfConditionProperties(BaseModel):
    expression: Optional[Expression] = None
    ifTrueActivities: List["Activity"] = Field(default_factory=list)
    ifFalseActivities: List["Activity"] = Field(default_factory=list)

class IfConditionActivity(BaseActivity):
    type: Literal["IfCondition"]
    typeProperties: IfConditionProperties

# ForEach
class ForEachProperties(BaseModel):
    activities: List["Activity"] = Field(default_factory=list)
    items: Optional[Any] = None  # Add later if you pass array/expr

class ForEachActivity(BaseActivity):
    type: Literal["ForEach"]
    typeProperties: ForEachProperties

# Filter
class FilterProperties(BaseModel):
    """Defines the typeProperties for a Filter activity."""
    items: Expression = Field(..., description="An expression that must evaluate to an array to be filtered.")
    condition: Expression = Field(..., description="A boolean expression to filter items. Use '@item()' to reference an item.")

class FilterActivity(BaseActivity):
    type: Literal["Filter"]
    typeProperties: FilterProperties

# Wait
class WaitProperties(BaseModel):
    waitTimeInSeconds: int

class WaitActivity(BaseActivity):
    type: Literal["Wait"]
    typeProperties: WaitProperties

# Until
class UntilProperties(BaseModel):
    """Defines the typeProperties for an Until activity (do-while loop)."""
    expression: Expression = Field(..., description="An expression that must evaluate to true to terminate the loop.")
    activities: List['Activity'] = Field(..., description="A list of activities to execute in each loop iteration.")
    timeout: Optional[str] = Field("0.12:00:00", description="Timeout for the loop. Default is 12 hours.")

class UntilActivity(BaseActivity):
    type: Literal["Until"]
    typeProperties: UntilProperties

# InvokePipeline
class InvokePipelineProperties(BaseModel):
    """Defines the typeProperties for an InvokePipeline activity, based on ground truth."""
    operationType: Literal["InvokeFabricPipeline"]
    workspaceId: str
    pipelineId: str
    waitOnCompletion: bool = True
    parameters: Optional[Dict[str, Any]] = None

class InvokePipelineActivity(BaseActivity):
    type: Literal["InvokePipeline"]
    typeProperties: InvokePipelineProperties
    externalReferences: ExternalReferences 

# DatabricksNotebook
class DatabricksNotebookProperties(BaseModel):
    newClusterNumOfWorker: Optional[str] = None
    clusterOption: Optional[str] = None
    newClusterInitScripts: Optional[List[Any]] = None

class DatabricksNotebookActivity(BaseActivity):
    type: Literal["DatabricksNotebook"]
    typeProperties: DatabricksNotebookProperties

# FabricSparkJobDefinition
class FabricSparkJobDefinitionProperties(BaseModel):
    workspaceId: str

class FabricSparkJobDefinitionActivity(BaseActivity):
    type: Literal["FabricSparkJobDefinition"]
    typeProperties: FabricSparkJobDefinitionProperties

# ---------- NEW ACTIVITIES ----------

# Fail
class FailProperties(BaseModel):
    message: Optional[str] = None
    errorCode: Optional[str] = None
    model_config = {"extra": "allow"}

class FailActivity(BaseActivity):
    type: Literal["Fail"]
    typeProperties: FailProperties

# WebActivity
class WebActivity(BaseActivity):
    type: Literal["WebActivity"]
    typeProperties: Dict[str, Any] = Field(default_factory=dict)

# WebHook
class WebHookProperties(BaseModel):
    method: Optional[str] = None
    timeout: Optional[str] = None
    model_config = {"extra": "allow"}

class WebHookActivity(BaseActivity):
    type: Literal["WebHook"]
    typeProperties: WebHookProperties

# Office365Outlook
class OfficeInputs(BaseModel):
    method: Optional[str] = None
    path: Optional[str] = None
    body: Optional[Dict[str, Any]] = None
    model_config = {"extra": "allow"}

class Office365OutlookProperties(BaseModel):
    inputs: OfficeInputs
    model_config = {"extra": "allow"}

class Office365OutlookActivity(BaseActivity):
    type: Literal["Office365Outlook"]
    typeProperties: Office365OutlookProperties

# AppendVariable
class AppendVariableProperties(BaseModel):
    variableName: str
    value: Optional[Any] = None
    model_config = {"extra": "allow"}

class AppendVariableActivity(BaseActivity):
    type: Literal["AppendVariable"]
    typeProperties: AppendVariableProperties

# ---------- Switch ----------
class SwitchCase(BaseModel):
    """Defines a case for the Switch activity."""
    value: str
    activities: List['Activity'] = Field(default_factory=list)

class SwitchProperties(BaseModel):
    """Defines the typeProperties for a Switch activity."""
    on: Expression = Field(..., description="The expression to evaluate for the switch condition.")
    cases: List[SwitchCase] = Field(default_factory=list)
    defaultActivities: Optional[List['Activity']] = None

class SwitchActivity(BaseActivity):
    type: Literal["Switch"]
    typeProperties: SwitchProperties

# ---------------- Script Activity ----------------

class ScriptParameter(BaseModel):
    """Defines a parameter for a script - based on verified working structure."""
    name: str = Field(..., description="Parameter name")
    type: str = Field(..., description="Parameter type like String, Int16, Int32, Int64, Boolean, Datetime, Byte[], etc.")
    value: str = Field(..., description="Parameter value")
    direction: Optional[str] = Field(None, description="Input, Output, or InputOutput")

class ScriptText(BaseModel):
    """Script text expression - matches Fabric's exact structure.""" 
    value: str = Field(..., description="The SQL/script text")
    type: Literal["Expression"] = "Expression"

class ScriptItem(BaseModel):
    """Defines a script item - based on verified working structure.""" 
    type: Literal["Query", "NonQuery"] = Field(..., description="Query returns data, NonQuery doesn't")
    text: ScriptText = Field(..., description="The script text as Expression object")
    parameters: Optional[List[ScriptParameter]] = Field(None, description="Script parameters")

class ScriptProperties(BaseModel):
    """Script activity properties - based on verified working structure."""
    scripts: List[ScriptItem] = Field(..., description="List of scripts to execute")
    scriptBlockExecutionTimeout: Optional[str] = Field("02:00:00", description="Timeout in format HH:MM:SS")
    database: Optional[str] = Field(None, description="Database name for SQL Server connections")
    connectionVersion: Optional[str] = Field(None, description="Connection version for some providers")

class ScriptActivity(BaseActivity):
    type: Literal["Script"]
    typeProperties: ScriptProperties
    linkedService: Optional[FabricLinkedService] = Field(None, description="For DataWarehouse connections only")
    externalReferences: Optional[Union[ConnectionRef, Dict[str, Any]]] = Field(None, description="For any external connections - all 52 verified types")
    
    @model_validator(mode='after')
    def validate_connection_pattern(self):
        """Ensure exactly one connection pattern is specified."""
        has_linked_service = self.linkedService is not None
        has_external_ref = self.externalReferences is not None
        
        if not (has_linked_service or has_external_ref):
            raise ValueError("Script activity requires either linkedService (DataWarehouse) or externalReferences (external database)")
        
        if has_linked_service and has_external_ref:
            raise ValueError("Script activity cannot have both linkedService and externalReferences - use one connection pattern")
            
        return self

# ---------------- Generic fallback ----------------

# ---------- Generic fallback ----------
class GenericActivity(BaseActivity):
    # Force a literal so the discriminator is happy
    type: Literal["Generic"]
    typeProperties: Dict[str, Any] = Field(default_factory=dict)

# ---------- Discriminated union ----------
Activity = Annotated[
    Union[
        TridentNotebookActivity,
        TeamsActivity,
        CopyActivity,
        RefreshDataflowActivity,
        GetMetadataActivity,
        LookupActivity,
        SqlServerStoredProcedureActivity,
        SetVariableActivity,
        IfConditionActivity,
        ForEachActivity,
        FilterActivity,
        WaitActivity,
        UntilActivity,
        InvokePipelineActivity,
        DatabricksNotebookActivity,
        FabricSparkJobDefinitionActivity,
        FailActivity,
        WebActivity,
        WebHookActivity,
        Office365OutlookActivity,
        AppendVariableActivity,
        SwitchActivity,
        ScriptActivity,
        GenericActivity,  # keep last
    ],
    Field(discriminator="type"),
]

# Rebuild forward refs (needed for recursive models)
IfConditionProperties.model_rebuild()
ForEachProperties.model_rebuild()
UntilProperties.model_rebuild()
SwitchCase.model_rebuild()
SwitchProperties.model_rebuild()
FilterProperties.model_rebuild() 