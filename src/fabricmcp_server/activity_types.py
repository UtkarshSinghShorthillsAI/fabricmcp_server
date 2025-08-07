# fabric_mcp_server/activity_types.py
from __future__ import annotations
from typing import List, Optional, Literal, Dict, Any, Union, Annotated
from pydantic import BaseModel, Field, model_validator
from .copy_activity_schemas import SourceConfig, SinkConfig, TabularTranslator, DatasetReference
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

class CopyProperties(BaseModel):
    """Defines the high-level, user-friendly configuration for a Copy activity."""
    source: SourceConfig
    sink: SinkConfig
    translator: Optional[TabularTranslator] = None
    enableStaging: bool = False

class CopyActivity(BaseActivity):
    """The final, robust Copy Activity model."""
    type: Literal["Copy"]
    typeProperties: CopyProperties
    inputs: List[DatasetReference]
    outputs: List[DatasetReference]


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
# (No typeProperties in your sample—keep optional dict)

class GetMetadataActivity(BaseActivity):
    type: Literal["GetMetadata"]
    typeProperties: Optional[Dict[str, Any]] = Field(default_factory=dict)

# ---------------- Lookup ----------------

class LookupActivity(BaseActivity):
    type: Literal["Lookup"]
    typeProperties: Optional[Dict[str, Any]] = Field(default_factory=dict)

# ---------------- SqlServerStoredProcedure ----------------

class StoredProcParam(BaseModel):
    value: Any
    type: Optional[str] = None   # "Boolean", "String", etc.

# No RootModel, just use a dict field directly
class SqlServerStoredProcedureProperties(BaseModel):
    storedProcedureParameters: Optional[Dict[str, StoredProcParam]] = None

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
    # your sample shows no typeProperties — keep as empty
    dummy: Optional[str] = None

class FilterActivity(BaseActivity):
    type: Literal["Filter"]
    typeProperties: Optional[FilterProperties] = Field(default_factory=dict)

# Wait
class WaitProperties(BaseModel):
    waitTimeInSeconds: int

class WaitActivity(BaseActivity):
    type: Literal["Wait"]
    typeProperties: WaitProperties

# Until
class UntilProperties(BaseModel):
    activities: List["Activity"] = Field(default_factory=list)
    timeout: Optional[str] = None

class UntilActivity(BaseActivity):
    type: Literal["Until"]
    typeProperties: UntilProperties

# InvokePipeline
class InvokePipelineProperties(BaseModel):
    waitOnCompletion: Optional[bool] = True
    operationType: Optional[str] = "InvokeFabricPipeline"
    pipeline: Optional[str] = None  # if you later pass target pipeline id
    parameters: Optional[Dict[str, Any]] = None

class InvokePipelineActivity(BaseActivity):
    type: Literal["InvokePipeline"]
    typeProperties: InvokePipelineProperties

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
    value: Any
    activities: List["Activity"] = Field(default_factory=list)
    model_config = {"extra": "allow"}

class SwitchProperties(BaseModel):
    # canonical form
    expression: Optional["Expression"] = None
    # accept LLM's shorthand; field name is on_, aliased to JSON key "on"
    on_: Optional[str] = Field(None, alias="on")

    cases: List[SwitchCase] = Field(default_factory=list)
    defaultActivities: List["Activity"] = Field(default_factory=list)

    model_config = {
        "extra": "allow",
        "populate_by_name": True,
    }

    @model_validator(mode="before")
    @classmethod
    def coerce_on_to_expression(cls, data: Dict[str, Any]):
        """Convert {'on': '@expr'} to {'expression': {...}} so Fabric accepts it."""
        if "on" in data and "expression" not in data:
            data["expression"] = {"type": "Expression", "value": data.pop("on")}
        return data

class SwitchActivity(BaseActivity):
    type: Literal["Switch"]
    typeProperties: SwitchProperties


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
        GenericActivity,  # keep last
    ],
    Field(discriminator="type"),
]

# Rebuild forward refs (needed for recursive models)
IfConditionProperties.model_rebuild()
ForEachProperties.model_rebuild()
UntilProperties.model_rebuild()
SwitchProperties.model_rebuild()
