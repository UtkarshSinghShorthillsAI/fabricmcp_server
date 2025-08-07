from typing import Any, Dict, List, Optional

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
    format: str = Field(..., description="The format of the definition, e.g., 'ipynb' for notebooks or 'pbidataset' for Power BI datasets.")
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

# --- Models for Connections ---

class ConnectionDetails(BaseModel):
    """Represents the comprehensive details of a Fabric connection."""
    id: str
    display_name: str
    connection_type: str
    connectivity_type: str
    credential_type: str
    connection_path: Optional[str] = None
    privacy_level: str
    allow_gateway_usage: bool
    gateway_id: Optional[str] = None