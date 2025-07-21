# In src/fabricmcp_server/fabric_models.py

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl

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

class ItemDefinition(BaseModel):
    parts: List[DefinitionPart]
    
    model_config = {"populate_by_name": True}

class ItemEntity(BaseModel):
    id: Optional[str] = Field(None, description="The item ID")
    workspace_id: Optional[str] = Field(None, alias="workspaceId", description="The workspace ID")
    type: Optional[str] = Field(None, description="The type of the item (e.g., 'Lakehouse', 'Notebook')")
    display_name: Optional[str] = Field(None, alias="displayName", description="The display name of the item")
    description: Optional[str] = Field(None, description="The description of the item")
    definition: Optional[ItemDefinition] = Field(None, description="The definition of the item")
    
    model_config = {"populate_by_name": True}

class CreateItemRequest(BaseModel):
    display_name: str = Field(..., alias="displayName")
    type: str
    description: Optional[str] = None
    workspace_id: Optional[str] = Field(None, alias="workspaceId") # Usually in path, but can be useful
    definition: Optional[ItemDefinition] = None # For creation with definition
    
    model_config = {"populate_by_name": True}

class UpdateItemDefinitionRequest(BaseModel):
    display_name: str = Field(..., alias="displayName")
    type: str
    definition: ItemDefinition

    model_config = {"populate_by_name": True}