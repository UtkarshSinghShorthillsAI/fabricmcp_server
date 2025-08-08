# This is a new file: src/fabricmcp_server/common_schemas.py

from __future__ import annotations
from typing import Literal, Optional, Dict, Any, List
from pydantic import BaseModel, Field

class Expression(BaseModel):
    """Represents a dynamic content expression in a Fabric pipeline."""
    value: str = Field(..., description="The expression string, e.g., '@pipeline().Pipeline' or '@variables(\\'myVar\\')'")
    type: Literal["Expression"] = "Expression"
class PipelineReference(BaseModel):
    """Represents a reference to another pipeline within Fabric."""
    referenceName: str = Field(..., description="The ID of the pipeline to be executed.")
    type: Literal["PipelineReference"] = "PipelineReference"

class ExternalReferences(BaseModel):
    connection: str

class DatasetReference(BaseModel):
    """Represents a reference to a Dataset (Semantic Model) item."""
    referenceName: str = Field(..., description="The ID of the referenced Dataset.")
    type: Literal["DatasetReference"] = "DatasetReference"
    parameters: Optional[Dict[str, Any]] = None

class TabularTranslator(BaseModel):
    """Defines a schema mapping translator for a Copy activity."""
    type: Literal["TabularTranslator"] = "TabularTranslator"