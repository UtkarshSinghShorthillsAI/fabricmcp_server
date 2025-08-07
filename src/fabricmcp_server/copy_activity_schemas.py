# This is the final, definitive, and correct file: src/fabricmcp_server/copy_activity_schemas.py

from __future__ import annotations
from typing import List, Optional, Literal, Dict, Any, Union, Annotated
from pydantic import BaseModel, Field

# =============================================================================
#  GENERIC & REUSABLE COMPONENTS
# =============================================================================
class ExternalReferences(BaseModel):
    connection: str

class DatasetReference(BaseModel):
    referenceName: str
    type: Literal["DatasetReference"] = "DatasetReference"

class TabularTranslator(BaseModel):
    type: Literal["TabularTranslator"] = "TabularTranslator"

# =============================================================================
#  SOURCE MODELS (User-Facing, High-Level Schemas)
# =============================================================================

class S3Source(BaseModel):
    connector_type: Literal["S3"]
    connection_id: str
    bucket_name: str
    folder_path: str
    file_name: str

class LakehouseTableSource(BaseModel):
    connector_type: Literal["LakehouseTable"]
    workspace_id: str
    lakehouse_name: str
    lakehouse_id: str
    table_name: str

# =============================================================================
#  SINK MODELS (User-Facing, High-Level Schemas)
# =============================================================================

class LakehouseFileSink(BaseModel):
    connector_type: Literal["LakehouseFile"]
    workspace_id: str
    lakehouse_name: str
    lakehouse_id: str
    folder_path: str
    file_name: str

class DataWarehouseSink(BaseModel):
    connector_type: Literal["DataWarehouse"]
    workspace_id: str
    warehouse_name: str
    warehouse_id: str
    table_name: str

class GCS_Sink(BaseModel):
    connector_type: Literal["GCS"]
    connection_id: str
    bucket_name: str
    folder_path: Optional[str] = None
    file_name: Optional[str] = None

# =============================================================================
#  MASTER UNIONS & API PAYLOAD BUILDERS
# =============================================================================

# These unions use the unique 'connector_type' to choose the right model.
SourceConfig = Annotated[Union[S3Source, LakehouseTableSource], Field(discriminator="connector_type")]
SinkConfig = Annotated[Union[LakehouseFileSink, DataWarehouseSink, GCS_Sink], Field(discriminator="connector_type")]

def build_source_payload(source: SourceConfig) -> Dict[str, Any]:
    """Builds the final API-compliant JSON for a source."""
    if isinstance(source, S3Source):
        return {
            "type": "BinarySource",
            "storeSettings": {"type": "AmazonS3ReadSettings", "recursive": True},
            "formatSettings": {"type": "BinaryReadSettings"},
            "datasetSettings": {
                "type": "Binary",
                "typeProperties": {
                    "location": {
                        "type": "AmazonS3Location",
                        "bucketName": source.bucket_name,
                        "folderPath": source.folder_path,
                        "fileName": source.file_name,
                    }
                },
                "externalReferences": {"connection": source.connection_id},
            },
        }
    if isinstance(source, LakehouseTableSource):
        return {
            "type": "LakehouseTableSource",
            "datasetSettings": {
                "type": "LakehouseTable",
                "typeProperties": {"table": source.table_name},
                "linkedService": {
                    "name": source.lakehouse_name,
                    "properties": {
                        "type": "Lakehouse",
                        "typeProperties": {
                            "workspaceId": source.workspace_id,
                            "artifactId": source.lakehouse_id,
                            "rootFolder": "Tables",
                        }
                    }
                }
            }
        }
    raise NotImplementedError(f"Source type '{source.connector_type}' is not supported.")

def build_sink_payload(sink: SinkConfig) -> Dict[str, Any]:
    """Builds the final API-compliant JSON for a sink."""
    if isinstance(sink, LakehouseFileSink):
        return {
            "type": "DelimitedTextSink",
            "storeSettings": {"type": "LakehouseWriteSettings"},
            "formatSettings": {"type": "DelimitedTextWriteSettings", "fileExtension": ".csv"},
            "datasetSettings": {
                "type": "DelimitedText",
                "typeProperties": {
                    "location": {
                        "type": "LakehouseLocation",
                        "folderPath": sink.folder_path,
                        "fileName": sink.file_name,
                    }
                },
                "linkedService": {
                    "name": sink.lakehouse_name,
                    "properties": {
                        "type": "Lakehouse",
                        "typeProperties": {
                            "workspaceId": sink.workspace_id,
                            "artifactId": sink.lakehouse_id,
                            "rootFolder": "Files",
                        }
                    }
                },
            },
        }
    if isinstance(sink, DataWarehouseSink):
        return {
            "type": "DataWarehouseSink",
            "allowCopyCommand": True,
            "datasetSettings": {
                "type": "DataWarehouseTable",
                "typeProperties": {"table": sink.table_name},
                "linkedService": {
                    "name": sink.warehouse_name,
                    "properties": {
                        "type": "DataWarehouse",
                        "typeProperties": {
                            "workspaceId": sink.workspace_id,
                            "artifactId": sink.warehouse_id,
                        }
                    }
                },
            },
        }
    if isinstance(sink, GCS_Sink):
        return {
            "type": "BinarySink",
            "storeSettings": {"type": "GoogleCloudStorageWriteSettings"},
            "datasetSettings": {
                "type": "Binary",
                "typeProperties": {
                    "location": {
                        "type": "GoogleCloudStorageLocation",
                        "bucketName": sink.bucket_name,
                        "folderPath": sink.folder_path,
                        "fileName": sink.file_name,
                    }
                },
                "externalReferences": {"connection": sink.connection_id},
            },
        }
    raise NotImplementedError(f"Sink type '{sink.connector_type}' is not supported.")