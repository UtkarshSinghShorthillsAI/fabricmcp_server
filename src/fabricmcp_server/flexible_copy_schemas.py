# Flexible Copy Activity schemas based on REAL working Fabric API patterns
# Eliminates overfitting by matching actual API structure
#
# ✅ VERIFIED WORKING PATTERNS (from systematic testing):
# - AzurePostgreSql → Lakehouse (200 OK)
# - GoogleCloudStorage → Lakehouse (200 OK) 
# - MySql → Lakehouse (200 OK)
# - SqlServer → Lakehouse (200 OK)
# - Oracle → DataWarehouse (200 OK)
# - Azure Blob → Lakehouse File (200 OK)

from __future__ import annotations
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from .common_schemas import DatasetReference, TabularTranslator

# =============================================================================
# FLEXIBLE API-ALIGNED MODELS (Based on Real Working Patterns)
# =============================================================================

class StoreSettings(BaseModel):
    """Flexible store settings - matches real API patterns"""
    type: str
    recursive: Optional[bool] = None
    wildcardFolderPath: Optional[str] = None
    wildcardFileName: Optional[str] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class FormatSettings(BaseModel):
    """Flexible format settings - matches real API patterns"""
    type: str
    skipLineCount: Optional[int] = None
    compressionProperties: Optional[Dict[str, Any]] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class LocationSettings(BaseModel):
    """Flexible location settings - matches real API patterns"""
    type: str
    bucketName: Optional[str] = None
    folderPath: Optional[str] = None
    fileName: Optional[str] = None
    container: Optional[str] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class TypeProperties(BaseModel):
    """Flexible type properties for datasets"""
    location: Optional[LocationSettings] = None
    table: Optional[str] = None
    schema: Optional[str] = None
    artifactId: Optional[str] = None
    workspaceId: Optional[str] = None
    rootFolder: Optional[str] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class LinkedService(BaseModel):
    """Flexible linked service definition"""
    name: str
    properties: Dict[str, Any]

class DatasetSettings(BaseModel):
    """Flexible dataset settings - matches real API patterns"""
    type: str
    typeProperties: Optional[TypeProperties] = None
    annotations: Optional[List[Any]] = None
    externalReferences: Optional[Dict[str, str]] = None
    linkedService: Optional[LinkedService] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class FlexibleSource(BaseModel):
    """Flexible source that matches real Fabric API patterns"""
    type: str
    storeSettings: Optional[StoreSettings] = None
    formatSettings: Optional[FormatSettings] = None
    datasetSettings: Optional[DatasetSettings] = None
    # SQL-specific properties
    sqlReaderQuery: Optional[str] = None
    oracleReaderQuery: Optional[str] = None
    queryTimeout: Optional[str] = None
    query: Optional[str] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class FlexibleSink(BaseModel):
    """Flexible sink that matches real Fabric API patterns"""
    type: str
    storeSettings: Optional[StoreSettings] = None
    formatSettings: Optional[FormatSettings] = None
    datasetSettings: Optional[DatasetSettings] = None
    # Table-specific properties
    tableOption: Optional[str] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class FlexibleCopyProperties(BaseModel):
    """Flexible Copy Properties matching real API patterns"""
    source: Optional[FlexibleSource] = None
    sink: Optional[FlexibleSink] = None
    translator: Optional[Union[TabularTranslator, Dict[str, Any]]] = None
    enableStaging: Optional[bool] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class FlexibleCopyActivity(BaseModel):
    """Flexible Copy Activity matching real Fabric API patterns"""
    name: str
    type: str = "Copy"
    typeProperties: FlexibleCopyProperties
    inputs: Optional[List[DatasetReference]] = None
    outputs: Optional[List[DatasetReference]] = None
    dependsOn: Optional[List[Dict[str, Any]]] = None
    policy: Optional[Dict[str, Any]] = None
    state: Optional[str] = None
    onInactiveMarkAs: Optional[str] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

# =============================================================================
# HELPER FUNCTIONS FOR COMMON PATTERNS
# =============================================================================

def create_s3_source(connection_id: str, **kwargs) -> FlexibleSource:
    """Create S3 source matching real UI pattern"""
    return FlexibleSource(
        type="BinarySource",
        storeSettings=StoreSettings(
            type="AmazonS3ReadSettings",
            recursive=kwargs.get("recursive", True)
        ),
        formatSettings=FormatSettings(type="BinaryReadSettings"),
        datasetSettings=DatasetSettings(
            type="Binary",
            typeProperties=TypeProperties(
                location=LocationSettings(type="AmazonS3Location")
            ),
            externalReferences={"connection": connection_id}
        )
    )

def create_lakehouse_table_sink(lakehouse_id: str, workspace_id: str, table_name: str, **kwargs) -> FlexibleSink:
    """Create Lakehouse table sink matching real UI pattern"""
    return FlexibleSink(
        type="LakehouseTableSink",
        datasetSettings=DatasetSettings(
            type="LakehouseTable",
            typeProperties=TypeProperties(table=table_name),
            linkedService=LinkedService(
                name=kwargs.get("lakehouse_name", "test_lakehouse"),
                properties={
                    "type": "Lakehouse",
                    "typeProperties": {
                        "artifactId": lakehouse_id,
                        "workspaceId": workspace_id
                    }
                }
            )
        )
    )

def create_azureblob_source(connection_id: str, **kwargs) -> FlexibleSource:
    """Create Azure Blob source matching real patterns"""
    return FlexibleSource(
        type="DelimitedTextSource",
        storeSettings=StoreSettings(
            type="AzureBlobStorageReadSettings",
            recursive=kwargs.get("recursive", True)
        ),
        formatSettings=FormatSettings(type="DelimitedTextReadSettings"),
        datasetSettings=DatasetSettings(
            type="DelimitedText",
            typeProperties=TypeProperties(
                location=LocationSettings(type="AzureBlobStorageLocation")
            ),
            externalReferences={"connection": connection_id}
        )
    )

def create_sqlserver_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create SQL Server source matching real patterns"""
    return FlexibleSource(
        type="SqlServerSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=DatasetSettings(
            type="SqlServerTable",
            typeProperties=TypeProperties(
                schema=kwargs.get("schema", "dbo"),
                table=kwargs.get("table", "test_table")
            ),
            externalReferences={"connection": connection_id}
        )
    )

def create_oracle_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create Oracle source matching real patterns"""
    return FlexibleSource(
        type="OracleSource",
        oracleReaderQuery=query,
        datasetSettings=DatasetSettings(
            type="OracleTable",
            typeProperties=TypeProperties(
                schema=kwargs.get("schema", "HR"),
                table=kwargs.get("table", "test_table")
            ),
            externalReferences={"connection": connection_id}
        )
    )

# =============================================================================
# NEWLY VERIFIED WORKING PATTERNS (Systematic Testing Results)
# =============================================================================

def create_mysql_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create MySQL source matching real API patterns - VERIFIED WORKING ✅"""
    return FlexibleSource(
        type="MySqlSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=DatasetSettings(
            type="MySqlTable",
            typeProperties=TypeProperties(
                schema=kwargs.get("schema", "dbo"),
                table=kwargs.get("table", "test_table")
            ),
            externalReferences={"connection": connection_id}
        )
    )

def create_azurepostgresql_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create Azure PostgreSQL source matching real API patterns - VERIFIED WORKING ✅"""
    return FlexibleSource(
        type="AzurePostgreSqlSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=DatasetSettings(
            type="AzurePostgreSqlTable",
            typeProperties=TypeProperties(
                schema=kwargs.get("schema", "dbo"),
                table=kwargs.get("table", "test_table")
            ),
            externalReferences={"connection": connection_id}
        )
    )

def create_googlecloudstorage_source(connection_id: str, **kwargs) -> FlexibleSource:
    """Create Google Cloud Storage source matching real API patterns - VERIFIED WORKING ✅"""
    return FlexibleSource(
        type="GoogleCloudStorageSource",
        storeSettings=StoreSettings(
            type="GoogleCloudStorageReadSettings",
            recursive=kwargs.get("recursive", True)
        ),
        formatSettings=FormatSettings(type="GoogleCloudStorageReadSettings"),
        datasetSettings=DatasetSettings(
            type="GoogleCloudStorage",
            typeProperties=TypeProperties(
                location=LocationSettings(type="GoogleCloudStorageLocation")
            ),
            externalReferences={"connection": connection_id}
        )
    )
