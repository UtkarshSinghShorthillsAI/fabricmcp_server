# Flexible Copy Activity schemas based on REAL working Fabric API patterns
# Eliminates overfitting by matching actual API structure
#
# ✅ ROUND-TRIP VERIFIED PATTERNS (Latest Session - API 200 + UI Persistence):
# - AzurePostgreSql → Lakehouse (200 OK)
# - AzureSqlDatabase → Lakehouse (200 OK)
# - Teradata → Lakehouse (200 OK)
# - GoogleCloudStorage → Lakehouse (200 OK)
# - AzureSqlDW → Lakehouse (200 OK)
# - PostgreSql → Lakehouse (200 OK)
# - Db2 → Lakehouse (200 OK)
#
# ✅ PREVIOUSLY VERIFIED PATTERNS:
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
    """Flexible source that matches real Fabric API patterns.
    
    Example for SqlServer source:
    {
        "type": "SqlServerSource",
        "datasetSettings": {
            "type": "SqlServerTable",
            "externalReferences": {"connection": "your_connection_id"}
        }
    }
    """
    type: str = Field(..., description="Source type, e.g., 'SqlServerSource', 'OracleSource', 'AzureBlobStorageSource'")
    storeSettings: Optional[StoreSettings] = Field(None, description="Storage settings for file-based sources")
    formatSettings: Optional[FormatSettings] = Field(None, description="Format settings for file-based sources")
    datasetSettings: Optional[DatasetSettings] = Field(None, description="Dataset configuration. For external connections, include 'externalReferences': {'connection': 'connection_id'}")
    # SQL-specific properties
    sqlReaderQuery: Optional[str] = None
    oracleReaderQuery: Optional[str] = None
    queryTimeout: Optional[str] = None
    query: Optional[str] = None
    # Allow any additional properties
    class Config:
        extra = "allow"

class FlexibleSink(BaseModel):
    """Flexible sink that matches real Fabric API patterns.
    
    Example for Lakehouse sink:
    {
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
    """
    type: str = Field(..., description="Sink type, e.g., 'LakehouseTableSink', 'DataWarehouseSink', 'AzureBlobStorageTable'")
    storeSettings: Optional[StoreSettings] = Field(None, description="Storage settings for file-based sinks")
    formatSettings: Optional[FormatSettings] = Field(None, description="Format settings for file-based sinks")
    datasetSettings: Optional[DatasetSettings] = Field(None, description="Dataset configuration. For Lakehouse/DataWarehouse, include linkedService. For external, include externalReferences")
    # Table-specific properties
    tableOption: Optional[str] = Field(None, description="Table creation option, e.g., 'autoCreate'")
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
# CENTRALIZED DATASET SETTINGS BUILDERS
# =============================================================================

def create_database_dataset_settings(connection_id: str, dataset_type: str, schema: str, table: str) -> DatasetSettings:
    """Centralized builder for database table dataset settings"""
    return DatasetSettings(
        type=f"{dataset_type}Table",
        typeProperties=TypeProperties(
            schema=schema,
            table=table
        ),
        externalReferences={"connection": connection_id}
    )

def create_storage_dataset_settings(connection_id: str, storage_type: str, location_type: str) -> DatasetSettings:
    """Centralized builder for storage dataset settings"""
    return DatasetSettings(
        type=storage_type,
        typeProperties=TypeProperties(
            location=LocationSettings(type=location_type)
        ),
        externalReferences={"connection": connection_id}
    )

def create_fabric_dataset_settings(service_type: str, artifact_id: str, workspace_id: str, table_name: str = None, service_name: str = "test_service") -> DatasetSettings:
    """Centralized builder for Fabric native service dataset settings"""
    type_properties = TypeProperties()
    if table_name:
        type_properties.table = table_name
        
    return DatasetSettings(
        type=f"{service_type}Table" if table_name else service_type,
        typeProperties=type_properties,
        linkedService=LinkedService(
            name=service_name,
            properties={
                "type": service_type,
                "typeProperties": {
                    "artifactId": artifact_id,
                    "workspaceId": workspace_id
                }
            }
        )
    )

# =============================================================================
# HELPER FUNCTIONS FOR COMMON PATTERNS (Using Centralized Builders)
# =============================================================================

def create_s3_source(connection_id: str, **kwargs) -> FlexibleSource:
    """Create S3 source matching real UI pattern - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="BinarySource",
        storeSettings=StoreSettings(
            type="AmazonS3ReadSettings",
            recursive=kwargs.get("recursive", True)
        ),
        formatSettings=FormatSettings(type="BinaryReadSettings"),
        datasetSettings=create_storage_dataset_settings(
            connection_id=connection_id,
            storage_type="Binary",
            location_type="AmazonS3Location"
        )
    )

def create_lakehouse_table_sink(lakehouse_id: str, workspace_id: str, table_name: str, **kwargs) -> FlexibleSink:
    """Create Lakehouse table sink matching real UI pattern - USING CENTRALIZED BUILDER"""
    return FlexibleSink(
        type="LakehouseTableSink",
        datasetSettings=create_fabric_dataset_settings(
            service_type="Lakehouse",
            artifact_id=lakehouse_id,
            workspace_id=workspace_id,
            table_name=table_name,
            service_name=kwargs.get("lakehouse_name", "test_lakehouse")
        )
    )

def create_azureblob_source(connection_id: str, **kwargs) -> FlexibleSource:
    """Create Azure Blob source matching real patterns - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="DelimitedTextSource",
        storeSettings=StoreSettings(
            type="AzureBlobStorageReadSettings",
            recursive=kwargs.get("recursive", True)
        ),
        formatSettings=FormatSettings(type="DelimitedTextReadSettings"),
        datasetSettings=create_storage_dataset_settings(
            connection_id=connection_id,
            storage_type="DelimitedText",
            location_type="AzureBlobStorageLocation"
        )
    )

def create_sqlserver_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create SQL Server source - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="SqlServerSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="SqlServer",
            schema=kwargs.get("schema", "dbo"),
            table=kwargs.get("table", "test_table")
        )
    )

def create_oracle_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create Oracle source - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="OracleSource",
        oracleReaderQuery=query,
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="Oracle",
            schema=kwargs.get("schema", "HR"),
            table=kwargs.get("table", "test_table")
        )
    )

# =============================================================================
# ROUND-TRIP VERIFIED PATTERNS (Latest Session - API 200 + UI Persistence)
# =============================================================================

def create_azuresqldatabase_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create Azure SQL Database source - ROUND-TRIP VERIFIED ✅ - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="AzureSqlSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="AzureSql",
            schema=kwargs.get("schema", "dbo"),
            table=kwargs.get("table", "test_table")
        )
    )

def create_teradata_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create Teradata source - ROUND-TRIP VERIFIED ✅ - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="TeradataSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="Teradata",
            schema=kwargs.get("schema", "DBC"),
            table=kwargs.get("table", "test_table")
        )
    )

def create_azuresqldw_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create Azure Synapse Analytics (SQL DW) source - ROUND-TRIP VERIFIED ✅ - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="AzureSqlDWSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="AzureSqlDW",
            schema=kwargs.get("schema", "dbo"),
            table=kwargs.get("table", "test_table")
        )
    )

def create_postgresql_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create PostgreSQL source - ROUND-TRIP VERIFIED ✅ - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="PostgreSqlSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="PostgreSql",
            schema=kwargs.get("schema", "public"),
            table=kwargs.get("table", "test_table")
        )
    )

def create_db2_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create IBM Db2 source - ROUND-TRIP VERIFIED ✅ - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="Db2Source",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="Db2",
            schema=kwargs.get("schema", "DB2ADMIN"),
            table=kwargs.get("table", "test_table")
        )
    )

# =============================================================================
# PREVIOUSLY VERIFIED PATTERNS (Earlier Testing)
# =============================================================================

def create_mysql_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create MySQL source - VERIFIED WORKING ✅ - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="MySqlSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="MySql",
            schema=kwargs.get("schema", "dbo"),
            table=kwargs.get("table", "test_table")
        )
    )

def create_azurepostgresql_source(connection_id: str, query: str, **kwargs) -> FlexibleSource:
    """Create Azure PostgreSQL source - ROUND-TRIP VERIFIED ✅ - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="AzurePostgreSqlSource",
        sqlReaderQuery=query,
        queryTimeout=kwargs.get("queryTimeout", "02:00:00"),
        datasetSettings=create_database_dataset_settings(
            connection_id=connection_id,
            dataset_type="AzurePostgreSql",
            schema=kwargs.get("schema", "dbo"),
            table=kwargs.get("table", "test_table")
        )
    )

def create_googlecloudstorage_source(connection_id: str, **kwargs) -> FlexibleSource:
    """Create Google Cloud Storage source - ROUND-TRIP VERIFIED ✅ - USING CENTRALIZED BUILDER"""
    return FlexibleSource(
        type="GoogleCloudStorageSource",
        storeSettings=StoreSettings(
            type="GoogleCloudStorageReadSettings",
            recursive=kwargs.get("recursive", True)
        ),
        formatSettings=FormatSettings(type="GoogleCloudStorageReadSettings"),
        datasetSettings=create_storage_dataset_settings(
            connection_id=connection_id,
            storage_type="GoogleCloudStorage",
            location_type="GoogleCloudStorageLocation"
        )
    )
