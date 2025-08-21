"""
Connection types for Microsoft Fabric Data Pipelines.
Based on verified round-trip testing of all 55 UI-available connection types.
52/55 working with the simple pattern: {ConnectionType}Source -> {ConnectionType}Table
"""

from typing import Dict, Any, Literal, Union, List, Optional
from pydantic import BaseModel, Field

# =============================================================================
# ALL 52 VERIFIED WORKING CONNECTION TYPES
# =============================================================================

# These 51 connection types passed round-trip testing with simple pattern
VERIFIED_CONNECTION_TYPES = [
    # Databases (24 working with simple pattern)
    "SqlServer", "Oracle", "PostgreSql", "MySql", "Db2", "Teradata", "SapHana",
    "AmazonRedshift", "Vertica", "AzureSqlDatabase", "AzureSqlDW",
    "AzurePostgreSql", "AzureSqlMI", "AzureMySql", "Cassandra", "AmazonRDSSqlServer",
    "Greenplum", "MariaDB", "MongoDbAtlas", "MongoDb", "CosmosDbMongoDb", "CosmosDb",
    "AzureDatabricks", "Dataverse",
    
    # Storage (11 working)
    "FileSystem", "Hdfs", "Ftp", "Sftp", "AzureTables", "AzureDataLakeStorageGen2",
    "AzureBlobStorage", "AzureFileStorage", "AmazonS3", "AmazonS3Compatible",
    "GoogleCloudStorage", "OracleCloudStorage",
    
    # Services (7 working)
    "SharePointOnlineList", "Salesforce", "OData", "SalesforceServiceCloud",
    "Dynamics365", "DynamicsAX", "DynamicsCRM", "ServiceNow",
    
    # Other (9 working)
    "Odbc", "Http", "RestService", "SapBWOpenHub", "SapBWMessageServer",
    "SapTableApplication", "SapTableMessage"
]

# 4 that don't work with simple pattern (need special handling)
# GoogleBigQuery uses GoogleBigQueryObject instead of GoogleBigQueryTable
SPECIAL_PATTERN_CONNECTIONS = ["GoogleBigQuery", "Snowflake", "AzureDataExplorer", "Office365"]

class DatabaseConnectionRef(BaseModel):
    """Database connection reference for externalReferences pattern."""
    connection: str = Field(..., description="Connection ID/name")
    connectionType: Literal[
        # All database connections that use externalReferences
        "SqlServer", "Oracle", "PostgreSql", "MySql", "Db2", "Teradata", "SapHana",
        "GoogleBigQuery", "AmazonRedshift", "Vertica", "AzureSqlDatabase", "AzureSqlDW",
        "AzurePostgreSql", "AzureSqlMI", "AzureMySql", "Cassandra", "AmazonRDSSqlServer",
        "Greenplum", "MariaDB", "MongoDbAtlas", "MongoDb", "CosmosDbMongoDb", "CosmosDb",
        "AzureDataExplorer", "AzureDatabricks", "Dataverse", "Snowflake"
    ] = Field(..., description="Database connection type")

class StorageConnectionRef(BaseModel):
    """Storage connection reference for externalReferences pattern."""
    connection: str = Field(..., description="Connection ID/name")
    connectionType: Literal[
        # All storage connections that use externalReferences
        "FileSystem", "Hdfs", "Ftp", "Sftp", "AzureTables", "AzureDataLakeStorageGen2",
        "AzureBlobStorage", "AzureFileStorage", "AmazonS3", "AmazonS3Compatible",
        "GoogleCloudStorage", "OracleCloudStorage"
    ] = Field(..., description="Storage connection type")

class ServiceConnectionRef(BaseModel):
    """Service connection reference for externalReferences pattern."""
    connection: str = Field(..., description="Connection ID/name")
    connectionType: Literal[
        # All service connections that use externalReferences
        "SharePointOnlineList", "Salesforce", "OData", "SalesforceServiceCloud",
        "Dynamics365", "DynamicsAX", "DynamicsCRM", "Office365", "ServiceNow"
    ] = Field(..., description="Service connection type")

class OtherConnectionRef(BaseModel):
    """Other connection types for externalReferences pattern."""
    connection: str = Field(..., description="Connection ID/name")
    connectionType: Literal[
        # Other connection types
        "Odbc", "Http", "RestService", "SapBWOpenHub", "SapBWMessageServer",
        "SapTableApplication", "SapTableMessage"
    ] = Field(..., description="Other connection type")

# Union type for any connection reference
ConnectionRef = Union[DatabaseConnectionRef, StorageConnectionRef, ServiceConnectionRef, OtherConnectionRef]

class FabricLinkedService(BaseModel):
    """Fabric-native linked service for DataWarehouse/Lakehouse pattern."""
    name: str = Field(..., description="Linked service name")
    properties: Dict[str, Any] = Field(..., description="Linked service properties")

# =============================================================================
# CONNECTION BUILDERS - Convert high-level to API payloads
# =============================================================================

def build_database_connection_ref(connection_id: str, connection_type: str) -> Dict[str, Any]:
    """Build externalReferences for database connections."""
    return {
        "connection": connection_id,
        "connectionType": connection_type
    }

def build_fabric_linkedservice(
    name: str, 
    service_type: Literal["DataWarehouse", "Lakehouse"],
    artifact_id: str,
    workspace_id: str
) -> Dict[str, Any]:
    """Build Fabric native linked service."""
    return {
        "name": name,
        "properties": {
            "type": service_type,
            "typeProperties": {
                "artifactId": artifact_id,
                "workspaceId": workspace_id
            }
        }
    }

# =============================================================================
# HELPER FUNCTIONS - Create activities using verified patterns
# =============================================================================

def create_copy_source_from_connection(connection_type: str, connection_id: str = "placeholder") -> Dict[str, Any]:
    """
    Create a Copy activity source using the verified simple pattern.
    Pattern: {ConnectionType}Source with {ConnectionType}Table dataset
    """
    if connection_type not in VERIFIED_CONNECTION_TYPES:
        if connection_type not in SPECIAL_PATTERN_CONNECTIONS:
            raise ValueError(f"Unknown connection type: {connection_type}")
        # Special patterns would be handled here when we get golden JSONs
        raise NotImplementedError(f"{connection_type} requires special pattern (not yet implemented)")
    
    return {
        "type": f"{connection_type}Source",
        "datasetSettings": {
            "type": f"{connection_type}Table",
            "externalReferences": {"connection": connection_id}
        }
    }

def create_copy_sink_from_connection(connection_type: str, connection_id: str = "placeholder") -> Dict[str, Any]:
    """
    Create a Copy activity sink using the verified simple pattern.
    Pattern: {ConnectionType}Sink with {ConnectionType}Table dataset
    """
    if connection_type not in VERIFIED_CONNECTION_TYPES:
        if connection_type not in SPECIAL_PATTERN_CONNECTIONS:
            raise ValueError(f"Unknown connection type: {connection_type}")
        raise NotImplementedError(f"{connection_type} requires special pattern (not yet implemented)")
    
    return {
        "type": f"{connection_type}Sink",
        "datasetSettings": {
            "type": f"{connection_type}Table",
            "externalReferences": {"connection": connection_id}
        }
    }

def is_valid_connection_type(connection_type: str) -> bool:
    """Check if a connection type is valid (either verified or special pattern)."""
    return connection_type in VERIFIED_CONNECTION_TYPES or connection_type in SPECIAL_PATTERN_CONNECTIONS

def get_connection_category(connection_type: str) -> Optional[str]:
    """Get the category of a connection type."""
    if connection_type in ["SqlServer", "Oracle", "PostgreSql", "MySql", "Db2", "Teradata", "SapHana",
                           "GoogleBigQuery", "AmazonRedshift", "Vertica", "AzureSqlDatabase", "AzureSqlDW",
                           "AzurePostgreSql", "AzureSqlMI", "AzureMySql", "Cassandra", "AmazonRDSSqlServer",
                           "Greenplum", "MariaDB", "MongoDbAtlas", "MongoDb", "CosmosDbMongoDb", "CosmosDb",
                           "AzureDataExplorer", "AzureDatabricks", "Dataverse", "Snowflake"]:
        return "database"
    elif connection_type in ["FileSystem", "Hdfs", "Ftp", "Sftp", "AzureTables", "AzureDataLakeStorageGen2",
                             "AzureBlobStorage", "AzureFileStorage", "AmazonS3", "AmazonS3Compatible",
                             "GoogleCloudStorage", "OracleCloudStorage"]:
        return "storage"
    elif connection_type in ["SharePointOnlineList", "Salesforce", "OData", "SalesforceServiceCloud",
                             "Dynamics365", "DynamicsAX", "DynamicsCRM", "Office365", "ServiceNow"]:
        return "service"
    elif connection_type in ["Odbc", "Http", "RestService", "SapBWOpenHub", "SapBWMessageServer",
                             "SapTableApplication", "SapTableMessage"]:
        return "other"
    return None
