"""
Modular connection types based on Azure Data Factory ground truth.
Can be used across Script, Copy, and other activities.
"""

from typing import Dict, Any, Literal, Union, List
from pydantic import BaseModel, Field

# =============================================================================
# CONNECTION TYPES - Based on verified Fabric + ADF ground truth
# =============================================================================

class DatabaseConnectionRef(BaseModel):
    """Database connection reference for externalReferences pattern - VERIFIED WORKING types only."""
    connection: str = Field(..., description="Connection ID/name")
    connectionType: Literal[
        "SqlServer",        # ✅ Verified (Script ✓, Copy ✓)
        "Oracle",           # ✅ Verified (Script ✓, Copy ✓)  
        "PostgreSql",       # ✅ Verified (Script ✓)
        "MySql",            # ✅ Verified (Script ✓)
        "Snowflake",        # ✅ Verified (Script ✓)
        "Db2",              # ✅ Verified (Script ✓) - NEW!
        "Teradata",         # ✅ Verified (Script ✓) - NEW!
        "SapHana",          # ✅ Verified (Script ✓) - NEW!
        "GoogleBigQuery",   # ✅ Verified (Script ✓) - NEW!
        "AmazonRedshift",   # ✅ Verified (Script ✓) - NEW!
        "AzureSqlDatabase", # ✅ Verified (Script ✓) - JUST TESTED!
        "AzureSqlDW",       # ✅ Verified (Script ✓) - Azure Synapse Analytics 
        "AzureSqlMI",       # ✅ Verified (Script ✓) - Azure SQL Managed Instance
        "AzurePostgreSql",  # ✅ Verified (Script ✓) - Azure Database for PostgreSQL
        "MariaDB",          # ✅ Verified (Script ✓) - MariaDB for Pipeline
        "CosmosDb",         # ✅ Verified (Script ✓) - Azure Cosmos DB v2
        "AzureDataExplorer" # ✅ Verified (Script ✓) - Azure Data Explorer (Kusto)
    ] = Field(..., description="Database connection type - verified working in Fabric")

class StorageConnectionRef(BaseModel):
    """Storage connection reference for externalReferences pattern - VERIFIED WORKING types only."""
    connection: str = Field(..., description="Connection ID/name")
    connectionType: Literal[
        "AzureBlobStorage"   # ✅ Verified (Copy ✓)
        # More will be added as we verify them
    ] = Field(..., description="Storage connection type - verified working in Fabric")

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
# VALIDATION HELPERS
# =============================================================================

VERIFIED_CONNECTION_TYPES = {
    # ✅ VERIFIED working in Fabric - Script Activities (10 total)
    "SqlServer": {"tested_in": ["Script", "Copy"], "status": "verified"},
    "Oracle": {"tested_in": ["Script", "Copy"], "status": "verified"},
    "PostgreSql": {"tested_in": ["Script"], "status": "verified"},
    "MySql": {"tested_in": ["Script"], "status": "verified"},
    "Snowflake": {"tested_in": ["Script"], "status": "verified"},
    "Db2": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW
    "Teradata": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW
    "SapHana": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW
    "GoogleBigQuery": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW
    "AmazonRedshift": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW
    "AzureSqlDatabase": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW - Azure SQL Database
    "AzureSqlDW": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW - Azure Synapse Analytics
    "AzureSqlMI": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW - Azure SQL Managed Instance  
    "AzurePostgreSql": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW - Azure Database for PostgreSQL
    "MariaDB": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW - MariaDB for Pipeline
    "CosmosDb": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW - Azure Cosmos DB v2
    "AzureDataExplorer": {"tested_in": ["Script"], "status": "verified"},  # ✅ NEW - Azure Data Explorer (Kusto)
    
    # ✅ VERIFIED working in Fabric - Storage/Copy Activities
    "AzureBlobStorage": {"tested_in": ["Copy"], "status": "verified"},
    
    # ✅ VERIFIED working in Fabric - Fabric Native
    "DataWarehouse": {"tested_in": ["Script", "Copy"], "status": "verified", "pattern": "linkedService"},
    "Lakehouse": {"tested_in": ["Copy"], "status": "verified", "pattern": "linkedService"},
    
    # 🔄 High priority for testing next
    "AzureSqlDatabase": {"tested_in": [], "status": "untested", "priority": "high"},
    "AzureFileStorage": {"tested_in": [], "status": "untested", "priority": "high"},
    "GoogleCloudStorage": {"tested_in": [], "status": "untested", "priority": "medium"},
    "AmazonS3Compatible": {"tested_in": [], "status": "untested", "priority": "medium"},
}

def get_connection_type_info(connection_type: str) -> Dict[str, Any]:
    """Get information about a connection type."""
    return VERIFIED_CONNECTION_TYPES.get(connection_type, {
        "tested_in": [], 
        "status": "unknown", 
        "priority": "low"
    })

def is_connection_type_verified(connection_type: str) -> bool:
    """Check if a connection type has been verified to work."""
    info = get_connection_type_info(connection_type)
    return info.get("status") == "verified"
