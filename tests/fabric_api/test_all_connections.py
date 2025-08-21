"""
Systematic testing of ALL 55 Fabric UI connection types.
Each connection is tested individually to isolate issues.
"""

import pytest
import asyncio
from typing import Dict, Any, List, Tuple
import json
from datetime import datetime

# Import our test utilities
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tests.fabric_api.conftest import (
    build_pipeline_definition_from_activity,
    verify_activity_persisted
)

def test_workspace_id():
    return os.getenv("FABRIC_WORKSPACE_ID", "4be6c4a0-4816-478d-bdc1-7bda19c32bc6")

def test_pipeline_id():
    return os.getenv("FABRIC_PIPELINE_ID", "31ea5ed4-3ed5-4b2d-b836-52a2ba3ea6c8")

# ============================================================================
# COMPLETE LIST OF 55 CONNECTION TYPES FROM FABRIC UI
# ============================================================================

DATABASE_CONNECTIONS = [
    # Core SQL Databases
    ("SqlServer", "SQL Server database"),
    ("Oracle", "Oracle database"),
    ("PostgreSql", "PostgreSQL database"),
    ("MySql", "MySQL database"),
    ("Db2", "IBM Db2 database"),
    ("Teradata", "Teradata database"),
    ("SapHana", "SAP HANA database"),
    
    # Cloud Data Warehouses
    ("Snowflake", "Snowflake"),
    ("GoogleBigQuery", "Google BigQuery"),
    ("AmazonRedshift", "Amazon Redshift"),
    ("Vertica", "Vertica"),
    
    # Azure SQL Family
    ("AzureSqlDatabase", "Azure SQL database"),
    ("AzureSqlDW", "Azure Synapse Analytics (SQL DW)"),
    ("AzurePostgreSql", "Azure Database for PostgreSQL"),
    ("AzureSqlMI", "Azure SQL Managed Instance"),
    ("AzureMySql", "Azure Database for MySQL"),
    
    # NoSQL & Other Databases
    ("Cassandra", "Cassandra"),
    ("AmazonRDSSqlServer", "Amazon RDS for SQL Server"),
    ("Greenplum", "Greenplum for Pipeline"),
    ("MariaDB", "MariaDB for Pipeline"),
    ("MongoDbAtlas", "MongoDB Atlas for Pipelines"),
    ("MongoDb", "MongoDB for Pipeline"),
    ("CosmosDbMongoDb", "Azure Cosmos DB for MongoDB"),
    ("CosmosDb", "Azure Cosmos DB v2"),
    
    # Analytics & Other
    ("AzureDataExplorer", "Azure Data Explorer (Kusto)"),
    ("AzureDatabricks", "Azure Databricks"),
    ("Dataverse", "Dataverse"),
]

STORAGE_CONNECTIONS = [
    # File Systems
    ("FileSystem", "Folder"),
    ("Hdfs", "Hadoop Distributed File System"),
    ("Ftp", "FTP"),
    ("Sftp", "SFTP"),
    
    # Azure Storage
    ("AzureTables", "Azure Tables"),
    ("AzureDataLakeStorageGen2", "Azure Data Lake Storage Gen2"),
    ("AzureBlobStorage", "Azure Blobs"),
    ("AzureFileStorage", "Azure Files"),
    
    # Other Cloud Storage
    ("AmazonS3", "Amazon S3"),
    ("AmazonS3Compatible", "Amazon S3 Compatible"),
    ("GoogleCloudStorage", "Google Cloud Storage"),
    ("OracleCloudStorage", "Oracle Cloud Storage"),
]

SERVICE_CONNECTIONS = [
    ("SharePointOnlineList", "SharePoint Online list"),
    ("Salesforce", "Salesforce objects"),
    ("OData", "OData"),
    ("SalesforceServiceCloud", "Salesforce Service Cloud"),
    ("Dynamics365", "Dynamics 365"),
    ("DynamicsAX", "Dynamics AX"),
    ("DynamicsCRM", "Dynamics CRM"),
    ("Office365", "Microsoft365"),
    ("ServiceNow", "ServiceNow"),
]

OTHER_CONNECTIONS = [
    ("Odbc", "Odbc"),
    ("Http", "Http"),
    ("RestService", "REST"),
    ("SapBWOpenHub", "SAP BW Open Hub Application"),
    ("SapBWMessageServer", "SAP BW Open Hub Message Server"),
    ("SapTableApplication", "SAP Table Application Server"),
    ("SapTableMessage", "SAP Table Message Server"),
]

# Real Lakehouse for testing
LAKEHOUSE_ID = "ae8303a9-bc0c-41fe-8fbc-833289b68145"
WORKSPACE_ID = "4be6c4a0-4816-478d-bdc1-7bda19c32bc6"

def create_test_copy_activity(connection_type: str, connection_name: str, index: int) -> Dict[str, Any]:
    """
    Create a minimal Copy activity for testing a connection type.
    Uses best-guess patterns - will be refined with golden JSONs.
    """
    activity_name = f"Test_{index:02d}_{connection_type}"
    
    # Base activity structure
    activity = {
        "name": activity_name,
        "type": "Copy",
        "typeProperties": {
            "enableStaging": False,
            "translator": {"type": "TabularTranslator"}
        }
    }
    
    # Determine source configuration based on connection type
    source_config = create_source_config(connection_type)
    sink_config = create_sink_config(connection_type)
    
    activity["typeProperties"]["source"] = source_config
    activity["typeProperties"]["sink"] = sink_config
    
    return activity

def create_source_config(connection_type: str) -> Dict[str, Any]:
    """Create source configuration for a connection type."""
    
    # Map connection types to their source types and queries
    source_patterns = {
        # SQL-based sources
        "SqlServer": ("SqlServerSource", "SELECT TOP 5 * FROM test", "SqlServerTable"),
        "Oracle": ("OracleSource", "SELECT * FROM test WHERE ROWNUM <= 5", "OracleTable"),
        "PostgreSql": ("PostgreSqlSource", "SELECT * FROM test LIMIT 5", "PostgreSqlTable"),
        "MySql": ("MySqlSource", "SELECT * FROM test LIMIT 5", "MySqlTable"),
        "Db2": ("Db2Source", "SELECT * FROM test FETCH FIRST 5 ROWS ONLY", "Db2Table"),
        "Teradata": ("TeradataSource", "SELECT TOP 5 * FROM test", "TeradataTable"),
        "SapHana": ("SapHanaSource", "SELECT TOP 5 * FROM test", "SapHanaTable"),
        "Snowflake": ("SnowflakeSource", "SELECT * FROM test LIMIT 5", "SnowflakeTable"),
        "GoogleBigQuery": ("GoogleBigQuerySource", "SELECT * FROM test LIMIT 5", "GoogleBigQueryObject"),  # Try different name
        "AmazonRedshift": ("AmazonRedshiftSource", "SELECT * FROM test LIMIT 5", "AmazonRedshiftTable"),
        "Vertica": ("VerticaSource", "SELECT * FROM test LIMIT 5", "VerticaTable"),
        "AzureSqlDatabase": ("AzureSqlSource", "SELECT TOP 5 * FROM test", "AzureSqlTable"),
        "AzureSqlDW": ("SqlDWSource", "SELECT TOP 5 * FROM test", "AzureSqlDWTable"),
        "AzurePostgreSql": ("AzurePostgreSqlSource", "SELECT * FROM test LIMIT 5", "AzurePostgreSqlTable"),
        "AzureSqlMI": ("SqlMISource", "SELECT TOP 5 * FROM test", "AzureSqlMITable"),
        "AzureMySql": ("AzureMySqlSource", "SELECT * FROM test LIMIT 5", "AzureMySqlTable"),
        "MariaDB": ("MariaDBSource", "SELECT * FROM test LIMIT 5", "MariaDBTable"),
        "Cassandra": ("CassandraSource", "SELECT * FROM test LIMIT 5", "CassandraTable"),
        "MongoDb": ("MongoDbSource", None, "MongoDbCollection"),
        "MongoDbAtlas": ("MongoDbAtlasSource", None, "MongoDbAtlasCollection"),
        "CosmosDb": ("CosmosDbSource", "SELECT * FROM c", "CosmosDbSqlApiCollection"),
        "CosmosDbMongoDb": ("CosmosDbMongoDbSource", None, "CosmosDbMongoDbCollection"),
        "AzureDataExplorer": ("AzureDataExplorerSource", "test | take 5", "AzureDataExplorerTable"),
        
        # Storage sources
        "AzureBlobStorage": ("BinarySource", None, "Binary"),
        "GoogleCloudStorage": ("BinarySource", None, "Binary"),
        "AmazonS3": ("BinarySource", None, "Binary"),
        "AzureDataLakeStorageGen2": ("BinarySource", None, "Binary"),
        "AzureFileStorage": ("BinarySource", None, "Binary"),
        "FileSystem": ("BinarySource", None, "Binary"),
        "Ftp": ("BinarySource", None, "Binary"),
        "Sftp": ("BinarySource", None, "Binary"),
        "Hdfs": ("BinarySource", None, "Binary"),
        
        # Service sources  
        "OData": ("ODataSource", None, "ODataResource"),
        "Salesforce": ("SalesforceSource", "SELECT Id, Name FROM Account LIMIT 5", "SalesforceObject"),
        "SharePointOnlineList": ("SharePointOnlineListSource", None, "SharePointOnlineListResource"),
        "RestService": ("RestSource", None, "RestResource"),
        "Http": ("BinarySource", None, "Binary"),
        
        # Default for unknown
        "default": ("BinarySource", None, "Binary")
    }
    
    source_type, query, dataset_type = source_patterns.get(
        connection_type, 
        source_patterns["default"]
    )
    
    source = {"type": source_type}
    
    # Add query if SQL-based
    if query and "query" in source_type.lower() or "sql" in source_type.lower():
        if "oracle" in source_type.lower():
            source["oracleReaderQuery"] = query
        elif "sql" in source_type.lower() and "nosql" not in source_type.lower():
            source["sqlReaderQuery"] = query
            source["queryTimeout"] = "02:00:00"
        else:
            source["query"] = query
    
    # Add dataset settings
    source["datasetSettings"] = {
        "type": dataset_type,
        "typeProperties": {},
        "externalReferences": {"connection": "test_conn"}
    }
    
    # Add schema/table for database sources
    if "Table" in dataset_type:
        source["datasetSettings"]["typeProperties"]["schema"] = "dbo"
        source["datasetSettings"]["typeProperties"]["table"] = "test"
    
    return source

def create_sink_config(connection_type: str) -> Dict[str, Any]:
    """Create sink configuration - always use Lakehouse for testing."""
    return {
        "type": "LakehouseTableSink",
        "datasetSettings": {
            "type": "LakehouseTable",
            "typeProperties": {
                "table": f"{connection_type.lower()}_test"
            },
            "linkedService": {
                "name": "sink_lakehouse_for_pipeline",
                "properties": {
                    "type": "Lakehouse",
                    "typeProperties": {
                        "artifactId": LAKEHOUSE_ID,
                        "workspaceId": WORKSPACE_ID
                    }
                }
            }
        }
    }

# ============================================================================
# TEST EXECUTION
# ============================================================================

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_individual_connection(fabric_client, connection_type: str, connection_name: str, index: int):
    """Test a single connection type."""
    activity = create_test_copy_activity(connection_type, connection_name, index)
    
    # Build pipeline definition
    pipeline_def = build_pipeline_definition_from_activity("TestPipeline", activity)
    
    # Update pipeline
    result = await fabric_client.update_item_definition(
        workspace_id=test_workspace_id(),
        item_id=test_pipeline_id(),
        definition=pipeline_def
    )
    
    return result.get("status") == "Succeeded"

@pytest.mark.fabric_api  
@pytest.mark.asyncio
async def test_all_database_connections(fabric_client):
    """Test all 27 database connection types."""
    results = []
    for index, (conn_type, conn_name) in enumerate(DATABASE_CONNECTIONS, 1):
        try:
            success = await test_individual_connection(fabric_client, conn_type, conn_name, index)
            results.append((conn_type, conn_name, "✅ PASS" if success else "❌ FAIL"))
            print(f"{index:2d}. {conn_name:40s} - {'✅ PASS' if success else '❌ FAIL'}")
        except Exception as e:
            results.append((conn_type, conn_name, f"❌ ERROR: {str(e)[:50]}"))
            print(f"{index:2d}. {conn_name:40s} - ❌ ERROR: {str(e)[:50]}")
    
    return results

@pytest.mark.fabric_api
@pytest.mark.asyncio  
async def test_all_storage_connections(fabric_client):
    """Test all 11 storage connection types."""
    results = []
    for index, (conn_type, conn_name) in enumerate(STORAGE_CONNECTIONS, 28):
        try:
            success = await test_individual_connection(fabric_client, conn_type, conn_name, index)
            results.append((conn_type, conn_name, "✅ PASS" if success else "❌ FAIL"))
            print(f"{index:2d}. {conn_name:40s} - {'✅ PASS' if success else '❌ FAIL'}")
        except Exception as e:
            results.append((conn_type, conn_name, f"❌ ERROR: {str(e)[:50]}"))
            print(f"{index:2d}. {conn_name:40s} - ❌ ERROR: {str(e)[:50]}")
    
    return results

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_all_service_connections(fabric_client):
    """Test all 8 service connection types."""
    results = []
    for index, (conn_type, conn_name) in enumerate(SERVICE_CONNECTIONS, 39):
        try:
            success = await test_individual_connection(fabric_client, conn_type, conn_name, index)
            results.append((conn_type, conn_name, "✅ PASS" if success else "❌ FAIL"))
            print(f"{index:2d}. {conn_name:40s} - {'✅ PASS' if success else '❌ FAIL'}")
        except Exception as e:
            results.append((conn_type, conn_name, f"❌ ERROR: {str(e)[:50]}"))
            print(f"{index:2d}. {conn_name:40s} - ❌ ERROR: {str(e)[:50]}")
    
    return results

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_all_other_connections(fabric_client):
    """Test all 9 other connection types."""
    results = []
    for index, (conn_type, conn_name) in enumerate(OTHER_CONNECTIONS, 47):
        try:
            success = await test_individual_connection(fabric_client, conn_type, conn_name, index)
            results.append((conn_type, conn_name, "✅ PASS" if success else "❌ FAIL"))
            print(f"{index:2d}. {conn_name:40s} - {'✅ PASS' if success else '❌ FAIL'}")
        except Exception as e:
            results.append((conn_type, conn_name, f"❌ ERROR: {str(e)[:50]}"))
            print(f"{index:2d}. {conn_name:40s} - ❌ ERROR: {str(e)[:50]}")
    
    return results

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_all_55_connections(fabric_client):
    """Complete test of all 55 connection types with summary report."""
    print("\n" + "="*80)
    print("TESTING ALL 55 FABRIC CONNECTION TYPES")
    print("="*80)
    
    all_results = []
    
    # Test each category
    print("\n📊 DATABASE CONNECTIONS (27 types)")
    print("-"*40)
    db_results = await test_all_database_connections(fabric_client)
    all_results.extend(db_results)
    
    print("\n📁 STORAGE CONNECTIONS (11 types)")
    print("-"*40)
    storage_results = await test_all_storage_connections(fabric_client)
    all_results.extend(storage_results)
    
    print("\n🌐 SERVICE CONNECTIONS (8 types)")
    print("-"*40)
    service_results = await test_all_service_connections(fabric_client)
    all_results.extend(service_results)
    
    print("\n🔧 OTHER CONNECTIONS (9 types)")
    print("-"*40)
    other_results = await test_all_other_connections(fabric_client)
    all_results.extend(other_results)
    
    # Generate summary report
    print("\n" + "="*80)
    print("SUMMARY REPORT")
    print("="*80)
    
    passed = [r for r in all_results if "✅ PASS" in r[2]]
    failed = [r for r in all_results if "❌" in r[2]]
    
    print(f"\n✅ PASSED: {len(passed)}/55 ({len(passed)*100//55}%)")
    print(f"❌ FAILED: {len(failed)}/55 ({len(failed)*100//55}%)")
    
    if failed:
        print("\n🔴 FAILED CONNECTIONS:")
        for conn_type, conn_name, result in failed:
            print(f"  - {conn_name}: {result}")
    
    # Save results to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"tests/fabric_api/connection_test_report_{timestamp}.json"
    
    with open(report_file, "w") as f:
        json.dump({
            "timestamp": timestamp,
            "total": 55,
            "passed": len(passed),
            "failed": len(failed),
            "results": [
                {"type": r[0], "name": r[1], "result": r[2]} 
                for r in all_results
            ]
        }, f, indent=2)
    
    print(f"\n📄 Report saved to: {report_file}")
    
    return all_results

# Individual test functions for each connection type
# This allows testing any single connection in isolation

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_sqlserver_connection(fabric_client):
    """Test SQL Server connection individually."""
    return await test_individual_connection(fabric_client, "SqlServer", "SQL Server database", 1)

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_oracle_connection(fabric_client):
    """Test Oracle connection individually."""
    return await test_individual_connection(fabric_client, "Oracle", "Oracle database", 2)

# ... (we can add individual test functions for all 55 if needed)
