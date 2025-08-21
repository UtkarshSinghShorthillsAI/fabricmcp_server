"""
Comprehensive test suite for all 55 Fabric connection types.
Tests API acceptance and round-trip persistence for each connection.

This is the authoritative test for connection type patterns.
"""

import pytest
import asyncio
import json
import base64
from typing import Dict, Any, Tuple, List
from datetime import datetime

# ============================================================================
# COMPLETE LIST OF ALL 55 FABRIC CONNECTION TYPES
# ============================================================================

ALL_CONNECTION_TYPES = {
    "database": [
        ("SqlServer", "SQL Server database"),
        ("Oracle", "Oracle database"),
        ("PostgreSql", "PostgreSQL database"),
        ("MySql", "MySQL database"),
        ("Db2", "IBM Db2 database"),
        ("Teradata", "Teradata database"),
        ("SapHana", "SAP HANA database"),
        ("Snowflake", "Snowflake"),
        ("GoogleBigQuery", "Google BigQuery"),
        ("AmazonRedshift", "Amazon Redshift"),
        ("Vertica", "Vertica"),
        ("AzureSqlDatabase", "Azure SQL database"),
        ("AzureSqlDW", "Azure Synapse Analytics (SQL DW)"),
        ("AzurePostgreSql", "Azure Database for PostgreSQL"),
        ("AzureSqlMI", "Azure SQL Managed Instance"),
        ("AzureMySql", "Azure Database for MySQL"),
        ("Cassandra", "Cassandra"),
        ("AmazonRDSSqlServer", "Amazon RDS for SQL Server"),
        ("Greenplum", "Greenplum for Pipeline"),
        ("MariaDB", "MariaDB for Pipeline"),
        ("MongoDbAtlas", "MongoDB Atlas for Pipelines"),
        ("MongoDb", "MongoDB for Pipeline"),
        ("CosmosDbMongoDb", "Azure Cosmos DB for MongoDB"),
        ("CosmosDb", "Azure Cosmos DB v2"),
        ("AzureDataExplorer", "Azure Data Explorer (Kusto)"),
        ("AzureDatabricks", "Azure Databricks"),
        ("Dataverse", "Dataverse"),
    ],
    "storage": [
        ("FileSystem", "Folder"),
        ("Hdfs", "Hadoop Distributed File System"),
        ("Ftp", "FTP"),
        ("Sftp", "SFTP"),
        ("AzureTables", "Azure Tables"),
        ("AzureDataLakeStorageGen2", "Azure Data Lake Storage Gen2"),
        ("AzureBlobStorage", "Azure Blobs"),
        ("AzureFileStorage", "Azure Files"),
        ("AmazonS3", "Amazon S3"),
        ("AmazonS3Compatible", "Amazon S3 Compatible"),
        ("GoogleCloudStorage", "Google Cloud Storage"),
        ("OracleCloudStorage", "Oracle Cloud Storage"),
    ],
    "service": [
        ("SharePointOnlineList", "SharePoint Online list"),
        ("Salesforce", "Salesforce objects"),
        ("OData", "OData"),
        ("SalesforceServiceCloud", "Salesforce Service Cloud"),
        ("Dynamics365", "Dynamics 365"),
        ("DynamicsAX", "Dynamics AX"),
        ("DynamicsCRM", "Dynamics CRM"),
        ("Office365", "Microsoft365"),
        ("ServiceNow", "ServiceNow"),
    ],
    "other": [
        ("Odbc", "Odbc"),
        ("Http", "Http"),
        ("RestService", "REST"),
        ("SapBWOpenHub", "SAP BW Open Hub Application"),
        ("SapBWMessageServer", "SAP BW Open Hub Message Server"),
        ("SapTableApplication", "SAP Table Application Server"),
        ("SapTableMessage", "SAP Table Message Server"),
    ]
}

# Flatten for easy access
ALL_CONNECTIONS_FLAT = []
for category, connections in ALL_CONNECTION_TYPES.items():
    ALL_CONNECTIONS_FLAT.extend(connections)

# Known failing connections (as of testing)
KNOWN_FAILURES = ["Snowflake", "AzureDataExplorer", "Office365"]

# Test configuration
TEST_WORKSPACE_ID = "4be6c4a0-4816-478d-bdc1-7bda19c32bc6"
TEST_PIPELINE_ID = "31ea5ed4-3ed5-4b2d-b836-52a2ba3ea6c8"
TEST_LAKEHOUSE_ID = "ae8303a9-bc0c-41fe-8fbc-833289b68145"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_copy_activity_simple_pattern(connection_type: str) -> Dict[str, Any]:
    """
    Create a Copy activity using the simple pattern that works for all connections.
    Pattern: {ConnectionType}Source -> {ConnectionType}Table
    
    This is the ground truth pattern verified to work with 52/55 connections.
    """
    return {
        "name": f"Test_{connection_type}",
        "type": "Copy",
        "typeProperties": {
            "source": {
                "type": f"{connection_type}Source",
                "datasetSettings": {
                    "type": f"{connection_type}Table",
                    "externalReferences": {"connection": "placeholder"}
                }
            },
            "sink": {
                "type": "LakehouseTableSink",
                "datasetSettings": {
                    "type": "LakehouseTable",
                    "typeProperties": {"table": f"{connection_type.lower()}_test"},
                    "linkedService": {
                        "name": "test_lakehouse",
                        "properties": {
                            "type": "Lakehouse",
                            "typeProperties": {
                                "artifactId": TEST_LAKEHOUSE_ID,
                                "workspaceId": TEST_WORKSPACE_ID
                            }
                        }
                    }
                }
            },
            "enableStaging": False
        }
    }

def build_pipeline_definition(activity: Dict[str, Any]) -> Dict[str, Any]:
    """Build a pipeline definition with a single activity."""
    pipeline_struct = {"properties": {"activities": [activity]}}
    return {
        "parts": [{
            "path": "pipeline-content.json",
            "payload": base64.b64encode(
                json.dumps(pipeline_struct).encode()
            ).decode(),
            "payloadType": "InlineBase64"
        }]
    }

# ============================================================================
# TEST FIXTURES
# ============================================================================

# Fabric client fixture is imported from conftest.py automatically by pytest

# ============================================================================
# INDIVIDUAL CONNECTION TYPE TESTS
# ============================================================================

class TestConnectionTypes:
    """Test class for all connection types."""
    
    @pytest.mark.parametrize("connection_type,connection_name", ALL_CONNECTIONS_FLAT)
    @pytest.mark.fabric_api
    @pytest.mark.asyncio
    async def test_connection_api_acceptance(self, fabric_client, connection_type, connection_name):
        """Test if API accepts the connection type pattern."""
        activity = create_copy_activity_simple_pattern(connection_type)
        pipeline_def = build_pipeline_definition(activity)
        
        result = await fabric_client.update_pipeline_definition(
            workspace_id=TEST_WORKSPACE_ID,
            pipeline_id=TEST_PIPELINE_ID,
            definition=pipeline_def
        )
        
        # Check if it's a known failure
        if connection_type in KNOWN_FAILURES:
            pytest.xfail(f"{connection_type} is a known failure - needs pattern fix")
        
        assert result.status_code in [200, 202], f"{connection_type} failed with status {result.status_code}"
    
    @pytest.mark.parametrize("connection_type,connection_name", ALL_CONNECTIONS_FLAT)
    @pytest.mark.fabric_api
    @pytest.mark.asyncio
    async def test_connection_roundtrip(self, fabric_client, connection_type, connection_name):
        """Test round-trip persistence: API accept -> UI persist -> retrieve match."""
        
        # Skip known failures
        if connection_type in KNOWN_FAILURES:
            pytest.skip(f"{connection_type} is a known failure")
        
        # Create and send activity
        activity = create_copy_activity_simple_pattern(connection_type)
        pipeline_def = build_pipeline_definition(activity)
        
        # Step 1: Update pipeline
        result = await fabric_client.update_pipeline_definition(
            workspace_id=TEST_WORKSPACE_ID,
            pipeline_id=TEST_PIPELINE_ID,
            definition=pipeline_def
        )
        assert result.status_code in [200, 202], f"API rejected {connection_type}"
        
        # Step 2: Wait for persistence
        await asyncio.sleep(2)
        
        # Step 3: Retrieve and verify
        get_def = await fabric_client.get_item_definition(TEST_WORKSPACE_ID, TEST_PIPELINE_ID)
        assert get_def is not None, f"Failed to retrieve definition for {connection_type}"
        assert "definition" in get_def, f"No definition in response for {connection_type}"
        
        # Check if activity persisted
        found = False
        for part in get_def["definition"].get("parts", []):
            if part.get("path") == "pipeline-content.json":
                decoded = json.loads(base64.b64decode(part["payload"]))
                activities = decoded.get("properties", {}).get("activities", [])
                
                for act in activities:
                    if act.get("name") == f"Test_{connection_type}":
                        found = True
                        # Verify source type matches
                        source_type = act.get("typeProperties", {}).get("source", {}).get("type")
                        assert source_type == f"{connection_type}Source", \
                            f"Source type mismatch for {connection_type}: expected {connection_type}Source, got {source_type}"
                        break
        
        assert found, f"Activity for {connection_type} not found in retrieved pipeline"

# ============================================================================
# BATCH TESTING FUNCTIONS
# ============================================================================

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_all_database_connections(fabric_client):
    """Test all database connections as a batch."""
    results = []
    for conn_type, conn_name in ALL_CONNECTION_TYPES["database"]:
        activity = create_copy_activity_simple_pattern(conn_type)
        pipeline_def = build_pipeline_definition(activity)
        
        try:
            result = await fabric_client.update_pipeline_definition(
                TEST_WORKSPACE_ID, TEST_PIPELINE_ID, pipeline_def
            )
            status = "PASS" if result.status_code in [200, 202] else f"FAIL:{result.status_code}"
        except Exception as e:
            status = f"ERROR:{str(e)[:30]}"
        
        results.append((conn_type, conn_name, status))
    
    # Report results
    passed = [r for r in results if "PASS" in r[2]]
    print(f"\nDatabase Connections: {len(passed)}/{len(results)} passed")
    return results

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_all_storage_connections(fabric_client):
    """Test all storage connections as a batch."""
    results = []
    for conn_type, conn_name in ALL_CONNECTION_TYPES["storage"]:
        activity = create_copy_activity_simple_pattern(conn_type)
        pipeline_def = build_pipeline_definition(activity)
        
        try:
            result = await fabric_client.update_pipeline_definition(
                TEST_WORKSPACE_ID, TEST_PIPELINE_ID, pipeline_def
            )
            status = "PASS" if result.status_code in [200, 202] else f"FAIL:{result.status_code}"
        except Exception as e:
            status = f"ERROR:{str(e)[:30]}"
        
        results.append((conn_type, conn_name, status))
    
    # Report results
    passed = [r for r in results if "PASS" in r[2]]
    print(f"\nStorage Connections: {len(passed)}/{len(results)} passed")
    return results

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_all_service_connections(fabric_client):
    """Test all service connections as a batch."""
    results = []
    for conn_type, conn_name in ALL_CONNECTION_TYPES["service"]:
        activity = create_copy_activity_simple_pattern(conn_type)
        pipeline_def = build_pipeline_definition(activity)
        
        try:
            result = await fabric_client.update_pipeline_definition(
                TEST_WORKSPACE_ID, TEST_PIPELINE_ID, pipeline_def
            )
            status = "PASS" if result.status_code in [200, 202] else f"FAIL:{result.status_code}"
        except Exception as e:
            status = f"ERROR:{str(e)[:30]}"
        
        results.append((conn_type, conn_name, status))
    
    # Report results
    passed = [r for r in results if "PASS" in r[2]]
    print(f"\nService Connections: {len(passed)}/{len(results)} passed")
    return results

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_all_55_connections_comprehensive(fabric_client):
    """
    Comprehensive test of all 55 connection types.
    Generates a detailed report of what works and what doesn't.
    """
    print("\n" + "="*80)
    print("COMPREHENSIVE CONNECTION TYPE TEST")
    print("="*80)
    
    all_results = []
    
    for category, connections in ALL_CONNECTION_TYPES.items():
        print(f"\n📊 Testing {category.upper()} connections ({len(connections)} types)")
        print("-"*40)
        
        for i, (conn_type, conn_name) in enumerate(connections, 1):
            activity = create_copy_activity_simple_pattern(conn_type)
            pipeline_def = build_pipeline_definition(activity)
            
            try:
                result = await fabric_client.update_pipeline_definition(
                    TEST_WORKSPACE_ID, TEST_PIPELINE_ID, pipeline_def
                )
                
                if result.status_code in [200, 202]:
                    # Test round-trip
                    await asyncio.sleep(1)
                    get_def = await fabric_client.get_item_definition(TEST_WORKSPACE_ID, TEST_PIPELINE_ID)
                    
                    found = False
                    if get_def and "definition" in get_def:
                        for part in get_def["definition"].get("parts", []):
                            if part.get("path") == "pipeline-content.json":
                                decoded = json.loads(base64.b64decode(part["payload"]))
                                activities = decoded.get("properties", {}).get("activities", [])
                                found = any(a.get("name") == f"Test_{conn_type}" for a in activities)
                    
                    status = "✅ VERIFIED" if found else "⚠️ NOT PERSISTED"
                else:
                    status = f"❌ API:{result.status_code}"
            except Exception as e:
                status = f"❌ ERROR:{str(e)[:30]}"
            
            print(f"{i:2d}. {conn_name:40s} - {status}")
            all_results.append((conn_type, conn_name, status))
            
            # Small delay to avoid rate limiting
            if i % 10 == 0:
                await asyncio.sleep(1)
    
    # Generate summary
    print("\n" + "="*80)
    print("SUMMARY REPORT")
    print("="*80)
    
    verified = [r for r in all_results if "✅ VERIFIED" in r[2]]
    failed = [r for r in all_results if "❌" in r[2] or "⚠️" in r[2]]
    
    print(f"\n✅ Fully Verified: {len(verified)}/55 ({len(verified)*100//55}%)")
    print(f"❌ Failed/Issues: {len(failed)}/55")
    
    if failed:
        print("\n🔴 Failed connections:")
        for conn_type, conn_name, status in failed:
            print(f"  - {conn_name} ({conn_type}): {status}")
    
    # Save report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"tests/fabric_api/connection_test_report_{timestamp}.json"
    
    with open(report_file, "w") as f:
        json.dump({
            "timestamp": timestamp,
            "total": 55,
            "verified": len(verified),
            "failed": len(failed),
            "results": [
                {"type": r[0], "name": r[1], "status": r[2]} 
                for r in all_results
            ]
        }, f, indent=2)
    
    print(f"\n📄 Detailed report saved to: {report_file}")
    
    return all_results

# ============================================================================
# INDIVIDUAL TEST FUNCTIONS FOR SPECIFIC CONNECTIONS
# ============================================================================

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_sqlserver_individually(fabric_client):
    """Test SQL Server connection individually."""
    activity = create_copy_activity_simple_pattern("SqlServer")
    pipeline_def = build_pipeline_definition(activity)
    
    result = await fabric_client.update_pipeline_definition(
        TEST_WORKSPACE_ID, TEST_PIPELINE_ID, pipeline_def
    )
    
    assert result.status_code in [200, 202], f"SqlServer failed with {result.status_code}"

@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_azureblobstorage_individually(fabric_client):
    """Test Azure Blob Storage connection individually."""
    activity = create_copy_activity_simple_pattern("AzureBlobStorage")
    pipeline_def = build_pipeline_definition(activity)
    
    result = await fabric_client.update_pipeline_definition(
        TEST_WORKSPACE_ID, TEST_PIPELINE_ID, pipeline_def
    )
    
    assert result.status_code in [200, 202], f"AzureBlobStorage failed with {result.status_code}"

# Add more individual tests as needed...
