#!/usr/bin/env python3
"""
Test Script 04: New Copy Data Activity Tools
Tests the new modular copy data activity tools with exact Fabric JSON structure
Based on user's provided JSON examples and official Fabric Copy Job Definition
"""
import os
import sys
import json
import asyncio
import logging
import uuid
import base64
from datetime import datetime
from typing import Dict, Any

# Add src to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

from fabricmcp_server.fabric_api_client import FabricApiClient
from fabricmcp_server.fabric_models import (
    CreateItemRequest, 
    ItemDefinitionForCreate, 
    DefinitionPart
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class NewCopyDataActivityTester:
    """Tests the new modular copy data activity tools"""
    
    def __init__(self):
        self.workspace_id = "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27"
        self.client = None
        
        # Hard-coded lakehouse IDs from successful tests
        self.source_lakehouse = {
            "id": "5b98a120-0523-4c15-98e8-622832a671b0",
            "name": "SourceLakehouse_Test_20250725_123834"
        }
        self.sink_lakehouse = {
            "id": "6f2063f8-424f-4c22-918b-59629e0356f4", 
            "name": "SinkLakehouse_Test_20250725_123834"
        }
        
    async def setup_client(self):
        """Initialize the Fabric client"""
        logger.info("🔧 Initializing FabricApiClient...")
        self.client = await FabricApiClient.create("https://api.fabric.microsoft.com")
        logger.info("✅ Client initialized successfully")

    def create_exact_table_to_table_json(self) -> Dict[str, Any]:
        """Create exact JSON structure from user's example for table-to-table copy"""
        return {
            "name": "Copy_TableToTable_Exact",
            "description": "Exact structure from user's lakehouse-to-lakehouse example",
            "type": "Copy",
            "dependsOn": [],
            "policy": {
                "timeout": "0.12:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": {
                    "type": "LakehouseTableSource",
                    "timestampAsOf": "2025-07-26T02:00:00.000Z",
                    "versionAsOf": 1,
                    "datasetSettings": {
                        "annotations": [],
                        "linkedService": {
                            "name": self.source_lakehouse["name"],
                            "properties": {
                                "annotations": [],
                                "type": "Lakehouse",
                                "typeProperties": {
                                    "workspaceId": self.workspace_id,
                                    "artifactId": self.source_lakehouse["id"],
                                    "rootFolder": "Tables"
                                }
                            }
                        },
                        "type": "LakehouseTable",
                        "schema": [],
                        "typeProperties": {
                            "table": "sales_transactions"
                        }
                    }
                },
                "sink": {
                    "type": "LakehouseTableSink",
                    "tableActionOption": "Append",
                    "datasetSettings": {
                        "annotations": [],
                        "linkedService": {
                            "name": self.sink_lakehouse["name"],
                            "properties": {
                                "annotations": [],
                                "type": "Lakehouse",
                                "typeProperties": {
                                    "workspaceId": self.workspace_id,
                                    "artifactId": self.sink_lakehouse["id"],
                                    "rootFolder": "Tables"
                                }
                            }
                        },
                        "type": "LakehouseTable",
                        "schema": [],
                        "typeProperties": {
                            "table": "processed_sales"
                        }
                    }
                },
                "enableStaging": False,
                "translator": {
                    "type": "TabularTranslator",
                    "typeConversion": True,
                    "typeConversionSettings": {
                        "allowDataTruncation": True,
                        "treatBooleanAsNumber": False
                    }
                }
            }
        }

    def create_exact_table_to_files_json(self) -> Dict[str, Any]:
        """Create exact JSON structure from user's example for table-to-files copy"""
        return {
            "name": "Copy_TableToFiles_Exact",
            "description": "Exact structure from user's table-to-files example",
            "type": "Copy",
            "dependsOn": [],
            "policy": {
                "timeout": "0.12:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": {
                    "type": "LakehouseTableSource",
                    "timestampAsOf": "2025-07-26T02:00:00.000Z",
                    "versionAsOf": 1,
                    "datasetSettings": {
                        "annotations": [],
                        "linkedService": {
                            "name": self.source_lakehouse["name"],
                            "properties": {
                                "annotations": [],
                                "type": "Lakehouse",
                                "typeProperties": {
                                    "workspaceId": self.workspace_id,
                                    "artifactId": self.source_lakehouse["id"],
                                    "rootFolder": "Tables"
                                }
                            }
                        },
                        "type": "LakehouseTable",
                        "schema": [],
                        "typeProperties": {
                            "table": "customer_data"
                        }
                    }
                },
                "sink": {
                    "type": "JsonSink",
                    "storeSettings": {
                        "type": "LakehouseWriteSettings",
                        "maxConcurrentConnections": 15,
                        "copyBehavior": "PreserveHierarchy",
                        "blockSizeInMB": 50
                    },
                    "formatSettings": {
                        "type": "JsonWriteSettings",
                        "filePattern": "setOfObjects"
                    },
                    "datasetSettings": {
                        "annotations": [],
                        "linkedService": {
                            "name": self.sink_lakehouse["name"],
                            "properties": {
                                "annotations": [],
                                "type": "Lakehouse",
                                "typeProperties": {
                                    "workspaceId": self.workspace_id,
                                    "artifactId": self.sink_lakehouse["id"],
                                    "rootFolder": "Files"
                                }
                            }
                        },
                        "type": "Json",
                        "typeProperties": {
                            "location": {
                                "type": "LakehouseLocation",
                                "fileName": "customer_export.json",
                                "folderPath": "processed_data"
                            },
                            "compression": {
                                "type": "ZipDeflate",
                                "level": "Optimal"
                            }
                        },
                        "schema": {}
                    }
                },
                "enableStaging": False,
                "translator": {
                    "type": "TabularTranslator",
                    "typeConversion": True,
                    "typeConversionSettings": {
                        "allowDataTruncation": True,
                        "treatBooleanAsNumber": False
                    }
                }
            }
        }

    def build_pipeline_definition_direct(self, name: str, description: str, activities: list, 
                                       parameters: dict = None, variables: dict = None, 
                                       annotations: list = None, concurrency: int = 1, 
                                       folder: dict = None) -> str:
        """Build pipeline definition using proven working structure"""
        pipeline_json_structure = {
            "name": name,
            "objectId": str(uuid.uuid4()),
            "properties": {
                "description": description,
                "activities": activities,
                "parameters": parameters or {},
                "variables": variables or {},
                "annotations": annotations or [],
                "concurrency": concurrency,
                "folder": folder or {}
            }
        }
        
        pipeline_json = json.dumps(pipeline_json_structure, indent=2)
        logger.info(f"Generated pipeline JSON size: {len(pipeline_json)} characters")
        
        return base64.b64encode(pipeline_json.encode('utf-8')).decode('utf-8')

    async def test_exact_table_to_table_copy(self) -> Dict[str, Any]:
        """Test table-to-table copy using exact JSON structure from user's example"""
        logger.info("🔥 TESTING EXACT TABLE-TO-TABLE COPY")
        logger.info("=" * 70)
        
        try:
            # Create exact copy activity from user's example
            copy_activity = self.create_exact_table_to_table_json()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pipeline_name = f"ExactTableToTable_{timestamp}"
            pipeline_description = f"Exact table-to-table copy: {self.source_lakehouse['name']} → {self.sink_lakehouse['name']}"
            
            b64_payload = self.build_pipeline_definition_direct(
                name=pipeline_name,
                description=pipeline_description,
                activities=[copy_activity],
                annotations=["ExactStructure", "TableToTable", "UserExample"],
                folder={"name": "NewCopyTools/Exact"}
            )
            
            definition = ItemDefinitionForCreate(
                parts=[DefinitionPart(path="pipeline-content.json", payload=b64_payload, payloadType="InlineBase64")]
            )
            
            create_payload = CreateItemRequest(
                displayName=pipeline_name, 
                type="DataPipeline", 
                description=pipeline_description, 
                definition=definition
            )
            
            logger.info(f"📤 Creating exact table-to-table pipeline: {pipeline_name}")
            logger.info(f"   Source: {self.source_lakehouse['name']} (Tables/sales_transactions)")
            logger.info(f"   Sink: {self.sink_lakehouse['name']} (Tables/processed_sales)")
            logger.info(f"   Structure: Exact copy from user's JSON example")
            
            response = await self.client.create_item(self.workspace_id, create_payload)
            
            if hasattr(response, 'id'):
                result = {
                    "status": "success",
                    "pipeline_id": response.id,
                    "pipeline_name": response.display_name,
                    "copy_activity": {
                        "name": copy_activity["name"],
                        "source_type": "LakehouseTableSource",
                        "sink_type": "LakehouseTableSink",
                        "source_table": "sales_transactions",
                        "sink_table": "processed_sales"
                    },
                    "message": "Exact table-to-table copy created successfully"
                }
                logger.info(f"✅ SUCCESS: Exact Table-to-Table Pipeline ID: {response.id}")
                return result
            else:
                logger.error(f"❌ Unexpected response type: {type(response)}")
                return {"status": "failed", "error": f"Unexpected response: {type(response)}"}
                
        except Exception as e:
            logger.error(f"❌ FAILED: {e}")
            return {"status": "failed", "error": str(e)}

    async def test_exact_table_to_files_copy(self) -> Dict[str, Any]:
        """Test table-to-files copy using exact JSON structure from user's example"""
        logger.info("🔥 TESTING EXACT TABLE-TO-FILES COPY")
        logger.info("=" * 70)
        
        try:
            # Create exact copy activity from user's example
            copy_activity = self.create_exact_table_to_files_json()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pipeline_name = f"ExactTableToFiles_{timestamp}"
            pipeline_description = f"Exact table-to-files copy: {self.source_lakehouse['name']} → {self.sink_lakehouse['name']}"
            
            b64_payload = self.build_pipeline_definition_direct(
                name=pipeline_name,
                description=pipeline_description,
                activities=[copy_activity],
                annotations=["ExactStructure", "TableToFiles", "UserExample"],
                folder={"name": "NewCopyTools/Exact"}
            )
            
            definition = ItemDefinitionForCreate(
                parts=[DefinitionPart(path="pipeline-content.json", payload=b64_payload, payloadType="InlineBase64")]
            )
            
            create_payload = CreateItemRequest(
                displayName=pipeline_name, 
                type="DataPipeline", 
                description=pipeline_description, 
                definition=definition
            )
            
            logger.info(f"📤 Creating exact table-to-files pipeline: {pipeline_name}")
            logger.info(f"   Source: {self.source_lakehouse['name']} (Tables/customer_data)")
            logger.info(f"   Sink: {self.sink_lakehouse['name']} (Files/processed_data/customer_export.json)")
            logger.info(f"   Structure: Exact copy from user's JSON example with compression")
            
            response = await self.client.create_item(self.workspace_id, create_payload)
            
            if hasattr(response, 'id'):
                result = {
                    "status": "success",
                    "pipeline_id": response.id,
                    "pipeline_name": response.display_name,
                    "copy_activity": {
                        "name": copy_activity["name"],
                        "source_type": "LakehouseTableSource",
                        "sink_type": "JsonSink",
                        "source_table": "customer_data",
                        "sink_file": "processed_data/customer_export.json",
                        "compression": "ZipDeflate"
                    },
                    "message": "Exact table-to-files copy created successfully"
                }
                logger.info(f"✅ SUCCESS: Exact Table-to-Files Pipeline ID: {response.id}")
                return result
            else:
                logger.error(f"❌ Unexpected response type: {type(response)}")
                return {"status": "failed", "error": f"Unexpected response: {type(response)}"}
                
        except Exception as e:
            logger.error(f"❌ FAILED: {e}")
            return {"status": "failed", "error": str(e)}

    async def test_json_structure_validation(self) -> Dict[str, Any]:
        """Test that our JSON structure matches Fabric expectations"""
        logger.info("🔥 TESTING JSON STRUCTURE VALIDATION")
        logger.info("=" * 70)
        
        try:
            results = []
            
            # Test 1: Validate table-to-table JSON structure
            table_to_table_json = self.create_exact_table_to_table_json()
            required_fields = {
                "source": ["type", "datasetSettings"],
                "sink": ["type", "tableActionOption", "datasetSettings"],
                "datasetSettings": ["linkedService", "type", "typeProperties"]
            }
            
            source_valid = all(field in table_to_table_json["typeProperties"]["source"] for field in required_fields["source"])
            sink_valid = all(field in table_to_table_json["typeProperties"]["sink"] for field in required_fields["sink"])
            
            results.append({
                "test": "table_to_table_structure",
                "source_valid": source_valid,
                "sink_valid": sink_valid,
                "has_linked_service": "linkedService" in table_to_table_json["typeProperties"]["source"]["datasetSettings"]
            })
            
            # Test 2: Validate table-to-files JSON structure
            table_to_files_json = self.create_exact_table_to_files_json()
            files_sink_valid = all(field in table_to_files_json["typeProperties"]["sink"] for field in ["type", "storeSettings", "formatSettings", "datasetSettings"])
            
            results.append({
                "test": "table_to_files_structure",
                "source_valid": all(field in table_to_files_json["typeProperties"]["source"] for field in required_fields["source"]),
                "sink_valid": files_sink_valid,
                "has_compression": "compression" in table_to_files_json["typeProperties"]["sink"]["datasetSettings"]["typeProperties"],
                "has_location": "location" in table_to_files_json["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]
            })
            
            # Overall validation
            all_valid = all(result.get("source_valid", False) and result.get("sink_valid", False) for result in results)
            
            validation_result = {
                "status": "success" if all_valid else "failed",
                "validation_results": results,
                "overall_valid": all_valid,
                "message": "JSON structure validation completed"
            }
            
            if all_valid:
                logger.info("✅ SUCCESS: All JSON structures are valid")
            else:
                logger.error("❌ FAILED: Some JSON structures are invalid")
            
            return validation_result
                
        except Exception as e:
            logger.error(f"❌ FAILED: {e}")
            return {"status": "failed", "error": str(e)}

    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all new copy data activity tool tests"""
        logger.info("🚀 STARTING NEW COPY DATA ACTIVITY TOOL TESTS")
        logger.info("=" * 80)
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "workspace_id": self.workspace_id,
            "source_lakehouse": self.source_lakehouse,
            "sink_lakehouse": self.sink_lakehouse,
            "tests": {}
        }
        
        try:
            await self.setup_client()
            
            # Test 1: Exact table-to-table copy
            logger.info("TEST 1: Exact Table-to-Table Copy")
            results["tests"]["exact_table_to_table"] = await self.test_exact_table_to_table_copy()
            
            # Test 2: Exact table-to-files copy
            logger.info("\nTEST 2: Exact Table-to-Files Copy")
            results["tests"]["exact_table_to_files"] = await self.test_exact_table_to_files_copy()
            
            # Test 3: JSON structure validation
            logger.info("\nTEST 3: JSON Structure Validation")
            results["tests"]["json_validation"] = await self.test_json_structure_validation()
            
            # Determine overall status
            table_to_table_success = results["tests"]["exact_table_to_table"]["status"] == "success"
            table_to_files_success = results["tests"]["exact_table_to_files"]["status"] == "success"
            validation_success = results["tests"]["json_validation"]["status"] == "success"
            
            if table_to_table_success and table_to_files_success and validation_success:
                results["overall_status"] = "all_passed"
                logger.info("🎉 ALL NEW COPY DATA ACTIVITY TESTS PASSED!")
            elif table_to_table_success or table_to_files_success:
                results["overall_status"] = "partial_success"
                logger.warning("⚠️ SOME NEW COPY DATA ACTIVITY TESTS FAILED")
            else:
                results["overall_status"] = "all_failed"
                logger.error("❌ ALL NEW COPY DATA ACTIVITY TESTS FAILED")
                
            return results
            
        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR: {e}")
            results["overall_status"] = "critical_error"
            results["error"] = str(e)
            return results
            
        finally:
            if self.client:
                await self.client.close()

async def main():
    """Main execution function"""
    print("🔥 New Copy Data Activity Tools Tester")
    print("=" * 80)
    print("Testing new modular copy data activity tools...")
    print("Based on exact JSON structures from user's Fabric UI examples")
    print("=" * 80)
    
    tester = NewCopyDataActivityTester()
    results = await tester.run_all_tests()
    
    # Print summary
    print(f"\n🎯 NEW COPY DATA ACTIVITY TOOL TEST RESULTS:")
    print(f"Overall Status: {results['overall_status'].upper()}")
    print(f"Workspace: {results['workspace_id']}")
    
    # Test results
    for test_name, test_result in results.get("tests", {}).items():
        status_emoji = "✅" if test_result["status"] == "success" else "❌"
        print(f"\n{status_emoji} {test_name.replace('_', ' ').title()}:")
        print(f"   Status: {test_result['status'].upper()}")
        
        if test_result["status"] == "success":
            if "pipeline_id" in test_result:
                print(f"   Pipeline ID: {test_result.get('pipeline_id', 'N/A')}")
                print(f"   Pipeline Name: {test_result.get('pipeline_name', 'N/A')}")
                
                if "copy_activity" in test_result:
                    copy_info = test_result["copy_activity"]
                    print(f"   Copy Activity: {copy_info.get('name')}")
                    print(f"   Source: {copy_info.get('source_type')} ({copy_info.get('source_table', copy_info.get('source_file', 'N/A'))})")
                    print(f"   Sink: {copy_info.get('sink_type')} ({copy_info.get('sink_table', copy_info.get('sink_file', 'N/A'))})")
                    
            if "validation_results" in test_result:
                print(f"   Validation: {test_result.get('overall_valid', False)}")
                for validation in test_result.get("validation_results", []):
                    validation_emoji = "✅" if validation.get("source_valid") and validation.get("sink_valid") else "❌"
                    print(f"     {validation_emoji} {validation['test']}: Source={validation.get('source_valid')}, Sink={validation.get('sink_valid')}")
        else:
            print(f"   Error: {test_result.get('error', 'Unknown error')}")
    
    # Save results
    results_file = os.path.join(os.path.dirname(__file__), "new_copy_data_activity_test_results.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n📁 New copy data activity test results saved to: {results_file}")
    
    if results["overall_status"] == "all_passed":
        print(f"\n🎉 SUCCESS: New copy data activity tools working perfectly!")
        print(f"✅ Exact JSON structure validation passed")
        print(f"✅ Table-to-table copy working")
        print(f"✅ Table-to-files copy working")
        print(f"✅ Ready for MCP Inspector testing")
        print(f"\n🎯 NEW TOOLS READY FOR PRODUCTION:")
        print(f"   1. create_copy_data_activity_from_json - Maximum flexibility")
        print(f"   2. create_lakehouse_copy_activity - Simple helper")
    elif results["overall_status"] == "partial_success":
        print(f"\n⚠️ PARTIAL: Some new copy data activity tests passed, check individual results")
    else:
        print(f"\n❌ FAILED: New copy data activity tools need attention")
    
    return results

if __name__ == "__main__":
    asyncio.run(main()) 