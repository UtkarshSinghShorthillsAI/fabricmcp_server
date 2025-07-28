#!/usr/bin/env python3
"""
Test Script 01: Create Test Lakehouses
Creates source and sink lakehouses for testing copy pipeline operations
"""
import os
import sys
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Add src to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

from fabricmcp_server.fabric_api_client import FabricApiClient
from fabricmcp_server.fabric_models import CreateItemRequest, ItemEntity

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class LakehouseCreator:
    """Creates test lakehouses for pipeline testing"""
    
    def __init__(self):
        self.workspace_id = "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27"
        self.client = None
        
    async def setup_client(self):
        """Initialize the Fabric client"""
        logger.info("🔧 Initializing FabricApiClient...")
        self.client = await FabricApiClient.create("https://api.fabric.microsoft.com")
        logger.info("✅ Client initialized successfully")
        
    async def create_lakehouse(self, name: str, description: str) -> Optional[ItemEntity]:
        """Create a single lakehouse"""
        logger.info(f"🏠 Creating lakehouse: {name}")
        try:
            payload = CreateItemRequest(
                displayName=name,
                type="Lakehouse",
                description=description
            )
            
            response = await self.client.create_item(
                workspace_id=self.workspace_id,
                payload=payload
            )
            
            if isinstance(response, ItemEntity):
                logger.info(f"✅ Lakehouse created successfully:")
                logger.info(f"   ID: {response.id}")
                logger.info(f"   Name: {response.display_name}")
                logger.info(f"   Type: {response.type}")
                return response
            else:
                logger.error(f"❌ Unexpected response type: {type(response)}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to create lakehouse {name}: {e}")
            return None
    
    async def create_test_lakehouses(self) -> Dict[str, Any]:
        """Create both source and sink lakehouses"""
        logger.info("🚀 STARTING LAKEHOUSE CREATION")
        logger.info("=" * 60)
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "workspace_id": self.workspace_id,
            "lakehouses": {},
            "status": "running"
        }
        
        try:
            await self.setup_client()
            
            # Create source lakehouse
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            source_name = f"SourceLakehouse_Test_{timestamp}"
            sink_name = f"SinkLakehouse_Test_{timestamp}"
            
            logger.info("Creating source lakehouse...")
            source_lakehouse = await self.create_lakehouse(
                name=source_name,
                description="Source lakehouse for copy pipeline testing"
            )
            
            if source_lakehouse:
                results["lakehouses"]["source"] = {
                    "id": source_lakehouse.id,
                    "name": source_lakehouse.display_name,
                    "status": "created"
                }
            else:
                results["lakehouses"]["source"] = {"status": "failed"}
            
            # Create sink lakehouse  
            logger.info("Creating sink lakehouse...")
            sink_lakehouse = await self.create_lakehouse(
                name=sink_name,
                description="Sink lakehouse for copy pipeline testing"
            )
            
            if sink_lakehouse:
                results["lakehouses"]["sink"] = {
                    "id": sink_lakehouse.id,
                    "name": sink_lakehouse.display_name,
                    "status": "created"
                }
            else:
                results["lakehouses"]["sink"] = {"status": "failed"}
            
            # Determine overall status
            source_success = results["lakehouses"].get("source", {}).get("status") == "created"
            sink_success = results["lakehouses"].get("sink", {}).get("status") == "created"
            
            if source_success and sink_success:
                results["status"] = "success"
                logger.info("🎉 BOTH LAKEHOUSES CREATED SUCCESSFULLY!")
            elif source_success or sink_success:
                results["status"] = "partial_success"
                logger.warning("⚠️ ONLY ONE LAKEHOUSE CREATED")
            else:
                results["status"] = "failed"
                logger.error("❌ FAILED TO CREATE LAKEHOUSES")
                
            return results
            
        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            return results
            
        finally:
            if self.client:
                await self.client.close()

async def main():
    """Main execution function"""
    print("🏠 Lakehouse Creator for Pipeline Testing")
    print("=" * 60)
    print("Creating source and sink lakehouses...")
    print("=" * 60)
    
    creator = LakehouseCreator()
    results = await creator.create_test_lakehouses()
    
    # Print results
    print(f"\n🎯 CREATION RESULTS:")
    print(f"Status: {results['status'].upper()}")
    print(f"Workspace: {results['workspace_id']}")
    
    if "source" in results["lakehouses"]:
        source = results["lakehouses"]["source"]
        print(f"\n📊 Source Lakehouse:")
        print(f"   Status: {source.get('status', 'unknown').upper()}")
        if 'id' in source:
            print(f"   ID: {source['id']}")
            print(f"   Name: {source['name']}")
    
    if "sink" in results["lakehouses"]:
        sink = results["lakehouses"]["sink"]
        print(f"\n📊 Sink Lakehouse:")
        print(f"   Status: {sink.get('status', 'unknown').upper()}")
        if 'id' in sink:
            print(f"   ID: {sink['id']}")
            print(f"   Name: {sink['name']}")
    
    if results["status"] == "success":
        print(f"\n✅ SUCCESS: Ready for copy pipeline testing!")
        
        # Save lakehouse info for other tests
        lakehouse_info = {
            "created_at": results["timestamp"],
            "workspace_id": results["workspace_id"],
            "source": results["lakehouses"].get("source", {}),
            "sink": results["lakehouses"].get("sink", {})
        }
        
        info_file = os.path.join(os.path.dirname(__file__), "lakehouse_info.json")
        with open(info_file, 'w') as f:
            json.dump(lakehouse_info, f, indent=2)
        print(f"📁 Lakehouse info saved to: {info_file}")
        
    else:
        print(f"\n❌ FAILED: Check logs for details")
        if "error" in results:
            print(f"Error: {results['error']}")
    
    return results

if __name__ == "__main__":
    asyncio.run(main()) 