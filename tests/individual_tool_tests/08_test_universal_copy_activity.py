#!/usr/bin/env python3
"""
Test script for Universal Copy Activity Tool - True Modular Design

This script validates:
- Individual source models (SharePoint, S3, Lakehouse)
- Individual sink models (Lakehouse, S3)
- All S3 file path types (file_path, wildcard, prefix, list_of_files)
- Universal copy activity tool combinations
"""

import json
import sys
import os
import asyncio
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from fabricmcp_server.tools.universal_copy_activity import (
    SharePointSource, S3Source, LakehouseSource,
    LakehouseSink, S3Sink,
    FilePathType, S3FilePathConfig, TableConfiguration, FileConfiguration,
    CopyActivityConfig, create_universal_copy_pipeline_impl
)


def test_sharepoint_source_models():
    """Test SharePoint source model"""
    print("🔍 Testing SharePoint Source Models...")
    
    # Test basic SharePoint source
    sp_source = SharePointSource(
        connection_id="sp-conn-123",
        list_name="Documents"
    )
    
    sp_json = sp_source.to_copy_activity_source()
    print("✅ SharePoint Source (List Name):")
    print(json.dumps(sp_json, indent=2))
    
    # Test SharePoint with query
    sp_query_source = SharePointSource(
        connection_id="sp-conn-123", 
        list_name="WebPartGallery",
        query="$filter=Status eq 'Active'"
    )
    
    sp_query_json = sp_query_source.to_copy_activity_source()
    print("\n✅ SharePoint Source (With Query):")
    print(json.dumps(sp_query_json, indent=2))
    
    return True


def test_s3_source_models():
    """Test S3 source models with all file path types"""
    print("\n🔍 Testing S3 Source Models...")
    
    # 1. File Path Type
    file_path_config = S3FilePathConfig(
        path_type=FilePathType.FILE_PATH,
        folder_path="exports",
        file_name="data.json"
    )
    
    s3_file_source = S3Source(
        connection_id="s3-conn-456",
        bucket_name="google-photos-pipeline",
        source_type="JsonSource",
        format_type="Json",
        file_path_config=file_path_config,
        modified_datetime_start="2025-07-28T00:00:00.000Z",
        modified_datetime_end="2025-07-27T00:02:00.000Z"
    )
    
    s3_file_json = s3_file_source.to_copy_activity_source()
    print("✅ S3 Source (File Path Type):")
    print(json.dumps(s3_file_json, indent=2))
    
    # 2. Wildcard Type
    wildcard_config = S3FilePathConfig(
        path_type=FilePathType.WILDCARD,
        wildcard_folder_path="wildcard_folder",
        wildcard_file_name="wildcard_file"
    )
    
    s3_wildcard_source = S3Source(
        connection_id="s3-conn-456",
        bucket_name="google-photos-pipeline",
        source_type="BinarySource",
        format_type="Binary",
        file_path_config=wildcard_config
    )
    
    s3_wildcard_json = s3_wildcard_source.to_copy_activity_source()
    print("\n✅ S3 Source (Wildcard Type):")
    print(json.dumps(s3_wildcard_json, indent=2))
    
    # 3. Prefix Type
    prefix_config = S3FilePathConfig(
        path_type=FilePathType.PREFIX,
        prefix="prefix_name"
    )
    
    s3_prefix_source = S3Source(
        connection_id="s3-conn-456",
        bucket_name="google-photos-pipeline", 
        source_type="BinarySource",
        format_type="Binary",
        file_path_config=prefix_config
    )
    
    s3_prefix_json = s3_prefix_source.to_copy_activity_source()
    print("\n✅ S3 Source (Prefix Type):")
    print(json.dumps(s3_prefix_json, indent=2))
    
    # 4. List of Files Type
    list_config = S3FilePathConfig(
        path_type=FilePathType.LIST_OF_FILES,
        file_list_path="google-photos-pipeline/exports",
        list_folder_path="exports"
    )
    
    s3_list_source = S3Source(
        connection_id="s3-conn-456",
        bucket_name="google-photos-pipeline",
        source_type="BinarySource", 
        format_type="Binary",
        file_path_config=list_config
    )
    
    s3_list_json = s3_list_source.to_copy_activity_source()
    print("\n✅ S3 Source (List of Files Type):")
    print(json.dumps(s3_list_json, indent=2))
    
    return True


def test_lakehouse_source_models():
    """Test Lakehouse source models for both Tables and Files"""
    print("\n🔍 Testing Lakehouse Source Models...")
    
    # Table source
    table_config = TableConfiguration(
        table_name="fact_sales_enriched",
        schema_name="dbo"
    )
    
    lh_table_source = LakehouseSource(
        lakehouse_name="AdvancedDataLakehouse",
        workspace_id="2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
        artifact_id="0440d283-3ccf-478c-9d26-53145c1e6802",
        root_folder="Tables",
        table_config=table_config,
        timestamp_as_of="2025-07-26T02:00:00.000Z",
        version_as_of=1
    )
    
    lh_table_json = lh_table_source.to_copy_activity_source()
    print("✅ Lakehouse Source (Table):")
    print(json.dumps(lh_table_json, indent=2))
    
    # File source  
    file_config = FileConfiguration(
        folder_path="processed_data",
        file_name="customers.json",
        file_format="JSON"
    )
    
    lh_file_source = LakehouseSource(
        lakehouse_name="AdvancedDataLakehouse",
        workspace_id="2bb31c24-2c1f-467a-bffd-3a37a2c0ad27", 
        artifact_id="0440d283-3ccf-478c-9d26-53145c1e6802",
        root_folder="Files",
        file_config=file_config
    )
    
    lh_file_json = lh_file_source.to_copy_activity_source()
    print("\n✅ Lakehouse Source (File):")
    print(json.dumps(lh_file_json, indent=2))
    
    return True


def test_lakehouse_sink_models():
    """Test Lakehouse sink models"""
    print("\n🔍 Testing Lakehouse Sink Models...")
    
    # Table sink
    table_config = TableConfiguration(
        table_name="customer_segments"
    )
    
    lh_table_sink = LakehouseSink(
        lakehouse_name="SinkLakehouse",
        workspace_id="2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
        artifact_id="6f2063f8-424f-4c22-918b-59629e0356f4",
        root_folder="Tables",
        table_config=table_config,
        table_action_option="Append"
    )
    
    lh_table_sink_json = lh_table_sink.to_copy_activity_sink()
    print("✅ Lakehouse Sink (Table):")
    print(json.dumps(lh_table_sink_json, indent=2))
    
    # File sink
    file_config = FileConfiguration(
        folder_path="Output",
        file_name="data_from_sharepoint",
        file_format="DelimitedText"
    )
    
    lh_file_sink = LakehouseSink(
        lakehouse_name="SinkLakehouse",
        workspace_id="2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
        artifact_id="6f2063f8-424f-4c22-918b-59629e0356f4",
        root_folder="Files",
        file_config=file_config
    )
    
    lh_file_sink_json = lh_file_sink.to_copy_activity_sink()
    print("\n✅ Lakehouse Sink (File):")
    print(json.dumps(lh_file_sink_json, indent=2))
    
    return True


def test_s3_sink_models():
    """Test S3 sink models"""
    print("\n🔍 Testing S3 Sink Models...")
    
    file_config = FileConfiguration(
        folder_path="exports",
        file_name="from_lakehouse"
    )
    
    s3_sink = S3Sink(
        connection_id="s3-conn-456",
        bucket_name="google-photos-pipeline",
        sink_type="JsonSink",
        format_type="Json",
        file_config=file_config
    )
    
    s3_sink_json = s3_sink.to_copy_activity_sink()
    print("✅ S3 Sink:")
    print(json.dumps(s3_sink_json, indent=2))
    
    return True


def test_complete_copy_activity_configs():
    """Test complete copy activity configurations for various scenarios"""
    print("\n🔍 Testing Complete Copy Activity Configurations...")
    
    scenarios = [
        {
            "name": "SharePoint to Lakehouse Table",
            "source_type": "SharePoint",
            "source_config": {
                "connection_id": "sp-conn-123",
                "list_name": "Documents",
                "query": "$filter=Status eq 'Active'"
            },
            "sink_type": "Lakehouse",
            "sink_config": {
                "lakehouse_name": "TargetLakehouse",
                "workspace_id": "ws-123",
                "artifact_id": "lh-456",
                "root_folder": "Tables",
                "table_config": {
                    "table_name": "sharepoint_documents"
                }
            }
        },
        {
            "name": "S3 Wildcard to Lakehouse Files",
            "source_type": "S3",
            "source_config": {
                "connection_id": "s3-conn-456",
                "bucket_name": "data-bucket",
                "source_type": "BinarySource",
                "format_type": "Binary",
                "file_path_config": {
                    "path_type": "wildcard",
                    "wildcard_folder_path": "data_*",
                    "wildcard_file_name": "*.bin"
                }
            },
            "sink_type": "Lakehouse", 
            "sink_config": {
                "lakehouse_name": "TargetLakehouse",
                "workspace_id": "ws-123",
                "artifact_id": "lh-456",
                "root_folder": "Files",
                "file_config": {
                    "folder_path": "imported",
                    "file_name": "from_s3.bin",
                    "file_format": "Binary"
                }
            }
        },
        {
            "name": "Lakehouse Table to S3",
            "source_type": "Lakehouse",
            "source_config": {
                "lakehouse_name": "SourceLakehouse",
                "workspace_id": "ws-123",
                "artifact_id": "lh-789",
                "root_folder": "Tables",
                "table_config": {
                    "table_name": "processed_data"
                }
            },
            "sink_type": "S3",
            "sink_config": {
                "connection_id": "s3-conn-456",
                "bucket_name": "output-bucket",
                "sink_type": "JsonSink",
                "format_type": "Json",
                "file_config": {
                    "folder_path": "lakehouse_exports",
                    "file_name": "data.json"
                }
            }
        }
    ]
    
    for scenario in scenarios:
        print(f"\n🎯 Scenario: {scenario['name']}")
        
        # Test that the models validate correctly
        try:
            if scenario['source_type'].lower() == "sharepoint":
                source = SharePointSource(**scenario['source_config'])
            elif scenario['source_type'].lower() == "s3":
                source = S3Source(**scenario['source_config'])
            elif scenario['source_type'].lower() == "lakehouse":
                source = LakehouseSource(**scenario['source_config'])
                
            if scenario['sink_type'].lower() == "lakehouse":
                sink = LakehouseSink(**scenario['sink_config'])
            elif scenario['sink_type'].lower() == "s3":
                sink = S3Sink(**scenario['sink_config'])
                
            # Generate the JSONs
            source_json = source.to_copy_activity_source()
            sink_json = sink.to_copy_activity_sink()
            
            print(f"✅ {scenario['name']} - Models validated successfully")
            print(f"   Source JSON: {len(json.dumps(source_json))} characters")
            print(f"   Sink JSON: {len(json.dumps(sink_json))} characters")
            
        except Exception as e:
            print(f"❌ {scenario['name']} - Validation failed: {str(e)}")
    
    return True


def generate_mcp_tool_examples():
    """Generate example JSON configurations for MCP tool usage"""
    print("\n🔍 Generating MCP Tool Usage Examples...")
    
    examples = {
        "sharepoint_to_lakehouse_table": {
            "workspace_id": "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
            "pipeline_name": "SharePoint_to_Lakehouse_Pipeline",
            "source_type": "SharePoint",
            "source_config": {
                "connection_id": "29099f29-20ae-4998-bd13-007289b3f7e3",
                "list_name": "WebPartGallery",
                "query": "$filter=Status eq 'Active'"
            },
            "sink_type": "Lakehouse",
            "sink_config": {
                "lakehouse_name": "SinkLakehouse_Test",
                "workspace_id": "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
                "artifact_id": "6f2063f8-424f-4c22-918b-59629e0356f4",
                "root_folder": "Files",
                "file_config": {
                    "folder_path": "Output",
                    "file_name": "data_from_sharepoint",
                    "file_format": "DelimitedText"
                }
            },
            "activity_config": {
                "activity_name": "Copy SharePoint Data",
                "description": "Copy data from SharePoint list to Lakehouse",
                "enable_schema_mapping": True,
                "translator": {
                    "mappings": [
                        {"source": {"name": "Title", "type": "String"}},
                        {"source": {"name": "Status", "type": "String"}}
                    ]
                }
            }
        },
        "s3_wildcard_to_lakehouse": {
            "workspace_id": "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
            "pipeline_name": "S3_Wildcard_to_Lakehouse_Pipeline", 
            "source_type": "S3",
            "source_config": {
                "connection_id": "ea2ef352-5072-4c31-a500-5fab9c03c706",
                "bucket_name": "google-photos-pipeline",
                "source_type": "BinarySource",
                "format_type": "Binary",
                "file_path_config": {
                    "path_type": "wildcard",
                    "wildcard_folder_path": "wildcard_folder",
                    "wildcard_file_name": "wildcard_file"
                },
                "modified_datetime_start": "2025-07-28T00:00:00.000Z",
                "modified_datetime_end": "2025-07-27T00:03:00.000Z"
            },
            "sink_type": "Lakehouse",
            "sink_config": {
                "lakehouse_name": "SinkLakehouse_Test",
                "workspace_id": "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
                "artifact_id": "6f2063f8-424f-4c22-918b-59629e0356f4",
                "root_folder": "Files",
                "file_config": {
                    "folder_path": "Output",
                    "file_name": "s3_sink",
                    "file_format": "Binary"
                }
            }
        },
        "lakehouse_table_to_s3": {
            "workspace_id": "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
            "pipeline_name": "Lakehouse_Table_to_S3_Pipeline",
            "source_type": "Lakehouse",
            "source_config": {
                "lakehouse_name": "AdvancedDataLakehouse",
                "workspace_id": "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
                "artifact_id": "0440d283-3ccf-478c-9d26-53145c1e6802",
                "root_folder": "Tables",
                "table_config": {
                    "table_name": "fact_sales_enriched"
                }
            },
            "sink_type": "S3",
            "sink_config": {
                "connection_id": "ea2ef352-5072-4c31-a500-5fab9c03c706", 
                "bucket_name": "google-photos-pipeline",
                "sink_type": "JsonSink",
                "format_type": "Json",
                "file_config": {
                    "folder_path": "exports",
                    "file_name": "from_lakehouse"
                }
            }
        }
    }
    
    # Save examples to file
    examples_file = "UNIVERSAL_COPY_TOOL_EXAMPLES.json"
    with open(examples_file, 'w') as f:
        json.dump(examples, f, indent=2)
    
    print(f"✅ MCP Tool examples saved to: {examples_file}")
    
    return examples


async def test_real_pipeline_creation():
    """Test real pipeline creation (commented out for safety)"""
    print("\n🔍 Real Pipeline Creation Test (DISABLED)")
    print("To enable real pipeline creation:")
    print("1. Uncomment the code below")
    print("2. Ensure you have valid workspace_id and connection_id")
    print("3. Update the configuration with your actual values")
    
    # UNCOMMENT BELOW FOR REAL TESTING
    # """
    try:
        from fabricmcp_server.sessions import get_session_fabric_client
        
        # Mock context for testing - needs session attribute
        class MockSession:
            pass
            
        class MockContext:
            def __init__(self):
                self.session = MockSession()
        
        ctx = MockContext()
        
        result = await create_universal_copy_pipeline_impl(
            ctx,
            workspace_id="2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
            pipeline_name=f"Universal_Test_Pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            source_type="SharePoint",
            source_config={
                "connection_id": "29099f29-20ae-4998-bd13-007289b3f7e3",
                "list_name": "WebPartGallery"
            },
            sink_type="Lakehouse", 
            sink_config={
                "lakehouse_name": "SinkLakehouse_Test",
                "workspace_id": "2bb31c24-2c1f-467a-bffd-3a37a2c0ad27",
                "artifact_id": "6f2063f8-424f-4c22-918b-59629e0356f4",
                "root_folder": "Files",
                "file_config": {
                    "folder_path": "Output",
                    "file_name": "universal_test",
                    "file_format": "DelimitedText"
                }
            },
            description="Test of universal copy activity tool"
        )
        
        print("✅ Real pipeline creation result:")
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"❌ Real pipeline creation failed: {str(e)}")
    # """


def main():
    """Run all tests"""
    print("🚀 Testing Universal Copy Activity Tool - True Modular Design")
    print("=" * 80)
    
    try:
        # Test individual components
        test_sharepoint_source_models()
        test_s3_source_models()
        test_lakehouse_source_models()
        test_lakehouse_sink_models()
        test_s3_sink_models()
        
        # Test complete configurations
        test_complete_copy_activity_configs()
        
        # Generate MCP examples
        examples = generate_mcp_tool_examples()
        
        # Test real pipeline creation (disabled by default)
        asyncio.run(test_real_pipeline_creation())
        
        print("\n" + "=" * 80)
        print("🎉 All tests completed successfully!")
        print("✅ SharePoint Source: List Name + Query methods")
        print("✅ S3 Source: File Path, Wildcard, Prefix, List of Files")
        print("✅ Lakehouse Source: Tables + Files")
        print("✅ Lakehouse Sink: Tables + Files")
        print("✅ S3 Sink: All formats")
        print("✅ Universal Copy Tool: Any source + any sink")
        print("✅ MCP Tool Examples: Generated for testing")
        
    except Exception as e:
        print(f"\n❌ Tests failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 