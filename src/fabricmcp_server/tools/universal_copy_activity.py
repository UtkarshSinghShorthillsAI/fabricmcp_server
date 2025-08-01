"""
Universal Copy Activity Tool - True Modular Design

This module implements the user's desired modularity:
- Individual source models for each source type (SharePoint, S3, Lakehouse)
- Individual sink models for each sink type (Lakehouse, S3)  
- Universal copy activity tool that accepts any source + sink combination
- Proper handling of file path types and table vs file configurations
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from enum import Enum

from fastmcp import FastMCP, Context
from pydantic import BaseModel, Field, model_validator, field_validator

from ..fabric_models import ItemDefinitionForCreate, CreateItemRequest, DefinitionPart
from ..sessions import get_session_fabric_client

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS AND BASE CONFIGURATIONS
# =============================================================================

class FilePathType(str, Enum):
    """S3 file path types that affect JSON structure"""
    FILE_PATH = "file_path"
    WILDCARD = "wildcard" 
    PREFIX = "prefix"
    LIST_OF_FILES = "list_of_files"


class TableConfiguration(BaseModel):
    """Table-specific configuration for table-based sources/sinks"""
    table_name: str
    schema_name: Optional[str] = None


class FileConfiguration(BaseModel):
    """File-specific configuration for file-based sources/sinks"""
    folder_path: Optional[str] = None
    file_name: Optional[str] = None
    file_format: str = "DelimitedText"  # DelimitedText, JSON, Binary, etc.


class S3FilePathConfig(BaseModel):
    """S3-specific file path configuration based on path type"""
    path_type: FilePathType
    
    # File path type
    folder_path: Optional[str] = None
    file_name: Optional[str] = None
    
    # Wildcard type  
    wildcard_folder_path: Optional[str] = None
    wildcard_file_name: Optional[str] = None
    
    # Prefix type
    prefix: Optional[str] = None
    
    # List of files type
    file_list_path: Optional[str] = None
    list_folder_path: Optional[str] = None
    
    @model_validator(mode='after')
    def validate_path_type_config(self):
        """Validate that the correct fields are provided for each path type"""
        if self.path_type == FilePathType.FILE_PATH:
            if not self.folder_path or not self.file_name:
                raise ValueError("folder_path and file_name required for FILE_PATH type")
        elif self.path_type == FilePathType.WILDCARD:
            if not self.wildcard_folder_path or not self.wildcard_file_name:
                raise ValueError("wildcard_folder_path and wildcard_file_name required for WILDCARD type")
        elif self.path_type == FilePathType.PREFIX:
            if not self.prefix:
                raise ValueError("prefix required for PREFIX type")
        elif self.path_type == FilePathType.LIST_OF_FILES:
            if not self.file_list_path or not self.list_folder_path:
                raise ValueError("file_list_path and list_folder_path required for LIST_OF_FILES type")
        return self


# =============================================================================
# SOURCE MODELS - Individual models for each source type
# =============================================================================

class SharePointSource(BaseModel):
    """SharePoint Online List as source"""
    connection_id: str
    list_name: str  
    query: Optional[str] = None
    http_request_timeout: str = "00:05:00"
    
    def to_copy_activity_source(self) -> Dict[str, Any]:
        """Generate SharePoint source JSON for copy activity"""
        source_config = {
            "type": "SharePointOnlineListSource",
            "httpRequestTimeout": self.http_request_timeout,
            "datasetSettings": {
                "annotations": [],
                "type": "SharePointOnlineListResource",
                "schema": [],
                "typeProperties": {
                    "listName": self.list_name
                },
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }
        
        # Add query if provided
        if self.query:
            source_config["query"] = self.query
            
        return source_config


class S3Source(BaseModel):
    """Amazon S3 as source with support for different file path types"""
    connection_id: str
    bucket_name: str
    source_type: str = "BinarySource"  # BinarySource, JsonSource, DelimitedTextSource
    format_type: str = "Binary"        # Binary, Json, DelimitedText
    
    # S3 file path configuration
    file_path_config: S3FilePathConfig
    
    # Store settings
    max_concurrent_connections: int = 1
    recursive: bool = True
    modified_datetime_start: Optional[str] = None
    modified_datetime_end: Optional[str] = None  
    delete_files_after_completion: bool = False
    enable_partition_discovery: bool = False
    
    def to_copy_activity_source(self) -> Dict[str, Any]:
        """Generate S3 source JSON based on file path type"""
        
        # Base store settings
        store_settings = {
            "type": "AmazonS3ReadSettings",
            "maxConcurrentConnections": self.max_concurrent_connections,
            "recursive": self.recursive,
            "deleteFilesAfterCompletion": self.delete_files_after_completion
        }
        
        # Add optional datetime filters
        if self.modified_datetime_start:
            store_settings["modifiedDatetimeStart"] = self.modified_datetime_start
        if self.modified_datetime_end:
            store_settings["modifiedDatetimeEnd"] = self.modified_datetime_end
        if self.enable_partition_discovery:
            store_settings["enablePartitionDiscovery"] = self.enable_partition_discovery
            
        # Base location (always has bucket)
        location = {
            "type": "AmazonS3Location",
            "bucketName": self.bucket_name
        }
        
        # Add path-type specific configurations
        if self.file_path_config.path_type == FilePathType.FILE_PATH:
            location["folderPath"] = self.file_path_config.folder_path
            location["fileName"] = self.file_path_config.file_name
            
        elif self.file_path_config.path_type == FilePathType.WILDCARD:
            store_settings["wildcardFolderPath"] = self.file_path_config.wildcard_folder_path
            store_settings["wildcardFileName"] = self.file_path_config.wildcard_file_name
            
        elif self.file_path_config.path_type == FilePathType.PREFIX:
            store_settings["prefix"] = self.file_path_config.prefix
            
        elif self.file_path_config.path_type == FilePathType.LIST_OF_FILES:
            location["folderPath"] = self.file_path_config.list_folder_path
            store_settings["fileListPath"] = self.file_path_config.file_list_path
        
        return {
            "type": self.source_type,
            "storeSettings": store_settings,
            "formatSettings": {
                "type": f"{self.format_type}ReadSettings"
            },
            "datasetSettings": {
                "annotations": [],
                "type": self.format_type,
                "typeProperties": {
                    "location": location
                },
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }


class LakehouseSource(BaseModel):
    """Fabric Lakehouse as source - supports both Tables and Files"""
    lakehouse_name: str
    workspace_id: str
    artifact_id: str
    root_folder: str = "Tables"  # "Tables" or "Files"
    
    # Conditional configurations
    table_config: Optional[TableConfiguration] = None
    file_config: Optional[FileConfiguration] = None
    
    # Advanced table options
    timestamp_as_of: Optional[str] = None
    version_as_of: Optional[int] = None
    
    @model_validator(mode='after')
    def validate_conditional_configs(self):
        """Validate that correct config is provided for root folder type"""
        if self.root_folder == "Tables" and not self.table_config:
            raise ValueError("table_config required when root_folder='Tables'")
        if self.root_folder == "Files" and not self.file_config:
            raise ValueError("file_config required when root_folder='Files'")
        return self
    
    def to_copy_activity_source(self) -> Dict[str, Any]:
        """Generate Lakehouse source JSON based on configuration"""
        
        # Determine source type based on root folder and format
        if self.root_folder == "Tables":
            source_type = "LakehouseTableSource"
            dataset_type = "LakehouseTable"
        else:
            # For files, use format from file_config
            format_type = self.file_config.file_format if self.file_config else "DelimitedText"
            if format_type == "JSON":
                source_type = "JsonSource"
                dataset_type = "Json"
            elif format_type == "Binary":
                source_type = "BinarySource" 
                dataset_type = "Binary"
            else:
                source_type = "DelimitedTextSource"
                dataset_type = "DelimitedText"
        
        base_config = {
            "type": source_type,
            "datasetSettings": {
                "annotations": [],
                "linkedService": {
                    "name": self.lakehouse_name,
                    "properties": {
                        "annotations": [],
                        "type": "Lakehouse",
                        "typeProperties": {
                            "workspaceId": self.workspace_id,
                            "artifactId": self.artifact_id,
                            "rootFolder": self.root_folder
                        }
                    }
                },
                "type": dataset_type,
                "schema": []
            }
        }
        
        # Add type properties based on configuration
        if self.root_folder == "Tables" and self.table_config:
            base_config["datasetSettings"]["typeProperties"] = {
                "table": self.table_config.table_name
            }
            if self.table_config.schema_name:
                base_config["datasetSettings"]["typeProperties"]["schema"] = self.table_config.schema_name
                
            # Add advanced table options
            if self.timestamp_as_of:
                base_config["timestampAsOf"] = self.timestamp_as_of
            if self.version_as_of:
                base_config["versionAsOf"] = self.version_as_of
                
        elif self.root_folder == "Files" and self.file_config:
            base_config["datasetSettings"]["typeProperties"] = {
                "location": {
                    "type": "LakehouseLocation",
                    "fileName": self.file_config.file_name,
                    "folderPath": self.file_config.folder_path
                }
            }
            
        return base_config



class HttpSource(BaseModel):
    """HTTP endpoint as source - supports any HTTP endpoint for data retrieval"""
    connection_id: str = Field(..., description="Connection ID for HTTP endpoint")
    relative_url: Optional[str] = Field(None, description="Relative URL path to append to base URL")
    
    # HTTP method and configuration
    request_method: str = Field("GET", description="HTTP method: GET or POST")
    request_body: Optional[str] = Field(None, description="Request body for POST requests")
    
    # Headers and authentication
    additional_headers: Optional[str] = Field(None, description="Additional HTTP headers as string")
    
    # Performance and timeout settings
    request_timeout: str = Field("00:02:40", description="HTTP request timeout (hh:mm:ss)")
    max_concurrent_connections: int = Field(1, description="Maximum concurrent connections")
    
    # Format settings for delimited text
    column_delimiter: str = Field(",", description="Column delimiter for delimited text")
    escape_char: str = Field("\\", description="Escape character")
    first_row_as_header: bool = Field(True, description="First row as header")
    quote_char: str = Field('"', description="Quote character")
    
    @field_validator('request_method')
    @classmethod 
    def validate_request_method(cls, v):
        if v not in ["GET", "POST"]:
            raise ValueError("request_method must be 'GET' or 'POST'")
        return v
    
    def to_copy_activity_source(self) -> Dict[str, Any]:
        """Generate Copy Activity source JSON for HTTP endpoint"""
        
        source_config = {
            "type": "DelimitedTextSource",
            "storeSettings": {
                "type": "HttpReadSettings",
                "maxConcurrentConnections": self.max_concurrent_connections,
                "requestMethod": self.request_method,
                "requestTimeout": self.request_timeout
            },
            "formatSettings": {
                "type": "DelimitedTextReadSettings"
            },
            "datasetSettings": {
                "annotations": [],
                "type": "DelimitedText",
                "typeProperties": {
                    "location": {
                        "type": "HttpServerLocation"
                    },
                    "columnDelimiter": self.column_delimiter,
                    "escapeChar": self.escape_char,
                    "firstRowAsHeader": self.first_row_as_header,
                    "quoteChar": self.quote_char
                },
                "schema": [],
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }
        
        # Add relative URL if specified
        if self.relative_url:
            source_config["datasetSettings"]["typeProperties"]["location"]["relativeUrl"] = self.relative_url
            
        # Add request body for POST requests
        if self.request_body and self.request_method == "POST":
            source_config["storeSettings"]["requestBody"] = self.request_body
            
        # Add additional headers
        if self.additional_headers:
            source_config["storeSettings"]["additionalHeaders"] = self.additional_headers
            
        return source_config


class RestSource(BaseModel):
    """REST API as source - specifically for RESTful APIs with JSON responses"""
    connection_id: str = Field(..., description="Connection ID for REST API endpoint")
    relative_url: Optional[str] = Field(None, description="Relative URL path to REST resource")
    
    # HTTP method and configuration
    request_method: str = Field("GET", description="HTTP method: GET or POST")
    request_body: Optional[str] = Field(None, description="Request body for POST requests")
    
    # Headers and authentication
    additional_headers: Optional[str] = Field(None, description="Additional HTTP headers as string")
    
    # Pagination support - simplified to match Fabric's actual structure
    support_rfc5988: str = Field("true", description="Support RFC5988 pagination")
    
    # Performance and timeout settings
    http_request_timeout: str = Field("00:01:40", description="HTTP request timeout (hh:mm:ss)")
    request_interval: str = Field("00.00:00:00.010", description="Time between requests for pagination")
    
    @field_validator('request_method')
    @classmethod
    def validate_request_method(cls, v):
        if v not in ["GET", "POST"]:
            raise ValueError("request_method must be 'GET' or 'POST'")
        return v
    
    def to_copy_activity_source(self) -> Dict[str, Any]:
        """Generate Copy Activity source JSON for REST API"""
        
        source_config = {
            "type": "RestSource",
            "httpRequestTimeout": self.http_request_timeout,
            "requestInterval": self.request_interval,
            "requestMethod": self.request_method,
            "paginationRules": {
                "supportRFC5988": self.support_rfc5988
            },
            "datasetSettings": {
                "annotations": [],
                "type": "RestResource",
                "typeProperties": {},
                "schema": [],
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }
        
        # Add relative URL if specified
        if self.relative_url:
            source_config["datasetSettings"]["typeProperties"]["relativeUrl"] = self.relative_url
            
        # Add request body for POST requests
        if self.request_body and self.request_method == "POST":
            source_config["requestBody"] = self.request_body
            
        # Add additional headers
        if self.additional_headers:
            source_config["additionalHeaders"] = self.additional_headers
            
        return source_config


# =============================================================================
# SINK MODELS - Individual models for each sink type  
# =============================================================================

class LakehouseSink(BaseModel):
    """Fabric Lakehouse as sink - supports both Tables and Files"""
    lakehouse_name: str
    workspace_id: str
    artifact_id: str
    root_folder: str = "Files"  # "Tables" or "Files"
    
    # Conditional configurations
    table_config: Optional[TableConfiguration] = None
    file_config: Optional[FileConfiguration] = None
    
    # Table-specific sink options
    table_action_option: str = "Append"  # Append, Overwrite, Upsert
    partition_option: str = "None"  # None, PartitionByKey
    
    # Write settings
    max_concurrent_connections: int = 1
    copy_behavior: str = "PreserveHierarchy"  # PreserveHierarchy, FlattenHierarchy, MergeFiles
    block_size_mb: int = 50
    
    @model_validator(mode='after')
    def validate_conditional_configs(self):
        """Validate that correct config is provided for root folder type"""
        if self.root_folder == "Tables" and not self.table_config:
            raise ValueError("table_config required when root_folder='Tables'")
        if self.root_folder == "Files" and not self.file_config:
            raise ValueError("file_config required when root_folder='Files'")
        return self
    
    def to_copy_activity_sink(self) -> Dict[str, Any]:
        """Generate Lakehouse sink JSON based on configuration"""
        
        # Determine sink type based on root folder and format
        if self.root_folder == "Tables":
            sink_type = "LakehouseTableSink"
            dataset_type = "LakehouseTable"
            store_settings_type = "LakehouseWriteSettings"
        else:
            # For files, use format from file_config
            format_type = self.file_config.file_format if self.file_config else "DelimitedText"
            if format_type == "JSON":
                sink_type = "JsonSink"
                dataset_type = "Json"
                store_settings_type = "LakehouseWriteSettings"
            elif format_type == "Binary":
                sink_type = "BinarySink"
                dataset_type = "Binary" 
                store_settings_type = "LakehouseWriteSettings"
            else:
                sink_type = "DelimitedTextSink"
                dataset_type = "DelimitedText"
                store_settings_type = "LakehouseWriteSettings"
        
        base_config = {
            "type": sink_type,
            "storeSettings": {
                "type": store_settings_type,
                "maxConcurrentConnections": self.max_concurrent_connections,
                "copyBehavior": self.copy_behavior,
                "blockSizeInMB": self.block_size_mb
            },
            "datasetSettings": {
                "annotations": [],
                "linkedService": {
                    "name": self.lakehouse_name,
                    "properties": {
                        "annotations": [],
                        "type": "Lakehouse",
                        "typeProperties": {
                            "workspaceId": self.workspace_id,
                            "artifactId": self.artifact_id,
                            "rootFolder": self.root_folder
                        }
                    }
                },
                "type": dataset_type,
                "schema": []
            }
        }
        
        # Add type properties and table-specific settings
        if self.root_folder == "Tables" and self.table_config:
            base_config["datasetSettings"]["typeProperties"] = {
                "table": self.table_config.table_name
            }
            if self.table_config.schema_name:
                base_config["datasetSettings"]["typeProperties"]["schema"] = self.table_config.schema_name
                
            # Add table action options
            base_config["tableActionOption"] = self.table_action_option
            if self.partition_option != "None":
                base_config["partitionOption"] = self.partition_option
                
        elif self.root_folder == "Files" and self.file_config:
            base_config["datasetSettings"]["typeProperties"] = {
                "location": {
                    "type": "LakehouseLocation",
                    "fileName": self.file_config.file_name,
                    "folderPath": self.file_config.folder_path
                }
            }
            
            # Add format settings for files
            if self.file_config.file_format == "DelimitedText":
                base_config["formatSettings"] = {
                    "type": "DelimitedTextWriteSettings",
                    "fileExtension": ".txt"
                }
            elif self.file_config.file_format == "JSON":
                base_config["formatSettings"] = {
                    "type": "JsonWriteSettings",
                    "filePattern": "setOfObjects"
                }
            
        return base_config


class S3Sink(BaseModel):
    """Amazon S3 as sink"""
    connection_id: str
    bucket_name: str
    sink_type: str = "BinarySink"  # BinarySink, JsonSink, DelimitedTextSink
    format_type: str = "Binary"    # Binary, Json, DelimitedText
    
    # File configuration
    file_config: FileConfiguration
    
    # Write settings
    max_concurrent_connections: int = 1
    copy_behavior: str = "PreserveHierarchy"
    
    def to_copy_activity_sink(self) -> Dict[str, Any]:
        """Generate S3 sink JSON"""
        
        store_settings_type = "AmazonS3CompatibleWriteSettings"
        
        base_config = {
            "type": self.sink_type,
            "storeSettings": {
                "type": store_settings_type,
                "maxConcurrentConnections": self.max_concurrent_connections,
                "copyBehavior": self.copy_behavior
            },
            "datasetSettings": {
                "annotations": [],
                "type": self.format_type,
                "typeProperties": {
                    "location": {
                        "type": "AmazonS3CompatibleLocation",
                        "bucketName": self.bucket_name,
                        "fileName": self.file_config.file_name,
                        "folderPath": self.file_config.folder_path
                    }
                },
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }
        
        # Add format settings
        if self.format_type == "DelimitedText":
            base_config["formatSettings"] = {
                "type": "DelimitedTextWriteSettings",
                "fileExtension": ".csv"
            }
        elif self.format_type == "JSON":
            base_config["formatSettings"] = {
                "type": "JsonWriteSettings",
                "filePattern": "setOfObjects"
            }
            
        return base_config




class RestSink(BaseModel):
    """REST API as sink - supports POST/PUT/PATCH to REST endpoints"""
    connection_id: str = Field(..., description="Connection ID for REST API endpoint")
    relative_url: Optional[str] = Field(None, description="Relative URL path to REST resource")
    
    # HTTP method and configuration
    request_method: str = Field("POST", description="HTTP method: POST, PUT, or PATCH")
    
    # Headers and authentication
    additional_headers: Optional[str] = Field(None, description="Additional HTTP headers as string")
    
    # Performance and timeout settings
    http_request_timeout: str = Field("00:05:00", description="HTTP request timeout (hh:mm:ss)")
    request_interval: int = Field(10, description="Interval between requests in milliseconds (10-60000)")
    write_batch_size: int = Field(10000, description="Number of records per batch")
    
    # Compression options
    http_compression_type: str = Field("none", description="HTTP compression: 'none' or 'gzip'")
    
    @field_validator('request_method')
    @classmethod
    def validate_request_method(cls, v):
        if v not in ["POST", "PUT", "PATCH"]:
            raise ValueError("request_method must be 'POST', 'PUT', or 'PATCH'")
        return v
        
    @field_validator('http_compression_type')
    @classmethod
    def validate_compression_type(cls, v):
        if v not in ["none", "gzip"]:
            raise ValueError("http_compression_type must be 'none' or 'gzip'")
        return v
        
    @field_validator('request_interval')
    @classmethod
    def validate_request_interval(cls, v):
        if not (10 <= v <= 60000):
            raise ValueError("request_interval must be between 10 and 60000 milliseconds")
        return v
    
    def to_copy_activity_sink(self) -> Dict[str, Any]:
        """Generate Copy Activity sink JSON for REST API"""
        
        sink_config = {
            "type": "RestSink",
            "httpRequestTimeout": self.http_request_timeout,
            "requestInterval": self.request_interval,
            "requestMethod": self.request_method,
            "writeBatchSize": self.write_batch_size,
            "httpCompressionType": self.http_compression_type,
            "datasetSettings": {
                "annotations": [],
                "type": "RestResource",
                "typeProperties": {},
                "schema": [],
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }
        
        # Add relative URL if specified
        if self.relative_url:
            sink_config["datasetSettings"]["typeProperties"]["relativeUrl"] = self.relative_url
            
        # Add additional headers
        if self.additional_headers:
            sink_config["additionalHeaders"] = self.additional_headers
            
        return sink_config


# =============================================================================
# COPY ACTIVITY CONFIGURATION
# =============================================================================

class CopyActivityConfig(BaseModel):
    """Additional copy activity components beyond source/sink"""
    activity_name: str = "Copy Activity"
    description: Optional[str] = None
    timeout: str = "0.12:00:00"
    retry_count: int = 0
    retry_interval_seconds: int = 30
    secure_output: bool = False
    secure_input: bool = False
    enable_staging: bool = False
    enable_logging: bool = False
    
    # Translator configuration
    enable_schema_mapping: bool = False
    translator: Optional[Dict[str, Any]] = None
    
    def get_translator(self) -> Optional[Dict[str, Any]]:
        """Generate translator only when needed"""
        if self.enable_schema_mapping and self.translator:
            return {
                "type": "TabularTranslator",
                "typeConversion": True,
                "typeConversionSettings": {
                    "allowDataTruncation": True,
                    "treatBooleanAsNumber": False
                },
                **self.translator
            }
        return None


# =============================================================================
# UNIVERSAL COPY ACTIVITY TOOL
# =============================================================================

async def create_universal_copy_pipeline_impl(
    ctx: Context,
    workspace_id: str,
    pipeline_name: str,
    source_type: str,
    source_config: Dict[str, Any],
    sink_type: str,
    sink_config: Dict[str, Any],
    activity_config: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """
    Universal copy pipeline tool - accepts any source + any sink combination
    
    Args:
        workspace_id: Target workspace ID
        pipeline_name: Name for the pipeline
        source_type: Type of source (SharePoint, S3, Lakehouse, HTTP, REST)
        source_config: Source configuration dictionary
        sink_type: Type of sink (Lakehouse, S3, REST)
        sink_config: Sink configuration dictionary  
        activity_config: Optional copy activity configuration
        description: Optional pipeline description
    """
    
    try:
        client = await get_session_fabric_client(ctx)
        
        # Parse source configuration
        if source_type.lower() == "sharepoint":
            source = SharePointSource(**source_config)
        elif source_type.lower() == "s3":
            source = S3Source(**source_config)
        elif source_type.lower() == "lakehouse":
            source = LakehouseSource(**source_config)
        elif source_type.lower() == "http":
            source = HttpSource(**source_config)
        elif source_type.lower() == "rest":
            source = RestSource(**source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}. Supported: SharePoint, S3, Lakehouse, HTTP, REST")
            
        # Parse sink configuration  
        if sink_type.lower() == "lakehouse":
            sink = LakehouseSink(**sink_config)
        elif sink_type.lower() == "s3":
            sink = S3Sink(**sink_config)
        elif sink_type.lower() == "rest":
            sink = RestSink(**sink_config)
        else:
            raise ValueError(f"Unsupported sink type: {sink_type}. Supported: Lakehouse, S3, REST")
            
        # Parse activity configuration
        if activity_config:
            config = CopyActivityConfig(**activity_config)
        else:
            config = CopyActivityConfig()
        
        # Generate source and sink JSON from the models
        source_json = source.to_copy_activity_source()
        sink_json = sink.to_copy_activity_sink()
        
        # Build complete copy activity JSON
        copy_activity = {
            "name": config.activity_name,
            "type": "Copy",
            "description": config.description,
            "dependsOn": [],
            "policy": {
                "timeout": config.timeout,
                "retry": config.retry_count,
                "retryIntervalInSeconds": config.retry_interval_seconds,
                "secureOutput": config.secure_output,
                "secureInput": config.secure_input
            },
            "typeProperties": {
                "source": source_json,
                "sink": sink_json,
                "enableStaging": config.enable_staging
            }
        }
        
        # Add translator if configured
        translator = config.get_translator()
        if translator:
            copy_activity["typeProperties"]["translator"] = translator
            
        # Build complete pipeline definition
        pipeline_structure = {
            "properties": {
                "activities": [copy_activity],
                "description": description or f"Universal copy pipeline: {source_type} to {sink_type}",
                "concurrency": 1,
                "annotations": ["Universal", "CopyData", f"{source_type}To{sink_type}"],
                "folder": {
                    "name": "Universal Copy Pipelines"
                }
            }
        }
        
        # Create pipeline via Fabric API
        import base64
        pipeline_json = json.dumps(pipeline_structure, indent=2)
        b64_payload = base64.b64encode(pipeline_json.encode("utf-8")).decode("utf-8")
        
        create_request = CreateItemRequest(
            displayName=pipeline_name,
            type="DataPipeline",
            definition=ItemDefinitionForCreate(
                parts=[DefinitionPart(path="pipeline-content.json", payload=b64_payload, payloadType="InlineBase64")]
            )
        )
        
        response = await client.create_item(workspace_id, create_request.model_dump(by_alias=True, exclude_none=True))
        
        return {
            "pipeline_id": response.id,
            "pipeline_name": response.display_name,
            "workspace_id": workspace_id,
            "source_type": source_type,
            "sink_type": sink_type,
            "status": "Created successfully",
            "pipeline_definition": pipeline_structure
        }
        
    except Exception as e:
        return {
            "error": f"Failed to create universal copy pipeline: {str(e)}",
            "source_type": source_type,
            "sink_type": sink_type
        }


# =============================================================================
# TOOL REGISTRATION
# =============================================================================

def register_universal_copy_tools(app: FastMCP):
    """Register universal copy activity tools with the MCP app"""
    logger.info("Registering Universal Copy Activity tools...")
    app.tool(name="create_universal_copy_pipeline")(create_universal_copy_pipeline_impl)
    logger.info("Universal Copy Activity tools registration complete.") 