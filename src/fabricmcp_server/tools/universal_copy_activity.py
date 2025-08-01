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

class FileSystemSource(BaseModel):
    """Local file system source configuration"""
    connection_id: str = Field(..., description="Connection ID for file system (on-premises gateway)")
    folder_path: Optional[str] = Field(None, description="Path to the source folder")
    file_name: Optional[str] = Field(None, description="Specific file name or wildcard pattern")
    file_format: str = Field("DelimitedText", description="File format: DelimitedText, JSON, Parquet, Binary")
    recursive: bool = Field(True, description="Read files recursively from subfolders")
    wildcard_folder_path: Optional[str] = Field(None, description="Wildcard pattern for folder filtering")
    wildcard_file_name: Optional[str] = Field(None, description="Wildcard pattern for file filtering")
    file_list_path: Optional[str] = Field(None, description="Path to file containing list of files to copy")
    delete_files_after_completion: bool = Field(False, description="Delete source files after successful copy")
    modified_datetime_start: Optional[str] = Field(None, description="Filter files by last modified start time (ISO format)")
    modified_datetime_end: Optional[str] = Field(None, description="Filter files by last modified end time (ISO format)")
    max_concurrent_connections: Optional[int] = Field(None, description="Maximum concurrent connections")
    
    # New fields based on user's manual JSON
    enable_partition_discovery: bool = Field(False, description="Enable automatic partition discovery")
    partition_root_path: Optional[str] = Field(None, description="Root path for partition discovery")
    add_file_name_column: bool = Field(False, description="Add file name as additional column")
    file_name_column_name: str = Field("file_name", description="Name for the file name column")
    
    # Delimited text specific properties
    column_delimiter: str = Field(",", description="Column delimiter for delimited text")
    escape_char: str = Field("\\", description="Escape character for delimited text")
    quote_char: str = Field("\"", description="Quote character for delimited text")
    first_row_as_header: bool = Field(True, description="Treat first row as header")

    def to_copy_activity_source(self) -> Dict[str, Any]:
        """Convert to copy activity source JSON structure"""
        source_config = {
            "type": f"{self.file_format}Source",
            "storeSettings": {
                "type": "FileServerReadSettings",
                "recursive": self.recursive
            },
            "formatSettings": {
                "type": f"{self.file_format}ReadSettings"
            },
            "datasetSettings": {
                "annotations": [],
                "type": self.file_format,
                "typeProperties": {
                    "location": {
                        "type": "FileServerLocation"
                    }
                },
                "schema": [],
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }

        # Add additional columns if requested (like file name)
        if self.add_file_name_column:
            source_config["additionalColumns"] = [
                {
                    "name": self.file_name_column_name,
                    "value": "$$FILEPATH"
                }
            ]

        # Add location properties
        location = source_config["datasetSettings"]["typeProperties"]["location"]
        if self.folder_path:
            location["folderPath"] = self.folder_path
        if self.file_name:
            location["fileName"] = self.file_name

        # Add store settings
        store_settings = source_config["storeSettings"]
        if self.wildcard_folder_path:
            store_settings["wildcardFolderPath"] = self.wildcard_folder_path
        if self.wildcard_file_name:
            store_settings["wildcardFileName"] = self.wildcard_file_name
        if self.file_list_path:
            store_settings["fileListPath"] = self.file_list_path
        if self.delete_files_after_completion:
            store_settings["deleteFilesAfterCompletion"] = self.delete_files_after_completion
        if self.modified_datetime_start:
            store_settings["modifiedDatetimeStart"] = self.modified_datetime_start
        if self.modified_datetime_end:
            store_settings["modifiedDatetimeEnd"] = self.modified_datetime_end
        if self.max_concurrent_connections:
            store_settings["maxConcurrentConnections"] = self.max_concurrent_connections
        if self.enable_partition_discovery:
            store_settings["enablePartitionDiscovery"] = self.enable_partition_discovery
        if self.partition_root_path:
            store_settings["partitionRootPath"] = self.partition_root_path

        # Add format-specific properties to typeProperties
        if self.file_format == "DelimitedText":
            source_config["datasetSettings"]["typeProperties"].update({
                "columnDelimiter": self.column_delimiter,
                "escapeChar": self.escape_char,
                "quoteChar": self.quote_char,
                "firstRowAsHeader": self.first_row_as_header
            })

        return source_config

class MySqlSource(BaseModel):
    """MySQL database source configuration (via on-premises gateway)"""
    connection_id: str = Field(..., description="Connection ID for MySQL database (on-premises gateway)")
    
    # Query options - either table_name OR query, not both
    table_name: Optional[str] = Field(None, description="Table name to read from (can include backticks like `table_name`)")
    query: Optional[str] = Field(None, description="Custom SQL query to execute")
    
    # Additional columns support
    additional_columns: Optional[List[Dict[str, str]]] = Field(None, description="Additional columns with computed values like $$COLUMN:sum")
    
    # These fields are not needed for the JSON generation but kept for completeness
    server: Optional[str] = Field(None, description="MySQL server hostname or IP address (not used in copy activity JSON)")
    port: Optional[int] = Field(3306, description="MySQL server port number (not used in copy activity JSON)")
    database: Optional[str] = Field(None, description="MySQL database name (not used in copy activity JSON)")
    username: Optional[str] = Field(None, description="MySQL username (not used in copy activity JSON)")
    password: Optional[str] = Field(None, description="MySQL password (not used in copy activity JSON)")

    def to_copy_activity_source(self) -> Dict[str, Any]:
        """Convert to copy activity source JSON structure based on user's manual JSON"""
        source_config = {
            "type": "MySqlSource",
            "datasetSettings": {
                "annotations": [],
                "type": "MySqlTable",
                "schema": [],
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }
        
        # Add additional columns if specified (like $$COLUMN:sum)
        if self.additional_columns:
            source_config["additionalColumns"] = self.additional_columns
        
        # Add table name to typeProperties if specified
        if self.table_name:
            source_config["datasetSettings"]["typeProperties"] = {
                "tableName": self.table_name
            }
        
        # Add query at root level if specified (not in datasetSettings)
        if self.query:
            source_config["query"] = self.query
        
        return source_config

# Google Cloud Storage Models (S3-compatible API)
class GoogleCloudStoragePathType(str, Enum):
    FILE_PATH = "file_path"
    WILDCARD = "wildcard" 
    PREFIX = "prefix"
    LIST_OF_FILES = "list_of_files"

class GoogleCloudStorageFilePathConfig(BaseModel):
    """Google Cloud Storage file path configuration"""
    path_type: GoogleCloudStoragePathType = GoogleCloudStoragePathType.FILE_PATH
    
    # For FILE_PATH
    bucket_name: Optional[str] = Field(None, description="GCS bucket name")
    object_key: Optional[str] = Field(None, description="Object key (file path) in bucket")
    
    # For WILDCARD
    wildcard_folder_path: Optional[str] = Field(None, description="Folder path with wildcards")
    wildcard_file_name: Optional[str] = Field(None, description="File name with wildcards")
    
    # For PREFIX
    prefix: Optional[str] = Field(None, description="Prefix for GCS key names")
    
    # For LIST_OF_FILES
    file_list_path: Optional[str] = Field(None, description="Path to text file containing list of files")

class GoogleCloudStorageSource(BaseModel):
    """Google Cloud Storage as source configuration"""
    connection_id: str = Field(..., description="Google Cloud Storage connection ID")
    bucket_name: str = Field(..., description="GCS bucket name")
    
    # File path configuration
    file_path_config: GoogleCloudStorageFilePathConfig = Field(default_factory=GoogleCloudStorageFilePathConfig)
    
    # Format and processing options
    file_format: str = Field("DelimitedText", description="File format: DelimitedText, JSON, Parquet, Avro, Binary")
    recursive: bool = Field(True, description="Read files recursively from subfolders")
    delete_files_after_completion: bool = Field(False, description="Delete files after successful copy")
    
    # Performance and filtering
    max_concurrent_connections: int = Field(1, description="Maximum concurrent connections")
    modified_datetime_start: Optional[str] = Field(None, description="Filter files by last modified start time")
    modified_datetime_end: Optional[str] = Field(None, description="Filter files by last modified end time")
    enable_partition_discovery: bool = Field(False, description="Parse partitions from file path")
    partition_root_path: Optional[str] = Field(None, description="Partition root path for discovery")
    
    # Additional columns support
    additional_columns: Optional[List[Dict[str, str]]] = Field(None, description="Additional columns with computed values")

    def to_copy_activity_source(self) -> Dict[str, Any]:
        """Convert to copy activity source JSON structure matching Fabric UI exactly"""
        # Determine source type based on file format
        source_type = f"{self.file_format}Source"
        format_settings = {
            "type": f"{self.file_format}ReadSettings"
        }

        # Build storeSettings
        store_settings = {
            "type": "GoogleCloudStorageReadSettings",
            "maxConcurrentConnections": self.max_concurrent_connections
        }

        # Add recursive only for certain path types
        path_config = self.file_path_config
        if path_config.path_type in [GoogleCloudStoragePathType.FILE_PATH, GoogleCloudStoragePathType.WILDCARD, GoogleCloudStoragePathType.PREFIX]:
            store_settings["recursive"] = self.recursive

        # Add delete files after completion if specified
        if self.delete_files_after_completion:
            store_settings["deleteFilesAfterCompletion"] = self.delete_files_after_completion

        # Add time filters if specified
        if self.modified_datetime_start:
            store_settings["modifiedDatetimeStart"] = self.modified_datetime_start
        if self.modified_datetime_end:
            store_settings["modifiedDatetimeEnd"] = self.modified_datetime_end

        # Add partition discovery
        if self.enable_partition_discovery:
            store_settings["enablePartitionDiscovery"] = True
            if self.partition_root_path:
                store_settings["partitionRootPath"] = self.partition_root_path

        # Add file path configuration based on type
        if path_config.path_type == GoogleCloudStoragePathType.WILDCARD:
            if path_config.wildcard_folder_path:
                store_settings["wildcardFolderPath"] = path_config.wildcard_folder_path
            if path_config.wildcard_file_name:
                store_settings["wildcardFileName"] = path_config.wildcard_file_name
        elif path_config.path_type == GoogleCloudStoragePathType.PREFIX:
            if path_config.prefix:
                store_settings["prefix"] = path_config.prefix
        elif path_config.path_type == GoogleCloudStoragePathType.LIST_OF_FILES:
            if path_config.file_list_path:
                store_settings["fileListPath"] = path_config.file_list_path

        source_config = {
            "type": source_type,
            "storeSettings": store_settings,
            "formatSettings": format_settings
        }

        # Add additional columns if specified
        if self.additional_columns:
            source_config["additionalColumns"] = self.additional_columns

        # Build dataset settings - location logic depends on path type
        location = {
            "type": "GoogleCloudStorageLocation",
            "bucketName": self.bucket_name
        }

        # Only add folderPath/fileName for FILE_PATH type or LIST_OF_FILES
        if path_config.path_type == GoogleCloudStoragePathType.FILE_PATH and path_config.object_key:
            # Split object key into folder path and file name
            if '/' in path_config.object_key:
                folder_path = '/'.join(path_config.object_key.split('/')[:-1])
                file_name = path_config.object_key.split('/')[-1]
                location["folderPath"] = folder_path
                location["fileName"] = file_name
            else:
                location["fileName"] = path_config.object_key
        elif path_config.path_type == GoogleCloudStoragePathType.LIST_OF_FILES and path_config.file_list_path:
            # For list of files, add folderPath if specified
            if '/' in path_config.file_list_path:
                folder_path = '/'.join(path_config.file_list_path.split('/')[:-1])
                location["folderPath"] = folder_path

        # Build type properties based on format
        if self.file_format == "DelimitedText":
            dataset_type = "DelimitedText"
            type_properties = {
                "location": location,
                "columnDelimiter": ",",
                "escapeChar": "\\",
                "firstRowAsHeader": True,
                "quoteChar": "\""
            }
        elif self.file_format == "JSON":
            dataset_type = "Json"
            type_properties = {
                "location": location
            }
        elif self.file_format == "Parquet":
            dataset_type = "Parquet"
            type_properties = {
                "location": location
            }
        elif self.file_format == "Avro":
            dataset_type = "Avro"
            type_properties = {
                "location": location
            }
        else:  # Binary
            dataset_type = "Binary"
            type_properties = {
                "location": location
            }

        source_config["datasetSettings"] = {
            "annotations": [],
            "type": dataset_type,
            "schema": [],
            "typeProperties": type_properties,
            "externalReferences": {
                "connection": self.connection_id
            }
        }

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
        
        # Base config - storeSettings only for file sinks, not table sinks
        base_config = {
            "type": sink_type,
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
        
        # Add storeSettings only for file sinks (not table sinks)
        if self.root_folder == "Files":
            base_config["storeSettings"] = {
                "type": store_settings_type,
                "maxConcurrentConnections": self.max_concurrent_connections,
                "copyBehavior": self.copy_behavior,
                "blockSizeInMB": self.block_size_mb
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
    """REST API sink configuration"""
    connection_id: str = Field(..., description="Connection ID for REST API")
    relative_url: Optional[str] = Field(None, description="Relative URL for the REST endpoint")
    request_method: str = Field("POST", description="HTTP method: GET, POST, PUT, DELETE")
    http_request_timeout: str = Field("00:05:00", description="HTTP request timeout (hh:mm:ss)")
    request_interval: int = Field(10, description="Interval between requests in milliseconds (10-60000)")
    write_batch_size: int = Field(10000, description="Number of records to write in each batch")
    http_compression_type: str = Field("none", description="HTTP compression: 'none' or 'gzip'")
    additional_headers: Optional[str] = Field(None, description="Additional HTTP headers as string")

    def to_copy_activity_sink(self) -> Dict[str, Any]:
        """Convert to copy activity sink JSON structure"""
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
        
        if self.relative_url:
            sink_config["datasetSettings"]["typeProperties"]["relativeUrl"] = self.relative_url
        if self.additional_headers:
            sink_config["additionalHeaders"] = self.additional_headers
            
        return sink_config

class FileSystemSink(BaseModel):
    """Local file system sink configuration"""
    connection_id: str = Field(..., description="Connection ID for file system (on-premises gateway)")
    folder_path: Optional[str] = Field(None, description="Path to the destination folder")
    file_name: Optional[str] = Field(None, description="Destination file name")
    file_format: str = Field("DelimitedText", description="File format: DelimitedText, JSON, Parquet, Binary")
    copy_behavior: str = Field("PreserveHierarchy", description="Copy behavior: PreserveHierarchy, FlattenHierarchy, MergeFiles")
    max_concurrent_connections: Optional[int] = Field(None, description="Maximum concurrent connections")
    
    # New fields based on user's manual JSON  
    file_extension: str = Field(".txt", description="File extension for output files")
    
    # Delimited text specific properties
    column_delimiter: str = Field(",", description="Column delimiter for delimited text")
    escape_char: str = Field("\\", description="Escape character for delimited text")
    quote_char: str = Field("\"", description="Quote character for delimited text")
    first_row_as_header: bool = Field(True, description="Treat first row as header")

    def to_copy_activity_sink(self) -> Dict[str, Any]:
        """Convert to copy activity sink JSON structure"""
        sink_config = {
            "type": f"{self.file_format}Sink",
            "storeSettings": {
                "type": "FileServerWriteSettings",
                "copyBehavior": self.copy_behavior
            },
            "formatSettings": {
                "type": f"{self.file_format}WriteSettings"
            },
            "datasetSettings": {
                "annotations": [],
                "type": self.file_format,
                "typeProperties": {
                    "location": {
                        "type": "FileServerLocation"
                    }
                },
                "schema": [],
                "externalReferences": {
                    "connection": self.connection_id
                }
            }
        }

        # Add location properties
        location = sink_config["datasetSettings"]["typeProperties"]["location"]
        if self.folder_path:
            location["folderPath"] = self.folder_path
        if self.file_name:
            location["fileName"] = self.file_name

        # Add store settings
        if self.max_concurrent_connections:
            sink_config["storeSettings"]["maxConcurrentConnections"] = self.max_concurrent_connections

        # Add format-specific settings and properties
        if self.file_format == "DelimitedText":
            # Add file extension to formatSettings
            sink_config["formatSettings"]["fileExtension"] = self.file_extension
            
            # Add delimited text properties to typeProperties
            sink_config["datasetSettings"]["typeProperties"].update({
                "columnDelimiter": self.column_delimiter,
                "escapeChar": self.escape_char,
                "quoteChar": self.quote_char,
                "firstRowAsHeader": self.first_row_as_header
            })
        elif self.file_format == "JSON":
            sink_config["formatSettings"]["filePattern"] = "setOfObjects"

        return sink_config

# Google Cloud Storage Models (S3-compatible API)
class GoogleCloudStorageSink(BaseModel):
    """Google Cloud Storage as sink configuration"""
    connection_id: str = Field(..., description="Google Cloud Storage connection ID")
    bucket_name: str = Field(..., description="GCS bucket name")
    folder_path: Optional[str] = Field(None, description="Folder path in bucket")
    file_name: Optional[str] = Field(None, description="File name")
    
    # File format and compression
    file_format: str = Field("DelimitedText", description="File format: DelimitedText, JSON, Parquet, Avro, Binary")
    compression_codec: Optional[str] = Field(None, description="Compression codec: gzip, snappy, etc.")
    
    # JSON specific settings
    json_file_pattern: str = Field("setOfObjects", description="JSON file pattern: setOfObjects or arrayOfObjects")
    
    # Write settings
    max_concurrent_connections: int = Field(1, description="Maximum concurrent connections")
    copy_behavior: str = Field("PreserveHierarchy", description="Copy behavior: PreserveHierarchy, FlattenHierarchy, MergeFiles")
    block_size_mb: int = Field(50, description="Block size in MB")
    
    # Metadata settings
    metadata: Optional[List[Dict[str, str]]] = Field(None, description="Custom metadata for files")

    def to_copy_activity_sink(self) -> Dict[str, Any]:
        """Convert to copy activity sink JSON structure matching Fabric UI exactly"""
        # Determine sink type based on file format with correct capitalization
        if self.file_format == "JSON":
            sink_type = "JsonSink"  # Use JsonSink, not JSONSink
        else:
            sink_type = f"{self.file_format}Sink"

        # Build storeSettings
        store_settings = {
            "type": "GoogleCloudStorageWriteSettings",
            "maxConcurrentConnections": self.max_concurrent_connections,
            "copyBehavior": self.copy_behavior
        }

        # Add block size if not default
        if self.block_size_mb != 50:
            store_settings["blockSizeInMB"] = self.block_size_mb

        # Add metadata if specified
        if self.metadata:
            store_settings["metadata"] = self.metadata

        sink_config = {
            "type": sink_type,
            "storeSettings": store_settings
        }

        # Add formatSettings based on format (but NOT for Binary)
        if self.file_format == "DelimitedText":
            sink_config["formatSettings"] = {
                "type": "DelimitedTextWriteSettings",
                "fileExtension": ".txt"
            }
        elif self.file_format == "JSON":
            sink_config["formatSettings"] = {
                "type": "JsonWriteSettings",
                "filePattern": self.json_file_pattern  # arrayOfObjects or setOfObjects
            }
        elif self.file_format == "Parquet":
            sink_config["formatSettings"] = {
                "type": "ParquetWriteSettings"
            }
        elif self.file_format == "Avro":
            sink_config["formatSettings"] = {
                "type": "AvroWriteSettings"
            }
        # Binary format does NOT have formatSettings

        # Build location
        location = {
            "type": "GoogleCloudStorageLocation",
            "bucketName": self.bucket_name
        }

        # Add folder path and file name if specified
        if self.folder_path:
            location["folderPath"] = self.folder_path
        if self.file_name:
            location["fileName"] = self.file_name

        # Build type properties based on format
        if self.file_format == "DelimitedText":
            dataset_type = "DelimitedText"
            type_properties = {
                "location": location,
                "columnDelimiter": ",",
                "escapeChar": "\\",
                "firstRowAsHeader": True,
                "quoteChar": "\""
            }
        elif self.file_format == "JSON":
            dataset_type = "Json"
            type_properties = {
                "location": location
            }
        elif self.file_format == "Parquet":
            dataset_type = "Parquet"
            type_properties = {
                "location": location
            }
        elif self.file_format == "Avro":
            dataset_type = "Avro"
            type_properties = {
                "location": location
            }
        else:  # Binary
            dataset_type = "Binary"
            type_properties = {
                "location": location
            }

        # Add compression if specified (not in location but as separate property)
        if self.compression_codec:
            type_properties["compression"] = {
                "type": self.compression_codec
            }

        # Build dataset settings with correct schema format
        dataset_settings = {
            "annotations": [],
            "type": dataset_type,
            "typeProperties": type_properties,
            "externalReferences": {
                "connection": self.connection_id
            }
        }

        # Set schema format based on type
        if self.file_format == "JSON":
            dataset_settings["schema"] = {}  # Empty object for JSON, not array
        else:
            dataset_settings["schema"] = []  # Array for other formats

        sink_config["datasetSettings"] = dataset_settings

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
        source_type: Type of source (SharePoint, S3, Lakehouse, HTTP, REST, FileSystem, MySQL)
        source_config: Source configuration dictionary
        sink_type: Type of sink (Lakehouse, S3, REST, FileSystem)
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
        elif source_type.lower() == "filesystem":
            source = FileSystemSource(**source_config)
        elif source_type.lower() == "mysql":
            source = MySqlSource(**source_config)
        elif source_type.lower() == "googlecloudstorage":
            source = GoogleCloudStorageSource(**source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}. Supported: SharePoint, S3, Lakehouse, HTTP, REST, FileSystem, MySQL, GoogleCloudStorage")
            
        # Parse sink configuration  
        if sink_type.lower() == "lakehouse":
            sink = LakehouseSink(**sink_config)
        elif sink_type.lower() == "s3":
            sink = S3Sink(**sink_config)
        elif sink_type.lower() == "rest":
            sink = RestSink(**sink_config)
        elif sink_type.lower() == "filesystem":
            sink = FileSystemSink(**sink_config)
        elif sink_type.lower() == "googlecloudstorage":
            sink = GoogleCloudStorageSink(**sink_config)
        else:
            raise ValueError(f"Unsupported sink type: {sink_type}. Supported: Lakehouse, S3, REST, FileSystem, GoogleCloudStorage")
            
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
