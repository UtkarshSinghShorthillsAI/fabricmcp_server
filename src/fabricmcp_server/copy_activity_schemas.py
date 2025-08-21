# This is the final, definitive, and correct file: src/fabricmcp_server/copy_activity_schemas.py

from __future__ import annotations
from typing import List, Optional, Literal, Dict, Any, Union, Annotated
from pydantic import BaseModel, Field, model_validator, field_validator
from enum import Enum

# =============================================================================
#  ENUMS AND SUPPORTING CLASSES
# =============================================================================

class FilePathType(str, Enum):
    """File path types that affect JSON structure"""
    FILE_PATH = "file_path"
    WILDCARD = "wildcard" 
    PREFIX = "prefix"
    LIST_OF_FILES = "list_of_files"

class GoogleCloudStoragePathType(str, Enum):
    """Google Cloud Storage path types"""
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
    prefix: Optional[str] = Field(None, description="Prefix for objects")
    
    # For LIST_OF_FILES
    file_list_path: Optional[str] = Field(None, description="Path to file list")

# =============================================================================
#  GENERIC & REUSABLE COMPONENTS
# =============================================================================
class ExternalReferences(BaseModel):
    connection: str

class DatasetReference(BaseModel):
    referenceName: str
    type: Literal["DatasetReference"] = "DatasetReference"

class TabularTranslator(BaseModel):
    type: Literal["TabularTranslator"] = "TabularTranslator"

# =============================================================================
#  SOURCE MODELS (User-Facing, High-Level Schemas)
# =============================================================================

class S3Source(BaseModel):
    connector_type: Literal["S3"]
    connection_id: str
    bucket_name: str
    folder_path: str
    file_name: str

class LakehouseTableSource(BaseModel):
    connector_type: Literal["LakehouseTable"]
    workspace_id: str
    lakehouse_name: str
    lakehouse_id: str
    table_name: str

class SharePointSource(BaseModel):
    """SharePoint Online List as source"""
    connector_type: Literal["SharePoint"]
    connection_id: str
    list_name: str  
    query: Optional[str] = None
    http_request_timeout: str = "00:05:00"

class HttpSource(BaseModel):
    """HTTP endpoint as source - supports any HTTP endpoint for data retrieval"""
    connector_type: Literal["Http"]
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

class RestSource(BaseModel):
    """REST API as source - specifically for RESTful APIs with JSON responses"""
    connector_type: Literal["Rest"]
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

class FileSystemSource(BaseModel):
    """Local file system source configuration"""
    connector_type: Literal["FileSystem"]
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
    enable_partition_discovery: bool = Field(False, description="Enable automatic partition discovery")
    partition_root_path: Optional[str] = Field(None, description="Root path for partition discovery")
    add_file_name_column: bool = Field(False, description="Add file name as additional column")
    file_name_column_name: str = Field("file_name", description="Name for the file name column")
    
    # Delimited text specific properties
    column_delimiter: str = Field(",", description="Column delimiter for delimited text")
    escape_char: str = Field("\\", description="Escape character for delimited text")
    quote_char: str = Field('"', description="Quote character for delimited text")
    first_row_as_header: bool = Field(True, description="Treat first row as header")

class MySqlSource(BaseModel):
    """MySQL database source configuration (via on-premises gateway)"""
    connector_type: Literal["MySql"]
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

class GoogleCloudStorageSource(BaseModel):
    """Google Cloud Storage as source configuration"""
    connector_type: Literal["GoogleCloudStorage"]
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

class LakehouseSource(BaseModel):
    """Enhanced Fabric Lakehouse as source - supports both Tables and Files"""
    connector_type: Literal["Lakehouse"]
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

# =============================================================================
#  SINK MODELS (User-Facing, High-Level Schemas)
# =============================================================================

class LakehouseFileSink(BaseModel):
    connector_type: Literal["LakehouseFile"]
    workspace_id: str
    lakehouse_name: str
    lakehouse_id: str
    folder_path: str
    file_name: str

class DataWarehouseSink(BaseModel):
    connector_type: Literal["DataWarehouse"]
    workspace_id: str
    warehouse_name: str
    warehouse_id: str
    table_name: str

class GCS_Sink(BaseModel):
    connector_type: Literal["GCS"]
    connection_id: str
    bucket_name: str
    folder_path: Optional[str] = None
    file_name: Optional[str] = None

class S3Sink(BaseModel):
    """Amazon S3 as sink"""
    connector_type: Literal["S3"]
    connection_id: str
    bucket_name: str
    sink_type: str = "BinarySink"  # BinarySink, JsonSink, DelimitedTextSink
    format_type: str = "Binary"    # Binary, Json, DelimitedText
    
    # File configuration
    file_config: FileConfiguration
    
    # Write settings
    max_concurrent_connections: int = 1
    copy_behavior: str = "PreserveHierarchy"

class RestSink(BaseModel):
    """REST API sink configuration"""
    connector_type: Literal["RestSink"]
    connection_id: str = Field(..., description="Connection ID for REST API")
    relative_url: Optional[str] = Field(None, description="Relative URL for the REST endpoint")
    request_method: str = Field("POST", description="HTTP method: GET, POST, PUT, DELETE")
    http_request_timeout: str = Field("00:05:00", description="HTTP request timeout (hh:mm:ss)")
    request_interval: int = Field(10, description="Interval between requests in milliseconds (10-60000)")
    write_batch_size: int = Field(10000, description="Number of records to write in each batch")
    http_compression_type: str = Field("none", description="HTTP compression: 'none' or 'gzip'")
    additional_headers: Optional[str] = Field(None, description="Additional HTTP headers as string")

class FileSystemSink(BaseModel):
    """Local file system sink configuration"""
    connector_type: Literal["FileSystem"]
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
    quote_char: str = Field('"', description="Quote character for delimited text")
    first_row_as_header: bool = Field(True, description="Treat first row as header")

class GoogleCloudStorageSink(BaseModel):
    """Google Cloud Storage as sink configuration"""
    connector_type: Literal["GoogleCloudStorageSink"]
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

class LakehouseSink(BaseModel):
    """Enhanced Fabric Lakehouse as sink - supports both Tables and Files"""
    connector_type: Literal["LakehouseSink"]
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

# =============================================================================
#  MASTER UNIONS & API PAYLOAD BUILDERS
# =============================================================================

# These unions use the unique 'connector_type' to choose the right model.
SourceConfig = Annotated[Union[
    S3Source, 
    LakehouseTableSource,
    SharePointSource,
    HttpSource,
    RestSource,
    FileSystemSource,
    MySqlSource,
    GoogleCloudStorageSource,
    LakehouseSource
], Field(discriminator="connector_type")]

SinkConfig = Annotated[Union[
    LakehouseFileSink, 
    DataWarehouseSink, 
    GCS_Sink,
    S3Sink,
    RestSink,
    FileSystemSink,
    GoogleCloudStorageSink,
    LakehouseSink
], Field(discriminator="connector_type")]

def build_source_payload(source: SourceConfig) -> Dict[str, Any]:
    """Builds the final API-compliant JSON for a source."""
    if isinstance(source, S3Source):
        return {
            "type": "BinarySource",
            "storeSettings": {"type": "AmazonS3ReadSettings", "recursive": True},
            "formatSettings": {"type": "BinaryReadSettings"},
            "datasetSettings": {
                "type": "Binary",
                "typeProperties": {
                    "location": {
                        "type": "AmazonS3Location",
                        "bucketName": source.bucket_name,
                        "folderPath": source.folder_path,
                        "fileName": source.file_name,
                    }
                },
                "externalReferences": {"connection": source.connection_id},
            },
        }
    if isinstance(source, LakehouseTableSource):
        return {
            "type": "LakehouseTableSource",
            "datasetSettings": {
                "type": "LakehouseTable",
                "typeProperties": {"table": source.table_name},
                "linkedService": {
                    "name": source.lakehouse_name,
                    "properties": {
                        "type": "Lakehouse",
                        "typeProperties": {
                            "workspaceId": source.workspace_id,
                            "artifactId": source.lakehouse_id,
                            "rootFolder": "Tables",
                        }
                    }
                }
            }
        }
    if isinstance(source, SharePointSource):
        payload = {
            "type": "SharePointOnlineListSource",
            "httpRequestTimeout": source.http_request_timeout,
            "datasetSettings": {
                "annotations": [],
                "type": "SharePointOnlineListResource",
                "schema": [],
                "typeProperties": {
                    "listName": source.list_name
                },
                "externalReferences": {
                    "connection": source.connection_id
                }
            }
        }
        if source.query:
            payload["query"] = source.query
        return payload
    if isinstance(source, HttpSource):
        return {
            "type": "DelimitedTextSource",
            "storeSettings": {
                "type": "HttpReadSettings",
                "requestMethod": source.request_method,
                "requestTimeout": source.request_timeout,
                "maxConcurrentConnections": source.max_concurrent_connections,
                "additionalHeaders": source.additional_headers,
                "requestBody": source.request_body
            },
            "formatSettings": {
                "type": "DelimitedTextReadSettings",
                "skipLineCount": 0,
                "firstRowAsHeader": source.first_row_as_header,
                "columnDelimiter": source.column_delimiter,
                "quoteChar": source.quote_char,
                "escapeChar": source.escape_char
            },
            "datasetSettings": {
                "annotations": [],
                "type": "DelimitedText",
                "schema": [],
                "typeProperties": {
                    "location": {
                        "type": "HttpServerLocation",
                        "relativeUrl": source.relative_url
                    }
                },
                "externalReferences": {
                    "connection": source.connection_id
                }
            }
        }
    if isinstance(source, RestSource):
        return {
            "type": "RestSource",
            "httpRequestTimeout": source.http_request_timeout,
            "requestInterval": source.request_interval,
            "datasetSettings": {
                "annotations": [],
                "type": "RestResource",
                "typeProperties": {
                    "relativeUrl": source.relative_url,
                    "requestMethod": source.request_method,
                    "requestBody": source.request_body,
                    "additionalHeaders": source.additional_headers,
                    "paginationRules": {
                        "supportRFC5988": source.support_rfc5988
                    }
                },
                "schema": [],
                "externalReferences": {
                    "connection": source.connection_id
                }
            }
        }
    if isinstance(source, FileSystemSource):
        source_type = f"{source.file_format}Source"
        store_settings = {
            "type": "FileServerReadSettings",
            "recursive": source.recursive,
            "wildcardFolderPath": source.wildcard_folder_path,
            "wildcardFileName": source.wildcard_file_name,
            "enablePartitionDiscovery": source.enable_partition_discovery,
            "deleteFilesAfterCompletion": source.delete_files_after_completion
        }
        if source.max_concurrent_connections:
            store_settings["maxConcurrentConnections"] = source.max_concurrent_connections
        
        dataset_settings = {
            "annotations": [],
            "type": source.file_format,
            "schema": [],
            "typeProperties": {
                "location": {
                    "type": "FileServerLocation",
                    "folderPath": source.folder_path,
                    "fileName": source.file_name
                }
            },
            "externalReferences": {
                "connection": source.connection_id
            }
        }
        
        payload = {
            "type": source_type,
            "storeSettings": store_settings,
            "datasetSettings": dataset_settings
        }
        
        if source.file_format == "DelimitedText":
            payload["formatSettings"] = {
                "type": "DelimitedTextReadSettings",
                "firstRowAsHeader": source.first_row_as_header,
                "columnDelimiter": source.column_delimiter,
                "quoteChar": source.quote_char,
                "escapeChar": source.escape_char
            }
        
        return payload
    if isinstance(source, MySqlSource):
        payload = {
            "type": "MySqlSource",
            "datasetSettings": {
                "annotations": [],
                "type": "MySqlTable",
                "schema": [],
                "externalReferences": {
                    "connection": source.connection_id
                }
            }
        }
        
        if source.table_name:
            payload["datasetSettings"]["typeProperties"] = {"tableName": source.table_name}
        if source.query:
            payload["sqlReaderQuery"] = source.query
        if source.additional_columns:
            payload["additionalColumns"] = source.additional_columns
            
        return payload
    if isinstance(source, GoogleCloudStorageSource):
        source_type = f"{source.file_format}Source"
        store_settings = {
            "type": "GoogleCloudStorageReadSettings",
            "recursive": source.recursive,
            "enablePartitionDiscovery": source.enable_partition_discovery,
            "maxConcurrentConnections": source.max_concurrent_connections,
            "deleteFilesAfterCompletion": source.delete_files_after_completion
        }
        
        location = {
            "type": "GoogleCloudStorageLocation",
            "bucketName": source.bucket_name
        }
        
        # Handle different path types
        config = source.file_path_config
        if config.path_type == GoogleCloudStoragePathType.FILE_PATH:
            if config.object_key:
                location["fileName"] = config.object_key
        elif config.path_type == GoogleCloudStoragePathType.WILDCARD:
            if config.wildcard_folder_path:
                store_settings["wildcardFolderPath"] = config.wildcard_folder_path
            if config.wildcard_file_name:
                store_settings["wildcardFileName"] = config.wildcard_file_name
        elif config.path_type == GoogleCloudStoragePathType.PREFIX:
            if config.prefix:
                store_settings["prefix"] = config.prefix
        elif config.path_type == GoogleCloudStoragePathType.LIST_OF_FILES:
            if config.file_list_path:
                store_settings["fileListPath"] = config.file_list_path
        
        payload = {
            "type": source_type,
            "storeSettings": store_settings,
            "datasetSettings": {
                "annotations": [],
                "type": source.file_format,
                "schema": [],
                "typeProperties": {
                    "location": location
                },
                "externalReferences": {
                    "connection": source.connection_id
                }
            }
        }
        
        if source.file_format == "DelimitedText":
            payload["formatSettings"] = {
                "type": "DelimitedTextReadSettings"
            }
        elif source.file_format == "JSON":
            payload["formatSettings"] = {
                "type": "JsonReadSettings"
            }
        
        return payload
    if isinstance(source, LakehouseSource):
        # Determine source type based on root folder and format
        if source.root_folder == "Tables":
            source_type = "LakehouseTableSource"
            dataset_type = "LakehouseTable"
        else:
            # For files, use format from file_config
            format_type = source.file_config.file_format if source.file_config else "DelimitedText"
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
                    "name": source.lakehouse_name,
                    "properties": {
                        "annotations": [],
                        "type": "Lakehouse",
                        "typeProperties": {
                            "workspaceId": source.workspace_id,
                            "artifactId": source.artifact_id,
                            "rootFolder": source.root_folder
                        }
                    }
                },
                "type": dataset_type,
                "schema": []
            }
        }
        
        # Add type properties based on configuration
        if source.root_folder == "Tables" and source.table_config:
            base_config["datasetSettings"]["typeProperties"] = {
                "table": source.table_config.table_name
            }
            if source.table_config.schema_name:
                base_config["datasetSettings"]["typeProperties"]["schema"] = source.table_config.schema_name
        elif source.root_folder == "Files" and source.file_config:
            location_props = {}
            if source.file_config.folder_path:
                location_props["folderPath"] = source.file_config.folder_path
            if source.file_config.file_name:
                location_props["fileName"] = source.file_config.file_name
            
            base_config["datasetSettings"]["typeProperties"] = {
                "location": {
                    "type": "LakehouseLocation",
                    **location_props
                }
            }
        
        return base_config
    raise NotImplementedError(f"Source type '{source.connector_type}' is not supported.")

def build_sink_payload(sink: SinkConfig) -> Dict[str, Any]:
    """Builds the final API-compliant JSON for a sink."""
    if isinstance(sink, LakehouseFileSink):
        return {
            "type": "DelimitedTextSink",
            "storeSettings": {"type": "LakehouseWriteSettings"},
            "formatSettings": {"type": "DelimitedTextWriteSettings", "fileExtension": ".csv"},
            "datasetSettings": {
                "type": "DelimitedText",
                "typeProperties": {
                    "location": {
                        "type": "LakehouseLocation",
                        "folderPath": sink.folder_path,
                        "fileName": sink.file_name,
                    }
                },
                "linkedService": {
                    "name": sink.lakehouse_name,
                    "properties": {
                        "type": "Lakehouse",
                        "typeProperties": {
                            "workspaceId": sink.workspace_id,
                            "artifactId": sink.lakehouse_id,
                            "rootFolder": "Files",
                        }
                    }
                },
            },
        }
    if isinstance(sink, DataWarehouseSink):
        return {
            "type": "DataWarehouseSink",
            "allowCopyCommand": True,
            "datasetSettings": {
                "type": "DataWarehouseTable",
                "typeProperties": {"table": sink.table_name},
                "linkedService": {
                    "name": sink.warehouse_name,
                    "properties": {
                        "type": "DataWarehouse",
                        "typeProperties": {
                            "workspaceId": sink.workspace_id,
                            "artifactId": sink.warehouse_id,
                        }
                    }
                },
            },
        }
    if isinstance(sink, GCS_Sink):
        return {
            "type": "BinarySink",
            "storeSettings": {"type": "GoogleCloudStorageWriteSettings"},
            "datasetSettings": {
                "type": "Binary",
                "typeProperties": {
                    "location": {
                        "type": "GoogleCloudStorageLocation",
                        "bucketName": sink.bucket_name,
                        "folderPath": sink.folder_path,
                        "fileName": sink.file_name,
                    }
                },
                "externalReferences": {"connection": sink.connection_id},
            },
        }
    if isinstance(sink, S3Sink):
        store_settings_type = "AmazonS3CompatibleWriteSettings"
        
        location = {
            "type": "AmazonS3Location",
            "bucketName": sink.bucket_name
        }
        
        if sink.file_config.folder_path:
            location["folderPath"] = sink.file_config.folder_path
        if sink.file_config.file_name:
            location["fileName"] = sink.file_config.file_name
        
        payload = {
            "type": sink.sink_type,
            "storeSettings": {
                "type": store_settings_type,
                "maxConcurrentConnections": sink.max_concurrent_connections,
                "copyBehavior": sink.copy_behavior
            },
            "datasetSettings": {
                "annotations": [],
                "type": sink.format_type,
                "typeProperties": {
                    "location": location
                },
                "schema": [],
                "externalReferences": {
                    "connection": sink.connection_id
                }
            }
        }
        
        # Add format settings based on format type
        if sink.format_type == "DelimitedText":
            payload["formatSettings"] = {
                "type": "DelimitedTextWriteSettings",
                "fileExtension": ".csv"
            }
        elif sink.format_type == "Json":
            payload["formatSettings"] = {
                "type": "JsonWriteSettings"
            }
        
        return payload
    if isinstance(sink, RestSink):
        payload = {
            "type": "RestSink",
            "httpRequestTimeout": sink.http_request_timeout,
            "requestInterval": sink.request_interval,
            "requestMethod": sink.request_method,
            "writeBatchSize": sink.write_batch_size,
            "httpCompressionType": sink.http_compression_type,
            "datasetSettings": {
                "annotations": [],
                "type": "RestResource",
                "typeProperties": {},
                "schema": [],
                "externalReferences": {
                    "connection": sink.connection_id
                }
            }
        }
        
        if sink.relative_url:
            payload["datasetSettings"]["typeProperties"]["relativeUrl"] = sink.relative_url
        if sink.additional_headers:
            payload["additionalHeaders"] = sink.additional_headers
            
        return payload
    if isinstance(sink, FileSystemSink):
        sink_type = f"{sink.file_format}Sink"
        store_settings = {
            "type": "FileServerWriteSettings",
            "copyBehavior": sink.copy_behavior
        }
        if sink.max_concurrent_connections:
            store_settings["maxConcurrentConnections"] = sink.max_concurrent_connections
        
        location = {
            "type": "FileServerLocation"
        }
        if sink.folder_path:
            location["folderPath"] = sink.folder_path
        if sink.file_name:
            location["fileName"] = sink.file_name
        
        payload = {
            "type": sink_type,
            "storeSettings": store_settings,
            "datasetSettings": {
                "annotations": [],
                "type": sink.file_format,
                "schema": [],
                "typeProperties": {
                    "location": location
                },
                "externalReferences": {
                    "connection": sink.connection_id
                }
            }
        }
        
        if sink.file_format == "DelimitedText":
            payload["formatSettings"] = {
                "type": "DelimitedTextWriteSettings",
                "fileExtension": sink.file_extension,
                "firstRowAsHeader": sink.first_row_as_header,
                "columnDelimiter": sink.column_delimiter,
                "quoteChar": sink.quote_char,
                "escapeChar": sink.escape_char
            }
        
        return payload
    if isinstance(sink, GoogleCloudStorageSink):
        # Determine sink type based on file format with correct capitalization
        if sink.file_format == "JSON":
            sink_type = "JsonSink"  # Use JsonSink, not JSONSink
        else:
            sink_type = f"{sink.file_format}Sink"
        
        # Build storeSettings
        store_settings = {
            "type": "GoogleCloudStorageWriteSettings",
            "maxConcurrentConnections": sink.max_concurrent_connections,
            "copyBehavior": sink.copy_behavior,
            "blockSizeInMB": sink.block_size_mb
        }
        
        # Add compression if specified
        if sink.compression_codec:
            store_settings["compressionCodec"] = sink.compression_codec
        
        # Build location
        location = {
            "type": "GoogleCloudStorageLocation",
            "bucketName": sink.bucket_name
        }
        
        if sink.folder_path:
            location["folderPath"] = sink.folder_path
        if sink.file_name:
            location["fileName"] = sink.file_name
        
        # Build the base payload
        payload = {
            "type": sink_type,
            "storeSettings": store_settings,
            "datasetSettings": {
                "annotations": [],
                "type": sink.file_format,
                "schema": [],
                "typeProperties": {
                    "location": location
                },
                "externalReferences": {
                    "connection": sink.connection_id
                }
            }
        }
        
        # Add format-specific settings
        if sink.file_format == "DelimitedText":
            payload["formatSettings"] = {
                "type": "DelimitedTextWriteSettings"
            }
        elif sink.file_format == "JSON":
            payload["formatSettings"] = {
                "type": "JsonWriteSettings",
                "filePattern": sink.json_file_pattern
            }
        elif sink.file_format == "Parquet":
            payload["formatSettings"] = {
                "type": "ParquetWriteSettings"
            }
        
        # Add metadata if specified
        if sink.metadata:
            payload["storeSettings"]["metadata"] = sink.metadata
        
        return payload
    if isinstance(sink, LakehouseSink):
        # Determine sink type based on root folder and format
        if sink.root_folder == "Tables":
            sink_type = "LakehouseTableSink"
            dataset_type = "LakehouseTable"
            store_settings_type = "LakehouseWriteSettings"
        else:
            # For files, use format from file_config
            format_type = sink.file_config.file_format if sink.file_config else "DelimitedText"
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
                    "name": sink.lakehouse_name,
                    "properties": {
                        "annotations": [],
                        "type": "Lakehouse",
                        "typeProperties": {
                            "workspaceId": sink.workspace_id,
                            "artifactId": sink.artifact_id,
                            "rootFolder": sink.root_folder
                        }
                    }
                },
                "type": dataset_type,
                "schema": []
            }
        }
        
        # Add storeSettings only for file sinks (not table sinks)
        if sink.root_folder == "Files":
            base_config["storeSettings"] = {
                "type": store_settings_type,
                "maxConcurrentConnections": sink.max_concurrent_connections,
                "copyBehavior": sink.copy_behavior,
                "blockSizeInMB": sink.block_size_mb
            }
        
        # Add type properties based on configuration
        if sink.root_folder == "Tables" and sink.table_config:
            base_config["datasetSettings"]["typeProperties"] = {
                "table": sink.table_config.table_name
            }
            if sink.table_config.schema_name:
                base_config["datasetSettings"]["typeProperties"]["schema"] = sink.table_config.schema_name
            
            # Add table-specific properties
            base_config["tableActionOption"] = sink.table_action_option
            if sink.partition_option != "None":
                base_config["partitionOption"] = sink.partition_option
                
        elif sink.root_folder == "Files" and sink.file_config:
            location_props = {}
            if sink.file_config.folder_path:
                location_props["folderPath"] = sink.file_config.folder_path
            if sink.file_config.file_name:
                location_props["fileName"] = sink.file_config.file_name
            
            base_config["datasetSettings"]["typeProperties"] = {
                "location": {
                    "type": "LakehouseLocation",
                    **location_props
                }
            }
            
            # Add format settings for file sinks
            if sink.file_config.file_format == "DelimitedText":
                base_config["formatSettings"] = {
                    "type": "DelimitedTextWriteSettings",
                    "fileExtension": ".csv"
                }
            elif sink.file_config.file_format == "JSON":
                base_config["formatSettings"] = {
                    "type": "JsonWriteSettings"
                }
        
        return base_config