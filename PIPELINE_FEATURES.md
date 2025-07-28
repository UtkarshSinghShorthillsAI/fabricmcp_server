# Microsoft Fabric Pipeline Management System

This document describes the comprehensive pipeline management capabilities implemented in the FabricMCP Server, providing full access to Microsoft Fabric's data pipeline canvas functionality.

## Overview

The enhanced pipeline system supports all major Microsoft Fabric data pipeline features, including:

- **Complete Activity Library**: All supported Fabric activity types
- **Advanced Pipeline Features**: Variables, parameters, dependencies, and conditional logic
- **Metadata-Driven Frameworks**: Scalable pipeline management using configuration tables
- **Specialized Pipeline Templates**: Pre-built patterns for common use cases
- **Error Handling & Monitoring**: Comprehensive error handling and notification systems

## Supported Activity Types

### Data Movement Activities
- **Copy Data**: Transfer data between sources and destinations
- **Lookup**: Retrieve configuration data from tables or datasets

### Control Flow Activities
- **ForEach**: Iterate over collections with parallel or sequential execution
- **If Condition**: Conditional branching based on expressions
- **Until**: Loop until a condition is met
- **Switch**: Multi-branch conditional execution
- **Wait**: Pause execution for a specified duration

### Execution Activities
- **Execute Pipeline**: Invoke child pipelines with parameters
- **Notebook (TridentNotebook)**: Execute Fabric notebooks with parameters
- **Script**: Run custom scripts (SQL, PowerShell, etc.)
- **Stored Procedure**: Execute database stored procedures

### Integration Activities
- **Web Activity**: Make HTTP requests to external APIs
- **WebHook**: Handle webhook callbacks
- **Teams Notification**: Send notifications to Microsoft Teams

### Variable Management
- **Set Variable**: Assign values to pipeline variables

## MCP Tools Available

### Core Pipeline Tools

#### `create_comprehensive_pipeline`
Creates a fully-featured pipeline with all advanced capabilities.

**Parameters:**
- `workspace_id`: Target workspace ID
- `pipeline_name`: Display name for the pipeline
- `pipeline_definition`: Complete pipeline definition with activities, parameters, variables
- `description`: Optional pipeline description

**Example Usage:**
```json
{
  "workspace_id": "your-workspace-id",
  "pipeline_name": "Comprehensive Data Pipeline",
  "pipeline_definition": {
    "name": "Comprehensive Data Pipeline",
    "parameters": {
      "Environment": {
        "type": "string",
        "defaultValue": "dev"
      }
    },
    "variables": {
      "ProcessingStatus": {
        "type": "string",
        "defaultValue": "Starting"
      }
    },
    "activities": [
      {
        "name": "CopyData1",
        "type": "Copy",
        "typeProperties": {
          "source": {"type": "SqlSource"},
          "sink": {"type": "LakehouseTableSink"}
        }
      }
    ]
  }
}
```

#### `create_copy_data_pipeline`
Creates an optimized pipeline for data copy operations.

**Parameters:**
- `workspace_id`: Target workspace ID
- `pipeline_name`: Display name for the pipeline
- `source_config`: Source configuration
- `sink_config`: Destination configuration
- `pipeline_parameters`: Optional pipeline parameters
- `pipeline_variables`: Optional pipeline variables

#### `create_metadata_driven_pipeline`
Creates a metadata-driven pipeline that processes data based on configuration tables.

**Parameters:**
- `workspace_id`: Target workspace ID
- `pipeline_name`: Display name for the pipeline
- `metadata_framework`: Configuration framework with sources, destinations, and processing rules

#### `create_orchestration_pipeline`
Creates a pipeline that orchestrates multiple child pipelines.

**Parameters:**
- `workspace_id`: Target workspace ID
- `pipeline_name`: Display name for the pipeline
- `child_pipelines`: List of child pipelines to orchestrate
- `execution_pattern`: "sequential", "parallel", or "conditional"
- `error_handling`: Error handling strategy
- `notification_config`: Teams notification configuration

#### `run_pipeline_with_parameters`
Executes a pipeline with custom parameters.

**Parameters:**
- `workspace_id`: Target workspace ID
- `pipeline_id`: Pipeline to execute
- `parameters`: Runtime parameters to pass
- `wait_for_completion`: Whether to wait for completion

#### `get_pipeline_definition`
Retrieves the complete definition of an existing pipeline.

### Pipeline Helper Tools

#### `create_activity`
Creates individual activity configurations for use in pipeline definitions.

**Parameters:**
- `activity_type`: Type of activity (Copy, TridentNotebook, ForEach, etc.)
- `activity_name`: Unique name for the activity
- `activity_config`: Activity-specific configuration
- `depends_on`: Activity dependencies
- `policy`: Execution policy (timeout, retry)

**Supported Activity Types:**
- Copy
- TridentNotebook
- SetVariable
- ForEach
- IfCondition
- Until
- Wait
- Lookup
- ExecutePipeline
- Script
- SqlServerStoredProcedure
- WebActivity
- WebHook
- TeamsNotification
- Switch

#### `build_medallion_architecture_pipeline`
Creates a medallion architecture pipeline (Bronze → Silver → Gold).

**Parameters:**
- `workspace_id`: Target workspace ID
- `pipeline_name`: Display name for the pipeline
- `bronze_sources`: List of bronze layer data sources
- `silver_transformations`: List of silver layer transformations
- `gold_aggregations`: List of gold layer aggregations
- `lakehouse_config`: Lakehouse configuration

#### `create_error_handling_pipeline_template`
Creates a pipeline template with comprehensive error handling.

**Parameters:**
- `pipeline_name`: Display name for the pipeline
- `main_activities`: Main activities to wrap with error handling
- `error_notification_config`: Error notification settings
- `retry_policy`: Default retry policy
- `deadletter_config`: Dead letter queue configuration

## Advanced Features

### Pipeline Parameters
Define runtime parameters that can be passed during execution:

```json
{
  "Environment": {
    "type": "string",
    "defaultValue": "dev",
    "description": "Target environment"
  },
  "ProcessingDate": {
    "type": "string", 
    "defaultValue": "@utcnow()",
    "description": "Processing date"
  }
}
```

### Pipeline Variables
Define variables for storing intermediate values during execution:

```json
{
  "ErrorLog": {
    "type": "array",
    "defaultValue": [],
    "description": "Collection of error messages"
  },
  "ProcessingStatus": {
    "type": "string",
    "defaultValue": "Starting",
    "description": "Current processing status"
  }
}
```

### Activity Dependencies
Control execution flow with dependencies:

```json
{
  "dependsOn": [
    {
      "activity": "CopyData1",
      "dependencyConditions": ["Succeeded"]
    }
  ]
}
```

**Dependency Conditions:**
- `Succeeded`: Activity completed successfully
- `Failed`: Activity failed
- `Completed`: Activity finished (success or failure)
- `Skipped`: Activity was skipped

### Activity Policies
Configure execution behavior:

```json
{
  "policy": {
    "timeout": "1.00:00:00",
    "retry": 3,
    "retryIntervalInSeconds": 60,
    "secureInput": false,
    "secureOutput": false
  }
}
```

## Metadata-Driven Framework

The metadata-driven framework enables scalable pipeline management using configuration tables:

### Configuration Table Structure
```json
{
  "configurations": [
    {
      "source_table": "source_table1",
      "destination_table": "dest_table1", 
      "load_type": "full",
      "batch_number": 1,
      "active": true,
      "watermark_column": "modified_date",
      "last_watermark": "2023-01-01"
    }
  ],
  "global_parameters": {
    "MaxParallelism": {
      "type": "integer",
      "value": 5,
      "description": "Maximum parallel activities"
    }
  },
  "environment": "dev"
}
```

### Benefits
- **Scalability**: Add new tables without changing pipeline code
- **Maintainability**: Centralized configuration management
- **Flexibility**: Support for full and incremental loads
- **Monitoring**: Built-in tracking and audit capabilities

## Expression Language Support

The pipeline system supports Microsoft Fabric's expression language:

### Common Expressions
- `@pipeline().parameters.ParameterName`: Access pipeline parameters
- `@variables('VariableName')`: Access pipeline variables
- `@activity('ActivityName').output`: Access activity outputs
- `@utcnow()`: Current UTC timestamp
- `@concat(string1, string2)`: String concatenation
- `@if(condition, trueValue, falseValue)`: Conditional logic

### Dynamic Content
Most activity properties support dynamic content using expressions:

```json
{
  "tableName": {
    "value": "@concat('table_', pipeline().parameters.Environment)",
    "type": "Expression"
  }
}
```

## Error Handling Patterns

### Retry Policies
Configure automatic retry for transient failures:

```json
{
  "policy": {
    "retry": 3,
    "retryIntervalInSeconds": 60
  }
}
```

### Error Notifications
Send notifications on pipeline failures:

```json
{
  "name": "SendErrorNotification",
  "type": "TeamsNotification",
  "typeProperties": {
    "webhookUrl": "https://your-teams-webhook",
    "message": {
      "value": "@concat('Pipeline failed: ', pipeline().Pipeline)",
      "type": "Expression"
    }
  },
  "dependsOn": [
    {
      "activity": "MainActivity",
      "dependencyConditions": ["Failed"]
    }
  ]
}
```

### Dead Letter Queues
Handle failed items systematically:

```json
{
  "deadletter_config": {
    "sink_config": {
      "type": "LakehouseTableSink",
      "tableName": "failed_items",
      "tableOption": "append"
    }
  }
}
```

## Best Practices

### Performance Optimization
1. **Parallel Execution**: Use ForEach with `isSequential: false` for parallel processing
2. **Batch Control**: Set appropriate `batchCount` to control resource usage
3. **Dependency Management**: Minimize unnecessary dependencies to allow parallel execution

### Error Handling
1. **Comprehensive Logging**: Use variables to track errors and processing status
2. **Graceful Degradation**: Use conditional activities to handle partial failures
3. **Notification Strategy**: Implement appropriate notifications for different error types

### Metadata Management
1. **Configuration Tables**: Use metadata tables for scalable pipeline management
2. **Environment Management**: Use parameters for environment-specific configurations
3. **Audit Trails**: Implement logging for all pipeline executions

### Security
1. **Secure Parameters**: Use secure parameters for sensitive information
2. **Activity Policies**: Set `secureInput` and `secureOutput` for sensitive activities
3. **Access Control**: Implement proper workspace and pipeline permissions

## Integration Examples

### Azure Data Factory Migration
The pipeline system provides compatibility with Azure Data Factory patterns:

```json
{
  "name": "ADF_Compatible_Pipeline",
  "activities": [
    {
      "name": "CopyFromSQL",
      "type": "Copy",
      "typeProperties": {
        "source": {
          "type": "SqlSource",
          "sqlReaderQuery": "SELECT * FROM source_table"
        },
        "sink": {
          "type": "LakehouseTableSink",
          "tableName": "destination_table"
        }
      }
    }
  ]
}
```

### Databricks Integration
Execute Databricks notebooks within pipelines:

```json
{
  "name": "RunDatabricksNotebook",
  "type": "TridentNotebook", 
  "typeProperties": {
    "notebookId": "notebook-id",
    "workspaceId": "workspace-id",
    "parameters": {
      "input_path": "/path/to/input",
      "output_path": "/path/to/output"
    }
  }
}
```

## Monitoring and Observability

### Pipeline Execution Tracking
- Job status monitoring with `get_operation_status`
- Real-time execution updates
- Comprehensive error reporting

### Activity-Level Monitoring
- Individual activity success/failure tracking
- Execution duration and performance metrics
- Resource usage optimization

### Custom Metrics
- Variable-based status tracking
- Custom notification systems
- Integration with external monitoring tools

This comprehensive pipeline management system provides full access to Microsoft Fabric's data pipeline capabilities while maintaining ease of use through the MCP interface. 