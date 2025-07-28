"""
FabricMCP Server Tools Package

This package contains MCP (Model Context Protocol) tools for interacting with Microsoft Fabric.
It provides comprehensive functionality for managing Fabric items, notebooks, and data pipelines.

Modules:
- items: General Fabric item management (create, read, update, delete)
- notebooks: Notebook creation, execution, and content management  
- pipelines: Comprehensive data pipeline management with full activity support
- pipeline_helpers: Utility functions for pipeline building and metadata management
"""

__all__ = [
    "items",
    "notebooks", 
    "pipelines",
    "pipeline_helpers"
]
