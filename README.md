# FabricMCP Server

MCP Server for interacting with Microsoft Fabric via a natural language interface.

## Overview

This server exposes Microsoft Fabric functionalities as "tools" that can be understood and invoked by a Large Language Model (LLM) through an MCP Client. It allows for the programmatic creation, retrieval, and management of Fabric items like Lakehouses, Notebooks, and Data Pipelines.

## Setup

1.  Clone the repository.
2.  Ensure Python 3.10+ and `uv` are installed.
3.  Create and activate a virtual environment:
    ```bash
    uv venv
    source .venv/bin/activate
    ```
4.  Install dependencies:
    ```bash
    uv pip install -e .[dev]
    ```
5.  Create a `.env` file from the `.env.example` template and fill in your Fabric App Registration (Service Principal) details.

## Running the Server

```bash
python3 -m src.fabricmcp_server.app