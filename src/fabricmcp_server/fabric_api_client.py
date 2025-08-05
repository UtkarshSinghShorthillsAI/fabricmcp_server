import httpx
import logging
import os
import json
from typing import Any, Dict, Optional, Union, Type, TypeVar, List
from azure.identity.aio import DefaultAzureCredential
from pydantic import BaseModel, ValidationError

from .fabric_models import (
    FabricApiException, FabricAuthException, ItemEntity, 
    CreateItemRequest, UpdateItemDefinitionRequest, LoadTableRequest
)

logger = logging.getLogger(__name__)
ResponseType = TypeVar("ResponseType", bound=BaseModel)

class FabricApiClient:
    def __init__(self, base_url: str, credential: DefaultAzureCredential):
        self._base_url = base_url.rstrip('/')
        self._onelake_url = "https://onelake.dfs.fabric.microsoft.com"
        self._credential = credential
        self._httpx_client = httpx.AsyncClient(
            headers={"User-Agent": "FabricMCP-Server/0.1.0"},
            timeout=300.0 # Increased timeout for large file operations
        )

    @classmethod
    async def create(cls, base_url: str) -> "FabricApiClient":
        logger.info("Initializing FabricApiClient with DefaultAzureCredential.")
        try:
            credential = DefaultAzureCredential()
            return cls(base_url, credential)
        except Exception as e:
            raise FabricAuthException(f"Failed to set up Azure credentials: {e}") from e

    async def close(self):
        await self._credential.close()
        if self._httpx_client and not self._httpx_client.is_closed:
            await self._httpx_client.aclose()
        logger.debug("Fabric API client and credentials closed.")

    async def _get_auth_header(self, scope: str) -> Dict[str, str]:
        """Gets an auth token for the specified API scope."""
        try:
            token_object = await self._credential.get_token(scope)
            return {"Authorization": f"Bearer {token_object.token}"}
        except Exception as e:
            raise FabricAuthException(f"Failed to refresh access token for scope {scope}: {e}") from e

    async def _make_request(
        self, method: str, url: str, params: Optional[Dict] = None, json_body: Optional[Any] = None,
        response_model: Optional[Type[ResponseType]] = None, headers: Optional[Dict] = None,
        allow_404: bool = False, content: Optional[bytes] = None
    ) -> Union[ResponseType, List[ResponseType], httpx.Response, Dict[str, Any], None]:
        
        json_payload = json_body.model_dump(by_alias=True, exclude_none=True) if isinstance(json_body, BaseModel) else json_body
        
        # Logging Block for debugging
        logger.info(f"--- START API REQUEST ---")
        logger.info(f"URL: {method} {url}")
        if json_payload: logger.info(f"BODY:\n{json.dumps(json_payload, indent=2)}")
        if content: logger.info(f"CONTENT: {len(content)} bytes")
        logger.info(f"--- END API REQUEST ---")
        
        try:
            response = await self._httpx_client.request(
                method, url, params=params, json=json_payload, headers=headers, content=content
            )
            if response.status_code == 202: return response
            if 200 <= response.status_code < 300:
                if response.status_code == 204 or not response.content: return response
                response_json = response.json()
                data_to_validate = response_json.get("value", response_json)
                if response_model:
                    if isinstance(data_to_validate, list):
                        return [response_model.model_validate(item) for item in data_to_validate]
                    return response_model.model_validate(data_to_validate)
                return response_json
            elif response.status_code == 404 and allow_404: return None
            else: response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise FabricApiException(e.response.status_code, "API request failed", e.response.text) from e
        except (ValidationError, json.JSONDecodeError) as e:
            raise FabricApiException(0, f"Failed to validate or decode API response: {e}. Raw: {response.text if 'response' in locals() else 'N/A'}")
        except httpx.RequestError as e:
            raise FabricApiException(0, f"HTTP request error: {e}")
        return None
    # Methods below are now correct because _make_request is fixed
    async def list_items(self, workspace_id: str, item_type: Optional[str] = None) -> Optional[List[ItemEntity]]:
        path = f"/v1/workspaces/{workspace_id}/items"
        params = {"type": item_type} if item_type else None
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("GET", f"{self._base_url}{path}", params=params, headers=headers, response_model=ItemEntity)

    async def get_item(self, workspace_id: str, item_id: str) -> Optional[ItemEntity]:
        path = f"/v1/workspaces/{workspace_id}/items/{item_id}"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("GET", f"{self._base_url}{path}", headers=headers, response_model=ItemEntity, allow_404=True)

    async def create_item(self, workspace_id: str, payload: CreateItemRequest) -> Union[ItemEntity, httpx.Response, None]:
        path = f"/v1/workspaces/{workspace_id}/items"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("POST", f"{self._base_url}{path}", json_body=payload, headers=headers, response_model=ItemEntity)

    async def delete_item(self, workspace_id: str, item_id: str) -> Optional[httpx.Response]:
        path = f"/v1/workspaces/{workspace_id}/items/{item_id}"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("DELETE", f"{self._base_url}{path}", headers=headers)

    async def get_item_definition(self, workspace_id: str, item_id: str) -> Optional[Dict]:
        path = f"/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("POST", f"{self._base_url}{path}", headers=headers)

    async def update_item_definition(self, workspace_id: str, item_id: str, payload: UpdateItemDefinitionRequest) -> Optional[httpx.Response]:
        path = f"/v1/workspaces/{workspace_id}/items/{item_id}/updateDefinition"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("POST", f"{self._base_url}{path}", json_body=payload, headers=headers)

    async def run_item(self, workspace_id: str, item_id: str, job_type: str) -> Optional[httpx.Response]:
        path = f"/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("POST", f"{self._base_url}{path}", headers=headers)

    async def poll_lro_status(self, operation_url: str) -> httpx.Response:
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        response = await self._httpx_client.get(operation_url, headers=headers)
        response.raise_for_status()
        return response
    async def get_job_instance_status(self, job_instance_url: str) -> Dict[str, Any]:
        """Gets the status of a specific job instance (e.g., a pipeline or notebook run)."""
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        response = await self._httpx_client.get(job_instance_url, headers=headers)
        response.raise_for_status()
        return response.json()
    
    async def update_pipeline_definition(
        self,
        workspace_id: str,
        pipeline_id: str,
        definition: Dict[str, Any],
        update_metadata: bool = False
    ) -> httpx.Response:
        """
        Calls the Fabric REST API to update a pipeline definition using updateDefinition.
        """
        url = f"{self._base_url}/v1/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/updateDefinition"
        params = {"updateMetadata": "true"} if update_metadata else None
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._httpx_client.post(url, json={"definition": definition}, params=params, headers=headers)

    
    # --- NEW: OneLake DFS API Methods ---
    async def upload_file_chunked(self, workspace_id: str, lakehouse_id: str, local_file_path: str, target_path: str) -> bool:
        """Uploads a local file to OneLake, creating the file and then appending in chunks."""
        file_url = f"{self._onelake_url}/{workspace_id}/{lakehouse_id}/{target_path}"
        headers = await self._get_auth_header("https://storage.azure.com/.default")

        # 1. Create the file resource (path)
        create_resp = await self._httpx_client.put(f"{file_url}?resource=file", headers=headers)
        if create_resp.status_code not in [201, 409]: # 409 Conflict is ok if it already exists
            raise FabricApiException(create_resp.status_code, "Failed to create file resource in OneLake", create_resp.text)
        
        # 2. Append data in chunks
        file_size = os.path.getsize(local_file_path)
        position = 0
        chunk_size = 4 * 1024 * 1024 # 4 MB chunks

        with open(local_file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk: break
                
                append_headers = headers.copy()
                append_headers['Content-Type'] = 'application/octet-stream'
                append_resp = await self._make_request(
                    "PATCH", f"{file_url}?action=append&position={position}", headers=append_headers, content=chunk
                )
                if append_resp.status_code != 202:
                    raise FabricApiException(append_resp.status_code, f"Failed to append chunk at position {position}", append_resp.text)
                position += len(chunk)

        # 3. Flush the file to finalize
        flush_headers = headers.copy()
        flush_headers['x-ms-content-length'] = str(file_size)
        flush_resp = await self._make_request(
            "PATCH", f"{file_url}?action=flush&position={file_size}", headers=flush_headers
        )
        if flush_resp.status_code != 200:
            raise FabricApiException(flush_resp.status_code, "Failed to flush file in OneLake", flush_resp.text)
            
        return True

    async def list_files(self, workspace_id: str, lakehouse_id: str, folder_path: str) -> List[Dict[str, Any]]:
        url = f"{self._onelake_url}/{workspace_id}/{lakehouse_id}/{folder_path}?resource=directory"
        headers = await self._get_auth_header("https://storage.azure.com/.default")
        response = await self._make_request("GET", url, headers=headers)
        return response.get("paths", []) if response else []

    # --- NEW: Lakehouse-Specific API Methods ---
    async def load_table(self, workspace_id: str, lakehouse_id: str, table_name: str, payload: LoadTableRequest) -> Optional[httpx.Response]:
        """Initiates a 'Load to Table' operation in a specific Lakehouse."""
        path = f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables/{table_name}/load"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("POST", f"{self._base_url}{path}", headers=headers, json_body=payload)

    # --- NEW: Generic API Methods for Connections ---
    async def get_connections(self) -> Optional[Dict[str, Any]]:
        """Gets all connections in the tenant using the Fabric connections API."""
        path = "/v1/connections"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("GET", f"{self._base_url}{path}", headers=headers)

    async def get_workspace_connections(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        """Gets connections for a specific workspace (if such endpoint exists)."""
        path = f"/v1/workspaces/{workspace_id}/connections"
        headers = await self._get_auth_header("https://api.fabric.microsoft.com/.default")
        return await self._make_request("GET", f"{self._base_url}{path}", headers=headers, allow_404=True)