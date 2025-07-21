# Iteration 2

import httpx
import logging
import uuid
from typing import Any, Dict, Optional, Union, Type, TypeVar, List
from azure.identity.aio import DefaultAzureCredential
from pydantic import BaseModel, ValidationError

from .fabric_models import FabricApiException, FabricAuthException, ItemEntity, CreateItemRequest

logger = logging.getLogger(__name__)
ResponseType = TypeVar("ResponseType", bound=BaseModel)

class FabricApiClient:
    def __init__(self, base_url: str, credential: DefaultAzureCredential):
        self._base_url = base_url.rstrip('/')
        self._credential = credential
        self._httpx_client = httpx.AsyncClient(
            headers={"User-Agent": "FabricMCP-Server/0.1.0"},
            timeout=120
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

    async def _get_auth_header(self) -> Dict[str, str]:
        try:
            token_object = await self._credential.get_token("https://api.fabric.microsoft.com/.default")
            return {"Authorization": f"Bearer {token_object.token}"}
        except Exception as e:
            raise FabricAuthException(f"Failed to refresh access token: {e}") from e

    async def _make_request(
        self, method: str, path: str, params: Optional[Dict] = None, json_body: Optional[Any] = None,
        response_model: Optional[Type[ResponseType]] = None, allow_404: bool = False
    ) -> Union[ResponseType, List[ResponseType], httpx.Response, None]:
        
        full_url = f"{self._base_url}{path}"
        json_payload = json_body.model_dump(by_alias=True, exclude_none=True) if isinstance(json_body, BaseModel) else json_body
        headers = self._httpx_client.headers.copy()
        headers.update(await self._get_auth_header())

        logger.debug(f"Request: {method} {full_url}")
        try:
            response = await self._httpx_client.request(method, full_url, params=params, json=json_payload, headers=headers)

            if response.status_code in [200, 201, 204]:
                if response.status_code == 204: return None
                response_json = response.json()
                data_to_validate = response_json.get("value", response_json)
                if response_model:
                    if isinstance(data_to_validate, list):
                        return [response_model.model_validate(item) for item in data_to_validate]
                    return response_model.model_validate(data_to_validate)
                return response_json
            
            if response.status_code == 202:
                logger.info("Received '202 Accepted', passing raw response to tool layer.")
                return response

            elif response.status_code == 404 and allow_404:
                return None
            else:
                response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise FabricApiException(e.response.status_code, "API request failed", e.response.text) from e
        except ValidationError as e:
            raise FabricApiException(0, f"Failed to validate API response: {e}. Raw: {response.text}") from e
        except httpx.RequestError as e:
            raise FabricApiException(0, f"HTTP request error: {e}") from e
        return None

    async def poll_lro_status(self, operation_url: str) -> httpx.Response:
        """Polls a given Operation-Location URL to get the status of an LRO."""
        headers = self._httpx_client.headers.copy()
        headers.update(await self._get_auth_header())
        response = await self._httpx_client.get(operation_url, headers=headers)
        response.raise_for_status()
        return response

    async def list_items(self, workspace_id: str, item_type: Optional[str] = None) -> Optional[List[ItemEntity]]:
        path = f"/v1/workspaces/{workspace_id}/items"
        params = {"type": item_type} if item_type else None
        return await self._make_request("GET", path, params=params, response_model=ItemEntity)

    async def get_item(self, workspace_id: str, item_id: str) -> Optional[ItemEntity]:
        path = f"/v1/workspaces/{workspace_id}/items/{item_id}"
        return await self._make_request("GET", path, response_model=ItemEntity, allow_404=True)
        
    async def create_item(self, workspace_id: str, payload: CreateItemRequest) -> Union[ItemEntity, httpx.Response, None]:
        path = f"/v1/workspaces/{workspace_id}/items"
        return await self._make_request("POST", path, json_body=payload, response_model=ItemEntity)

    async def delete_item(self, workspace_id: str, item_id: str) -> Optional[httpx.Response]:
        path = f"/v1/workspaces/{workspace_id}/items/{item_id}"
        return await self._make_request("DELETE", path)