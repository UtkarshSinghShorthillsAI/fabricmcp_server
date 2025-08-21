import os
import json
import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Generator

import pytest
import pytest_asyncio

try:
    import dotenv  # type: ignore
    dotenv.load_dotenv()
except Exception:
    pass

from src.fabricmcp_server.fabric_api_client import FabricApiClient, FabricApiException


def _default_workspace_id() -> str:
    return os.getenv("FABRIC_WORKSPACE_ID", "4be6c4a0-4816-478d-bdc1-7bda19c32bc6")


def _default_pipeline_id() -> str:
    return os.getenv("FABRIC_PIPELINE_ID", "31ea5ed4-3ed5-4b2d-b836-52a2ba3ea6c8")


def build_pipeline_definition_from_activity(pipeline_name: str, activity: Dict[str, Any]) -> Dict[str, Any]:
    """Create a Fabric pipeline definition payload (InlineBase64) from a single activity."""
    pipeline_struct = {"properties": {"activities": [activity]}}
    json_string = json.dumps(pipeline_struct, separators=(",", ":"))
    b64_payload = base64.b64encode(json_string.encode("utf-8")).decode("utf-8")
    return {
        "parts": [
            {"path": "pipeline-content.json", "payload": b64_payload, "payloadType": "InlineBase64"}
        ],
    }


def _extract_definition(def_response: Dict[str, Any]) -> Dict[str, Any]:
    """getDefinition may wrap the actual definition in a 'definition' key. Normalize it."""
    if isinstance(def_response, dict) and "definition" in def_response:
        return def_response["definition"]
    return def_response


@pytest_asyncio.fixture(scope="function")
async def fabric_client() -> Generator[FabricApiClient, None, None]:
    """Create a fresh Fabric API client for each test function.
    
    Using function scope to avoid event loop issues when running multiple async tests.
    Each test gets its own client instance that's properly closed after the test.
    """
    base_url = os.getenv("FABRIC_API_BASE_URL", "https://api.fabric.microsoft.com")
    client = await FabricApiClient.create(base_url)
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="session")
def workspace_id() -> str:
    return _default_workspace_id()


@pytest.fixture(scope="session")
def pipeline_id() -> str:
    return _default_pipeline_id()


# Removed backup/restore - focus on API success + UI persistence only


def capture_400_failure(activity_type: str, payload: Dict[str, Any], error: FabricApiException) -> Path:
    """Capture a 400 failure to the artifacts directory for analysis."""
    artifacts_dir = Path("tests/fabric_api/artifacts/failures")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{activity_type}_400.json"
    filepath = artifacts_dir / filename
    
    artifact = {
        "timestamp": datetime.now().isoformat(),
        "activity_type": activity_type,
        "status_code": error.status_code,
        "error_message": error.message,
        "error_response": error.response_text,
        "payload": payload,
    }
    
    with open(filepath, "w") as f:
        json.dump(artifact, f, indent=2)
    
    return filepath


async def verify_activity_persisted(fabric_client: FabricApiClient, workspace_id: str, pipeline_id: str, 
                                   expected_activity_name: str, expected_activity_type: str) -> bool:
    """
    CRITICAL: Verify that an activity actually persisted to the pipeline canvas.
    API 200/202 != actual persistence!
    """
    try:
        definition = await fabric_client.get_item_definition(workspace_id, pipeline_id)
        
        if "definition" not in definition:
            return False
            
        pipeline_def = definition["definition"]
        
        # Handle Base64 encoded parts format (Fabric's new format)
        if "parts" in pipeline_def:
            for part in pipeline_def["parts"]:
                if part.get("path") == "pipeline-content.json":
                    encoded_payload = part.get("payload", "")
                    if encoded_payload:
                        try:
                            decoded_bytes = base64.b64decode(encoded_payload)
                            decoded_str = decoded_bytes.decode('utf-8')
                            pipeline_content = json.loads(decoded_str)
                            pipeline_def = pipeline_content
                            break
                        except Exception:
                            return False
        
        if "properties" in pipeline_def and "activities" in pipeline_def["properties"]:
            activities = pipeline_def["properties"]["activities"]
            
            for activity in activities:
                if (activity.get("name") == expected_activity_name and 
                    activity.get("type") == expected_activity_type):
                    return True
                    
        return False
        
    except Exception:
        return False


