import json
import os
from pathlib import Path
import pytest

from src.fabricmcp_server.fabric_api_client import FabricApiException
from tests.fabric_api.conftest import build_pipeline_definition_from_activity, capture_400_failure


def load_corpus_entries():
    """Load all activity corpus entries from the corpus directory."""
    corpus_dir = Path(__file__).parent / "corpus" / "activities"
    entries = []
    
    for activity_dir in corpus_dir.iterdir():
        if activity_dir.is_dir():
            for json_file in activity_dir.glob("*.json"):
                with open(json_file) as f:
                    entry = json.load(f)
                    entry["_filepath"] = str(json_file)
                    entries.append(entry)
    
    return entries


# Parametrize tests with all corpus entries
corpus_entries = load_corpus_entries()
test_ids = [entry["id"] for entry in corpus_entries]


@pytest.mark.fabric_api
@pytest.mark.asyncio
@pytest.mark.parametrize("corpus_entry", corpus_entries, ids=test_ids)
async def test_corpus_activity_updateDefinition(
    corpus_entry,
    fabric_client,
    workspace_id,
    pipeline_id,
):
    """Test that each corpus activity can be applied via updateDefinition."""
    activity = corpus_entry["activity"]
    expected_statuses = corpus_entry["expected"]["updateDefinition"]
    
    # Build definition payload
    definition = build_pipeline_definition_from_activity("MockPipeline", activity)
    
    # Exercise updateDefinition
    try:
        resp = await fabric_client.update_pipeline_definition(
            workspace_id=workspace_id,
            pipeline_id=pipeline_id,
            definition=definition,
            update_metadata=False,
        )
        
        assert resp is not None
        if resp.status_code not in expected_statuses:
            print(f"\n❌ {corpus_entry['id']}: Got {resp.status_code}")
            print(f"Response body: {resp.text}")
            pytest.fail(f"Expected {expected_statuses}, got {resp.status_code}")
        
        print(f"✓ {corpus_entry['id']}: updateDefinition returned {resp.status_code}")
        
    except FabricApiException as e:
        if e.status_code == 400:
            # Capture failure for analysis
            filepath = capture_400_failure(
                activity_type=activity["type"],
                payload=definition,
                error=e
            )
            pytest.fail(
                f"400 Bad Request for {corpus_entry['id']}. "
                f"Captured to {filepath}. Error: {e.message}"
            )
        else:
            raise
