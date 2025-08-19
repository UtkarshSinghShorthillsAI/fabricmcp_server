import json
import os
import time
import pytest

from .conftest import build_pipeline_definition_from_activity


@pytest.mark.fabric_api
@pytest.mark.asyncio
async def test_apply_webhook_minimal(
    fabric_client,
    workspace_id,
    pipeline_id,
    backup_pipeline_definition,
):
    """Apply a minimal WebHook activity and assert updateDefinition is accepted.

    This is a non-destructive acceptance test that restores the original pipeline
    definition after completion (unless FABRIC_API_KEEP=1).
    """
    activity = {
        "name": "WebHook_Minimal",
        "type": "WebHook",
        "typeProperties": {
            "method": "POST",
            "timeout": "0.00:10:00"
        },
    }

    # Build definition payload
    definition = build_pipeline_definition_from_activity("MockPipeline", activity)

    # Exercise updateDefinition
    resp = await fabric_client.update_pipeline_definition(
        workspace_id=workspace_id,
        pipeline_id=pipeline_id,
        definition=definition,
        update_metadata=False,
    )

    assert resp is not None
    assert 200 <= resp.status_code < 300 or resp.status_code == 202


