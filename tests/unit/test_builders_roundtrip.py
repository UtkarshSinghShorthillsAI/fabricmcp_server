from src.fabricmcp_server.copy_activity_schemas import (
    LakehouseFileSink,
    build_sink_payload,
)


def test_lakehouse_file_sink_builder_minimal():
    sink = LakehouseFileSink(
        connector_type="LakehouseFile",
        workspace_id="ws",
        lakehouse_name="lh",
        lakehouse_id="lhid",
        folder_path="Files/out",
        file_name="out.csv",
    )
    payload = build_sink_payload(sink)
    assert payload["type"] == "DelimitedTextSink"
    assert payload["datasetSettings"]["linkedService"]["properties"]["type"] == "Lakehouse"


