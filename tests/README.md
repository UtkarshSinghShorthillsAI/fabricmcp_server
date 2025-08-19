# Microsoft Fabric MCP Server - Testing Suite

This directory contains a comprehensive testing framework designed to validate the Microsoft Fabric MCP Server against the live Fabric API and ensure robust, reliable pipeline activity models.

## 🎯 Testing Philosophy

**API-First Validation**: Every activity pattern must be tested against the real Microsoft Fabric API before being implemented in Pydantic models. This prevents overfitting and ensures compatibility.

## 📁 Directory Structure

```
tests/
├── fabric_api/           # Live API integration tests
│   ├── conftest.py       # Fabric API client fixtures
│   ├── test_corpus_activities.py  # Corpus-driven API tests
│   └── corpus/           # Ground truth JSON patterns
│       └── activities/   # Activity-specific patterns
│           ├── Copy/     # Copy activity patterns
│           ├── Script/   # Script activity patterns
│           └── ...       # Other activity types
├── unit/                 # Unit tests for internal logic
├── pytest.ini           # Pytest configuration
└── README.md            # This file
```

## 🔄 Testing Flow

### 1. **API Test First** 
```bash
# Test a specific pattern against live Fabric API
pytest tests/fabric_api/test_corpus_activities.py -k "mysql_to_lakehouse" -v
```

### 2. **Model Implementation**
- Update Pydantic models based on verified working patterns
- Use flexible schemas to avoid overfitting

### 3. **Server Restart**
- Restart MCP server with updated models
- Ensure new patterns are available to MCP clients

### 4. **End-to-End Test**
- Test via MCP client tools (e.g., `update_pipeline`)
- Verify complete round-trip functionality

### 5. **Persistence Verification**
- Confirm activities appear on Fabric UI canvas
- Validate API success + UI visibility

## 🧪 Test Types

### Fabric API Tests (`fabric_api/`)

**Purpose**: Validate patterns against live Microsoft Fabric API

**Key Features**:
- Tests against real workspace: `4be6c4a0-4816-478d-bdc1-7bda19c32bc6`
- Uses test pipeline: `31ea5ed4-3ed5-4b2d-b836-52a2ba3ea6c8`
- No backup/restore - activities are ephemeral
- Round-trip persistence verification

**Run Commands**:
```bash
# Run all API tests
pytest tests/fabric_api/ -v -m fabric_api

# Test specific activity type
pytest tests/fabric_api/ -k "Script" -v

# Skip live API tests (for development)
pytest tests/fabric_api/ -m "not fabric_api"
```

### Unit Tests (`unit/`)

**Purpose**: Test internal logic without external dependencies

**Coverage**:
- Pydantic model validation
- Payload building functions
- Schema transformations

## 📊 Corpus-Driven Testing

### Ground Truth JSON Patterns

The `corpus/activities/` directory contains real, working JSON patterns extracted from the Fabric UI:

```
corpus/activities/
├── Copy/
│   ├── amazons3_source_real.json     # ✅ Real S3 pattern
│   ├── mysql_to_lakehouse.json       # ✅ Verified working
│   └── azurepostgresql_to_lakehouse.json  # ✅ Verified working
├── Script/
│   ├── sqlserver_minimal.json        # ✅ Verified working
│   ├── oracle_with_parameters.json   # ✅ Verified working
│   └── azuresqldatabase_minimal.json # ✅ Verified working
└── ...
```

### Corpus File Format

Each corpus file follows this structure:

```json
{
  "id": "Copy/mysql_to_lakehouse_systematic",
  "activity": {
    "name": "Test_Activity",
    "type": "Copy",
    "typeProperties": { /* Real API structure */ }
  },
  "requirements": {
    "connections": ["MySql"],
    "linkedServices": ["Lakehouse"]
  },
  "expected": {
    "updateDefinition": [200, 202],
    "runnable": false
  },
  "notes": "Description of test purpose and any special requirements"
}
```

## 🎯 Systematic Connection Testing

### Coverage Matrix

We systematically test all connection types in both source and sink roles:

| Connection Type | Source Tested | Sink Tested | Status |
|----------------|---------------|-------------|---------|
| MySql | ✅ | ➖ | Working |
| AzurePostgreSql | ✅ | ➖ | Working |
| GoogleCloudStorage | ✅ | 🔄 | Working |
| SqlServer | ✅ | ➖ | Working |
| Oracle | ✅ | ➖ | Working |
| Snowflake | ❌ | ➖ | Pattern Issue |
| ... | ... | ... | ... |

**Legend**: ✅ Tested & Working | 🔄 Needs Testing | ❌ Pattern Issue | ➖ Not Applicable

### Priority Groups

1. **High Priority**: Database + Storage connections (MySql, PostgreSql, GoogleCloudStorage, etc.)
2. **Medium Priority**: Analytics platforms (Snowflake, BigQuery, etc.)
3. **Low Priority**: Specialized connectors (Salesforce, Office365, etc.)

## 🚀 Running Tests

### Prerequisites

```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Configure FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET, etc.
```

### Test Commands

```bash
# Run all tests
pytest tests/ -v

# Run only unit tests (fast, no API calls)
pytest tests/unit/ -v

# Run API tests (requires credentials)
pytest tests/fabric_api/ -v -m fabric_api

# Test specific connection type
pytest tests/fabric_api/ -k "mysql" -v

# Generate coverage report
pytest tests/ --cov=src/fabricmcp_server --cov-report=html
```

### Development Workflow

1. **Add New Connection Pattern**:
   ```bash
   # 1. Create corpus entry
   tests/fabric_api/corpus/activities/Copy/newconnection_to_lakehouse.json
   
   # 2. Test against API
   pytest tests/fabric_api/test_corpus_activities.py -k "newconnection" -v
   
   # 3. Update Pydantic models if successful
   # 4. Restart server and test via MCP client
   ```

2. **Fix Failing Pattern**:
   ```bash
   # 1. Identify failing test
   pytest tests/fabric_api/ -k "snowflake" -v --tb=short
   
   # 2. Update corpus JSON with corrected pattern
   # 3. Re-test until API returns 200/202
   # 4. Update models accordingly
   ```

## 📈 Success Metrics

### Current Status
- **Connection Types Tested**: 8/41 (High Priority)
- **Success Rate**: 75% (excluding network issues)
- **Verified Working Patterns**: 6
- **API-First Validation**: 100% coverage for tested patterns

### Goals
- **100% Connection Coverage**: All 41 connection types tested
- **Source/Sink Matrix**: Every applicable connection tested in both roles
- **Zero Overfitting**: All models based on real API patterns
- **Complete MCP Validation**: All activities testable via MCP client

## 🔧 Troubleshooting

### Common Issues

1. **400 Bad Request**: Pattern doesn't match Fabric API expectations
   - Solution: Update corpus JSON with correct pattern structure

2. **RuntimeError: Event loop is closed**: Known pytest-asyncio cleanup issue
   - Solution: Ignore if API returned 200/202 (test passed)

3. **Connection not found**: Test requires real Fabric connections
   - Solution: Either mock or skip tests requiring specific connections

### Debug Mode

```bash
# Enable verbose logging
pytest tests/fabric_api/ -v -s --log-cli-level=DEBUG

# Single test with full traceback
pytest tests/fabric_api/test_corpus_activities.py::test_specific -vvv --tb=long
```

## 🎯 Contributing

1. **Always test API patterns first** before implementing Pydantic models
2. **Use real UI-extracted JSONs** as ground truth in corpus
3. **Follow the systematic testing workflow** (API → Model → Server → MCP)
4. **Document new connection types** in coverage matrix
5. **Ensure round-trip validation** (API success + UI persistence)

---

*This testing framework ensures that the Microsoft Fabric MCP Server is robust, reliable, and capable of handling any Data Pipeline configuration that an LLM might generate.*
