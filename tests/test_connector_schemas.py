from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURES = ROOT / "tests" / "fixtures" / "connector_contracts"
SCHEMAS = ROOT / "schemas" / "connector_contracts"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.connector_schemas import build_connector_schema_documents, build_connector_schema_models


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _without_none(value):
    if isinstance(value, dict):
        return {
            key: _without_none(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_without_none(item) for item in value]
    return value


class AuditFlowConnectorSchemaTests(unittest.TestCase):
    def test_stored_connector_schema_files_match_current_model_generation(self) -> None:
        expected_documents = build_connector_schema_documents()

        for filename, expected_document in expected_documents.items():
            actual_document = _load_json(SCHEMAS / filename)
            self.assertEqual(actual_document, expected_document, filename)

    def test_canonical_connector_fixtures_validate_against_schema_models(self) -> None:
        schema_models = build_connector_schema_models()
        fixture_map = {
            "jira_issue_request.schema.json": "jira_issue_request.json",
            "jira_issue_response.schema.json": "jira_issue_response.json",
            "confluence_page_request.schema.json": "confluence_page_request.json",
            "confluence_page_response.schema.json": "confluence_page_response.json",
        }

        for schema_filename, fixture_filename in fixture_map.items():
            model = schema_models[schema_filename]
            fixture_payload = _load_json(FIXTURES / fixture_filename)
            payload = {
                field_name: fixture_payload[field_name]
                for field_name in model.model_fields
                if field_name in fixture_payload
            }
            validated = model.model_validate(payload)
            self.assertEqual(
                validated.model_dump(mode="json", exclude_none=True),
                _without_none(payload),
                schema_filename,
            )


if __name__ == "__main__":
    unittest.main()
