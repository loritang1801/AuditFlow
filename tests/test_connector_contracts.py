from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURES = ROOT / "tests" / "fixtures" / "connector_contracts"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.connectors import EnvConfiguredConnectorResolver


class _FakeResponse:
    def __init__(self, *, status_code: int, headers: dict[str, str], json_payload=None, text: str = "", url: str = "") -> None:
        self.status_code = status_code
        self.headers = headers
        self._json_payload = json_payload
        self.text = text
        self.url = url

    def json(self):
        return self._json_payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeHttpClient:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, *, headers: dict[str, str], follow_redirects: bool, timeout: float):
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "follow_redirects": follow_redirects,
                "timeout": timeout,
            }
        )
        return self.response


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class AuditFlowConnectorContractTests(unittest.TestCase):
    def test_jira_issue_contract_fixture_round_trip(self) -> None:
        request_fixture = _load_fixture("jira_issue_request.json")
        response_fixture = _load_fixture("jira_issue_response.json")
        fake_client = _FakeHttpClient(
            _FakeResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                json_payload=response_fixture,
                url=str(request_fixture["expected_url"]),
            )
        )
        resolver = EnvConfiguredConnectorResolver(http_client=fake_client)

        with patch.dict(
            os.environ,
            {
                "AUDITFLOW_JIRA_FETCH_MODE": "http",
                "AUDITFLOW_JIRA_URL_TEMPLATE": str(request_fixture["url_template"]),
            },
            clear=False,
        ):
            result = resolver.fetch(
                "jira",
                selector=str(request_fixture["selector"]),
                query=request_fixture["query"],
                display_name=str(request_fixture["display_name"]),
                source_locator=request_fixture["source_locator"],
                connection_id=request_fixture["connection_id"],
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(fake_client.calls[0]["url"], request_fixture["expected_url"])
        self.assertEqual(result.display_name, "Quarterly access review")
        self.assertIn("Jira issue SEC-123", result.artifact_text)
        self.assertIn("Status: Done", result.artifact_text)
        self.assertIn("Reviewer confirmed the revocation evidence.", result.artifact_text)
        self.assertEqual(result.metadata_update["provider_key"], "SEC-123")
        self.assertEqual(result.allowed_evidence_types, ["ticket"])

    def test_confluence_page_contract_fixture_round_trip(self) -> None:
        request_fixture = _load_fixture("confluence_page_request.json")
        response_fixture = _load_fixture("confluence_page_response.json")
        fake_client = _FakeHttpClient(
            _FakeResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                json_payload=response_fixture,
                url=str(request_fixture["expected_url"]),
            )
        )
        resolver = EnvConfiguredConnectorResolver(http_client=fake_client)

        with patch.dict(
            os.environ,
            {
                "AUDITFLOW_CONFLUENCE_FETCH_MODE": "http",
                "AUDITFLOW_CONFLUENCE_URL_TEMPLATE": str(request_fixture["url_template"]),
            },
            clear=False,
        ):
            result = resolver.fetch(
                "confluence",
                selector=str(request_fixture["selector"]),
                query=request_fixture["query"],
                display_name=str(request_fixture["display_name"]),
                source_locator=request_fixture["source_locator"],
                connection_id=request_fixture["connection_id"],
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(fake_client.calls[0]["url"], request_fixture["expected_url"])
        self.assertEqual(result.display_name, "Quarterly Access Review")
        self.assertIn("<h1>Quarterly Access Review</h1>", result.artifact_text)
        self.assertIn("data-page-id=\"PAGE-1\"", result.artifact_text)
        self.assertEqual(result.metadata_update["provider_key"], "PAGE-1")
        self.assertEqual(result.allowed_evidence_types, ["document"])


if __name__ == "__main__":
    unittest.main()
