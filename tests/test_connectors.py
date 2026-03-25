from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
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


class AuditFlowConnectorResolverTests(unittest.TestCase):
    def test_resolver_fetches_jira_issue_from_env_template(self) -> None:
        fake_client = _FakeHttpClient(
            _FakeResponse(
                status_code=200,
                headers={"content-type": "application/json"},
                json_payload={
                    "key": "SEC-123",
                    "fields": {
                        "summary": "Quarterly access review",
                        "status": {"name": "Done"},
                        "assignee": {"displayName": "Alice Reviewer"},
                        "description": {"text": "Emergency access was reviewed and revoked where needed."},
                    },
                },
                url="https://jira.example.test/rest/api/3/issue/SEC-123",
            )
        )
        resolver = EnvConfiguredConnectorResolver(http_client=fake_client)

        with patch.dict(
            os.environ,
            {
                "AUDITFLOW_JIRA_FETCH_MODE": "http",
                "AUDITFLOW_JIRA_URL_TEMPLATE": "https://jira.example.test/rest/api/3/issue/{selector}",
                "AUDITFLOW_JIRA_AUTH_TOKEN": "jira-token",
            },
            clear=False,
        ):
            result = resolver.fetch(
                "jira",
                selector="SEC-123",
                query=None,
                display_name="JIRA import SEC-123",
                source_locator=None,
                connection_id="connection-jira-1",
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.display_name, "Quarterly access review")
        self.assertIn("Status: Done", result.artifact_text)
        self.assertIn("Emergency access was reviewed", result.artifact_text)
        self.assertEqual(result.allowed_evidence_types, ["ticket"])
        self.assertEqual(fake_client.calls[0]["url"], "https://jira.example.test/rest/api/3/issue/SEC-123")
        self.assertEqual(fake_client.calls[0]["headers"]["Authorization"], "Bearer jira-token")

    def test_resolver_returns_none_in_auto_mode_without_templates(self) -> None:
        resolver = EnvConfiguredConnectorResolver(http_client=_FakeHttpClient(_FakeResponse(status_code=200, headers={})))

        with patch.dict(os.environ, {}, clear=True):
            result = resolver.fetch(
                "confluence",
                selector="PAGE-1",
                query=None,
                display_name="Confluence import PAGE-1",
                source_locator=None,
                connection_id="connection-confluence-1",
            )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
