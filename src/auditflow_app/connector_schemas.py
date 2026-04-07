from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"
SCHEMA_VERSION = "2026-03-27.1"


class _CanonicalConnectorModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CanonicalJiraIssueFetchRequest(_CanonicalConnectorModel):
    selector: str
    query: str | None = None
    display_name: str
    source_locator: str | None = None
    connection_id: str | None = None


class CanonicalConfluencePageFetchRequest(_CanonicalConnectorModel):
    selector: str
    query: str | None = None
    display_name: str
    source_locator: str | None = None
    connection_id: str | None = None


class CanonicalJiraTextBlock(_CanonicalConnectorModel):
    text: str


class CanonicalJiraStatus(_CanonicalConnectorModel):
    name: str


class CanonicalJiraAssignee(_CanonicalConnectorModel):
    displayName: str


class CanonicalJiraComment(_CanonicalConnectorModel):
    body: CanonicalJiraTextBlock


class CanonicalJiraCommentCollection(_CanonicalConnectorModel):
    comments: list[CanonicalJiraComment]


class CanonicalJiraFields(_CanonicalConnectorModel):
    summary: str
    status: CanonicalJiraStatus | None = None
    assignee: CanonicalJiraAssignee | None = None
    description: CanonicalJiraTextBlock | None = None
    comment: CanonicalJiraCommentCollection | None = None


class CanonicalJiraIssueResponse(_CanonicalConnectorModel):
    key: str
    fields: CanonicalJiraFields


class CanonicalConfluenceBodyValue(_CanonicalConnectorModel):
    value: str


class CanonicalConfluenceBody(_CanonicalConnectorModel):
    storage: CanonicalConfluenceBodyValue | None = None
    view: CanonicalConfluenceBodyValue | None = None
    export_view: CanonicalConfluenceBodyValue | None = None
    plain: CanonicalConfluenceBodyValue | None = None


class CanonicalConfluencePageResponse(_CanonicalConnectorModel):
    id: str
    title: str
    body: CanonicalConfluenceBody


def connector_schema_output_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "schemas" / "connector_contracts"


def build_connector_schema_models() -> dict[str, type[BaseModel]]:
    return {
        "jira_issue_request.schema.json": CanonicalJiraIssueFetchRequest,
        "jira_issue_response.schema.json": CanonicalJiraIssueResponse,
        "confluence_page_request.schema.json": CanonicalConfluencePageFetchRequest,
        "confluence_page_response.schema.json": CanonicalConfluencePageResponse,
    }


def _schema_document(model: type[BaseModel]) -> dict[str, Any]:
    document = model.model_json_schema()
    return {
        "$schema": JSON_SCHEMA_DRAFT,
        "x-auditflow-schema-version": SCHEMA_VERSION,
        **document,
    }


def build_connector_schema_documents() -> dict[str, dict[str, Any]]:
    return {
        filename: _schema_document(model)
        for filename, model in build_connector_schema_models().items()
    }


def write_connector_schema_documents(output_dir: Path | None = None) -> list[Path]:
    target_dir = output_dir or connector_schema_output_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    written_paths: list[Path] = []
    for filename, document in build_connector_schema_documents().items():
        output_path = target_dir / filename
        output_path.write_text(
            json.dumps(document, indent=2, ensure_ascii=True, sort_keys=False) + "\n",
            encoding="utf-8",
        )
        written_paths.append(output_path)
    return written_paths
