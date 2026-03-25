from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Protocol
from urllib.parse import quote

from .shared_runtime import load_shared_agent_platform


class HttpClient(Protocol):
    def get(self, url: str, *, headers: dict[str, str], follow_redirects: bool, timeout: float): ...


@dataclass(slots=True)
class ConnectorFetchResult:
    display_name: str | None = None
    source_locator: str | None = None
    artifact_text: str | None = None
    artifact_bytes_base64: str | None = None
    captured_at: str | None = None
    extracted_text_or_summary: str | None = None
    metadata_update: dict[str, object] = field(default_factory=dict)
    allowed_evidence_types: list[str] = field(default_factory=list)


class EnvConfiguredConnectorResolver:
    def __init__(self, *, http_client: HttpClient | None = None) -> None:
        self._http_client = http_client
        self._shared_platform = load_shared_agent_platform()

    def _env_value(self, name: str) -> str | None:
        return self._shared_platform.env_value(name)

    @staticmethod
    def _provider_prefix(provider: str) -> str:
        return f"AUDITFLOW_{provider.strip().upper()}"

    def _fetch_mode(self, provider: str) -> str:
        return self._shared_platform.normalize_requested_mode(
            self._env_value(f"{self._provider_prefix(provider)}_FETCH_MODE"),
            allowed_modes=("auto", "local", "http"),
            default="auto",
        )

    def describe_capability(self, provider: str) -> dict[str, object]:
        requested_mode = self._fetch_mode(provider)
        prefix = self._provider_prefix(provider)
        has_url_template = self._env_value(f"{prefix}_URL_TEMPLATE") is not None
        has_query_template = self._env_value(f"{prefix}_QUERY_URL_TEMPLATE") is not None
        has_auth = (
            self._env_value(f"{prefix}_AUTH_TOKEN") is not None
            or (
                self._env_value(f"{prefix}_USERNAME") is not None
                and self._env_value(f"{prefix}_PASSWORD") is not None
            )
        )
        decision = self._shared_platform.resolve_remote_mode(
            requested_mode=requested_mode,
            allowed_modes=("auto", "local", "http"),
            local_mode="local",
            remote_mode="http",
            has_remote_configuration=has_url_template or has_query_template,
            auto_fallback_reason="CONNECTOR_HTTP_TEMPLATE_NOT_CONFIGURED",
        )
        return self._shared_platform.RuntimeCapabilityDescriptor(
            requested_mode=decision.requested_mode,
            effective_mode=decision.effective_mode,
            backend_id=(
                f"{provider}-http-template"
                if decision.effective_mode == "http"
                else f"{provider}-synthetic"
            ),
            fallback_reason=decision.fallback_reason,
            details={
                "has_url_template": has_url_template,
                "has_query_template": has_query_template,
                "has_auth": has_auth,
            },
        ).as_dict()

    def _timeout_seconds(self, provider: str) -> float:
        configured = self._env_value(f"{self._provider_prefix(provider)}_TIMEOUT_SECONDS")
        if configured is None:
            return 20.0
        try:
            parsed = float(configured)
        except ValueError:
            parsed = 20.0
        return max(1.0, parsed)

    def _build_http_client(self) -> HttpClient:
        if self._http_client is not None:
            return self._http_client
        import httpx

        self._http_client = httpx.Client()
        return self._http_client

    def _build_headers(self, provider: str) -> dict[str, str]:
        prefix = self._provider_prefix(provider)
        headers: dict[str, str] = {}
        headers_json = self._env_value(f"{prefix}_HEADERS_JSON")
        if headers_json is not None:
            parsed = json.loads(headers_json)
            if isinstance(parsed, dict):
                headers.update({str(key): str(value) for key, value in parsed.items()})
        auth_type = (self._env_value(f"{prefix}_AUTH_TYPE") or "").lower()
        bearer_token = self._env_value(f"{prefix}_AUTH_TOKEN")
        username = self._env_value(f"{prefix}_USERNAME")
        password = self._env_value(f"{prefix}_PASSWORD")
        if auth_type in {"", "bearer"} and bearer_token is not None:
            headers.setdefault("Authorization", f"Bearer {bearer_token}")
        elif auth_type == "basic" and username is not None and password is not None:
            encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            headers.setdefault("Authorization", f"Basic {encoded}")
        headers.setdefault("Accept", "application/json, text/html, text/plain;q=0.9, */*;q=0.8")
        return headers

    def _resolve_target_url(
        self,
        provider: str,
        *,
        mode: str,
        selector: str | None,
        query: str | None,
        display_name: str,
        source_locator: str | None,
        connection_id: str | None,
    ) -> str | None:
        locator = (source_locator or "").strip()
        if mode == "http" and locator.startswith(("https://", "http://")) and query is None:
            return locator
        prefix = self._provider_prefix(provider)
        template_name = f"{prefix}_QUERY_URL_TEMPLATE" if query is not None else f"{prefix}_URL_TEMPLATE"
        template = self._env_value(template_name)
        if template is None:
            return None
        raw_selector = selector or ""
        raw_query = query or ""
        return template.format(
            selector=quote(raw_selector, safe=""),
            selector_raw=raw_selector,
            query=quote(raw_query, safe=""),
            query_raw=raw_query,
            display_name=quote(display_name, safe=""),
            display_name_raw=display_name,
            connection_id=quote(connection_id or "", safe=""),
            connection_id_raw=connection_id or "",
        )

    @staticmethod
    def _extract_captured_at(headers: Any) -> str | None:
        if not isinstance(headers, dict):
            try:
                headers = dict(headers)
            except Exception:
                headers = {}
        last_modified = str(headers.get("last-modified") or headers.get("Last-Modified") or "").strip()
        if not last_modified:
            return None
        try:
            parsed = parsedate_to_datetime(last_modified)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat()

    @staticmethod
    def _looks_like_html(value: str) -> bool:
        normalized = value.strip().lower()
        return normalized.startswith("<!doctype html") or normalized.startswith("<html") or bool(
            re.search(r"<(div|p|h1|h2|section|article|body)\b", normalized)
        )

    @staticmethod
    def _lookup_nested(value: Any, path: tuple[str, ...]) -> Any:
        current = value
        for key in path:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current

    @classmethod
    def _flatten_text(cls, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (int, float, bool)):
            return str(value)
        if isinstance(value, list):
            parts = [cls._flatten_text(item) for item in value]
            return "\n".join(part for part in parts if part)
        if isinstance(value, dict):
            if isinstance(value.get("text"), str):
                direct = value["text"].strip()
                if direct:
                    return direct
            ordered_parts = []
            for key in ("title", "summary", "name", "value", "description", "body", "content", "fields"):
                flattened = cls._flatten_text(value.get(key))
                if flattened:
                    ordered_parts.append(flattened)
            if ordered_parts:
                return "\n".join(ordered_parts)
            nested_parts = [cls._flatten_text(item) for item in value.values()]
            return "\n".join(part for part in nested_parts if part)
        return ""

    @classmethod
    def _render_jira_payload(cls, payload: Any, selector: str | None, display_name: str) -> ConnectorFetchResult:
        if not isinstance(payload, dict):
            text = json.dumps(payload, ensure_ascii=True, indent=2, default=str)
            return ConnectorFetchResult(
                display_name=display_name,
                artifact_text=text,
                extracted_text_or_summary=(text.splitlines()[0] if text else display_name),
                allowed_evidence_types=["ticket"],
            )
        fields = payload.get("fields") if isinstance(payload.get("fields"), dict) else {}
        issue_key = str(payload.get("key") or selector or display_name)
        summary = str(fields.get("summary") or payload.get("summary") or display_name)
        status_name = cls._flatten_text(cls._lookup_nested(fields, ("status", "name")))
        assignee_name = cls._flatten_text(cls._lookup_nested(fields, ("assignee", "displayName")))
        description = cls._flatten_text(fields.get("description"))
        comments = cls._flatten_text(cls._lookup_nested(fields, ("comment", "comments")))
        lines = [
            f"Jira issue {issue_key}",
            "",
            f"Summary: {summary}",
        ]
        if status_name:
            lines.append(f"Status: {status_name}")
        if assignee_name:
            lines.append(f"Assignee: {assignee_name}")
        if description:
            lines.extend(["", "Description:", description])
        if comments:
            lines.extend(["", "Comments:", comments])
        return ConnectorFetchResult(
            display_name=summary,
            artifact_text="\n".join(lines).strip(),
            extracted_text_or_summary=summary,
            metadata_update={"provider_key": issue_key},
            allowed_evidence_types=["ticket"],
        )

    @classmethod
    def _render_confluence_payload(cls, payload: Any, selector: str | None, display_name: str) -> ConnectorFetchResult:
        if not isinstance(payload, dict):
            text = json.dumps(payload, ensure_ascii=True, indent=2, default=str)
            return ConnectorFetchResult(
                display_name=display_name,
                artifact_text=text,
                extracted_text_or_summary=(text.splitlines()[0] if text else display_name),
                allowed_evidence_types=["document"],
            )
        title = str(payload.get("title") or display_name)
        page_id = str(payload.get("id") or selector or display_name)
        body_html = ""
        for path in (("body", "storage", "value"), ("body", "view", "value"), ("body", "export_view", "value")):
            candidate = cls._lookup_nested(payload, path)
            if isinstance(candidate, str) and candidate.strip():
                body_html = candidate.strip()
                break
        if body_html:
            artifact_text = (
                "<html><body>"
                f"<h1>{title}</h1>"
                f"<div data-page-id=\"{page_id}\">{body_html}</div>"
                "</body></html>"
            )
        else:
            excerpt = cls._flatten_text(cls._lookup_nested(payload, ("body", "plain", "value"))) or cls._flatten_text(payload)
            artifact_text = (
                f"Confluence page {page_id}\n\n"
                f"Title: {title}\n\n"
                f"{excerpt}".strip()
            )
        return ConnectorFetchResult(
            display_name=title,
            artifact_text=artifact_text,
            extracted_text_or_summary=title,
            metadata_update={"provider_key": page_id},
            allowed_evidence_types=["document"],
        )

    @classmethod
    def _render_generic_json_payload(
        cls,
        payload: Any,
        *,
        display_name: str,
        provider: str,
    ) -> ConnectorFetchResult:
        text = json.dumps(payload, ensure_ascii=True, indent=2, default=str)
        return ConnectorFetchResult(
            display_name=display_name,
            artifact_text=text,
            extracted_text_or_summary=f"{provider.title()} import {display_name}",
        )

    def _parse_response(
        self,
        *,
        provider: str,
        selector: str | None,
        query: str | None,
        display_name: str,
        url: str,
        response: Any,
    ) -> ConnectorFetchResult:
        headers = getattr(response, "headers", {}) or {}
        content_type = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
        response_status = int(getattr(response, "status_code", 0) or 0)
        final_url = str(getattr(response, "url", url) or url)
        metadata_update = {
            "fetch_mode": "live_http",
            "connector_response_status": response_status,
            "connector_content_type": content_type,
            "connector_source_locator": final_url,
        }
        if "json" in content_type:
            json_payload = response.json()
            if provider == "jira":
                rendered = self._render_jira_payload(json_payload, selector, display_name)
            elif provider == "confluence":
                rendered = self._render_confluence_payload(json_payload, selector, display_name)
            else:
                rendered = self._render_generic_json_payload(
                    json_payload,
                    display_name=display_name,
                    provider=provider,
                )
        else:
            text = getattr(response, "text", None)
            if text is None:
                content = getattr(response, "content", b"")
                text = content.decode("utf-8", errors="replace") if isinstance(content, (bytes, bytearray)) else str(content)
            rendered = ConnectorFetchResult(
                display_name=display_name,
                artifact_text=text,
                extracted_text_or_summary=display_name,
            )
            if self._looks_like_html(text):
                rendered.allowed_evidence_types = ["document"]
        rendered.metadata_update = {**metadata_update, **rendered.metadata_update}
        rendered.source_locator = final_url
        rendered.captured_at = self._extract_captured_at(headers)
        if not rendered.allowed_evidence_types:
            rendered.allowed_evidence_types = ["ticket" if provider == "jira" else "document"]
        return rendered

    def fetch(
        self,
        provider: str,
        *,
        selector: str | None,
        query: str | None,
        display_name: str,
        source_locator: str | None,
        connection_id: str | None = None,
    ) -> ConnectorFetchResult | None:
        mode = self._fetch_mode(provider)
        if mode == "local":
            return None
        url = self._resolve_target_url(
            provider,
            mode=mode,
            selector=selector,
            query=query,
            display_name=display_name,
            source_locator=source_locator,
            connection_id=connection_id,
        )
        if url is None:
            if mode == "http":
                raise ValueError(f"{self._provider_prefix(provider)}_URL_TEMPLATE")
            return None
        try:
            response = self._build_http_client().get(
                url,
                headers=self._build_headers(provider),
                follow_redirects=True,
                timeout=self._timeout_seconds(provider),
            )
            response.raise_for_status()
            return self._parse_response(
                provider=provider,
                selector=selector,
                query=query,
                display_name=display_name,
                url=url,
                response=response,
            )
        except Exception:
            if mode == "http":
                raise
            return None
