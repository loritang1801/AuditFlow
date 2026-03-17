from __future__ import annotations

from dataclasses import dataclass
from time import sleep
from typing import Any, Protocol

from .shared_runtime import load_shared_agent_platform


class ImportHandler(Protocol):
    source_type: str

    def normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(slots=True)
class FilteredOutboxStore:
    store: object
    event_name: str

    def append(self, event) -> None:
        self.store.append(event)

    def list_pending(self):
        return [item for item in self.store.list_pending() if item.event.event_name == self.event_name]

    def mark_dispatched(self, event_id: str, dispatched_at) -> None:
        self.store.mark_dispatched(event_id, dispatched_at)

    def mark_failed(self, event_id: str, failure_message: str) -> None:
        self.store.mark_failed(event_id, failure_message)


class UploadImportHandler:
    source_type = "upload"

    def normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload)
        display_name = str(payload["display_name"])
        normalized["extracted_text_or_summary"] = (
            f"Uploaded evidence '{display_name}' was received and queued for reviewer confirmation."
        )
        normalized["control_text"] = "Review uploaded evidence and verify the latest control execution."
        normalized["allowed_evidence_types"] = [payload.get("evidence_type", "document")]
        normalized["metadata_update"] = {
            "handler_name": "upload",
            "ingest_mode": "artifact_upload",
            "display_name": display_name,
        }
        return normalized


class JiraImportHandler:
    source_type = "jira"

    def normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload)
        locator = str(payload.get("source_locator") or payload["display_name"])
        normalized["extracted_text_or_summary"] = (
            f"Imported Jira issue {locator} as evidence for access review or audit activity."
        )
        normalized["control_text"] = "Review Jira issue activity and confirm it evidences the target control."
        normalized["allowed_evidence_types"] = ["ticket"]
        normalized["metadata_update"] = {
            "handler_name": "jira",
            "provider_object_type": "issue",
            "normalized_locator": locator,
        }
        return normalized


class ConfluenceImportHandler:
    source_type = "confluence"

    def normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload)
        locator = str(payload.get("source_locator") or payload["display_name"])
        normalized["extracted_text_or_summary"] = (
            f"Imported Confluence page {locator} as policy or procedure evidence."
        )
        normalized["control_text"] = "Review Confluence documentation and confirm it represents the governed process."
        normalized["allowed_evidence_types"] = ["document"]
        normalized["metadata_update"] = {
            "handler_name": "confluence",
            "provider_object_type": "page",
            "normalized_locator": locator,
        }
        return normalized


class AuditFlowImportWorker:
    def __init__(self, app_service) -> None:
        self.app_service = app_service
        self._shared_platform = load_shared_agent_platform()
        self._handlers: dict[str, ImportHandler] = {}
        self._register_default_handlers()

    def register_handler(self, handler: ImportHandler) -> None:
        self._handlers[handler.source_type] = handler

    def dispatch_once(self):
        if self.app_service.runtime_stores is None or not hasattr(self.app_service.runtime_stores, "outbox_store"):
            raise ValueError("Import worker requires runtime outbox support")
        filtered_store = FilteredOutboxStore(
            self.app_service.runtime_stores.outbox_store,
            "auditflow.import.requested",
        )
        dispatcher = self._shared_platform.OutboxDispatcher(
            filtered_store,
            self._handle_event,
        )
        dispatched_at = self.app_service._worker_now_utc()
        return dispatcher.dispatch_pending(dispatched_at=dispatched_at)

    def run_polling(
        self,
        *,
        poll_interval_seconds: float = 1.0,
        max_iterations: int | None = None,
        max_idle_polls: int | None = None,
    ) -> list[object]:
        results = []
        iteration = 0
        idle_polls = 0
        while max_iterations is None or iteration < max_iterations:
            result = self.dispatch_once()
            results.append(result)
            iteration += 1
            if result.attempted_count == 0:
                idle_polls += 1
            else:
                idle_polls = 0
            if max_idle_polls is not None and idle_polls >= max_idle_polls:
                break
            if max_iterations is not None and iteration >= max_iterations:
                break
            sleep(poll_interval_seconds)
        return results

    def _handle_event(self, event) -> None:
        payload = dict(event.payload)
        source_type = str(payload.get("source_type", "upload"))
        handler = self._handlers.get(source_type)
        if handler is None:
            raise ValueError(f"Unsupported import source type: {source_type}")
        normalized_payload = handler.normalize_payload(payload)
        self.app_service.process_import_event(normalized_payload)

    def _register_default_handlers(self) -> None:
        self.register_handler(UploadImportHandler())
        self.register_handler(JiraImportHandler())
        self.register_handler(ConfluenceImportHandler())
