from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from time import sleep
from typing import Any, Callable, Protocol

from .connectors import ConnectorFetchResult, EnvConfiguredConnectorResolver
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


@dataclass(slots=True)
class ImportWorkerHeartbeat:
    iteration: int
    status: str
    attempted_count: int
    dispatched_count: int
    failed_count: int
    idle_polls: int
    consecutive_failures: int
    emitted_at: datetime
    error_message: str | None = None

    def to_dict(self) -> dict[str, object]:
        timestamp = self.emitted_at
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        return {
            "iteration": self.iteration,
            "status": self.status,
            "attempted_count": self.attempted_count,
            "dispatched_count": self.dispatched_count,
            "failed_count": self.failed_count,
            "idle_polls": self.idle_polls,
            "consecutive_failures": self.consecutive_failures,
            "emitted_at": timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "error_message": self.error_message,
        }


class UploadImportHandler:
    source_type = "upload"

    def normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload)
        display_name = str(payload["display_name"])
        artifact_text = str(
            payload.get("artifact_text")
            or (
                f"{display_name}\n\n"
                f"Source locator: {payload.get('source_locator') or 'upload'}\n"
                "Uploaded evidence was received for audit review.\n"
                "Control owner should confirm the latest execution evidence."
            )
        )
        normalized["extracted_text_or_summary"] = (
            artifact_text.splitlines()[0]
        )
        normalized["control_text"] = "Review uploaded evidence and verify the latest control execution."
        normalized["artifact_text"] = artifact_text
        normalized["allowed_evidence_types"] = [payload.get("evidence_type", "document")]
        normalized["metadata_update"] = {
            "handler_name": "upload",
            "ingest_mode": "artifact_upload",
            "display_name": display_name,
        }
        return normalized


class ExternalConnectorImportHandler:
    source_type = ""
    provider_object_type = ""
    default_evidence_type = "document"

    def __init__(self, connector_resolver: EnvConfiguredConnectorResolver | None = None) -> None:
        self.connector_resolver = connector_resolver

    def _fetch_live_payload(self, payload: dict[str, Any]) -> ConnectorFetchResult | None:
        if self.connector_resolver is None:
            return None
        selector = None
        upstream_object_id = payload.get("upstream_object_id")
        source_locator = payload.get("source_locator")
        query = payload.get("query")
        if upstream_object_id is not None:
            selector = str(upstream_object_id)
        elif source_locator is not None and str(source_locator).strip() and not str(source_locator).endswith(":query"):
            selector = str(source_locator)
        return self.connector_resolver.fetch(
            self.source_type,
            selector=selector,
            query=(str(query) if query is not None and str(query).strip() else None),
            display_name=str(payload["display_name"]),
            source_locator=(str(source_locator) if source_locator is not None else None),
            connection_id=(str(payload["connection_id"]) if payload.get("connection_id") is not None else None),
        )

    @staticmethod
    def _first_non_empty_line(value: str, *, fallback: str) -> str:
        for line in value.splitlines():
            candidate = line.strip()
            if candidate:
                return candidate
        return fallback

    def _apply_live_result(
        self,
        payload: dict[str, Any],
        result: ConnectorFetchResult,
        *,
        control_text: str,
    ) -> dict[str, Any]:
        normalized = dict(payload)
        normalized["display_name"] = result.display_name or str(payload["display_name"])
        if result.source_locator is not None:
            normalized["source_locator"] = result.source_locator
        if result.artifact_text is not None:
            normalized["artifact_text"] = result.artifact_text
        if result.artifact_bytes_base64 is not None:
            normalized["artifact_bytes_base64"] = result.artifact_bytes_base64
        extracted_text = result.extracted_text_or_summary or self._first_non_empty_line(
            str(result.artifact_text or normalized["display_name"]),
            fallback=str(normalized["display_name"]),
        )
        normalized["extracted_text_or_summary"] = extracted_text
        normalized["control_text"] = control_text
        normalized["allowed_evidence_types"] = list(result.allowed_evidence_types or [self.default_evidence_type])
        if result.captured_at is not None:
            normalized["captured_at"] = result.captured_at
        metadata_update = {
            "handler_name": self.source_type,
            "provider_object_type": self.provider_object_type,
            "fetch_mode": "live_http",
        }
        metadata_update.update(result.metadata_update)
        normalized["metadata_update"] = metadata_update
        return normalized


class JiraImportHandler(ExternalConnectorImportHandler):
    source_type = "jira"
    provider_object_type = "issue"
    default_evidence_type = "ticket"

    def normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        live_result = self._fetch_live_payload(payload)
        if live_result is not None:
            return self._apply_live_result(
                payload,
                live_result,
                control_text="Review Jira issue activity and confirm it evidences the target control.",
            )
        normalized = dict(payload)
        locator = str(payload.get("source_locator") or payload["display_name"])
        artifact_text = (
            f"Jira issue evidence\n\n"
            f"Issue locator: {locator}\n"
            f"Display name: {payload['display_name']}\n"
            "Issue imported for control verification and reviewer confirmation.\n"
            "Check approval comments, assignee history, and linked remediation notes."
        )
        normalized["extracted_text_or_summary"] = (
            artifact_text.splitlines()[0]
        )
        normalized["control_text"] = "Review Jira issue activity and confirm it evidences the target control."
        normalized["artifact_text"] = artifact_text
        normalized["allowed_evidence_types"] = ["ticket"]
        normalized["metadata_update"] = {
            "handler_name": "jira",
            "provider_object_type": "issue",
            "normalized_locator": locator,
            "fetch_mode": "synthetic_fallback",
        }
        return normalized


class ConfluenceImportHandler(ExternalConnectorImportHandler):
    source_type = "confluence"
    provider_object_type = "page"
    default_evidence_type = "document"

    def normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        live_result = self._fetch_live_payload(payload)
        if live_result is not None:
            return self._apply_live_result(
                payload,
                live_result,
                control_text="Review Confluence documentation and confirm it represents the governed process.",
            )
        normalized = dict(payload)
        locator = str(payload.get("source_locator") or payload["display_name"])
        artifact_text = (
            f"Confluence page evidence\n\n"
            f"Page locator: {locator}\n"
            f"Display name: {payload['display_name']}\n"
            "Documentation imported as policy or procedure evidence.\n"
            "Review the described workflow, ownership, and control checkpoints."
        )
        normalized["extracted_text_or_summary"] = (
            artifact_text.splitlines()[0]
        )
        normalized["control_text"] = "Review Confluence documentation and confirm it represents the governed process."
        normalized["artifact_text"] = artifact_text
        normalized["allowed_evidence_types"] = ["document"]
        normalized["metadata_update"] = {
            "handler_name": "confluence",
            "provider_object_type": "page",
            "normalized_locator": locator,
            "fetch_mode": "synthetic_fallback",
        }
        return normalized


class AuditFlowImportWorkerSupervisor:
    def __init__(
        self,
        worker: "AuditFlowImportWorker",
        *,
        sleep_fn: Callable[[float], None] = sleep,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.worker = worker
        self._sleep_fn = sleep_fn
        self._now_fn = now_fn or worker.app_service._worker_now_utc

    def run(
        self,
        *,
        poll_interval_seconds: float = 1.0,
        max_iterations: int | None = None,
        max_idle_polls: int | None = None,
        max_consecutive_failures: int = 3,
        failure_backoff_seconds: float = 5.0,
        heartbeat_every_iterations: int = 1,
        heartbeat_callback: Callable[[ImportWorkerHeartbeat], None] | None = None,
    ) -> list[ImportWorkerHeartbeat]:
        if max_consecutive_failures < 1:
            raise ValueError("max_consecutive_failures must be >= 1")
        if heartbeat_every_iterations < 1:
            raise ValueError("heartbeat_every_iterations must be >= 1")

        heartbeats: list[ImportWorkerHeartbeat] = []
        iteration = 0
        idle_polls = 0
        consecutive_failures = 0
        normalized_max_idle_polls = None if max_idle_polls is not None and max_idle_polls <= 0 else max_idle_polls

        while max_iterations is None or iteration < max_iterations:
            iteration += 1
            try:
                result = self.worker.dispatch_once()
            except Exception as exc:
                consecutive_failures += 1
                heartbeat = self._record_heartbeat(
                    heartbeats,
                    iteration=iteration,
                    status=("retrying" if consecutive_failures < max_consecutive_failures else "failed"),
                    attempted_count=0,
                    dispatched_count=0,
                    failed_count=1,
                    idle_polls=idle_polls,
                    consecutive_failures=consecutive_failures,
                    error_message=str(exc),
                    heartbeat_callback=heartbeat_callback,
                )
                if consecutive_failures >= max_consecutive_failures:
                    raise
                if failure_backoff_seconds > 0:
                    self._sleep_fn(failure_backoff_seconds)
                continue

            consecutive_failures = 0
            attempted_count = int(getattr(result, "attempted_count", 0))
            dispatched_count = int(getattr(result, "dispatched_count", 0))
            failed_count = int(getattr(result, "failed_count", 0))
            idle_polls = idle_polls + 1 if attempted_count == 0 else 0
            status = "degraded" if failed_count > 0 else ("idle" if attempted_count == 0 else "active")
            should_emit_heartbeat = (
                attempted_count > 0
                or failed_count > 0
                or iteration % heartbeat_every_iterations == 0
                or (
                    normalized_max_idle_polls is not None
                    and idle_polls >= normalized_max_idle_polls
                )
            )
            if should_emit_heartbeat:
                self._record_heartbeat(
                    heartbeats,
                    iteration=iteration,
                    status=status,
                    attempted_count=attempted_count,
                    dispatched_count=dispatched_count,
                    failed_count=failed_count,
                    idle_polls=idle_polls,
                    consecutive_failures=consecutive_failures,
                    error_message=None,
                    heartbeat_callback=heartbeat_callback,
                )

            if normalized_max_idle_polls is not None and idle_polls >= normalized_max_idle_polls:
                break
            if max_iterations is not None and iteration >= max_iterations:
                break
            if poll_interval_seconds > 0:
                self._sleep_fn(poll_interval_seconds)

        return heartbeats

    def _record_heartbeat(
        self,
        heartbeats: list[ImportWorkerHeartbeat],
        *,
        iteration: int,
        status: str,
        attempted_count: int,
        dispatched_count: int,
        failed_count: int,
        idle_polls: int,
        consecutive_failures: int,
        error_message: str | None,
        heartbeat_callback: Callable[[ImportWorkerHeartbeat], None] | None,
    ) -> ImportWorkerHeartbeat:
        heartbeat = ImportWorkerHeartbeat(
            iteration=iteration,
            status=status,
            attempted_count=attempted_count,
            dispatched_count=dispatched_count,
            failed_count=failed_count,
            idle_polls=idle_polls,
            consecutive_failures=consecutive_failures,
            emitted_at=self._now_fn(),
            error_message=error_message,
        )
        heartbeats.append(heartbeat)
        if heartbeat_callback is not None:
            heartbeat_callback(heartbeat)
        return heartbeat


class AuditFlowImportWorker:
    def __init__(self, app_service, *, connector_resolver: EnvConfiguredConnectorResolver | None = None) -> None:
        self.app_service = app_service
        self._shared_platform = load_shared_agent_platform()
        self._handlers: dict[str, ImportHandler] = {}
        self.connector_resolver = connector_resolver or EnvConfiguredConnectorResolver()
        self._register_default_handlers()

    def register_handler(self, handler: ImportHandler) -> None:
        self._handlers[handler.source_type] = handler

    def build_supervisor(
        self,
        *,
        sleep_fn: Callable[[float], None] = sleep,
        now_fn: Callable[[], datetime] | None = None,
    ) -> AuditFlowImportWorkerSupervisor:
        return AuditFlowImportWorkerSupervisor(
            self,
            sleep_fn=sleep_fn,
            now_fn=now_fn,
        )

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
        self.register_handler(JiraImportHandler(self.connector_resolver))
        self.register_handler(ConfluenceImportHandler(self.connector_resolver))
