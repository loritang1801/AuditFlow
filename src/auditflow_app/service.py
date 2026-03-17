from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from typing import Any, TypeVar
from uuid import uuid4

from .api_models import (
    AuditCycleDashboardResponse,
    AuditFlowRunResponse,
    AuditFlowWorkflowStateResponse,
    AuditWorkspaceSummary,
    ControlCoverageSummary,
    ControlDetailResponse,
    CreateCycleCommand,
    CreateWorkspaceCommand,
    CycleProcessingCommand,
    EvidenceDetail,
    ExternalImportCommand,
    GapDecisionCommand,
    GapSummary,
    ImportAcceptedResponse,
    ImportDispatchResponse,
    ImportListResponse,
    ExportCreateCommand,
    ExportGenerationCommand,
    ExportPackageSummary,
    MappingReviewCommand,
    MappingReviewResponse,
    NarrativeSummary,
    ReviewDecisionListResponse,
    ReviewQueueResponse,
    UploadImportCommand,
)
from .repository import AuditFlowRepository
from .shared_runtime import load_shared_agent_platform

CommandT = TypeVar("CommandT", CycleProcessingCommand, ExportGenerationCommand)


@dataclass(slots=True)
class ParsedImportArtifact:
    raw_artifact_id: str
    normalized_artifact_id: str
    raw_text: str
    normalized_text: str
    summary: str
    chunk_texts: list[str]
    parser_kind: str
    parser_metadata: dict[str, object]


class AuditFlowAppService:
    def __init__(
        self,
        workflow_api_service,
        repository: AuditFlowRepository,
        runtime_stores=None,
    ) -> None:
        self.workflow_api_service = workflow_api_service
        self.repository = repository
        self.runtime_stores = runtime_stores
        self._shared_platform = load_shared_agent_platform()

    @staticmethod
    def _coerce_command(command: CommandT | dict[str, Any], model_type: type[CommandT]) -> CommandT:
        if isinstance(command, model_type):
            return command
        if isinstance(command, dict):
            return model_type.model_validate(command)
        raise TypeError(f"Expected {model_type.__name__} or dict, got {type(command).__name__}")

    @staticmethod
    def _to_run_response(result) -> AuditFlowRunResponse:
        return AuditFlowRunResponse.model_validate(result.model_dump())

    def list_workflows(self):
        return self.workflow_api_service.list_workflows()

    def create_workspace(
        self,
        command: CreateWorkspaceCommand | dict[str, Any],
    ) -> AuditWorkspaceSummary:
        if isinstance(command, dict):
            command = CreateWorkspaceCommand.model_validate(command)
        return self.repository.create_workspace(command)

    def get_workspace(self, workspace_id: str) -> AuditWorkspaceSummary:
        return self.repository.get_workspace(workspace_id)

    def create_cycle(
        self,
        command: CreateCycleCommand | dict[str, Any],
    ):
        if isinstance(command, dict):
            command = CreateCycleCommand.model_validate(command)
        return self.repository.create_cycle(command)

    def list_cycles(self, workspace_id: str):
        return self.repository.list_cycles(workspace_id)

    def get_cycle_dashboard(self, cycle_id: str) -> AuditCycleDashboardResponse:
        return self.repository.get_cycle_dashboard(cycle_id)

    def list_controls(self, cycle_id: str) -> list[ControlCoverageSummary]:
        return self.repository.list_controls(cycle_id)

    def get_control_detail(self, control_state_id: str) -> ControlDetailResponse:
        return self.repository.get_control_detail(control_state_id)

    def get_evidence(self, evidence_id: str) -> EvidenceDetail:
        return self.repository.get_evidence(evidence_id)

    def list_review_queue(self, cycle_id: str) -> ReviewQueueResponse:
        return self.repository.list_review_queue(cycle_id)

    def list_review_decisions(
        self,
        cycle_id: str,
        *,
        mapping_id: str | None = None,
        gap_id: str | None = None,
    ) -> ReviewDecisionListResponse:
        return self.repository.list_review_decisions(
            cycle_id,
            mapping_id=mapping_id,
            gap_id=gap_id,
        )

    def list_imports(
        self,
        cycle_id: str,
        *,
        ingest_status: str | None = None,
        source_type: str | None = None,
    ) -> ImportListResponse:
        return self.repository.list_imports(
            cycle_id,
            ingest_status=ingest_status,
            source_type=source_type,
        )

    def create_upload_import(
        self,
        cycle_id: str,
        command: UploadImportCommand | dict[str, Any],
    ) -> ImportAcceptedResponse:
        if isinstance(command, dict):
            command = UploadImportCommand.model_validate(command)
        response = self.repository.create_upload_import(cycle_id, command)
        if response.evidence_source_ids:
            self._enqueue_import_job(
                cycle_id=cycle_id,
                evidence_source_id=response.evidence_source_ids[0],
                workflow_run_id=response.workflow_run_id,
                source_type="upload",
                artifact_id=command.artifact_id,
                display_name=command.display_name,
                evidence_type=command.evidence_type_hint or "document",
                source_locator=command.source_locator,
                captured_at=command.captured_at,
                artifact_text=command.artifact_text,
                organization_id=command.organization_id,
                workspace_id=command.workspace_id,
            )
        return response

    def create_external_import(
        self,
        cycle_id: str,
        command: ExternalImportCommand | dict[str, Any],
    ) -> ImportAcceptedResponse:
        if isinstance(command, dict):
            command = ExternalImportCommand.model_validate(command)
        response = self.repository.create_external_import(cycle_id, command)
        selectors = command.upstream_ids or [command.query or ""]
        for index, evidence_source_id in enumerate(response.evidence_source_ids):
            selector = selectors[index] if index < len(selectors) else selectors[-1]
            workflow_run_id = (
                response.workflow_run_id
                if len(response.evidence_source_ids) == 1
                else f"{response.workflow_run_id}-{index + 1}"
            )
            display_name = f"{command.provider.upper()} import {selector}"
            self._enqueue_import_job(
                cycle_id=cycle_id,
                evidence_source_id=evidence_source_id,
                workflow_run_id=workflow_run_id,
                source_type=command.provider,
                artifact_id=f"artifact-{evidence_source_id}",
                display_name=display_name,
                evidence_type="ticket" if command.provider == "jira" else "document",
                source_locator=selector if command.query is None else f"{command.provider}:query",
                captured_at=None,
                artifact_text=None,
                organization_id=command.organization_id,
                workspace_id=command.workspace_id,
            )
        return response

    def dispatch_import_jobs(self) -> ImportDispatchResponse:
        from .worker import AuditFlowImportWorker

        result = AuditFlowImportWorker(self).dispatch_once()
        return ImportDispatchResponse.model_validate(result.model_dump())

    def review_mapping(
        self,
        mapping_id: str,
        command: MappingReviewCommand | dict[str, Any],
    ) -> MappingReviewResponse:
        if isinstance(command, dict):
            command = MappingReviewCommand.model_validate(command)
        return self.repository.review_mapping(mapping_id, command)

    def decide_gap(
        self,
        gap_id: str,
        command: GapDecisionCommand | dict[str, Any],
    ) -> GapSummary:
        if isinstance(command, dict):
            command = GapDecisionCommand.model_validate(command)
        return self.repository.decide_gap(gap_id, command)

    def list_narratives(
        self,
        cycle_id: str,
        *,
        snapshot_version: int | None = None,
        narrative_type: str | None = None,
    ) -> list[NarrativeSummary]:
        return self.repository.list_narratives(
            cycle_id,
            snapshot_version=snapshot_version,
            narrative_type=narrative_type,
        )

    def get_export_package(self, package_id: str) -> ExportPackageSummary:
        return self.repository.get_export_package(package_id)

    def process_cycle(self, command: CycleProcessingCommand | dict[str, Any]) -> AuditFlowRunResponse:
        command = self._coerce_command(command, CycleProcessingCommand)
        result = self.workflow_api_service.start_workflow(
            {
                "workflow_name": "auditflow_cycle_processing",
                "workflow_run_id": command.workflow_run_id,
                "input_payload": {
                    "audit_cycle_id": command.audit_cycle_id,
                    "audit_workspace_id": command.audit_workspace_id,
                    "source_id": command.source_id,
                    "source_type": command.source_type,
                    "artifact_id": command.artifact_id,
                    "extracted_text_or_summary": command.extracted_text_or_summary,
                    "allowed_evidence_types": command.allowed_evidence_types,
                    "evidence_item_id": command.evidence_item_id,
                    "evidence_chunk_refs": command.evidence_chunk_refs,
                    "in_scope_controls": command.in_scope_controls,
                    "framework_name": command.framework_name,
                    "mapping_payloads": command.mapping_payloads,
                    "control_text": command.control_text,
                    "organization_id": command.organization_id,
                    "workspace_id": command.workspace_id,
                },
                "state_overrides": command.state_overrides,
            }
        )
        response = self._to_run_response(result)
        self.repository.record_cycle_processing_result(
            cycle_id=command.audit_cycle_id,
            workflow_run_id=response.workflow_run_id,
            checkpoint_seq=response.checkpoint_seq,
        )
        return response

    def generate_export(self, command: ExportGenerationCommand | dict[str, Any]) -> AuditFlowRunResponse:
        command = self._coerce_command(command, ExportGenerationCommand)
        result = self.workflow_api_service.start_workflow(
            {
                "workflow_name": "auditflow_export_generation",
                "workflow_run_id": command.workflow_run_id,
                "input_payload": {
                    "audit_cycle_id": command.audit_cycle_id,
                    "audit_workspace_id": command.audit_workspace_id,
                    "working_snapshot_version": command.working_snapshot_version,
                    "accepted_mapping_refs": command.accepted_mapping_refs,
                    "open_gap_refs": command.open_gap_refs,
                    "export_scope": command.export_scope,
                    "organization_id": command.organization_id,
                    "workspace_id": command.workspace_id,
                },
                "state_overrides": command.state_overrides,
            }
        )
        response = self._to_run_response(result)
        self.repository.record_export_result(
            cycle_id=command.audit_cycle_id,
            workflow_run_id=response.workflow_run_id,
            snapshot_version=command.working_snapshot_version,
            checkpoint_seq=response.checkpoint_seq,
        )
        return response

    def create_export_package(
        self,
        cycle_id: str,
        command: ExportCreateCommand | dict[str, Any],
    ) -> ExportPackageSummary:
        if isinstance(command, dict):
            command = ExportCreateCommand.model_validate(command)
        dashboard = self.repository.get_cycle_dashboard(cycle_id)
        if (
            dashboard.accepted_mapping_count == 0
            or dashboard.review_queue_count > 0
            or dashboard.open_gap_count > 0
        ):
            raise ValueError("CYCLE_NOT_READY_FOR_EXPORT")
        latest_export = dashboard.latest_export_package
        if latest_export is not None and latest_export.snapshot_version > command.snapshot_version:
            raise ValueError("SNAPSHOT_STALE")
        if (
            latest_export is not None
            and latest_export.snapshot_version == command.snapshot_version
            and latest_export.status in {"queued", "building"}
        ):
            raise ValueError("EXPORT_ALREADY_RUNNING")
        self.generate_export(
            ExportGenerationCommand(
                workflow_run_id=command.workflow_run_id,
                audit_cycle_id=cycle_id,
                working_snapshot_version=command.snapshot_version,
                organization_id=command.organization_id,
                workspace_id=command.workspace_id,
            )
        )
        dashboard = self.repository.get_cycle_dashboard(cycle_id)
        if dashboard.latest_export_package is None:
            raise RuntimeError("Expected export package to be created")
        return dashboard.latest_export_package

    def get_workflow_state(self, workflow_run_id: str) -> AuditFlowWorkflowStateResponse:
        state = self.workflow_api_service.execution_service.load_workflow_state(workflow_run_id)
        return AuditFlowWorkflowStateResponse(
            workflow_run_id=workflow_run_id,
            workflow_type=str(state.get("workflow_type", "auditflow_cycle")),
            current_state=str(state.get("current_state", "")),
            checkpoint_seq=int(state.get("checkpoint_seq", 0)),
            raw_state=state,
        )

    def close(self) -> None:
        if self.runtime_stores is not None and hasattr(self.runtime_stores, "dispose"):
            self.runtime_stores.dispose()

    def _enqueue_import_job(
        self,
        *,
        cycle_id: str,
        evidence_source_id: str,
        workflow_run_id: str,
        source_type: str,
        artifact_id: str | None,
        display_name: str,
        evidence_type: str,
        source_locator: str | None,
        captured_at,
        artifact_text: str | None,
        organization_id: str,
        workspace_id: str,
    ) -> None:
        self.runtime_stores.outbox_store.append(
            self._shared_platform.OutboxEvent(
                event_id=f"import-job-{uuid4().hex[:10]}",
                event_name="auditflow.import.requested",
                workflow_run_id=workflow_run_id,
                workflow_type="auditflow_import",
                node_name="import_requested",
                aggregate_type="audit_cycle",
                aggregate_id=cycle_id,
                payload={
                    "cycle_id": cycle_id,
                    "evidence_source_id": evidence_source_id,
                    "workflow_run_id": workflow_run_id,
                    "source_type": source_type,
                    "artifact_id": artifact_id,
                    "display_name": display_name,
                    "evidence_type": evidence_type,
                    "source_locator": source_locator,
                    "artifact_text": artifact_text,
                    "captured_at": (
                        captured_at.isoformat() if hasattr(captured_at, "isoformat") and captured_at is not None else None
                    ),
                    "organization_id": organization_id,
                    "workspace_id": workspace_id,
                },
                emitted_at=datetime.now(UTC),
            )
        )

    def process_import_event(self, payload: dict[str, Any]) -> None:
        workflow_run_id = str(payload["workflow_run_id"])
        cycle_id = str(payload["cycle_id"])
        source_type = str(payload["source_type"])
        display_name = str(payload["display_name"])
        artifact_id = payload.get("artifact_id")
        source_locator = payload.get("source_locator")
        evidence_type = str(payload.get("evidence_type", "document"))
        parsed_artifact = self._parse_import_artifact(
            source_type=source_type,
            display_name=display_name,
            artifact_id=(str(artifact_id) if artifact_id is not None else f"artifact-{payload['evidence_source_id']}"),
            source_locator=(str(source_locator) if source_locator is not None else None),
            artifact_text=(str(payload["artifact_text"]) if payload.get("artifact_text") is not None else None),
        )
        self.repository.upsert_artifact_blob(
            artifact_id=parsed_artifact.raw_artifact_id,
            artifact_type=f"{source_type}_raw",
            content_text=parsed_artifact.raw_text,
            metadata_payload={
                "display_name": display_name,
                "source_type": source_type,
                "source_locator": source_locator,
                "parser_kind": parsed_artifact.parser_kind,
                **parsed_artifact.parser_metadata,
            },
        )
        self.repository.upsert_artifact_blob(
            artifact_id=parsed_artifact.normalized_artifact_id,
            artifact_type=f"{source_type}_normalized",
            content_text=parsed_artifact.normalized_text,
            metadata_payload={
                "display_name": display_name,
                "source_type": source_type,
                "source_locator": source_locator,
                "parser_kind": parsed_artifact.parser_kind,
                **parsed_artifact.parser_metadata,
            },
        )
        extracted_text_or_summary = str(
            payload.get("extracted_text_or_summary", parsed_artifact.summary)
        )
        control_text = str(
            payload.get("control_text", "Review imported evidence for control coverage.")
        )
        allowed_evidence_types = list(payload.get("allowed_evidence_types", [evidence_type]))
        mapping_payloads = list(payload.get("mapping_payloads", []))
        metadata_update = dict(payload.get("metadata_update", {}))
        metadata_update.update(
            {
                "parser_kind": parsed_artifact.parser_kind,
                "parser_metadata": parsed_artifact.parser_metadata,
            }
        )
        self.process_cycle(
            CycleProcessingCommand(
                workflow_run_id=workflow_run_id,
                audit_cycle_id=cycle_id,
                source_id=str(payload["evidence_source_id"]),
                source_type=source_type,
                artifact_id=parsed_artifact.raw_artifact_id,
                extracted_text_or_summary=extracted_text_or_summary,
                allowed_evidence_types=allowed_evidence_types,
                evidence_item_id=f"evidence-{uuid4().hex[:10]}",
                evidence_chunk_refs=[
                    {
                        "kind": "artifact_chunk_preview",
                        "artifact_id": parsed_artifact.normalized_artifact_id,
                        "chunk_index": index,
                    }
                    for index, _chunk in enumerate(parsed_artifact.chunk_texts)
                ],
                in_scope_controls=["control-1"],
                framework_name="SOC2",
                mapping_payloads=mapping_payloads,
                control_text=control_text,
                organization_id=str(payload.get("organization_id", "org-1")),
                workspace_id=str(payload.get("workspace_id", "ws-1")),
            )
        )
        self.repository.complete_import_processing(
            cycle_id=cycle_id,
            evidence_source_id=str(payload["evidence_source_id"]),
            workflow_run_id=workflow_run_id,
            title=display_name,
            evidence_type=evidence_type,
            summary=parsed_artifact.summary,
            artifact_id=parsed_artifact.raw_artifact_id,
            normalized_artifact_id=parsed_artifact.normalized_artifact_id,
            source_locator=(str(source_locator) if source_locator is not None else None),
            captured_at=(
                datetime.fromisoformat(payload["captured_at"])
                if payload.get("captured_at")
                else None
            ),
            chunk_texts=parsed_artifact.chunk_texts,
            metadata_update=metadata_update,
        )

    @staticmethod
    def _worker_now_utc() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def _parse_import_artifact(
        *,
        source_type: str,
        display_name: str,
        artifact_id: str,
        source_locator: str | None,
        artifact_text: str | None,
    ) -> ParsedImportArtifact:
        raw_text = (artifact_text or f"{display_name}\n\nSource: {source_locator or source_type}").strip()
        artifact_format = AuditFlowAppService._detect_artifact_format(
            artifact_id=artifact_id,
            source_locator=source_locator,
            raw_text=raw_text,
        )
        if artifact_format == "csv":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_csv_artifact(
                display_name=display_name,
                raw_text=raw_text,
            )
        elif artifact_format == "json":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_json_artifact(
                display_name=display_name,
                raw_text=raw_text,
            )
        else:
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
                source_type=source_type,
                display_name=display_name,
                raw_text=raw_text,
            )
        parser_kind = str(parser_metadata.get("source_format", artifact_format))
        summary = AuditFlowAppService._build_artifact_summary(chunk_texts[0] if chunk_texts else raw_text)
        if len(summary) > 180:
            summary = f"{summary[:177]}..."
        return ParsedImportArtifact(
            raw_artifact_id=artifact_id,
            normalized_artifact_id=f"{artifact_id}-normalized",
            raw_text=raw_text,
            normalized_text=normalized_text or raw_text,
            summary=summary,
            chunk_texts=chunk_texts,
            parser_kind=parser_kind,
            parser_metadata=parser_metadata,
        )

    @staticmethod
    def _detect_artifact_format(
        *,
        artifact_id: str,
        source_locator: str | None,
        raw_text: str,
    ) -> str:
        locator_candidates = [
            value.split("?", maxsplit=1)[0].lower()
            for value in (source_locator, artifact_id)
            if value is not None
        ]
        stripped = raw_text.lstrip()
        first_line = stripped.splitlines()[0] if stripped else ""
        if any(candidate.endswith(".json") for candidate in locator_candidates) and stripped.startswith(("{", "[")):
            return "json"
        if any(candidate.endswith(".csv") for candidate in locator_candidates) and "," in first_line:
            return "csv"
        return "text"

    @staticmethod
    def _parse_csv_artifact(
        *,
        display_name: str,
        raw_text: str,
    ) -> tuple[str, list[str], dict[str, object]]:
        reader = csv.DictReader(StringIO(raw_text))
        fieldnames = [name.strip() for name in (reader.fieldnames or []) if name and name.strip()]
        if len(fieldnames) < 2:
            return AuditFlowAppService._parse_text_artifact(
                source_type="upload",
                display_name=display_name,
                raw_text=raw_text,
            )
        row_chunks: list[str] = []
        for index, row in enumerate(reader, start=1):
            row_lines = [
                f"{column}: {value.strip()}"
                for column in fieldnames
                for value in [str(row.get(column, "")).strip()]
                if value
            ]
            if not row_lines:
                continue
            row_chunks.append(f"CSV row {index}\n" + "\n".join(row_lines))
        if not row_chunks:
            return AuditFlowAppService._parse_text_artifact(
                source_type="upload",
                display_name=display_name,
                raw_text=raw_text,
            )
        normalized_text = "\n\n".join([f"CSV import: {display_name}", *row_chunks])
        return normalized_text, row_chunks, {
            "source_format": "csv",
            "column_names": fieldnames,
            "row_count": len(row_chunks),
        }

    @staticmethod
    def _parse_json_artifact(
        *,
        display_name: str,
        raw_text: str,
    ) -> tuple[str, list[str], dict[str, object]]:
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            return AuditFlowAppService._parse_text_artifact(
                source_type="upload",
                display_name=display_name,
                raw_text=raw_text,
            )
        section_chunks = AuditFlowAppService._render_json_sections(parsed)
        if not section_chunks:
            return AuditFlowAppService._parse_text_artifact(
                source_type="upload",
                display_name=display_name,
                raw_text=raw_text,
            )
        normalized_text = "\n\n".join([f"JSON import: {display_name}", *section_chunks])
        metadata: dict[str, object] = {"source_format": "json"}
        if isinstance(parsed, dict):
            metadata["top_level_keys"] = list(parsed.keys())
        elif isinstance(parsed, list):
            metadata["top_level_count"] = len(parsed)
        return normalized_text, section_chunks, metadata

    @staticmethod
    def _parse_text_artifact(
        *,
        source_type: str,
        display_name: str,
        raw_text: str,
    ) -> tuple[str, list[str], dict[str, object]]:
        normalized_lines = [line.strip() for line in raw_text.replace("\r\n", "\n").split("\n")]
        normalized_text = "\n".join(normalized_lines).strip()
        paragraphs = [part.strip() for part in normalized_text.split("\n\n") if part.strip()]
        chunk_texts: list[str] = []
        current_chunk = ""
        for paragraph in paragraphs or [normalized_text]:
            candidate = paragraph if not current_chunk else f"{current_chunk}\n\n{paragraph}"
            if current_chunk and len(candidate) > 220:
                chunk_texts.append(current_chunk)
                current_chunk = paragraph
                continue
            current_chunk = candidate
        if current_chunk:
            chunk_texts.append(current_chunk)
        if not chunk_texts:
            chunk_texts.append(f"Imported {source_type}: {display_name}")
        return normalized_text or raw_text, chunk_texts, {
            "source_format": "text",
            "paragraph_count": len(paragraphs) if paragraphs else 1,
        }

    @staticmethod
    def _render_json_sections(parsed: object) -> list[str]:
        if isinstance(parsed, dict):
            sections: list[str] = []
            for key, value in parsed.items():
                lines = AuditFlowAppService._flatten_json_value(key, value)
                if lines:
                    sections.append("\n".join(lines))
            return sections
        if isinstance(parsed, list):
            sections = []
            for index, item in enumerate(parsed, start=1):
                lines = AuditFlowAppService._flatten_json_value(f"item[{index}]", item)
                if lines:
                    sections.append("\n".join(lines))
            return sections
        return ["\n".join(AuditFlowAppService._flatten_json_value("value", parsed))]

    @staticmethod
    def _flatten_json_value(prefix: str, value: object) -> list[str]:
        if isinstance(value, dict):
            lines: list[str] = []
            for key, nested in value.items():
                lines.extend(AuditFlowAppService._flatten_json_value(f"{prefix}.{key}", nested))
            return lines
        if isinstance(value, list):
            lines: list[str] = []
            for index, nested in enumerate(value):
                lines.extend(AuditFlowAppService._flatten_json_value(f"{prefix}[{index}]", nested))
            return lines
        return [f"{prefix}: {value}"]

    @staticmethod
    def _build_artifact_summary(chunk_text: str) -> str:
        return chunk_text.replace("\n", " ")
