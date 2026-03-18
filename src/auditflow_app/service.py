from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import html
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from typing import Any, TypeVar
from uuid import uuid4

from .api_models import (
    AuditCycleSummary,
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
    MappingListResponse,
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

    @staticmethod
    def _hash_request_payload(payload: dict[str, Any]) -> str:
        normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _load_idempotent_response(self, *, operation: str, idempotency_key: str | None, request_payload: dict[str, Any], model_type):
        if not idempotency_key:
            return None
        payload = self.repository.load_idempotency_response(
            operation=operation,
            idempotency_key=idempotency_key,
            request_hash=self._hash_request_payload(request_payload),
        )
        if payload is None:
            return None
        return model_type.model_validate(payload)

    def _store_idempotent_response(
        self,
        *,
        operation: str,
        idempotency_key: str | None,
        request_payload: dict[str, Any],
        response_payload: dict[str, Any],
    ) -> None:
        if not idempotency_key:
            return
        self.repository.store_idempotency_response(
            operation=operation,
            idempotency_key=idempotency_key,
            request_hash=self._hash_request_payload(request_payload),
            response_payload=response_payload,
        )

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
        *,
        idempotency_key: str | None = None,
    ):
        if isinstance(command, dict):
            command = CreateCycleCommand.model_validate(command)
        request_payload = command.model_dump(mode="json")
        cached = self._load_idempotent_response(
            operation="auditflow.create_cycle",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=AuditCycleSummary,
        )
        if cached is not None:
            return cached
        response = self.repository.create_cycle(command)
        self._store_idempotent_response(
            operation="auditflow.create_cycle",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            response_payload=response.model_dump(mode="json"),
        )
        return response

    def list_cycles(
        self,
        workspace_id: str,
        *,
        status: str | None = None,
    ):
        return self.repository.list_cycles(workspace_id, status=status)

    def get_cycle_dashboard(self, cycle_id: str) -> AuditCycleDashboardResponse:
        return self.repository.get_cycle_dashboard(cycle_id)

    def list_controls(
        self,
        cycle_id: str,
        *,
        coverage_status: str | None = None,
        search: str | None = None,
    ) -> list[ControlCoverageSummary]:
        return self.repository.list_controls(
            cycle_id,
            coverage_status=coverage_status,
            search=search,
        )

    def list_mappings(
        self,
        cycle_id: str,
        *,
        control_state_id: str | None = None,
        mapping_status: str | None = None,
    ) -> MappingListResponse:
        return self.repository.list_mappings(
            cycle_id,
            control_state_id=control_state_id,
            mapping_status=mapping_status,
        )

    def get_control_detail(self, control_state_id: str) -> ControlDetailResponse:
        return self.repository.get_control_detail(control_state_id)

    def get_evidence(self, evidence_id: str) -> EvidenceDetail:
        return self.repository.get_evidence(evidence_id)

    def list_gaps(
        self,
        cycle_id: str,
        *,
        status: str | None = None,
        severity: str | None = None,
    ) -> list[GapSummary]:
        return self.repository.list_gaps(cycle_id, status=status, severity=severity)

    def list_review_queue(
        self,
        cycle_id: str,
        *,
        control_state_id: str | None = None,
        severity: str | None = None,
        sort: str = "recent",
    ) -> ReviewQueueResponse:
        return self.repository.list_review_queue(
            cycle_id,
            control_state_id=control_state_id,
            severity=severity,
            sort=sort,
        )

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
        *,
        idempotency_key: str | None = None,
    ) -> ImportAcceptedResponse:
        if isinstance(command, dict):
            command = UploadImportCommand.model_validate(command)
        request_payload = {"cycle_id": cycle_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.create_upload_import",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=ImportAcceptedResponse,
        )
        if cached is not None:
            return cached
        response = self.repository.create_upload_import(cycle_id, command)
        if response.evidence_source_ids:
            self._emit_product_event(
                event_name="auditflow.import.accepted",
                workflow_run_id=response.workflow_run_id,
                aggregate_type="audit_cycle",
                aggregate_id=cycle_id,
                node_name="import_accepted",
                payload={
                    "cycle_id": cycle_id,
                    "evidence_source_id": response.evidence_source_ids[0],
                    "source_type": "upload",
                    "artifact_id": command.artifact_id,
                    "organization_id": command.organization_id,
                    "workspace_id": command.workspace_id,
                },
            )
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
                artifact_bytes_base64=command.artifact_bytes_base64,
                organization_id=command.organization_id,
                workspace_id=command.workspace_id,
            )
        self._store_idempotent_response(
            operation="auditflow.create_upload_import",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            response_payload=response.model_dump(mode="json"),
        )
        return response

    def create_external_import(
        self,
        cycle_id: str,
        command: ExternalImportCommand | dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> ImportAcceptedResponse:
        if isinstance(command, dict):
            command = ExternalImportCommand.model_validate(command)
        request_payload = {"cycle_id": cycle_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.create_external_import",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=ImportAcceptedResponse,
        )
        if cached is not None:
            return cached
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
            self._emit_product_event(
                event_name="auditflow.import.accepted",
                workflow_run_id=workflow_run_id,
                aggregate_type="audit_cycle",
                aggregate_id=cycle_id,
                node_name="import_accepted",
                payload={
                    "cycle_id": cycle_id,
                    "evidence_source_id": evidence_source_id,
                    "source_type": command.provider,
                    "source_locator": selector if command.query is None else f"{command.provider}:query",
                    "organization_id": command.organization_id,
                    "workspace_id": command.workspace_id,
                },
            )
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
                artifact_bytes_base64=None,
                organization_id=command.organization_id,
                workspace_id=command.workspace_id,
            )
        self._store_idempotent_response(
            operation="auditflow.create_external_import",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            response_payload=response.model_dump(mode="json"),
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
        *,
        idempotency_key: str | None = None,
    ) -> MappingReviewResponse:
        if isinstance(command, dict):
            command = MappingReviewCommand.model_validate(command)
        request_payload = {"mapping_id": mapping_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.review_mapping",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=MappingReviewResponse,
        )
        if cached is not None:
            return cached
        response = self.repository.review_mapping(mapping_id, command)
        context = self.repository.get_mapping_event_context(mapping_id)
        decisions = self.repository.list_review_decisions(context["cycle_id"], mapping_id=mapping_id)
        latest_decision_id = decisions.items[0].review_decision_id if decisions.items else None
        self._emit_product_event(
            event_name="auditflow.review.recorded",
            workflow_run_id=f"auditflow-review-{mapping_id}-{uuid4().hex[:8]}",
            aggregate_type="evidence_mapping",
            aggregate_id=mapping_id,
            node_name="mapping_review_recorded",
            payload={
                "cycle_id": context["cycle_id"],
                "workspace_id": context["workspace_id"],
                "review_decision_id": latest_decision_id,
                "mapping_id": mapping_id,
                "control_state_id": context["control_state_id"],
                "decision": command.decision,
            },
        )
        self._store_idempotent_response(
            operation="auditflow.review_mapping",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            response_payload=response.model_dump(mode="json"),
        )
        return response

    def decide_gap(
        self,
        gap_id: str,
        command: GapDecisionCommand | dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> GapSummary:
        if isinstance(command, dict):
            command = GapDecisionCommand.model_validate(command)
        request_payload = {"gap_id": gap_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.decide_gap",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=GapSummary,
        )
        if cached is not None:
            return cached
        response = self.repository.decide_gap(gap_id, command)
        context = self.repository.get_gap_event_context(gap_id)
        decisions = self.repository.list_review_decisions(context["cycle_id"], gap_id=gap_id)
        latest_decision_id = decisions.items[0].review_decision_id if decisions.items else None
        self._emit_product_event(
            event_name="auditflow.review.recorded",
            workflow_run_id=f"auditflow-gap-{gap_id}-{uuid4().hex[:8]}",
            aggregate_type="gap_record",
            aggregate_id=gap_id,
            node_name="gap_review_recorded",
            payload={
                "cycle_id": context["cycle_id"],
                "workspace_id": context["workspace_id"],
                "review_decision_id": latest_decision_id,
                "gap_id": gap_id,
                "control_state_id": context["control_state_id"],
                "decision": command.decision,
            },
        )
        self._store_idempotent_response(
            operation="auditflow.decide_gap",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            response_payload=response.model_dump(mode="json"),
        )
        return response

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

    def list_export_packages(
        self,
        cycle_id: str,
        *,
        snapshot_version: int | None = None,
        status: str | None = None,
    ) -> list[ExportPackageSummary]:
        return self.repository.list_export_packages(
            cycle_id,
            snapshot_version=snapshot_version,
            status=status,
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
        dashboard = self.repository.get_cycle_dashboard(command.audit_cycle_id)
        self._emit_product_event(
            event_name="auditflow.mapping.progress",
            workflow_run_id=response.workflow_run_id,
            aggregate_type="audit_cycle",
            aggregate_id=command.audit_cycle_id,
            node_name="cycle_processing_completed",
            payload={
                "cycle_id": command.audit_cycle_id,
                "workspace_id": command.workspace_id,
                "mapped_controls": dashboard.accepted_mapping_count,
                "pending_review_count": dashboard.review_queue_count,
                "organization_id": command.organization_id,
            },
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
        export_package = self.repository.record_export_result(
            cycle_id=command.audit_cycle_id,
            workflow_run_id=response.workflow_run_id,
            snapshot_version=command.working_snapshot_version,
            checkpoint_seq=response.checkpoint_seq,
        )
        self._emit_product_event(
            event_name="auditflow.package.ready",
            workflow_run_id=response.workflow_run_id,
            aggregate_type="audit_package",
            aggregate_id=export_package.package_id,
            node_name="export_completed",
            payload={
                "cycle_id": command.audit_cycle_id,
                "workspace_id": command.workspace_id,
                "package_id": export_package.package_id,
                "snapshot_version": export_package.snapshot_version,
                "artifact_id": export_package.artifact_id,
                "organization_id": command.organization_id,
            },
        )
        return response

    def create_export_package(
        self,
        cycle_id: str,
        command: ExportCreateCommand | dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> ExportPackageSummary:
        if isinstance(command, dict):
            command = ExportCreateCommand.model_validate(command)
        request_payload = {"cycle_id": cycle_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.create_export_package",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=ExportPackageSummary,
        )
        if cached is not None:
            return cached
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
        existing_snapshot_packages = self.repository.list_export_packages(
            cycle_id,
            snapshot_version=command.snapshot_version,
        )
        existing_snapshot_package = existing_snapshot_packages[0] if existing_snapshot_packages else None
        if (
            existing_snapshot_package is not None
            and existing_snapshot_package.status == "ready"
            and existing_snapshot_package.immutable_at is not None
        ):
            self._store_idempotent_response(
                operation="auditflow.create_export_package",
                idempotency_key=idempotency_key,
                request_payload=request_payload,
                response_payload=existing_snapshot_package.model_dump(mode="json"),
            )
            return existing_snapshot_package
        if (
            existing_snapshot_package is not None
            and existing_snapshot_package.status in {"queued", "building"}
        ):
            raise ValueError("EXPORT_ALREADY_RUNNING")
        self._emit_product_event(
            event_name="auditflow.export.progress",
            workflow_run_id=command.workflow_run_id,
            aggregate_type="audit_cycle",
            aggregate_id=cycle_id,
            node_name="export_requested",
            payload={
                "cycle_id": cycle_id,
                "workspace_id": command.workspace_id,
                "snapshot_version": command.snapshot_version,
                "status": "building",
                "organization_id": command.organization_id,
            },
        )
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
        response = dashboard.latest_export_package
        self._store_idempotent_response(
            operation="auditflow.create_export_package",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            response_payload=response.model_dump(mode="json"),
        )
        return response

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

    def _emit_product_event(
        self,
        *,
        event_name: str,
        workflow_run_id: str,
        aggregate_type: str,
        aggregate_id: str,
        node_name: str,
        payload: dict[str, object],
    ) -> None:
        if self.runtime_stores is None or not hasattr(self.runtime_stores, "outbox_store"):
            return
        self.runtime_stores.outbox_store.append(
            self._shared_platform.OutboxEvent(
                event_id=f"product-event-{uuid4().hex[:10]}",
                event_name=event_name,
                workflow_run_id=workflow_run_id,
                workflow_type="auditflow_import",
                node_name=node_name,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                payload=payload,
                emitted_at=datetime.now(UTC),
            )
        )

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
        artifact_bytes_base64: str | None,
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
                    "artifact_bytes_base64": artifact_bytes_base64,
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
            artifact_bytes_base64=(
                str(payload["artifact_bytes_base64"]) if payload.get("artifact_bytes_base64") is not None else None
            ),
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
        artifact_bytes_base64: str | None = None,
    ) -> ParsedImportArtifact:
        artifact_bytes = None
        if artifact_bytes_base64 is not None:
            artifact_bytes = AuditFlowAppService._decode_artifact_bytes(artifact_bytes_base64)
            artifact_format = AuditFlowAppService._detect_binary_artifact_format(
                artifact_id=artifact_id,
                source_locator=source_locator,
                artifact_bytes=artifact_bytes,
            )
            raw_text = AuditFlowAppService._build_binary_raw_text(
                display_name=display_name,
                source_type=source_type,
                source_locator=source_locator,
                artifact_format=artifact_format,
                artifact_bytes=artifact_bytes,
            )
        else:
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
        elif artifact_format == "markdown":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_markdown_artifact(
                display_name=display_name,
                raw_text=raw_text,
            )
        elif artifact_format == "html":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_html_artifact(
                display_name=display_name,
                raw_text=raw_text,
            )
        elif artifact_format == "pdf":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_pdf_artifact(
                display_name=display_name,
                artifact_bytes=artifact_bytes or b"",
            )
        elif artifact_format in {"png", "jpeg", "jpg", "image"}:
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_image_artifact(
                display_name=display_name,
                artifact_format=artifact_format,
                artifact_bytes=artifact_bytes or b"",
            )
        elif artifact_format == "binary":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_binary_artifact(
                display_name=display_name,
                artifact_bytes=artifact_bytes or b"",
            )
        else:
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
                source_type=source_type,
                display_name=display_name,
                raw_text=raw_text,
            )
        parser_kind = str(parser_metadata.get("parser_kind", parser_metadata.get("source_format", artifact_format)))
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
        if any(candidate.endswith((".md", ".markdown")) for candidate in locator_candidates):
            return "markdown"
        if any(candidate.endswith((".html", ".htm")) for candidate in locator_candidates):
            return "html"
        if stripped.startswith(("#", "- ", "* ", "1. ")):
            return "markdown"
        if re.search(r"(?is)<(html|body|section|article|div|p|table|h1|h2)\b", stripped):
            return "html"
        return "text"

    @staticmethod
    def _detect_binary_artifact_format(
        *,
        artifact_id: str,
        source_locator: str | None,
        artifact_bytes: bytes,
    ) -> str:
        locator_candidates = [
            value.split("?", maxsplit=1)[0].lower()
            for value in (source_locator, artifact_id)
            if value is not None
        ]
        if any(candidate.endswith(".pdf") for candidate in locator_candidates) or artifact_bytes.startswith(b"%PDF-"):
            return "pdf"
        if any(candidate.endswith(".png") for candidate in locator_candidates) or artifact_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png"
        if any(candidate.endswith((".jpg", ".jpeg")) for candidate in locator_candidates) or artifact_bytes.startswith(b"\xff\xd8"):
            return "jpeg"
        if any(candidate.endswith((".gif", ".bmp", ".webp")) for candidate in locator_candidates):
            return "image"
        return "binary"

    @staticmethod
    def _decode_artifact_bytes(value: str) -> bytes:
        encoded = value.strip()
        if encoded.startswith("data:"):
            _, _, encoded = encoded.partition(",")
        try:
            return base64.b64decode(encoded, validate=True)
        except binascii.Error as exc:
            raise ValueError("INVALID_ARTIFACT_BYTES") from exc

    @staticmethod
    def _build_binary_raw_text(
        *,
        display_name: str,
        source_type: str,
        source_locator: str | None,
        artifact_format: str,
        artifact_bytes: bytes,
    ) -> str:
        return "\n".join(
            [
                f"Binary import: {display_name}",
                f"Source: {source_locator or source_type}",
                f"Detected format: {artifact_format}",
                f"Byte size: {len(artifact_bytes)}",
            ]
        ).strip()

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
    def _parse_markdown_artifact(
        *,
        display_name: str,
        raw_text: str,
    ) -> tuple[str, list[str], dict[str, object]]:
        normalized_lines = raw_text.replace("\r\n", "\n").split("\n")
        heading_count = 0
        bullet_count = 0
        sections: list[str] = []
        current_heading = display_name
        current_lines: list[str] = []

        def flush_section() -> None:
            nonlocal current_lines
            content = "\n".join(line for line in current_lines if line).strip()
            if content:
                sections.append(f"{current_heading}\n{content}".strip())
            current_lines = []

        for raw_line in normalized_lines:
            line = raw_line.strip()
            if not line:
                if current_lines and current_lines[-1] != "":
                    current_lines.append("")
                continue
            heading_match = re.match(r"^#{1,6}\s+(.*)$", line)
            if heading_match is not None:
                flush_section()
                heading_count += 1
                current_heading = heading_match.group(1).strip() or display_name
                continue
            if re.match(r"^[-*+]\s+", line):
                bullet_count += 1
                current_lines.append(re.sub(r"^[-*+]\s+", "", line))
                continue
            if re.match(r"^\d+\.\s+", line):
                bullet_count += 1
                current_lines.append(re.sub(r"^\d+\.\s+", "", line))
                continue
            current_lines.append(line)

        flush_section()
        if not sections:
            return AuditFlowAppService._parse_text_artifact(
                source_type="upload",
                display_name=display_name,
                raw_text=raw_text,
            )
        normalized_text = "\n\n".join([f"Markdown import: {display_name}", *sections])
        return normalized_text, sections, {
            "source_format": "markdown",
            "heading_count": heading_count,
            "bullet_count": bullet_count,
            "section_count": len(sections),
        }

    @staticmethod
    def _parse_html_artifact(
        *,
        display_name: str,
        raw_text: str,
    ) -> tuple[str, list[str], dict[str, object]]:
        cleaned = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1>", " ", raw_text)
        cleaned = re.sub(r"(?i)<br\s*/?>", "\n", cleaned)
        cleaned = re.sub(r"(?i)</(p|div|section|article|li|tr|h[1-6]|ul|ol|table|thead|tbody|tfoot)>", "\n", cleaned)
        cleaned = re.sub(r"(?i)<li\b[^>]*>", "- ", cleaned)
        cleaned = re.sub(r"(?i)<(p|div|section|article|table|tr|h[1-6])\b[^>]*>", "\n", cleaned)
        cleaned = re.sub(r"(?s)<[^>]+>", " ", cleaned)
        cleaned = html.unescape(cleaned)
        cleaned = "\n".join(
            re.sub(r"\s+", " ", line).strip()
            for line in cleaned.splitlines()
        ).strip()
        normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
            source_type="upload",
            display_name=display_name,
            raw_text=cleaned or display_name,
        )
        parser_metadata.update(
            {
                "source_format": "html",
                "heading_count": len(re.findall(r"(?i)<h[1-6]\b", raw_text)),
            }
        )
        return normalized_text, chunk_texts, parser_metadata

    @staticmethod
    def _parse_pdf_artifact(
        *,
        display_name: str,
        artifact_bytes: bytes,
    ) -> tuple[str, list[str], dict[str, object]]:
        extracted_sections = AuditFlowAppService._extract_pdf_text_candidates(artifact_bytes)
        extracted_text = "\n\n".join(extracted_sections).strip()
        if not extracted_text:
            extracted_text = (
                f"{display_name}\n"
                "PDF document imported, but no extractable text was found. Manual reviewer follow-up is required."
            )
        normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
            source_type="upload",
            display_name=display_name,
            raw_text=extracted_text,
        )
        parser_metadata.update(
            {
                "source_format": "pdf",
                "parser_kind": "pdf_text_extract",
                "byte_size": len(artifact_bytes),
                "extraction_method": "heuristic_pdf_text_extract",
                "ocr_used": False,
                "section_count": len(extracted_sections) if extracted_sections else 1,
            }
        )
        return f"PDF import: {display_name}\n\n{normalized_text}", chunk_texts, parser_metadata

    @staticmethod
    def _parse_image_artifact(
        *,
        display_name: str,
        artifact_format: str,
        artifact_bytes: bytes,
    ) -> tuple[str, list[str], dict[str, object]]:
        extracted_sections: list[str] = []
        if artifact_format == "png":
            extracted_sections.extend(AuditFlowAppService._extract_png_text_chunks(artifact_bytes))
        extracted_sections.extend(AuditFlowAppService._extract_binary_text_candidates(artifact_bytes))
        extracted_sections = AuditFlowAppService._dedupe_text_sections(extracted_sections)
        extracted_text = "\n\n".join(extracted_sections).strip()
        if not extracted_text:
            extracted_text = (
                f"{display_name}\n"
                "Image evidence imported, but no OCR-like text could be recovered. Manual reviewer follow-up is required."
            )
        normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
            source_type="upload",
            display_name=display_name,
            raw_text=extracted_text,
        )
        parser_metadata.update(
            {
                "source_format": artifact_format,
                "parser_kind": "image_ocr_heuristic",
                "byte_size": len(artifact_bytes),
                "extraction_method": "metadata_and_printable_text_heuristic",
                "ocr_used": bool(extracted_sections),
                "section_count": len(extracted_sections) if extracted_sections else 1,
            }
        )
        return f"Image import: {display_name}\n\n{normalized_text}", chunk_texts, parser_metadata

    @staticmethod
    def _parse_binary_artifact(
        *,
        display_name: str,
        artifact_bytes: bytes,
    ) -> tuple[str, list[str], dict[str, object]]:
        extracted_sections = AuditFlowAppService._extract_binary_text_candidates(artifact_bytes)
        extracted_text = "\n\n".join(extracted_sections).strip()
        if not extracted_text:
            extracted_text = (
                f"{display_name}\n"
                "Binary evidence imported without extractable text. Manual reviewer follow-up is required."
            )
        normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
            source_type="upload",
            display_name=display_name,
            raw_text=extracted_text,
        )
        parser_metadata.update(
            {
                "source_format": "binary",
                "parser_kind": "binary_text_extract",
                "byte_size": len(artifact_bytes),
                "extraction_method": "printable_text_heuristic",
                "section_count": len(extracted_sections) if extracted_sections else 1,
            }
        )
        return f"Binary import: {display_name}\n\n{normalized_text}", chunk_texts, parser_metadata

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

    @staticmethod
    def _extract_pdf_text_candidates(artifact_bytes: bytes) -> list[str]:
        sections: list[str] = []
        for match in re.findall(rb"\(([^()]*)\)", artifact_bytes):
            decoded = match.decode("utf-8", errors="ignore").strip()
            if decoded and len(decoded) >= 6:
                sections.append(decoded)
        if sections:
            return AuditFlowAppService._dedupe_text_sections(sections)
        return AuditFlowAppService._extract_binary_text_candidates(artifact_bytes)

    @staticmethod
    def _extract_png_text_chunks(artifact_bytes: bytes) -> list[str]:
        if not artifact_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            return []
        sections: list[str] = []
        offset = 8
        while offset + 8 <= len(artifact_bytes):
            length = int.from_bytes(artifact_bytes[offset : offset + 4], byteorder="big")
            chunk_type = artifact_bytes[offset + 4 : offset + 8]
            data_start = offset + 8
            data_end = data_start + length
            crc_end = data_end + 4
            if crc_end > len(artifact_bytes):
                break
            chunk_data = artifact_bytes[data_start:data_end]
            if chunk_type == b"tEXt" and b"\x00" in chunk_data:
                _keyword, text_data = chunk_data.split(b"\x00", maxsplit=1)
                decoded = text_data.decode("utf-8", errors="ignore").strip()
                if decoded:
                    sections.append(decoded)
            offset = crc_end
            if chunk_type == b"IEND":
                break
        return sections

    @staticmethod
    def _extract_binary_text_candidates(artifact_bytes: bytes) -> list[str]:
        decoded = artifact_bytes.decode("latin-1", errors="ignore")
        matches = re.findall(r"[A-Za-z0-9][A-Za-z0-9 \t:/._#@(),'-]{5,}", decoded)
        cleaned = [
            re.sub(r"\s+", " ", match).strip()
            for match in matches
        ]
        filtered = [
            section
            for section in cleaned
            if len(section) >= 6 and not re.fullmatch(r"[A-Za-z0-9._/-]{1,8}", section)
        ]
        return AuditFlowAppService._dedupe_text_sections(filtered)

    @staticmethod
    def _dedupe_text_sections(sections: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for section in sections:
            normalized = section.strip()
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen:
                continue
            deduped.append(normalized)
            seen.add(key)
        return deduped
