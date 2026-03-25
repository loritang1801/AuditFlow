from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import html
import io
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from typing import Any, TypeVar
from uuid import uuid4
import xml.etree.ElementTree as ET
import zipfile

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
    EvidenceSearchResponse,
    ExternalImportCommand,
    GapDecisionCommand,
    GapSummary,
    ImportAcceptedResponse,
    ImportDispatchResponse,
    ImportListResponse,
    MappingClaimCommand,
    MappingClaimReleaseCommand,
    MappingClaimResponse,
    MappingListResponse,
    ExportCreateCommand,
    ExportGenerationCommand,
    ExportPackageSummary,
    MemoryRecordListResponse,
    MappingReviewCommand,
    MappingReviewResponse,
    NarrativeSummary,
    RuntimeCapabilitiesResponse,
    ReviewDecisionListResponse,
    ReviewQueueResponse,
    ToolAccessAuditListResponse,
    UploadImportCommand,
)
from .connectors import EnvConfiguredConnectorResolver
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
        auth_service=None,
    ) -> None:
        self.workflow_api_service = workflow_api_service
        self.repository = repository
        self.runtime_stores = runtime_stores
        self.auth_service = auth_service
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

    def _resolve_cycle_scope(
        self,
        cycle_id: str,
        *,
        organization_id: str | None = None,
    ) -> dict[str, str]:
        return self.repository.get_cycle_context(
            cycle_id,
            organization_id=organization_id,
        )

    @staticmethod
    def _workflow_auth_state(auth_context: Any | None = None) -> dict[str, str]:
        if auth_context is None:
            return {}
        fields: dict[str, str] = {}
        for key in ("user_id", "role", "session_id"):
            raw_value = auth_context.get(key) if isinstance(auth_context, dict) else getattr(auth_context, key, None)
            if raw_value not in {None, ""}:
                fields[key] = str(raw_value)
        return fields

    def _run_registered_workflow(
        self,
        *,
        workflow_name: str,
        workflow_run_id: str,
        input_payload: dict[str, Any],
        state_overrides: dict[str, Any] | None = None,
        auth_context: Any | None = None,
    ) -> tuple[AuditFlowRunResponse, Any]:
        definition = self.workflow_api_service.workflow_registry.get(workflow_name)
        merged_state_overrides = dict(state_overrides or {})
        merged_state_overrides.update(self._workflow_auth_state(auth_context))
        initial_state = definition.initial_state_builder(
            workflow_run_id,
            input_payload,
            merged_state_overrides,
        )
        run_result = self.workflow_api_service.execution_service.run_workflow(
            workflow_run_id=workflow_run_id,
            workflow_type=definition.workflow_type,
            initial_state=initial_state,
            steps=definition.steps,
            source_builders=definition.source_builders,
        )
        response = AuditFlowRunResponse(
            workflow_name=workflow_name,
            workflow_run_id=run_result.workflow_run_id,
            workflow_type=run_result.workflow_type,
            current_state=str(run_result.final_state.get("current_state", "")),
            checkpoint_seq=int(run_result.final_state.get("checkpoint_seq", 0)),
            emitted_events=[event for step in run_result.step_results for event in step.emitted_events],
        )
        return response, run_result

    @staticmethod
    def _step_structured_output(run_result, node_name: str) -> dict[str, Any]:
        for step_result in run_result.step_results:
            if step_result.trace.node_name == node_name:
                return (
                    dict(step_result.agent_output.structured_output)
                    if isinstance(step_result.agent_output.structured_output, dict)
                    else {}
                )
        return {}

    @staticmethod
    def _final_state_payloads(run_result, key: str) -> list[dict[str, Any]]:
        payloads = run_result.final_state.get(key)
        if not isinstance(payloads, list):
            return []
        return [dict(item) for item in payloads if isinstance(item, dict)]

    @staticmethod
    def _final_state_ids(run_result, key: str) -> list[str]:
        values = run_result.final_state.get(key)
        if not isinstance(values, list):
            return []
        return [str(value) for value in values if value]

    @staticmethod
    def _coerce_datetime(value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if not isinstance(value, str) or value == "":
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _stable_import_evidence_id(*, cycle_id: str, evidence_source_id: str) -> str:
        digest = hashlib.sha256(f"{cycle_id}::{evidence_source_id}".encode("utf-8")).hexdigest()[:10]
        return f"evidence-{digest}"

    def _execute_cycle_processing(
        self,
        command: CycleProcessingCommand,
        *,
        organization_id: str | None = None,
        auth_context: Any | None = None,
    ) -> tuple[AuditFlowRunResponse, Any, dict[str, str]]:
        cycle_scope = self._resolve_cycle_scope(
            command.audit_cycle_id,
            organization_id=organization_id or command.organization_id,
        )
        response, run_result = self._run_registered_workflow(
            workflow_name="auditflow_cycle_processing",
            workflow_run_id=command.workflow_run_id,
            input_payload={
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
                "mapping_memory_context": command.mapping_memory_context,
                "challenge_memory_context": command.challenge_memory_context,
                "freshness_policy": command.freshness_policy,
                "control_text": command.control_text,
                "organization_id": cycle_scope["organization_id"],
                "workspace_id": cycle_scope["workspace_id"],
            },
            state_overrides=command.state_overrides,
            auth_context=auth_context,
        )
        self.repository.record_cycle_processing_result(
            cycle_id=command.audit_cycle_id,
            workflow_run_id=response.workflow_run_id,
            checkpoint_seq=response.checkpoint_seq,
            organization_id=cycle_scope["organization_id"],
            evidence_item_id=command.evidence_item_id,
            mapping_output=self._step_structured_output(run_result, "mapping"),
            challenge_output=self._step_structured_output(run_result, "challenge"),
            mapping_payloads=self._final_state_payloads(run_result, "mapping_payloads"),
        )
        dashboard = self.repository.get_cycle_dashboard(
            command.audit_cycle_id,
            organization_id=cycle_scope["organization_id"],
        )
        self._emit_product_event(
            event_name="auditflow.mapping.progress",
            workflow_run_id=response.workflow_run_id,
            aggregate_type="audit_cycle",
            aggregate_id=command.audit_cycle_id,
            node_name="cycle_processing_completed",
            payload={
                "cycle_id": command.audit_cycle_id,
                "workspace_id": cycle_scope["workspace_id"],
                "mapped_controls": dashboard.accepted_mapping_count,
                "pending_review_count": dashboard.review_queue_count,
                "organization_id": cycle_scope["organization_id"],
            },
        )
        return response, run_result, cycle_scope

    def _execute_export_generation(
        self,
        command: ExportGenerationCommand,
        *,
        organization_id: str | None = None,
        auth_context: Any | None = None,
    ) -> tuple[AuditFlowRunResponse, Any, dict[str, str], ExportPackageSummary]:
        cycle_scope = self._resolve_cycle_scope(
            command.audit_cycle_id,
            organization_id=organization_id or command.organization_id,
        )
        snapshot_refs = self.repository.read_snapshot_refs(
            command.audit_cycle_id,
            working_snapshot_version=command.working_snapshot_version,
            organization_id=cycle_scope["organization_id"],
        )
        accepted_mapping_refs = (
            list(command.accepted_mapping_refs)
            if command.accepted_mapping_refs
            else list(snapshot_refs.get("accepted_mapping_ids", []))
        )
        open_gap_refs = (
            list(command.open_gap_refs)
            if command.open_gap_refs
            else list(snapshot_refs.get("open_gap_ids", []))
        )
        response, run_result = self._run_registered_workflow(
            workflow_name="auditflow_export_generation",
            workflow_run_id=command.workflow_run_id,
            input_payload={
                "audit_cycle_id": command.audit_cycle_id,
                "audit_workspace_id": command.audit_workspace_id,
                "working_snapshot_version": command.working_snapshot_version,
                "accepted_mapping_refs": accepted_mapping_refs,
                "open_gap_refs": open_gap_refs,
                "export_scope": command.export_scope,
                "organization_id": cycle_scope["organization_id"],
                "workspace_id": cycle_scope["workspace_id"],
            },
            state_overrides=command.state_overrides,
            auth_context=auth_context,
        )
        export_package = self.repository.record_export_result(
            cycle_id=command.audit_cycle_id,
            workflow_run_id=response.workflow_run_id,
            snapshot_version=command.working_snapshot_version,
            checkpoint_seq=response.checkpoint_seq,
            organization_id=cycle_scope["organization_id"],
            writer_output=self._step_structured_output(run_result, "package_generation"),
            narrative_ids=self._final_state_ids(run_result, "narrative_ids"),
        )
        self._emit_product_event(
            event_name="auditflow.package.ready",
            workflow_run_id=response.workflow_run_id,
            aggregate_type="audit_package",
            aggregate_id=export_package.package_id,
            node_name="export_completed",
            payload={
                "cycle_id": command.audit_cycle_id,
                "workspace_id": cycle_scope["workspace_id"],
                "package_id": export_package.package_id,
                "snapshot_version": export_package.snapshot_version,
                "artifact_id": export_package.artifact_id,
                "organization_id": cycle_scope["organization_id"],
            },
        )
        return response, run_result, cycle_scope, export_package

    def list_workflows(self):
        return self.workflow_api_service.list_workflows()

    def get_runtime_capabilities(self) -> RuntimeCapabilitiesResponse:
        connector_resolver = EnvConfiguredConnectorResolver()
        model_gateway = getattr(self.workflow_api_service.execution_service, "model_gateway", None)
        model_provider = (
            model_gateway.describe_capability()
            if model_gateway is not None and hasattr(model_gateway, "describe_capability")
            else {
                "requested_mode": "unknown",
                "effective_mode": "unknown",
                "backend_id": "unknown",
                "fallback_reason": None,
                "details": {},
            }
        )
        return RuntimeCapabilitiesResponse.model_validate(
            {
                "product": "auditflow",
                "model_provider": model_provider,
                "embedding_provider": self.repository.describe_embedding_capability(),
                "vector_search": self.repository.describe_vector_search_capability(),
                "connectors": {
                    "jira": connector_resolver.describe_capability("jira"),
                    "confluence": connector_resolver.describe_capability("confluence"),
                },
            }
        )

    def create_workspace(
        self,
        command: CreateWorkspaceCommand | dict[str, Any],
        *,
        organization_id: str | None = None,
    ) -> AuditWorkspaceSummary:
        if isinstance(command, dict):
            command = CreateWorkspaceCommand.model_validate(command)
        return self.repository.create_workspace(command, organization_id=organization_id)

    def get_workspace(
        self,
        workspace_id: str,
        *,
        organization_id: str | None = None,
    ) -> AuditWorkspaceSummary:
        return self.repository.get_workspace(workspace_id, organization_id=organization_id)

    def create_cycle(
        self,
        command: CreateCycleCommand | dict[str, Any],
        *,
        idempotency_key: str | None = None,
        organization_id: str | None = None,
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
        response = self.repository.create_cycle(command, organization_id=organization_id)
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
        organization_id: str | None = None,
    ):
        return self.repository.list_cycles(workspace_id, status=status, organization_id=organization_id)

    def get_cycle_dashboard(
        self,
        cycle_id: str,
        *,
        organization_id: str | None = None,
    ) -> AuditCycleDashboardResponse:
        return self.repository.get_cycle_dashboard(cycle_id, organization_id=organization_id)

    def list_controls(
        self,
        cycle_id: str,
        *,
        coverage_status: str | None = None,
        search: str | None = None,
        organization_id: str | None = None,
    ) -> list[ControlCoverageSummary]:
        return self.repository.list_controls(
            cycle_id,
            coverage_status=coverage_status,
            search=search,
            organization_id=organization_id,
        )

    def list_mappings(
        self,
        cycle_id: str,
        *,
        control_state_id: str | None = None,
        mapping_status: str | None = None,
        organization_id: str | None = None,
    ) -> MappingListResponse:
        return self.repository.list_mappings(
            cycle_id,
            control_state_id=control_state_id,
            mapping_status=mapping_status,
            organization_id=organization_id,
        )

    def get_control_detail(
        self,
        control_state_id: str,
        *,
        organization_id: str | None = None,
    ) -> ControlDetailResponse:
        return self.repository.get_control_detail(control_state_id, organization_id=organization_id)

    def get_evidence(
        self,
        evidence_id: str,
        *,
        organization_id: str | None = None,
    ) -> EvidenceDetail:
        return self.repository.get_evidence(evidence_id, organization_id=organization_id)

    def search_evidence(
        self,
        cycle_id: str,
        *,
        query: str,
        limit: int = 5,
        organization_id: str | None = None,
    ) -> EvidenceSearchResponse:
        return self.repository.search_evidence(
            cycle_id=cycle_id,
            query=query,
            limit=limit,
            organization_id=organization_id,
        )

    def list_memory_records(
        self,
        cycle_id: str,
        *,
        scope: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        memory_type: str | None = None,
        status: str | None = "active",
        organization_id: str | None = None,
    ) -> MemoryRecordListResponse:
        return self.repository.list_memory_records(
            cycle_id,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            memory_type=memory_type,
            status=status,
            organization_id=organization_id,
        )

    def list_gaps(
        self,
        cycle_id: str,
        *,
        status: str | None = None,
        severity: str | None = None,
        organization_id: str | None = None,
    ) -> list[GapSummary]:
        return self.repository.list_gaps(
            cycle_id,
            status=status,
            severity=severity,
            organization_id=organization_id,
        )

    def list_review_queue(
        self,
        cycle_id: str,
        *,
        control_state_id: str | None = None,
        severity: str | None = None,
        claim_state: str | None = None,
        sort: str = "recent",
        organization_id: str | None = None,
        viewer_user_id: str | None = None,
    ) -> ReviewQueueResponse:
        return self.repository.list_review_queue(
            cycle_id,
            control_state_id=control_state_id,
            severity=severity,
            claim_state=claim_state,
            sort=sort,
            organization_id=organization_id,
            viewer_user_id=viewer_user_id,
        )

    def list_review_decisions(
        self,
        cycle_id: str,
        *,
        mapping_id: str | None = None,
        gap_id: str | None = None,
        organization_id: str | None = None,
    ) -> ReviewDecisionListResponse:
        return self.repository.list_review_decisions(
            cycle_id,
            mapping_id=mapping_id,
            gap_id=gap_id,
            organization_id=organization_id,
        )

    def list_tool_access_audit(
        self,
        *,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        execution_status: str | None = None,
        organization_id: str | None = None,
    ) -> ToolAccessAuditListResponse:
        return self.repository.list_tool_access_audit(
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            tool_name=tool_name,
            subject_type=subject_type,
            subject_id=subject_id,
            execution_status=execution_status,
            organization_id=organization_id,
        )

    def list_cycle_tool_access_audit(
        self,
        cycle_id: str,
        *,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        organization_id: str | None = None,
    ) -> ToolAccessAuditListResponse:
        return self.repository.list_cycle_tool_access_audit(
            cycle_id,
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            tool_name=tool_name,
            execution_status=execution_status,
            organization_id=organization_id,
        )

    def list_control_tool_access_audit(
        self,
        control_state_id: str,
        *,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        organization_id: str | None = None,
    ) -> ToolAccessAuditListResponse:
        return self.repository.list_control_tool_access_audit(
            control_state_id,
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            tool_name=tool_name,
            execution_status=execution_status,
            organization_id=organization_id,
        )

    def list_mapping_tool_access_audit(
        self,
        mapping_id: str,
        *,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        organization_id: str | None = None,
    ) -> ToolAccessAuditListResponse:
        return self.repository.list_mapping_tool_access_audit(
            mapping_id,
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            tool_name=tool_name,
            execution_status=execution_status,
            organization_id=organization_id,
        )

    def list_imports(
        self,
        cycle_id: str,
        *,
        ingest_status: str | None = None,
        source_type: str | None = None,
        organization_id: str | None = None,
    ) -> ImportListResponse:
        return self.repository.list_imports(
            cycle_id,
            ingest_status=ingest_status,
            source_type=source_type,
            organization_id=organization_id,
        )

    def create_upload_import(
        self,
        cycle_id: str,
        command: UploadImportCommand | dict[str, Any],
        *,
        idempotency_key: str | None = None,
        organization_id: str | None = None,
        auth_context: Any | None = None,
    ) -> ImportAcceptedResponse:
        if isinstance(command, dict):
            command = UploadImportCommand.model_validate(command)
        cycle_scope = self._resolve_cycle_scope(cycle_id, organization_id=organization_id)
        request_payload = {"cycle_id": cycle_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.create_upload_import",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=ImportAcceptedResponse,
        )
        if cached is not None:
            return cached
        response = self.repository.create_upload_import(
            cycle_id,
            command,
            organization_id=cycle_scope["organization_id"],
        )
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
                    "organization_id": cycle_scope["organization_id"],
                    "workspace_id": cycle_scope["workspace_id"],
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
                organization_id=cycle_scope["organization_id"],
                workspace_id=cycle_scope["workspace_id"],
                connection_id=None,
                upstream_object_id=None,
                query=None,
                auth_context=auth_context,
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
        organization_id: str | None = None,
        auth_context: Any | None = None,
    ) -> ImportAcceptedResponse:
        if isinstance(command, dict):
            command = ExternalImportCommand.model_validate(command)
        cycle_scope = self._resolve_cycle_scope(cycle_id, organization_id=organization_id)
        request_payload = {"cycle_id": cycle_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.create_external_import",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=ImportAcceptedResponse,
        )
        if cached is not None:
            return cached
        response = self.repository.create_external_import(
            cycle_id,
            command,
            organization_id=cycle_scope["organization_id"],
        )
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
                    "organization_id": cycle_scope["organization_id"],
                    "workspace_id": cycle_scope["workspace_id"],
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
                organization_id=cycle_scope["organization_id"],
                workspace_id=cycle_scope["workspace_id"],
                connection_id=command.connection_id,
                upstream_object_id=(selector if command.query is None else None),
                query=command.query,
                auth_context=auth_context,
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
        organization_id: str | None = None,
        reviewer_id: str | None = None,
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
        response = self.repository.review_mapping(
            mapping_id,
            command,
            organization_id=organization_id,
            reviewer_id=reviewer_id,
        )
        context = self.repository.get_mapping_event_context(mapping_id, organization_id=organization_id)
        decisions = self.repository.list_review_decisions(
            context["cycle_id"],
            mapping_id=mapping_id,
            organization_id=context["organization_id"],
        )
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
                "organization_id": context["organization_id"],
            },
        )
        self._store_idempotent_response(
            operation="auditflow.review_mapping",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            response_payload=response.model_dump(mode="json"),
        )
        return response

    def claim_mapping(
        self,
        mapping_id: str,
        command: MappingClaimCommand | dict[str, Any],
        *,
        idempotency_key: str | None = None,
        organization_id: str | None = None,
        reviewer_id: str,
    ) -> MappingClaimResponse:
        if isinstance(command, dict):
            command = MappingClaimCommand.model_validate(command)
        request_payload = {"mapping_id": mapping_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.claim_mapping",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=MappingClaimResponse,
        )
        if cached is not None:
            return cached
        response = self.repository.claim_mapping(
            mapping_id,
            command,
            organization_id=organization_id,
            reviewer_id=reviewer_id,
        )
        context = self.repository.get_mapping_event_context(mapping_id, organization_id=organization_id)
        self._emit_product_event(
            event_name="auditflow.review.claimed",
            workflow_run_id=f"auditflow-claim-{mapping_id}-{uuid4().hex[:8]}",
            aggregate_type="evidence_mapping",
            aggregate_id=mapping_id,
            node_name="mapping_review_claimed",
            payload={
                "cycle_id": context["cycle_id"],
                "workspace_id": context["workspace_id"],
                "mapping_id": mapping_id,
                "control_state_id": context["control_state_id"],
                "reviewer_id": reviewer_id,
                "organization_id": context["organization_id"],
                "claim_expires_at": (
                    response.claim_expires_at.isoformat() if response.claim_expires_at is not None else None
                ),
            },
        )
        self._store_idempotent_response(
            operation="auditflow.claim_mapping",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            response_payload=response.model_dump(mode="json"),
        )
        return response

    def release_mapping_claim(
        self,
        mapping_id: str,
        command: MappingClaimReleaseCommand | dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        organization_id: str | None = None,
        reviewer_id: str,
    ) -> MappingClaimResponse:
        if command is None:
            command = MappingClaimReleaseCommand()
        elif isinstance(command, dict):
            command = MappingClaimReleaseCommand.model_validate(command)
        request_payload = {"mapping_id": mapping_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.release_mapping_claim",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=MappingClaimResponse,
        )
        if cached is not None:
            return cached
        response = self.repository.release_mapping_claim(
            mapping_id,
            command,
            organization_id=organization_id,
            reviewer_id=reviewer_id,
        )
        context = self.repository.get_mapping_event_context(mapping_id, organization_id=organization_id)
        self._emit_product_event(
            event_name="auditflow.review.claim_released",
            workflow_run_id=f"auditflow-claim-release-{mapping_id}-{uuid4().hex[:8]}",
            aggregate_type="evidence_mapping",
            aggregate_id=mapping_id,
            node_name="mapping_review_claim_released",
            payload={
                "cycle_id": context["cycle_id"],
                "workspace_id": context["workspace_id"],
                "mapping_id": mapping_id,
                "control_state_id": context["control_state_id"],
                "reviewer_id": reviewer_id,
                "organization_id": context["organization_id"],
            },
        )
        self._store_idempotent_response(
            operation="auditflow.release_mapping_claim",
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
        organization_id: str | None = None,
        reviewer_id: str | None = None,
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
        response = self.repository.decide_gap(
            gap_id,
            command,
            organization_id=organization_id,
            reviewer_id=reviewer_id,
        )
        context = self.repository.get_gap_event_context(gap_id, organization_id=organization_id)
        decisions = self.repository.list_review_decisions(
            context["cycle_id"],
            gap_id=gap_id,
            organization_id=context["organization_id"],
        )
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
                "organization_id": context["organization_id"],
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
        organization_id: str | None = None,
    ) -> list[NarrativeSummary]:
        return self.repository.list_narratives(
            cycle_id,
            snapshot_version=snapshot_version,
            narrative_type=narrative_type,
            organization_id=organization_id,
        )

    def list_export_packages(
        self,
        cycle_id: str,
        *,
        snapshot_version: int | None = None,
        status: str | None = None,
        organization_id: str | None = None,
    ) -> list[ExportPackageSummary]:
        return self.repository.list_export_packages(
            cycle_id,
            snapshot_version=snapshot_version,
            status=status,
            organization_id=organization_id,
        )

    def get_export_package(
        self,
        package_id: str,
        *,
        organization_id: str | None = None,
    ) -> ExportPackageSummary:
        return self.repository.get_export_package(package_id, organization_id=organization_id)

    def process_cycle(
        self,
        command: CycleProcessingCommand | dict[str, Any],
        *,
        organization_id: str | None = None,
        auth_context: Any | None = None,
    ) -> AuditFlowRunResponse:
        command = self._coerce_command(command, CycleProcessingCommand)
        response, _run_result, _cycle_scope = self._execute_cycle_processing(
            command,
            organization_id=organization_id,
            auth_context=auth_context,
        )
        return response

    def generate_export(
        self,
        command: ExportGenerationCommand | dict[str, Any],
        *,
        organization_id: str | None = None,
        auth_context: Any | None = None,
    ) -> AuditFlowRunResponse:
        command = self._coerce_command(command, ExportGenerationCommand)
        response, _run_result, _cycle_scope, _export_package = self._execute_export_generation(
            command,
            organization_id=organization_id,
            auth_context=auth_context,
        )
        return response

    def create_export_package(
        self,
        cycle_id: str,
        command: ExportCreateCommand | dict[str, Any],
        *,
        idempotency_key: str | None = None,
        organization_id: str | None = None,
        auth_context: Any | None = None,
    ) -> ExportPackageSummary:
        if isinstance(command, dict):
            command = ExportCreateCommand.model_validate(command)
        cycle_scope = self._resolve_cycle_scope(cycle_id, organization_id=organization_id or command.organization_id)
        request_payload = {"cycle_id": cycle_id, **command.model_dump(mode="json")}
        cached = self._load_idempotent_response(
            operation="auditflow.create_export_package",
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            model_type=ExportPackageSummary,
        )
        if cached is not None:
            return cached
        dashboard = self.repository.get_cycle_dashboard(cycle_id, organization_id=cycle_scope["organization_id"])
        if (
            dashboard.accepted_mapping_count == 0
            or dashboard.review_queue_count > 0
            or dashboard.open_gap_count > 0
        ):
            raise ValueError("CYCLE_NOT_READY_FOR_EXPORT")
        if command.snapshot_version != dashboard.cycle.current_snapshot_version:
            raise ValueError("SNAPSHOT_STALE")
        latest_export = dashboard.latest_export_package
        if latest_export is not None and latest_export.snapshot_version > command.snapshot_version:
            raise ValueError("SNAPSHOT_STALE")
        existing_snapshot_packages = self.repository.list_export_packages(
            cycle_id,
            snapshot_version=command.snapshot_version,
            organization_id=cycle_scope["organization_id"],
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
                "workspace_id": cycle_scope["workspace_id"],
                "snapshot_version": command.snapshot_version,
                "status": "building",
                "organization_id": cycle_scope["organization_id"],
            },
        )
        self.generate_export(
            ExportGenerationCommand(
                workflow_run_id=command.workflow_run_id,
                audit_cycle_id=cycle_id,
                working_snapshot_version=command.snapshot_version,
                organization_id=cycle_scope["organization_id"],
                workspace_id=cycle_scope["workspace_id"],
            ),
            auth_context=auth_context,
        )
        dashboard = self.repository.get_cycle_dashboard(cycle_id, organization_id=cycle_scope["organization_id"])
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

    def get_workflow_state(
        self,
        workflow_run_id: str,
        *,
        organization_id: str | None = None,
    ) -> AuditFlowWorkflowStateResponse:
        state = self.workflow_api_service.execution_service.load_workflow_state(workflow_run_id)
        if organization_id is not None and str(state.get("organization_id") or "") != organization_id:
            raise KeyError(workflow_run_id)
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
        connection_id: str | None = None,
        upstream_object_id: str | None = None,
        query: str | None = None,
        auth_context: Any | None = None,
    ) -> None:
        auth_state = self._workflow_auth_state(auth_context)
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
                    "connection_id": connection_id,
                    "upstream_object_id": upstream_object_id,
                    "query": query,
                    **auth_state,
                },
                emitted_at=datetime.now(UTC),
            )
        )

    def process_import_event(self, payload: dict[str, Any]) -> None:
        workflow_run_id = str(payload["workflow_run_id"])
        cycle_id = str(payload["cycle_id"])
        cycle_scope = self._resolve_cycle_scope(
            cycle_id,
            organization_id=str(payload.get("organization_id", "")) or None,
        )
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
                "organization_id": cycle_scope["organization_id"],
                "workspace_id": cycle_scope["workspace_id"],
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
                "organization_id": cycle_scope["organization_id"],
                "workspace_id": cycle_scope["workspace_id"],
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
        grounding = self.repository.build_cycle_processing_grounding(
            cycle_id=cycle_id,
            evidence_summary=parsed_artifact.summary,
            chunk_texts=parsed_artifact.chunk_texts,
            organization_id=cycle_scope["organization_id"],
        )
        artifact_chunk_refs = [
            {
                "kind": "artifact_chunk_preview",
                "artifact_id": parsed_artifact.normalized_artifact_id,
                "chunk_index": index,
                "text_excerpt": chunk[:280],
                "summary": self._build_artifact_summary(chunk)[:200],
            }
            for index, chunk in enumerate(parsed_artifact.chunk_texts)
        ]
        evidence_chunk_refs = artifact_chunk_refs + list(grounding.get("historical_evidence_refs", []))
        grounded_mapping_payloads = list(grounding.get("mapping_payloads", []))
        grounded_mapping_payloads.extend(mapping_payloads)
        preferred_evidence_id = self._stable_import_evidence_id(
            cycle_id=cycle_id,
            evidence_source_id=str(payload["evidence_source_id"]),
        )
        cycle_command = CycleProcessingCommand(
            workflow_run_id=workflow_run_id,
            audit_cycle_id=cycle_id,
            source_id=str(payload["evidence_source_id"]),
            source_type=source_type,
            artifact_id=parsed_artifact.raw_artifact_id,
            extracted_text_or_summary=extracted_text_or_summary,
            allowed_evidence_types=allowed_evidence_types,
            evidence_item_id=preferred_evidence_id,
            evidence_chunk_refs=evidence_chunk_refs,
            in_scope_controls=list(grounding.get("in_scope_controls", [])),
            framework_name=str(grounding.get("framework_name", "SOC2")),
            mapping_payloads=grounded_mapping_payloads,
            mapping_memory_context=list(grounding.get("mapping_memory_context", [])),
            challenge_memory_context=list(grounding.get("challenge_memory_context", [])),
            freshness_policy=dict(grounding.get("freshness_policy", {"mode": "standard"})),
            control_text=str(grounding.get("control_text") or control_text),
            organization_id=cycle_scope["organization_id"],
            workspace_id=cycle_scope["workspace_id"],
        )
        _response, run_result, _resolved_cycle_scope = self._execute_cycle_processing(
            cycle_command,
            organization_id=cycle_scope["organization_id"],
            auth_context=payload,
        )
        normalization_output = self._step_structured_output(run_result, "normalization")
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
                self._coerce_datetime(payload["captured_at"])
                if payload.get("captured_at")
                else None
            ),
            preferred_evidence_id=preferred_evidence_id,
            preferred_title=str(display_name or normalization_output.get("normalized_title") or ""),
            preferred_evidence_type=str(normalization_output.get("evidence_type") or evidence_type),
            preferred_summary=str(parsed_artifact.summary or normalization_output.get("summary") or ""),
            preferred_captured_at=(
                self._coerce_datetime(normalization_output.get("captured_at"))
                if normalization_output.get("captured_at") is not None
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
        elif artifact_format == "docx":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_docx_artifact(
                display_name=display_name,
                artifact_bytes=artifact_bytes or b"",
            )
        elif artifact_format == "xlsx":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_xlsx_artifact(
                display_name=display_name,
                artifact_bytes=artifact_bytes or b"",
            )
        elif artifact_format == "zip":
            normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_zip_artifact(
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
        openxml_format = AuditFlowAppService._detect_openxml_artifact_format(artifact_bytes)
        if openxml_format is not None:
            return openxml_format
        locator_candidates = [
            value.split("?", maxsplit=1)[0].lower()
            for value in (source_locator, artifact_id)
            if value is not None
        ]
        if any(candidate.endswith(".pdf") for candidate in locator_candidates) or artifact_bytes.startswith(b"%PDF-"):
            return "pdf"
        if any(candidate.endswith(".docx") for candidate in locator_candidates):
            return "docx"
        if any(candidate.endswith(".xlsx") for candidate in locator_candidates):
            return "xlsx"
        if any(candidate.endswith(".zip") for candidate in locator_candidates):
            return "zip"
        if any(candidate.endswith(".png") for candidate in locator_candidates) or artifact_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png"
        if any(candidate.endswith((".jpg", ".jpeg")) for candidate in locator_candidates) or artifact_bytes.startswith(b"\xff\xd8"):
            return "jpeg"
        if any(candidate.endswith((".gif", ".bmp", ".webp")) for candidate in locator_candidates):
            return "image"
        if zipfile.is_zipfile(io.BytesIO(artifact_bytes)):
            return "zip"
        return "binary"

    @staticmethod
    def _detect_openxml_artifact_format(artifact_bytes: bytes) -> str | None:
        try:
            with zipfile.ZipFile(io.BytesIO(artifact_bytes)) as archive:
                names = set(archive.namelist())
        except zipfile.BadZipFile:
            return None
        if "word/document.xml" in names:
            return "docx"
        if any(name.startswith("xl/worksheets/") and name.endswith(".xml") for name in names):
            return "xlsx"
        return None

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
    def _parse_docx_artifact(
        *,
        display_name: str,
        artifact_bytes: bytes,
    ) -> tuple[str, list[str], dict[str, object]]:
        paragraphs = AuditFlowAppService._extract_docx_paragraphs(artifact_bytes)
        extracted_text = "\n\n".join(paragraphs).strip()
        if not extracted_text:
            extracted_text = (
                f"{display_name}\n"
                "DOCX document imported, but no extractable text was found. Manual reviewer follow-up is required."
            )
        normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
            source_type="upload",
            display_name=display_name,
            raw_text=extracted_text,
        )
        parser_metadata.update(
            {
                "source_format": "docx",
                "parser_kind": "docx_xml_extract",
                "byte_size": len(artifact_bytes),
                "extraction_method": "openxml_document_extract",
                "section_count": len(paragraphs) if paragraphs else 1,
            }
        )
        return f"DOCX import: {display_name}\n\n{normalized_text}", chunk_texts, parser_metadata

    @staticmethod
    def _parse_xlsx_artifact(
        *,
        display_name: str,
        artifact_bytes: bytes,
    ) -> tuple[str, list[str], dict[str, object]]:
        worksheet_chunks = AuditFlowAppService._extract_xlsx_rows(artifact_bytes)
        extracted_text = "\n\n".join(worksheet_chunks).strip()
        if not extracted_text:
            extracted_text = (
                f"{display_name}\n"
                "XLSX workbook imported, but no extractable cell text was found. Manual reviewer follow-up is required."
            )
        normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
            source_type="upload",
            display_name=display_name,
            raw_text=extracted_text,
        )
        parser_metadata.update(
            {
                "source_format": "xlsx",
                "parser_kind": "xlsx_xml_extract",
                "byte_size": len(artifact_bytes),
                "extraction_method": "openxml_spreadsheet_extract",
                "worksheet_count": len(
                    {
                        chunk.split("\n", maxsplit=1)[0].split(" row ", maxsplit=1)[0]
                        for chunk in worksheet_chunks
                    }
                )
                if worksheet_chunks
                else 1,
                "row_count": len(worksheet_chunks) if worksheet_chunks else 1,
            }
        )
        return f"XLSX import: {display_name}\n\n{normalized_text}", chunk_texts, parser_metadata

    @staticmethod
    def _parse_zip_artifact(
        *,
        display_name: str,
        artifact_bytes: bytes,
    ) -> tuple[str, list[str], dict[str, object]]:
        entry_sections = AuditFlowAppService._extract_zip_entry_sections(artifact_bytes)
        extracted_text = "\n\n".join(entry_sections).strip()
        if not extracted_text:
            extracted_text = (
                f"{display_name}\n"
                "ZIP archive imported, but no extractable text entries were found. Manual reviewer follow-up is required."
            )
        normalized_text, chunk_texts, parser_metadata = AuditFlowAppService._parse_text_artifact(
            source_type="upload",
            display_name=display_name,
            raw_text=extracted_text,
        )
        parser_metadata.update(
            {
                "source_format": "zip",
                "parser_kind": "zip_entry_extract",
                "byte_size": len(artifact_bytes),
                "extraction_method": "archive_entry_text_extract",
                "entry_count": len(entry_sections) if entry_sections else 1,
            }
        )
        return f"ZIP import: {display_name}\n\n{normalized_text}", chunk_texts, parser_metadata

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

    @staticmethod
    def _extract_docx_paragraphs(artifact_bytes: bytes) -> list[str]:
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        try:
            with zipfile.ZipFile(io.BytesIO(artifact_bytes)) as archive:
                entries = [
                    name
                    for name in archive.namelist()
                    if name.startswith("word/") and name.endswith(".xml")
                ]
                paragraphs: list[str] = []
                for entry in sorted(entries):
                    if not any(token in entry for token in ("document.xml", "header", "footer")):
                        continue
                    root = ET.fromstring(archive.read(entry))
                    for paragraph in root.findall(".//w:p", ns):
                        fragments = [
                            fragment.text.strip()
                            for fragment in paragraph.findall(".//w:t", ns)
                            if fragment.text and fragment.text.strip()
                        ]
                        if fragments:
                            paragraphs.append("".join(fragments))
        except (zipfile.BadZipFile, KeyError, ET.ParseError):
            return AuditFlowAppService._extract_binary_text_candidates(artifact_bytes)
        return AuditFlowAppService._dedupe_text_sections(paragraphs)

    @staticmethod
    def _extract_xlsx_rows(artifact_bytes: bytes) -> list[str]:
        ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        try:
            with zipfile.ZipFile(io.BytesIO(artifact_bytes)) as archive:
                shared_strings = AuditFlowAppService._extract_xlsx_shared_strings(archive)
                sheet_entries = [
                    name
                    for name in archive.namelist()
                    if name.startswith("xl/worksheets/") and name.endswith(".xml")
                ]
                row_chunks: list[str] = []
                for entry in sorted(sheet_entries):
                    sheet_name = entry.rsplit("/", maxsplit=1)[-1].removesuffix(".xml")
                    root = ET.fromstring(archive.read(entry))
                    for row in root.findall(".//x:sheetData/x:row", ns):
                        row_number = row.attrib.get("r", "?")
                        cell_lines: list[str] = []
                        for cell in row.findall("x:c", ns):
                            cell_ref = cell.attrib.get("r", "")
                            cell_value = AuditFlowAppService._extract_xlsx_cell_value(
                                cell,
                                shared_strings=shared_strings,
                            )
                            if cell_value:
                                cell_lines.append(f"{cell_ref}: {cell_value}")
                        if cell_lines:
                            row_chunks.append(f"{sheet_name} row {row_number}\n" + "\n".join(cell_lines))
        except (zipfile.BadZipFile, KeyError, ET.ParseError, ValueError):
            return AuditFlowAppService._extract_binary_text_candidates(artifact_bytes)
        return AuditFlowAppService._dedupe_text_sections(row_chunks)

    @staticmethod
    def _extract_xlsx_shared_strings(archive: zipfile.ZipFile) -> list[str]:
        ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        if "xl/sharedStrings.xml" not in archive.namelist():
            return []
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        shared_strings: list[str] = []
        for item in root.findall(".//x:si", ns):
            fragments = [
                fragment.text.strip()
                for fragment in item.findall(".//x:t", ns)
                if fragment.text and fragment.text.strip()
            ]
            shared_strings.append("".join(fragments))
        return shared_strings

    @staticmethod
    def _extract_xlsx_cell_value(cell: ET.Element, *, shared_strings: list[str]) -> str:
        ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        cell_type = cell.attrib.get("t")
        if cell_type == "inlineStr":
            fragments = [
                fragment.text.strip()
                for fragment in cell.findall(".//x:t", ns)
                if fragment.text and fragment.text.strip()
            ]
            return "".join(fragments)
        value_node = cell.find("x:v", ns)
        value = value_node.text.strip() if value_node is not None and value_node.text is not None else ""
        if not value:
            return ""
        if cell_type == "s":
            index = int(value)
            return shared_strings[index] if 0 <= index < len(shared_strings) else value
        return value

    @staticmethod
    def _extract_zip_entry_sections(artifact_bytes: bytes) -> list[str]:
        try:
            with zipfile.ZipFile(io.BytesIO(artifact_bytes)) as archive:
                entry_sections: list[str] = []
                for entry in sorted(archive.namelist()):
                    if entry.endswith("/"):
                        continue
                    if not AuditFlowAppService._is_textual_archive_entry(entry):
                        continue
                    raw_entry_bytes = archive.read(entry)
                    entry_text = raw_entry_bytes.decode("utf-8", errors="ignore").strip()
                    if not entry_text:
                        continue
                    artifact_format = AuditFlowAppService._detect_artifact_format(
                        artifact_id=entry,
                        source_locator=entry,
                        raw_text=entry_text,
                    )
                    if artifact_format == "csv":
                        normalized_text, _chunk_texts, _metadata = AuditFlowAppService._parse_csv_artifact(
                            display_name=entry,
                            raw_text=entry_text,
                        )
                    elif artifact_format == "json":
                        normalized_text, _chunk_texts, _metadata = AuditFlowAppService._parse_json_artifact(
                            display_name=entry,
                            raw_text=entry_text,
                        )
                    elif artifact_format == "markdown":
                        normalized_text, _chunk_texts, _metadata = AuditFlowAppService._parse_markdown_artifact(
                            display_name=entry,
                            raw_text=entry_text,
                        )
                    elif artifact_format == "html":
                        normalized_text, _chunk_texts, _metadata = AuditFlowAppService._parse_html_artifact(
                            display_name=entry,
                            raw_text=entry_text,
                        )
                    else:
                        normalized_text, _chunk_texts, _metadata = AuditFlowAppService._parse_text_artifact(
                            source_type="upload",
                            display_name=entry,
                            raw_text=entry_text,
                        )
                    entry_sections.append(f"{entry}\n{normalized_text}".strip())
        except (zipfile.BadZipFile, KeyError, ET.ParseError):
            return AuditFlowAppService._extract_binary_text_candidates(artifact_bytes)
        return AuditFlowAppService._dedupe_text_sections(entry_sections)

    @staticmethod
    def _is_textual_archive_entry(entry_name: str) -> bool:
        normalized = entry_name.lower()
        return normalized.endswith(
            (
                ".txt",
                ".md",
                ".markdown",
                ".json",
                ".csv",
                ".html",
                ".htm",
                ".log",
                ".yaml",
                ".yml",
            )
        )
