from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class AuditFlowModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AuditWorkspaceSummary(AuditFlowModel):
    workspace_id: str
    workspace_name: str
    framework_name: str
    workspace_status: str


class CreateWorkspaceCommand(AuditFlowModel):
    workspace_name: str = Field(min_length=1)
    framework_name: str = "SOC2"
    workspace_status: Literal["active"] = "active"


class AuditCycleSummary(AuditFlowModel):
    cycle_id: str
    workspace_id: str
    cycle_name: str
    cycle_status: str
    framework_name: str
    coverage_status: str
    review_queue_count: int
    open_gap_count: int
    latest_workflow_run_id: str | None = None


class CreateCycleCommand(AuditFlowModel):
    workspace_id: str
    cycle_name: str = Field(min_length=1)
    framework_name: str = "SOC2"
    cycle_status: Literal["draft"] = "draft"


class ControlCoverageSummary(AuditFlowModel):
    control_state_id: str
    control_code: str
    coverage_status: str
    mapped_evidence_count: int
    open_gap_count: int


class MappingSummary(AuditFlowModel):
    mapping_id: str
    control_state_id: str
    control_code: str
    mapping_status: str
    evidence_item_id: str
    rationale_summary: str
    citation_refs: list[dict[str, Any]] = Field(default_factory=list)
    updated_at: datetime


class ReviewQueueItem(AuditFlowModel):
    mapping_id: str
    control_state_id: str
    control_code: str
    coverage_status: str
    evidence_item_id: str
    rationale_summary: str
    citation_refs: list[dict[str, Any]] = Field(default_factory=list)
    updated_at: datetime


class ReviewQueueResponse(AuditFlowModel):
    cycle_id: str
    total_count: int
    items: list[ReviewQueueItem] = Field(default_factory=list)


class ReviewDecisionSummary(AuditFlowModel):
    review_decision_id: str
    cycle_id: str
    mapping_id: str | None = None
    gap_id: str | None = None
    decision: str
    from_status: str | None = None
    to_status: str | None = None
    reviewer_id: str
    comment: str | None = None
    feedback_tags: list[str] = Field(default_factory=list)
    created_at: datetime


class ReviewDecisionListResponse(AuditFlowModel):
    cycle_id: str
    total_count: int
    items: list[ReviewDecisionSummary] = Field(default_factory=list)


class EvidenceImportSummary(AuditFlowModel):
    evidence_source_id: str
    cycle_id: str
    source_type: str
    display_name: str
    ingest_status: str
    latest_workflow_run_id: str | None = None
    artifact_id: str | None = None
    connection_id: str | None = None
    upstream_object_id: str | None = None
    source_locator: str | None = None
    captured_at: datetime | None = None
    last_synced_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ImportListResponse(AuditFlowModel):
    cycle_id: str
    total_count: int
    items: list[EvidenceImportSummary] = Field(default_factory=list)


class GapSummary(AuditFlowModel):
    gap_id: str
    control_state_id: str
    gap_type: str
    severity: str
    status: str
    title: str
    recommended_action: str
    updated_at: datetime


class EvidenceChunk(AuditFlowModel):
    chunk_id: str
    chunk_index: int
    section_label: str | None = None
    text_excerpt: str


class EvidenceDetail(AuditFlowModel):
    evidence_id: str
    audit_cycle_id: str
    title: str
    evidence_type: str
    parse_status: str
    captured_at: datetime
    summary: str
    source: dict[str, Any]
    chunks: list[EvidenceChunk] = Field(default_factory=list)


class NarrativeSummary(AuditFlowModel):
    narrative_id: str
    narrative_type: str
    status: str
    control_state_id: str
    snapshot_version: int
    content_markdown: str


class ExportPackageSummary(AuditFlowModel):
    package_id: str
    cycle_id: str
    snapshot_version: int
    status: str
    artifact_id: str | None = None
    workflow_run_id: str | None = None
    created_at: datetime


class AuditCycleDashboardResponse(AuditFlowModel):
    cycle: AuditCycleSummary
    review_queue_count: int
    open_gap_count: int
    accepted_mapping_count: int
    export_ready: bool
    controls: list[ControlCoverageSummary] = Field(default_factory=list)
    latest_export_package: ExportPackageSummary | None = None


class ControlDetailResponse(AuditFlowModel):
    control_state: ControlCoverageSummary
    accepted_mappings: list[MappingSummary] = Field(default_factory=list)
    pending_mappings: list[MappingSummary] = Field(default_factory=list)
    open_gaps: list[GapSummary] = Field(default_factory=list)


class MappingReviewCommand(AuditFlowModel):
    decision: Literal["accept", "reject", "reassign"]
    comment: str = ""
    target_control_id: str | None = None
    expected_updated_at: datetime | None = None


class MappingReviewResponse(AuditFlowModel):
    mapping_id: str
    mapping_status: str
    control_state: ControlCoverageSummary


class GapDecisionCommand(AuditFlowModel):
    decision: Literal["resolve_gap", "reopen_gap", "acknowledge"]
    comment: str = ""
    expected_updated_at: datetime | None = None


class UploadImportCommand(AuditFlowModel):
    workflow_run_id: str | None = None
    artifact_id: str
    display_name: str
    captured_at: datetime | None = None
    evidence_type_hint: str | None = None
    source_locator: str | None = None
    artifact_text: str | None = None
    organization_id: str = "org-1"
    workspace_id: str = "ws-1"


class ExternalImportCommand(AuditFlowModel):
    workflow_run_id: str | None = None
    connection_id: str
    provider: Literal["jira", "confluence"]
    upstream_ids: list[str] = Field(default_factory=list)
    query: str | None = None
    organization_id: str = "org-1"
    workspace_id: str = "ws-1"

    @model_validator(mode="after")
    def validate_selector(self) -> "ExternalImportCommand":
        has_upstream_ids = len(self.upstream_ids) > 0
        has_query = self.query is not None and self.query != ""
        if has_upstream_ids == has_query:
            raise ValueError("Exactly one of upstream_ids or query must be supplied")
        return self


class ImportAcceptedResponse(AuditFlowModel):
    workflow_run_id: str
    accepted_count: int
    evidence_source_ids: list[str] = Field(default_factory=list)
    artifact_id: str | None = None
    ingest_status: str


class ImportDispatchResponse(AuditFlowModel):
    attempted_count: int = 0
    dispatched_count: int = 0
    failed_event_ids: list[str] = Field(default_factory=list)


class CycleProcessingCommand(AuditFlowModel):
    workflow_run_id: str
    audit_cycle_id: str
    audit_workspace_id: str = "audit-ws-1"
    source_id: str
    source_type: str = "upload"
    artifact_id: str
    extracted_text_or_summary: str
    allowed_evidence_types: list[str] = Field(default_factory=lambda: ["ticket"])
    evidence_item_id: str = "evidence-1"
    evidence_chunk_refs: list[dict[str, Any]] = Field(default_factory=list)
    in_scope_controls: list[str] = Field(default_factory=list)
    framework_name: str = "SOC2"
    mapping_payloads: list[dict[str, Any]] = Field(default_factory=list)
    control_text: str = ""
    organization_id: str = "org-1"
    workspace_id: str = "ws-1"
    state_overrides: dict[str, Any] = Field(default_factory=dict)


class ExportGenerationCommand(AuditFlowModel):
    workflow_run_id: str
    audit_cycle_id: str
    audit_workspace_id: str = "audit-ws-1"
    working_snapshot_version: int
    accepted_mapping_refs: list[str] = Field(default_factory=list)
    open_gap_refs: list[str] = Field(default_factory=list)
    export_scope: str = "cycle_package"
    organization_id: str = "org-1"
    workspace_id: str = "ws-1"
    state_overrides: dict[str, Any] = Field(default_factory=dict)


class ExportCreateCommand(AuditFlowModel):
    workflow_run_id: str
    snapshot_version: int
    format: str = "zip"
    organization_id: str = "org-1"
    workspace_id: str = "ws-1"


class AuditFlowRunResponse(AuditFlowModel):
    workflow_name: Literal["auditflow_cycle_processing", "auditflow_export_generation"]
    workflow_run_id: str
    workflow_type: str
    current_state: str
    checkpoint_seq: int
    emitted_events: list[str] = Field(default_factory=list)


class AuditFlowWorkflowStateResponse(AuditFlowModel):
    workflow_run_id: str
    workflow_type: str
    current_state: str
    checkpoint_seq: int
    raw_state: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(AuditFlowModel):
    status: Literal["ok"]
    product: Literal["auditflow"]
