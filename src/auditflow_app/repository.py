from __future__ import annotations

from collections import defaultdict
import hashlib
import importlib.util
import json
import re
from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Date, DateTime, Float, Integer, String, Text, TypeDecorator, UniqueConstraint, and_, or_, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .api_models import (
    AuditCycleDashboardResponse,
    AuditCycleSummary,
    AuditWorkspaceSummary,
    ControlCoverageSummary,
    ControlDetailResponse,
    CreateCycleCommand,
    CreateWorkspaceCommand,
    EvidenceChunk,
    EvidenceDetail,
    EvidenceImportSummary,
    EvidenceSearchItem,
    EvidenceSearchResponse,
    ExternalImportCommand,
    ExportPackageSummary,
    GapDecisionCommand,
    GapSummary,
    ImportAcceptedResponse,
    ImportListResponse,
    MappingClaimCommand,
    MappingClaimResponse,
    MappingClaimReleaseCommand,
    MappingListResponse,
    MappingReviewCommand,
    MappingReviewResponse,
    MappingSummary,
    MemoryRecordListResponse,
    MemoryRecordSummary,
    NarrativeSummary,
    ReviewDecisionListResponse,
    ReviewDecisionSummary,
    ReviewQueueItem,
    ReviewQueueResponse,
    ToolAccessAuditListResponse,
    ToolAccessAuditSummary,
    ToolAccessSummary,
    UploadImportCommand,
)
from .shared_runtime import load_shared_agent_platform


CONTROL_CATALOG_SEEDS = {
    "SOC2": (
        {
            "control_code": "CC6.1",
            "domain_name": "access_control",
            "title": "Access permissions are scoped and reviewed.",
            "description": "Provisioning and periodic access reviews should demonstrate who can reach in-scope systems.",
            "guidance_markdown": "Prefer review exports, approval tickets, and joiner/mover/leaver evidence.",
            "common_evidence_payload": [{"kind": "access_review_report"}, {"kind": "ticket"}],
            "sort_order": 10,
        },
        {
            "control_code": "CC6.2",
            "domain_name": "identity_lifecycle",
            "title": "Identity lifecycle changes are approved and recorded.",
            "description": "Joiner, mover, and leaver changes should be approved and leave an auditable trail.",
            "guidance_markdown": "Look for termination tickets, access removal logs, and manager approvals.",
            "common_evidence_payload": [{"kind": "ticket"}, {"kind": "change_log"}],
            "sort_order": 20,
        },
        {
            "control_code": "CC7.2",
            "domain_name": "monitoring",
            "title": "Security-relevant events are monitored and triaged.",
            "description": "Alerting and triage workflows should show detection, investigation, and response coverage.",
            "guidance_markdown": "Useful evidence includes alerts, incident tickets, and review notes.",
            "common_evidence_payload": [{"kind": "alert"}, {"kind": "incident_ticket"}],
            "sort_order": 30,
        },
        {
            "control_code": "CC8.1",
            "domain_name": "change_management",
            "title": "Changes are reviewed before production release.",
            "description": "Production changes should retain approval, deployment, and rollback evidence.",
            "guidance_markdown": "Capture deployment approvals, release notes, and rollback records.",
            "common_evidence_payload": [{"kind": "deployment_record"}, {"kind": "approval"}],
            "sort_order": 40,
        },
    ),
}

SEEDED_CONTROL_STATE_IDS = {
    "CC6.1": "control-state-1",
    "CC6.2": "control-state-2",
    "CC7.2": "control-state-3",
    "CC8.1": "control-state-4",
}

DEFAULT_REVIEWER_ID = "reviewer-demo"
DEFAULT_ORGANIZATION_ID = "org-1"
DEFAULT_REVIEW_CLAIM_LEASE_SECONDS = 900
LEXICAL_MODEL_NAME = "lexical-v1"
SEMANTIC_MODEL_NAME = "semantic-v1"
EMBEDDING_VECTOR_DIMENSION = 96
SEMANTIC_SYNONYMS = {
    "access": ("permission", "permissions", "privilege", "privileged", "entitlement", "entitlements"),
    "approval": ("approve", "approved", "signoff", "authorized"),
    "change": ("deploy", "deployment", "release"),
    "evidence": ("artifact", "record", "proof"),
    "incident": ("alert", "case", "finding"),
    "joiner": ("onboarding", "hire"),
    "leaver": ("offboarding", "termination"),
    "review": ("attestation", "certification", "recertification"),
    "ticket": ("issue", "request", "task"),
}


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return normalized.strip("-") or f"audit-workspace-{uuid4().hex[:8]}"


def _pgvector_package_available() -> bool:
    return importlib.util.find_spec("pgvector") is not None


def _configure_pgvector_dialect(engine: Engine) -> None:
    ready = False
    if engine.dialect.name == "postgresql" and _pgvector_package_available():
        try:
            with engine.begin() as connection:
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            ready = True
        except Exception:
            ready = False
    setattr(engine.dialect, "_auditflow_pgvector_ready", ready)


class NativeVectorType(TypeDecorator):
    impl = JSON
    cache_ok = True

    def __init__(self, dimension: int) -> None:
        super().__init__()
        self.dimension = dimension

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql" and getattr(dialect, "_auditflow_pgvector_ready", False):
            from pgvector.sqlalchemy import Vector

            return dialect.type_descriptor(Vector(self.dimension))
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return [float(item) for item in value]

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return None
        if isinstance(value, tuple):
            value = list(value)
        if not isinstance(value, list):
            return None
        return [float(item) for item in value]


class AuditFlowRepository(Protocol):
    def create_workspace(
        self,
        command: CreateWorkspaceCommand,
        *,
        organization_id: str | None = None,
    ) -> AuditWorkspaceSummary: ...

    def get_workspace(
        self,
        workspace_id: str,
        *,
        organization_id: str | None = None,
    ) -> AuditWorkspaceSummary: ...

    def create_cycle(
        self,
        command: CreateCycleCommand,
        *,
        organization_id: str | None = None,
    ) -> AuditCycleSummary: ...

    def list_cycles(
        self,
        workspace_id: str,
        *,
        status: str | None = None,
        organization_id: str | None = None,
    ) -> list[AuditCycleSummary]: ...

    def get_cycle_dashboard(
        self,
        cycle_id: str,
        *,
        organization_id: str | None = None,
    ) -> AuditCycleDashboardResponse: ...

    def get_cycle_context(
        self,
        cycle_id: str,
        *,
        organization_id: str | None = None,
    ) -> dict[str, str]: ...

    def describe_embedding_capability(self) -> dict[str, object]: ...

    def describe_vector_search_capability(self) -> dict[str, object]: ...

    def list_controls(
        self,
        cycle_id: str,
        *,
        coverage_status: str | None = None,
        search: str | None = None,
        organization_id: str | None = None,
    ) -> list[ControlCoverageSummary]: ...

    def list_mappings(
        self,
        cycle_id: str,
        *,
        control_state_id: str | None = None,
        mapping_status: str | None = None,
        organization_id: str | None = None,
    ) -> MappingListResponse: ...

    def get_control_detail(
        self,
        control_state_id: str,
        *,
        organization_id: str | None = None,
    ) -> ControlDetailResponse: ...

    def get_evidence(
        self,
        evidence_id: str,
        *,
        organization_id: str | None = None,
    ) -> EvidenceDetail: ...

    def search_evidence(
        self,
        *,
        cycle_id: str,
        query: str,
        limit: int = 5,
        organization_id: str | None = None,
        workspace_id: str | None = None,
    ) -> EvidenceSearchResponse: ...

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
    ) -> MemoryRecordListResponse: ...

    def build_cycle_processing_grounding(
        self,
        *,
        cycle_id: str,
        evidence_summary: str,
        chunk_texts: list[str],
        max_historical_hits: int = 3,
        max_memory_items: int = 5,
        organization_id: str | None = None,
    ) -> dict[str, object]: ...

    def list_gaps(
        self,
        cycle_id: str,
        *,
        status: str | None = None,
        severity: str | None = None,
        organization_id: str | None = None,
    ) -> list[GapSummary]: ...

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
    ) -> ReviewQueueResponse: ...

    def list_review_decisions(
        self,
        cycle_id: str,
        *,
        mapping_id: str | None = None,
        gap_id: str | None = None,
        organization_id: str | None = None,
    ) -> ReviewDecisionListResponse: ...

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
    ) -> ToolAccessAuditListResponse: ...

    def list_cycle_tool_access_audit(
        self,
        cycle_id: str,
        *,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        organization_id: str | None = None,
    ) -> ToolAccessAuditListResponse: ...

    def list_control_tool_access_audit(
        self,
        control_state_id: str,
        *,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        organization_id: str | None = None,
    ) -> ToolAccessAuditListResponse: ...

    def list_mapping_tool_access_audit(
        self,
        mapping_id: str,
        *,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        organization_id: str | None = None,
    ) -> ToolAccessAuditListResponse: ...

    def get_mapping_event_context(
        self,
        mapping_id: str,
        *,
        organization_id: str | None = None,
    ) -> dict[str, str]: ...

    def get_gap_event_context(
        self,
        gap_id: str,
        *,
        organization_id: str | None = None,
    ) -> dict[str, str]: ...

    def review_mapping(
        self,
        mapping_id: str,
        command: MappingReviewCommand,
        *,
        organization_id: str | None = None,
        reviewer_id: str | None = None,
    ) -> MappingReviewResponse: ...

    def claim_mapping(
        self,
        mapping_id: str,
        command: MappingClaimCommand,
        *,
        organization_id: str | None = None,
        reviewer_id: str,
    ) -> MappingClaimResponse: ...

    def release_mapping_claim(
        self,
        mapping_id: str,
        command: MappingClaimReleaseCommand,
        *,
        organization_id: str | None = None,
        reviewer_id: str,
    ) -> MappingClaimResponse: ...

    def list_imports(
        self,
        cycle_id: str,
        *,
        ingest_status: str | None = None,
        source_type: str | None = None,
        organization_id: str | None = None,
    ) -> ImportListResponse: ...

    def create_upload_import(
        self,
        cycle_id: str,
        command: UploadImportCommand,
        *,
        organization_id: str | None = None,
    ) -> ImportAcceptedResponse: ...

    def create_external_import(
        self,
        cycle_id: str,
        command: ExternalImportCommand,
        *,
        organization_id: str | None = None,
    ) -> ImportAcceptedResponse: ...

    def decide_gap(
        self,
        gap_id: str,
        command: GapDecisionCommand,
        *,
        organization_id: str | None = None,
        reviewer_id: str | None = None,
    ) -> GapSummary: ...

    def complete_import_processing(
        self,
        *,
        cycle_id: str,
        evidence_source_id: str,
        workflow_run_id: str,
        title: str,
        evidence_type: str,
        summary: str,
        artifact_id: str | None,
        normalized_artifact_id: str | None,
        source_locator: str | None,
        captured_at: datetime | None,
        preferred_evidence_id: str | None = None,
        preferred_title: str | None = None,
        preferred_evidence_type: str | None = None,
        preferred_summary: str | None = None,
        preferred_captured_at: datetime | None = None,
        chunk_texts: list[str] | None = None,
        metadata_update: dict[str, object] | None = None,
    ) -> None: ...

    def upsert_artifact_blob(
        self,
        *,
        artifact_id: str,
        artifact_type: str,
        content_text: str,
        metadata_payload: dict[str, object] | None = None,
    ) -> None: ...

    def list_narratives(
        self,
        cycle_id: str,
        *,
        snapshot_version: int | None = None,
        narrative_type: str | None = None,
        organization_id: str | None = None,
    ) -> list[NarrativeSummary]: ...

    def read_snapshot_refs(
        self,
        cycle_id: str,
        *,
        working_snapshot_version: int,
        organization_id: str | None = None,
    ) -> dict[str, list[str]]: ...

    def list_export_packages(
        self,
        cycle_id: str,
        *,
        snapshot_version: int | None = None,
        status: str | None = None,
        organization_id: str | None = None,
    ) -> list[ExportPackageSummary]: ...

    def get_export_package(
        self,
        package_id: str,
        *,
        organization_id: str | None = None,
    ) -> ExportPackageSummary: ...

    def record_cycle_processing_result(
        self,
        cycle_id: str,
        workflow_run_id: str,
        checkpoint_seq: int,
        *,
        organization_id: str | None = None,
        evidence_item_id: str | None = None,
        mapping_output: dict[str, object] | None = None,
        challenge_output: dict[str, object] | None = None,
        mapping_payloads: list[dict[str, object]] | None = None,
    ) -> None: ...

    def record_export_result(
        self,
        *,
        cycle_id: str,
        workflow_run_id: str,
        snapshot_version: int,
        checkpoint_seq: int,
        organization_id: str | None = None,
        writer_output: dict[str, object] | None = None,
        narrative_ids: list[str] | None = None,
    ) -> ExportPackageSummary: ...

    def load_idempotency_response(
        self,
        *,
        operation: str,
        idempotency_key: str,
        request_hash: str,
    ) -> dict[str, object] | None: ...

    def store_idempotency_response(
        self,
        *,
        operation: str,
        idempotency_key: str,
        request_hash: str,
        response_payload: dict[str, object],
    ) -> None: ...

    def record_tool_access(
        self,
        *,
        tool_call_id: str,
        workflow_run_id: str,
        node_name: str | None,
        tool_name: str,
        tool_version: str,
        adapter_type: str,
        subject_type: str,
        subject_id: str,
        organization_id: str,
        workspace_id: str | None,
        user_id: str | None,
        role: str | None,
        session_id: str | None,
        connection_id: str | None,
        execution_status: str,
        error_code: str | None,
        arguments_payload: dict[str, object],
        source_locator: str | None,
        recorded_at: datetime,
        completed_at: datetime,
    ) -> None: ...


class Base(DeclarativeBase):
    pass


class AuditWorkspaceRow(Base):
    __tablename__ = "auditflow_workspace"
    __table_args__ = (
        UniqueConstraint("organization_id", "slug", name="uq_auditflow_workspace_org_slug"),
    )

    workspace_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    workspace_name: Mapped[str] = mapped_column(String(255))
    slug: Mapped[str] = mapped_column(String(255), index=True)
    framework_name: Mapped[str] = mapped_column(String(50))
    workspace_status: Mapped[str] = mapped_column(String(50))
    default_owner_user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    settings_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class AuditCycleRow(Base):
    __tablename__ = "auditflow_cycle"

    cycle_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    workspace_id: Mapped[str] = mapped_column(String(255), index=True)
    cycle_name: Mapped[str] = mapped_column(String(255))
    cycle_status: Mapped[str] = mapped_column(String(50))
    framework_name: Mapped[str] = mapped_column(String(50))
    audit_period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    audit_period_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    owner_user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    current_snapshot_version: Mapped[int] = mapped_column(Integer, default=0)
    last_mapped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    last_reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    coverage_status: Mapped[str] = mapped_column(String(50))
    review_queue_count: Mapped[int] = mapped_column(Integer, default=0)
    open_gap_count: Mapped[int] = mapped_column(Integer, default=0)
    latest_workflow_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class ControlCatalogRow(Base):
    __tablename__ = "auditflow_control_catalog"
    __table_args__ = (
        UniqueConstraint(
            "framework_name",
            "control_code",
            name="ux_auditflow_control_catalog_framework_code",
        ),
    )

    control_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    framework_name: Mapped[str] = mapped_column(String(50), index=True)
    control_code: Mapped[str] = mapped_column(String(100))
    domain_name: Mapped[str] = mapped_column(String(100))
    title: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(Text)
    guidance_markdown: Mapped[str | None] = mapped_column(Text, nullable=True)
    common_evidence_payload: Mapped[list[dict]] = mapped_column(JSON)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    sort_order: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class ControlCoverageRow(Base):
    __tablename__ = "auditflow_control_coverage"

    control_state_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    control_code: Mapped[str] = mapped_column(String(100))
    coverage_status: Mapped[str] = mapped_column(String(50))
    mapped_evidence_count: Mapped[int] = mapped_column(Integer, default=0)
    open_gap_count: Mapped[int] = mapped_column(Integer, default=0)


class EvidenceSourceRow(Base):
    __tablename__ = "auditflow_evidence_source"

    evidence_source_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    source_type: Mapped[str] = mapped_column(String(50))
    connection_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    artifact_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    upstream_object_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_locator: Mapped[str | None] = mapped_column(Text, nullable=True)
    display_name: Mapped[str] = mapped_column(String(255))
    ingest_status: Mapped[str] = mapped_column(String(50))
    latest_workflow_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    captured_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    metadata_payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class MappingRow(Base):
    __tablename__ = "auditflow_mapping"

    mapping_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    control_state_id: Mapped[str] = mapped_column(String(255), index=True)
    control_code: Mapped[str] = mapped_column(String(100))
    mapping_status: Mapped[str] = mapped_column(String(50))
    snapshot_version: Mapped[int] = mapped_column(Integer, default=1)
    evidence_item_id: Mapped[str] = mapped_column(String(255))
    rationale_summary: Mapped[str] = mapped_column(Text)
    citation_refs: Mapped[list[dict]] = mapped_column(JSON)
    reviewer_locked: Mapped[bool] = mapped_column(default=False)
    reviewer_claimed_by_user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    reviewer_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    reviewer_claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class GapRow(Base):
    __tablename__ = "auditflow_gap"

    gap_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    control_state_id: Mapped[str] = mapped_column(String(255), index=True)
    gap_type: Mapped[str] = mapped_column(String(100))
    severity: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(50))
    snapshot_version: Mapped[int] = mapped_column(Integer, default=1)
    title: Mapped[str] = mapped_column(String(255))
    recommended_action: Mapped[str] = mapped_column(Text)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class ReviewDecisionRow(Base):
    __tablename__ = "auditflow_review_decision"

    review_decision_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    mapping_id: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    gap_id: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    decision: Mapped[str] = mapped_column(String(50))
    from_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    to_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    reviewer_id: Mapped[str] = mapped_column(String(255))
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    feedback_tags: Mapped[list[str]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class ToolAccessAuditRow(Base):
    __tablename__ = "auditflow_tool_access_audit"
    __table_args__ = (
        UniqueConstraint("tool_call_id", name="uq_auditflow_tool_access_tool_call"),
    )

    tool_access_audit_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    workspace_id: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    workflow_run_id: Mapped[str] = mapped_column(String(255), index=True)
    node_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    tool_call_id: Mapped[str] = mapped_column(String(255), index=True)
    tool_name: Mapped[str] = mapped_column(String(120), index=True)
    tool_version: Mapped[str] = mapped_column(String(50))
    adapter_type: Mapped[str] = mapped_column(String(80))
    subject_type: Mapped[str] = mapped_column(String(80), index=True)
    subject_id: Mapped[str] = mapped_column(String(255), index=True)
    user_id: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    role: Mapped[str | None] = mapped_column(String(80), nullable=True)
    session_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    connection_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    execution_status: Mapped[str] = mapped_column(String(30), index=True)
    error_code: Mapped[str | None] = mapped_column(String(255), nullable=True)
    arguments_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    source_locator: Mapped[str | None] = mapped_column(Text, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class ArtifactBlobRow(Base):
    __tablename__ = "auditflow_artifact_blob"

    artifact_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    artifact_type: Mapped[str] = mapped_column(String(80))
    content_text: Mapped[str] = mapped_column(Text)
    metadata_payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class IdempotencyKeyRow(Base):
    __tablename__ = "auditflow_idempotency_key"

    record_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    operation: Mapped[str] = mapped_column(String(100), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), index=True)
    request_hash: Mapped[str] = mapped_column(String(128))
    response_payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class EvidenceRow(Base):
    __tablename__ = "auditflow_evidence"

    evidence_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    audit_cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    source_artifact_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_artifact_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    title: Mapped[str] = mapped_column(String(255))
    evidence_type: Mapped[str] = mapped_column(String(50))
    parse_status: Mapped[str] = mapped_column(String(50))
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    summary: Mapped[str] = mapped_column(Text)
    source_payload: Mapped[dict] = mapped_column(JSON)


class EvidenceChunkRow(Base):
    __tablename__ = "auditflow_evidence_chunk"

    chunk_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    evidence_id: Mapped[str] = mapped_column(String(255), index=True)
    chunk_index: Mapped[int] = mapped_column(Integer)
    section_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    text_excerpt: Mapped[str] = mapped_column(Text)


class MemoryRecordRow(Base):
    __tablename__ = "auditflow_memory_record"
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "scope",
            "subject_type",
            "subject_id",
            "memory_key",
            "status",
            name="uq_auditflow_memory_record_scope_subject_key_status",
        ),
    )

    memory_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    workspace_id: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    scope: Mapped[str] = mapped_column(String(40), index=True)
    subject_type: Mapped[str] = mapped_column(String(80), index=True)
    subject_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    memory_key: Mapped[str] = mapped_column(String(120))
    memory_type: Mapped[str] = mapped_column(String(50), index=True)
    value_payload: Mapped[dict] = mapped_column(JSON)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    source_kind: Mapped[str] = mapped_column(String(40))
    source_ref_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String(30), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class EmbeddingChunkRow(Base):
    __tablename__ = "auditflow_embedding_chunk"
    __table_args__ = (
        UniqueConstraint(
            "subject_type",
            "subject_id",
            "chunk_index",
            "model_name",
            name="uq_auditflow_embedding_chunk_subject_chunk_model",
        ),
    )

    embedding_chunk_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    workspace_id: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    subject_type: Mapped[str] = mapped_column(String(80), index=True)
    subject_id: Mapped[str] = mapped_column(String(255), index=True)
    chunk_index: Mapped[int] = mapped_column(Integer)
    text_content: Mapped[str] = mapped_column(Text)
    metadata_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    model_name: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class SemanticVectorRow(Base):
    __tablename__ = "auditflow_semantic_vector"
    __table_args__ = (
        UniqueConstraint(
            "subject_type",
            "subject_id",
            "chunk_index",
            "model_name",
            name="uq_auditflow_semantic_vector_subject_chunk_model",
        ),
    )

    semantic_vector_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    organization_id: Mapped[str] = mapped_column(String(255), index=True)
    workspace_id: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    subject_type: Mapped[str] = mapped_column(String(80), index=True)
    subject_id: Mapped[str] = mapped_column(String(255), index=True)
    chunk_id: Mapped[str] = mapped_column(String(255), index=True)
    chunk_index: Mapped[int] = mapped_column(Integer)
    text_content: Mapped[str] = mapped_column(Text)
    metadata_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    ann_bucket_keys: Mapped[list[str]] = mapped_column(JSON)
    semantic_terms: Mapped[list[str]] = mapped_column(JSON)
    embedding_dimension: Mapped[int] = mapped_column(Integer)
    embedding_vector: Mapped[list[float] | None] = mapped_column(NativeVectorType(EMBEDDING_VECTOR_DIMENSION), nullable=True)
    model_name: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class NarrativeRow(Base):
    __tablename__ = "auditflow_narrative"

    narrative_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    control_state_id: Mapped[str] = mapped_column(String(255), index=True)
    narrative_type: Mapped[str] = mapped_column(String(100))
    snapshot_version: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(50))
    content_markdown: Mapped[str] = mapped_column(Text)


class CycleSnapshotRow(Base):
    __tablename__ = "auditflow_cycle_snapshot"
    __table_args__ = (UniqueConstraint("cycle_id", "snapshot_version", name="uq_auditflow_cycle_snapshot"),)

    snapshot_record_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    snapshot_version: Mapped[int] = mapped_column(Integer)
    snapshot_status: Mapped[str] = mapped_column(String(50))
    trigger_kind: Mapped[str] = mapped_column(String(50))
    workflow_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    review_decision_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    package_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    accepted_mapping_ids_payload: Mapped[list[str]] = mapped_column(JSON, default=list)
    pending_mapping_ids_payload: Mapped[list[str]] = mapped_column(JSON, default=list)
    open_gap_ids_payload: Mapped[list[str]] = mapped_column(JSON, default=list)
    narrative_ids_payload: Mapped[list[str]] = mapped_column(JSON, default=list)
    accepted_mapping_count: Mapped[int] = mapped_column(Integer, default=0)
    review_queue_count: Mapped[int] = mapped_column(Integer, default=0)
    open_gap_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    frozen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class ExportPackageRow(Base):
    __tablename__ = "auditflow_export_package"
    __table_args__ = (UniqueConstraint("cycle_id", "snapshot_version", name="uq_auditflow_export_cycle_snapshot"),)

    package_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    snapshot_version: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(50))
    artifact_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    manifest_artifact_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    workflow_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))
    immutable_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


def create_auditflow_tables(engine: Engine) -> None:
    _configure_pgvector_dialect(engine)
    Base.metadata.create_all(engine)


class SqlAlchemyAuditFlowRepository:
    def __init__(self, session_factory: sessionmaker[Session], engine: Engine) -> None:
        self.session_factory = session_factory
        self.engine = engine
        self._shared_platform = load_shared_agent_platform()
        _configure_pgvector_dialect(engine)
        self.embedding_provider_mode = self._shared_platform.normalize_requested_mode(
            self._env_value("AUDITFLOW_EMBEDDING_PROVIDER"),
            allowed_modes=("auto", "local", "openai"),
            default="auto",
        )
        self.vector_search_requested_mode = self._shared_platform.normalize_requested_mode(
            self._env_value("AUDITFLOW_VECTOR_SEARCH_MODE"),
            allowed_modes=("auto", "ann", "flat", "pgvector"),
            default="auto",
        )
        self.vector_search_mode, self.vector_search_backend_id, self.vector_search_fallback_reason = (
            self._resolve_vector_search_backend()
        )
        self.semantic_candidate_limit = self._resolve_semantic_candidate_limit()
        self.semantic_ann_bucket_count = self._resolve_semantic_ann_bucket_count()
        self.semantic_model_name = self._resolve_semantic_model_name()
        self.semantic_vector_dimension = self._resolve_semantic_vector_dimension()
        self._allow_embedding_fallback = self.embedding_provider_mode == "auto"
        self._embedding_mode_decision = None
        self._openai_embedding_client = self._build_openai_embedding_client()
        if self._openai_embedding_client is None and self.embedding_provider_mode != "openai":
            self.semantic_model_name = SEMANTIC_MODEL_NAME
            self.semantic_vector_dimension = EMBEDDING_VECTOR_DIMENSION
        create_auditflow_tables(engine)
        self.seed_if_empty()
        self.backfill_retrieval_state()
        self.backfill_cycle_snapshots()

    @classmethod
    def from_runtime_stores(cls, runtime_stores) -> "SqlAlchemyAuditFlowRepository":
        return cls(runtime_stores.session_factory, runtime_stores.engine)

    def record_tool_access(
        self,
        *,
        tool_call_id: str,
        workflow_run_id: str,
        node_name: str | None,
        tool_name: str,
        tool_version: str,
        adapter_type: str,
        subject_type: str,
        subject_id: str,
        organization_id: str,
        workspace_id: str | None,
        user_id: str | None,
        role: str | None,
        session_id: str | None,
        connection_id: str | None,
        execution_status: str,
        error_code: str | None,
        arguments_payload: dict[str, object],
        source_locator: str | None,
        recorded_at: datetime,
        completed_at: datetime,
    ) -> None:
        normalized_recorded_at = self._normalize_timestamp(recorded_at) or self._utcnow_naive()
        normalized_completed_at = self._normalize_timestamp(completed_at) or normalized_recorded_at
        with self.session_factory() as session:
            existing = session.scalars(
                select(ToolAccessAuditRow).where(ToolAccessAuditRow.tool_call_id == tool_call_id)
            ).first()
            if existing is None:
                session.add(
                    ToolAccessAuditRow(
                        tool_access_audit_id=f"tool-access-{uuid4().hex[:12]}",
                        organization_id=organization_id,
                        workspace_id=workspace_id,
                        workflow_run_id=workflow_run_id,
                        node_name=node_name,
                        tool_call_id=tool_call_id,
                        tool_name=tool_name,
                        tool_version=tool_version,
                        adapter_type=adapter_type,
                        subject_type=subject_type,
                        subject_id=subject_id,
                        user_id=user_id,
                        role=role,
                        session_id=session_id,
                        connection_id=connection_id,
                        execution_status=execution_status,
                        error_code=error_code,
                        arguments_payload=dict(arguments_payload),
                        source_locator=source_locator,
                        recorded_at=normalized_recorded_at,
                        completed_at=normalized_completed_at,
                    )
                )
            else:
                existing.organization_id = organization_id
                existing.workspace_id = workspace_id
                existing.workflow_run_id = workflow_run_id
                existing.node_name = node_name
                existing.tool_name = tool_name
                existing.tool_version = tool_version
                existing.adapter_type = adapter_type
                existing.subject_type = subject_type
                existing.subject_id = subject_id
                existing.user_id = user_id
                existing.role = role
                existing.session_id = session_id
                existing.connection_id = connection_id
                existing.execution_status = execution_status
                existing.error_code = error_code
                existing.arguments_payload = dict(arguments_payload)
                existing.source_locator = source_locator
                existing.recorded_at = normalized_recorded_at
                existing.completed_at = normalized_completed_at
            session.commit()

    @staticmethod
    def _env_value(name: str) -> str | None:
        return load_shared_agent_platform().env_value(name)

    def _resolve_semantic_model_name(self) -> str:
        provider = (self._env_value("AUDITFLOW_EMBEDDING_PROVIDER") or "auto").lower()
        openai_model = self._env_value("AUDITFLOW_OPENAI_EMBEDDING_MODEL")
        if provider == "openai":
            return f"openai:{openai_model or 'unconfigured'}"
        if provider == "auto" and self._env_value("OPENAI_API_KEY") and openai_model:
            return f"openai:{openai_model}"
        return SEMANTIC_MODEL_NAME

    def _resolve_semantic_vector_dimension(self) -> int:
        configured = self._env_value("AUDITFLOW_OPENAI_EMBEDDING_DIMENSIONS")
        if configured is not None:
            try:
                parsed = int(configured)
            except ValueError:
                parsed = 0
            if parsed > 0:
                return parsed
        return EMBEDDING_VECTOR_DIMENSION

    def _resolve_semantic_candidate_limit(self) -> int:
        configured = self._env_value("AUDITFLOW_VECTOR_CANDIDATE_LIMIT")
        if configured is not None:
            try:
                parsed = int(configured)
            except ValueError:
                parsed = 0
            if parsed > 0:
                return parsed
        return 64

    def _resolve_semantic_ann_bucket_count(self) -> int:
        configured = self._env_value("AUDITFLOW_VECTOR_ANN_BUCKETS")
        if configured is not None:
            try:
                parsed = int(configured)
            except ValueError:
                parsed = 0
            if parsed > 1:
                return parsed
        return 8

    def _pgvector_backend_ready(self) -> bool:
        return bool(getattr(self.engine.dialect, "_auditflow_pgvector_ready", False))

    def _resolve_vector_search_backend(self) -> tuple[str, str, str | None]:
        requested_mode = self.vector_search_requested_mode
        if requested_mode == "flat":
            return "flat", "flat-metadata-json", None
        if requested_mode == "ann":
            return "ann", "ann-metadata-json", None
        if requested_mode == "auto":
            if self._pgvector_backend_ready():
                return "pgvector", "pgvector-native", None
            return "ann", "ann-metadata-json", None
        if requested_mode == "pgvector":
            if self._pgvector_backend_ready():
                return "pgvector", "pgvector-native", None
            return "ann", "ann-metadata-json", "PGVECTOR_BACKEND_NOT_AVAILABLE"
        return "ann", "ann-metadata-json", "INVALID_VECTOR_SEARCH_MODE"

    def _build_openai_embedding_client(self):
        api_key = self._env_value("OPENAI_API_KEY")
        model_name = self._env_value("AUDITFLOW_OPENAI_EMBEDDING_MODEL")
        self._embedding_mode_decision = self._shared_platform.resolve_remote_mode(
            requested_mode=self.embedding_provider_mode,
            allowed_modes=("auto", "local", "openai"),
            local_mode="local",
            remote_mode="openai",
            has_remote_configuration=api_key is not None and model_name is not None,
            strict_remote_mode="openai",
            strict_missing_error="AUDITFLOW_OPENAI_EMBEDDING_MODEL",
            auto_fallback_reason="OPENAI_EMBEDDING_NOT_CONFIGURED",
        )
        if not self._embedding_mode_decision.use_remote:
            return None
        try:
            from openai import OpenAI
        except ImportError:
            if self.embedding_provider_mode == "openai":
                raise
            self._embedding_mode_decision = self._shared_platform.RuntimeModeDecision(
                requested_mode=self._embedding_mode_decision.requested_mode,
                effective_mode="local",
                use_remote=False,
                allow_fallback=True,
                fallback_reason="OPENAI_EMBEDDING_CLIENT_NOT_AVAILABLE",
            )
            return None
        timeout_seconds = float(self._env_value("AUDITFLOW_OPENAI_TIMEOUT_SECONDS") or 20.0)
        return OpenAI(
            api_key=api_key,
            base_url=self._env_value("AUDITFLOW_OPENAI_BASE_URL"),
            timeout=timeout_seconds,
        )

    def describe_embedding_capability(self) -> dict[str, object]:
        decision = getattr(self, "_embedding_mode_decision", None) or self._shared_platform.RuntimeModeDecision(
            requested_mode=self.embedding_provider_mode,
            effective_mode="openai" if self._openai_embedding_client is not None else "local",
            use_remote=self._openai_embedding_client is not None,
            allow_fallback=self._allow_embedding_fallback,
            fallback_reason=(
                "OPENAI_EMBEDDING_NOT_CONFIGURED"
                if self._openai_embedding_client is None and self.embedding_provider_mode == "auto"
                else None
            ),
        )
        return self._shared_platform.RuntimeCapabilityDescriptor(
            requested_mode=decision.requested_mode,
            effective_mode=decision.effective_mode,
            backend_id=self.semantic_model_name,
            fallback_reason=decision.fallback_reason,
            details={
                "vector_dimension": self.semantic_vector_dimension,
                "openai_model": self._env_value("AUDITFLOW_OPENAI_EMBEDDING_MODEL"),
                "fallback_enabled": self._allow_embedding_fallback,
            },
        ).as_dict()

    def describe_vector_search_capability(self) -> dict[str, object]:
        return self._shared_platform.RuntimeCapabilityDescriptor(
            requested_mode=self.vector_search_requested_mode,
            effective_mode=self.vector_search_mode,
            backend_id=self.vector_search_backend_id,
            fallback_reason=self.vector_search_fallback_reason,
            details={
                "dialect_name": self.engine.dialect.name,
                "pgvector_package_available": _pgvector_package_available(),
                "pgvector_backend_ready": self._pgvector_backend_ready(),
                "semantic_candidate_limit": self.semantic_candidate_limit,
                "semantic_ann_bucket_count": self.semantic_ann_bucket_count,
                "semantic_model_name": self.semantic_model_name,
                "semantic_store_table": SemanticVectorRow.__tablename__,
            },
        ).as_dict()

    @staticmethod
    def _normalize_organization_id(organization_id: str | None) -> str | None:
        normalized = (organization_id or "").strip()
        return normalized or None

    @classmethod
    def _assert_workspace_scope(
        cls,
        row: AuditWorkspaceRow | None,
        *,
        workspace_id: str,
        organization_id: str | None,
    ) -> AuditWorkspaceRow:
        normalized_org_id = cls._normalize_organization_id(organization_id)
        if row is None or (normalized_org_id is not None and row.organization_id != normalized_org_id):
            raise KeyError(workspace_id)
        return row

    @classmethod
    def _assert_cycle_scope(
        cls,
        row: AuditCycleRow | None,
        *,
        cycle_id: str,
        organization_id: str | None,
    ) -> AuditCycleRow:
        normalized_org_id = cls._normalize_organization_id(organization_id)
        if row is None or (normalized_org_id is not None and row.organization_id != normalized_org_id):
            raise KeyError(cycle_id)
        return row

    def _get_workspace_row(
        self,
        session: Session,
        *,
        workspace_id: str,
        organization_id: str | None = None,
    ) -> AuditWorkspaceRow:
        return self._assert_workspace_scope(
            session.get(AuditWorkspaceRow, workspace_id),
            workspace_id=workspace_id,
            organization_id=organization_id,
        )

    def _get_cycle_scope(
        self,
        session: Session,
        *,
        cycle_id: str,
        organization_id: str | None = None,
        workspace_id: str | None = None,
    ) -> tuple[AuditCycleRow, AuditWorkspaceRow]:
        cycle_row = self._assert_cycle_scope(
            session.get(AuditCycleRow, cycle_id),
            cycle_id=cycle_id,
            organization_id=organization_id,
        )
        workspace_row = self._get_workspace_row(
            session,
            workspace_id=cycle_row.workspace_id,
            organization_id=cycle_row.organization_id,
        )
        if workspace_id is not None and workspace_row.workspace_id != workspace_id:
            raise KeyError(cycle_id)
        return cycle_row, workspace_row

    def get_cycle_context(
        self,
        cycle_id: str,
        *,
        organization_id: str | None = None,
    ) -> dict[str, str]:
        with self.session_factory() as session:
            cycle_row, workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            return {
                "cycle_id": cycle_row.cycle_id,
                "organization_id": cycle_row.organization_id,
                "workspace_id": workspace_row.workspace_id,
            }

    def _get_control_scope(
        self,
        session: Session,
        *,
        control_state_id: str,
        organization_id: str | None = None,
    ) -> tuple[ControlCoverageRow, AuditCycleRow, AuditWorkspaceRow]:
        control_row = session.get(ControlCoverageRow, control_state_id)
        if control_row is None:
            raise KeyError(control_state_id)
        cycle_row, workspace_row = self._get_cycle_scope(
            session,
            cycle_id=control_row.cycle_id,
            organization_id=organization_id,
        )
        return control_row, cycle_row, workspace_row

    def _get_evidence_scope(
        self,
        session: Session,
        *,
        evidence_id: str,
        organization_id: str | None = None,
    ) -> tuple[EvidenceRow, AuditCycleRow, AuditWorkspaceRow]:
        evidence_row = session.get(EvidenceRow, evidence_id)
        if evidence_row is None:
            raise KeyError(evidence_id)
        cycle_row, workspace_row = self._get_cycle_scope(
            session,
            cycle_id=evidence_row.audit_cycle_id,
            organization_id=organization_id,
        )
        return evidence_row, cycle_row, workspace_row

    def _get_mapping_scope(
        self,
        session: Session,
        *,
        mapping_id: str,
        organization_id: str | None = None,
    ) -> tuple[MappingRow, AuditCycleRow, AuditWorkspaceRow]:
        mapping_row = session.get(MappingRow, mapping_id)
        if mapping_row is None:
            raise KeyError(mapping_id)
        cycle_row, workspace_row = self._get_cycle_scope(
            session,
            cycle_id=mapping_row.cycle_id,
            organization_id=organization_id,
        )
        return mapping_row, cycle_row, workspace_row

    def _get_gap_scope(
        self,
        session: Session,
        *,
        gap_id: str,
        organization_id: str | None = None,
    ) -> tuple[GapRow, ControlCoverageRow, AuditCycleRow, AuditWorkspaceRow]:
        gap_row = session.get(GapRow, gap_id)
        if gap_row is None:
            raise KeyError(gap_id)
        control_row, cycle_row, workspace_row = self._get_control_scope(
            session,
            control_state_id=gap_row.control_state_id,
            organization_id=organization_id,
        )
        return gap_row, control_row, cycle_row, workspace_row

    def _get_export_package_scope(
        self,
        session: Session,
        *,
        package_id: str,
        organization_id: str | None = None,
    ) -> tuple[ExportPackageRow, AuditCycleRow, AuditWorkspaceRow]:
        package_row = session.get(ExportPackageRow, package_id)
        if package_row is None:
            raise KeyError(package_id)
        cycle_row, workspace_row = self._get_cycle_scope(
            session,
            cycle_id=package_row.cycle_id,
            organization_id=organization_id,
        )
        return package_row, cycle_row, workspace_row

    def seed_if_empty(self) -> None:
        with self.session_factory.begin() as session:
            self._seed_control_catalog(session)
            existing = session.scalar(select(AuditWorkspaceRow.workspace_id).limit(1))
            if existing is not None:
                return

            now = datetime.now(UTC)
            session.add(
                AuditWorkspaceRow(
                    workspace_id="audit-ws-1",
                    organization_id=DEFAULT_ORGANIZATION_ID,
                    workspace_name="Acme Security Workspace",
                    slug="acme-security-workspace",
                    framework_name="SOC2",
                    workspace_status="active",
                    default_owner_user_id="owner-acme-audit",
                    settings_payload={"freshness_days_default": 90},
                    created_at=now,
                    updated_at=now,
                )
            )
            cycle_row = AuditCycleRow(
                cycle_id="cycle-1",
                organization_id=DEFAULT_ORGANIZATION_ID,
                workspace_id="audit-ws-1",
                cycle_name="SOC2 2026",
                cycle_status="pending_review",
                framework_name="SOC2",
                audit_period_start=date(2026, 1, 1),
                audit_period_end=date(2026, 12, 31),
                owner_user_id="owner-acme-audit",
                current_snapshot_version=1,
                last_mapped_at=now,
                last_reviewed_at=now,
                coverage_status="pending_review",
                review_queue_count=1,
                open_gap_count=1,
                latest_workflow_run_id=None,
                created_at=now,
                updated_at=now,
            )
            session.add(cycle_row)
            self._seed_cycle_control_states(
                session,
                cycle_id="cycle-1",
                framework_name="SOC2",
                fixed_state_ids=SEEDED_CONTROL_STATE_IDS,
                state_overrides={
                    "CC6.1": {
                        "coverage_status": "pending_review",
                        "mapped_evidence_count": 1,
                        "open_gap_count": 1,
                    }
                },
            )
            session.add(
                EvidenceSourceRow(
                    evidence_source_id="source-1",
                    cycle_id="cycle-1",
                    source_type="jira",
                    connection_id="connection-jira-1",
                    artifact_id="artifact-1",
                    upstream_object_id="SEC-123",
                    source_locator="https://jira.example.com/browse/SEC-123",
                    display_name="Jira Access Review Ticket",
                    ingest_status="normalized",
                    latest_workflow_run_id=None,
                    captured_at=now,
                    last_synced_at=now,
                    metadata_payload={"provider": "jira"},
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                MappingRow(
                    mapping_id="mapping-1",
                    cycle_id="cycle-1",
                    control_state_id="control-state-1",
                    control_code="CC6.1",
                    mapping_status="proposed",
                    snapshot_version=1,
                    evidence_item_id="evidence-1",
                    rationale_summary="Quarterly access review evidence requires reviewer confirmation.",
                    citation_refs=[{"kind": "evidence_chunk", "id": "chunk-1"}],
                    reviewer_locked=False,
                    updated_at=now,
                )
            )
            session.add(
                GapRow(
                    gap_id="gap-1",
                    control_state_id="control-state-1",
                    gap_type="stale_evidence",
                    severity="medium",
                    status="acknowledged",
                    snapshot_version=1,
                    title="Access review evidence needs current quarter refresh",
                    recommended_action="Upload the most recent quarterly access review export.",
                    resolved_at=None,
                    updated_at=now,
                )
            )
            session.add(
                EvidenceRow(
                    evidence_id="evidence-1",
                    audit_cycle_id="cycle-1",
                    source_artifact_id="artifact-1",
                    normalized_artifact_id=None,
                    title="Jira Access Review Ticket",
                    evidence_type="ticket",
                    parse_status="parsed",
                    captured_at=now,
                    summary="Quarterly access review completed for production systems.",
                    source_payload={
                        "source_type": "jira",
                        "source_locator": "https://jira.example.com/browse/SEC-123",
                    },
                )
            )
            session.add(
                EvidenceChunkRow(
                    chunk_id="chunk-1",
                    evidence_id="evidence-1",
                    chunk_index=0,
                    section_label="Description",
                    text_excerpt="Quarterly access review completed for production systems.",
                )
            )
            session.add(
                ArtifactBlobRow(
                    artifact_id="artifact-1",
                    artifact_type="upload",
                    content_text="Quarterly access review completed for production systems.",
                    metadata_payload={
                        "parser_status": "completed",
                        "organization_id": DEFAULT_ORGANIZATION_ID,
                        "workspace_id": "audit-ws-1",
                        "source_type": "jira",
                    },
                    created_at=now,
                    updated_at=now,
                )
            )
            self._record_cycle_snapshot(
                session,
                cycle_row=cycle_row,
                snapshot_version=1,
                snapshot_status="current",
                trigger_kind="seed",
                updated_at=self._normalize_timestamp(now) or self._utcnow_naive(),
            )

    def backfill_retrieval_state(self) -> None:
        with self.session_factory.begin() as session:
            cycle_rows = session.scalars(select(AuditCycleRow)).all()
            cycles_by_id = {row.cycle_id: row for row in cycle_rows}
            evidence_rows = session.scalars(select(EvidenceRow).order_by(EvidenceRow.evidence_id.asc())).all()
            for evidence_row in evidence_rows:
                cycle_row = cycles_by_id.get(evidence_row.audit_cycle_id)
                if cycle_row is None:
                    continue
                chunk_rows = session.scalars(
                    select(EvidenceChunkRow)
                    .where(EvidenceChunkRow.evidence_id == evidence_row.evidence_id)
                    .order_by(EvidenceChunkRow.chunk_index.asc())
                ).all()
                if not chunk_rows:
                    continue
                lexical_count = len(
                    session.scalars(
                        select(EmbeddingChunkRow.embedding_chunk_id)
                        .where(EmbeddingChunkRow.subject_type == "audit_evidence")
                        .where(EmbeddingChunkRow.subject_id == evidence_row.evidence_id)
                        .where(EmbeddingChunkRow.model_name == LEXICAL_MODEL_NAME)
                    ).all()
                )
                semantic_count = len(
                    session.scalars(
                        select(SemanticVectorRow.semantic_vector_id)
                        .where(SemanticVectorRow.subject_type == "audit_evidence")
                        .where(SemanticVectorRow.subject_id == evidence_row.evidence_id)
                        .where(SemanticVectorRow.model_name == self.semantic_model_name)
                    ).all()
                )
                if lexical_count == len(chunk_rows) and semantic_count == len(chunk_rows):
                    continue
                self._sync_embedding_chunks(
                    session,
                    cycle_row=cycle_row,
                    evidence_row=evidence_row,
                    chunk_rows=chunk_rows,
                )

    def backfill_cycle_snapshots(self) -> None:
        with self.session_factory.begin() as session:
            cycle_rows = session.scalars(select(AuditCycleRow).order_by(AuditCycleRow.cycle_id.asc())).all()
            for cycle_row in cycle_rows:
                snapshot_version = max(int(cycle_row.current_snapshot_version or 0), 0)
                existing_row = session.scalars(
                    select(CycleSnapshotRow)
                    .where(CycleSnapshotRow.cycle_id == cycle_row.cycle_id)
                    .where(CycleSnapshotRow.snapshot_version == snapshot_version)
                    .limit(1)
                ).first()
                if existing_row is not None:
                    continue
                export_row = session.scalars(
                    select(ExportPackageRow)
                    .where(ExportPackageRow.cycle_id == cycle_row.cycle_id)
                    .where(ExportPackageRow.snapshot_version == snapshot_version)
                    .order_by(ExportPackageRow.created_at.desc())
                ).first()
                snapshot_status = (
                    "frozen"
                    if export_row is not None
                    and export_row.status == "ready"
                    and export_row.immutable_at is not None
                    else "current"
                )
                self._record_cycle_snapshot(
                    session,
                    cycle_row=cycle_row,
                    snapshot_version=snapshot_version,
                    snapshot_status=snapshot_status,
                    trigger_kind="backfill",
                    workflow_run_id=export_row.workflow_run_id if export_row is not None else cycle_row.latest_workflow_run_id,
                    package_id=export_row.package_id if export_row is not None else None,
                    frozen_at=export_row.immutable_at if export_row is not None else None,
                    updated_at=self._normalize_timestamp(cycle_row.updated_at) or self._utcnow_naive(),
                )

    def _sync_embedding_chunks(
        self,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        evidence_row: EvidenceRow,
        chunk_rows: list[EvidenceChunkRow],
    ) -> None:
        existing_rows = session.scalars(
            select(EmbeddingChunkRow)
            .where(EmbeddingChunkRow.subject_type == "audit_evidence")
            .where(EmbeddingChunkRow.subject_id == evidence_row.evidence_id)
        ).all()
        for row in existing_rows:
            session.delete(row)
        existing_semantic_rows = session.scalars(
            select(SemanticVectorRow)
            .where(SemanticVectorRow.subject_type == "audit_evidence")
            .where(SemanticVectorRow.subject_id == evidence_row.evidence_id)
        ).all()
        for row in existing_semantic_rows:
            session.delete(row)
        created_at = self._normalize_timestamp(evidence_row.captured_at) or self._utcnow_naive()
        source_payload = evidence_row.source_payload if isinstance(evidence_row.source_payload, dict) else {}
        for chunk_row in chunk_rows:
            metadata_payload = self._build_embedding_metadata(
                cycle_row=cycle_row,
                evidence_row=evidence_row,
                chunk_row=chunk_row,
                source_payload=source_payload,
            )
            session.add(
                EmbeddingChunkRow(
                    embedding_chunk_id=f"embedding-{uuid4().hex[:10]}",
                    organization_id=cycle_row.organization_id,
                    workspace_id=cycle_row.workspace_id,
                    subject_type="audit_evidence",
                    subject_id=evidence_row.evidence_id,
                    chunk_index=chunk_row.chunk_index,
                    text_content=chunk_row.text_excerpt,
                    metadata_payload=metadata_payload,
                    model_name=LEXICAL_MODEL_NAME,
                    created_at=created_at,
                )
            )
            semantic_payload = self._build_semantic_embedding_payload(
                text_content=chunk_row.text_excerpt,
                metadata_payload=metadata_payload,
            )
            session.add(
                EmbeddingChunkRow(
                    embedding_chunk_id=f"embedding-{uuid4().hex[:10]}",
                    organization_id=cycle_row.organization_id,
                    workspace_id=cycle_row.workspace_id,
                    subject_type="audit_evidence",
                    subject_id=evidence_row.evidence_id,
                    chunk_index=chunk_row.chunk_index,
                    text_content=chunk_row.text_excerpt,
                    metadata_payload=semantic_payload,
                    model_name=self.semantic_model_name,
                    created_at=created_at,
                )
            )
            session.add(
                SemanticVectorRow(
                    semantic_vector_id=f"semantic-vector-{uuid4().hex[:10]}",
                    organization_id=cycle_row.organization_id,
                    workspace_id=cycle_row.workspace_id,
                    cycle_id=cycle_row.cycle_id,
                    subject_type="audit_evidence",
                    subject_id=evidence_row.evidence_id,
                    chunk_id=chunk_row.chunk_id,
                    chunk_index=chunk_row.chunk_index,
                    text_content=chunk_row.text_excerpt,
                    metadata_payload=dict(semantic_payload),
                    ann_bucket_keys=[str(value) for value in semantic_payload.get("ann_bucket_keys", [])],
                    semantic_terms=[str(value) for value in semantic_payload.get("semantic_terms", [])],
                    embedding_dimension=int(semantic_payload.get("embedding_dimension") or len(semantic_payload.get("embedding_vector", []))),
                    embedding_vector=list(semantic_payload.get("embedding_vector", [])),
                    model_name=self.semantic_model_name,
                    created_at=created_at,
                )
            )

    @classmethod
    def _build_embedding_metadata(
        cls,
        *,
        cycle_row: AuditCycleRow,
        evidence_row: EvidenceRow,
        chunk_row: EvidenceChunkRow,
        source_payload: dict[str, object],
    ) -> dict[str, object]:
        return {
            "cycle_id": cycle_row.cycle_id,
            "framework_name": cycle_row.framework_name,
            "evidence_item_id": evidence_row.evidence_id,
            "chunk_id": chunk_row.chunk_id,
            "title": evidence_row.title,
            "summary": evidence_row.summary,
            "section_label": chunk_row.section_label,
            "source_type": source_payload.get("source_type"),
            "source_locator": source_payload.get("source_locator"),
            "captured_at": cls._serialize_timestamp(evidence_row.captured_at),
        }

    def _build_semantic_embedding_payload(
        self,
        *,
        text_content: str,
        metadata_payload: dict[str, object],
    ) -> dict[str, object]:
        combined_text = "\n".join(
            str(part).strip()
            for part in (
                metadata_payload.get("title"),
                metadata_payload.get("summary"),
                metadata_payload.get("section_label"),
                text_content,
            )
            if str(part).strip()
        )
        semantic_weights = self._semantic_term_weights(combined_text)
        payload = dict(metadata_payload)
        embedding_vector = self._embed_semantic_text(combined_text)
        payload["embedding_vector"] = embedding_vector
        payload["embedding_dimension"] = len(embedding_vector)
        payload["embedding_provider"] = self.semantic_model_name.split(":", 1)[0]
        payload["embedding_model_name"] = self.semantic_model_name
        payload["vector_search_backend"] = self.vector_search_backend_id
        payload["ann_bucket_keys"] = self._build_ann_bucket_keys(embedding_vector)
        payload["semantic_weights"] = semantic_weights
        payload["semantic_terms"] = sorted(semantic_weights)
        return payload

    def _embed_semantic_text(self, value: str) -> list[float]:
        if self._openai_embedding_client is None:
            return self._build_embedding_vector(value)
        model_name = self._env_value("AUDITFLOW_OPENAI_EMBEDDING_MODEL")
        if model_name is None and self.semantic_model_name.startswith("openai:"):
            model_name = self.semantic_model_name.split(":", 1)[1]
        if model_name is None:
            return self._build_embedding_vector(value)
        try:
            response = self._openai_embedding_client.embeddings.create(
                model=model_name,
                input=value,
                dimensions=self.semantic_vector_dimension,
                encoding_format="float",
            )
            data = getattr(response, "data", None) or []
            if not data:
                raise ValueError("AUDITFLOW_OPENAI_EMBEDDING_EMPTY")
            embedding = getattr(data[0], "embedding", None)
            if not isinstance(embedding, list) or not embedding:
                raise ValueError("AUDITFLOW_OPENAI_EMBEDDING_EMPTY")
            return self._normalize_vector([float(item) for item in embedding])
        except Exception:
            if not self._allow_embedding_fallback:
                raise
            return self._build_embedding_vector(value)

    @classmethod
    def _upsert_memory_record(
        cls,
        session: Session,
        *,
        organization_id: str = DEFAULT_ORGANIZATION_ID,
        workspace_id: str | None,
        scope: str,
        subject_type: str,
        subject_id: str | None,
        memory_key: str,
        memory_type: str,
        value_payload: dict[str, object],
        confidence: float | None,
        source_kind: str,
        source_ref_payload: dict[str, object] | None,
        status: str = "active",
    ) -> MemoryRecordRow:
        stmt = (
            select(MemoryRecordRow)
            .where(MemoryRecordRow.organization_id == organization_id)
            .where(MemoryRecordRow.scope == scope)
            .where(MemoryRecordRow.subject_type == subject_type)
            .where(MemoryRecordRow.memory_key == memory_key)
            .where(MemoryRecordRow.status == status)
        )
        if subject_id is None:
            stmt = stmt.where(MemoryRecordRow.subject_id.is_(None))
        else:
            stmt = stmt.where(MemoryRecordRow.subject_id == subject_id)
        row = session.scalars(stmt.limit(1)).first()
        now = cls._utcnow_naive()
        if row is None:
            row = MemoryRecordRow(
                memory_id=f"memory-{uuid4().hex[:10]}",
                organization_id=organization_id,
                workspace_id=workspace_id,
                scope=scope,
                subject_type=subject_type,
                subject_id=subject_id,
                memory_key=memory_key,
                memory_type=memory_type,
                value_payload=dict(value_payload),
                confidence=confidence,
                source_kind=source_kind,
                source_ref_payload=(dict(source_ref_payload) if source_ref_payload is not None else None),
                status=status,
                created_at=now,
                updated_at=now,
            )
            session.add(row)
            return row
        row.workspace_id = workspace_id
        row.memory_type = memory_type
        row.value_payload = dict(value_payload)
        row.confidence = confidence
        row.source_kind = source_kind
        row.source_ref_payload = dict(source_ref_payload) if source_ref_payload is not None else None
        row.updated_at = now
        return row

    @classmethod
    def _record_mapping_memories(
        cls,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        mapping_row: MappingRow,
        evidence_row: EvidenceRow | None,
        decision_row: ReviewDecisionRow,
        decision: str,
        comment: str,
        from_status: str,
        subject_control_state_id: str,
        subject_control_code: str,
    ) -> None:
        source_ref_payload = {
            "cycle_id": cycle_row.cycle_id,
            "mapping_id": mapping_row.mapping_id,
            "review_decision_id": decision_row.review_decision_id,
            "evidence_item_id": mapping_row.evidence_item_id,
        }
        memory_value = {
            "decision": decision,
            "from_status": from_status,
            "to_status": mapping_row.mapping_status,
            "control_state_id": subject_control_state_id,
            "control_code": subject_control_code,
            "framework_name": cycle_row.framework_name,
            "cycle_id": cycle_row.cycle_id,
            "evidence_item_id": mapping_row.evidence_item_id,
            "evidence_title": evidence_row.title if evidence_row is not None else None,
            "evidence_summary": evidence_row.summary if evidence_row is not None else None,
            "rationale_summary": mapping_row.rationale_summary,
            "citation_refs": list(mapping_row.citation_refs or []),
            "comment": comment or None,
        }
        subject_id = f"{cycle_row.framework_name}:{subject_control_code}"
        confidence = 1.0 if decision == "accept" else 0.85 if decision == "reassign" else 0.7
        cls._upsert_memory_record(
            session,
            organization_id=cycle_row.organization_id,
            workspace_id=cycle_row.workspace_id,
            scope="organization",
            subject_type="framework_control",
            subject_id=subject_id,
            memory_key=f"mapping:{mapping_row.mapping_id}",
            memory_type="pattern",
            value_payload=memory_value,
            confidence=confidence,
            source_kind="human_feedback",
            source_ref_payload=source_ref_payload,
        )
        cls._upsert_memory_record(
            session,
            organization_id=cycle_row.organization_id,
            workspace_id=cycle_row.workspace_id,
            scope="cycle",
            subject_type="audit_cycle",
            subject_id=cycle_row.cycle_id,
            memory_key=f"mapping:{mapping_row.mapping_id}",
            memory_type="fact",
            value_payload=memory_value,
            confidence=confidence,
            source_kind="human_feedback",
            source_ref_payload=source_ref_payload,
        )

    @classmethod
    def _record_gap_memory(
        cls,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        control_row: ControlCoverageRow,
        gap_row: GapRow,
        decision_row: ReviewDecisionRow,
        decision: str,
        comment: str,
        from_status: str,
    ) -> None:
        cls._upsert_memory_record(
            session,
            organization_id=cycle_row.organization_id,
            workspace_id=cycle_row.workspace_id,
            scope="cycle",
            subject_type="audit_cycle",
            subject_id=cycle_row.cycle_id,
            memory_key=f"gap:{gap_row.gap_id}",
            memory_type="fact",
            value_payload={
                "decision": decision,
                "from_status": from_status,
                "to_status": gap_row.status,
                "cycle_id": cycle_row.cycle_id,
                "framework_name": cycle_row.framework_name,
                "control_state_id": control_row.control_state_id,
                "control_code": control_row.control_code,
                "gap_id": gap_row.gap_id,
                "gap_type": gap_row.gap_type,
                "severity": gap_row.severity,
                "title": gap_row.title,
                "recommended_action": gap_row.recommended_action,
                "comment": comment or None,
            },
            confidence=1.0 if decision == "resolve_gap" else 0.8,
            source_kind="human_feedback",
            source_ref_payload={
                "cycle_id": cycle_row.cycle_id,
                "gap_id": gap_row.gap_id,
                "review_decision_id": decision_row.review_decision_id,
            },
        )

    @staticmethod
    def _to_grounding_memory(row: MemoryRecordRow) -> dict[str, object]:
        value_payload = dict(row.value_payload) if isinstance(row.value_payload, dict) else {}
        return {
            "memory_id": row.memory_id,
            "scope": row.scope,
            "subject_type": row.subject_type,
            "subject_id": row.subject_id,
            "memory_key": row.memory_key,
            "memory_type": row.memory_type,
            "decision": value_payload.get("decision"),
            "control_code": value_payload.get("control_code"),
            "evidence_summary": value_payload.get("evidence_summary"),
            "comment": value_payload.get("comment"),
            "confidence": row.confidence,
            "updated_at": row.updated_at.isoformat(),
            "source_ref": (
                dict(row.source_ref_payload)
                if isinstance(row.source_ref_payload, dict)
                else None
            ),
        }

    @staticmethod
    def _build_grounding_query(*, evidence_summary: str, chunk_texts: list[str]) -> str:
        sections = [evidence_summary.strip(), *(chunk.strip() for chunk in chunk_texts[:2])]
        normalized = " ".join(section for section in sections if section)
        return normalized[:500].strip()

    @staticmethod
    def _tokenize_search_terms(value: str) -> list[str]:
        return re.findall(r"[a-z0-9]+", value.lower())

    @classmethod
    def _normalize_semantic_token(cls, token: str) -> str:
        normalized = token.lower().strip()
        if normalized.endswith("ies") and len(normalized) > 4:
            normalized = normalized[:-3] + "y"
        elif normalized.endswith("ing") and len(normalized) > 5:
            normalized = normalized[:-3]
        elif normalized.endswith("ed") and len(normalized) > 4:
            normalized = normalized[:-2]
        elif normalized.endswith(("ses", "xes", "zes", "ches", "shes")) and len(normalized) > 4:
            normalized = normalized[:-2]
        elif normalized.endswith("s") and len(normalized) > 3:
            normalized = normalized[:-1]
        return normalized

    @classmethod
    def _semantic_term_weights(cls, value: str) -> dict[str, float]:
        weights: defaultdict[str, float] = defaultdict(float)
        for token in cls._tokenize_search_terms(value):
            normalized = cls._normalize_semantic_token(token)
            if normalized == "":
                continue
            weights[normalized] += 1.0
            for synonym in SEMANTIC_SYNONYMS.get(normalized, ()):
                weights[cls._normalize_semantic_token(synonym)] += 0.35
        return {token: round(weight, 4) for token, weight in weights.items()}

    @staticmethod
    def _normalize_vector(values: list[float]) -> list[float]:
        norm = sum(value * value for value in values) ** 0.5
        if norm == 0:
            return [0.0 for _ in values]
        return [round(value / norm, 6) for value in values]

    def _build_ann_bucket_keys(self, vector: list[float]) -> list[str]:
        if not vector:
            return []
        bucket_count = max(2, min(self.semantic_ann_bucket_count, len(vector)))
        segment_size = max(1, len(vector) // bucket_count)
        keys: list[str] = []
        for bucket_index in range(bucket_count):
            start = bucket_index * segment_size
            end = len(vector) if bucket_index == bucket_count - 1 else min(len(vector), start + segment_size)
            segment = vector[start:end]
            if not segment:
                continue
            sample = segment[: min(len(segment), 8)]
            sign_mask = 0
            for index, value in enumerate(sample):
                if value >= 0:
                    sign_mask |= 1 << index
            magnitude = min(int(sum(abs(value) for value in sample) * 100), 999)
            keys.append(f"{bucket_index}:{sign_mask:02x}:{magnitude:03d}")
        return keys

    def _extract_ann_bucket_keys(
        self,
        *,
        metadata_payload: dict[str, object],
        text_content: str,
        vector: list[float] | None = None,
    ) -> set[str]:
        stored = metadata_payload.get("ann_bucket_keys")
        if isinstance(stored, list):
            normalized = {
                str(value).strip()
                for value in stored
                if str(value).strip()
            }
            if normalized:
                return normalized
        normalized_vector = vector or self._extract_embedding_vector(
            metadata_payload=metadata_payload,
            text_content=text_content,
        )
        return set(self._build_ann_bucket_keys(normalized_vector))

    @classmethod
    def _build_embedding_vector(cls, value: str) -> list[float]:
        vector = [0.0] * EMBEDDING_VECTOR_DIMENSION
        for token, weight in cls._semantic_term_weights(value).items():
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            primary_index = int.from_bytes(digest[0:4], byteorder="big") % EMBEDDING_VECTOR_DIMENSION
            secondary_index = int.from_bytes(digest[4:8], byteorder="big") % EMBEDDING_VECTOR_DIMENSION
            tertiary_index = int.from_bytes(digest[8:12], byteorder="big") % EMBEDDING_VECTOR_DIMENSION
            sign = -1.0 if digest[12] % 2 else 1.0
            vector[primary_index] += weight
            vector[secondary_index] += weight * 0.5
            vector[tertiary_index] += weight * 0.2 * sign
        return cls._normalize_vector(vector)

    def _extract_embedding_vector(
        self,
        *,
        metadata_payload: dict[str, object],
        text_content: str,
    ) -> list[float]:
        stored = metadata_payload.get("embedding_vector")
        if isinstance(stored, list) and stored:
            try:
                vector = [float(value) for value in stored]
            except (TypeError, ValueError):
                vector = []
            if vector:
                return self._normalize_vector(vector)
        return self._embed_semantic_text(
            "\n".join(
                str(part).strip()
                for part in (
                    metadata_payload.get("title"),
                    metadata_payload.get("summary"),
                    metadata_payload.get("section_label"),
                    text_content,
                )
                if str(part).strip()
            )
        )

    @staticmethod
    def _cosine_similarity(lhs: list[float], rhs: list[float]) -> float:
        if not lhs or not rhs:
            return 0.0
        if len(lhs) != len(rhs):
            return 0.0
        return sum(lhs[index] * rhs[index] for index in range(len(lhs)))

    @staticmethod
    def _tokenize_search_text(value: str) -> set[str]:
        return set(re.findall(r"[a-z0-9]+", value.lower()))

    @classmethod
    def _estimate_semantic_candidate_hint(
        cls,
        *,
        query_terms: set[str],
        query_ann_keys: set[str],
        metadata_payload: dict[str, object],
        row_ann_keys: set[str],
    ) -> float:
        semantic_terms_raw = metadata_payload.get("semantic_terms")
        semantic_terms = (
            {
                str(term).strip().lower()
                for term in semantic_terms_raw
                if str(term).strip()
            }
            if isinstance(semantic_terms_raw, list)
            else set()
        )
        title_terms = cls._tokenize_search_text(str(metadata_payload.get("title") or ""))
        ann_overlap = len(query_ann_keys & row_ann_keys)
        semantic_overlap = len(query_terms & semantic_terms)
        title_overlap = len(query_terms & title_terms)
        return round((ann_overlap * 2.0) + semantic_overlap + min(title_overlap * 0.5, 1.5), 4)

    @classmethod
    def _score_search_match(
        cls,
        *,
        query: str,
        text_content: str,
        metadata_payload: dict[str, object],
    ) -> float:
        normalized_query = query.strip().lower()
        if not normalized_query:
            return 0.0
        query_tokens = cls._tokenize_search_text(normalized_query)
        if not query_tokens:
            return 0.0
        title = str(metadata_payload.get("title") or "")
        summary = str(metadata_payload.get("summary") or "")
        section_label = str(metadata_payload.get("section_label") or "")
        search_text = "\n".join(part for part in (title, summary, section_label, text_content) if part).lower()
        search_tokens = cls._tokenize_search_text(search_text)
        overlap = len(query_tokens & search_tokens)
        if overlap == 0 and normalized_query not in search_text:
            return 0.0
        score = overlap / len(query_tokens)
        if normalized_query in search_text:
            score += 1.2
        if normalized_query in text_content.lower():
            score += 0.6
        if title and normalized_query in title.lower():
            score += 0.35
        if summary and normalized_query in summary.lower():
            score += 0.2
        score += min(len(query_tokens & cls._tokenize_search_text(title)) * 0.1, 0.3)
        score += min(len(query_tokens & cls._tokenize_search_text(summary)) * 0.05, 0.2)
        return round(score, 4)

    def _score_semantic_match(
        self,
        *,
        query: str,
        text_content: str,
        metadata_payload: dict[str, object],
        query_vector: list[float] | None = None,
    ) -> float:
        normalized_query_vector = query_vector or self._embed_semantic_text(query)
        row_vector = self._extract_embedding_vector(
            metadata_payload=metadata_payload,
            text_content=text_content,
        )
        score = self._cosine_similarity(normalized_query_vector, row_vector)
        if score <= 0:
            return 0.0
        query_weights = self._semantic_term_weights(query)
        title_terms = self._semantic_term_weights(str(metadata_payload.get("title") or ""))
        title_overlap = len(set(query_weights) & set(title_terms))
        if title_overlap:
            score += min(title_overlap * 0.05, 0.15)
        return round(score, 4)

    @staticmethod
    def _coerce_json_dict(value: object) -> dict[str, object]:
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return dict(loaded) if isinstance(loaded, dict) else {}
        return {}

    @staticmethod
    def _coerce_string_list(value: object) -> list[str]:
        if isinstance(value, list):
            return [str(item) for item in value if str(item)]
        if isinstance(value, str):
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                return []
            if isinstance(loaded, list):
                return [str(item) for item in loaded if str(item)]
        return []

    @classmethod
    def _coerce_vector_value(cls, value: object) -> list[float]:
        if isinstance(value, tuple):
            value = list(value)
        if isinstance(value, str):
            try:
                loaded = json.loads(value)
            except json.JSONDecodeError:
                loaded = None
            if isinstance(loaded, list):
                value = loaded
        if isinstance(value, list):
            try:
                return cls._normalize_vector([float(item) for item in value])
            except (TypeError, ValueError):
                return []
        return []

    @staticmethod
    def _pgvector_query_literal(vector: list[float]) -> str:
        return "[" + ",".join(f"{float(value):.6f}" for value in vector) + "]"

    def _load_semantic_vector_rows(
        self,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        chunk_ids: set[str] | None = None,
    ) -> list[SemanticVectorRow]:
        stmt = (
            select(SemanticVectorRow)
            .where(SemanticVectorRow.organization_id == cycle_row.organization_id)
            .where(SemanticVectorRow.workspace_id == cycle_row.workspace_id)
            .where(SemanticVectorRow.cycle_id == cycle_row.cycle_id)
            .where(SemanticVectorRow.subject_type == "audit_evidence")
            .where(SemanticVectorRow.model_name == self.semantic_model_name)
            .order_by(SemanticVectorRow.created_at.desc(), SemanticVectorRow.chunk_index.asc())
        )
        if chunk_ids:
            stmt = stmt.where(SemanticVectorRow.chunk_id.in_(sorted(chunk_ids)))
        return session.scalars(stmt).all()

    def _search_semantic_candidates_pgvector(
        self,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        query_vector: list[float],
        limit: int,
    ) -> list[dict[str, object]] | None:
        if not self._pgvector_backend_ready():
            return None
        try:
            rows = session.execute(
                text(
                    """
                    SELECT
                        semantic_vector_id,
                        subject_id,
                        chunk_id,
                        chunk_index,
                        text_content,
                        metadata_payload,
                        ann_bucket_keys,
                        semantic_terms,
                        embedding_vector,
                        created_at
                    FROM auditflow_semantic_vector
                    WHERE organization_id = :organization_id
                      AND workspace_id = :workspace_id
                      AND cycle_id = :cycle_id
                      AND subject_type = 'audit_evidence'
                      AND model_name = :model_name
                    ORDER BY embedding_vector <=> CAST(:query_vector AS vector)
                    LIMIT :candidate_limit
                    """
                ),
                {
                    "organization_id": cycle_row.organization_id,
                    "workspace_id": cycle_row.workspace_id,
                    "cycle_id": cycle_row.cycle_id,
                    "model_name": self.semantic_model_name,
                    "query_vector": self._pgvector_query_literal(query_vector),
                    "candidate_limit": max(1, limit),
                },
            ).mappings().all()
        except Exception:
            return None
        candidates: list[dict[str, object]] = []
        for row in rows:
            candidates.append(
                {
                    "subject_id": str(row["subject_id"]),
                    "chunk_id": str(row["chunk_id"]),
                    "chunk_index": int(row["chunk_index"]),
                    "text_content": str(row["text_content"]),
                    "metadata_payload": self._coerce_json_dict(row["metadata_payload"]),
                    "ann_bucket_keys": self._coerce_string_list(row["ann_bucket_keys"]),
                    "semantic_terms": self._coerce_string_list(row["semantic_terms"]),
                    "embedding_vector": self._coerce_vector_value(row["embedding_vector"]),
                    "created_at": row["created_at"],
                }
            )
        return candidates

    @classmethod
    def _semantic_candidate_from_row(cls, row: SemanticVectorRow) -> dict[str, object]:
        return {
            "subject_id": row.subject_id,
            "chunk_id": row.chunk_id,
            "chunk_index": row.chunk_index,
            "text_content": row.text_content,
            "metadata_payload": dict(row.metadata_payload) if isinstance(row.metadata_payload, dict) else {},
            "ann_bucket_keys": [str(value) for value in row.ann_bucket_keys],
            "semantic_terms": [str(value) for value in row.semantic_terms],
            "embedding_vector": cls._coerce_vector_value(row.embedding_vector),
            "created_at": row.created_at,
        }

    @staticmethod
    def _combine_search_scores(*, lexical_score: float, semantic_score: float) -> float:
        if lexical_score <= 0 and semantic_score <= 0:
            return 0.0
        score = lexical_score + (semantic_score * 1.15)
        if lexical_score > 0 and semantic_score > 0:
            score += 0.2
        return round(score, 4)

    @classmethod
    def _serialize_timestamp(cls, value: datetime | None) -> str | None:
        normalized = cls._normalize_timestamp(value)
        if normalized is None:
            return None
        return normalized.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")

    @classmethod
    def _parse_timestamp(cls, value: object) -> datetime | None:
        if isinstance(value, datetime):
            return cls._normalize_timestamp(value)
        if not isinstance(value, str) or value == "":
            return None
        try:
            return cls._normalize_timestamp(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None

    @staticmethod
    def _stable_workflow_entity_id(prefix: str, workflow_run_id: str, *parts: object) -> str:
        normalized = "||".join("" if part is None else str(part) for part in parts)
        digest = hashlib.sha256(f"{workflow_run_id}::{normalized}".encode("utf-8")).hexdigest()[:12]
        return f"{prefix}-{digest}"

    @staticmethod
    def _to_output_dict(value: object) -> dict[str, object]:
        return dict(value) if isinstance(value, dict) else {}

    @classmethod
    def _to_output_dict_list(cls, value: object) -> list[dict[str, object]]:
        if not isinstance(value, list):
            return []
        return [cls._to_output_dict(item) for item in value if isinstance(item, dict)]

    @classmethod
    def _citation_dicts(cls, value: object) -> list[dict[str, object]]:
        return cls._to_output_dict_list(value)

    @staticmethod
    def _cycle_snapshot_record_id(cycle_id: str, snapshot_version: int) -> str:
        return f"cycle-snapshot-{cycle_id}-{snapshot_version}"

    @staticmethod
    def _json_str_list(value: object) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(item) for item in value if item]

    def _collect_cycle_snapshot_refs(
        self,
        session: Session,
        *,
        cycle_id: str,
        snapshot_version: int,
    ) -> dict[str, list[str]]:
        accepted_mapping_ids = session.scalars(
            select(MappingRow.mapping_id)
            .where(MappingRow.cycle_id == cycle_id)
            .where(MappingRow.mapping_status == "accepted")
            .order_by(MappingRow.mapping_id.asc())
        ).all()
        pending_mapping_ids = session.scalars(
            select(MappingRow.mapping_id)
            .where(MappingRow.cycle_id == cycle_id)
            .where(MappingRow.mapping_status.in_(("proposed", "reassigned")))
            .order_by(MappingRow.mapping_id.asc())
        ).all()
        open_gap_ids = session.scalars(
            select(GapRow.gap_id)
            .join(ControlCoverageRow, GapRow.control_state_id == ControlCoverageRow.control_state_id)
            .where(ControlCoverageRow.cycle_id == cycle_id)
            .where(GapRow.status != "resolved")
            .order_by(GapRow.gap_id.asc())
        ).all()
        narrative_ids = session.scalars(
            select(NarrativeRow.narrative_id)
            .where(NarrativeRow.cycle_id == cycle_id)
            .where(NarrativeRow.snapshot_version == snapshot_version)
            .order_by(NarrativeRow.narrative_id.asc())
        ).all()
        return {
            "accepted_mapping_ids": list(accepted_mapping_ids),
            "pending_mapping_ids": list(pending_mapping_ids),
            "open_gap_ids": list(open_gap_ids),
            "narrative_ids": list(narrative_ids),
        }

    def _record_cycle_snapshot(
        self,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        snapshot_version: int,
        snapshot_status: str,
        trigger_kind: str,
        workflow_run_id: str | None = None,
        review_decision_id: str | None = None,
        package_id: str | None = None,
        frozen_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> CycleSnapshotRow:
        snapshot_refs = self._collect_cycle_snapshot_refs(
            session,
            cycle_id=cycle_row.cycle_id,
            snapshot_version=snapshot_version,
        )
        now = updated_at or self._utcnow_naive()
        row = session.get(
            CycleSnapshotRow,
            self._cycle_snapshot_record_id(cycle_row.cycle_id, snapshot_version),
        )
        if row is None:
            row = CycleSnapshotRow(
                snapshot_record_id=self._cycle_snapshot_record_id(cycle_row.cycle_id, snapshot_version),
                cycle_id=cycle_row.cycle_id,
                snapshot_version=snapshot_version,
                snapshot_status=snapshot_status,
                trigger_kind=trigger_kind,
                workflow_run_id=workflow_run_id,
                review_decision_id=review_decision_id,
                package_id=package_id,
                accepted_mapping_ids_payload=list(snapshot_refs["accepted_mapping_ids"]),
                pending_mapping_ids_payload=list(snapshot_refs["pending_mapping_ids"]),
                open_gap_ids_payload=list(snapshot_refs["open_gap_ids"]),
                narrative_ids_payload=list(snapshot_refs["narrative_ids"]),
                accepted_mapping_count=len(snapshot_refs["accepted_mapping_ids"]),
                review_queue_count=len(snapshot_refs["pending_mapping_ids"]),
                open_gap_count=len(snapshot_refs["open_gap_ids"]),
                created_at=now,
                updated_at=now,
                frozen_at=frozen_at,
            )
            session.add(row)
        else:
            row.snapshot_status = snapshot_status
            row.trigger_kind = trigger_kind
            if workflow_run_id is not None:
                row.workflow_run_id = workflow_run_id
            if review_decision_id is not None:
                row.review_decision_id = review_decision_id
            if package_id is not None:
                row.package_id = package_id
            row.accepted_mapping_ids_payload = list(snapshot_refs["accepted_mapping_ids"])
            row.pending_mapping_ids_payload = list(snapshot_refs["pending_mapping_ids"])
            row.open_gap_ids_payload = list(snapshot_refs["open_gap_ids"])
            row.narrative_ids_payload = list(snapshot_refs["narrative_ids"])
            row.accepted_mapping_count = len(snapshot_refs["accepted_mapping_ids"])
            row.review_queue_count = len(snapshot_refs["pending_mapping_ids"])
            row.open_gap_count = len(snapshot_refs["open_gap_ids"])
            row.updated_at = now
            if frozen_at is not None:
                row.frozen_at = frozen_at
        historical_rows = session.scalars(
            select(CycleSnapshotRow)
            .where(CycleSnapshotRow.cycle_id == cycle_row.cycle_id)
            .where(CycleSnapshotRow.snapshot_version != snapshot_version)
        ).all()
        for historical_row in historical_rows:
            if historical_row.snapshot_status == "frozen":
                continue
            historical_row.snapshot_status = "superseded"
            historical_row.updated_at = now
        return row

    @staticmethod
    def _rebase_cycle_live_snapshot(
        session: Session,
        *,
        cycle_id: str,
        snapshot_version: int,
    ) -> None:
        mapping_rows = session.scalars(
            select(MappingRow).where(MappingRow.cycle_id == cycle_id)
        ).all()
        for mapping_row in mapping_rows:
            mapping_row.snapshot_version = snapshot_version
        gap_rows = session.scalars(
            select(GapRow)
            .join(ControlCoverageRow, GapRow.control_state_id == ControlCoverageRow.control_state_id)
            .where(ControlCoverageRow.cycle_id == cycle_id)
        ).all()
        for gap_row in gap_rows:
            gap_row.snapshot_version = snapshot_version

    @staticmethod
    def _snapshot_refs(
        session: Session,
        *,
        cycle_id: str,
        working_snapshot_version: int,
    ) -> dict[str, list[str]]:
        snapshot_row = session.scalars(
            select(CycleSnapshotRow)
            .where(CycleSnapshotRow.cycle_id == cycle_id)
            .where(CycleSnapshotRow.snapshot_version == working_snapshot_version)
            .limit(1)
        ).first()
        if snapshot_row is not None:
            prior_narrative_ids: list[str] = []
            prior_snapshot_rows = session.scalars(
                select(CycleSnapshotRow)
                .where(CycleSnapshotRow.cycle_id == cycle_id)
                .where(CycleSnapshotRow.snapshot_version < working_snapshot_version)
                .order_by(CycleSnapshotRow.snapshot_version.asc())
            ).all()
            for prior_row in prior_snapshot_rows:
                prior_narrative_ids.extend(
                    [
                        item
                        for item in SqlAlchemyAuditFlowRepository._json_str_list(prior_row.narrative_ids_payload)
                        if item not in prior_narrative_ids
                    ]
                )
            return {
                "accepted_mapping_ids": SqlAlchemyAuditFlowRepository._json_str_list(
                    snapshot_row.accepted_mapping_ids_payload
                ),
                "open_gap_ids": SqlAlchemyAuditFlowRepository._json_str_list(
                    snapshot_row.open_gap_ids_payload
                ),
                "prior_narrative_ids": prior_narrative_ids,
            }
        accepted_mapping_ids = session.scalars(
            select(MappingRow.mapping_id)
            .where(MappingRow.cycle_id == cycle_id)
            .where(MappingRow.mapping_status == "accepted")
            .where(MappingRow.snapshot_version <= working_snapshot_version)
            .order_by(MappingRow.mapping_id.asc())
        ).all()
        open_gap_ids = session.scalars(
            select(GapRow.gap_id)
            .join(ControlCoverageRow, GapRow.control_state_id == ControlCoverageRow.control_state_id)
            .where(ControlCoverageRow.cycle_id == cycle_id)
            .where(GapRow.status != "resolved")
            .where(GapRow.snapshot_version <= working_snapshot_version)
            .order_by(GapRow.gap_id.asc())
        ).all()
        prior_narrative_ids = session.scalars(
            select(NarrativeRow.narrative_id)
            .where(NarrativeRow.cycle_id == cycle_id)
            .where(NarrativeRow.snapshot_version < working_snapshot_version)
            .order_by(NarrativeRow.narrative_id.asc())
        ).all()
        return {
            "accepted_mapping_ids": list(accepted_mapping_ids),
            "open_gap_ids": list(open_gap_ids),
            "prior_narrative_ids": list(prior_narrative_ids),
        }

    def _resolve_control_row_for_reference(
        self,
        session: Session,
        *,
        cycle_id: str,
        control_reference: object | None = None,
        mapping_reference: object | None = None,
    ) -> ControlCoverageRow | None:
        references: list[str] = []
        if isinstance(control_reference, str) and control_reference.strip():
            references.append(control_reference.strip())
        if isinstance(mapping_reference, str) and mapping_reference.strip():
            references.append(mapping_reference.strip())
        for reference in references:
            control_row = session.get(ControlCoverageRow, reference)
            if control_row is not None and control_row.cycle_id == cycle_id:
                return control_row
            control_row = session.scalars(
                select(ControlCoverageRow)
                .where(ControlCoverageRow.cycle_id == cycle_id)
                .where(ControlCoverageRow.control_code == reference)
                .limit(1)
            ).first()
            if control_row is not None:
                return control_row
            mapping_row = session.get(MappingRow, reference)
            if mapping_row is not None and mapping_row.cycle_id == cycle_id:
                resolved_control = session.get(ControlCoverageRow, mapping_row.control_state_id)
                if resolved_control is not None:
                    return resolved_control
        return None

    @staticmethod
    def _to_workspace(row: AuditWorkspaceRow) -> AuditWorkspaceSummary:
        return AuditWorkspaceSummary(
            workspace_id=row.workspace_id,
            workspace_name=row.workspace_name,
            slug=row.slug,
            framework_name=row.framework_name,
            workspace_status=row.workspace_status,
            default_owner_user_id=row.default_owner_user_id,
            created_at=row.created_at,
        )

    @staticmethod
    def _to_cycle(row: AuditCycleRow) -> AuditCycleSummary:
        return AuditCycleSummary(
            cycle_id=row.cycle_id,
            workspace_id=row.workspace_id,
            cycle_name=row.cycle_name,
            cycle_status=row.cycle_status,
            framework_name=row.framework_name,
            audit_period_start=row.audit_period_start,
            audit_period_end=row.audit_period_end,
            owner_user_id=row.owner_user_id,
            current_snapshot_version=row.current_snapshot_version,
            last_mapped_at=row.last_mapped_at,
            last_reviewed_at=row.last_reviewed_at,
            coverage_status=row.coverage_status,
            review_queue_count=row.review_queue_count,
            open_gap_count=row.open_gap_count,
            latest_workflow_run_id=row.latest_workflow_run_id,
        )

    @staticmethod
    def _to_control(row: ControlCoverageRow) -> ControlCoverageSummary:
        return ControlCoverageSummary(
            control_state_id=row.control_state_id,
            control_code=row.control_code,
            coverage_status=row.coverage_status,
            mapped_evidence_count=row.mapped_evidence_count,
            open_gap_count=row.open_gap_count,
        )

    @staticmethod
    def _to_mapping(row: MappingRow) -> MappingSummary:
        return MappingSummary(
            mapping_id=row.mapping_id,
            control_state_id=row.control_state_id,
            control_code=row.control_code,
            mapping_status=row.mapping_status,
            snapshot_version=row.snapshot_version,
            evidence_item_id=row.evidence_item_id,
            rationale_summary=row.rationale_summary,
            citation_refs=row.citation_refs,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _claim_is_active(row: MappingRow, *, now: datetime) -> bool:
        if not row.reviewer_claimed_by_user_id:
            return False
        if row.reviewer_claim_expires_at is None:
            return True
        return row.reviewer_claim_expires_at > now

    @classmethod
    def _claim_status(
        cls,
        row: MappingRow,
        *,
        viewer_user_id: str | None,
        now: datetime,
    ) -> str:
        if not cls._claim_is_active(row, now=now):
            return "unclaimed"
        if viewer_user_id is not None and row.reviewer_claimed_by_user_id == viewer_user_id:
            return "claimed_by_me"
        return "claimed_by_other"

    @classmethod
    def _clear_mapping_claim(cls, row: MappingRow) -> None:
        row.reviewer_claimed_by_user_id = None
        row.reviewer_claimed_at = None
        row.reviewer_claim_expires_at = None

    @classmethod
    def _normalize_claim_lease_seconds(cls, lease_seconds: int | None) -> int:
        if lease_seconds is None:
            return DEFAULT_REVIEW_CLAIM_LEASE_SECONDS
        return max(60, min(int(lease_seconds), 7200))

    @classmethod
    def _to_review_item(
        cls,
        row: MappingRow,
        control_row: ControlCoverageRow | None = None,
        *,
        viewer_user_id: str | None = None,
        now: datetime | None = None,
        tool_access_summary: ToolAccessSummary | None = None,
    ) -> ReviewQueueItem:
        claim_now = now or cls._utcnow_naive()
        claim_status = cls._claim_status(
            row,
            viewer_user_id=viewer_user_id,
            now=claim_now,
        )
        return ReviewQueueItem(
            mapping_id=row.mapping_id,
            control_state_id=row.control_state_id,
            control_code=row.control_code,
            coverage_status=(control_row.coverage_status if control_row else "pending_review"),
            snapshot_version=row.snapshot_version,
            evidence_item_id=row.evidence_item_id,
            rationale_summary=row.rationale_summary,
            citation_refs=row.citation_refs,
            claimed_by_user_id=(
                row.reviewer_claimed_by_user_id
                if claim_status != "unclaimed"
                else None
            ),
            claimed_at=row.reviewer_claimed_at if claim_status != "unclaimed" else None,
            claim_expires_at=(
                row.reviewer_claim_expires_at
                if claim_status != "unclaimed"
                else None
            ),
            claim_status=claim_status,
            updated_at=row.updated_at,
            tool_access_summary=(tool_access_summary or ToolAccessSummary()),
        )

    @staticmethod
    def _to_review_decision(row: ReviewDecisionRow) -> ReviewDecisionSummary:
        return ReviewDecisionSummary(
            review_decision_id=row.review_decision_id,
            cycle_id=row.cycle_id,
            mapping_id=row.mapping_id,
            gap_id=row.gap_id,
            decision=row.decision,
            from_status=row.from_status,
            to_status=row.to_status,
            reviewer_id=row.reviewer_id,
            comment=row.comment,
            feedback_tags=row.feedback_tags,
            created_at=row.created_at,
        )

    @staticmethod
    def _to_tool_access_audit(row: ToolAccessAuditRow) -> ToolAccessAuditSummary:
        return ToolAccessAuditSummary(
            tool_access_audit_id=row.tool_access_audit_id,
            workflow_run_id=row.workflow_run_id,
            node_name=row.node_name,
            tool_call_id=row.tool_call_id,
            tool_name=row.tool_name,
            tool_version=row.tool_version,
            adapter_type=row.adapter_type,
            subject_type=row.subject_type,
            subject_id=row.subject_id,
            workspace_id=row.workspace_id,
            user_id=row.user_id,
            role=row.role,
            session_id=row.session_id,
            connection_id=row.connection_id,
            execution_status=row.execution_status,
            error_code=row.error_code,
            arguments=(dict(row.arguments_payload) if isinstance(row.arguments_payload, dict) else {}),
            source_locator=row.source_locator,
            recorded_at=row.recorded_at,
            completed_at=row.completed_at,
        )

    @classmethod
    def _build_tool_access_manifest_payload(
        cls,
        rows: list[ToolAccessAuditRow],
        *,
        export_workflow_run_id: str,
    ) -> dict[str, object]:
        items = [cls._to_tool_access_audit(row).model_dump(mode="json") for row in rows]
        execution_status_counts: dict[str, int] = defaultdict(int)
        for item in items:
            status = str(item.get("execution_status") or "unknown")
            execution_status_counts[status] += 1
        return {
            "tool_access_audit_summary": {
                "total_count": len(items),
                "export_workflow_count": sum(
                    1 for item in items if item.get("workflow_run_id") == export_workflow_run_id
                ),
                "workflow_run_ids": sorted(
                    {
                        str(item["workflow_run_id"])
                        for item in items
                        if item.get("workflow_run_id") is not None
                    }
                ),
                "tool_names": sorted(
                    {
                        str(item["tool_name"])
                        for item in items
                        if item.get("tool_name") is not None
                    }
                ),
                "user_ids": sorted(
                    {
                        str(item["user_id"])
                        for item in items
                        if item.get("user_id") is not None
                    }
                ),
                "execution_status_counts": dict(sorted(execution_status_counts.items())),
            },
            "tool_access_audit": items,
        }

    @classmethod
    def _to_tool_access_summary(cls, rows: list[ToolAccessAuditRow]) -> ToolAccessSummary:
        execution_status_counts: dict[str, int] = defaultdict(int)
        recent_tool_names: list[str] = []
        latest_completed_at: datetime | None = None
        latest_workflow_run_id: str | None = None
        for row in sorted(rows, key=lambda item: item.completed_at, reverse=True):
            execution_status_counts[row.execution_status] += 1
            if latest_completed_at is None:
                latest_completed_at = row.completed_at
                latest_workflow_run_id = row.workflow_run_id
            if row.tool_name not in recent_tool_names:
                recent_tool_names.append(row.tool_name)
            if len(recent_tool_names) >= 5:
                break
        return ToolAccessSummary(
            total_count=len(rows),
            latest_completed_at=latest_completed_at,
            latest_workflow_run_id=latest_workflow_run_id,
            recent_tool_names=recent_tool_names,
            execution_status_counts=dict(sorted(execution_status_counts.items())),
        )

    @staticmethod
    def _json_str_set(value: object) -> set[str]:
        if isinstance(value, list):
            return {str(item) for item in value if item}
        if value:
            return {str(value)}
        return set()

    @classmethod
    def _tool_access_payload_refs(
        cls,
        payload: object,
        *keys: str,
    ) -> set[str]:
        if not isinstance(payload, dict):
            return set()
        refs: set[str] = set()
        for key in keys:
            refs.update(cls._json_str_set(payload.get(key)))
        return refs

    @classmethod
    def _tool_access_row_matches_control(
        cls,
        row: ToolAccessAuditRow,
        *,
        control_state_id: str,
        control_code: str,
        mapping_ids: set[str],
    ) -> bool:
        if row.subject_type == "framework_control" and row.subject_id in {control_state_id, control_code}:
            return True
        if row.subject_type == "mapping" and row.subject_id in mapping_ids:
            return True
        control_refs = cls._tool_access_payload_refs(
            row.arguments_payload,
            "control_id",
            "control_state_id",
            "control_code",
            "control_ids",
            "control_state_ids",
            "control_codes",
        )
        if {control_state_id, control_code}.intersection(control_refs):
            return True
        mapping_refs = cls._tool_access_payload_refs(
            row.arguments_payload,
            "mapping_id",
            "mapping_ids",
        )
        return not mapping_ids.isdisjoint(mapping_refs)

    def _list_control_tool_access_rows(
        self,
        session: Session,
        *,
        organization_id: str,
        control_row: ControlCoverageRow,
        mapping_ids: set[str],
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
    ) -> list[ToolAccessAuditRow]:
        subject_filters = [
            and_(
                ToolAccessAuditRow.subject_type == "audit_cycle",
                ToolAccessAuditRow.subject_id == control_row.cycle_id,
            ),
            and_(
                ToolAccessAuditRow.subject_type == "framework_control",
                ToolAccessAuditRow.subject_id.in_([control_row.control_state_id, control_row.control_code]),
            ),
        ]
        if mapping_ids:
            subject_filters.append(
                and_(
                    ToolAccessAuditRow.subject_type == "mapping",
                    ToolAccessAuditRow.subject_id.in_(sorted(mapping_ids)),
                )
            )
        stmt = select(ToolAccessAuditRow).where(
            ToolAccessAuditRow.organization_id == organization_id
        )
        if workflow_run_id is not None:
            stmt = stmt.where(ToolAccessAuditRow.workflow_run_id == workflow_run_id)
        if user_id is not None:
            stmt = stmt.where(ToolAccessAuditRow.user_id == user_id)
        if tool_name is not None:
            stmt = stmt.where(ToolAccessAuditRow.tool_name == tool_name)
        if execution_status is not None:
            stmt = stmt.where(ToolAccessAuditRow.execution_status == execution_status)
        rows = session.scalars(
            stmt.where(or_(*subject_filters)).order_by(
                ToolAccessAuditRow.completed_at.desc(),
                ToolAccessAuditRow.tool_access_audit_id.desc(),
            )
        ).all()
        return [
            row
            for row in rows
            if self._tool_access_row_matches_control(
                row,
                control_state_id=control_row.control_state_id,
                control_code=control_row.control_code,
                mapping_ids=mapping_ids,
            )
        ]

    @classmethod
    def _tool_access_row_matches_mapping(
        cls,
        row: ToolAccessAuditRow,
        *,
        mapping_row: MappingRow,
    ) -> bool:
        if row.subject_type == "mapping" and row.subject_id == mapping_row.mapping_id:
            return True
        mapping_refs = cls._tool_access_payload_refs(
            row.arguments_payload,
            "mapping_id",
            "mapping_ids",
        )
        if mapping_row.mapping_id in mapping_refs:
            return True
        evidence_refs = cls._tool_access_payload_refs(
            row.arguments_payload,
            "evidence_item_id",
            "evidence_id",
            "evidence_item_ids",
            "evidence_ids",
        )
        control_refs = cls._tool_access_payload_refs(
            row.arguments_payload,
            "control_id",
            "control_state_id",
            "control_code",
            "control_ids",
            "control_state_ids",
            "control_codes",
        )
        return (
            mapping_row.evidence_item_id in evidence_refs
            and not {mapping_row.control_state_id, mapping_row.control_code}.isdisjoint(control_refs)
        )

    def _list_mapping_tool_access_rows(
        self,
        session: Session,
        *,
        organization_id: str,
        mapping_row: MappingRow,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
    ) -> list[ToolAccessAuditRow]:
        stmt = select(ToolAccessAuditRow).where(
            ToolAccessAuditRow.organization_id == organization_id
        )
        if workflow_run_id is not None:
            stmt = stmt.where(ToolAccessAuditRow.workflow_run_id == workflow_run_id)
        if user_id is not None:
            stmt = stmt.where(ToolAccessAuditRow.user_id == user_id)
        if tool_name is not None:
            stmt = stmt.where(ToolAccessAuditRow.tool_name == tool_name)
        if execution_status is not None:
            stmt = stmt.where(ToolAccessAuditRow.execution_status == execution_status)
        rows = session.scalars(
            stmt.where(
                or_(
                    and_(
                        ToolAccessAuditRow.subject_type == "mapping",
                        ToolAccessAuditRow.subject_id == mapping_row.mapping_id,
                    ),
                    and_(
                        ToolAccessAuditRow.subject_type == "audit_cycle",
                        ToolAccessAuditRow.subject_id == mapping_row.cycle_id,
                    ),
                    and_(
                        ToolAccessAuditRow.subject_type == "framework_control",
                        ToolAccessAuditRow.subject_id.in_(
                            [mapping_row.control_state_id, mapping_row.control_code]
                        ),
                    ),
                    and_(
                        ToolAccessAuditRow.subject_type == "audit_evidence",
                        ToolAccessAuditRow.subject_id == mapping_row.evidence_item_id,
                    ),
                )
            ).order_by(
                ToolAccessAuditRow.completed_at.desc(),
                ToolAccessAuditRow.tool_access_audit_id.desc(),
            )
        ).all()
        return [
            row
            for row in rows
            if self._tool_access_row_matches_mapping(
                row,
                mapping_row=mapping_row,
            )
        ]

    @staticmethod
    def _to_memory_record(row: MemoryRecordRow) -> MemoryRecordSummary:
        return MemoryRecordSummary(
            memory_id=row.memory_id,
            scope=row.scope,
            subject_type=row.subject_type,
            subject_id=row.subject_id,
            memory_key=row.memory_key,
            memory_type=row.memory_type,
            value=(dict(row.value_payload) if isinstance(row.value_payload, dict) else {}),
            confidence=row.confidence,
            source_kind=row.source_kind,
            source_ref=(
                dict(row.source_ref_payload)
                if isinstance(row.source_ref_payload, dict)
                else None
            ),
            status=row.status,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _to_import(row: EvidenceSourceRow) -> EvidenceImportSummary:
        return EvidenceImportSummary(
            evidence_source_id=row.evidence_source_id,
            cycle_id=row.cycle_id,
            source_type=row.source_type,
            display_name=row.display_name,
            ingest_status=row.ingest_status,
            latest_workflow_run_id=row.latest_workflow_run_id,
            artifact_id=row.artifact_id,
            connection_id=row.connection_id,
            upstream_object_id=row.upstream_object_id,
            source_locator=row.source_locator,
            captured_at=row.captured_at,
            last_synced_at=row.last_synced_at,
            metadata=row.metadata_payload,
        )

    @staticmethod
    def _to_gap(row: GapRow) -> GapSummary:
        return GapSummary(
            gap_id=row.gap_id,
            control_state_id=row.control_state_id,
            gap_type=row.gap_type,
            severity=row.severity,
            status=row.status,
            snapshot_version=row.snapshot_version,
            title=row.title,
            recommended_action=row.recommended_action,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _to_export_package(row: ExportPackageRow) -> ExportPackageSummary:
        return ExportPackageSummary(
            package_id=row.package_id,
            cycle_id=row.cycle_id,
            snapshot_version=row.snapshot_version,
            status=row.status,
            artifact_id=row.artifact_id,
            package_artifact_id=row.artifact_id,
            manifest_artifact_id=row.manifest_artifact_id,
            workflow_run_id=row.workflow_run_id,
            created_at=row.created_at,
            immutable_at=row.immutable_at,
        )

    @staticmethod
    def _to_narrative(row: NarrativeRow) -> NarrativeSummary:
        return NarrativeSummary(
            narrative_id=row.narrative_id,
            narrative_type=row.narrative_type,
            status=row.status,
            control_state_id=row.control_state_id,
            snapshot_version=row.snapshot_version,
            content_markdown=row.content_markdown,
        )

    def create_workspace(
        self,
        command: CreateWorkspaceCommand,
        *,
        organization_id: str | None = None,
    ) -> AuditWorkspaceSummary:
        workspace_id = f"audit-ws-{uuid4().hex[:10]}"
        now = self._utcnow_naive()
        slug = command.slug or _slugify(command.workspace_name)
        normalized_org_id = self._normalize_organization_id(organization_id) or DEFAULT_ORGANIZATION_ID
        with self.session_factory.begin() as session:
            existing_slug = session.scalar(
                select(AuditWorkspaceRow.workspace_id)
                .where(AuditWorkspaceRow.organization_id == normalized_org_id)
                .where(AuditWorkspaceRow.slug == slug)
                .limit(1)
            )
            if existing_slug is not None:
                raise ValueError("WORKSPACE_SLUG_ALREADY_EXISTS")
            row = AuditWorkspaceRow(
                workspace_id=workspace_id,
                organization_id=normalized_org_id,
                workspace_name=command.workspace_name,
                slug=slug,
                framework_name=command.framework_name,
                workspace_status=command.workspace_status,
                default_owner_user_id=command.default_owner_user_id,
                settings_payload=command.settings,
                created_at=now,
                updated_at=now,
            )
            session.add(row)
        return self.get_workspace(workspace_id, organization_id=normalized_org_id)

    def get_workspace(
        self,
        workspace_id: str,
        *,
        organization_id: str | None = None,
    ) -> AuditWorkspaceSummary:
        with self.session_factory() as session:
            row = self._get_workspace_row(
                session,
                workspace_id=workspace_id,
                organization_id=organization_id,
            )
            return self._to_workspace(row)

    def create_cycle(
        self,
        command: CreateCycleCommand,
        *,
        organization_id: str | None = None,
    ) -> AuditCycleSummary:
        cycle_id = f"cycle-{uuid4().hex[:10]}"
        now = self._utcnow_naive()
        with self.session_factory.begin() as session:
            workspace_row = self._get_workspace_row(
                session,
                workspace_id=command.workspace_id,
                organization_id=organization_id,
            )
            framework_name = command.framework_name or workspace_row.framework_name
            existing_cycle = session.scalar(
                select(AuditCycleRow.cycle_id)
                .where(AuditCycleRow.organization_id == workspace_row.organization_id)
                .where(AuditCycleRow.workspace_id == command.workspace_id)
                .where(AuditCycleRow.cycle_name == command.cycle_name)
                .limit(1)
            )
            if existing_cycle is not None:
                raise ValueError("CYCLE_NAME_ALREADY_EXISTS")

            cycle_row = AuditCycleRow(
                cycle_id=cycle_id,
                organization_id=workspace_row.organization_id,
                workspace_id=command.workspace_id,
                cycle_name=command.cycle_name,
                cycle_status=command.cycle_status,
                framework_name=framework_name,
                audit_period_start=command.audit_period_start,
                audit_period_end=command.audit_period_end,
                owner_user_id=command.owner_user_id or workspace_row.default_owner_user_id,
                current_snapshot_version=0,
                last_mapped_at=None,
                last_reviewed_at=None,
                coverage_status="not_started",
                review_queue_count=0,
                open_gap_count=0,
                latest_workflow_run_id=None,
                created_at=now,
                updated_at=now,
            )
            session.add(cycle_row)
            session.flush()

            self._seed_cycle_control_states(
                session,
                cycle_id=cycle_id,
                framework_name=framework_name,
            )
            session.flush()
            self._refresh_cycle_counts(session, cycle_id)
            self._record_cycle_snapshot(
                session,
                cycle_row=cycle_row,
                snapshot_version=cycle_row.current_snapshot_version,
                snapshot_status="current",
                trigger_kind="cycle_created",
                updated_at=now,
            )
            return self._to_cycle(cycle_row)

    def list_cycles(
        self,
        workspace_id: str,
        *,
        status: str | None = None,
        organization_id: str | None = None,
    ) -> list[AuditCycleSummary]:
        with self.session_factory() as session:
            workspace_row = self._get_workspace_row(
                session,
                workspace_id=workspace_id,
                organization_id=organization_id,
            )
            stmt = (
                select(AuditCycleRow)
                .where(AuditCycleRow.organization_id == workspace_row.organization_id)
                .where(AuditCycleRow.workspace_id == workspace_id)
                .order_by(AuditCycleRow.cycle_name.asc())
            )
            if status is not None:
                normalized_status = "pending_review" if status == "reviewing" else status
                stmt = stmt.where(AuditCycleRow.cycle_status == normalized_status)
            rows = session.scalars(stmt).all()
            return [self._to_cycle(row) for row in rows]

    def get_cycle_dashboard(
        self,
        cycle_id: str,
        *,
        organization_id: str | None = None,
    ) -> AuditCycleDashboardResponse:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            controls = session.scalars(
                select(ControlCoverageRow)
                .where(ControlCoverageRow.cycle_id == cycle_id)
                .order_by(ControlCoverageRow.control_code.asc())
            ).all()
            latest_export_row = session.scalars(
                select(ExportPackageRow)
                .where(ExportPackageRow.cycle_id == cycle_id)
                .order_by(ExportPackageRow.created_at.desc())
            ).first()
            tool_access_rows = session.scalars(
                select(ToolAccessAuditRow)
                .where(ToolAccessAuditRow.organization_id == cycle_row.organization_id)
                .where(ToolAccessAuditRow.subject_type == "audit_cycle")
                .where(ToolAccessAuditRow.subject_id == cycle_id)
                .order_by(ToolAccessAuditRow.completed_at.desc(), ToolAccessAuditRow.tool_access_audit_id.desc())
            ).all()
            control_models = [self._to_control(row) for row in controls]
            accepted_mapping_count = sum(1 for item in control_models if item.coverage_status == "covered")
            return AuditCycleDashboardResponse(
                cycle=self._to_cycle(cycle_row),
                review_queue_count=cycle_row.review_queue_count,
                open_gap_count=cycle_row.open_gap_count,
                accepted_mapping_count=accepted_mapping_count,
                export_ready=(
                    accepted_mapping_count > 0
                    and cycle_row.review_queue_count == 0
                    and cycle_row.open_gap_count == 0
                ),
                controls=control_models,
                latest_export_package=(
                    self._to_export_package(latest_export_row) if latest_export_row is not None else None
                ),
                tool_access_summary=self._to_tool_access_summary(list(tool_access_rows)),
            )

    def list_controls(
        self,
        cycle_id: str,
        *,
        coverage_status: str | None = None,
        search: str | None = None,
        organization_id: str | None = None,
    ) -> list[ControlCoverageSummary]:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = select(ControlCoverageRow).where(ControlCoverageRow.cycle_id == cycle_id)
            if coverage_status is not None:
                stmt = stmt.where(ControlCoverageRow.coverage_status == coverage_status)
            rows = session.scalars(stmt.order_by(ControlCoverageRow.control_code.asc())).all()
            if search is not None and search.strip():
                needle = search.strip().lower()
                catalog_by_code = {
                    row.control_code: row
                    for row in self._list_control_catalog(session, cycle_row.framework_name)
                }
                rows = [
                    row
                    for row in rows
                    if needle in row.control_code.lower()
                    or needle in str(catalog_by_code.get(row.control_code).title if row.control_code in catalog_by_code else "").lower()
                ]
            return [self._to_control(row) for row in rows]

    def list_mappings(
        self,
        cycle_id: str,
        *,
        control_state_id: str | None = None,
        mapping_status: str | None = None,
        organization_id: str | None = None,
    ) -> MappingListResponse:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = select(MappingRow).where(MappingRow.cycle_id == cycle_id)
            if control_state_id is not None:
                stmt = stmt.where(MappingRow.control_state_id == control_state_id)
            if mapping_status is not None:
                stmt = stmt.where(MappingRow.mapping_status == mapping_status)
            rows = session.scalars(stmt.order_by(MappingRow.updated_at.desc())).all()
            items = [self._to_mapping(row) for row in rows]
            return MappingListResponse(cycle_id=cycle_id, total_count=len(items), items=items)

    def get_control_detail(
        self,
        control_state_id: str,
        *,
        organization_id: str | None = None,
    ) -> ControlDetailResponse:
        with self.session_factory() as session:
            control_row, cycle_row, _workspace_row = self._get_control_scope(
                session,
                control_state_id=control_state_id,
                organization_id=organization_id,
            )
            mapping_rows = session.scalars(
                select(MappingRow)
                .where(MappingRow.control_state_id == control_state_id)
                .order_by(MappingRow.updated_at.desc())
            ).all()
            gap_rows = session.scalars(
                select(GapRow)
                .where(GapRow.control_state_id == control_state_id)
                .where(GapRow.status != "resolved")
                .order_by(GapRow.updated_at.desc())
            ).all()
            control_tool_access_rows = self._list_control_tool_access_rows(
                session,
                organization_id=cycle_row.organization_id,
                control_row=control_row,
                mapping_ids={row.mapping_id for row in mapping_rows},
            )
            accepted = [self._to_mapping(row) for row in mapping_rows if row.mapping_status == "accepted"]
            pending = [
                self._to_mapping(row)
                for row in mapping_rows
                if row.mapping_status in {"proposed", "reassigned"}
            ]
            return ControlDetailResponse(
                control_state=self._to_control(control_row),
                accepted_mappings=accepted,
                pending_mappings=pending,
                open_gaps=[self._to_gap(row) for row in gap_rows],
                tool_access_summary=self._to_tool_access_summary(control_tool_access_rows),
            )

    def get_evidence(
        self,
        evidence_id: str,
        *,
        organization_id: str | None = None,
    ) -> EvidenceDetail:
        with self.session_factory() as session:
            evidence_row, _cycle_row, _workspace_row = self._get_evidence_scope(
                session,
                evidence_id=evidence_id,
                organization_id=organization_id,
            )
            chunk_rows = session.scalars(
                select(EvidenceChunkRow)
                .where(EvidenceChunkRow.evidence_id == evidence_id)
                .order_by(EvidenceChunkRow.chunk_index.asc())
            ).all()
            return EvidenceDetail(
                evidence_id=evidence_row.evidence_id,
                audit_cycle_id=evidence_row.audit_cycle_id,
                title=evidence_row.title,
                evidence_type=evidence_row.evidence_type,
                parse_status=evidence_row.parse_status,
                captured_at=evidence_row.captured_at,
                summary=evidence_row.summary,
                source=evidence_row.source_payload,
                chunks=[
                    EvidenceChunk(
                        chunk_id=row.chunk_id,
                        chunk_index=row.chunk_index,
                        section_label=row.section_label,
                        text_excerpt=row.text_excerpt,
                    )
                    for row in chunk_rows
                ],
            )

    def search_evidence(
        self,
        *,
        cycle_id: str,
        query: str,
        limit: int = 5,
        organization_id: str | None = None,
        workspace_id: str | None = None,
    ) -> EvidenceSearchResponse:
        normalized_query = query.strip()
        if normalized_query == "":
            raise ValueError("INVALID_SEARCH_QUERY")
        normalized_limit = max(1, min(limit, 20))
        with self.session_factory() as session:
            cycle_row, workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
                workspace_id=workspace_id,
            )
            lexical_rows = session.scalars(
                select(EmbeddingChunkRow)
                .where(EmbeddingChunkRow.organization_id == cycle_row.organization_id)
                .where(EmbeddingChunkRow.workspace_id == cycle_row.workspace_id)
                .where(EmbeddingChunkRow.subject_type == "audit_evidence")
                .where(EmbeddingChunkRow.model_name == LEXICAL_MODEL_NAME)
                .order_by(EmbeddingChunkRow.created_at.desc(), EmbeddingChunkRow.chunk_index.asc())
            ).all()
            ranked_chunks: dict[str, dict[str, object]] = {}
            lexical_positive_chunk_ids: set[str] = set()
            for row in lexical_rows:
                metadata_payload = row.metadata_payload if isinstance(row.metadata_payload, dict) else {}
                if metadata_payload.get("cycle_id") != cycle_id:
                    continue
                chunk_id = str(metadata_payload.get("chunk_id") or f"{row.subject_id}:{row.chunk_index}")
                ranked = ranked_chunks.setdefault(
                    chunk_id,
                    {
                        "lexical": 0.0,
                        "semantic": 0.0,
                        "metadata": metadata_payload,
                        "text_excerpt": row.text_content,
                        "subject_id": row.subject_id,
                    },
                )
                lexical_score = self._score_search_match(
                    query=normalized_query,
                    text_content=row.text_content,
                    metadata_payload=metadata_payload,
                )
                ranked["lexical"] = max(float(ranked["lexical"]), lexical_score)
                if lexical_score > 0:
                    lexical_positive_chunk_ids.add(chunk_id)
                ranked["metadata"] = metadata_payload
                ranked["text_excerpt"] = row.text_content

            query_vector = self._embed_semantic_text(normalized_query)
            query_semantic_terms = set(self._semantic_term_weights(normalized_query))
            query_ann_keys = set(self._build_ann_bucket_keys(query_vector))
            semantic_candidate_budget = max(1, self.semantic_candidate_limit, len(lexical_positive_chunk_ids))
            selected_semantic_candidates: dict[str, dict[str, object]] = {}

            if self.vector_search_mode == "pgvector":
                pgvector_candidates = self._search_semantic_candidates_pgvector(
                    session,
                    cycle_row=cycle_row,
                    query_vector=query_vector,
                    limit=max(semantic_candidate_budget, normalized_limit),
                )
                if pgvector_candidates is not None:
                    for candidate in pgvector_candidates:
                        metadata_payload = self._coerce_json_dict(candidate.get("metadata_payload"))
                        if metadata_payload.get("cycle_id") != cycle_id:
                            continue
                        selected_semantic_candidates[str(candidate["chunk_id"])] = {
                            **candidate,
                            "metadata_payload": metadata_payload,
                        }
                    missing_chunk_ids = lexical_positive_chunk_ids - set(selected_semantic_candidates)
                    if missing_chunk_ids:
                        for row in self._load_semantic_vector_rows(
                            session,
                            cycle_row=cycle_row,
                            chunk_ids=missing_chunk_ids,
                        ):
                            candidate = self._semantic_candidate_from_row(row)
                            metadata_payload = self._coerce_json_dict(candidate["metadata_payload"])
                            if metadata_payload.get("cycle_id") != cycle_id:
                                continue
                            selected_semantic_candidates[str(candidate["chunk_id"])] = {
                                **candidate,
                                "metadata_payload": metadata_payload,
                            }

            if not selected_semantic_candidates:
                semantic_candidates: dict[str, dict[str, object]] = {}
                semantic_rows = self._load_semantic_vector_rows(
                    session,
                    cycle_row=cycle_row,
                )
                for row in semantic_rows:
                    candidate = self._semantic_candidate_from_row(row)
                    metadata_payload = self._coerce_json_dict(candidate["metadata_payload"])
                    if metadata_payload.get("cycle_id") != cycle_id:
                        continue
                    chunk_id = str(candidate["chunk_id"])
                    if self.vector_search_mode == "flat":
                        selected_semantic_candidates[chunk_id] = {
                            **candidate,
                            "metadata_payload": metadata_payload,
                        }
                        continue
                    row_ann_keys = set(candidate["ann_bucket_keys"]) or self._extract_ann_bucket_keys(
                        metadata_payload=metadata_payload,
                        text_content=str(candidate["text_content"]),
                        vector=self._coerce_vector_value(candidate["embedding_vector"]),
                    )
                    hint_score = self._estimate_semantic_candidate_hint(
                        query_terms=query_semantic_terms,
                        query_ann_keys=query_ann_keys,
                        metadata_payload=metadata_payload,
                        row_ann_keys=row_ann_keys,
                    )
                    existing_candidate = semantic_candidates.get(chunk_id)
                    if existing_candidate is None or hint_score > float(existing_candidate["hint_score"]):
                        semantic_candidates[chunk_id] = {
                            **candidate,
                            "metadata_payload": metadata_payload,
                            "hint_score": hint_score,
                        }
                selected_semantic_chunk_ids = set(lexical_positive_chunk_ids)
                for chunk_id, candidate in sorted(
                    semantic_candidates.items(),
                    key=lambda item: (
                        float(item[1]["hint_score"]),
                        item[1]["created_at"],
                    ),
                    reverse=True,
                ):
                    if chunk_id in selected_semantic_chunk_ids:
                        continue
                    if len(selected_semantic_chunk_ids) >= semantic_candidate_budget:
                        break
                    selected_semantic_chunk_ids.add(chunk_id)
                for chunk_id in selected_semantic_chunk_ids:
                    candidate = semantic_candidates.get(chunk_id)
                    if candidate is None:
                        continue
                    selected_semantic_candidates[chunk_id] = candidate

            for chunk_id, candidate in selected_semantic_candidates.items():
                metadata_payload = self._coerce_json_dict(candidate.get("metadata_payload"))
                text_excerpt = str(candidate.get("text_content") or "")
                ranked = ranked_chunks.setdefault(
                    chunk_id,
                    {
                        "lexical": 0.0,
                        "semantic": 0.0,
                        "metadata": metadata_payload,
                        "text_excerpt": text_excerpt,
                        "subject_id": str(candidate["subject_id"]),
                    },
                )
                semantic_metadata = dict(metadata_payload)
                semantic_metadata["embedding_vector"] = self._coerce_vector_value(
                    candidate.get("embedding_vector")
                )
                semantic_metadata["ann_bucket_keys"] = self._coerce_string_list(
                    candidate.get("ann_bucket_keys")
                )
                semantic_metadata["semantic_terms"] = self._coerce_string_list(
                    candidate.get("semantic_terms")
                )
                ranked["semantic"] = max(
                    float(ranked["semantic"]),
                    self._score_semantic_match(
                        query=normalized_query,
                        query_vector=query_vector,
                        text_content=text_excerpt,
                        metadata_payload=semantic_metadata,
                    ),
                )
            items: list[EvidenceSearchItem] = []
            for chunk_id, ranked in ranked_chunks.items():
                lexical_score = float(ranked["lexical"])
                semantic_score = float(ranked["semantic"])
                score = self._combine_search_scores(
                    lexical_score=lexical_score,
                    semantic_score=semantic_score,
                )
                if score <= 0:
                    continue
                metadata_payload = dict(ranked["metadata"]) if isinstance(ranked["metadata"], dict) else {}
                subject_id = str(ranked["subject_id"])
                items.append(
                    EvidenceSearchItem(
                        evidence_chunk_id=chunk_id,
                        evidence_item_id=str(metadata_payload.get("evidence_item_id") or subject_id),
                        score=score,
                        summary=str(metadata_payload.get("summary") or str(ranked["text_excerpt"])[:160]),
                        title=str(metadata_payload.get("title") or subject_id),
                        section_label=(
                            str(metadata_payload["section_label"])
                            if metadata_payload.get("section_label") is not None
                            else None
                        ),
                        text_excerpt=str(ranked["text_excerpt"]),
                        source_type=(
                            str(metadata_payload["source_type"])
                            if metadata_payload.get("source_type") is not None
                            else None
                        ),
                        captured_at=self._parse_timestamp(metadata_payload.get("captured_at")),
                    )
                )
            items.sort(
                key=lambda item: (
                    item.score,
                    item.captured_at or datetime.min,
                    item.evidence_chunk_id,
                ),
                reverse=True,
            )
            items = items[:normalized_limit]
            return EvidenceSearchResponse(
                cycle_id=cycle_id,
                workspace_id=workspace_row.workspace_id,
                query=normalized_query,
                total_count=len(items),
                items=items,
            )

    def build_cycle_processing_grounding(
        self,
        *,
        cycle_id: str,
        evidence_summary: str,
        chunk_texts: list[str],
        max_historical_hits: int = 3,
        max_memory_items: int = 5,
        organization_id: str | None = None,
    ) -> dict[str, object]:
        with self.session_factory() as session:
            cycle_row, workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )

            control_rows = session.scalars(
                select(ControlCoverageRow)
                .where(ControlCoverageRow.cycle_id == cycle_id)
                .order_by(ControlCoverageRow.control_code.asc())
            ).all()
            catalog_rows = self._list_control_catalog(session, cycle_row.framework_name)
            catalog_by_code = {row.control_code: row for row in catalog_rows}

            in_scope_controls: list[dict[str, object]] = []
            control_text_sections: list[str] = []
            framework_subject_ids: set[str] = set()
            for control_row in control_rows:
                catalog_row = catalog_by_code.get(control_row.control_code)
                subject_id = f"{cycle_row.framework_name}:{control_row.control_code}"
                framework_subject_ids.add(subject_id)
                control_payload = {
                    "control_state_id": control_row.control_state_id,
                    "control_code": control_row.control_code,
                    "coverage_status": control_row.coverage_status,
                    "title": catalog_row.title if catalog_row is not None else control_row.control_code,
                    "description": catalog_row.description if catalog_row is not None else "",
                    "guidance_markdown": catalog_row.guidance_markdown if catalog_row is not None else None,
                    "expected_evidence": (
                        list(catalog_row.common_evidence_payload)
                        if catalog_row is not None
                        else []
                    ),
                }
                in_scope_controls.append(control_payload)

                control_text_sections.append(
                    "\n".join(
                        line
                        for line in (
                            f"{control_row.control_code}: {control_payload['title']}",
                            f"Coverage status: {control_row.coverage_status}",
                            str(control_payload["description"]).strip(),
                            (
                                f"Guidance: {control_payload['guidance_markdown']}"
                                if control_payload["guidance_markdown"]
                                else ""
                            ),
                        )
                        if line
                    )
                )

            query_text = self._build_grounding_query(evidence_summary=evidence_summary, chunk_texts=chunk_texts)
            similar_hits = (
                self.search_evidence(
                    cycle_id=cycle_id,
                    query=query_text,
                    limit=max_historical_hits,
                    organization_id=cycle_row.organization_id,
                    workspace_id=cycle_row.workspace_id,
                ).items
                if query_text
                else []
            )
            historical_evidence_refs = [
                {
                    "kind": "historical_evidence_chunk",
                    "evidence_chunk_id": item.evidence_chunk_id,
                    "evidence_item_id": item.evidence_item_id,
                    "title": item.title,
                    "summary": item.summary,
                    "section_label": item.section_label,
                    "text_excerpt": item.text_excerpt,
                    "score": item.score,
                    "source_type": item.source_type,
                    "captured_at": item.captured_at.isoformat() if item.captured_at is not None else None,
                }
                for item in similar_hits
            ]

            memory_rows = session.scalars(
                select(MemoryRecordRow)
                .where(MemoryRecordRow.organization_id == cycle_row.organization_id)
                .where(MemoryRecordRow.workspace_id == cycle_row.workspace_id)
                .where(MemoryRecordRow.status == "active")
                .order_by(MemoryRecordRow.updated_at.desc(), MemoryRecordRow.memory_id.desc())
            ).all()

            mapping_memory_context: list[dict[str, object]] = []
            challenge_memory_context: list[dict[str, object]] = []
            for row in memory_rows:
                value_payload = row.value_payload if isinstance(row.value_payload, dict) else {}
                decision = str(value_payload.get("decision") or "")
                if (
                    row.scope == "organization"
                    and row.subject_type == "framework_control"
                    and row.subject_id in framework_subject_ids
                    and decision in {"accept", "reassign"}
                    and len(mapping_memory_context) < max_memory_items
                ):
                    mapping_memory_context.append(self._to_grounding_memory(row))
                if (
                    len(challenge_memory_context) >= max_memory_items
                    or decision == ""
                ):
                    continue
                if (
                    row.scope == "organization"
                    and row.subject_type == "framework_control"
                    and row.subject_id in framework_subject_ids
                    and decision in {"reject", "reassign"}
                ):
                    challenge_memory_context.append(self._to_grounding_memory(row))
                    continue
                if (
                    row.scope == "cycle"
                    and row.subject_type == "audit_cycle"
                    and row.subject_id == cycle_id
                    and decision in {"reject", "reassign", "acknowledge", "reopen_gap", "resolve_gap"}
                ):
                    challenge_memory_context.append(self._to_grounding_memory(row))

            relevant_hits = historical_evidence_refs[:2]
            mapping_payloads = [
                {
                    "control_state_id": control["control_state_id"],
                    "control_code": control["control_code"],
                    "coverage_status": control["coverage_status"],
                    "title": control["title"],
                    "description": control["description"],
                    "guidance_markdown": control["guidance_markdown"],
                    "expected_evidence": control["expected_evidence"],
                    "incoming_evidence_summary": evidence_summary,
                    "incoming_evidence_preview": chunk_texts[:2],
                    "historical_hits": relevant_hits,
                }
                for control in in_scope_controls[: min(len(in_scope_controls), 4)]
            ]

            settings_payload = workspace_row.settings_payload if isinstance(workspace_row.settings_payload, dict) else {}
            freshness_days_default = int(settings_payload.get("freshness_days_default", 90))
            return {
                "framework_name": cycle_row.framework_name,
                "in_scope_controls": in_scope_controls,
                "control_text": "\n\n".join(section for section in control_text_sections if section).strip(),
                "historical_evidence_refs": historical_evidence_refs,
                "mapping_payloads": mapping_payloads,
                "mapping_memory_context": mapping_memory_context,
                "challenge_memory_context": challenge_memory_context,
                "freshness_policy": {
                    "mode": "workspace_default",
                    "max_age_days": freshness_days_default,
                    "framework_name": cycle_row.framework_name,
                    "workspace_id": cycle_row.workspace_id,
                },
            }

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
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = (
                select(MemoryRecordRow)
                .where(MemoryRecordRow.organization_id == cycle_row.organization_id)
                .where(MemoryRecordRow.workspace_id == cycle_row.workspace_id)
            )
            if scope is not None:
                stmt = stmt.where(MemoryRecordRow.scope == scope)
            if subject_type is not None:
                stmt = stmt.where(MemoryRecordRow.subject_type == subject_type)
            if subject_id is not None:
                stmt = stmt.where(MemoryRecordRow.subject_id == subject_id)
            if memory_type is not None:
                stmt = stmt.where(MemoryRecordRow.memory_type == memory_type)
            if status is not None:
                stmt = stmt.where(MemoryRecordRow.status == status)
            rows = session.scalars(
                stmt.order_by(MemoryRecordRow.updated_at.desc(), MemoryRecordRow.memory_id.desc())
            ).all()
            items = [self._to_memory_record(row) for row in rows]
            return MemoryRecordListResponse(
                cycle_id=cycle_id,
                workspace_id=cycle_row.workspace_id,
                total_count=len(items),
                items=items,
            )

    def list_gaps(
        self,
        cycle_id: str,
        *,
        status: str | None = None,
        severity: str | None = None,
        organization_id: str | None = None,
    ) -> list[GapSummary]:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = select(GapRow).join(
                ControlCoverageRow,
                GapRow.control_state_id == ControlCoverageRow.control_state_id,
            ).where(ControlCoverageRow.cycle_id == cycle_row.cycle_id)
            if status is not None:
                stmt = stmt.where(GapRow.status == status)
            if severity is not None:
                stmt = stmt.where(GapRow.severity == severity)
            rows = session.scalars(stmt.order_by(GapRow.updated_at.desc())).all()
            return [self._to_gap(row) for row in rows]

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
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            claim_state_filter = (claim_state or "").strip().lower() or None
            stmt = (
                select(MappingRow)
                .where(MappingRow.cycle_id == cycle_row.cycle_id)
                .where(MappingRow.mapping_status.in_(("proposed", "reassigned")))
            )
            if control_state_id is not None:
                stmt = stmt.where(MappingRow.control_state_id == control_state_id)
            if severity is not None:
                gap_control_ids = select(GapRow.control_state_id).where(
                    GapRow.severity == severity,
                    GapRow.status != "resolved",
                )
                stmt = stmt.where(MappingRow.control_state_id.in_(gap_control_ids))
            mapping_rows = session.scalars(stmt).all()
            now = self._utcnow_naive()
            if claim_state_filter is not None:
                if claim_state_filter not in {"unclaimed", "claimed_by_me", "claimed_by_other"}:
                    raise ValueError("INVALID_REVIEW_QUEUE_CLAIM_STATE")
                mapping_rows = [
                    row
                    for row in mapping_rows
                    if self._claim_status(
                        row,
                        viewer_user_id=viewer_user_id,
                        now=now,
                    )
                    == claim_state_filter
                ]
            if sort == "recent":
                mapping_rows.sort(key=lambda row: row.updated_at, reverse=True)
            elif sort == "ranking":
                mapping_rows.sort(
                    key=lambda row: (
                        len(row.citation_refs or []),
                        row.updated_at,
                    ),
                    reverse=True,
                )
            elif sort == "claim":
                claim_order = {"claimed_by_me": 0, "unclaimed": 1, "claimed_by_other": 2}
                mapping_rows.sort(
                    key=lambda row: (
                        claim_order[
                            self._claim_status(
                                row,
                                viewer_user_id=viewer_user_id,
                                now=now,
                            )
                        ],
                        -int(row.updated_at.timestamp()),
                    )
                )
            else:
                raise ValueError("INVALID_REVIEW_QUEUE_SORT")
            items: list[ReviewQueueItem] = []
            for mapping_row in mapping_rows:
                control_row = session.get(ControlCoverageRow, mapping_row.control_state_id)
                mapping_tool_access_rows = self._list_mapping_tool_access_rows(
                    session,
                    organization_id=cycle_row.organization_id,
                    mapping_row=mapping_row,
                )
                items.append(
                    self._to_review_item(
                        mapping_row,
                        control_row,
                        viewer_user_id=viewer_user_id,
                        now=now,
                        tool_access_summary=self._to_tool_access_summary(mapping_tool_access_rows),
                    )
                )
            return ReviewQueueResponse(cycle_id=cycle_row.cycle_id, total_count=len(items), items=items)

    def list_review_decisions(
        self,
        cycle_id: str,
        *,
        mapping_id: str | None = None,
        gap_id: str | None = None,
        organization_id: str | None = None,
    ) -> ReviewDecisionListResponse:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = select(ReviewDecisionRow).where(ReviewDecisionRow.cycle_id == cycle_row.cycle_id)
            if mapping_id is not None:
                stmt = stmt.where(ReviewDecisionRow.mapping_id == mapping_id)
            if gap_id is not None:
                stmt = stmt.where(ReviewDecisionRow.gap_id == gap_id)
            rows = session.scalars(stmt.order_by(ReviewDecisionRow.created_at.desc())).all()
            items = [self._to_review_decision(row) for row in rows]
            return ReviewDecisionListResponse(cycle_id=cycle_row.cycle_id, total_count=len(items), items=items)

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
        with self.session_factory() as session:
            normalized_organization_id = organization_id or DEFAULT_ORGANIZATION_ID
            stmt = select(ToolAccessAuditRow).where(
                ToolAccessAuditRow.organization_id == normalized_organization_id
            )
            if workflow_run_id is not None:
                stmt = stmt.where(ToolAccessAuditRow.workflow_run_id == workflow_run_id)
            if user_id is not None:
                stmt = stmt.where(ToolAccessAuditRow.user_id == user_id)
            if tool_name is not None:
                stmt = stmt.where(ToolAccessAuditRow.tool_name == tool_name)
            if subject_type is not None:
                stmt = stmt.where(ToolAccessAuditRow.subject_type == subject_type)
            if subject_id is not None:
                stmt = stmt.where(ToolAccessAuditRow.subject_id == subject_id)
            if execution_status is not None:
                stmt = stmt.where(ToolAccessAuditRow.execution_status == execution_status)
            rows = session.scalars(
                stmt.order_by(ToolAccessAuditRow.completed_at.desc(), ToolAccessAuditRow.tool_access_audit_id.desc())
            ).all()
            items = [self._to_tool_access_audit(row) for row in rows]
            return ToolAccessAuditListResponse(total_count=len(items), items=items)

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
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = (
                select(ToolAccessAuditRow)
                .where(ToolAccessAuditRow.organization_id == cycle_row.organization_id)
                .where(ToolAccessAuditRow.subject_type == "audit_cycle")
                .where(ToolAccessAuditRow.subject_id == cycle_id)
            )
            if workflow_run_id is not None:
                stmt = stmt.where(ToolAccessAuditRow.workflow_run_id == workflow_run_id)
            if user_id is not None:
                stmt = stmt.where(ToolAccessAuditRow.user_id == user_id)
            if tool_name is not None:
                stmt = stmt.where(ToolAccessAuditRow.tool_name == tool_name)
            if execution_status is not None:
                stmt = stmt.where(ToolAccessAuditRow.execution_status == execution_status)
            rows = session.scalars(
                stmt.order_by(ToolAccessAuditRow.completed_at.desc(), ToolAccessAuditRow.tool_access_audit_id.desc())
            ).all()
            items = [self._to_tool_access_audit(row) for row in rows]
            return ToolAccessAuditListResponse(total_count=len(items), items=items)

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
        with self.session_factory() as session:
            control_row, cycle_row, _workspace_row = self._get_control_scope(
                session,
                control_state_id=control_state_id,
                organization_id=organization_id,
            )
            mapping_ids = set(
                session.scalars(
                    select(MappingRow.mapping_id).where(MappingRow.control_state_id == control_state_id)
                ).all()
            )
            rows = self._list_control_tool_access_rows(
                session,
                organization_id=cycle_row.organization_id,
                control_row=control_row,
                mapping_ids=mapping_ids,
                workflow_run_id=workflow_run_id,
                user_id=user_id,
                tool_name=tool_name,
                execution_status=execution_status,
            )
            items = [self._to_tool_access_audit(row) for row in rows]
            return ToolAccessAuditListResponse(total_count=len(items), items=items)

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
        with self.session_factory() as session:
            mapping_row, cycle_row, _workspace_row = self._get_mapping_scope(
                session,
                mapping_id=mapping_id,
                organization_id=organization_id,
            )
            rows = self._list_mapping_tool_access_rows(
                session,
                organization_id=cycle_row.organization_id,
                mapping_row=mapping_row,
                workflow_run_id=workflow_run_id,
                user_id=user_id,
                tool_name=tool_name,
                execution_status=execution_status,
            )
            items = [self._to_tool_access_audit(row) for row in rows]
            return ToolAccessAuditListResponse(total_count=len(items), items=items)

    def get_mapping_event_context(
        self,
        mapping_id: str,
        *,
        organization_id: str | None = None,
    ) -> dict[str, str]:
        with self.session_factory() as session:
            mapping_row, cycle_row, workspace_row = self._get_mapping_scope(
                session,
                mapping_id=mapping_id,
                organization_id=organization_id,
            )
            return {
                "mapping_id": mapping_id,
                "cycle_id": mapping_row.cycle_id,
                "organization_id": cycle_row.organization_id,
                "workspace_id": workspace_row.workspace_id,
                "control_state_id": mapping_row.control_state_id,
            }

    def get_gap_event_context(
        self,
        gap_id: str,
        *,
        organization_id: str | None = None,
    ) -> dict[str, str]:
        with self.session_factory() as session:
            gap_row, control_row, cycle_row, workspace_row = self._get_gap_scope(
                session,
                gap_id=gap_id,
                organization_id=organization_id,
            )
            return {
                "gap_id": gap_id,
                "cycle_id": control_row.cycle_id,
                "organization_id": cycle_row.organization_id,
                "workspace_id": workspace_row.workspace_id,
                "control_state_id": gap_row.control_state_id,
            }

    def list_imports(
        self,
        cycle_id: str,
        *,
        ingest_status: str | None = None,
        source_type: str | None = None,
        organization_id: str | None = None,
    ) -> ImportListResponse:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = select(EvidenceSourceRow).where(EvidenceSourceRow.cycle_id == cycle_row.cycle_id)
            if ingest_status is not None:
                stmt = stmt.where(EvidenceSourceRow.ingest_status == ingest_status)
            if source_type is not None:
                stmt = stmt.where(EvidenceSourceRow.source_type == source_type)
            rows = session.scalars(stmt.order_by(EvidenceSourceRow.updated_at.desc())).all()
            items = [self._to_import(row) for row in rows]
            return ImportListResponse(cycle_id=cycle_row.cycle_id, total_count=len(items), items=items)

    def create_upload_import(
        self,
        cycle_id: str,
        command: UploadImportCommand,
        *,
        organization_id: str | None = None,
    ) -> ImportAcceptedResponse:
        created_at = self._utcnow_naive()
        evidence_source_id = f"source-{uuid4().hex[:10]}"
        workflow_run_id = command.workflow_run_id or f"auditflow-import-upload-{uuid4().hex[:10]}"
        captured_at = self._normalize_timestamp(command.captured_at)
        fingerprint = self._build_source_fingerprint(
            cycle_id,
            "upload",
            command.artifact_id,
            command.source_locator,
            captured_at.isoformat() if captured_at is not None else None,
        )
        with self.session_factory.begin() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            duplicate_row = self._find_duplicate_source(
                session,
                cycle_id=cycle_id,
                source_type="upload",
                fingerprint=fingerprint,
                artifact_id=command.artifact_id,
            )
            if duplicate_row is not None:
                duplicate_row.updated_at = created_at
                return ImportAcceptedResponse(
                    workflow_run_id=workflow_run_id,
                    accepted_count=0,
                    evidence_source_ids=[],
                    artifact_id=duplicate_row.artifact_id or command.artifact_id,
                    ingest_status=duplicate_row.ingest_status,
                )
            session.add(
                EvidenceSourceRow(
                    evidence_source_id=evidence_source_id,
                    cycle_id=cycle_id,
                    source_type="upload",
                    connection_id=None,
                    artifact_id=command.artifact_id,
                    upstream_object_id=None,
                    source_locator=command.source_locator,
                    display_name=command.display_name,
                    ingest_status="pending",
                    latest_workflow_run_id=None,
                    captured_at=captured_at,
                    last_synced_at=None,
                    metadata_payload={
                        "evidence_type_hint": command.evidence_type_hint,
                        "fingerprint": fingerprint,
                    },
                    created_at=created_at,
                    updated_at=created_at,
                )
            )
        return ImportAcceptedResponse(
            workflow_run_id=workflow_run_id,
            accepted_count=1,
            evidence_source_ids=[evidence_source_id],
            artifact_id=command.artifact_id,
            ingest_status="pending",
        )

    def create_external_import(
        self,
        cycle_id: str,
        command: ExternalImportCommand,
        *,
        organization_id: str | None = None,
    ) -> ImportAcceptedResponse:
        created_at = self._utcnow_naive()
        workflow_run_id = command.workflow_run_id or f"auditflow-import-external-{uuid4().hex[:10]}"
        selectors = command.upstream_ids or [command.query or ""]
        evidence_source_ids: list[str] = []
        duplicate_row: EvidenceSourceRow | None = None
        with self.session_factory.begin() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            for selector in selectors:
                upstream_object_id = selector if command.upstream_ids else None
                source_locator = selector if command.query is None else f"{command.provider}:query"
                fingerprint = self._build_source_fingerprint(
                    cycle_id,
                    command.provider,
                    command.connection_id,
                    upstream_object_id,
                    source_locator,
                    command.query,
                )
                existing_row = self._find_duplicate_source(
                    session,
                    cycle_id=cycle_id,
                    source_type=command.provider,
                    fingerprint=fingerprint,
                    connection_id=command.connection_id,
                    upstream_object_id=upstream_object_id,
                    source_locator=source_locator,
                )
                if existing_row is not None:
                    existing_row.updated_at = created_at
                    if duplicate_row is None:
                        duplicate_row = existing_row
                    continue
                evidence_source_id = f"source-{uuid4().hex[:10]}"
                evidence_source_ids.append(evidence_source_id)
                session.add(
                    EvidenceSourceRow(
                        evidence_source_id=evidence_source_id,
                        cycle_id=cycle_id,
                        source_type=command.provider,
                        connection_id=command.connection_id,
                        artifact_id=None,
                        upstream_object_id=upstream_object_id,
                        source_locator=source_locator,
                        display_name=f"{command.provider.upper()} import {selector}",
                        ingest_status="pending",
                        latest_workflow_run_id=None,
                        captured_at=None,
                        last_synced_at=None,
                        metadata_payload={"query": command.query, "fingerprint": fingerprint},
                        created_at=created_at,
                        updated_at=created_at,
                    )
                )
        return ImportAcceptedResponse(
            workflow_run_id=workflow_run_id,
            accepted_count=len(evidence_source_ids),
            evidence_source_ids=evidence_source_ids,
            artifact_id=None,
            ingest_status=("pending" if evidence_source_ids else duplicate_row.ingest_status if duplicate_row else "pending"),
        )

    def complete_import_processing(
        self,
        *,
        cycle_id: str,
        evidence_source_id: str,
        workflow_run_id: str,
        title: str,
        evidence_type: str,
        summary: str,
        artifact_id: str | None,
        normalized_artifact_id: str | None,
        source_locator: str | None,
        captured_at: datetime | None,
        preferred_evidence_id: str | None = None,
        preferred_title: str | None = None,
        preferred_evidence_type: str | None = None,
        preferred_summary: str | None = None,
        preferred_captured_at: datetime | None = None,
        chunk_texts: list[str] | None = None,
        metadata_update: dict[str, object] | None = None,
    ) -> None:
        with self.session_factory.begin() as session:
            source_row = session.get(EvidenceSourceRow, evidence_source_id)
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if source_row is None or cycle_row is None:
                raise KeyError(evidence_source_id)
            now = self._utcnow_naive()
            source_row.ingest_status = "normalized"
            source_row.latest_workflow_run_id = workflow_run_id
            source_row.last_synced_at = now
            source_row.updated_at = now
            source_row.artifact_id = artifact_id
            merged_metadata = dict(source_row.metadata_payload or {})
            if metadata_update:
                merged_metadata.update(metadata_update)
            source_row.metadata_payload = merged_metadata

            normalized_title = preferred_title or title
            normalized_evidence_type = preferred_evidence_type or evidence_type
            normalized_summary = preferred_summary or summary
            normalized_captured_at = (
                self._normalize_timestamp(preferred_captured_at)
                or self._normalize_timestamp(captured_at)
                or now
            )
            source_row.captured_at = normalized_captured_at

            evidence_row = session.get(EvidenceRow, preferred_evidence_id) if preferred_evidence_id else None
            if evidence_row is None:
                evidence_rows = session.scalars(
                    select(EvidenceRow).where(EvidenceRow.audit_cycle_id == cycle_id)
                ).all()
                evidence_row = next(
                    (
                        row
                        for row in evidence_rows
                        if isinstance(row.source_payload, dict)
                        and row.source_payload.get("evidence_source_id") == evidence_source_id
                    ),
                    None,
                )
            normalized_chunks = chunk_texts or [normalized_summary]
            if evidence_row is None:
                evidence_id = preferred_evidence_id or f"evidence-{uuid4().hex[:10]}"
                evidence_row = EvidenceRow(
                    evidence_id=evidence_id,
                    audit_cycle_id=cycle_id,
                    source_artifact_id=artifact_id,
                    normalized_artifact_id=normalized_artifact_id,
                    title=normalized_title,
                    evidence_type=normalized_evidence_type,
                    parse_status="parsed",
                    captured_at=normalized_captured_at,
                    summary=normalized_summary,
                    source_payload={
                        "source_type": source_row.source_type,
                        "source_locator": source_locator,
                        "artifact_id": artifact_id,
                        "normalized_artifact_id": normalized_artifact_id,
                        "evidence_source_id": evidence_source_id,
                    },
                )
                session.add(evidence_row)
            else:
                evidence_row.source_artifact_id = artifact_id
                evidence_row.normalized_artifact_id = normalized_artifact_id
                evidence_row.title = normalized_title
                evidence_row.evidence_type = normalized_evidence_type
                evidence_row.parse_status = "parsed"
                evidence_row.captured_at = normalized_captured_at
                evidence_row.summary = normalized_summary
                evidence_row.source_payload = {
                    "source_type": source_row.source_type,
                    "source_locator": source_locator,
                    "artifact_id": artifact_id,
                    "normalized_artifact_id": normalized_artifact_id,
                    "evidence_source_id": evidence_source_id,
                }
                existing_chunks = session.scalars(
                    select(EvidenceChunkRow).where(EvidenceChunkRow.evidence_id == evidence_row.evidence_id)
                ).all()
                for chunk_row in existing_chunks:
                    session.delete(chunk_row)

            chunk_rows_to_index: list[EvidenceChunkRow] = []
            for chunk_index, chunk_text in enumerate(normalized_chunks):
                chunk_row = EvidenceChunkRow(
                    chunk_id=f"chunk-{uuid4().hex[:10]}",
                    evidence_id=evidence_row.evidence_id,
                    chunk_index=chunk_index,
                    section_label=f"Chunk {chunk_index + 1}",
                    text_excerpt=chunk_text,
                )
                session.add(chunk_row)
                chunk_rows_to_index.append(chunk_row)

            self._sync_embedding_chunks(
                session,
                cycle_row=cycle_row,
                evidence_row=evidence_row,
                chunk_rows=chunk_rows_to_index,
            )
            self._refresh_cycle_counts(session, cycle_id)
            cycle_row.latest_workflow_run_id = workflow_run_id
            cycle_row.last_mapped_at = now
            cycle_row.updated_at = now
            if cycle_row.cycle_status == "exported":
                cycle_row.cycle_status = "pending_review"
                cycle_row.coverage_status = "pending_review"

    def upsert_artifact_blob(
        self,
        *,
        artifact_id: str,
        artifact_type: str,
        content_text: str,
        metadata_payload: dict[str, object] | None = None,
    ) -> None:
        with self.session_factory.begin() as session:
            now = self._utcnow_naive()
            row = session.get(ArtifactBlobRow, artifact_id)
            if row is None:
                session.add(
                    ArtifactBlobRow(
                        artifact_id=artifact_id,
                        artifact_type=artifact_type,
                        content_text=content_text,
                        metadata_payload=dict(metadata_payload or {}),
                        created_at=now,
                        updated_at=now,
                    )
                )
                return
            row.artifact_type = artifact_type
            row.content_text = content_text
            row.metadata_payload = dict(metadata_payload or {})
            row.updated_at = now

    def review_mapping(
        self,
        mapping_id: str,
        command: MappingReviewCommand,
        *,
        organization_id: str | None = None,
        reviewer_id: str | None = None,
    ) -> MappingReviewResponse:
        with self.session_factory.begin() as session:
            mapping_row, cycle_row, _workspace_row = self._get_mapping_scope(
                session,
                mapping_id=mapping_id,
                organization_id=organization_id,
            )
            evidence_row = session.get(EvidenceRow, mapping_row.evidence_item_id)
            if command.expected_updated_at is not None and not self._timestamps_match(
                mapping_row.updated_at,
                command.expected_updated_at,
            ):
                raise ValueError("CONFLICT_STALE_RESOURCE")
            self._validate_review_snapshot(
                item_snapshot_version=mapping_row.snapshot_version,
                cycle_snapshot_version=cycle_row.current_snapshot_version,
                expected_snapshot_version=command.expected_snapshot_version,
            )
            if mapping_row.reviewer_locked and mapping_row.mapping_status in {"accepted", "rejected"}:
                raise ValueError("MAPPING_ALREADY_TERMINAL")
            review_time = self._utcnow_naive()
            active_claim_status = self._claim_status(
                mapping_row,
                viewer_user_id=reviewer_id,
                now=review_time,
            )
            if active_claim_status == "claimed_by_other":
                raise ValueError("REVIEW_CLAIM_CONFLICT")
            previous_status = mapping_row.mapping_status
            original_control_id = mapping_row.control_state_id
            original_control_code = mapping_row.control_code
            if command.decision == "reassign":
                if command.target_control_id is None:
                    raise ValueError("target_control_id is required for reassign")
                target_control = session.get(ControlCoverageRow, command.target_control_id)
                if target_control is None:
                    raise KeyError(command.target_control_id)
                mapping_row.control_state_id = target_control.control_state_id
                mapping_row.control_code = target_control.control_code
                mapping_row.mapping_status = "reassigned"
            elif command.decision == "accept":
                mapping_row.mapping_status = "accepted"
            else:
                mapping_row.mapping_status = "rejected"
            mapping_row.reviewer_locked = True
            self._clear_mapping_claim(mapping_row)
            mapping_row.updated_at = review_time
            cycle_row.last_reviewed_at = review_time
            cycle_row.updated_at = review_time
            decision_row = self._append_review_decision(
                session,
                cycle_id=mapping_row.cycle_id,
                mapping_id=mapping_row.mapping_id,
                gap_id=None,
                decision=command.decision,
                from_status=previous_status,
                to_status=mapping_row.mapping_status,
                comment=command.comment,
                reviewer_id=reviewer_id,
            )
            subject_control_state_id = (
                original_control_id if command.decision == "reassign" else mapping_row.control_state_id
            )
            subject_control_code = (
                original_control_code if command.decision == "reassign" else mapping_row.control_code
            )
            self._record_mapping_memories(
                session,
                cycle_row=cycle_row,
                mapping_row=mapping_row,
                evidence_row=evidence_row,
                decision_row=decision_row,
                decision=command.decision,
                comment=command.comment,
                from_status=previous_status,
                subject_control_state_id=subject_control_state_id,
                subject_control_code=subject_control_code,
            )

            for control_id in {original_control_id, mapping_row.control_state_id}:
                self._refresh_control_state(session, control_id)
                cycle_id = session.get(ControlCoverageRow, control_id).cycle_id  # type: ignore[union-attr]
                self._refresh_cycle_counts(session, cycle_id)
            next_snapshot_version = max(int(cycle_row.current_snapshot_version or 0), 0) + 1
            cycle_row.current_snapshot_version = next_snapshot_version
            self._rebase_cycle_live_snapshot(
                session,
                cycle_id=cycle_row.cycle_id,
                snapshot_version=next_snapshot_version,
            )
            self._record_cycle_snapshot(
                session,
                cycle_row=cycle_row,
                snapshot_version=next_snapshot_version,
                snapshot_status="current",
                trigger_kind="mapping_review",
                review_decision_id=decision_row.review_decision_id,
                updated_at=review_time,
            )

            control_row = session.get(ControlCoverageRow, mapping_row.control_state_id)
            if control_row is None:
                raise KeyError(mapping_row.control_state_id)
            return MappingReviewResponse(
                mapping_id=mapping_row.mapping_id,
                mapping_status=mapping_row.mapping_status,
                control_state=self._to_control(control_row),
            )

    def claim_mapping(
        self,
        mapping_id: str,
        command: MappingClaimCommand,
        *,
        organization_id: str | None = None,
        reviewer_id: str,
    ) -> MappingClaimResponse:
        with self.session_factory.begin() as session:
            mapping_row, _cycle_row, _workspace_row = self._get_mapping_scope(
                session,
                mapping_id=mapping_id,
                organization_id=organization_id,
            )
            if command.expected_updated_at is not None and not self._timestamps_match(
                mapping_row.updated_at,
                command.expected_updated_at,
            ):
                raise ValueError("CONFLICT_STALE_RESOURCE")
            if mapping_row.reviewer_locked and mapping_row.mapping_status in {"accepted", "rejected"}:
                raise ValueError("MAPPING_ALREADY_TERMINAL")
            now = self._utcnow_naive()
            claim_status = self._claim_status(
                mapping_row,
                viewer_user_id=reviewer_id,
                now=now,
            )
            if claim_status == "claimed_by_other":
                raise ValueError("REVIEW_CLAIM_CONFLICT")
            mapping_row.reviewer_claimed_by_user_id = reviewer_id
            mapping_row.reviewer_claimed_at = now
            mapping_row.reviewer_claim_expires_at = now.replace(
                microsecond=0
            ) + timedelta(seconds=self._normalize_claim_lease_seconds(command.lease_seconds))
            mapping_row.updated_at = now
            return MappingClaimResponse(
                mapping_id=mapping_row.mapping_id,
                mapping_status=mapping_row.mapping_status,
                claimed_by_user_id=mapping_row.reviewer_claimed_by_user_id,
                claimed_at=mapping_row.reviewer_claimed_at,
                claim_expires_at=mapping_row.reviewer_claim_expires_at,
                claim_status="claimed_by_me",
            )

    def release_mapping_claim(
        self,
        mapping_id: str,
        command: MappingClaimReleaseCommand,
        *,
        organization_id: str | None = None,
        reviewer_id: str,
    ) -> MappingClaimResponse:
        with self.session_factory.begin() as session:
            mapping_row, _cycle_row, _workspace_row = self._get_mapping_scope(
                session,
                mapping_id=mapping_id,
                organization_id=organization_id,
            )
            if command.expected_updated_at is not None and not self._timestamps_match(
                mapping_row.updated_at,
                command.expected_updated_at,
            ):
                raise ValueError("CONFLICT_STALE_RESOURCE")
            now = self._utcnow_naive()
            claim_status = self._claim_status(
                mapping_row,
                viewer_user_id=reviewer_id,
                now=now,
            )
            if claim_status == "claimed_by_other":
                raise ValueError("REVIEW_CLAIM_CONFLICT")
            self._clear_mapping_claim(mapping_row)
            mapping_row.updated_at = now
            return MappingClaimResponse(
                mapping_id=mapping_row.mapping_id,
                mapping_status=mapping_row.mapping_status,
                claimed_by_user_id=None,
                claimed_at=None,
                claim_expires_at=None,
                claim_status="unclaimed",
            )

    def decide_gap(
        self,
        gap_id: str,
        command: GapDecisionCommand,
        *,
        organization_id: str | None = None,
        reviewer_id: str | None = None,
    ) -> GapSummary:
        with self.session_factory.begin() as session:
            gap_row, control_row, cycle_row, _workspace_row = self._get_gap_scope(
                session,
                gap_id=gap_id,
                organization_id=organization_id,
            )
            if command.expected_updated_at is not None and not self._timestamps_match(
                gap_row.updated_at,
                command.expected_updated_at,
            ):
                raise ValueError("CONFLICT_STALE_RESOURCE")
            self._validate_review_snapshot(
                item_snapshot_version=gap_row.snapshot_version,
                cycle_snapshot_version=cycle_row.current_snapshot_version,
                expected_snapshot_version=command.expected_snapshot_version,
            )
            previous_status = gap_row.status
            if command.decision == "resolve_gap":
                if gap_row.status == "resolved":
                    raise ValueError("GAP_STATUS_CONFLICT")
                gap_row.status = "resolved"
                gap_row.resolved_at = self._utcnow_naive()
            elif command.decision == "reopen_gap":
                if gap_row.status != "resolved":
                    raise ValueError("GAP_STATUS_CONFLICT")
                gap_row.status = "open"
                gap_row.resolved_at = None
            else:
                if gap_row.status != "open":
                    raise ValueError("GAP_STATUS_CONFLICT")
                gap_row.status = "acknowledged"
                gap_row.resolved_at = None
            review_time = self._utcnow_naive()
            gap_row.updated_at = review_time
            cycle_row.last_reviewed_at = review_time
            cycle_row.updated_at = review_time
            decision_row = self._append_review_decision(
                session,
                cycle_id=control_row.cycle_id,
                mapping_id=None,
                gap_id=gap_row.gap_id,
                decision=command.decision,
                from_status=previous_status,
                to_status=gap_row.status,
                comment=command.comment,
                reviewer_id=reviewer_id,
            )
            self._record_gap_memory(
                session,
                cycle_row=cycle_row,
                control_row=control_row,
                gap_row=gap_row,
                decision_row=decision_row,
                decision=command.decision,
                comment=command.comment,
                from_status=previous_status,
            )
            self._refresh_control_state(session, control_row.control_state_id)
            self._refresh_cycle_counts(session, control_row.cycle_id)
            next_snapshot_version = max(int(cycle_row.current_snapshot_version or 0), 0) + 1
            cycle_row.current_snapshot_version = next_snapshot_version
            self._rebase_cycle_live_snapshot(
                session,
                cycle_id=cycle_row.cycle_id,
                snapshot_version=next_snapshot_version,
            )
            self._record_cycle_snapshot(
                session,
                cycle_row=cycle_row,
                snapshot_version=next_snapshot_version,
                snapshot_status="current",
                trigger_kind="gap_decision",
                review_decision_id=decision_row.review_decision_id,
                updated_at=review_time,
            )
            return self._to_gap(gap_row)

    def list_narratives(
        self,
        cycle_id: str,
        *,
        snapshot_version: int | None = None,
        narrative_type: str | None = None,
        organization_id: str | None = None,
    ) -> list[NarrativeSummary]:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = select(NarrativeRow).where(NarrativeRow.cycle_id == cycle_row.cycle_id)
            if snapshot_version is not None:
                stmt = stmt.where(NarrativeRow.snapshot_version == snapshot_version)
            if narrative_type is not None:
                stmt = stmt.where(NarrativeRow.narrative_type == narrative_type)
            rows = session.scalars(stmt.order_by(NarrativeRow.snapshot_version.desc())).all()
            return [self._to_narrative(row) for row in rows]

    def read_snapshot_refs(
        self,
        cycle_id: str,
        *,
        working_snapshot_version: int,
        organization_id: str | None = None,
    ) -> dict[str, list[str]]:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            return self._snapshot_refs(
                session,
                cycle_id=cycle_row.cycle_id,
                working_snapshot_version=working_snapshot_version,
            )

    def list_export_packages(
        self,
        cycle_id: str,
        *,
        snapshot_version: int | None = None,
        status: str | None = None,
        organization_id: str | None = None,
    ) -> list[ExportPackageSummary]:
        with self.session_factory() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            stmt = select(ExportPackageRow).where(ExportPackageRow.cycle_id == cycle_row.cycle_id)
            if snapshot_version is not None:
                stmt = stmt.where(ExportPackageRow.snapshot_version == snapshot_version)
            if status is not None:
                stmt = stmt.where(ExportPackageRow.status == status)
            rows = session.scalars(
                stmt.order_by(ExportPackageRow.created_at.desc(), ExportPackageRow.package_id.desc())
            ).all()
            return [self._to_export_package(row) for row in rows]

    def get_export_package(
        self,
        package_id: str,
        *,
        organization_id: str | None = None,
    ) -> ExportPackageSummary:
        with self.session_factory() as session:
            row, _cycle_row, _workspace_row = self._get_export_package_scope(
                session,
                package_id=package_id,
                organization_id=organization_id,
            )
            return self._to_export_package(row)

    def _upsert_workflow_mappings(
        self,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        workflow_run_id: str,
        evidence_item_id: str | None,
        mapping_output: dict[str, object] | None,
        mapping_payloads: list[dict[str, object]] | None,
        updated_at: datetime,
    ) -> set[str]:
        resolved_payloads = [dict(item) for item in mapping_payloads or [] if isinstance(item, dict)]
        if not resolved_payloads and isinstance(mapping_output, dict):
            for index, candidate in enumerate(self._to_output_dict_list(mapping_output.get("mapping_candidates"))):
                control_reference = (
                    candidate.get("control_state_id")
                    or candidate.get("control_code")
                    or candidate.get("control_id")
                )
                resolved_payloads.append(
                    {
                        "mapping_id": self._stable_workflow_entity_id(
                            "mapping",
                            workflow_run_id,
                            cycle_row.cycle_id,
                            evidence_item_id,
                            control_reference,
                            index,
                        ),
                        "control_state_id": control_reference,
                        "control_code": candidate.get("control_code"),
                        "rationale_summary": candidate.get("rationale"),
                        "citation_refs": self._citation_dicts(candidate.get("citation_refs")),
                    }
                )

        touched_control_ids: set[str] = set()
        for index, payload in enumerate(resolved_payloads):
            control_row = self._resolve_control_row_for_reference(
                session,
                cycle_id=cycle_row.cycle_id,
                control_reference=(
                    payload.get("control_state_id")
                    or payload.get("control_code")
                    or payload.get("control_id")
                ),
                mapping_reference=payload.get("mapping_id"),
            )
            if control_row is None:
                continue
            mapping_id = str(
                payload.get("mapping_id")
                or self._stable_workflow_entity_id(
                    "mapping",
                    workflow_run_id,
                    cycle_row.cycle_id,
                    evidence_item_id,
                    control_row.control_state_id,
                    index,
                )
            )
            citation_refs = self._citation_dicts(payload.get("citation_refs"))
            rationale_summary = str(
                payload.get("rationale_summary")
                or payload.get("rationale")
                or f"Workflow-generated mapping candidate for {control_row.control_code}."
            )
            mapping_row = session.get(MappingRow, mapping_id)
            if mapping_row is None:
                mapping_row = MappingRow(
                    mapping_id=mapping_id,
                    cycle_id=cycle_row.cycle_id,
                    control_state_id=control_row.control_state_id,
                    control_code=control_row.control_code,
                    mapping_status="proposed",
                    snapshot_version=max(cycle_row.current_snapshot_version, 1),
                    evidence_item_id=str(evidence_item_id or ""),
                    rationale_summary=rationale_summary,
                    citation_refs=citation_refs,
                    reviewer_locked=False,
                    updated_at=updated_at,
                )
                session.add(mapping_row)
            else:
                if mapping_row.cycle_id != cycle_row.cycle_id:
                    continue
                mapping_row.control_state_id = control_row.control_state_id
                mapping_row.control_code = control_row.control_code
                mapping_row.snapshot_version = max(cycle_row.current_snapshot_version, 1)
                if evidence_item_id:
                    mapping_row.evidence_item_id = evidence_item_id
                mapping_row.rationale_summary = rationale_summary
                mapping_row.citation_refs = citation_refs
                if not mapping_row.reviewer_locked:
                    mapping_row.mapping_status = "proposed"
                mapping_row.updated_at = updated_at
            touched_control_ids.add(control_row.control_state_id)
        return touched_control_ids

    def _upsert_workflow_gaps(
        self,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        workflow_run_id: str,
        challenge_output: dict[str, object] | None,
        updated_at: datetime,
    ) -> set[str]:
        if not isinstance(challenge_output, dict):
            return set()
        touched_control_ids: set[str] = set()
        for index, payload in enumerate(self._to_output_dict_list(challenge_output.get("gaps"))):
            control_row = self._resolve_control_row_for_reference(
                session,
                cycle_id=cycle_row.cycle_id,
                control_reference=payload.get("control_state_id") or payload.get("control_code"),
            )
            if control_row is None:
                continue
            gap_id = str(
                payload.get("gap_id")
                or self._stable_workflow_entity_id(
                    "gap",
                    workflow_run_id,
                    cycle_row.cycle_id,
                    control_row.control_state_id,
                    payload.get("gap_type"),
                    index,
                )
            )
            gap_row = session.get(GapRow, gap_id)
            if gap_row is None:
                gap_row = GapRow(
                    gap_id=gap_id,
                    control_state_id=control_row.control_state_id,
                    gap_type=str(payload.get("gap_type") or "coverage_gap"),
                    severity=str(payload.get("severity") or "medium"),
                    status="open",
                    snapshot_version=max(cycle_row.current_snapshot_version, 1),
                    title=str(payload.get("title") or f"{control_row.control_code} requires reviewer attention"),
                    recommended_action=str(
                        payload.get("recommended_action")
                        or "Review the generated finding and attach current supporting evidence."
                    ),
                    resolved_at=None,
                    updated_at=updated_at,
                )
                session.add(gap_row)
            else:
                gap_row.control_state_id = control_row.control_state_id
                gap_row.gap_type = str(payload.get("gap_type") or gap_row.gap_type)
                gap_row.severity = str(payload.get("severity") or gap_row.severity)
                gap_row.snapshot_version = max(cycle_row.current_snapshot_version, 1)
                gap_row.title = str(payload.get("title") or gap_row.title)
                gap_row.recommended_action = str(
                    payload.get("recommended_action")
                    or gap_row.recommended_action
                )
                gap_row.updated_at = updated_at
            touched_control_ids.add(control_row.control_state_id)
        return touched_control_ids

    def _upsert_workflow_narratives(
        self,
        session: Session,
        *,
        cycle_row: AuditCycleRow,
        workflow_run_id: str,
        snapshot_version: int,
        writer_output: dict[str, object] | None,
        narrative_ids: list[str] | None,
    ) -> None:
        if not isinstance(writer_output, dict):
            return
        for index, payload in enumerate(self._to_output_dict_list(writer_output.get("narratives"))):
            citation_refs = self._citation_dicts(payload.get("citation_refs"))
            mapping_reference = next(
                (
                    str(item.get("id"))
                    for item in citation_refs
                    if str(item.get("kind") or "") == "mapping" and item.get("id") is not None
                ),
                None,
            )
            control_row = self._resolve_control_row_for_reference(
                session,
                cycle_id=cycle_row.cycle_id,
                control_reference=payload.get("control_state_id"),
                mapping_reference=mapping_reference,
            )
            if control_row is None:
                continue
            narrative_id = None
            if narrative_ids is not None and index < len(narrative_ids):
                candidate_narrative_id = narrative_ids[index]
                if candidate_narrative_id:
                    narrative_id = str(candidate_narrative_id)
            if narrative_id is None:
                narrative_id = self._stable_workflow_entity_id(
                    "narrative",
                    workflow_run_id,
                    cycle_row.cycle_id,
                    snapshot_version,
                    control_row.control_state_id,
                    payload.get("narrative_type"),
                    index,
                )
            narrative_row = session.get(NarrativeRow, narrative_id)
            if narrative_row is None:
                narrative_row = NarrativeRow(
                    narrative_id=narrative_id,
                    cycle_id=cycle_row.cycle_id,
                    control_state_id=control_row.control_state_id,
                    narrative_type=str(payload.get("narrative_type") or "control_summary"),
                    snapshot_version=snapshot_version,
                    status="draft",
                    content_markdown=str(
                        payload.get("content_markdown")
                        or f"Control {control_row.control_code} is supported by accepted evidence."
                    ),
                )
                session.add(narrative_row)
            else:
                narrative_row.cycle_id = cycle_row.cycle_id
                narrative_row.control_state_id = control_row.control_state_id
                narrative_row.narrative_type = str(
                    payload.get("narrative_type") or narrative_row.narrative_type
                )
                narrative_row.snapshot_version = snapshot_version
                narrative_row.status = "draft"
                narrative_row.content_markdown = str(
                    payload.get("content_markdown") or narrative_row.content_markdown
                )

    def record_cycle_processing_result(
        self,
        cycle_id: str,
        workflow_run_id: str,
        checkpoint_seq: int,
        *,
        organization_id: str | None = None,
        evidence_item_id: str | None = None,
        mapping_output: dict[str, object] | None = None,
        challenge_output: dict[str, object] | None = None,
        mapping_payloads: list[dict[str, object]] | None = None,
    ) -> None:
        with self.session_factory.begin() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            processed_at = self._utcnow_naive()
            if checkpoint_seq > 0:
                next_snapshot_version = max(int(cycle_row.current_snapshot_version or 0), 0) + 1
                cycle_row.current_snapshot_version = next_snapshot_version
                self._rebase_cycle_live_snapshot(
                    session,
                    cycle_id=cycle_row.cycle_id,
                    snapshot_version=next_snapshot_version,
                )
            touched_control_ids = self._upsert_workflow_mappings(
                session,
                cycle_row=cycle_row,
                workflow_run_id=workflow_run_id,
                evidence_item_id=evidence_item_id,
                mapping_output=mapping_output,
                mapping_payloads=mapping_payloads,
                updated_at=processed_at,
            )
            touched_control_ids.update(
                self._upsert_workflow_gaps(
                    session,
                    cycle_row=cycle_row,
                    workflow_run_id=workflow_run_id,
                    challenge_output=challenge_output,
                    updated_at=processed_at,
                )
            )
            for control_state_id in touched_control_ids:
                self._refresh_control_state(session, control_state_id)
            cycle_row.cycle_status = "pending_review"
            cycle_row.coverage_status = "pending_review"
            cycle_row.latest_workflow_run_id = workflow_run_id
            cycle_row.last_mapped_at = processed_at
            cycle_row.updated_at = processed_at
            self._refresh_cycle_counts(session, cycle_id)
            if checkpoint_seq > 0:
                self._record_cycle_snapshot(
                    session,
                    cycle_row=cycle_row,
                    snapshot_version=cycle_row.current_snapshot_version,
                    snapshot_status="current",
                    trigger_kind="cycle_processing",
                    workflow_run_id=workflow_run_id,
                    updated_at=processed_at,
                )
            if checkpoint_seq > 0 and cycle_row.review_queue_count == 0 and cycle_row.open_gap_count == 0:
                cycle_row.cycle_status = "reviewed"
                cycle_row.coverage_status = "covered"

    def record_export_result(
        self,
        *,
        cycle_id: str,
        workflow_run_id: str,
        snapshot_version: int,
        checkpoint_seq: int,
        organization_id: str | None = None,
        writer_output: dict[str, object] | None = None,
        narrative_ids: list[str] | None = None,
    ) -> ExportPackageSummary:
        created_at = datetime.now(UTC)
        created_at_naive = self._normalize_timestamp(created_at) or self._utcnow_naive()
        package_id = f"pkg-{uuid4().hex[:10]}"
        artifact_id = f"artifact-export-{cycle_id}-{snapshot_version}"
        manifest_artifact_id = f"{artifact_id}-manifest"
        immutable_at = created_at if checkpoint_seq > 0 else None
        with self.session_factory.begin() as session:
            cycle_row, _workspace_row = self._get_cycle_scope(
                session,
                cycle_id=cycle_id,
                organization_id=organization_id,
            )
            cycle_row.cycle_status = "exported"
            cycle_row.coverage_status = "covered"
            cycle_row.latest_workflow_run_id = workflow_run_id
            cycle_row.current_snapshot_version = max(cycle_row.current_snapshot_version, snapshot_version)
            cycle_row.review_queue_count = 0
            cycle_row.updated_at = self._normalize_timestamp(created_at) or self._utcnow_naive()

            control_rows = session.scalars(
                select(ControlCoverageRow).where(ControlCoverageRow.cycle_id == cycle_id)
            ).all()
            mapping_rows = session.scalars(
                select(MappingRow)
                .where(MappingRow.cycle_id == cycle_id)
                .where(MappingRow.mapping_status == "accepted")
                .order_by(MappingRow.control_code.asc(), MappingRow.mapping_id.asc())
            ).all()
            gap_rows = session.scalars(
                select(GapRow)
                .join(ControlCoverageRow, GapRow.control_state_id == ControlCoverageRow.control_state_id)
                .where(ControlCoverageRow.cycle_id == cycle_id)
                .where(GapRow.status != "resolved")
                .order_by(GapRow.severity.asc(), GapRow.gap_id.asc())
            ).all()
            for row in control_rows:
                row.coverage_status = "covered"

            self._upsert_workflow_narratives(
                session,
                cycle_row=cycle_row,
                workflow_run_id=workflow_run_id,
                snapshot_version=snapshot_version,
                writer_output=writer_output,
                narrative_ids=narrative_ids,
            )

            for control_row in control_rows:
                narrative_exists = session.scalar(
                    select(NarrativeRow.narrative_id)
                    .where(NarrativeRow.cycle_id == cycle_id)
                    .where(NarrativeRow.control_state_id == control_row.control_state_id)
                    .where(NarrativeRow.snapshot_version == snapshot_version)
                    .limit(1)
                )
                if narrative_exists is None:
                    session.add(
                        NarrativeRow(
                            narrative_id=f"narrative-{uuid4().hex[:10]}",
                            cycle_id=cycle_id,
                            control_state_id=control_row.control_state_id,
                            narrative_type="control_summary",
                            snapshot_version=snapshot_version,
                            status="draft",
                            content_markdown=(
                                f"Control {control_row.control_code} is supported by accepted evidence for snapshot "
                                f"{snapshot_version}."
                            ),
                        )
                    )

            narrative_rows = session.scalars(
                select(NarrativeRow)
                .where(NarrativeRow.cycle_id == cycle_id)
                .where(NarrativeRow.snapshot_version == snapshot_version)
                .order_by(NarrativeRow.control_state_id.asc())
            ).all()
            tool_access_rows = session.scalars(
                select(ToolAccessAuditRow)
                .where(ToolAccessAuditRow.organization_id == cycle_row.organization_id)
                .where(ToolAccessAuditRow.subject_type == "audit_cycle")
                .where(ToolAccessAuditRow.subject_id == cycle_id)
                .where(ToolAccessAuditRow.completed_at <= created_at_naive)
                .order_by(ToolAccessAuditRow.completed_at.asc(), ToolAccessAuditRow.tool_access_audit_id.asc())
            ).all()
            tool_access_payload = self._build_tool_access_manifest_payload(
                list(tool_access_rows),
                export_workflow_run_id=workflow_run_id,
            )
            manifest_payload = {
                "package_id": package_id,
                "cycle_id": cycle_id,
                "snapshot_version": snapshot_version,
                "workflow_run_id": workflow_run_id,
                "created_at": created_at.isoformat().replace("+00:00", "Z"),
                "immutable_at": (
                    immutable_at.isoformat().replace("+00:00", "Z")
                    if immutable_at is not None
                    else None
                ),
                "controls": [
                    {
                        "control_state_id": row.control_state_id,
                        "control_code": row.control_code,
                        "coverage_status": row.coverage_status,
                    }
                    for row in sorted(control_rows, key=lambda item: item.control_code)
                ],
                "accepted_mappings": [
                    {
                        "mapping_id": row.mapping_id,
                        "control_state_id": row.control_state_id,
                        "control_code": row.control_code,
                        "evidence_item_id": row.evidence_item_id,
                    }
                    for row in mapping_rows
                ],
                "open_gaps": [
                    {
                        "gap_id": row.gap_id,
                        "control_state_id": row.control_state_id,
                        "severity": row.severity,
                        "status": row.status,
                    }
                    for row in gap_rows
                ],
                "narratives": [
                    {
                        "narrative_id": row.narrative_id,
                        "control_state_id": row.control_state_id,
                        "narrative_type": row.narrative_type,
                    }
                    for row in narrative_rows
                ],
                **tool_access_payload,
            }
            package_payload = {
                "package_id": package_id,
                "cycle_id": cycle_id,
                "snapshot_version": snapshot_version,
                "manifest_artifact_id": manifest_artifact_id,
                "tool_access_audit_count": len(tool_access_payload["tool_access_audit"]),
                "narrative_markdown": [
                    {
                        "control_state_id": row.control_state_id,
                        "content_markdown": row.content_markdown,
                    }
                    for row in narrative_rows
                ],
            }
            session.merge(
                ArtifactBlobRow(
                    artifact_id=artifact_id,
                    artifact_type="audit_export_package",
                    content_text=json.dumps(package_payload, sort_keys=True),
                    metadata_payload={
                        "package_id": package_id,
                        "cycle_id": cycle_id,
                        "snapshot_version": snapshot_version,
                        "manifest_artifact_id": manifest_artifact_id,
                    },
                    created_at=created_at,
                    updated_at=created_at,
                )
            )
            session.merge(
                ArtifactBlobRow(
                    artifact_id=manifest_artifact_id,
                    artifact_type="audit_export_manifest",
                    content_text=json.dumps(manifest_payload, sort_keys=True),
                    metadata_payload={
                        "package_id": package_id,
                        "cycle_id": cycle_id,
                        "snapshot_version": snapshot_version,
                    },
                    created_at=created_at,
                    updated_at=created_at,
                )
            )

            session.add(
                ExportPackageRow(
                    package_id=package_id,
                    cycle_id=cycle_id,
                    snapshot_version=snapshot_version,
                    status="ready" if checkpoint_seq > 0 else "queued",
                    artifact_id=artifact_id,
                    manifest_artifact_id=manifest_artifact_id,
                    workflow_run_id=workflow_run_id,
                    created_at=created_at,
                    immutable_at=immutable_at,
                )
            )
            self._record_cycle_snapshot(
                session,
                cycle_row=cycle_row,
                snapshot_version=snapshot_version,
                snapshot_status="frozen" if immutable_at is not None else "current",
                trigger_kind="export",
                workflow_run_id=workflow_run_id,
                package_id=package_id,
                frozen_at=immutable_at,
                updated_at=self._normalize_timestamp(created_at) or self._utcnow_naive(),
            )
        return ExportPackageSummary(
            package_id=package_id,
            cycle_id=cycle_id,
            snapshot_version=snapshot_version,
            status="ready" if checkpoint_seq > 0 else "queued",
            artifact_id=artifact_id,
            package_artifact_id=artifact_id,
            manifest_artifact_id=manifest_artifact_id,
            workflow_run_id=workflow_run_id,
            created_at=created_at,
            immutable_at=immutable_at,
        )

    @staticmethod
    def _normalize_timestamp(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)

    @classmethod
    def _utcnow_naive(cls) -> datetime:
        return cls._normalize_timestamp(datetime.now(UTC))  # type: ignore[return-value]

    @classmethod
    def _timestamps_match(cls, stored: datetime, expected: datetime) -> bool:
        return cls._normalize_timestamp(stored) == cls._normalize_timestamp(expected)

    @staticmethod
    def _idempotency_record_id(operation: str, idempotency_key: str) -> str:
        return f"{operation}:{idempotency_key}"

    def load_idempotency_response(
        self,
        *,
        operation: str,
        idempotency_key: str,
        request_hash: str,
    ) -> dict[str, object] | None:
        record_id = self._idempotency_record_id(operation, idempotency_key)
        with self.session_factory() as session:
            row = session.get(IdempotencyKeyRow, record_id)
            if row is None:
                return None
            if row.request_hash != request_hash:
                raise ValueError("IDEMPOTENCY_CONFLICT")
            payload = row.response_payload if isinstance(row.response_payload, dict) else {}
            return dict(payload)

    def store_idempotency_response(
        self,
        *,
        operation: str,
        idempotency_key: str,
        request_hash: str,
        response_payload: dict[str, object],
    ) -> None:
        now = self._utcnow_naive()
        record_id = self._idempotency_record_id(operation, idempotency_key)
        with self.session_factory.begin() as session:
            session.merge(
                IdempotencyKeyRow(
                    record_id=record_id,
                    operation=operation,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    response_payload=dict(response_payload),
                    created_at=now,
                    updated_at=now,
                )
            )

    @staticmethod
    def _refresh_control_state(session: Session, control_state_id: str) -> None:
        control_row = session.get(ControlCoverageRow, control_state_id)
        if control_row is None:
            return
        mappings = session.scalars(
            select(MappingRow).where(MappingRow.control_state_id == control_state_id)
        ).all()
        gaps = session.scalars(
            select(GapRow).where(GapRow.control_state_id == control_state_id).where(GapRow.status != "resolved")
        ).all()
        accepted_count = sum(1 for row in mappings if row.mapping_status == "accepted")
        pending_count = sum(1 for row in mappings if row.mapping_status in {"proposed", "reassigned"})
        control_row.mapped_evidence_count = accepted_count
        control_row.open_gap_count = len(gaps)
        if accepted_count > 0 and len(gaps) == 0:
            control_row.coverage_status = "covered"
        elif pending_count > 0:
            control_row.coverage_status = "pending_review"
        elif len(gaps) > 0:
            control_row.coverage_status = "needs_attention"
        else:
            control_row.coverage_status = "not_started"

    @staticmethod
    def _refresh_cycle_counts(session: Session, cycle_id: str) -> None:
        cycle_row = session.get(AuditCycleRow, cycle_id)
        if cycle_row is None:
            return
        control_rows = session.scalars(
            select(ControlCoverageRow).where(ControlCoverageRow.cycle_id == cycle_id)
        ).all()
        review_count = len(
            session.scalars(
                select(MappingRow.mapping_id)
                .where(MappingRow.cycle_id == cycle_id)
                .where(MappingRow.mapping_status.in_(("proposed", "reassigned")))
            ).all()
        )
        gap_count = len(
            session.scalars(
                select(GapRow.gap_id).join(
                    ControlCoverageRow,
                    GapRow.control_state_id == ControlCoverageRow.control_state_id,
                )
                .where(ControlCoverageRow.cycle_id == cycle_id)
                .where(GapRow.status != "resolved")
            ).all()
        )
        cycle_row.review_queue_count = review_count
        cycle_row.open_gap_count = gap_count
        if len(control_rows) == 0:
            cycle_row.coverage_status = "not_started"
            if cycle_row.cycle_status != "exported":
                cycle_row.cycle_status = "draft"
        elif all(row.coverage_status == "covered" for row in control_rows) and review_count == 0 and gap_count == 0:
            cycle_row.coverage_status = "covered"
            if cycle_row.cycle_status != "exported":
                cycle_row.cycle_status = "reviewed"
        elif all(row.coverage_status == "not_started" for row in control_rows) and review_count == 0 and gap_count == 0:
            cycle_row.coverage_status = "not_started"
            if cycle_row.cycle_status != "exported":
                cycle_row.cycle_status = "draft"
        elif review_count > 0:
            cycle_row.coverage_status = "pending_review"
            if cycle_row.cycle_status != "exported":
                cycle_row.cycle_status = "pending_review"
        else:
            cycle_row.coverage_status = "needs_attention"
            if cycle_row.cycle_status != "exported":
                cycle_row.cycle_status = "pending_review"

    @classmethod
    def _seed_control_catalog(cls, session: Session) -> None:
        now = cls._utcnow_naive()
        for framework_name, templates in CONTROL_CATALOG_SEEDS.items():
            existing_codes = set(
                session.scalars(
                    select(ControlCatalogRow.control_code).where(
                        ControlCatalogRow.framework_name == framework_name
                    )
                ).all()
            )
            for template in templates:
                control_code = str(template["control_code"])
                if control_code in existing_codes:
                    continue
                session.add(
                    ControlCatalogRow(
                        control_id=f"control-catalog-{framework_name.lower()}-{control_code.lower()}",
                        framework_name=framework_name,
                        control_code=control_code,
                        domain_name=str(template["domain_name"]),
                        title=str(template["title"]),
                        description=str(template["description"]),
                        guidance_markdown=str(template["guidance_markdown"]),
                        common_evidence_payload=list(template["common_evidence_payload"]),
                        is_active=True,
                        sort_order=int(template["sort_order"]),
                        created_at=now,
                        updated_at=now,
                    )
                )

    @staticmethod
    def _list_control_catalog(session: Session, framework_name: str) -> list[ControlCatalogRow]:
        return session.scalars(
            select(ControlCatalogRow)
            .where(ControlCatalogRow.framework_name == framework_name)
            .where(ControlCatalogRow.is_active.is_(True))
            .order_by(ControlCatalogRow.sort_order.asc(), ControlCatalogRow.control_code.asc())
        ).all()

    @classmethod
    def _seed_cycle_control_states(
        cls,
        session: Session,
        *,
        cycle_id: str,
        framework_name: str,
        fixed_state_ids: dict[str, str] | None = None,
        state_overrides: dict[str, dict[str, object]] | None = None,
    ) -> None:
        control_templates = cls._list_control_catalog(session, framework_name)
        if not control_templates:
            raise ValueError("CONTROL_TEMPLATES_NOT_FOUND")
        fixed_state_ids = fixed_state_ids or {}
        state_overrides = state_overrides or {}
        for template in control_templates:
            overrides = state_overrides.get(template.control_code, {})
            session.add(
                ControlCoverageRow(
                    control_state_id=fixed_state_ids.get(
                        template.control_code,
                        f"control-state-{uuid4().hex[:10]}",
                    ),
                    cycle_id=cycle_id,
                    control_code=template.control_code,
                    coverage_status=str(overrides.get("coverage_status", "not_started")),
                    mapped_evidence_count=int(overrides.get("mapped_evidence_count", 0)),
                    open_gap_count=int(overrides.get("open_gap_count", 0)),
                )
            )

    @staticmethod
    def _build_source_fingerprint(*parts: object | None) -> str:
        normalized = "||".join("" if part is None else str(part) for part in parts)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def _find_duplicate_source(
        session: Session,
        *,
        cycle_id: str,
        source_type: str,
        fingerprint: str,
        artifact_id: str | None = None,
        connection_id: str | None = None,
        upstream_object_id: str | None = None,
        source_locator: str | None = None,
    ) -> EvidenceSourceRow | None:
        candidate_rows = session.scalars(
            select(EvidenceSourceRow)
            .where(EvidenceSourceRow.cycle_id == cycle_id)
            .where(EvidenceSourceRow.source_type == source_type)
        ).all()
        for row in candidate_rows:
            metadata_payload = row.metadata_payload if isinstance(row.metadata_payload, dict) else {}
            if metadata_payload.get("fingerprint") == fingerprint:
                return row
            if artifact_id is not None and row.artifact_id == artifact_id:
                return row
            if (
                connection_id is not None
                and row.connection_id == connection_id
                and upstream_object_id is not None
                and row.upstream_object_id == upstream_object_id
            ):
                return row
            if (
                connection_id is not None
                and row.connection_id == connection_id
                and upstream_object_id is None
                and source_locator is not None
                and row.source_locator == source_locator
            ):
                return row
        return None

    @classmethod
    def _append_review_decision(
        cls,
        session: Session,
        *,
        cycle_id: str,
        mapping_id: str | None,
        gap_id: str | None,
        decision: str,
        from_status: str | None,
        to_status: str | None,
        comment: str | None,
        reviewer_id: str | None = None,
    ) -> ReviewDecisionRow:
        row = ReviewDecisionRow(
            review_decision_id=f"review-decision-{uuid4().hex[:10]}",
            cycle_id=cycle_id,
            mapping_id=mapping_id,
            gap_id=gap_id,
            decision=decision,
            from_status=from_status,
            to_status=to_status,
            reviewer_id=(reviewer_id or DEFAULT_REVIEWER_ID),
            comment=comment or None,
            feedback_tags=cls._decision_feedback_tags(decision, to_status),
            created_at=cls._utcnow_naive(),
        )
        session.add(row)
        return row

    @staticmethod
    def _decision_feedback_tags(decision: str, to_status: str | None) -> list[str]:
        tags = [f"decision:{decision}"]
        if to_status:
            tags.append(f"status:{to_status}")
        return tags

    @staticmethod
    def _validate_review_snapshot(
        *,
        item_snapshot_version: int,
        cycle_snapshot_version: int,
        expected_snapshot_version: int | None,
    ) -> None:
        if item_snapshot_version != cycle_snapshot_version:
            raise ValueError("CONFLICT_STALE_RESOURCE")
        if expected_snapshot_version is not None and expected_snapshot_version != item_snapshot_version:
            raise ValueError("CONFLICT_STALE_RESOURCE")
