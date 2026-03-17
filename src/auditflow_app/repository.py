from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Integer, String, Text, UniqueConstraint, select
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
    ExternalImportCommand,
    ExportPackageSummary,
    GapDecisionCommand,
    GapSummary,
    ImportAcceptedResponse,
    ImportListResponse,
    MappingListResponse,
    MappingReviewCommand,
    MappingReviewResponse,
    MappingSummary,
    NarrativeSummary,
    ReviewDecisionListResponse,
    ReviewDecisionSummary,
    ReviewQueueItem,
    ReviewQueueResponse,
    UploadImportCommand,
)


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


class AuditFlowRepository(Protocol):
    def create_workspace(self, command: CreateWorkspaceCommand) -> AuditWorkspaceSummary: ...

    def get_workspace(self, workspace_id: str) -> AuditWorkspaceSummary: ...

    def create_cycle(self, command: CreateCycleCommand) -> AuditCycleSummary: ...

    def list_cycles(self, workspace_id: str) -> list[AuditCycleSummary]: ...

    def get_cycle_dashboard(self, cycle_id: str) -> AuditCycleDashboardResponse: ...

    def list_controls(
        self,
        cycle_id: str,
        *,
        coverage_status: str | None = None,
        search: str | None = None,
    ) -> list[ControlCoverageSummary]: ...

    def list_mappings(
        self,
        cycle_id: str,
        *,
        control_state_id: str | None = None,
        mapping_status: str | None = None,
    ) -> MappingListResponse: ...

    def get_control_detail(self, control_state_id: str) -> ControlDetailResponse: ...

    def get_evidence(self, evidence_id: str) -> EvidenceDetail: ...

    def list_gaps(
        self,
        cycle_id: str,
        *,
        status: str | None = None,
        severity: str | None = None,
    ) -> list[GapSummary]: ...

    def list_review_queue(self, cycle_id: str) -> ReviewQueueResponse: ...

    def list_review_decisions(
        self,
        cycle_id: str,
        *,
        mapping_id: str | None = None,
        gap_id: str | None = None,
    ) -> ReviewDecisionListResponse: ...

    def review_mapping(self, mapping_id: str, command: MappingReviewCommand) -> MappingReviewResponse: ...

    def list_imports(
        self,
        cycle_id: str,
        *,
        ingest_status: str | None = None,
        source_type: str | None = None,
    ) -> ImportListResponse: ...

    def create_upload_import(self, cycle_id: str, command: UploadImportCommand) -> ImportAcceptedResponse: ...

    def create_external_import(self, cycle_id: str, command: ExternalImportCommand) -> ImportAcceptedResponse: ...

    def decide_gap(self, gap_id: str, command: GapDecisionCommand) -> GapSummary: ...

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
    ) -> list[NarrativeSummary]: ...

    def get_export_package(self, package_id: str) -> ExportPackageSummary: ...

    def record_cycle_processing_result(self, cycle_id: str, workflow_run_id: str, checkpoint_seq: int) -> None: ...

    def record_export_result(
        self,
        *,
        cycle_id: str,
        workflow_run_id: str,
        snapshot_version: int,
        checkpoint_seq: int,
    ) -> ExportPackageSummary: ...


class Base(DeclarativeBase):
    pass


class AuditWorkspaceRow(Base):
    __tablename__ = "auditflow_workspace"

    workspace_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workspace_name: Mapped[str] = mapped_column(String(255))
    framework_name: Mapped[str] = mapped_column(String(50))
    workspace_status: Mapped[str] = mapped_column(String(50))


class AuditCycleRow(Base):
    __tablename__ = "auditflow_cycle"

    cycle_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workspace_id: Mapped[str] = mapped_column(String(255), index=True)
    cycle_name: Mapped[str] = mapped_column(String(255))
    cycle_status: Mapped[str] = mapped_column(String(50))
    framework_name: Mapped[str] = mapped_column(String(50))
    coverage_status: Mapped[str] = mapped_column(String(50))
    review_queue_count: Mapped[int] = mapped_column(Integer, default=0)
    open_gap_count: Mapped[int] = mapped_column(Integer, default=0)
    latest_workflow_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)


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
    evidence_item_id: Mapped[str] = mapped_column(String(255))
    rationale_summary: Mapped[str] = mapped_column(Text)
    citation_refs: Mapped[list[dict]] = mapped_column(JSON)
    reviewer_locked: Mapped[bool] = mapped_column(default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


class GapRow(Base):
    __tablename__ = "auditflow_gap"

    gap_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    control_state_id: Mapped[str] = mapped_column(String(255), index=True)
    gap_type: Mapped[str] = mapped_column(String(100))
    severity: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(50))
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


class ArtifactBlobRow(Base):
    __tablename__ = "auditflow_artifact_blob"

    artifact_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    artifact_type: Mapped[str] = mapped_column(String(80))
    content_text: Mapped[str] = mapped_column(Text)
    metadata_payload: Mapped[dict] = mapped_column(JSON)
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


class NarrativeRow(Base):
    __tablename__ = "auditflow_narrative"

    narrative_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    control_state_id: Mapped[str] = mapped_column(String(255), index=True)
    narrative_type: Mapped[str] = mapped_column(String(100))
    snapshot_version: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(50))
    content_markdown: Mapped[str] = mapped_column(Text)


class ExportPackageRow(Base):
    __tablename__ = "auditflow_export_package"

    package_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(255), index=True)
    snapshot_version: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(50))
    artifact_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    workflow_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False))


def create_auditflow_tables(engine: Engine) -> None:
    Base.metadata.create_all(engine)


class SqlAlchemyAuditFlowRepository:
    def __init__(self, session_factory: sessionmaker[Session], engine: Engine) -> None:
        self.session_factory = session_factory
        self.engine = engine
        create_auditflow_tables(engine)
        self.seed_if_empty()

    @classmethod
    def from_runtime_stores(cls, runtime_stores) -> "SqlAlchemyAuditFlowRepository":
        return cls(runtime_stores.session_factory, runtime_stores.engine)

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
                    workspace_name="Acme Security Workspace",
                    framework_name="SOC2",
                    workspace_status="active",
                )
            )
            session.add(
                AuditCycleRow(
                    cycle_id="cycle-1",
                    workspace_id="audit-ws-1",
                    cycle_name="SOC2 2026",
                    cycle_status="pending_review",
                    framework_name="SOC2",
                    coverage_status="pending_review",
                    review_queue_count=1,
                    open_gap_count=1,
                    latest_workflow_run_id=None,
                )
            )
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
                    artifact_id=None,
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
                    source_artifact_id=None,
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

    @staticmethod
    def _to_workspace(row: AuditWorkspaceRow) -> AuditWorkspaceSummary:
        return AuditWorkspaceSummary(
            workspace_id=row.workspace_id,
            workspace_name=row.workspace_name,
            framework_name=row.framework_name,
            workspace_status=row.workspace_status,
        )

    @staticmethod
    def _to_cycle(row: AuditCycleRow) -> AuditCycleSummary:
        return AuditCycleSummary(
            cycle_id=row.cycle_id,
            workspace_id=row.workspace_id,
            cycle_name=row.cycle_name,
            cycle_status=row.cycle_status,
            framework_name=row.framework_name,
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
            evidence_item_id=row.evidence_item_id,
            rationale_summary=row.rationale_summary,
            citation_refs=row.citation_refs,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _to_review_item(row: MappingRow, control_row: ControlCoverageRow | None = None) -> ReviewQueueItem:
        return ReviewQueueItem(
            mapping_id=row.mapping_id,
            control_state_id=row.control_state_id,
            control_code=row.control_code,
            coverage_status=(control_row.coverage_status if control_row else "pending_review"),
            evidence_item_id=row.evidence_item_id,
            rationale_summary=row.rationale_summary,
            citation_refs=row.citation_refs,
            updated_at=row.updated_at,
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
            workflow_run_id=row.workflow_run_id,
            created_at=row.created_at,
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

    def create_workspace(self, command: CreateWorkspaceCommand) -> AuditWorkspaceSummary:
        workspace_id = f"audit-ws-{uuid4().hex[:10]}"
        with self.session_factory.begin() as session:
            row = AuditWorkspaceRow(
                workspace_id=workspace_id,
                workspace_name=command.workspace_name,
                framework_name=command.framework_name,
                workspace_status=command.workspace_status,
            )
            session.add(row)
        return AuditWorkspaceSummary(
            workspace_id=workspace_id,
            workspace_name=command.workspace_name,
            framework_name=command.framework_name,
            workspace_status=command.workspace_status,
        )

    def get_workspace(self, workspace_id: str) -> AuditWorkspaceSummary:
        with self.session_factory() as session:
            row = session.get(AuditWorkspaceRow, workspace_id)
            if row is None:
                raise KeyError(workspace_id)
            return self._to_workspace(row)

    def create_cycle(self, command: CreateCycleCommand) -> AuditCycleSummary:
        cycle_id = f"cycle-{uuid4().hex[:10]}"
        with self.session_factory.begin() as session:
            workspace_row = session.get(AuditWorkspaceRow, command.workspace_id)
            if workspace_row is None:
                raise KeyError(command.workspace_id)
            existing_cycle = session.scalar(
                select(AuditCycleRow.cycle_id)
                .where(AuditCycleRow.workspace_id == command.workspace_id)
                .where(AuditCycleRow.cycle_name == command.cycle_name)
                .limit(1)
            )
            if existing_cycle is not None:
                raise ValueError("CYCLE_NAME_ALREADY_EXISTS")

            cycle_row = AuditCycleRow(
                cycle_id=cycle_id,
                workspace_id=command.workspace_id,
                cycle_name=command.cycle_name,
                cycle_status=command.cycle_status,
                framework_name=command.framework_name,
                coverage_status="not_started",
                review_queue_count=0,
                open_gap_count=0,
                latest_workflow_run_id=None,
            )
            session.add(cycle_row)
            session.flush()

            self._seed_cycle_control_states(
                session,
                cycle_id=cycle_id,
                framework_name=command.framework_name,
            )
            session.flush()
            self._refresh_cycle_counts(session, cycle_id)
            return self._to_cycle(cycle_row)

    def list_cycles(self, workspace_id: str) -> list[AuditCycleSummary]:
        with self.session_factory() as session:
            rows = session.scalars(
                select(AuditCycleRow)
                .where(AuditCycleRow.workspace_id == workspace_id)
                .order_by(AuditCycleRow.cycle_name.asc())
            ).all()
            return [self._to_cycle(row) for row in rows]

    def get_cycle_dashboard(self, cycle_id: str) -> AuditCycleDashboardResponse:
        with self.session_factory() as session:
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if cycle_row is None:
                raise KeyError(cycle_id)
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
            )

    def list_controls(
        self,
        cycle_id: str,
        *,
        coverage_status: str | None = None,
        search: str | None = None,
    ) -> list[ControlCoverageSummary]:
        with self.session_factory() as session:
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if cycle_row is None:
                raise KeyError(cycle_id)
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
    ) -> MappingListResponse:
        with self.session_factory() as session:
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if cycle_row is None:
                raise KeyError(cycle_id)
            stmt = select(MappingRow).where(MappingRow.cycle_id == cycle_id)
            if control_state_id is not None:
                stmt = stmt.where(MappingRow.control_state_id == control_state_id)
            if mapping_status is not None:
                stmt = stmt.where(MappingRow.mapping_status == mapping_status)
            rows = session.scalars(stmt.order_by(MappingRow.updated_at.desc())).all()
            items = [self._to_mapping(row) for row in rows]
            return MappingListResponse(cycle_id=cycle_id, total_count=len(items), items=items)

    def get_control_detail(self, control_state_id: str) -> ControlDetailResponse:
        with self.session_factory() as session:
            control_row = session.get(ControlCoverageRow, control_state_id)
            if control_row is None:
                raise KeyError(control_state_id)
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
            )

    def get_evidence(self, evidence_id: str) -> EvidenceDetail:
        with self.session_factory() as session:
            evidence_row = session.get(EvidenceRow, evidence_id)
            if evidence_row is None:
                raise KeyError(evidence_id)
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

    def list_gaps(
        self,
        cycle_id: str,
        *,
        status: str | None = None,
        severity: str | None = None,
    ) -> list[GapSummary]:
        with self.session_factory() as session:
            stmt = select(GapRow).join(
                ControlCoverageRow,
                GapRow.control_state_id == ControlCoverageRow.control_state_id,
            ).where(ControlCoverageRow.cycle_id == cycle_id)
            if status is not None:
                stmt = stmt.where(GapRow.status == status)
            if severity is not None:
                stmt = stmt.where(GapRow.severity == severity)
            rows = session.scalars(stmt.order_by(GapRow.updated_at.desc())).all()
            return [self._to_gap(row) for row in rows]

    def list_review_queue(self, cycle_id: str) -> ReviewQueueResponse:
        with self.session_factory() as session:
            mapping_rows = session.scalars(
                select(MappingRow)
                .where(MappingRow.cycle_id == cycle_id)
                .where(MappingRow.mapping_status.in_(("proposed", "reassigned")))
                .order_by(MappingRow.updated_at.desc())
            ).all()
            items: list[ReviewQueueItem] = []
            for mapping_row in mapping_rows:
                control_row = session.get(ControlCoverageRow, mapping_row.control_state_id)
                items.append(self._to_review_item(mapping_row, control_row))
            return ReviewQueueResponse(cycle_id=cycle_id, total_count=len(items), items=items)

    def list_review_decisions(
        self,
        cycle_id: str,
        *,
        mapping_id: str | None = None,
        gap_id: str | None = None,
    ) -> ReviewDecisionListResponse:
        with self.session_factory() as session:
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if cycle_row is None:
                raise KeyError(cycle_id)
            stmt = select(ReviewDecisionRow).where(ReviewDecisionRow.cycle_id == cycle_id)
            if mapping_id is not None:
                stmt = stmt.where(ReviewDecisionRow.mapping_id == mapping_id)
            if gap_id is not None:
                stmt = stmt.where(ReviewDecisionRow.gap_id == gap_id)
            rows = session.scalars(stmt.order_by(ReviewDecisionRow.created_at.desc())).all()
            items = [self._to_review_decision(row) for row in rows]
            return ReviewDecisionListResponse(cycle_id=cycle_id, total_count=len(items), items=items)

    def list_imports(
        self,
        cycle_id: str,
        *,
        ingest_status: str | None = None,
        source_type: str | None = None,
    ) -> ImportListResponse:
        with self.session_factory() as session:
            stmt = select(EvidenceSourceRow).where(EvidenceSourceRow.cycle_id == cycle_id)
            if ingest_status is not None:
                stmt = stmt.where(EvidenceSourceRow.ingest_status == ingest_status)
            if source_type is not None:
                stmt = stmt.where(EvidenceSourceRow.source_type == source_type)
            rows = session.scalars(stmt.order_by(EvidenceSourceRow.updated_at.desc())).all()
            items = [self._to_import(row) for row in rows]
            return ImportListResponse(cycle_id=cycle_id, total_count=len(items), items=items)

    def create_upload_import(self, cycle_id: str, command: UploadImportCommand) -> ImportAcceptedResponse:
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
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if cycle_row is None:
                raise KeyError(cycle_id)
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

    def create_external_import(self, cycle_id: str, command: ExternalImportCommand) -> ImportAcceptedResponse:
        created_at = self._utcnow_naive()
        workflow_run_id = command.workflow_run_id or f"auditflow-import-external-{uuid4().hex[:10]}"
        selectors = command.upstream_ids or [command.query or ""]
        evidence_source_ids: list[str] = []
        duplicate_row: EvidenceSourceRow | None = None
        with self.session_factory.begin() as session:
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if cycle_row is None:
                raise KeyError(cycle_id)
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
            normalized_chunks = chunk_texts or [summary]
            if evidence_row is None:
                evidence_id = f"evidence-{uuid4().hex[:10]}"
                evidence_row = EvidenceRow(
                    evidence_id=evidence_id,
                    audit_cycle_id=cycle_id,
                    source_artifact_id=artifact_id,
                    normalized_artifact_id=normalized_artifact_id,
                    title=title,
                    evidence_type=evidence_type,
                    parse_status="parsed",
                    captured_at=self._normalize_timestamp(captured_at) or now,
                    summary=summary,
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
                evidence_row.title = title
                evidence_row.evidence_type = evidence_type
                evidence_row.parse_status = "parsed"
                evidence_row.captured_at = self._normalize_timestamp(captured_at) or now
                evidence_row.summary = summary
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

            for chunk_index, chunk_text in enumerate(normalized_chunks):
                session.add(
                    EvidenceChunkRow(
                        chunk_id=f"chunk-{uuid4().hex[:10]}",
                        evidence_id=evidence_row.evidence_id,
                        chunk_index=chunk_index,
                        section_label=f"Chunk {chunk_index + 1}",
                        text_excerpt=chunk_text,
                    )
                )

            control_row = session.scalars(
                select(ControlCoverageRow)
                .where(ControlCoverageRow.cycle_id == cycle_id)
                .order_by(ControlCoverageRow.control_code.asc())
            ).first()
            if control_row is not None:
                session.add(
                    MappingRow(
                        mapping_id=f"mapping-{uuid4().hex[:10]}",
                        cycle_id=cycle_id,
                        control_state_id=control_row.control_state_id,
                        control_code=control_row.control_code,
                        mapping_status="proposed",
                        evidence_item_id=evidence_row.evidence_id,
                        rationale_summary=f"Imported evidence '{title}' requires reviewer confirmation.",
                        citation_refs=[{"kind": "evidence_item", "id": evidence_row.evidence_id}],
                        reviewer_locked=False,
                        updated_at=now,
                    )
                )
                self._refresh_control_state(session, control_row.control_state_id)
            self._refresh_cycle_counts(session, cycle_id)
            cycle_row.latest_workflow_run_id = workflow_run_id
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

    def review_mapping(self, mapping_id: str, command: MappingReviewCommand) -> MappingReviewResponse:
        with self.session_factory.begin() as session:
            mapping_row = session.get(MappingRow, mapping_id)
            if mapping_row is None:
                raise KeyError(mapping_id)
            if command.expected_updated_at is not None and not self._timestamps_match(
                mapping_row.updated_at,
                command.expected_updated_at,
            ):
                raise ValueError("CONFLICT_STALE_RESOURCE")
            if mapping_row.reviewer_locked and mapping_row.mapping_status in {"accepted", "rejected"}:
                raise ValueError("MAPPING_ALREADY_TERMINAL")
            previous_status = mapping_row.mapping_status
            original_control_id = mapping_row.control_state_id
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
            mapping_row.updated_at = self._utcnow_naive()
            self._append_review_decision(
                session,
                cycle_id=mapping_row.cycle_id,
                mapping_id=mapping_row.mapping_id,
                gap_id=None,
                decision=command.decision,
                from_status=previous_status,
                to_status=mapping_row.mapping_status,
                comment=command.comment,
            )

            for control_id in {original_control_id, mapping_row.control_state_id}:
                self._refresh_control_state(session, control_id)
                cycle_id = session.get(ControlCoverageRow, control_id).cycle_id  # type: ignore[union-attr]
                self._refresh_cycle_counts(session, cycle_id)

            control_row = session.get(ControlCoverageRow, mapping_row.control_state_id)
            if control_row is None:
                raise KeyError(mapping_row.control_state_id)
            return MappingReviewResponse(
                mapping_id=mapping_row.mapping_id,
                mapping_status=mapping_row.mapping_status,
                control_state=self._to_control(control_row),
            )

    def decide_gap(self, gap_id: str, command: GapDecisionCommand) -> GapSummary:
        with self.session_factory.begin() as session:
            gap_row = session.get(GapRow, gap_id)
            if gap_row is None:
                raise KeyError(gap_id)
            if command.expected_updated_at is not None and not self._timestamps_match(
                gap_row.updated_at,
                command.expected_updated_at,
            ):
                raise ValueError("CONFLICT_STALE_RESOURCE")
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
            gap_row.updated_at = self._utcnow_naive()
            control_row = session.get(ControlCoverageRow, gap_row.control_state_id)
            if control_row is None:
                raise KeyError(gap_row.control_state_id)
            self._append_review_decision(
                session,
                cycle_id=control_row.cycle_id,
                mapping_id=None,
                gap_id=gap_row.gap_id,
                decision=command.decision,
                from_status=previous_status,
                to_status=gap_row.status,
                comment=command.comment,
            )
            self._refresh_control_state(session, control_row.control_state_id)
            self._refresh_cycle_counts(session, control_row.cycle_id)
            return self._to_gap(gap_row)

    def list_narratives(
        self,
        cycle_id: str,
        *,
        snapshot_version: int | None = None,
        narrative_type: str | None = None,
    ) -> list[NarrativeSummary]:
        with self.session_factory() as session:
            stmt = select(NarrativeRow).where(NarrativeRow.cycle_id == cycle_id)
            if snapshot_version is not None:
                stmt = stmt.where(NarrativeRow.snapshot_version == snapshot_version)
            if narrative_type is not None:
                stmt = stmt.where(NarrativeRow.narrative_type == narrative_type)
            rows = session.scalars(stmt.order_by(NarrativeRow.snapshot_version.desc())).all()
            return [self._to_narrative(row) for row in rows]

    def get_export_package(self, package_id: str) -> ExportPackageSummary:
        with self.session_factory() as session:
            row = session.get(ExportPackageRow, package_id)
            if row is None:
                raise KeyError(package_id)
            return self._to_export_package(row)

    def record_cycle_processing_result(self, cycle_id: str, workflow_run_id: str, checkpoint_seq: int) -> None:
        with self.session_factory.begin() as session:
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if cycle_row is None:
                raise KeyError(cycle_id)
            cycle_row.cycle_status = "pending_review"
            cycle_row.coverage_status = "pending_review"
            cycle_row.latest_workflow_run_id = workflow_run_id
            self._refresh_cycle_counts(session, cycle_id)
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
    ) -> ExportPackageSummary:
        created_at = datetime.now(UTC)
        package_id = f"pkg-{uuid4().hex[:10]}"
        artifact_id = f"artifact-export-{cycle_id}-{snapshot_version}"
        with self.session_factory.begin() as session:
            cycle_row = session.get(AuditCycleRow, cycle_id)
            if cycle_row is None:
                raise KeyError(cycle_id)
            cycle_row.cycle_status = "exported"
            cycle_row.coverage_status = "covered"
            cycle_row.latest_workflow_run_id = workflow_run_id
            cycle_row.review_queue_count = 0

            control_rows = session.scalars(
                select(ControlCoverageRow).where(ControlCoverageRow.cycle_id == cycle_id)
            ).all()
            for row in control_rows:
                row.coverage_status = "covered"

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

            session.add(
                ExportPackageRow(
                    package_id=package_id,
                    cycle_id=cycle_id,
                    snapshot_version=snapshot_version,
                    status="ready" if checkpoint_seq > 0 else "queued",
                    artifact_id=artifact_id,
                    workflow_run_id=workflow_run_id,
                    created_at=created_at,
                )
            )
        return ExportPackageSummary(
            package_id=package_id,
            cycle_id=cycle_id,
            snapshot_version=snapshot_version,
            status="ready" if checkpoint_seq > 0 else "queued",
            artifact_id=artifact_id,
            workflow_run_id=workflow_run_id,
            created_at=created_at,
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
    ) -> None:
        session.add(
            ReviewDecisionRow(
                review_decision_id=f"review-decision-{uuid4().hex[:10]}",
                cycle_id=cycle_id,
                mapping_id=mapping_id,
                gap_id=gap_id,
                decision=decision,
                from_status=from_status,
                to_status=to_status,
                reviewer_id=DEFAULT_REVIEWER_ID,
                comment=comment or None,
                feedback_tags=cls._decision_feedback_tags(decision, to_status),
                created_at=cls._utcnow_naive(),
            )
        )

    @staticmethod
    def _decision_feedback_tags(decision: str, to_status: str | None) -> list[str]:
        tags = [f"decision:{decision}"]
        if to_status:
            tags.append(f"status:{to_status}")
        return tags
