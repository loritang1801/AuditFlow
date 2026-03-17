from __future__ import annotations

import importlib

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
    ExportCreateCommand,
    ExportGenerationCommand,
    ExportPackageSummary,
    HealthResponse,
    MappingReviewCommand,
    MappingReviewResponse,
    NarrativeSummary,
    ReviewQueueResponse,
    UploadImportCommand,
)
from .service import AuditFlowAppService
from .shared_runtime import load_shared_agent_platform


def create_fastapi_app(service: AuditFlowAppService):
    ap = load_shared_agent_platform()
    try:
        from fastapi import FastAPI
    except ImportError as exc:
        errors_module = importlib.import_module(f"{ap.__name__}.errors")
        FastAPIUnavailableError = errors_module.FastAPIUnavailableError

        raise FastAPIUnavailableError("fastapi is not installed") from exc

    app = FastAPI(title="AuditFlow API")

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok", product="auditflow")

    @app.get("/api/v1/workflows")
    def list_workflows():
        return service.list_workflows()

    @app.get("/api/v1/workflows/{workflow_run_id}", response_model=AuditFlowWorkflowStateResponse)
    def get_workflow_state(workflow_run_id: str) -> AuditFlowWorkflowStateResponse:
        return service.get_workflow_state(workflow_run_id)

    @app.post("/api/v1/auditflow/workspaces", response_model=AuditWorkspaceSummary, status_code=201)
    def create_workspace(command: CreateWorkspaceCommand) -> AuditWorkspaceSummary:
        return service.create_workspace(command)

    @app.get("/api/v1/auditflow/workspaces/{workspace_id}", response_model=AuditWorkspaceSummary)
    def get_workspace(workspace_id: str) -> AuditWorkspaceSummary:
        return service.get_workspace(workspace_id)

    @app.post("/api/v1/auditflow/cycles", response_model=AuditCycleSummary, status_code=201)
    def create_cycle(command: CreateCycleCommand) -> AuditCycleSummary:
        return service.create_cycle(command)

    @app.get("/api/v1/auditflow/cycles", response_model=list[AuditCycleSummary])
    def list_cycles(workspace_id: str) -> list[AuditCycleSummary]:
        return service.list_cycles(workspace_id)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/dashboard", response_model=AuditCycleDashboardResponse)
    def get_cycle_dashboard(cycle_id: str) -> AuditCycleDashboardResponse:
        return service.get_cycle_dashboard(cycle_id)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/controls")
    def list_controls(cycle_id: str) -> list[ControlCoverageSummary]:
        return service.list_controls(cycle_id)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/controls/{control_state_id}", response_model=ControlDetailResponse)
    def get_control_detail(cycle_id: str, control_state_id: str) -> ControlDetailResponse:
        del cycle_id
        return service.get_control_detail(control_state_id)

    @app.get("/api/v1/auditflow/evidence/{evidence_id}", response_model=EvidenceDetail)
    def get_evidence(evidence_id: str) -> EvidenceDetail:
        return service.get_evidence(evidence_id)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/review-queue", response_model=ReviewQueueResponse)
    def list_review_queue(cycle_id: str) -> ReviewQueueResponse:
        return service.list_review_queue(cycle_id)

    @app.get("/api/v1/auditflow/review-queue", response_model=ReviewQueueResponse)
    def list_review_queue_global(cycle_id: str) -> ReviewQueueResponse:
        return service.list_review_queue(cycle_id)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/imports", response_model=ImportListResponse)
    def list_imports(
        cycle_id: str,
        ingest_status: str | None = None,
        source_type: str | None = None,
    ) -> ImportListResponse:
        return service.list_imports(cycle_id, ingest_status=ingest_status, source_type=source_type)

    @app.post("/api/v1/auditflow/cycles/{cycle_id}/imports/upload", response_model=ImportAcceptedResponse)
    def create_upload_import(cycle_id: str, command: UploadImportCommand) -> ImportAcceptedResponse:
        return service.create_upload_import(cycle_id, command)

    @app.post("/api/v1/auditflow/cycles/{cycle_id}/imports/external", response_model=ImportAcceptedResponse)
    def create_external_import(cycle_id: str, command: ExternalImportCommand) -> ImportAcceptedResponse:
        return service.create_external_import(cycle_id, command)

    @app.post("/api/v1/auditflow/import-jobs/dispatch", response_model=ImportDispatchResponse)
    def dispatch_import_jobs() -> ImportDispatchResponse:
        return service.dispatch_import_jobs()

    @app.post("/api/v1/auditflow/mappings/{mapping_id}/review", response_model=MappingReviewResponse)
    def review_mapping(mapping_id: str, command: MappingReviewCommand) -> MappingReviewResponse:
        return service.review_mapping(mapping_id, command)

    @app.post("/api/v1/auditflow/gaps/{gap_id}/decision", response_model=GapSummary)
    def decide_gap(gap_id: str, command: GapDecisionCommand) -> GapSummary:
        return service.decide_gap(gap_id, command)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/narratives")
    def list_narratives(
        cycle_id: str,
        snapshot_version: int | None = None,
        narrative_type: str | None = None,
    ) -> list[NarrativeSummary]:
        return service.list_narratives(
            cycle_id,
            snapshot_version=snapshot_version,
            narrative_type=narrative_type,
        )

    @app.post("/api/v1/auditflow/cycles/process", response_model=AuditFlowRunResponse)
    def process_cycle(command: CycleProcessingCommand) -> AuditFlowRunResponse:
        return service.process_cycle(command)

    @app.post("/api/v1/auditflow/cycles/{cycle_id}/exports", response_model=ExportPackageSummary)
    def create_export_package(cycle_id: str, command: ExportCreateCommand) -> ExportPackageSummary:
        return service.create_export_package(cycle_id, command)

    @app.post("/api/v1/auditflow/exports/generate", response_model=AuditFlowRunResponse)
    def generate_export(command: ExportGenerationCommand) -> AuditFlowRunResponse:
        return service.generate_export(command)

    @app.get("/api/v1/auditflow/exports/{package_id}", response_model=ExportPackageSummary)
    def get_export_package(package_id: str) -> ExportPackageSummary:
        return service.get_export_package(package_id)

    return app
