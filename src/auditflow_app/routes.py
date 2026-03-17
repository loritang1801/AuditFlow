from __future__ import annotations

import importlib

ERROR_STATUS_BY_CODE = {
    "CYCLE_NAME_ALREADY_EXISTS": 409,
    "CONFLICT_STALE_RESOURCE": 409,
    "MAPPING_ALREADY_TERMINAL": 409,
    "GAP_STATUS_CONFLICT": 409,
    "EXPORT_ALREADY_RUNNING": 409,
    "SNAPSHOT_STALE": 409,
    "CYCLE_NOT_READY_FOR_EXPORT": 422,
}

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
    ReviewDecisionListResponse,
    ReviewQueueResponse,
    UploadImportCommand,
)
from .service import AuditFlowAppService
from .shared_runtime import load_shared_agent_platform


def map_domain_error(exc: Exception, *, path: str = "") -> tuple[int, dict[str, object]]:
    if isinstance(exc, KeyError):
        if "/workspaces/" in path:
            code = "AUDIT_WORKSPACE_NOT_FOUND"
        elif "/cycles/" in path:
            code = "AUDIT_CYCLE_NOT_FOUND"
        elif "/controls/" in path:
            code = "CONTROL_STATE_NOT_FOUND"
        elif "/evidence/" in path:
            code = "EVIDENCE_NOT_FOUND"
        elif "/exports/" in path:
            code = "EXPORT_PACKAGE_NOT_FOUND"
        else:
            code = "RESOURCE_NOT_FOUND"
        resource_id = str(exc.args[0]) if exc.args else "resource"
        return 404, {"error": {"code": code, "message": f"{code}: {resource_id}"}}
    if isinstance(exc, ValueError):
        code = str(exc)
        if code == "CONFLICT_STALE_RESOURCE" and "/mappings/" in path:
            code = "MAPPING_REVIEW_CONFLICT"
        status_code = ERROR_STATUS_BY_CODE.get(str(exc), 400)
        if code == "MAPPING_REVIEW_CONFLICT":
            status_code = 409
        return status_code, {"error": {"code": code, "message": code}}
    return 500, {"error": {"code": "INTERNAL_ERROR", "message": "Internal server error"}}


def create_fastapi_app(service: AuditFlowAppService):
    ap = load_shared_agent_platform()
    try:
        from fastapi import FastAPI, Request
        from fastapi.responses import JSONResponse
    except ImportError as exc:
        errors_module = importlib.import_module(f"{ap.__name__}.errors")
        FastAPIUnavailableError = errors_module.FastAPIUnavailableError

        raise FastAPIUnavailableError("fastapi is not installed") from exc

    app = FastAPI(title="AuditFlow API")

    @app.exception_handler(KeyError)
    def handle_key_error(request: Request, exc: KeyError):
        status_code, payload = map_domain_error(exc, path=str(request.url.path))
        return JSONResponse(status_code=status_code, content=payload)

    @app.exception_handler(ValueError)
    def handle_value_error(request: Request, exc: ValueError):
        status_code, payload = map_domain_error(exc, path=str(request.url.path))
        return JSONResponse(status_code=status_code, content=payload)

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

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/review-decisions", response_model=ReviewDecisionListResponse)
    def list_review_decisions(
        cycle_id: str,
        mapping_id: str | None = None,
        gap_id: str | None = None,
    ) -> ReviewDecisionListResponse:
        return service.list_review_decisions(cycle_id, mapping_id=mapping_id, gap_id=gap_id)

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
