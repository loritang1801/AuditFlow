from __future__ import annotations

import base64
import binascii
import importlib
from typing import Any

ERROR_STATUS_BY_CODE = {
    "WORKSPACE_SLUG_ALREADY_EXISTS": 409,
    "CYCLE_NAME_ALREADY_EXISTS": 409,
    "IDEMPOTENCY_CONFLICT": 409,
    "CONFLICT_STALE_RESOURCE": 409,
    "MAPPING_ALREADY_TERMINAL": 409,
    "GAP_STATUS_CONFLICT": 409,
    "EXPORT_ALREADY_RUNNING": 409,
    "SNAPSHOT_STALE": 409,
    "CYCLE_NOT_READY_FOR_EXPORT": 422,
    "INVALID_REVIEW_QUEUE_SORT": 400,
    "INVALID_CURSOR": 400,
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
    MappingListResponse,
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

DEFAULT_PAGE_LIMIT = 20
MAX_PAGE_LIMIT = 100


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


def _serialize_data(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(by_alias=True)
    if isinstance(value, list):
        return [_serialize_data(item) for item in value]
    return value


def success_envelope(
    data: Any,
    *,
    request_id: str | None = None,
    workflow_run_id: str | None = None,
    next_cursor: str | None = None,
    has_more: bool = False,
) -> dict[str, object]:
    meta: dict[str, object] = {"request_id": request_id, "has_more": has_more}
    if next_cursor is not None:
        meta["next_cursor"] = next_cursor
    if workflow_run_id is not None:
        meta["workflow_run_id"] = workflow_run_id
    return {"data": _serialize_data(data), "meta": meta}


def _encode_cursor(offset: int) -> str | None:
    if offset <= 0:
        return None
    return base64.urlsafe_b64encode(f"offset:{offset}".encode("utf-8")).decode("ascii")


def _decode_cursor(cursor: str | None) -> int:
    if cursor in {None, ""}:
        return 0
    try:
        decoded = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
        raise ValueError("INVALID_CURSOR") from exc
    prefix, separator, raw_offset = decoded.partition(":")
    if prefix != "offset" or separator != ":" or not raw_offset.isdigit():
        raise ValueError("INVALID_CURSOR")
    return int(raw_offset)


def paginate_collection(items: list[Any], *, cursor: str | None = None, limit: int = DEFAULT_PAGE_LIMIT) -> tuple[list[Any], str | None, bool]:
    normalized_limit = max(1, min(limit, MAX_PAGE_LIMIT))
    start = _decode_cursor(cursor)
    page = items[start : start + normalized_limit]
    next_offset = start + normalized_limit
    has_more = next_offset < len(items)
    return page, (_encode_cursor(next_offset) if has_more else None), has_more


def create_fastapi_app(service: AuditFlowAppService):
    ap = load_shared_agent_platform()
    try:
        from fastapi import FastAPI, Header, Request
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

    @app.post("/api/v1/auditflow/workspaces", status_code=201)
    def create_workspace(
        command: CreateWorkspaceCommand,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        return success_envelope(
            service.create_workspace(command),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/workspaces/{workspace_id}")
    def get_workspace(
        workspace_id: str,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        return success_envelope(
            service.get_workspace(workspace_id),
            request_id=request_id,
        )

    @app.post("/api/v1/auditflow/cycles", status_code=201)
    def create_cycle(
        command: CreateCycleCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        return success_envelope(
            service.create_cycle(command, idempotency_key=idempotency_key),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/cycles")
    def list_cycles(
        workspace_id: str,
        status: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        items = service.list_cycles(workspace_id, status=status)
        page_items, next_cursor, has_more = paginate_collection(items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/dashboard", response_model=AuditCycleDashboardResponse)
    def get_cycle_dashboard(cycle_id: str) -> AuditCycleDashboardResponse:
        return service.get_cycle_dashboard(cycle_id)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/controls")
    def list_controls(
        cycle_id: str,
        coverage_status: str | None = None,
        search: str | None = None,
    ) -> list[ControlCoverageSummary]:
        return service.list_controls(cycle_id, coverage_status=coverage_status, search=search)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/mappings", response_model=MappingListResponse)
    def list_mappings(
        cycle_id: str,
        control_state_id: str | None = None,
        mapping_status: str | None = None,
    ) -> MappingListResponse:
        return service.list_mappings(
            cycle_id,
            control_state_id=control_state_id,
            mapping_status=mapping_status,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/controls/{control_state_id}", response_model=ControlDetailResponse)
    def get_control_detail(cycle_id: str, control_state_id: str) -> ControlDetailResponse:
        del cycle_id
        return service.get_control_detail(control_state_id)

    @app.get("/api/v1/auditflow/evidence/{evidence_id}", response_model=EvidenceDetail)
    def get_evidence(evidence_id: str) -> EvidenceDetail:
        return service.get_evidence(evidence_id)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/gaps", response_model=list[GapSummary])
    def list_gaps(
        cycle_id: str,
        status: str | None = None,
        severity: str | None = None,
    ) -> list[GapSummary]:
        return service.list_gaps(cycle_id, status=status, severity=severity)

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/review-queue", response_model=ReviewQueueResponse)
    def list_review_queue(
        cycle_id: str,
        control_state_id: str | None = None,
        severity: str | None = None,
        sort: str = "recent",
    ) -> ReviewQueueResponse:
        return service.list_review_queue(
            cycle_id,
            control_state_id=control_state_id,
            severity=severity,
            sort=sort,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/review-decisions", response_model=ReviewDecisionListResponse)
    def list_review_decisions(
        cycle_id: str,
        mapping_id: str | None = None,
        gap_id: str | None = None,
    ) -> ReviewDecisionListResponse:
        return service.list_review_decisions(cycle_id, mapping_id=mapping_id, gap_id=gap_id)

    @app.get("/api/v1/auditflow/review-queue", response_model=ReviewQueueResponse)
    def list_review_queue_global(
        cycle_id: str,
        control_state_id: str | None = None,
        severity: str | None = None,
        sort: str = "recent",
    ) -> ReviewQueueResponse:
        return service.list_review_queue(
            cycle_id,
            control_state_id=control_state_id,
            severity=severity,
            sort=sort,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/imports")
    def list_imports(
        cycle_id: str,
        status: str | None = None,
        ingest_status: str | None = None,
        source_type: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        response = service.list_imports(
            cycle_id,
            ingest_status=status or ingest_status,
            source_type=source_type,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.post(
        "/api/v1/auditflow/cycles/{cycle_id}/imports/upload",
        status_code=202,
    )
    def create_upload_import(
        cycle_id: str,
        command: UploadImportCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        response = service.create_upload_import(cycle_id, command, idempotency_key=idempotency_key)
        return success_envelope(
            response,
            request_id=request_id,
            workflow_run_id=response.workflow_run_id,
        )

    @app.post(
        "/api/v1/auditflow/cycles/{cycle_id}/imports/external",
        status_code=202,
    )
    def create_external_import(
        cycle_id: str,
        command: ExternalImportCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        response = service.create_external_import(cycle_id, command, idempotency_key=idempotency_key)
        return success_envelope(
            response,
            request_id=request_id,
            workflow_run_id=response.workflow_run_id,
        )

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

    @app.post(
        "/api/v1/auditflow/cycles/{cycle_id}/exports",
        status_code=202,
    )
    def create_export_package(
        cycle_id: str,
        command: ExportCreateCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        response = service.create_export_package(cycle_id, command, idempotency_key=idempotency_key)
        return success_envelope(
            response,
            request_id=request_id,
            workflow_run_id=response.workflow_run_id,
        )

    @app.post("/api/v1/auditflow/exports/generate", response_model=AuditFlowRunResponse)
    def generate_export(command: ExportGenerationCommand) -> AuditFlowRunResponse:
        return service.generate_export(command)

    @app.get("/api/v1/auditflow/exports/{package_id}")
    def get_export_package(
        package_id: str,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        return success_envelope(
            service.get_export_package(package_id),
            request_id=request_id,
        )

    return app
