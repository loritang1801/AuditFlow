from __future__ import annotations

import asyncio
import base64
import binascii
import importlib
import json
from datetime import UTC, datetime
from typing import Any

from .auth import (
    AuditFlowAuthorizationError,
    AuditFlowAuthorizer,
    CurrentUserResponse,
    HeaderAuditFlowAuthorizer,
    SessionCreateCommand,
)

ERROR_STATUS_BY_CODE = {
    "WORKSPACE_SLUG_ALREADY_EXISTS": 409,
    "CYCLE_NAME_ALREADY_EXISTS": 409,
    "IDEMPOTENCY_CONFLICT": 409,
    "CONFLICT_STALE_RESOURCE": 409,
    "MAPPING_ALREADY_TERMINAL": 409,
    "REVIEW_CLAIM_CONFLICT": 409,
    "GAP_STATUS_CONFLICT": 409,
    "EXPORT_ALREADY_RUNNING": 409,
    "SNAPSHOT_STALE": 409,
    "CYCLE_NOT_READY_FOR_EXPORT": 422,
    "INVALID_REVIEW_QUEUE_SORT": 400,
    "INVALID_REVIEW_QUEUE_CLAIM_STATE": 400,
    "INVALID_CURSOR": 400,
    "INVALID_ARTIFACT_BYTES": 400,
    "INVALID_SEARCH_QUERY": 400,
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
    MappingClaimCommand,
    MappingClaimReleaseCommand,
    MappingClaimResponse,
    MappingListResponse,
    ExportCreateCommand,
    ExportGenerationCommand,
    ExportPackageSummary,
    HealthResponse,
    MemoryRecordListResponse,
    MappingReviewCommand,
    MappingReviewResponse,
    NarrativeSummary,
    RuntimeCapabilitiesResponse,
    ReviewDecisionListResponse,
    ReviewQueueResponse,
    ToolAccessAuditListResponse,
    UploadImportCommand,
    EvidenceSearchResponse,
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


def _event_topic(event_name: str) -> str:
    if event_name.startswith("workflow."):
        return "workflow"
    if event_name.startswith("approval."):
        return "approval"
    if event_name.startswith("artifact."):
        return "artifact"
    if event_name.startswith("auditflow."):
        return "auditflow"
    return "workspace"


def _isoformat_utc(value: datetime) -> str:
    timestamp = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _payload_lookup(payload: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return str(value)
    return None


def _event_topics(context: dict[str, object]) -> set[str]:
    payload = context.get("payload")
    normalized_payload = payload if isinstance(payload, dict) else {}
    topics = {str(context["topic"]), f"auditflow.workspace.{context['workspace_id']}"}

    cycle_id = _payload_lookup(normalized_payload, "cycle_id")
    if cycle_id is None and str(context["subject_type"]) in {"audit_cycle", "auditflow_cycle", "cycle"}:
        cycle_id = str(context["subject_id"])
    if cycle_id is not None:
        topics.add(f"auditflow.cycle.{cycle_id}")

    package_id = _payload_lookup(normalized_payload, "package_id")
    if package_id is None and str(context["subject_type"]) in {"audit_package", "export_package", "export"}:
        package_id = str(context["subject_id"])
    if package_id is not None:
        topics.add(f"auditflow.export.{package_id}")

    return topics


def _matches_event_topic(context: dict[str, object], requested_topic: str | None) -> bool:
    if requested_topic is None:
        return True
    return requested_topic in _event_topics(context)


def _normalize_resume_after_id(pending_events: list[Any], resume_after_id: str | None) -> str | None:
    if resume_after_id is None:
        return None
    for stored in pending_events:
        if stored.event.event_id == resume_after_id:
            return resume_after_id
    return None


def _format_sse_message(*, event_id: str, event_name: str, payload: dict[str, object]) -> str:
    return f"id: {event_id}\nevent: {event_name}\ndata: {json.dumps(payload, sort_keys=True)}\n\n"


def _resolve_outbox_event_context(service: AuditFlowAppService, event) -> dict[str, object] | None:
    payload = dict(getattr(event, "payload", {}) or {})
    state: dict[str, object] = {}
    runtime_stores = getattr(service, "runtime_stores", None)
    if runtime_stores is not None and hasattr(runtime_stores, "state_store"):
        try:
            state_record = runtime_stores.state_store.load(event.workflow_run_id)
        except Exception:  # noqa: BLE001
            state = {}
        else:
            state = dict(getattr(state_record, "state", {}) or {})
    workspace_id = (
        state.get("workspace_id")
        or state.get("audit_workspace_id")
        or state.get("workspace")
        or payload.get("workspace_id")
        or payload.get("audit_workspace_id")
        or payload.get("workspace")
    )
    if workspace_id is None:
        return None
    subject_type = (
        state.get("subject_type")
        or payload.get("subject_type")
        or ("audit_cycle" if payload.get("cycle_id") is not None else None)
        or ("audit_package" if payload.get("package_id") is not None else None)
        or event.aggregate_type
    )
    subject_id = (
        state.get("subject_id")
        or payload.get("subject_id")
        or payload.get("cycle_id")
        or payload.get("package_id")
        or event.aggregate_id
    )
    return {
        "event_id": event.event_id,
        "event_type": event.event_name,
        "organization_id": str(
            state.get("organization_id")
            or payload.get("organization_id")
            or "unknown-org"
        ),
        "workspace_id": str(workspace_id),
        "subject_type": str(subject_type),
        "subject_id": str(subject_id),
        "occurred_at": _isoformat_utc(event.emitted_at),
        "payload": payload,
        "topic": _event_topic(event.event_name),
    }


def create_fastapi_app(service: AuditFlowAppService, *, authorizer: AuditFlowAuthorizer | None = None):
    ap = load_shared_agent_platform()
    try:
        from fastapi import Cookie, Depends, FastAPI, Header, Request, Response
        from fastapi.responses import JSONResponse, StreamingResponse
    except ImportError as exc:
        errors_module = importlib.import_module(f"{ap.__name__}.errors")
        FastAPIUnavailableError = errors_module.FastAPIUnavailableError

        raise FastAPIUnavailableError("fastapi is not installed") from exc

    app = FastAPI(title="AuditFlow API")
    auth_service = getattr(service, "auth_service", None)
    route_authorizer = (
        authorizer
        or (auth_service.build_authorizer() if auth_service is not None else HeaderAuditFlowAuthorizer())
    )

    @app.exception_handler(KeyError)
    def handle_key_error(request: Request, exc: KeyError):
        status_code, payload = map_domain_error(exc, path=str(request.url.path))
        return JSONResponse(status_code=status_code, content=payload)

    @app.exception_handler(ValueError)
    def handle_value_error(request: Request, exc: ValueError):
        status_code, payload = map_domain_error(exc, path=str(request.url.path))
        return JSONResponse(status_code=status_code, content=payload)

    @app.exception_handler(AuditFlowAuthorizationError)
    def handle_authorization_error(request: Request, exc: AuditFlowAuthorizationError):
        del request
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    def _build_access_dependency(required_role: str):
        def require_access(
            authorization: str | None = Header(default=None, alias="Authorization"),
            organization_id: str | None = Header(default=None, alias="X-Organization-Id"),
            user_id: str | None = Header(default=None, alias="X-User-Id"),
            user_role: str | None = Header(default=None, alias="X-User-Role"),
        ):
            return route_authorizer.authorize(
                required_role=required_role,
                authorization=authorization,
                organization_id=organization_id,
                user_id=user_id,
                user_role=user_role,
            )

        return require_access

    require_viewer_access = _build_access_dependency("viewer")
    require_reviewer_access = _build_access_dependency("reviewer")
    require_product_admin_access = _build_access_dependency("product_admin")

    if auth_service is not None:
        @app.post("/api/v1/auth/session")
        def create_auth_session(
            command: SessionCreateCommand,
            request: Request,
            request_id: str | None = Header(default=None, alias="X-Request-Id"),
        ):
            issue = auth_service.create_session(
                command,
                ip_address=(request.client.host if request.client is not None else None),
                user_agent=request.headers.get("User-Agent"),
            )
            response = JSONResponse(
                status_code=200,
                content=success_envelope(
                    issue.response,
                    request_id=request_id,
                ),
            )
            response.set_cookie(
                key="refresh_token",
                value=issue.refresh_token,
                httponly=True,
                samesite="lax",
                secure=False,
                path="/",
            )
            return response

        @app.post("/api/v1/auth/session/refresh")
        def refresh_auth_session(
            request: Request,
            refresh_token: str | None = Cookie(default=None, alias="refresh_token"),
            request_id: str | None = Header(default=None, alias="X-Request-Id"),
        ):
            issue = auth_service.refresh_session(
                refresh_token,
                ip_address=(request.client.host if request.client is not None else None),
                user_agent=request.headers.get("User-Agent"),
            )
            response = JSONResponse(
                status_code=200,
                content=success_envelope(
                    issue.response,
                    request_id=request_id,
                ),
            )
            response.set_cookie(
                key="refresh_token",
                value=issue.refresh_token,
                httponly=True,
                samesite="lax",
                secure=False,
                path="/",
            )
            return response

        @app.delete("/api/v1/auth/session/current", status_code=204)
        def revoke_current_auth_session(
            auth_context=Depends(require_viewer_access),
        ):
            auth_service.revoke_session(auth_context.session_id)
            response = Response(status_code=204)
            response.delete_cookie("refresh_token", path="/")
            return response

        @app.get("/api/v1/me")
        def get_current_user(
            request_id: str | None = Header(default=None, alias="X-Request-Id"),
            auth_context=Depends(require_viewer_access),
        ) -> dict[str, object]:
            return success_envelope(
                auth_service.get_current_user(auth_context),
                request_id=request_id,
            )

    @app.get("/health")
    def health(
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
    ) -> dict[str, object]:
        return success_envelope(
            HealthResponse(status="ok", product="auditflow"),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/runtime-capabilities")
    def get_runtime_capabilities(
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_product_admin_access),
    ) -> dict[str, object]:
        del auth_context
        return success_envelope(
            service.get_runtime_capabilities(),
            request_id=request_id,
        )

    @app.get("/api/v1/workflows")
    def list_workflows(
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        del auth_context
        return success_envelope(
            service.list_workflows(),
            request_id=request_id,
        )

    @app.get("/api/v1/workflows/{workflow_run_id}")
    def get_workflow_state(
        workflow_run_id: str,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.get_workflow_state(
                workflow_run_id,
                organization_id=auth_context.organization_id,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/events/stream")
    async def stream_events(
        workspace_id: str,
        topic: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
        auth_context=Depends(require_viewer_access),
    ):
        service.get_workspace(
            workspace_id,
            organization_id=auth_context.organization_id,
        )
        runtime_stores = getattr(service, "runtime_stores", None)
        outbox_store = getattr(runtime_stores, "outbox_store", None)

        async def event_stream():
            seen_event_ids: set[str] = set()
            resume_after_id = last_event_id
            while True:
                emitted_any = False
                pending = outbox_store.list_pending() if outbox_store is not None else []
                resume_after_id = _normalize_resume_after_id(pending, resume_after_id)
                resume_matched = resume_after_id is None
                for stored in pending:
                    event = stored.event
                    if event.event_id in seen_event_ids:
                        continue
                    if not resume_matched:
                        if event.event_id == resume_after_id:
                            resume_matched = True
                        continue
                    context = _resolve_outbox_event_context(service, event)
                    if context is None:
                        continue
                    if str(context["workspace_id"]) != workspace_id:
                        continue
                    if not _matches_event_topic(context, topic):
                        continue
                    if subject_type is not None and str(context["subject_type"]) != subject_type:
                        continue
                    if subject_id is not None and str(context["subject_id"]) != subject_id:
                        continue
                    seen_event_ids.add(event.event_id)
                    emitted_any = True
                    yield _format_sse_message(
                        event_id=event.event_id,
                        event_name=event.event_name,
                        payload={
                            key: value
                            for key, value in context.items()
                            if key != "topic"
                        },
                    )
                resume_after_id = None
                if not emitted_any:
                    heartbeat_at = datetime.now(UTC)
                    heartbeat = {
                        "workspace_id": workspace_id,
                        "occurred_at": _isoformat_utc(heartbeat_at),
                    }
                    yield _format_sse_message(
                        event_id=f"heartbeat-{int(heartbeat_at.timestamp() * 1000)}",
                        event_name="heartbeat",
                        payload=heartbeat,
                    )
                await asyncio.sleep(15)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.post("/api/v1/auditflow/workspaces", status_code=201)
    def create_workspace(
        command: CreateWorkspaceCommand,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_product_admin_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.create_workspace(
                command,
                organization_id=auth_context.organization_id,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/workspaces/{workspace_id}")
    def get_workspace(
        workspace_id: str,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.get_workspace(
                workspace_id,
                organization_id=auth_context.organization_id,
            ),
            request_id=request_id,
        )

    @app.post("/api/v1/auditflow/cycles", status_code=201)
    def create_cycle(
        command: CreateCycleCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.create_cycle(
                command,
                idempotency_key=idempotency_key,
                organization_id=auth_context.organization_id,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/cycles")
    def list_cycles(
        workspace_id: str,
        status: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        items = service.list_cycles(
            workspace_id,
            status=status,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/dashboard")
    def get_cycle_dashboard(
        cycle_id: str,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.get_cycle_dashboard(
                cycle_id,
                organization_id=auth_context.organization_id,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/controls")
    def list_controls(
        cycle_id: str,
        coverage_status: str | None = None,
        search: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        items = service.list_controls(
            cycle_id,
            coverage_status=coverage_status,
            search=search,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/mappings")
    def list_mappings(
        cycle_id: str,
        control_state_id: str | None = None,
        mapping_status: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.list_mappings(
            cycle_id,
            control_state_id=control_state_id,
            mapping_status=mapping_status,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/controls/{control_state_id}")
    def get_control_detail(
        cycle_id: str,
        control_state_id: str,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        del cycle_id
        return success_envelope(
            service.get_control_detail(
                control_state_id,
                organization_id=auth_context.organization_id,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/controls/{control_state_id}/tool-access-audit")
    def list_control_tool_access_audit(
        cycle_id: str,
        control_state_id: str,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        del cycle_id
        response = service.list_control_tool_access_audit(
            control_state_id,
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            tool_name=tool_name,
            execution_status=execution_status,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        paged_response = ToolAccessAuditListResponse(
            total_count=response.total_count,
            items=page_items,
        )
        return success_envelope(
            paged_response,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/evidence/{evidence_id}")
    def get_evidence(
        evidence_id: str,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.get_evidence(
                evidence_id,
                organization_id=auth_context.organization_id,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/evidence-search")
    def search_evidence(
        cycle_id: str,
        query: str,
        limit: int = 5,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        response = service.search_evidence(
            cycle_id,
            query=query,
            limit=limit,
            organization_id=auth_context.organization_id,
        )
        return success_envelope(
            response,
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/memory-records")
    def list_memory_records(
        cycle_id: str,
        scope: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        memory_type: str | None = None,
        status: str | None = "active",
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.list_memory_records(
            cycle_id,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            memory_type=memory_type,
            status=status,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        paged_response = MemoryRecordListResponse(
            cycle_id=response.cycle_id,
            workspace_id=response.workspace_id,
            total_count=response.total_count,
            items=page_items,
        )
        return success_envelope(
            paged_response,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/gaps")
    def list_gaps(
        cycle_id: str,
        status: str | None = None,
        severity: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        items = service.list_gaps(
            cycle_id,
            status=status,
            severity=severity,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/review-queue")
    def list_review_queue(
        cycle_id: str,
        control_state_id: str | None = None,
        severity: str | None = None,
        claim_state: str | None = None,
        sort: str = "recent",
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.list_review_queue(
            cycle_id,
            control_state_id=control_state_id,
            severity=severity,
            claim_state=claim_state,
            sort=sort,
            organization_id=auth_context.organization_id,
            viewer_user_id=auth_context.user_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/review-decisions")
    def list_review_decisions(
        cycle_id: str,
        mapping_id: str | None = None,
        gap_id: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.list_review_decisions(
            cycle_id,
            mapping_id=mapping_id,
            gap_id=gap_id,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/tool-access-audit")
    def list_cycle_tool_access_audit(
        cycle_id: str,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.list_cycle_tool_access_audit(
            cycle_id,
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            tool_name=tool_name,
            execution_status=execution_status,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        paged_response = ToolAccessAuditListResponse(
            total_count=response.total_count,
            items=page_items,
        )
        return success_envelope(
            paged_response,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/tool-access-audit")
    def list_tool_access_audit(
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        execution_status: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.list_tool_access_audit(
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            tool_name=tool_name,
            subject_type=subject_type,
            subject_id=subject_id,
            execution_status=execution_status,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        paged_response = ToolAccessAuditListResponse(
            total_count=response.total_count,
            items=page_items,
        )
        return success_envelope(
            paged_response,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/mappings/{mapping_id}/tool-access-audit")
    def list_mapping_tool_access_audit(
        mapping_id: str,
        workflow_run_id: str | None = None,
        user_id: str | None = None,
        tool_name: str | None = None,
        execution_status: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.list_mapping_tool_access_audit(
            mapping_id,
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            tool_name=tool_name,
            execution_status=execution_status,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        paged_response = ToolAccessAuditListResponse(
            total_count=response.total_count,
            items=page_items,
        )
        return success_envelope(
            paged_response,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.get("/api/v1/auditflow/review-queue")
    def list_review_queue_global(
        cycle_id: str,
        control_state_id: str | None = None,
        severity: str | None = None,
        claim_state: str | None = None,
        sort: str = "recent",
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.list_review_queue(
            cycle_id,
            control_state_id=control_state_id,
            severity=severity,
            claim_state=claim_state,
            sort=sort,
            organization_id=auth_context.organization_id,
            viewer_user_id=auth_context.user_id,
        )
        page_items, next_cursor, has_more = paginate_collection(response.items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
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
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        response = service.list_imports(
            cycle_id,
            ingest_status=status or ingest_status,
            source_type=source_type,
            organization_id=auth_context.organization_id,
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
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.create_upload_import(
            cycle_id,
            command,
            idempotency_key=idempotency_key,
            organization_id=auth_context.organization_id,
            auth_context=auth_context,
        )
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
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.create_external_import(
            cycle_id,
            command,
            idempotency_key=idempotency_key,
            organization_id=auth_context.organization_id,
            auth_context=auth_context,
        )
        return success_envelope(
            response,
            request_id=request_id,
            workflow_run_id=response.workflow_run_id,
        )

    @app.post("/api/v1/auditflow/import-jobs/dispatch")
    def dispatch_import_jobs(
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_product_admin_access),
    ) -> dict[str, object]:
        del auth_context
        return success_envelope(
            service.dispatch_import_jobs(),
            request_id=request_id,
        )

    @app.post("/api/v1/auditflow/mappings/{mapping_id}/review")
    def review_mapping(
        mapping_id: str,
        command: MappingReviewCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.review_mapping(
                mapping_id,
                command,
                idempotency_key=idempotency_key,
                organization_id=auth_context.organization_id,
                reviewer_id=auth_context.user_id,
            ),
            request_id=request_id,
        )

    @app.post("/api/v1/auditflow/mappings/{mapping_id}/claim")
    def claim_mapping(
        mapping_id: str,
        command: MappingClaimCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.claim_mapping(
                mapping_id,
                command,
                idempotency_key=idempotency_key,
                organization_id=auth_context.organization_id,
                reviewer_id=auth_context.user_id,
            ),
            request_id=request_id,
        )

    @app.post("/api/v1/auditflow/mappings/{mapping_id}/claim/release")
    def release_mapping_claim(
        mapping_id: str,
        command: MappingClaimReleaseCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.release_mapping_claim(
                mapping_id,
                command,
                idempotency_key=idempotency_key,
                organization_id=auth_context.organization_id,
                reviewer_id=auth_context.user_id,
            ),
            request_id=request_id,
        )

    @app.post("/api/v1/auditflow/gaps/{gap_id}/decision")
    def decide_gap(
        gap_id: str,
        command: GapDecisionCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.decide_gap(
                gap_id,
                command,
                idempotency_key=idempotency_key,
                organization_id=auth_context.organization_id,
                reviewer_id=auth_context.user_id,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/narratives")
    def list_narratives(
        cycle_id: str,
        snapshot_version: int | None = None,
        narrative_type: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        items = service.list_narratives(
            cycle_id,
            snapshot_version=snapshot_version,
            narrative_type=narrative_type,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.post("/api/v1/auditflow/cycles/process")
    def process_cycle(
        command: CycleProcessingCommand,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_product_admin_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.process_cycle(
                command.model_copy(update={"organization_id": auth_context.organization_id}),
                organization_id=auth_context.organization_id,
                auth_context=auth_context,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/cycles/{cycle_id}/exports")
    def list_export_packages(
        cycle_id: str,
        snapshot_version: int | None = None,
        status: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        items = service.list_export_packages(
            cycle_id,
            snapshot_version=snapshot_version,
            status=status,
            organization_id=auth_context.organization_id,
        )
        page_items, next_cursor, has_more = paginate_collection(items, cursor=cursor, limit=limit)
        return success_envelope(
            page_items,
            request_id=request_id,
            next_cursor=next_cursor,
            has_more=has_more,
        )

    @app.post(
        "/api/v1/auditflow/cycles/{cycle_id}/exports",
        status_code=202,
    )
    def create_export_package(
        cycle_id: str,
        command: ExportCreateCommand,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_reviewer_access),
    ) -> dict[str, object]:
        response = service.create_export_package(
            cycle_id,
            command.model_copy(update={"organization_id": auth_context.organization_id}),
            idempotency_key=idempotency_key,
            organization_id=auth_context.organization_id,
            auth_context=auth_context,
        )
        return success_envelope(
            response,
            request_id=request_id,
            workflow_run_id=response.workflow_run_id,
        )

    @app.post("/api/v1/auditflow/exports/generate")
    def generate_export(
        command: ExportGenerationCommand,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_product_admin_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.generate_export(
                command.model_copy(update={"organization_id": auth_context.organization_id}),
                organization_id=auth_context.organization_id,
                auth_context=auth_context,
            ),
            request_id=request_id,
        )

    @app.get("/api/v1/auditflow/exports/{package_id}")
    def get_export_package(
        package_id: str,
        request_id: str | None = Header(default=None, alias="X-Request-Id"),
        auth_context=Depends(require_viewer_access),
    ) -> dict[str, object]:
        return success_envelope(
            service.get_export_package(
                package_id,
                organization_id=auth_context.organization_id,
            ),
            request_id=request_id,
        )

    return app
