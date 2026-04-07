from __future__ import annotations

from datetime import UTC, datetime
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.api_models import (
    AuditCycleDashboardResponse,
    AuditCycleSummary,
    AuditWorkspaceSummary,
    ControlCoverageSummary,
    ControlDetailResponse,
    EvidenceSearchItem,
    EvidenceSearchResponse,
    MemoryRecordListResponse,
    MemoryRecordSummary,
    ReviewQueueItem,
    ReviewQueueResponse,
    ToolAccessAuditListResponse,
    ToolAccessAuditSummary,
    ToolAccessSummary,
)
from auditflow_app.auth import AuditFlowAuthorizationError, HeaderAuditFlowAuthorizer
from auditflow_app.routes import (
    _event_topics,
    _event_topic,
    _format_sse_message,
    _matches_event_topic,
    _normalize_resume_after_id,
    _resolve_outbox_event_context,
    create_fastapi_app,
    map_domain_error,
    paginate_collection,
    success_envelope,
)
from auditflow_app.shared_runtime import load_shared_agent_platform

_AP = load_shared_agent_platform()
TestClient = _AP.fastapi_test_client_class()


class AuditFlowRouteErrorMappingTests(unittest.TestCase):
    def test_maps_workspace_not_found_key_error_to_404(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                KeyError("workspace-404"),
                path="/api/v1/auditflow/workspaces/workspace-404",
            ),
            status_code=404,
            error_code="AUDIT_WORKSPACE_NOT_FOUND",
        )

    def test_maps_cycle_not_found_key_error_to_404(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                KeyError("cycle-404"),
                path="/api/v1/auditflow/cycles/cycle-404/dashboard",
            ),
            status_code=404,
            error_code="AUDIT_CYCLE_NOT_FOUND",
        )

    def test_maps_mapping_stale_conflict_to_contract_code(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                ValueError("CONFLICT_STALE_RESOURCE"),
                path="/api/v1/auditflow/mappings/mapping-1/review",
            ),
            status_code=409,
            error_code="MAPPING_REVIEW_CONFLICT",
        )

    def test_maps_export_readiness_error_to_422(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                ValueError("CYCLE_NOT_READY_FOR_EXPORT"),
                path="/api/v1/auditflow/cycles/cycle-1/exports",
            ),
            status_code=422,
            error_code="CYCLE_NOT_READY_FOR_EXPORT",
        )

    def test_maps_invalid_review_queue_sort_to_400(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                ValueError("INVALID_REVIEW_QUEUE_SORT"),
                path="/api/v1/auditflow/review-queue",
            ),
            status_code=400,
            error_code="INVALID_REVIEW_QUEUE_SORT",
        )

    def test_maps_duplicate_workspace_slug_to_409(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                ValueError("WORKSPACE_SLUG_ALREADY_EXISTS"),
                path="/api/v1/auditflow/workspaces",
            ),
            status_code=409,
            error_code="WORKSPACE_SLUG_ALREADY_EXISTS",
        )

    def test_success_envelope_includes_workflow_and_request_metadata(self) -> None:
        payload = success_envelope(
            {"package_id": "pkg-1"},
            request_id="req-123",
            workflow_run_id="wf-123",
        )

        _AP.assert_success_envelope(
            self,
            payload,
            data_expected_fields={"package_id": "pkg-1"},
            meta_expected_fields={
                "request_id": "req-123",
                "workflow_run_id": "wf-123",
                "has_more": False,
            },
        )

    def test_paginate_collection_uses_cursor_metadata(self) -> None:
        next_cursor = _AP.assert_paginated_window(
            self,
            paginate_collection([1, 2, 3], limit=2),
            expected_items=[1, 2],
            has_more=True,
            next_cursor_present=True,
        )
        _AP.assert_paginated_window(
            self,
            paginate_collection(
                [1, 2, 3],
                cursor=next_cursor,
                limit=2,
            ),
            expected_items=[3],
            has_more=False,
            next_cursor_present=False,
        )

    def test_invalid_pagination_cursor_maps_to_400(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_CURSOR"):
            paginate_collection([1, 2], cursor="bad-cursor", limit=1)

    def test_maps_idempotency_conflict_to_409(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                ValueError("IDEMPOTENCY_CONFLICT"),
                path="/api/v1/auditflow/cycles",
            ),
            status_code=409,
            error_code="IDEMPOTENCY_CONFLICT",
        )

    def test_maps_invalid_artifact_bytes_to_400(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                ValueError("INVALID_ARTIFACT_BYTES"),
                path="/api/v1/auditflow/cycles/cycle-1/imports/upload",
            ),
            status_code=400,
            error_code="INVALID_ARTIFACT_BYTES",
        )

    def test_maps_invalid_search_query_to_400(self) -> None:
        _AP.assert_domain_error_mapping(
            self,
            map_domain_error(
                ValueError("INVALID_SEARCH_QUERY"),
                path="/api/v1/auditflow/cycles/cycle-1/evidence-search",
            ),
            status_code=400,
            error_code="INVALID_SEARCH_QUERY",
        )

    def test_resolves_sse_event_context_from_runtime_state(self) -> None:
        state_store = SimpleNamespace(
            load=lambda workflow_run_id: SimpleNamespace(
                state={
                    "organization_id": "org-1",
                    "workspace_id": "ws-1",
                    "subject_type": "audit_cycle",
                    "subject_id": "cycle-1",
                }
            )
        )
        service = SimpleNamespace(runtime_stores=SimpleNamespace(state_store=state_store))
        event = SimpleNamespace(
            event_id="evt-1",
            event_name="workflow.step.completed",
            workflow_run_id="wf-1",
            aggregate_type="audit_cycle",
            aggregate_id="cycle-1",
            payload={"current_state": "review"},
            emitted_at=datetime(2026, 3, 17, 10, 0, tzinfo=UTC),
        )

        context = _resolve_outbox_event_context(service, event)

        _AP.assert_fields(
            self,
            context,
            expected_fields={
                "workspace_id": "ws-1",
                "subject_type": "audit_cycle",
                "subject_id": "cycle-1",
                "topic": "workflow",
            },
        )

    def test_resolves_sse_event_context_from_event_payload_fallback(self) -> None:
        state_store = SimpleNamespace(load=lambda workflow_run_id: (_ for _ in ()).throw(KeyError(workflow_run_id)))
        service = SimpleNamespace(runtime_stores=SimpleNamespace(state_store=state_store))
        event = SimpleNamespace(
            event_id="evt-2",
            event_name="auditflow.import.requested",
            workflow_run_id="wf-missing",
            aggregate_type="audit_cycle",
            aggregate_id="cycle-2",
            payload={
                "organization_id": "org-2",
                "workspace_id": "ws-2",
                "cycle_id": "cycle-2",
            },
            emitted_at=datetime(2026, 3, 17, 10, 5, tzinfo=UTC),
        )

        context = _resolve_outbox_event_context(service, event)

        _AP.assert_fields(
            self,
            context,
            expected_fields={
                "organization_id": "org-2",
                "workspace_id": "ws-2",
                "subject_type": "audit_cycle",
                "subject_id": "cycle-2",
            },
        )

    def test_formats_sse_message_with_json_payload(self) -> None:
        message = _format_sse_message(
            event_id="evt-1",
            event_name="auditflow.package.ready",
            payload={"workspace_id": "ws-1"},
        )

        _AP.assert_sse_message_contract(
            self,
            message,
            event_id="evt-1",
            event_name="auditflow.package.ready",
            expected_substrings=['"workspace_id": "ws-1"'],
            resolved_topic=_event_topic("auditflow.package.ready"),
            expected_topic="auditflow",
        )

    def test_event_topics_include_domain_specific_aliases(self) -> None:
        context = {
            "topic": "auditflow",
            "workspace_id": "ws-1",
            "subject_type": "audit_cycle",
            "subject_id": "cycle-1",
            "payload": {"cycle_id": "cycle-1", "package_id": "pkg-1"},
        }

        topics = _event_topics(context)

        _AP.assert_event_topic_routing(
            self,
            context,
            topics,
            expected_topics=[
                "auditflow.workspace.ws-1",
                "auditflow.cycle.cycle-1",
                "auditflow.export.pkg-1",
            ],
            matcher=_matches_event_topic,
            matching_topic="auditflow.cycle.cycle-1",
            rejected_topic="workflow",
        )

    def test_missing_last_event_id_does_not_block_stream_progress(self) -> None:
        pending = [SimpleNamespace(event=SimpleNamespace(event_id="evt-1"))]

        _AP.assert_resume_after_id_contract(
            self,
            _normalize_resume_after_id,
            pending,
            missing_id="evt-missing",
            existing_id="evt-1",
        )


class AuditFlowAuthorizationTests(unittest.TestCase):
    def test_authorizer_defaults_to_viewer_role(self) -> None:
        context = HeaderAuditFlowAuthorizer().authorize(
            required_role="viewer",
            authorization="Bearer test-token",
            organization_id="org-1",
        )

        self.assertEqual(context.organization_id, "org-1")
        self.assertEqual(context.role, "viewer")
        self.assertEqual(context.user_id, "demo-user")

    def test_authorizer_rejects_missing_tenant_header(self) -> None:
        with self.assertRaises(AuditFlowAuthorizationError) as context:
            HeaderAuditFlowAuthorizer().authorize(
                required_role="viewer",
                authorization="Bearer test-token",
                organization_id=None,
            )

        self.assertEqual(context.exception.code, "TENANT_CONTEXT_REQUIRED")
        self.assertEqual(context.exception.status_code, 400)

    def test_authorizer_rejects_insufficient_role(self) -> None:
        with self.assertRaises(AuditFlowAuthorizationError) as context:
            HeaderAuditFlowAuthorizer().authorize(
                required_role="reviewer",
                authorization="Bearer test-token",
                organization_id="org-1",
                user_role="viewer",
            )

        self.assertEqual(context.exception.code, "AUTH_FORBIDDEN")
        self.assertEqual(context.exception.status_code, 403)

    def test_authorizer_accepts_org_admin_alias_for_product_admin_routes(self) -> None:
        context = HeaderAuditFlowAuthorizer().authorize(
            required_role="product_admin",
            authorization="Bearer test-token",
            organization_id="org-1",
            user_role="org_admin",
            user_id="admin-1",
        )

        self.assertEqual(context.role, "product_admin")
        self.assertEqual(context.user_id, "admin-1")


@unittest.skipIf(TestClient is None, "fastapi test client unavailable")
class AuditFlowRouteAuthorizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = _AP.create_managed_test_client(self, create_fastapi_app(_stub_service()))

    def test_health_route_remains_public(self) -> None:
        response = self.client.get("/health")

        _AP.assert_health_response(self, response, product="auditflow")

    def test_viewer_route_requires_authorization_header(self) -> None:
        _AP.request_and_assert_json_error(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/cycles",
            status_code=401,
            error_code="AUTH_REQUIRED",
            params={"workspace_id": "ws-1"},
            headers={"X-Organization-Id": "org-1"},
        )

    def test_viewer_route_requires_organization_context(self) -> None:
        _AP.request_and_assert_json_error(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/cycles",
            status_code=400,
            error_code="TENANT_CONTEXT_REQUIRED",
            params={"workspace_id": "ws-1"},
            headers={"Authorization": "Bearer test-token"},
        )

    def test_reviewer_route_rejects_viewer_role(self) -> None:
        _AP.request_with_header_auth_and_assert_json_error(
            self,
            self.client,
            "POST",
            "/api/v1/auditflow/cycles",
            role="viewer",
            status_code=403,
            error_code="AUTH_FORBIDDEN",
            headers={
                "Idempotency-Key": "cycle-create-1",
            },
            json={
                "workspace_id": "ws-1",
                "cycle_name": "SOC2 2026",
            },
        )

    def test_reviewer_route_allows_reviewer_role(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "POST",
            "/api/v1/auditflow/cycles",
            role="reviewer",
            status_code=201,
            headers={
                "Idempotency-Key": "cycle-create-2",
            },
            json={
                "workspace_id": "ws-1",
                "cycle_name": "SOC2 2026",
            },
        )

        _AP.assert_fields(self, data, expected_fields={"id": "cycle-1"})

    def test_product_admin_route_accepts_org_admin_alias(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "POST",
            "/api/v1/auditflow/workspaces",
            role="org_admin",
            status_code=201,
            json={
                "name": "Acme SOC2",
                "slug": "acme-soc2",
                "default_framework": "SOC2",
            },
        )

        _AP.assert_fields(self, data, expected_fields={"id": "ws-1"})

    def test_runtime_capabilities_route_requires_product_admin_access(self) -> None:
        _AP.request_with_header_auth_and_assert_json_error(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/runtime-capabilities",
            role="reviewer",
            status_code=403,
            error_code="AUTH_FORBIDDEN",
        )

    def test_runtime_capabilities_route_returns_capability_payload_for_admin(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/runtime-capabilities",
            role="org_admin",
        )

        _AP.assert_fields(
            self,
            data,
            expected_fields={
                "product": "auditflow",
                "vector_search.backend_id": "ann-metadata-json",
            },
        )

    def test_viewer_route_allows_evidence_search(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/cycles/cycle-1/evidence-search",
            role="viewer",
            params={"query": "access review"},
        )

        _AP.assert_fields(
            self,
            data,
            expected_fields={"total_count": 1, "items.0.evidence_chunk_id": "chunk-1"},
        )

    def test_memory_records_route_requires_reviewer_role(self) -> None:
        _AP.request_with_header_auth_and_assert_json_error(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/cycles/cycle-1/memory-records",
            role="viewer",
            status_code=403,
            error_code="AUTH_FORBIDDEN",
        )

    def test_reviewer_route_allows_mapping_claim(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "POST",
            "/api/v1/auditflow/mappings/mapping-1/claim",
            role="reviewer",
            headers={
                "Idempotency-Key": "claim-1",
            },
            json={"lease_seconds": 600},
        )

        _AP.assert_fields(
            self,
            data,
            expected_fields={"mapping_id": "mapping-1", "claimed_by_user_id": "user-1"},
        )

    def test_product_admin_route_allows_mapping_assignment(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "POST",
            "/api/v1/auditflow/mappings/mapping-1/assign",
            role="org_admin",
            headers={
                "Idempotency-Key": "assign-1",
            },
            json={"reviewer_user_id": "user-2", "note": "Route to the access-review owner."},
        )

        _AP.assert_fields(
            self,
            data,
            expected_fields={"mapping_id": "mapping-1", "assigned_reviewer_id": "user-2"},
        )

    def test_reviewer_route_rejects_mapping_assignment_without_admin_access(self) -> None:
        _AP.request_with_header_auth_and_assert_json_error(
            self,
            self.client,
            "POST",
            "/api/v1/auditflow/mappings/mapping-1/assign",
            role="reviewer",
            status_code=403,
            error_code="AUTH_FORBIDDEN",
            headers={
                "Idempotency-Key": "assign-2",
            },
            json={"reviewer_user_id": "user-2", "note": "Route to the access-review owner."},
        )

    def test_reviewer_route_lists_tool_access_audit(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/tool-access-audit",
            role="reviewer",
            params={"workflow_run_id": "wf-tool-1", "tool_name": "evidence.search"},
        )

        _AP.assert_fields(self, data, expected_fields={"total_count": 1})
        _AP.assert_collection_contains(
            self,
            data["items"],
            expected_fields={"workflow_run_id": "wf-tool-1", "tool_name": "evidence.search"},
        )

    def test_viewer_route_returns_cycle_dashboard_with_tool_access_summary(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/cycles/cycle-1/dashboard",
            role="viewer",
        )

        _AP.assert_fields(
            self,
            data,
            expected_fields={
                "cycle.id": "cycle-1",
                "tool_access_summary.total_count": 2,
                "tool_access_summary.latest_workflow_run_id": "wf-tool-2",
            },
        )

    def test_reviewer_route_lists_cycle_tool_access_audit(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/cycles/cycle-1/tool-access-audit",
            role="reviewer",
            params={"workflow_run_id": "wf-tool-2", "tool_name": "mapping.read_candidates"},
        )

        _AP.assert_fields(self, data, expected_fields={"total_count": 1})
        _AP.assert_collection_contains(
            self,
            data["items"],
            expected_fields={"workflow_run_id": "wf-tool-2", "tool_name": "mapping.read_candidates"},
        )

    def test_viewer_route_returns_control_detail_with_tool_access_summary(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/cycles/cycle-1/controls/control-state-1",
            role="viewer",
        )

        _AP.assert_fields(
            self,
            data,
            expected_fields={
                "control_state.control_code": "CC6.1",
                "tool_access_summary.total_count": 2,
                "tool_access_summary.latest_workflow_run_id": "wf-tool-3",
            },
        )

    def test_reviewer_route_lists_control_tool_access_audit(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/cycles/cycle-1/controls/control-state-1/tool-access-audit",
            role="reviewer",
            params={"tool_name": "mapping.read_candidates"},
        )

        _AP.assert_fields(self, data, expected_fields={"total_count": 1})
        _AP.assert_collection_contains(
            self,
            data["items"],
            expected_fields={"workflow_run_id": "wf-tool-2", "tool_name": "mapping.read_candidates"},
        )

    def test_reviewer_route_returns_review_queue_with_mapping_tool_access_summary(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/review-queue",
            role="reviewer",
            params={"cycle_id": "cycle-1"},
        )

        _AP.assert_collection_size(self, data, size=1)
        _AP.assert_collection_contains(
            self,
            data,
            expected_fields={
                "mapping_id": "mapping-1",
                "tool_access_summary.total_count": 2,
                "tool_access_summary.latest_workflow_run_id": "wf-tool-3",
            },
        )

    def test_reviewer_route_lists_mapping_tool_access_audit(self) -> None:
        _response, data = _AP.request_with_header_auth_and_get_data(
            self,
            self.client,
            "GET",
            "/api/v1/auditflow/mappings/mapping-1/tool-access-audit",
            role="reviewer",
            params={"tool_name": "mapping.read_candidates"},
        )

        _AP.assert_fields(self, data, expected_fields={"total_count": 1})
        _AP.assert_collection_contains(
            self,
            data["items"],
            expected_fields={"workflow_run_id": "wf-tool-2", "tool_name": "mapping.read_candidates"},
        )

def _stub_service():
    created_at = datetime(2026, 3, 18, 15, 0, tzinfo=UTC)
    workspace = AuditWorkspaceSummary(
        workspace_id="ws-1",
        workspace_name="Acme SOC2",
        slug="acme-soc2",
        framework_name="SOC2",
        workspace_status="active",
        created_at=created_at,
    )
    cycle = AuditCycleSummary(
        cycle_id="cycle-1",
        workspace_id="ws-1",
        cycle_name="SOC2 2026",
        cycle_status="draft",
        framework_name="SOC2",
        coverage_status="pending_review",
        review_queue_count=0,
        open_gap_count=0,
    )
    search_response = EvidenceSearchResponse(
        cycle_id="cycle-1",
        workspace_id="ws-1",
        query="access review",
        total_count=1,
        items=[
            EvidenceSearchItem(
                evidence_chunk_id="chunk-1",
                evidence_item_id="evidence-1",
                score=1.8,
                summary="Quarterly access review completed.",
                title="Jira Access Review Ticket",
                section_label="Description",
                text_excerpt="Quarterly access review completed for production systems.",
                source_type="jira",
                captured_at=created_at,
            )
        ],
    )
    memory_response = MemoryRecordListResponse(
        cycle_id="cycle-1",
        workspace_id="ws-1",
        total_count=1,
        items=[
            MemoryRecordSummary(
                memory_id="memory-1",
                scope="organization",
                subject_type="framework_control",
                subject_id="SOC2:CC6.1",
                memory_key="mapping:mapping-1",
                memory_type="pattern",
                value={"decision": "accept", "control_code": "CC6.1"},
                confidence=1.0,
                source_kind="human_feedback",
                source_ref={"mapping_id": "mapping-1"},
                status="active",
                created_at=created_at,
                updated_at=created_at,
            )
        ],
    )
    tool_access_response = ToolAccessAuditListResponse(
        total_count=1,
        items=[
            ToolAccessAuditSummary(
                tool_access_audit_id="tool-access-1",
                workflow_run_id="wf-tool-1",
                node_name="mapping",
                tool_call_id="tool-call-1",
                tool_name="evidence.search",
                tool_version="2026-03-16.1",
                adapter_type="vector_store",
                subject_type="audit_cycle",
                subject_id="cycle-1",
                workspace_id="ws-1",
                user_id="user-1",
                role="reviewer",
                session_id="auth-session-1",
                connection_id=None,
                execution_status="success",
                error_code=None,
                arguments={"query": "access review", "limit": 5},
                source_locator="auditflow://cycles/cycle-1/evidence-search?query=access review",
                recorded_at=created_at,
                completed_at=created_at,
            )
        ],
    )
    cycle_tool_access_response = ToolAccessAuditListResponse(
        total_count=1,
        items=[
            ToolAccessAuditSummary(
                tool_access_audit_id="tool-access-2",
                workflow_run_id="wf-tool-2",
                node_name="challenge",
                tool_call_id="tool-call-2",
                tool_name="mapping.read_candidates",
                tool_version="2026-03-16.1",
                adapter_type="auditflow_database",
                subject_type="audit_cycle",
                subject_id="cycle-1",
                workspace_id="ws-1",
                user_id="user-2",
                role="reviewer",
                session_id="auth-session-2",
                connection_id=None,
                execution_status="success",
                error_code=None,
                arguments={"control_id": "control-state-1"},
                source_locator="auditflow://cycles/cycle-1/mappings",
                recorded_at=created_at,
                completed_at=created_at,
            )
        ],
    )
    control_tool_access_response = ToolAccessAuditListResponse(
        total_count=1,
        items=[
            ToolAccessAuditSummary(
                tool_access_audit_id="tool-access-3",
                workflow_run_id="wf-tool-2",
                node_name="challenge",
                tool_call_id="tool-call-3",
                tool_name="mapping.read_candidates",
                tool_version="2026-03-16.1",
                adapter_type="auditflow_database",
                subject_type="audit_cycle",
                subject_id="cycle-1",
                workspace_id="ws-1",
                user_id="user-2",
                role="reviewer",
                session_id="auth-session-2",
                connection_id=None,
                execution_status="success",
                error_code=None,
                arguments={"control_id": "control-state-1"},
                source_locator="auditflow://cycles/cycle-1/mappings",
                recorded_at=created_at,
                completed_at=created_at,
            )
        ],
    )
    mapping_tool_access_response = ToolAccessAuditListResponse(
        total_count=1,
        items=[
            ToolAccessAuditSummary(
                tool_access_audit_id="tool-access-4",
                workflow_run_id="wf-tool-2",
                node_name="challenge",
                tool_call_id="tool-call-4",
                tool_name="mapping.read_candidates",
                tool_version="2026-03-16.1",
                adapter_type="auditflow_database",
                subject_type="audit_cycle",
                subject_id="cycle-1",
                workspace_id="ws-1",
                user_id="user-2",
                role="reviewer",
                session_id="auth-session-2",
                connection_id=None,
                execution_status="success",
                error_code=None,
                arguments={"evidence_item_id": "evidence-1", "control_id": "control-state-1"},
                source_locator="auditflow://cycles/cycle-1/mappings",
                recorded_at=created_at,
                completed_at=created_at,
            )
        ],
    )
    dashboard = AuditCycleDashboardResponse(
        cycle=cycle,
        review_queue_count=0,
        open_gap_count=0,
        accepted_mapping_count=1,
        export_ready=False,
        controls=[],
        latest_export_package=None,
        tool_access_summary=ToolAccessSummary(
            total_count=2,
            latest_completed_at=created_at,
            latest_workflow_run_id="wf-tool-2",
            recent_tool_names=["mapping.read_candidates", "evidence.search"],
            execution_status_counts={"success": 2},
        ),
    )
    control_detail = ControlDetailResponse(
        control_state=ControlCoverageSummary(
            control_state_id="control-state-1",
            control_code="CC6.1",
            coverage_status="pending_review",
            mapped_evidence_count=1,
            open_gap_count=0,
        ),
        accepted_mappings=[],
        pending_mappings=[],
        open_gaps=[],
        tool_access_summary=ToolAccessSummary(
            total_count=2,
            latest_completed_at=created_at,
            latest_workflow_run_id="wf-tool-3",
            recent_tool_names=["review_decision.read_history", "mapping.read_candidates"],
            execution_status_counts={"success": 2},
        ),
    )
    review_queue = ReviewQueueResponse(
        cycle_id="cycle-1",
        total_count=1,
        items=[
            ReviewQueueItem(
                mapping_id="mapping-1",
                control_state_id="control-state-1",
                control_code="CC6.1",
                coverage_status="pending_review",
                snapshot_version=1,
                evidence_item_id="evidence-1",
                rationale_summary="Candidate mapping awaiting reviewer confirmation.",
                citation_refs=[],
                assigned_reviewer_id=None,
                assigned_at=None,
                assignment_note=None,
                assignment_status="unassigned",
                claimed_by_user_id=None,
                claimed_at=None,
                claim_expires_at=None,
                claim_status="unclaimed",
                priority_tier="medium",
                priority_score=48.0,
                priority_reason="Pending mapping is waiting for routine reviewer confirmation.",
                updated_at=created_at,
                tool_access_summary=ToolAccessSummary(
                    total_count=2,
                    latest_completed_at=created_at,
                    latest_workflow_run_id="wf-tool-3",
                    recent_tool_names=["review_decision.read_history", "mapping.read_candidates"],
                    execution_status_counts={"success": 2},
                ),
            )
        ],
    )
    return SimpleNamespace(
        list_workflows=lambda: [],
        get_workflow_state=lambda workflow_run_id, organization_id=None: {
            "workflow_run_id": workflow_run_id,
            "workflow_type": "auditflow_cycle",
            "current_state": "reviewing",
            "checkpoint_seq": 1,
            "raw_state": {},
        },
        create_workspace=lambda command, organization_id=None: workspace,
        get_workspace=lambda workspace_id, organization_id=None: workspace,
        get_runtime_capabilities=lambda: {
            "product": "auditflow",
            "model_provider": {
                "requested_mode": "auto",
                "effective_mode": "local",
                "backend_id": "heuristic-local",
                "fallback_reason": "MODEL_PROVIDER_NOT_CONFIGURED",
                "details": {},
            },
            "embedding_provider": {
                "requested_mode": "auto",
                "effective_mode": "local",
                "backend_id": "semantic-v1",
                "fallback_reason": "OPENAI_EMBEDDING_NOT_CONFIGURED",
                "details": {},
            },
            "vector_search": {
                "requested_mode": "auto",
                "effective_mode": "ann",
                "backend_id": "ann-metadata-json",
                "fallback_reason": None,
                "details": {},
            },
            "connectors": {
                "jira": {
                    "requested_mode": "auto",
                    "effective_mode": "local",
                    "backend_id": "jira-synthetic",
                    "fallback_reason": "CONNECTOR_HTTP_TEMPLATE_NOT_CONFIGURED",
                    "details": {},
                }
            },
        },
        create_cycle=lambda command, idempotency_key=None, organization_id=None: cycle,
        list_cycles=lambda workspace_id, status=None, organization_id=None: [cycle],
        get_cycle_dashboard=lambda cycle_id, organization_id=None: dashboard,
        list_controls=lambda cycle_id, coverage_status=None, search=None, organization_id=None: [],
        list_mappings=lambda cycle_id, control_state_id=None, mapping_status=None, organization_id=None: SimpleNamespace(items=[]),
        get_control_detail=lambda control_state_id, organization_id=None: control_detail,
        get_evidence=lambda evidence_id, organization_id=None: SimpleNamespace(model_dump=lambda **kwargs: {}),
        search_evidence=lambda cycle_id, query, limit=5, organization_id=None: search_response.model_copy(update={"query": query}),
        list_memory_records=lambda cycle_id, **filters: memory_response,
        list_gaps=lambda cycle_id, status=None, severity=None, organization_id=None: [],
        list_review_queue=lambda cycle_id, control_state_id=None, severity=None, claim_state=None, assignment_state=None, priority=None, sort="recent", organization_id=None, viewer_user_id=None: review_queue,
        list_review_decisions=lambda cycle_id, mapping_id=None, gap_id=None, organization_id=None: SimpleNamespace(items=[]),
        list_tool_access_audit=lambda workflow_run_id=None, user_id=None, tool_name=None, subject_type=None, subject_id=None, execution_status=None, organization_id=None: tool_access_response,
        list_cycle_tool_access_audit=lambda cycle_id, workflow_run_id=None, user_id=None, tool_name=None, execution_status=None, organization_id=None: cycle_tool_access_response,
        list_control_tool_access_audit=lambda control_state_id, workflow_run_id=None, user_id=None, tool_name=None, execution_status=None, organization_id=None: control_tool_access_response,
        list_mapping_tool_access_audit=lambda mapping_id, workflow_run_id=None, user_id=None, tool_name=None, execution_status=None, organization_id=None: mapping_tool_access_response,
        list_imports=lambda cycle_id, ingest_status=None, source_type=None, organization_id=None: SimpleNamespace(items=[]),
        create_upload_import=lambda cycle_id, command, idempotency_key=None, organization_id=None, auth_context=None: SimpleNamespace(
            workflow_run_id="wf-1",
            model_dump=lambda **kwargs: {"workflow_run_id": "wf-1"},
        ),
        create_external_import=lambda cycle_id, command, idempotency_key=None, organization_id=None, auth_context=None: SimpleNamespace(
            workflow_run_id="wf-2",
            model_dump=lambda **kwargs: {"workflow_run_id": "wf-2"},
        ),
        claim_mapping=lambda mapping_id, command, idempotency_key=None, organization_id=None, reviewer_id=None: SimpleNamespace(
            model_dump=lambda **kwargs: {
                "mapping_id": mapping_id,
                "mapping_status": "proposed",
                "assigned_reviewer_id": None,
                "assigned_at": None,
                "assignment_note": None,
                "assignment_status": "unassigned",
                "claimed_by_user_id": reviewer_id,
                "claimed_at": created_at,
                "claim_expires_at": created_at,
                "claim_status": "claimed_by_me",
            }
        ),
        assign_mapping=lambda mapping_id, command, idempotency_key=None, organization_id=None, reviewer_id=None, reviewer_role=None: SimpleNamespace(
            model_dump=lambda **kwargs: {
                "mapping_id": mapping_id,
                "mapping_status": "proposed",
                "assigned_reviewer_id": command.reviewer_user_id,
                "assigned_at": created_at,
                "assignment_note": command.note,
                "assignment_status": (
                    "assigned_to_me" if command.reviewer_user_id == reviewer_id else "assigned_to_other"
                ),
                "claimed_by_user_id": None,
                "claimed_at": None,
                "claim_expires_at": None,
                "claim_status": "unclaimed",
            }
        ),
        release_mapping_claim=lambda mapping_id, command=None, idempotency_key=None, organization_id=None, reviewer_id=None: SimpleNamespace(
            model_dump=lambda **kwargs: {
                "mapping_id": mapping_id,
                "mapping_status": "proposed",
                "assigned_reviewer_id": None,
                "assigned_at": None,
                "assignment_note": None,
                "assignment_status": "unassigned",
                "claimed_by_user_id": None,
                "claimed_at": None,
                "claim_expires_at": None,
                "claim_status": "unclaimed",
            }
        ),
        release_mapping_assignment=lambda mapping_id, command=None, idempotency_key=None, organization_id=None, reviewer_id=None, reviewer_role=None: SimpleNamespace(
            model_dump=lambda **kwargs: {
                "mapping_id": mapping_id,
                "mapping_status": "proposed",
                "assigned_reviewer_id": None,
                "assigned_at": None,
                "assignment_note": None,
                "assignment_status": "unassigned",
                "claimed_by_user_id": None,
                "claimed_at": None,
                "claim_expires_at": None,
                "claim_status": "unclaimed",
            }
        ),
        review_mapping=lambda mapping_id, command, idempotency_key=None, organization_id=None, reviewer_id=None: SimpleNamespace(model_dump=lambda **kwargs: {}),
        decide_gap=lambda gap_id, command, idempotency_key=None, organization_id=None, reviewer_id=None: SimpleNamespace(model_dump=lambda **kwargs: {}),
        list_narratives=lambda cycle_id, snapshot_version=None, narrative_type=None, organization_id=None: [],
        process_cycle=lambda command, organization_id=None, auth_context=None: SimpleNamespace(model_dump=lambda **kwargs: {}),
        list_export_packages=lambda cycle_id, snapshot_version=None, status=None, organization_id=None: [],
        create_export_package=lambda cycle_id, command, idempotency_key=None, organization_id=None, auth_context=None: SimpleNamespace(
            workflow_run_id="wf-3",
            model_dump=lambda **kwargs: {"workflow_run_id": "wf-3"},
        ),
        generate_export=lambda command, organization_id=None, auth_context=None: SimpleNamespace(model_dump=lambda **kwargs: {}),
        get_export_package=lambda package_id, organization_id=None: SimpleNamespace(model_dump=lambda **kwargs: {}),
    )


if __name__ == "__main__":
    unittest.main()
