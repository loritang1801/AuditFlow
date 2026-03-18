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

from auditflow_app.api_models import AuditCycleSummary, AuditWorkspaceSummary
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

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover - exercised only when fastapi is absent
    TestClient = None


class AuditFlowRouteErrorMappingTests(unittest.TestCase):
    def test_maps_workspace_not_found_key_error_to_404(self) -> None:
        status_code, payload = map_domain_error(
            KeyError("workspace-404"),
            path="/api/v1/auditflow/workspaces/workspace-404",
        )

        self.assertEqual(status_code, 404)
        self.assertEqual(payload["error"]["code"], "AUDIT_WORKSPACE_NOT_FOUND")

    def test_maps_cycle_not_found_key_error_to_404(self) -> None:
        status_code, payload = map_domain_error(
            KeyError("cycle-404"),
            path="/api/v1/auditflow/cycles/cycle-404/dashboard",
        )

        self.assertEqual(status_code, 404)
        self.assertEqual(payload["error"]["code"], "AUDIT_CYCLE_NOT_FOUND")

    def test_maps_mapping_stale_conflict_to_contract_code(self) -> None:
        status_code, payload = map_domain_error(
            ValueError("CONFLICT_STALE_RESOURCE"),
            path="/api/v1/auditflow/mappings/mapping-1/review",
        )

        self.assertEqual(status_code, 409)
        self.assertEqual(payload["error"]["code"], "MAPPING_REVIEW_CONFLICT")

    def test_maps_export_readiness_error_to_422(self) -> None:
        status_code, payload = map_domain_error(
            ValueError("CYCLE_NOT_READY_FOR_EXPORT"),
            path="/api/v1/auditflow/cycles/cycle-1/exports",
        )

        self.assertEqual(status_code, 422)
        self.assertEqual(payload["error"]["code"], "CYCLE_NOT_READY_FOR_EXPORT")

    def test_maps_invalid_review_queue_sort_to_400(self) -> None:
        status_code, payload = map_domain_error(
            ValueError("INVALID_REVIEW_QUEUE_SORT"),
            path="/api/v1/auditflow/review-queue",
        )

        self.assertEqual(status_code, 400)
        self.assertEqual(payload["error"]["code"], "INVALID_REVIEW_QUEUE_SORT")

    def test_maps_duplicate_workspace_slug_to_409(self) -> None:
        status_code, payload = map_domain_error(
            ValueError("WORKSPACE_SLUG_ALREADY_EXISTS"),
            path="/api/v1/auditflow/workspaces",
        )

        self.assertEqual(status_code, 409)
        self.assertEqual(payload["error"]["code"], "WORKSPACE_SLUG_ALREADY_EXISTS")

    def test_success_envelope_includes_workflow_and_request_metadata(self) -> None:
        payload = success_envelope(
            {"package_id": "pkg-1"},
            request_id="req-123",
            workflow_run_id="wf-123",
        )

        self.assertEqual(payload["data"]["package_id"], "pkg-1")
        self.assertEqual(payload["meta"]["request_id"], "req-123")
        self.assertEqual(payload["meta"]["workflow_run_id"], "wf-123")
        self.assertEqual(payload["meta"]["has_more"], False)

    def test_paginate_collection_uses_cursor_metadata(self) -> None:
        page_one, next_cursor, has_more = paginate_collection([1, 2, 3], limit=2)
        page_two, second_cursor, second_has_more = paginate_collection(
            [1, 2, 3],
            cursor=next_cursor,
            limit=2,
        )

        self.assertEqual(page_one, [1, 2])
        self.assertTrue(has_more)
        self.assertIsNotNone(next_cursor)
        self.assertEqual(page_two, [3])
        self.assertFalse(second_has_more)
        self.assertIsNone(second_cursor)

    def test_invalid_pagination_cursor_maps_to_400(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVALID_CURSOR"):
            paginate_collection([1, 2], cursor="bad-cursor", limit=1)

    def test_maps_idempotency_conflict_to_409(self) -> None:
        status_code, payload = map_domain_error(
            ValueError("IDEMPOTENCY_CONFLICT"),
            path="/api/v1/auditflow/cycles",
        )

        self.assertEqual(status_code, 409)
        self.assertEqual(payload["error"]["code"], "IDEMPOTENCY_CONFLICT")

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

        self.assertEqual(context["workspace_id"], "ws-1")
        self.assertEqual(context["subject_type"], "audit_cycle")
        self.assertEqual(context["subject_id"], "cycle-1")
        self.assertEqual(context["topic"], "workflow")

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

        self.assertEqual(context["organization_id"], "org-2")
        self.assertEqual(context["workspace_id"], "ws-2")
        self.assertEqual(context["subject_type"], "audit_cycle")
        self.assertEqual(context["subject_id"], "cycle-2")

    def test_formats_sse_message_with_json_payload(self) -> None:
        message = _format_sse_message(
            event_id="evt-1",
            event_name="auditflow.package.ready",
            payload={"workspace_id": "ws-1"},
        )

        self.assertIn("id: evt-1", message)
        self.assertIn("event: auditflow.package.ready", message)
        self.assertIn('"workspace_id": "ws-1"', message)
        self.assertEqual(_event_topic("auditflow.package.ready"), "auditflow")

    def test_event_topics_include_domain_specific_aliases(self) -> None:
        context = {
            "topic": "auditflow",
            "workspace_id": "ws-1",
            "subject_type": "audit_cycle",
            "subject_id": "cycle-1",
            "payload": {"cycle_id": "cycle-1", "package_id": "pkg-1"},
        }

        topics = _event_topics(context)

        self.assertIn("auditflow.workspace.ws-1", topics)
        self.assertIn("auditflow.cycle.cycle-1", topics)
        self.assertIn("auditflow.export.pkg-1", topics)
        self.assertTrue(_matches_event_topic(context, "auditflow.cycle.cycle-1"))
        self.assertFalse(_matches_event_topic(context, "workflow"))

    def test_missing_last_event_id_does_not_block_stream_progress(self) -> None:
        pending = [SimpleNamespace(event=SimpleNamespace(event_id="evt-1"))]

        self.assertIsNone(_normalize_resume_after_id(pending, "evt-missing"))
        self.assertEqual(_normalize_resume_after_id(pending, "evt-1"), "evt-1")


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
        self.client = TestClient(create_fastapi_app(_stub_service()))

    def test_health_route_remains_public(self) -> None:
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["status"], "ok")

    def test_viewer_route_requires_authorization_header(self) -> None:
        response = self.client.get(
            "/api/v1/auditflow/cycles",
            params={"workspace_id": "ws-1"},
            headers={"X-Organization-Id": "org-1"},
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "AUTH_REQUIRED")

    def test_viewer_route_requires_organization_context(self) -> None:
        response = self.client.get(
            "/api/v1/auditflow/cycles",
            params={"workspace_id": "ws-1"},
            headers={"Authorization": "Bearer test-token"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "TENANT_CONTEXT_REQUIRED")

    def test_reviewer_route_rejects_viewer_role(self) -> None:
        response = self.client.post(
            "/api/v1/auditflow/cycles",
            headers={
                **_auth_headers(role="viewer"),
                "Idempotency-Key": "cycle-create-1",
            },
            json={
                "workspace_id": "ws-1",
                "cycle_name": "SOC2 2026",
            },
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"]["code"], "AUTH_FORBIDDEN")

    def test_reviewer_route_allows_reviewer_role(self) -> None:
        response = self.client.post(
            "/api/v1/auditflow/cycles",
            headers={
                **_auth_headers(role="reviewer"),
                "Idempotency-Key": "cycle-create-2",
            },
            json={
                "workspace_id": "ws-1",
                "cycle_name": "SOC2 2026",
            },
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["data"]["id"], "cycle-1")

    def test_product_admin_route_accepts_org_admin_alias(self) -> None:
        response = self.client.post(
            "/api/v1/auditflow/workspaces",
            headers=_auth_headers(role="org_admin"),
            json={
                "name": "Acme SOC2",
                "slug": "acme-soc2",
                "default_framework": "SOC2",
            },
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["data"]["id"], "ws-1")


def _auth_headers(*, role: str) -> dict[str, str]:
    return {
        "Authorization": "Bearer test-token",
        "X-Organization-Id": "org-1",
        "X-User-Id": "user-1",
        "X-User-Role": role,
    }


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
    return SimpleNamespace(
        list_workflows=lambda: [],
        get_workflow_state=lambda workflow_run_id: {
            "workflow_run_id": workflow_run_id,
            "workflow_type": "auditflow_cycle",
            "current_state": "reviewing",
            "checkpoint_seq": 1,
            "raw_state": {},
        },
        create_workspace=lambda command: workspace,
        get_workspace=lambda workspace_id: workspace,
        create_cycle=lambda command, idempotency_key=None: cycle,
        list_cycles=lambda workspace_id, status=None: [cycle],
    )


if __name__ == "__main__":
    unittest.main()
