from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.routes import map_domain_error, paginate_collection, success_envelope


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


if __name__ == "__main__":
    unittest.main()
