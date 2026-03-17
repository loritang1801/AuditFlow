from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.routes import map_domain_error


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


if __name__ == "__main__":
    unittest.main()
