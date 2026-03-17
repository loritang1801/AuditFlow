from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_api_service, build_fastapi_app, build_import_worker, list_supported_workflows
from auditflow_app.sample_payloads import export_generation_request, upload_import_command


class AuditFlowBootstrapTests(unittest.TestCase):
    def test_lists_product_workflows_only(self) -> None:
        self.assertEqual(
            list_supported_workflows(),
            ("auditflow_cycle_processing", "auditflow_export_generation"),
        )

    def test_build_api_service_and_run_export_demo(self) -> None:
        api_service = build_api_service()
        self.addCleanup(api_service.close)
        self.assertEqual(len(api_service.list_workflows()), 2)
        result = api_service.start_workflow(export_generation_request(workflow_run_id="auditflow-test-1"))
        self.assertEqual(result.workflow_name, "auditflow_export_generation")
        self.assertEqual(result.current_state, "exported")

    def test_build_fastapi_app_or_raise_expected_error(self) -> None:
        try:
            app = build_fastapi_app()
        except Exception as exc:
            self.assertEqual(exc.__class__.__name__, "FastAPIUnavailableError")
        else:
            self.assertTrue(hasattr(app, "routes"))

    def test_build_import_worker_and_dispatch_jobs(self) -> None:
        worker = build_import_worker()
        self.addCleanup(worker.app_service.close)

        worker.app_service.create_upload_import("cycle-1", upload_import_command())
        result = worker.dispatch_once()
        imports = worker.app_service.list_imports("cycle-1")

        self.assertEqual(result.dispatched_count, 1)
        self.assertTrue(any(item.ingest_status == "normalized" for item in imports.items))


if __name__ == "__main__":
    unittest.main()
