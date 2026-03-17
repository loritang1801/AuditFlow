from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_import_worker
from auditflow_app.sample_payloads import external_import_command, upload_import_command
from auditflow_app.shared_runtime import load_shared_agent_platform


class AuditFlowWorkerTests(unittest.TestCase):
    def test_worker_filters_non_import_events_and_keeps_them_pending(self) -> None:
        worker = build_import_worker()
        self.addCleanup(worker.app_service.close)
        ap = load_shared_agent_platform()

        worker.app_service.create_upload_import("cycle-1", upload_import_command())
        worker.app_service.runtime_stores.outbox_store.append(
            ap.OutboxEvent(
                event_id="unrelated-event-1",
                event_name="auditflow.package.ready",
                workflow_run_id="wf-unrelated-1",
                workflow_type="auditflow_cycle",
                node_name="package_generation",
                aggregate_type="audit_cycle",
                aggregate_id="cycle-1",
                payload={},
                emitted_at=worker.app_service._worker_now_utc(),
            )
        )

        result = worker.dispatch_once()
        pending = worker.app_service.runtime_stores.outbox_store.list_pending()

        self.assertEqual(result.dispatched_count, 1)
        self.assertTrue(any(item.event.event_id == "unrelated-event-1" for item in pending))

    def test_worker_applies_connector_specific_handler_metadata_and_stops_on_idle(self) -> None:
        worker = build_import_worker()
        self.addCleanup(worker.app_service.close)

        worker.app_service.create_external_import(
            "cycle-1",
            external_import_command(provider="confluence", upstream_ids=["PAGE-1"]),
        )
        results = worker.run_polling(poll_interval_seconds=0, max_iterations=5, max_idle_polls=1)
        imports = worker.app_service.list_imports("cycle-1", source_type="confluence")

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].dispatched_count, 1)
        self.assertEqual(results[1].attempted_count, 0)
        self.assertEqual(imports.items[0].metadata["handler_name"], "confluence")
        self.assertEqual(imports.items[0].metadata["provider_object_type"], "page")


if __name__ == "__main__":
    unittest.main()
