from __future__ import annotations

import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_import_worker
from auditflow_app.sample_payloads import external_import_command, upload_import_command
from auditflow_app.shared_runtime import load_shared_agent_platform
from auditflow_app.worker import AuditFlowImportWorkerSupervisor


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

    def test_supervisor_retries_transient_failure_and_emits_heartbeat(self) -> None:
        emitted: list[object] = []
        sleeps: list[float] = []
        timestamps = iter(
            (
                datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
                datetime(2026, 3, 18, 10, 0, 5, tzinfo=UTC),
            )
        )

        class FakeWorker:
            def __init__(self) -> None:
                self.attempts = 0
                self.app_service = SimpleNamespace(
                    _worker_now_utc=lambda: datetime(2026, 3, 18, 10, 0, tzinfo=UTC)
                )

            def dispatch_once(self):
                self.attempts += 1
                if self.attempts == 1:
                    raise RuntimeError("transient failure")
                return SimpleNamespace(attempted_count=1, dispatched_count=1, failed_count=0)

        supervisor = AuditFlowImportWorkerSupervisor(
            FakeWorker(),
            sleep_fn=sleeps.append,
            now_fn=lambda: next(timestamps),
        )

        heartbeats = supervisor.run(
            poll_interval_seconds=0,
            max_iterations=2,
            max_consecutive_failures=2,
            failure_backoff_seconds=0.25,
            heartbeat_callback=emitted.append,
        )

        self.assertEqual([heartbeat.status for heartbeat in heartbeats], ["retrying", "active"])
        self.assertEqual(heartbeats[0].consecutive_failures, 1)
        self.assertEqual(heartbeats[1].consecutive_failures, 0)
        self.assertEqual(heartbeats[1].dispatched_count, 1)
        self.assertEqual(sleeps, [0.25])
        self.assertEqual(len(emitted), 2)

    def test_supervisor_raises_after_reaching_failure_threshold(self) -> None:
        emitted: list[object] = []
        sleeps: list[float] = []
        timestamps = iter(
            (
                datetime(2026, 3, 18, 10, 5, tzinfo=UTC),
                datetime(2026, 3, 18, 10, 5, 5, tzinfo=UTC),
            )
        )

        class FailingWorker:
            app_service = SimpleNamespace(
                _worker_now_utc=lambda: datetime(2026, 3, 18, 10, 5, tzinfo=UTC)
            )

            @staticmethod
            def dispatch_once():
                raise RuntimeError("persistent failure")

        supervisor = AuditFlowImportWorkerSupervisor(
            FailingWorker(),
            sleep_fn=sleeps.append,
            now_fn=lambda: next(timestamps),
        )

        with self.assertRaisesRegex(RuntimeError, "persistent failure"):
            supervisor.run(
                poll_interval_seconds=0,
                max_iterations=5,
                max_consecutive_failures=2,
                failure_backoff_seconds=0.5,
                heartbeat_callback=emitted.append,
            )

        self.assertEqual([heartbeat.status for heartbeat in emitted], ["retrying", "failed"])
        self.assertEqual(sleeps, [0.5])


if __name__ == "__main__":
    unittest.main()
