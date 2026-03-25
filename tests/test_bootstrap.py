from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import (
    build_api_service,
    build_runtime_components,
    build_fastapi_app,
    build_import_worker,
    build_import_worker_supervisor,
    build_replay_harness,
    list_supported_workflows,
)
from auditflow_app.replay_harness import AuditFlowReplayHarness
from auditflow_app.sample_payloads import cycle_processing_request, export_generation_request, upload_import_command


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

    def test_build_runtime_components_uses_product_gateway_with_dynamic_tool_calls(self) -> None:
        components = build_runtime_components()
        runtime_stores = components["runtime_stores"]
        if runtime_stores is not None and hasattr(runtime_stores, "dispose"):
            self.addCleanup(runtime_stores.dispose)

        registry = components["workflow_registry"]
        definition = registry.get("auditflow_cycle_processing")
        state = definition.initial_state_builder(
            "auditflow-runtime-test-1",
            cycle_processing_request(workflow_run_id="auditflow-runtime-test-1")["input_payload"],
            {},
        )
        result = components["execution_service"].run_workflow(
            workflow_run_id="auditflow-runtime-test-1",
            workflow_type=definition.workflow_type,
            initial_state=state,
            steps=definition.steps[:1],
            source_builders=definition.source_builders,
        )

        self.assertNotEqual(components["model_gateway"].__class__.__name__, "StaticModelGateway")
        self.assertEqual(result.final_state["current_state"], "mapping")
        self.assertGreaterEqual(len(result.traces[0].tool_traces), 1)
        self.assertEqual(result.traces[0].tool_traces[0].tool_name, "artifact.read")

    def test_dynamic_tool_calls_include_user_auth_context(self) -> None:
        components = build_runtime_components()
        runtime_stores = components["runtime_stores"]
        if runtime_stores is not None and hasattr(runtime_stores, "dispose"):
            self.addCleanup(runtime_stores.dispose)

        captured_context = {}
        tool_executor = components["tool_executor"]
        original_adapter = tool_executor._adapters["artifact_store"]

        class _CapturingAdapter:
            def execute(self, *, tool, call, arguments):
                captured_context["organization_id"] = call.authorization_context.organization_id
                captured_context["workspace_id"] = call.authorization_context.workspace_id
                captured_context["user_id"] = call.authorization_context.user_id
                captured_context["role"] = call.authorization_context.role
                captured_context["session_id"] = call.authorization_context.session_id
                return original_adapter.execute(tool=tool, call=call, arguments=arguments)

        tool_executor.register_adapter("artifact_store", _CapturingAdapter())
        registry = components["workflow_registry"]
        definition = registry.get("auditflow_cycle_processing")
        state = definition.initial_state_builder(
            "auditflow-runtime-auth-test-1",
            cycle_processing_request(workflow_run_id="auditflow-runtime-auth-test-1")["input_payload"],
            {
                "user_id": "user-admin-1",
                "role": "product_admin",
                "session_id": "auth-session-1",
            },
        )
        result = components["execution_service"].run_workflow(
            workflow_run_id="auditflow-runtime-auth-test-1",
            workflow_type=definition.workflow_type,
            initial_state=state,
            steps=definition.steps[:1],
            source_builders=definition.source_builders,
        )

        self.assertEqual(result.final_state["current_state"], "mapping")
        self.assertEqual(result.traces[0].tool_traces[0].user_id, "user-admin-1")
        self.assertEqual(result.traces[0].tool_traces[0].role, "product_admin")
        self.assertEqual(result.traces[0].tool_traces[0].session_id, "auth-session-1")
        self.assertEqual(captured_context["organization_id"], "org-1")
        self.assertEqual(captured_context["workspace_id"], "audit-ws-1")
        self.assertEqual(captured_context["user_id"], "user-admin-1")
        self.assertEqual(captured_context["role"], "product_admin")
        self.assertEqual(captured_context["session_id"], "auth-session-1")

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

    def test_build_import_worker_supervisor_and_emit_idle_heartbeat(self) -> None:
        supervisor = build_import_worker_supervisor()
        self.addCleanup(supervisor.worker.app_service.close)

        heartbeats = supervisor.run(
            poll_interval_seconds=0,
            max_iterations=1,
            max_idle_polls=1,
            heartbeat_every_iterations=1,
        )

        self.assertEqual(len(heartbeats), 1)
        self.assertEqual(heartbeats[0].status, "idle")

    def test_build_replay_harness(self) -> None:
        harness = build_replay_harness()

        self.assertIsInstance(harness, AuditFlowReplayHarness)


if __name__ == "__main__":
    unittest.main()
