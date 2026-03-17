from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_app_service
from auditflow_app.repository import ReviewDecisionRow
from auditflow_app.sample_payloads import (
    cycle_create_command,
    cycle_processing_command,
    external_import_command,
    export_create_command,
    export_generation_command,
    gap_decision_command,
    mapping_review_command,
    upload_import_command,
    workspace_create_command,
)

EXPECTED_SOC2_CONTROL_CODES = ["CC6.1", "CC6.2", "CC7.2", "CC8.1"]


class AuditFlowServiceTests(unittest.TestCase):
    def test_create_workspace_and_cycle(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        workspace = service.create_workspace(workspace_create_command(workspace_name="Customer Audit Workspace"))
        cycle = service.create_cycle(
            cycle_create_command(workspace_id=workspace.workspace_id, cycle_name="SOC2 2027")
        )
        cycles = service.list_cycles(workspace.workspace_id)
        dashboard = service.get_cycle_dashboard(cycle.cycle_id)

        self.assertEqual(workspace.workspace_name, "Customer Audit Workspace")
        self.assertEqual(cycle.workspace_id, workspace.workspace_id)
        self.assertEqual(cycle.cycle_name, "SOC2 2027")
        self.assertEqual(cycle.cycle_status, "draft")
        self.assertEqual(cycle.coverage_status, "not_started")
        self.assertEqual(len(cycles), 1)
        self.assertEqual(dashboard.cycle.cycle_id, cycle.cycle_id)
        self.assertEqual(dashboard.cycle.coverage_status, "not_started")
        self.assertFalse(dashboard.export_ready)
        self.assertEqual(
            [control.control_code for control in dashboard.controls],
            EXPECTED_SOC2_CONTROL_CODES,
        )

    def test_query_workspace_cycle_and_review_queue(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        workspace = service.get_workspace("audit-ws-1")
        cycles = service.list_cycles("audit-ws-1")
        dashboard = service.get_cycle_dashboard("cycle-1")
        review_queue = service.list_review_queue("cycle-1")
        controls = service.list_controls("cycle-1")
        control_detail = service.get_control_detail("control-state-1")
        evidence = service.get_evidence("evidence-1")

        self.assertEqual(workspace.workspace_name, "Acme Security Workspace")
        self.assertEqual(len(cycles), 1)
        self.assertEqual(dashboard.cycle.cycle_id, "cycle-1")
        self.assertEqual(review_queue.total_count, 1)
        self.assertEqual([control.control_code for control in controls], EXPECTED_SOC2_CONTROL_CODES)
        self.assertEqual(control_detail.control_state.control_code, "CC6.1")
        self.assertEqual(evidence.evidence_id, "evidence-1")

    def test_new_cycles_inherit_control_catalog_template_set(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        workspace = service.create_workspace(workspace_create_command(workspace_name="Template Workspace"))
        cycle_one = service.create_cycle(
            cycle_create_command(workspace_id=workspace.workspace_id, cycle_name="SOC2 2027")
        )
        cycle_two = service.create_cycle(
            cycle_create_command(workspace_id=workspace.workspace_id, cycle_name="SOC2 2028")
        )
        controls_one = service.list_controls(cycle_one.cycle_id)
        controls_two = service.list_controls(cycle_two.cycle_id)

        self.assertEqual([control.control_code for control in controls_one], EXPECTED_SOC2_CONTROL_CODES)
        self.assertEqual([control.control_code for control in controls_two], EXPECTED_SOC2_CONTROL_CODES)
        self.assertTrue(
            {control.control_state_id for control in controls_one}.isdisjoint(
                {control.control_state_id for control in controls_two}
            )
        )

    def test_imports_and_gap_decision(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        imports_before = service.list_imports("cycle-1")
        uploaded = service.create_upload_import("cycle-1", upload_import_command())
        external = service.create_external_import("cycle-1", external_import_command())
        imports_pending = service.list_imports("cycle-1")
        dispatch = service.dispatch_import_jobs()
        gap = service.decide_gap("gap-1", gap_decision_command())
        imports_after = service.list_imports("cycle-1")
        control_detail = service.get_control_detail("control-state-1")
        review_queue = service.list_review_queue("cycle-1")

        self.assertEqual(imports_before.total_count, 1)
        self.assertEqual(uploaded.accepted_count, 1)
        self.assertEqual(external.accepted_count, 2)
        self.assertEqual(uploaded.ingest_status, "pending")
        self.assertEqual(external.ingest_status, "pending")
        self.assertEqual(sum(1 for item in imports_pending.items if item.ingest_status == "pending"), 3)
        self.assertEqual(dispatch.dispatched_count, 3)
        self.assertEqual(gap.status, "resolved")
        self.assertEqual(imports_after.total_count, 4)
        self.assertTrue(all(item.ingest_status == "normalized" for item in imports_after.items))
        self.assertEqual(len(control_detail.open_gaps), 0)
        self.assertGreaterEqual(review_queue.total_count, 4)

    def test_duplicate_upload_imports_collapse_before_dispatch(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        first = service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-duplicate-1",
                display_name="Duplicate Access Review Export",
            ),
        )
        duplicate = service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-duplicate-1",
                display_name="Duplicate Access Review Export",
            ),
        )
        imports = service.list_imports("cycle-1")
        dispatch = service.dispatch_import_jobs()

        self.assertEqual(first.accepted_count, 1)
        self.assertEqual(duplicate.accepted_count, 0)
        self.assertEqual(duplicate.evidence_source_ids, [])
        self.assertEqual(imports.total_count, 2)
        self.assertEqual(dispatch.dispatched_count, 1)

    def test_mapping_and_gap_reviews_append_review_decision_audit_rows(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command(comment="Accepting mapped evidence."))
        service.decide_gap("gap-1", gap_decision_command(comment="Gap is resolved."))

        with service.repository.session_factory() as session:
            decision_rows = session.query(ReviewDecisionRow).order_by(ReviewDecisionRow.created_at.asc()).all()

        self.assertEqual(len(decision_rows), 2)
        self.assertEqual(decision_rows[0].mapping_id, "mapping-1")
        self.assertIsNone(decision_rows[0].gap_id)
        self.assertEqual(decision_rows[0].decision, "accept")
        self.assertEqual(decision_rows[0].from_status, "proposed")
        self.assertEqual(decision_rows[0].to_status, "accepted")
        self.assertEqual(decision_rows[1].gap_id, "gap-1")
        self.assertIsNone(decision_rows[1].mapping_id)
        self.assertEqual(decision_rows[1].decision, "resolve_gap")
        self.assertEqual(decision_rows[1].from_status, "acknowledged")
        self.assertEqual(decision_rows[1].to_status, "resolved")

    def test_process_cycle_and_load_state(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        result = service.process_cycle(cycle_processing_command(workflow_run_id="auditflow-service-cycle-1"))
        state = service.get_workflow_state("auditflow-service-cycle-1")

        self.assertEqual(result.current_state, "human_review")
        self.assertEqual(state.current_state, "human_review")
        self.assertEqual(state.workflow_type, "auditflow_cycle")

    def test_review_mapping_and_generate_export(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        review_result = service.review_mapping("mapping-1", mapping_review_command())
        review_queue = service.list_review_queue("cycle-1")
        result = service.generate_export(export_generation_command(workflow_run_id="auditflow-service-export-1"))
        dashboard = service.get_cycle_dashboard("cycle-1")
        export_package = service.get_export_package(dashboard.latest_export_package.package_id)
        narratives = service.list_narratives("cycle-1")

        self.assertEqual(review_result.mapping_status, "accepted")
        self.assertEqual(review_queue.total_count, 0)
        self.assertEqual(result.current_state, "exported")
        self.assertEqual(export_package.status, "ready")
        self.assertGreaterEqual(len(narratives), 1)

    def test_create_export_package_returns_latest_package(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command())
        package = service.create_export_package("cycle-1", export_create_command(workflow_run_id="auditflow-service-export-2"))

        self.assertEqual(package.cycle_id, "cycle-1")
        self.assertEqual(package.snapshot_version, 3)

    def test_sqlalchemy_repository_persists_export_across_service_instances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            database_url = f"sqlite+pysqlite:///{Path(tmp_dir) / 'auditflow.db'}"

            service_one = build_app_service(database_url=database_url)
            service_two = None
            try:
                result = service_one.generate_export(
                    export_generation_command(workflow_run_id="auditflow-persist-export-1")
                )
                service_one.close()

                service_two = build_app_service(database_url=database_url)
                dashboard = service_two.get_cycle_dashboard("cycle-1")

                self.assertEqual(result.current_state, "exported")
                self.assertIsNotNone(dashboard.latest_export_package)
                self.assertEqual(dashboard.latest_export_package.workflow_run_id, "auditflow-persist-export-1")
            finally:
                service_one.close()
                if service_two is not None:
                    service_two.close()
