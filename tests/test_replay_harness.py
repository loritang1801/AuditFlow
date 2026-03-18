from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_replay_harness
from auditflow_app.replay_harness import (
    AuditFlowReplayHarness,
    ReplayNodeSummary,
    ReplayWorkflowSummary,
    load_replay_baseline,
    load_replay_report,
)


def _create_repo_tempdir(prefix: str) -> Path:
    temp_root = ROOT / ".tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=temp_root))


class AuditFlowReplayHarnessTests(unittest.TestCase):
    def test_list_demo_scenarios_returns_multi_format_catalog(self) -> None:
        harness = build_replay_harness()

        scenarios = harness.list_demo_scenarios()

        self.assertEqual(
            [scenario.scenario_name for scenario in scenarios],
            [
                "demo_cycle_export",
                "csv_access_review",
                "json_access_review",
                "markdown_access_review",
                "html_access_review",
            ],
        )
        self.assertEqual(
            [scenario.source_format for scenario in scenarios],
            ["text", "csv", "json", "markdown", "html"],
        )

    def test_capture_and_evaluate_demo_scenario_writes_artifacts(self) -> None:
        tmp_dir = _create_repo_tempdir("auditflow-replay-")
        self.addCleanup(shutil.rmtree, tmp_dir, ignore_errors=True)
        database_url = f"sqlite+pysqlite:///{(tmp_dir / 'auditflow.db').resolve().as_posix()}"
        baseline_root = tmp_dir / "baselines"
        report_root = tmp_dir / "reports"

        harness = build_replay_harness(
            database_url=database_url,
            baseline_root=str(baseline_root),
            report_root=str(report_root),
        )
        baseline = harness.capture_demo_baseline(scenario_name="json_access_review")
        loaded = load_replay_baseline(baseline.baseline_artifact_path)
        evaluation = harness.evaluate_demo_scenario(loaded)

        self.assertEqual(loaded.baseline_id, baseline.baseline_id)
        self.assertEqual(loaded.scenario_name, "json_access_review")
        self.assertEqual(loaded.source_format, "json")
        self.assertEqual(evaluation.status, "matched")
        self.assertEqual(evaluation.score, 1.0)
        self.assertEqual(evaluation.mismatch_count, 0)
        self.assertEqual(evaluation.scenario_title, "JSON access review")
        self.assertEqual(
            [workflow.workflow_name for workflow in baseline.workflows],
            ["auditflow_cycle_processing", "auditflow_export_generation"],
        )
        self.assertTrue(Path(baseline.baseline_artifact_path).exists())
        self.assertTrue(Path(evaluation.report_artifact_path).exists())
        self.assertTrue(Path(evaluation.markdown_report_path).exists())

    def test_capture_demo_baselines_can_run_subset(self) -> None:
        tmp_dir = _create_repo_tempdir("auditflow-replay-suite-")
        self.addCleanup(shutil.rmtree, tmp_dir, ignore_errors=True)
        database_url = f"sqlite+pysqlite:///{(tmp_dir / 'auditflow.db').resolve().as_posix()}"

        harness = build_replay_harness(
            database_url=database_url,
            baseline_root=str(tmp_dir / "baselines"),
            report_root=str(tmp_dir / "reports"),
        )

        baselines = harness.capture_demo_baselines(
            scenario_names=["csv_access_review", "html_access_review"]
        )

        self.assertEqual([baseline.scenario_name for baseline in baselines], ["csv_access_review", "html_access_review"])
        self.assertTrue(all(Path(baseline.baseline_artifact_path).exists() for baseline in baselines))

    def test_saved_baseline_and_report_catalog_support_filtering_and_latest_lookup(self) -> None:
        tmp_dir = _create_repo_tempdir("auditflow-replay-catalog-")
        self.addCleanup(shutil.rmtree, tmp_dir, ignore_errors=True)
        database_url = f"sqlite+pysqlite:///{(tmp_dir / 'auditflow.db').resolve().as_posix()}"
        baseline_root = tmp_dir / "baselines"
        report_root = tmp_dir / "reports"

        harness = build_replay_harness(
            database_url=database_url,
            baseline_root=str(baseline_root),
            report_root=str(report_root),
        )
        baseline_one = harness.capture_demo_baseline(scenario_name="csv_access_review")
        report_one = harness.evaluate_demo_scenario(baseline_one)
        baseline_two = harness.capture_demo_baseline(scenario_name="json_access_review")
        report_two = harness.evaluate_demo_scenario(baseline_two)

        baselines = harness.list_saved_baselines()
        csv_baselines = harness.list_saved_baselines(scenario_name="csv_access_review")
        reports = harness.list_saved_reports()
        matched_reports = harness.list_saved_reports(status="matched")
        latest_json = harness.get_latest_baseline(scenario_name="json_access_review")
        loaded_report = load_replay_report(report_two.report_artifact_path)

        self.assertEqual(len(baselines), 2)
        self.assertEqual(csv_baselines[0].scenario_name, "csv_access_review")
        self.assertEqual(len(reports), 2)
        self.assertEqual(len(matched_reports), 2)
        self.assertEqual(latest_json.baseline_id, baseline_two.baseline_id)
        self.assertEqual(loaded_report.report_id, report_two.report_id)
        self.assertEqual(Path(loaded_report.report_artifact_path), Path(report_two.report_artifact_path))
        self.assertEqual(Path(loaded_report.markdown_report_path), Path(report_two.markdown_report_path))

    def test_evaluate_workflow_reports_mismatch_details(self) -> None:
        origin = datetime(2026, 3, 18, 10, 0, tzinfo=UTC)
        baseline_workflow = ReplayWorkflowSummary(
            workflow_name="auditflow_cycle_processing",
            workflow_run_id="baseline-cycle-1",
            workflow_type="auditflow_cycle",
            final_state="human_review",
            checkpoint_seq=2,
            node_summaries=[
                ReplayNodeSummary(
                    checkpoint_seq=1,
                    bundle_id="auditflow.ingestor",
                    bundle_version="2026-03-18",
                    model_profile_id="gpt-5.4",
                    response_schema_ref="schemas/ingestor.json",
                    output_summary="Ingested uploaded evidence.",
                    recorded_at=origin,
                ),
                ReplayNodeSummary(
                    checkpoint_seq=2,
                    bundle_id="auditflow.mapper",
                    bundle_version="2026-03-18",
                    model_profile_id="gpt-5.4",
                    response_schema_ref="schemas/mapper.json",
                    output_summary="Prepared reviewer mappings.",
                    recorded_at=origin + timedelta(seconds=1),
                ),
            ],
        )
        replay_workflow = ReplayWorkflowSummary(
            workflow_name="auditflow_cycle_processing",
            workflow_run_id="replay-cycle-1",
            workflow_type="auditflow_cycle",
            final_state="exported",
            checkpoint_seq=3,
            node_summaries=[
                ReplayNodeSummary(
                    checkpoint_seq=1,
                    bundle_id="auditflow.ingestor",
                    bundle_version="2026-03-18",
                    model_profile_id="gpt-5.4",
                    response_schema_ref="schemas/ingestor.json",
                    output_summary="Ingested uploaded evidence.",
                    recorded_at=origin + timedelta(milliseconds=200),
                ),
                ReplayNodeSummary(
                    checkpoint_seq=2,
                    bundle_id="auditflow.writer",
                    bundle_version="2026-03-19",
                    model_profile_id="gpt-5.4",
                    response_schema_ref="schemas/writer.json",
                    output_summary="Prepared export package.",
                    recorded_at=origin + timedelta(seconds=2),
                ),
            ],
        )

        evaluation = AuditFlowReplayHarness._evaluate_workflow(baseline_workflow, replay_workflow)

        self.assertEqual(evaluation.status, "mismatched")
        self.assertEqual(evaluation.mismatch_count, 5)
        self.assertEqual(evaluation.baseline_final_state, "human_review")
        self.assertEqual(evaluation.replay_final_state, "exported")
        self.assertEqual(evaluation.node_diffs[0].latency_delta_ms, 0)
        self.assertFalse(evaluation.node_diffs[1].matched)
        self.assertIn("bundle mismatch", evaluation.node_diffs[1].mismatch_reasons[0])
        self.assertIn("version mismatch", evaluation.node_diffs[1].mismatch_reasons[1])
        self.assertIn("summary mismatch", evaluation.node_diffs[1].mismatch_reasons[2])


if __name__ == "__main__":
    unittest.main()
