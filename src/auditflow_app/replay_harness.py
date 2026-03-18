from __future__ import annotations

from dataclasses import dataclass
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from .sample_payloads import (
    cycle_create_command,
    export_create_command,
    gap_decision_command,
    mapping_review_command,
    upload_import_command,
    workspace_create_command,
)


class ReplayHarnessModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ReplayNodeSummary(ReplayHarnessModel):
    checkpoint_seq: int
    bundle_id: str
    bundle_version: str
    model_profile_id: str
    response_schema_ref: str
    output_summary: str
    recorded_at: datetime | None = None


class ReplayNodeDiffSummary(ReplayHarnessModel):
    checkpoint_seq: int
    matched: bool
    expected_bundle_id: str
    actual_bundle_id: str | None = None
    expected_bundle_version: str
    actual_bundle_version: str | None = None
    expected_output_summary: str
    actual_output_summary: str | None = None
    baseline_elapsed_ms: int | None = None
    replay_elapsed_ms: int | None = None
    latency_delta_ms: int | None = None
    mismatch_reasons: list[str] = Field(default_factory=list)


class ReplayWorkflowSummary(ReplayHarnessModel):
    workflow_name: Literal["auditflow_cycle_processing", "auditflow_export_generation"]
    workflow_run_id: str
    workflow_type: str
    final_state: str
    checkpoint_seq: int
    node_summaries: list[ReplayNodeSummary] = Field(default_factory=list)


class ReplayWorkflowEvaluation(ReplayHarnessModel):
    workflow_name: Literal["auditflow_cycle_processing", "auditflow_export_generation"]
    baseline_workflow_run_id: str
    replay_workflow_run_id: str
    status: Literal["matched", "mismatched"]
    mismatch_count: int
    baseline_final_state: str
    replay_final_state: str
    baseline_checkpoint_seq: int
    replay_checkpoint_seq: int
    node_diffs: list[ReplayNodeDiffSummary] = Field(default_factory=list)


class ReplayScenarioInfo(ReplayHarnessModel):
    scenario_name: str
    title: str
    description: str
    source_format: Literal["text", "csv", "json", "markdown", "html"]


class ReplayScenarioBaseline(ReplayHarnessModel):
    baseline_id: str
    scenario_name: str
    scenario_title: str | None = None
    scenario_description: str | None = None
    source_format: str | None = None
    captured_at: datetime
    workflows: list[ReplayWorkflowSummary] = Field(default_factory=list)
    baseline_artifact_path: str | None = None


class ReplayScenarioEvaluation(ReplayHarnessModel):
    report_id: str
    baseline_id: str
    scenario_name: str
    scenario_title: str | None = None
    scenario_description: str | None = None
    source_format: str | None = None
    status: Literal["matched", "mismatched"]
    score: float
    mismatch_count: int
    workflow_reports: list[ReplayWorkflowEvaluation] = Field(default_factory=list)
    report_artifact_path: str | None = None
    markdown_report_path: str | None = None
    created_at: datetime


class ReplayScenarioExecution(ReplayHarnessModel):
    scenario_name: str
    scenario_title: str | None = None
    scenario_description: str | None = None
    source_format: str | None = None
    workspace_id: str
    cycle_id: str
    package_id: str
    workflows: list[ReplayWorkflowSummary] = Field(default_factory=list)


class ReplayStoredArtifactSummary(ReplayHarnessModel):
    artifact_path: str
    scenario_name: str
    created_at: datetime
    status: str | None = None
    score: float | None = None


@dataclass(frozen=True, slots=True)
class DemoReplayScenario:
    scenario_name: str
    title: str
    description: str
    source_format: Literal["text", "csv", "json", "markdown", "html"]
    upload_overrides: dict[str, Any]


DEMO_REPLAY_SCENARIOS: tuple[DemoReplayScenario, ...] = (
    DemoReplayScenario(
        scenario_name="demo_cycle_export",
        title="Plain text access review",
        description="Baseline text evidence path from import to export package generation.",
        source_format="text",
        upload_overrides={
            "display_name": "Replay Access Review Text",
            "source_locator": "uploads/access-review.txt",
            "artifact_text": (
                "Quarterly Access Review Export\n\n"
                "Control owner: Security Engineering\n"
                "Review period: 2026-Q1\n"
                "Result: All privileged access assignments were reviewed and approved.\n\n"
                "Reviewer notes:\n"
                "- Production admins remain limited to the platform team.\n"
                "- Two stale contractor accounts were removed before sign-off."
            ),
        },
    ),
    DemoReplayScenario(
        scenario_name="csv_access_review",
        title="CSV access review",
        description="Structured CSV import exercising row-aware normalization and chunking.",
        source_format="csv",
        upload_overrides={
            "display_name": "Replay Access Review CSV",
            "source_locator": "uploads/access-review.csv",
            "artifact_text": (
                "reviewer,system,status\n"
                "Security Engineering,production-admins,approved\n"
                "Security Engineering,break-glass,approved\n"
            ),
        },
    ),
    DemoReplayScenario(
        scenario_name="json_access_review",
        title="JSON access review",
        description="Nested JSON import exercising flattened retrieval-ready evidence fields.",
        source_format="json",
        upload_overrides={
            "display_name": "Replay Access Review JSON",
            "source_locator": "uploads/access-review.json",
            "artifact_text": json.dumps(
                {
                    "review": {
                        "owner": "Security Engineering",
                        "quarter": "2026-Q1",
                    },
                    "controls": [
                        {"code": "CC6.1", "status": "covered"},
                        {"code": "CC6.2", "status": "covered"},
                    ],
                }
            ),
        },
    ),
    DemoReplayScenario(
        scenario_name="markdown_access_review",
        title="Markdown access review",
        description="Sectioned Markdown import covering heading and bullet-preserving normalization.",
        source_format="markdown",
        upload_overrides={
            "display_name": "Replay Access Review Markdown",
            "source_locator": "uploads/access-review.md",
            "artifact_text": (
                "# Access Review\n\n"
                "- Reviewer: Security Engineering\n"
                "- Quarter: 2026-Q1\n\n"
                "## Findings\n\n"
                "1. Two stale contractor accounts were removed.\n"
                "2. Break-glass access remained limited.\n"
            ),
        },
    ),
    DemoReplayScenario(
        scenario_name="html_access_review",
        title="HTML access review",
        description="Portal-style HTML import covering markup stripping and textual chunk output.",
        source_format="html",
        upload_overrides={
            "display_name": "Replay Access Review HTML",
            "source_locator": "uploads/access-review.html",
            "artifact_text": (
                "<html><body><h1>Access Review</h1><p>Reviewer: Security Engineering</p>"
                "<ul><li>Production admins approved</li><li>Break-glass reviewed</li></ul>"
                "<p>All actions completed.</p></body></html>"
            ),
        },
    ),
)


def replay_baseline_root() -> Path:
    root = Path(__file__).resolve().parents[2] / "replay_baselines"
    root.mkdir(parents=True, exist_ok=True)
    return root


def replay_report_root() -> Path:
    root = Path(__file__).resolve().parents[2] / "replay_reports"
    root.mkdir(parents=True, exist_ok=True)
    return root


def load_replay_baseline(path: str | Path) -> ReplayScenarioBaseline:
    baseline = ReplayScenarioBaseline.model_validate_json(Path(path).read_text(encoding="utf-8"))
    return _hydrate_scenario_metadata(
        baseline.model_copy(update={"baseline_artifact_path": str(Path(path))})
    )


def load_replay_report(path: str | Path) -> ReplayScenarioEvaluation:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    report_payload = payload["report"] if isinstance(payload, dict) and "report" in payload else payload
    report = ReplayScenarioEvaluation.model_validate(report_payload)
    markdown_path = None
    candidate_markdown = Path(path).with_suffix(".md")
    if candidate_markdown.exists():
        markdown_path = str(candidate_markdown)
    return _hydrate_scenario_metadata(
        report.model_copy(
            update={
                "report_artifact_path": str(Path(path)),
                "markdown_report_path": markdown_path or report.markdown_report_path,
            }
        )
    )


def _scenario_metadata(scenario_name: str) -> dict[str, str | None]:
    for scenario in DEMO_REPLAY_SCENARIOS:
        if scenario.scenario_name == scenario_name:
            return {
                "scenario_title": scenario.title,
                "scenario_description": scenario.description,
                "source_format": scenario.source_format,
            }
    return {
        "scenario_title": None,
        "scenario_description": None,
        "source_format": None,
    }


def _hydrate_scenario_metadata(model: ReplayScenarioBaseline | ReplayScenarioEvaluation):
    metadata = _scenario_metadata(model.scenario_name)
    return model.model_copy(
        update={
            "scenario_title": model.scenario_title or metadata["scenario_title"],
            "scenario_description": model.scenario_description or metadata["scenario_description"],
            "source_format": model.source_format or metadata["source_format"],
        }
    )


class AuditFlowReplayHarness:
    def __init__(
        self,
        *,
        service_factory: Callable[[], Any],
        baseline_root: str | Path | None = None,
        report_root: str | Path | None = None,
    ) -> None:
        self._service_factory = service_factory
        self._baseline_root = Path(baseline_root) if baseline_root is not None else replay_baseline_root()
        self._report_root = Path(report_root) if report_root is not None else replay_report_root()
        self._baseline_root.mkdir(parents=True, exist_ok=True)
        self._report_root.mkdir(parents=True, exist_ok=True)

    def list_demo_scenarios(self) -> list[ReplayScenarioInfo]:
        return [
            ReplayScenarioInfo(
                scenario_name=scenario.scenario_name,
                title=scenario.title,
                description=scenario.description,
                source_format=scenario.source_format,
            )
            for scenario in DEMO_REPLAY_SCENARIOS
        ]

    def list_saved_baselines(
        self,
        *,
        scenario_name: str | None = None,
        limit: int | None = None,
    ) -> list[ReplayScenarioBaseline]:
        baselines = [
            load_replay_baseline(path)
            for path in sorted(self._baseline_root.glob("*.json"))
        ]
        if scenario_name is not None:
            baselines = [baseline for baseline in baselines if baseline.scenario_name == scenario_name]
        baselines.sort(key=lambda baseline: baseline.captured_at, reverse=True)
        return baselines[:limit] if limit is not None else baselines

    def list_saved_reports(
        self,
        *,
        scenario_name: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[ReplayScenarioEvaluation]:
        reports = [
            load_replay_report(path)
            for path in sorted(self._report_root.glob("*.json"))
        ]
        if scenario_name is not None:
            reports = [report for report in reports if report.scenario_name == scenario_name]
        if status is not None:
            reports = [report for report in reports if report.status == status]
        reports.sort(key=lambda report: report.created_at, reverse=True)
        return reports[:limit] if limit is not None else reports

    def list_saved_artifacts(self) -> dict[str, list[ReplayStoredArtifactSummary]]:
        return {
            "baselines": [
                ReplayStoredArtifactSummary(
                    artifact_path=str(Path(baseline.baseline_artifact_path or "")),
                    scenario_name=baseline.scenario_name,
                    created_at=baseline.captured_at,
                )
                for baseline in self.list_saved_baselines()
            ],
            "reports": [
                ReplayStoredArtifactSummary(
                    artifact_path=str(Path(report.report_artifact_path or "")),
                    scenario_name=report.scenario_name,
                    created_at=report.created_at,
                    status=report.status,
                    score=report.score,
                )
                for report in self.list_saved_reports()
            ],
        }

    def get_latest_baseline(self, *, scenario_name: str | None = None) -> ReplayScenarioBaseline:
        baselines = self.list_saved_baselines(scenario_name=scenario_name, limit=1)
        if not baselines:
            raise KeyError(scenario_name or "latest-baseline")
        return baselines[0]

    def capture_demo_baselines(
        self,
        *,
        scenario_names: list[str] | None = None,
    ) -> list[ReplayScenarioBaseline]:
        return [
            self.capture_demo_baseline(scenario_name=scenario_name)
            for scenario_name in self._resolve_demo_scenario_names(scenario_names)
        ]

    def evaluate_demo_suite(
        self,
        baselines: list[ReplayScenarioBaseline],
    ) -> list[ReplayScenarioEvaluation]:
        return [self.evaluate_demo_scenario(baseline) for baseline in baselines]

    def capture_demo_baseline(self, *, scenario_name: str = "demo_cycle_export") -> ReplayScenarioBaseline:
        scenario = self._lookup_demo_scenario(scenario_name)
        execution = self._run_demo_scenario(scenario_name=scenario_name, run_label="baseline")
        baseline = ReplayScenarioBaseline(
            baseline_id=f"baseline-{uuid4().hex[:10]}",
            scenario_name=scenario_name,
            scenario_title=scenario.title,
            scenario_description=scenario.description,
            source_format=scenario.source_format,
            captured_at=datetime.now(UTC),
            workflows=execution.workflows,
        )
        artifact_path = self._write_baseline_artifact(baseline)
        return baseline.model_copy(update={"baseline_artifact_path": str(artifact_path)})

    def evaluate_demo_scenario(self, baseline: ReplayScenarioBaseline) -> ReplayScenarioEvaluation:
        scenario = self._lookup_demo_scenario(baseline.scenario_name)
        execution = self._run_demo_scenario(scenario_name=baseline.scenario_name, run_label="replay")
        workflow_reports: list[ReplayWorkflowEvaluation] = []
        total_mismatches = 0
        for workflow_name in ("auditflow_cycle_processing", "auditflow_export_generation"):
            baseline_workflow = self._workflow_by_name(baseline.workflows, workflow_name)
            replay_workflow = self._workflow_by_name(execution.workflows, workflow_name)
            report = self._evaluate_workflow(baseline_workflow, replay_workflow)
            workflow_reports.append(report)
            total_mismatches += report.mismatch_count
        max_checks = max(1, len(workflow_reports) * 4)
        evaluation = ReplayScenarioEvaluation(
            report_id=f"report-{uuid4().hex[:10]}",
            baseline_id=baseline.baseline_id,
            scenario_name=baseline.scenario_name,
            scenario_title=baseline.scenario_title or scenario.title,
            scenario_description=baseline.scenario_description or scenario.description,
            source_format=baseline.source_format or scenario.source_format,
            status=("matched" if total_mismatches == 0 else "mismatched"),
            score=max(0.0, 1.0 - (total_mismatches / max_checks)),
            mismatch_count=total_mismatches,
            workflow_reports=workflow_reports,
            created_at=datetime.now(UTC),
        )
        report_paths = self._write_report_artifacts(baseline, execution, evaluation)
        return evaluation.model_copy(
            update={
                "report_artifact_path": str(report_paths["json"]),
                "markdown_report_path": str(report_paths["markdown"]),
            }
        )

    def _run_demo_scenario(
        self,
        *,
        scenario_name: str,
        run_label: str,
    ) -> ReplayScenarioExecution:
        scenario = self._lookup_demo_scenario(scenario_name)
        service = self._service_factory()
        suffix = uuid4().hex[:8]
        workspace = None
        try:
            workspace = service.create_workspace(
                workspace_create_command(
                    workspace_name=f"Replay {scenario_name} {run_label}",
                    slug=f"replay-{scenario_name}-{run_label}-{suffix}",
                )
            )
            cycle = service.create_cycle(
                cycle_create_command(
                    workspace_id=workspace.workspace_id,
                    cycle_name=f"Replay {scenario_name} {run_label} {suffix}",
                )
            )
            cycle_workflow_run_id = f"{scenario_name}-{run_label}-cycle-{suffix}"
            export_workflow_run_id = f"{scenario_name}-{run_label}-export-{suffix}"
            accepted = service.create_upload_import(
                cycle.cycle_id,
                self._scenario_upload_command(
                    scenario,
                    workflow_run_id=cycle_workflow_run_id,
                    run_label=run_label,
                    suffix=suffix,
                ),
            )
            if accepted.accepted_count == 0:
                raise RuntimeError("Expected replay scenario upload import to be accepted")
            service.dispatch_import_jobs()

            review_queue = service.list_review_queue(cycle.cycle_id)
            for item in review_queue.items:
                service.review_mapping(
                    item.mapping_id,
                    mapping_review_command(
                        comment="Replay harness accepted mapping.",
                        expected_snapshot_version=item.snapshot_version,
                    ),
                )

            gaps = service.list_gaps(cycle.cycle_id)
            for gap in gaps:
                if gap.status == "resolved":
                    continue
                service.decide_gap(
                    gap.gap_id,
                    gap_decision_command(
                        decision="resolve_gap",
                        comment="Replay harness resolved gap.",
                        expected_snapshot_version=gap.snapshot_version,
                    ),
                )

            dashboard = service.get_cycle_dashboard(cycle.cycle_id)
            export_package = service.create_export_package(
                cycle.cycle_id,
                export_create_command(
                    workflow_run_id=export_workflow_run_id,
                    snapshot_version=dashboard.cycle.current_snapshot_version,
                ),
            )

            return ReplayScenarioExecution(
                scenario_name=scenario_name,
                scenario_title=scenario.title,
                scenario_description=scenario.description,
                source_format=scenario.source_format,
                workspace_id=workspace.workspace_id,
                cycle_id=cycle.cycle_id,
                package_id=export_package.package_id,
                workflows=[
                    self._capture_workflow_summary(
                        service,
                        workflow_name="auditflow_cycle_processing",
                        workflow_run_id=cycle_workflow_run_id,
                    ),
                    self._capture_workflow_summary(
                        service,
                        workflow_name="auditflow_export_generation",
                        workflow_run_id=export_workflow_run_id,
                    ),
                ],
            )
        finally:
            if hasattr(service, "close"):
                service.close()

    @staticmethod
    def _scenario_upload_command(
        scenario: DemoReplayScenario,
        *,
        workflow_run_id: str,
        run_label: str,
        suffix: str,
    ) -> dict[str, Any]:
        command = upload_import_command(
            workflow_run_id=workflow_run_id,
            artifact_id=f"artifact-{scenario.scenario_name}-{run_label}-{suffix}",
            display_name=scenario.title,
        )
        command.update(scenario.upload_overrides)
        command["workflow_run_id"] = workflow_run_id
        command["artifact_id"] = f"artifact-{scenario.scenario_name}-{run_label}-{suffix}"
        return command

    @staticmethod
    def _resolve_demo_scenario_names(scenario_names: list[str] | None) -> list[str]:
        if scenario_names is None:
            return [scenario.scenario_name for scenario in DEMO_REPLAY_SCENARIOS]
        return scenario_names

    @staticmethod
    def _lookup_demo_scenario(scenario_name: str) -> DemoReplayScenario:
        for scenario in DEMO_REPLAY_SCENARIOS:
            if scenario.scenario_name == scenario_name:
                return scenario
        raise KeyError(f"Unknown replay scenario: {scenario_name}")

    @staticmethod
    def _capture_workflow_summary(service, *, workflow_name: str, workflow_run_id: str) -> ReplayWorkflowSummary:
        state = service.get_workflow_state(workflow_run_id)
        replay_records = service.runtime_stores.replay_store.list_for_run(workflow_run_id)
        return ReplayWorkflowSummary(
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
            workflow_type=state.workflow_type,
            final_state=state.current_state,
            checkpoint_seq=state.checkpoint_seq,
            node_summaries=[
                ReplayNodeSummary(
                    checkpoint_seq=record.checkpoint_seq,
                    bundle_id=record.bundle_id,
                    bundle_version=record.bundle_version,
                    model_profile_id=record.model_profile_id,
                    response_schema_ref=record.response_schema_ref,
                    output_summary=record.output_summary,
                    recorded_at=record.recorded_at,
                )
                for record in replay_records
            ],
        )

    @staticmethod
    def _workflow_by_name(
        workflows: list[ReplayWorkflowSummary],
        workflow_name: str,
    ) -> ReplayWorkflowSummary:
        for workflow in workflows:
            if workflow.workflow_name == workflow_name:
                return workflow
        raise KeyError(workflow_name)

    @classmethod
    def _evaluate_workflow(
        cls,
        baseline_workflow: ReplayWorkflowSummary,
        replay_workflow: ReplayWorkflowSummary,
    ) -> ReplayWorkflowEvaluation:
        mismatch_count = 0
        node_diffs: list[ReplayNodeDiffSummary] = []
        baseline_origin = cls._workflow_origin(baseline_workflow.node_summaries)
        replay_origin = cls._workflow_origin(replay_workflow.node_summaries)
        max_nodes = max(len(baseline_workflow.node_summaries), len(replay_workflow.node_summaries))

        if baseline_workflow.final_state != replay_workflow.final_state:
            mismatch_count += 1
        if baseline_workflow.checkpoint_seq != replay_workflow.checkpoint_seq:
            mismatch_count += 1
        if len(baseline_workflow.node_summaries) != len(replay_workflow.node_summaries):
            mismatch_count += 1

        for index in range(max_nodes):
            baseline_node = (
                baseline_workflow.node_summaries[index]
                if index < len(baseline_workflow.node_summaries)
                else None
            )
            replay_node = (
                replay_workflow.node_summaries[index]
                if index < len(replay_workflow.node_summaries)
                else None
            )
            mismatch_reasons: list[str] = []
            if baseline_node is None:
                mismatch_reasons.append("missing baseline node")
            if replay_node is None:
                mismatch_reasons.append("missing replay node")
            if baseline_node is not None and replay_node is not None:
                if baseline_node.bundle_id != replay_node.bundle_id:
                    mismatch_reasons.append(
                        f"bundle mismatch: expected {baseline_node.bundle_id}, got {replay_node.bundle_id}"
                    )
                if baseline_node.bundle_version != replay_node.bundle_version:
                    mismatch_reasons.append(
                        f"version mismatch: expected {baseline_node.bundle_version}, got {replay_node.bundle_version}"
                    )
                if baseline_node.output_summary != replay_node.output_summary:
                    mismatch_reasons.append(
                        f"summary mismatch: expected '{baseline_node.output_summary}', got '{replay_node.output_summary}'"
                    )
            if mismatch_reasons:
                mismatch_count += len(mismatch_reasons)
            node_diffs.append(
                ReplayNodeDiffSummary(
                    checkpoint_seq=(
                        baseline_node.checkpoint_seq
                        if baseline_node is not None
                        else (replay_node.checkpoint_seq if replay_node is not None else index + 1)
                    ),
                    matched=not mismatch_reasons,
                    expected_bundle_id=baseline_node.bundle_id if baseline_node is not None else "missing",
                    actual_bundle_id=replay_node.bundle_id if replay_node is not None else None,
                    expected_bundle_version=(
                        baseline_node.bundle_version if baseline_node is not None else "missing"
                    ),
                    actual_bundle_version=replay_node.bundle_version if replay_node is not None else None,
                    expected_output_summary=(
                        baseline_node.output_summary if baseline_node is not None else "missing"
                    ),
                    actual_output_summary=replay_node.output_summary if replay_node is not None else None,
                    baseline_elapsed_ms=cls._elapsed_ms(baseline_origin, baseline_node.recorded_at if baseline_node else None),
                    replay_elapsed_ms=cls._elapsed_ms(replay_origin, replay_node.recorded_at if replay_node else None),
                    latency_delta_ms=cls._latency_delta_ms(
                        baseline_origin,
                        baseline_node.recorded_at if baseline_node else None,
                        replay_origin,
                        replay_node.recorded_at if replay_node else None,
                    ),
                    mismatch_reasons=mismatch_reasons,
                )
            )

        return ReplayWorkflowEvaluation(
            workflow_name=baseline_workflow.workflow_name,
            baseline_workflow_run_id=baseline_workflow.workflow_run_id,
            replay_workflow_run_id=replay_workflow.workflow_run_id,
            status=("matched" if mismatch_count == 0 else "mismatched"),
            mismatch_count=mismatch_count,
            baseline_final_state=baseline_workflow.final_state,
            replay_final_state=replay_workflow.final_state,
            baseline_checkpoint_seq=baseline_workflow.checkpoint_seq,
            replay_checkpoint_seq=replay_workflow.checkpoint_seq,
            node_diffs=node_diffs,
        )

    @staticmethod
    def _workflow_origin(nodes: list[ReplayNodeSummary]) -> datetime | None:
        for node in nodes:
            if node.recorded_at is not None:
                return node.recorded_at
        return None

    @staticmethod
    def _elapsed_ms(origin: datetime | None, recorded_at: datetime | None) -> int | None:
        if origin is None or recorded_at is None:
            return None
        return int((recorded_at - origin).total_seconds() * 1000)

    @classmethod
    def _latency_delta_ms(
        cls,
        baseline_origin: datetime | None,
        baseline_recorded_at: datetime | None,
        replay_origin: datetime | None,
        replay_recorded_at: datetime | None,
    ) -> int | None:
        baseline_elapsed = cls._elapsed_ms(baseline_origin, baseline_recorded_at)
        replay_elapsed = cls._elapsed_ms(replay_origin, replay_recorded_at)
        if baseline_elapsed is None or replay_elapsed is None:
            return None
        return replay_elapsed - baseline_elapsed

    def _write_baseline_artifact(self, baseline: ReplayScenarioBaseline) -> Path:
        target = self._baseline_root / f"{baseline.baseline_id}.json"
        target.write_text(baseline.model_dump_json(indent=2), encoding="utf-8")
        return target

    def _write_report_artifacts(
        self,
        baseline: ReplayScenarioBaseline,
        execution: ReplayScenarioExecution,
        evaluation: ReplayScenarioEvaluation,
    ) -> dict[str, Path]:
        json_path = self._report_root / f"{evaluation.report_id}.json"
        markdown_path = self._report_root / f"{evaluation.report_id}.md"
        payload = {
            "baseline": baseline.model_dump(mode="json"),
            "replay": execution.model_dump(mode="json"),
            "report": evaluation.model_dump(mode="json"),
        }
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        markdown_path.write_text(self._to_markdown(evaluation), encoding="utf-8")
        return {
            "json": json_path,
            "markdown": markdown_path,
        }

    @staticmethod
    def _to_markdown(evaluation: ReplayScenarioEvaluation) -> str:
        lines = [
            f"# Replay Report {evaluation.report_id}",
            "",
            f"- Scenario: `{evaluation.scenario_name}`",
            f"- Title: {evaluation.scenario_title or evaluation.scenario_name}",
            f"- Source format: `{evaluation.source_format or 'unknown'}`",
            f"- Description: {evaluation.scenario_description or 'n/a'}",
            f"- Baseline: `{evaluation.baseline_id}`",
            f"- Status: `{evaluation.status}`",
            f"- Score: `{evaluation.score}`",
            f"- Mismatch count: `{evaluation.mismatch_count}`",
            "",
        ]
        for workflow in evaluation.workflow_reports:
            lines.extend(
                [
                    f"## {workflow.workflow_name}",
                    "",
                    f"- Status: `{workflow.status}`",
                    f"- Baseline run: `{workflow.baseline_workflow_run_id}`",
                    f"- Replay run: `{workflow.replay_workflow_run_id}`",
                    f"- Baseline final state: `{workflow.baseline_final_state}`",
                    f"- Replay final state: `{workflow.replay_final_state}`",
                    f"- Mismatch count: `{workflow.mismatch_count}`",
                    "",
                ]
            )
            for diff in workflow.node_diffs:
                lines.extend(
                    [
                        f"### Node {diff.checkpoint_seq}",
                        f"- Matched: `{diff.matched}`",
                        f"- Expected bundle: `{diff.expected_bundle_id}@{diff.expected_bundle_version}`",
                        f"- Actual bundle: `{diff.actual_bundle_id}@{diff.actual_bundle_version}`",
                        f"- Expected summary: {diff.expected_output_summary}",
                        f"- Actual summary: {diff.actual_output_summary}",
                        f"- Mismatch reasons: {', '.join(diff.mismatch_reasons) or 'none'}",
                        "",
                    ]
                )
        return "\n".join(lines)
