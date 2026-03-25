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

from auditflow_app.bootstrap import build_runtime_components
from auditflow_app.repository import ToolAccessAuditRow
from auditflow_app.shared_runtime import load_shared_agent_platform


class AuditFlowToolAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.components = build_runtime_components()
        self.repository = self.components["repository"]
        self.tool_executor = self.components["tool_executor"]
        self.shared_platform = load_shared_agent_platform()

    def tearDown(self) -> None:
        runtime_stores = self.components.get("runtime_stores")
        if runtime_stores is not None and hasattr(runtime_stores, "dispose"):
            runtime_stores.dispose()

    def _tool_call(self, *, tool_name: str, arguments: dict[str, object]):
        return SimpleNamespace(
            tool_call_id=f"call-{tool_name}",
            tool_name=tool_name,
            tool_version="2026-03-16.1",
            workflow_run_id="wf-tool-adapter-1",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments=arguments,
            idempotency_key=f"wf-tool-adapter-1:{tool_name}",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-1",
                role="reviewer",
                session_id="auth-session-1",
            ),
        )

    def test_evidence_search_tool_uses_product_repository_adapter(self) -> None:
        outcome = self.tool_executor.execute(
            self._tool_call(
                tool_name="evidence.search",
                arguments={
                    "workspace_id": "audit-ws-1",
                    "audit_cycle_id": "cycle-1",
                    "query": "access review",
                    "limit": 5,
                },
            )
        )

        self.assertEqual(outcome.trace.adapter_type, "vector_store")
        self.assertGreaterEqual(len(outcome.envelope.normalized_payload["items"]), 1)
        self.assertEqual(
            outcome.envelope.normalized_payload["items"][0]["evidence_chunk_id"],
            "chunk-1",
        )

    def test_database_and_snapshot_tools_use_product_rows(self) -> None:
        mapping_outcome = self.tool_executor.execute(
            self._tool_call(
                tool_name="mapping.read_candidates",
                arguments={
                    "audit_cycle_id": "cycle-1",
                    "evidence_item_id": "evidence-1",
                    "control_id": "control-state-1",
                },
            )
        )
        history_outcome = self.tool_executor.execute(
            self._tool_call(
                tool_name="review_decision.read_history",
                arguments={
                    "audit_cycle_id": "cycle-1",
                    "control_id": "control-state-1",
                    "mapping_id": None,
                },
            )
        )
        snapshot_outcome = self.tool_executor.execute(
            self._tool_call(
                tool_name="export.snapshot_validate",
                arguments={
                    "audit_cycle_id": "cycle-1",
                    "working_snapshot_version": 1,
                },
            )
        )
        snapshot_read_outcome = self.tool_executor.execute(
            self._tool_call(
                tool_name="narrative.snapshot_read",
                arguments={
                    "audit_cycle_id": "cycle-1",
                    "working_snapshot_version": 1,
                },
            )
        )

        self.assertEqual(mapping_outcome.trace.adapter_type, "auditflow_database")
        self.assertEqual(mapping_outcome.envelope.normalized_payload["candidates"][0]["mapping_id"], "mapping-1")
        self.assertEqual(history_outcome.envelope.normalized_payload["decisions"], [])
        self.assertFalse(snapshot_outcome.envelope.normalized_payload["eligible"])
        self.assertEqual(snapshot_read_outcome.trace.adapter_type, "snapshot_reader")
        self.assertEqual(snapshot_read_outcome.envelope.normalized_payload["prior_narrative_ids"], [])
        self.assertIn(
            "open_gaps",
            snapshot_outcome.envelope.normalized_payload["blocker_codes"],
        )

    def test_artifact_and_chunk_tools_read_product_artifact_storage(self) -> None:
        self.repository.upsert_artifact_blob(
            artifact_id="artifact-tool-1",
            artifact_type="upload_raw",
            content_text="Artifact tool raw content",
            metadata_payload={"parser_status": "completed"},
        )
        self.repository.upsert_artifact_blob(
            artifact_id="artifact-tool-1-normalized",
            artifact_type="upload_normalized",
            content_text="Artifact tool normalized content",
            metadata_payload={"parser_status": "completed"},
        )
        self.repository.complete_import_processing(
            cycle_id="cycle-1",
            evidence_source_id="source-1",
            workflow_run_id="wf-tool-import-1",
            title="Artifact Tool Evidence",
            evidence_type="report",
            summary="Artifact tool normalized content",
            artifact_id="artifact-tool-1",
            normalized_artifact_id="artifact-tool-1-normalized",
            source_locator="uploads/artifact-tool-1.txt",
            captured_at=datetime.now(UTC),
            chunk_texts=["Artifact tool normalized content", "Chunk two"],
            metadata_update={"parser_status": "completed"},
        )

        artifact_outcome = self.tool_executor.execute(
            self._tool_call(
                tool_name="artifact.read",
                arguments={"artifact_id": "artifact-tool-1-normalized"},
            )
        )
        chunk_id = artifact_outcome.envelope.normalized_payload["text_ref_ids"][0]
        chunk_outcome = self.tool_executor.execute(
            self._tool_call(
                tool_name="artifact.preview_chunk",
                arguments={
                    "artifact_id": "artifact-tool-1-normalized",
                    "chunk_id": chunk_id,
                },
            )
        )

        self.assertEqual(artifact_outcome.trace.adapter_type, "artifact_store")
        self.assertEqual(artifact_outcome.envelope.normalized_payload["artifact_id"], "artifact-tool-1-normalized")
        self.assertGreaterEqual(len(artifact_outcome.envelope.normalized_payload["text_ref_ids"]), 1)
        self.assertEqual(chunk_outcome.envelope.normalized_payload["chunk_id"], chunk_id)
        self.assertIn("Artifact tool normalized content", chunk_outcome.envelope.normalized_payload["text"])

    def test_tool_calls_record_access_audit_with_actor_context(self) -> None:
        call = self._tool_call(
            tool_name="evidence.search",
            arguments={
                "workspace_id": "audit-ws-1",
                "audit_cycle_id": "cycle-1",
                "query": "access review",
                "limit": 5,
            },
        )

        outcome = self.tool_executor.execute(call)

        with self.repository.session_factory() as session:
            audit_row = session.query(ToolAccessAuditRow).filter_by(tool_call_id=call.tool_call_id).one()

        self.assertEqual(outcome.trace.user_id, "user-reviewer-1")
        self.assertEqual(outcome.trace.role, "reviewer")
        self.assertEqual(outcome.trace.session_id, "auth-session-1")
        self.assertEqual(audit_row.workflow_run_id, "wf-tool-adapter-1")
        self.assertEqual(audit_row.tool_name, "evidence.search")
        self.assertEqual(audit_row.adapter_type, "vector_store")
        self.assertEqual(audit_row.subject_type, "audit_cycle")
        self.assertEqual(audit_row.subject_id, "cycle-1")
        self.assertEqual(audit_row.user_id, "user-reviewer-1")
        self.assertEqual(audit_row.role, "reviewer")
        self.assertEqual(audit_row.session_id, "auth-session-1")
        self.assertEqual(audit_row.execution_status, "success")
        self.assertEqual(audit_row.arguments_payload["query"], "access review")
        self.assertIn("/evidence-search?query=access review", audit_row.source_locator)


if __name__ == "__main__":
    unittest.main()
