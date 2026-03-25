from __future__ import annotations

import base64
import io
import json
import shutil
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
import zipfile

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_app_service
from auditflow_app.repository import (
    ArtifactBlobRow,
    AuditCycleRow,
    CycleSnapshotRow,
    EmbeddingChunkRow,
    EvidenceChunkRow,
    EvidenceRow,
    ExportPackageRow,
    GapRow,
    MappingRow,
    MemoryRecordRow,
    ReviewDecisionRow,
    SemanticVectorRow,
)
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


def _png_with_text(text: str) -> bytes:
    text_bytes = text.encode("utf-8")
    text_chunk = len(b"Comment\x00" + text_bytes).to_bytes(4, byteorder="big")
    return (
        b"\x89PNG\r\n\x1a\n"
        + text_chunk
        + b"tEXt"
        + b"Comment\x00"
        + text_bytes
        + b"\x00\x00\x00\x00"
        + b"\x00\x00\x00\x00IEND\x00\x00\x00\x00"
    )


def _docx_with_paragraphs(paragraphs: list[str]) -> bytes:
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:body>"
        + "".join(
            f"<w:p><w:r><w:t>{paragraph}</w:t></w:r></w:p>"
            for paragraph in paragraphs
        )
        + "</w:body></w:document>"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("word/document.xml", document_xml)
    return buffer.getvalue()


def _xlsx_with_rows(rows: list[list[str]]) -> bytes:
    shared_strings = [value for row in rows for value in row]
    shared_strings_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        + "".join(f"<si><t>{value}</t></si>" for value in shared_strings)
        + "</sst>"
    )
    shared_index = 0
    sheet_rows: list[str] = []
    for row_number, row in enumerate(rows, start=1):
        cell_entries: list[str] = []
        for column_index, value in enumerate(row, start=1):
            column_letter = chr(ord("A") + column_index - 1)
            cell_entries.append(
                f'<c r="{column_letter}{row_number}" t="s"><v>{shared_index}</v></c>'
            )
            shared_index += 1
        sheet_rows.append(f'<row r="{row_number}">{"".join(cell_entries)}</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{''.join(sheet_rows)}</sheetData>"
        "</worksheet>"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("xl/sharedStrings.xml", shared_strings_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()


def _zip_with_entries(entries: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        for entry_name, content in entries.items():
            archive.writestr(entry_name, content)
    return buffer.getvalue()


def _create_repo_tempdir(prefix: str) -> Path:
    temp_root = ROOT / ".tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=temp_root))


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
        self.assertEqual(dashboard.tool_access_summary.total_count, 0)
        self.assertEqual(
            [control.control_code for control in dashboard.controls],
            EXPECTED_SOC2_CONTROL_CODES,
        )

    def test_workspace_and_cycle_contract_fields_serialize_with_aliases(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        workspace = service.create_workspace(
            {
                "name": "Alias Contract Workspace",
                "slug": "alias-contract-workspace",
                "default_framework": "SOC2",
                "default_owner_user_id": "owner-contract-1",
                "settings": {"freshness_days_default": 45},
            }
        )
        cycle = service.create_cycle(
            {
                "workspace_id": workspace.workspace_id,
                "cycle_name": "SOC2 2029",
                "audit_period_start": "2029-01-01",
                "audit_period_end": "2029-12-31",
                "owner_user_id": "owner-contract-2",
            }
        )

        workspace_payload = workspace.model_dump(by_alias=True)
        cycle_payload = cycle.model_dump(by_alias=True)

        self.assertEqual(workspace_payload["id"], workspace.workspace_id)
        self.assertEqual(workspace_payload["name"], "Alias Contract Workspace")
        self.assertEqual(workspace_payload["slug"], "alias-contract-workspace")
        self.assertEqual(workspace_payload["default_framework"], "SOC2")
        self.assertEqual(workspace_payload["default_owner_user_id"], "owner-contract-1")
        self.assertIn("created_at", workspace_payload)
        self.assertEqual(cycle_payload["id"], cycle.cycle_id)
        self.assertEqual(cycle_payload["status"], "draft")
        self.assertEqual(cycle_payload["framework"], "SOC2")
        self.assertEqual(cycle_payload["audit_period_start"].isoformat(), "2029-01-01")
        self.assertEqual(cycle_payload["audit_period_end"].isoformat(), "2029-12-31")
        self.assertEqual(cycle_payload["owner_user_id"], "owner-contract-2")
        self.assertEqual(cycle_payload["current_snapshot_version"], 0)

    def test_create_workspace_rejects_duplicate_slug(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.create_workspace(
            {
                "name": "Duplicate Slug Workspace",
                "slug": "duplicate-audit-workspace",
            }
        )

        with self.assertRaisesRegex(ValueError, "WORKSPACE_SLUG_ALREADY_EXISTS"):
            service.create_workspace(
                {
                    "name": "Another Duplicate Slug Workspace",
                    "slug": "duplicate-audit-workspace",
                }
            )

    def test_workspace_slug_is_scoped_per_organization(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        org_one = service.create_workspace(
            workspace_create_command(
                workspace_name="Tenant One Workspace",
                slug="shared-slug",
            ),
            organization_id="org-1",
        )
        org_two = service.create_workspace(
            workspace_create_command(
                workspace_name="Tenant Two Workspace",
                slug="shared-slug",
            ),
            organization_id="org-2",
        )

        self.assertNotEqual(org_one.workspace_id, org_two.workspace_id)

    def test_create_cycle_is_idempotent_for_repeated_key(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        first = service.create_cycle(
            cycle_create_command(workspace_id="audit-ws-1", cycle_name="SOC2 2030"),
            idempotency_key="cycle-create-1",
        )
        second = service.create_cycle(
            cycle_create_command(workspace_id="audit-ws-1", cycle_name="SOC2 2030"),
            idempotency_key="cycle-create-1",
        )
        cycles = [item for item in service.list_cycles("audit-ws-1") if item.cycle_name == "SOC2 2030"]

        self.assertEqual(first.cycle_id, second.cycle_id)
        self.assertEqual(len(cycles), 1)

    def test_create_cycle_rejects_idempotency_conflict(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.create_cycle(
            cycle_create_command(workspace_id="audit-ws-1", cycle_name="SOC2 2031"),
            idempotency_key="cycle-create-conflict",
        )

        with self.assertRaisesRegex(ValueError, "IDEMPOTENCY_CONFLICT"):
            service.create_cycle(
                cycle_create_command(workspace_id="audit-ws-1", cycle_name="SOC2 2032"),
                idempotency_key="cycle-create-conflict",
            )

    def test_query_workspace_cycle_and_review_queue(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        workspace = service.get_workspace("audit-ws-1")
        cycles = service.list_cycles("audit-ws-1")
        dashboard = service.get_cycle_dashboard("cycle-1")
        gaps = service.list_gaps("cycle-1")
        mappings = service.list_mappings("cycle-1")
        review_queue = service.list_review_queue("cycle-1")
        controls = service.list_controls("cycle-1")
        control_detail = service.get_control_detail("control-state-1")
        evidence = service.get_evidence("evidence-1")

        self.assertEqual(workspace.workspace_name, "Acme Security Workspace")
        self.assertEqual(len(cycles), 1)
        self.assertEqual(dashboard.cycle.cycle_id, "cycle-1")
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].gap_id, "gap-1")
        self.assertEqual(mappings.total_count, 1)
        self.assertEqual(mappings.items[0].mapping_id, "mapping-1")
        self.assertEqual(review_queue.total_count, 1)
        self.assertEqual(review_queue.items[0].tool_access_summary.total_count, 0)
        self.assertEqual([control.control_code for control in controls], EXPECTED_SOC2_CONTROL_CODES)
        self.assertEqual(control_detail.control_state.control_code, "CC6.1")
        self.assertEqual(control_detail.tool_access_summary.total_count, 0)
        self.assertEqual(evidence.evidence_id, "evidence-1")

    def test_tenant_scoping_rejects_cross_organization_reads(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        with self.assertRaises(KeyError):
            service.get_workspace("audit-ws-1", organization_id="org-2")

        with self.assertRaises(KeyError):
            service.get_cycle_dashboard("cycle-1", organization_id="org-2")

        with self.assertRaises(KeyError):
            service.search_evidence("cycle-1", query="access review", organization_id="org-2")

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

    def test_list_controls_supports_coverage_status_and_search_filters(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        pending = service.list_controls("cycle-1", coverage_status="pending_review")
        needs_attention = service.list_controls("cycle-1", coverage_status="needs_attention")
        searched = service.list_controls("cycle-1", search="access")

        self.assertEqual([item.control_code for item in pending], ["CC6.1"])
        self.assertEqual(needs_attention, [])
        self.assertEqual([item.control_code for item in searched], ["CC6.1"])

    def test_list_cycles_supports_status_filter(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        draft_cycle = service.create_cycle(
            cycle_create_command(workspace_id="audit-ws-1", cycle_name="SOC2 2028")
        )

        reviewing = service.list_cycles("audit-ws-1", status="reviewing")
        draft = service.list_cycles("audit-ws-1", status="draft")

        self.assertEqual([item.cycle_id for item in reviewing], ["cycle-1"])
        self.assertEqual([item.cycle_id for item in draft], [draft_cycle.cycle_id])

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

    def test_list_gaps_supports_status_and_severity_filters(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        open_gaps = service.list_gaps("cycle-1", status="acknowledged")
        high_gaps = service.list_gaps("cycle-1", severity="high")

        self.assertEqual(len(open_gaps), 1)
        self.assertEqual(open_gaps[0].gap_id, "gap-1")
        self.assertEqual(high_gaps, [])

    def test_list_mappings_supports_cycle_and_status_filters(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        proposed = service.list_mappings("cycle-1", mapping_status="proposed")
        accepted = service.list_mappings("cycle-1", mapping_status="accepted")
        by_control = service.list_mappings("cycle-1", control_state_id="control-state-1")

        self.assertEqual(proposed.total_count, 1)
        self.assertEqual(proposed.items[0].mapping_id, "mapping-1")
        self.assertEqual(accepted.total_count, 0)
        self.assertEqual(by_control.total_count, 1)

    def test_list_review_queue_supports_control_filter(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        matching = service.list_review_queue("cycle-1", control_state_id="control-state-1")
        missing = service.list_review_queue("cycle-1", control_state_id="control-state-2")

        self.assertEqual(matching.total_count, 1)
        self.assertEqual(matching.items[0].mapping_id, "mapping-1")
        self.assertEqual(missing.total_count, 0)

    def test_list_review_queue_supports_severity_filter_and_sort_modes(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        with service.repository.session_factory.begin() as session:
            session.add(
                MappingRow(
                    mapping_id="mapping-ranked",
                    cycle_id="cycle-1",
                    control_state_id="control-state-1",
                    control_code="CC6.1",
                    mapping_status="proposed",
                    evidence_item_id="evidence-1",
                    rationale_summary="Higher-ranked evidence package.",
                    citation_refs=[
                        {"kind": "evidence_chunk", "id": "chunk-1"},
                        {"kind": "evidence_chunk", "id": "chunk-2"},
                    ],
                    reviewer_locked=False,
                    updated_at=datetime(2026, 3, 15, 9, 0),
                )
            )

        medium = service.list_review_queue("cycle-1", severity="medium")
        high = service.list_review_queue("cycle-1", severity="high")
        recent = service.list_review_queue("cycle-1", sort="recent")
        ranking = service.list_review_queue("cycle-1", sort="ranking")

        self.assertEqual(medium.total_count, 2)
        self.assertEqual(high.total_count, 0)
        self.assertEqual(recent.items[0].mapping_id, "mapping-1")
        self.assertEqual(ranking.items[0].mapping_id, "mapping-ranked")

    def test_review_queue_claims_surface_status_and_filters(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        claimed = service.claim_mapping(
            "mapping-1",
            {"lease_seconds": 600},
            reviewer_id="user-reviewer-1",
            organization_id="org-1",
        )
        mine = service.list_review_queue(
            "cycle-1",
            claim_state="claimed_by_me",
            organization_id="org-1",
            viewer_user_id="user-reviewer-1",
        )
        others = service.list_review_queue(
            "cycle-1",
            claim_state="claimed_by_other",
            organization_id="org-1",
            viewer_user_id="user-reviewer-2",
        )

        self.assertEqual(claimed.claim_status, "claimed_by_me")
        self.assertEqual(mine.total_count, 1)
        self.assertEqual(mine.items[0].claim_status, "claimed_by_me")
        self.assertEqual(others.total_count, 1)
        self.assertEqual(others.items[0].claim_status, "claimed_by_other")

    def test_claimed_mapping_rejects_other_reviewer_actions_until_release(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.claim_mapping(
            "mapping-1",
            {"lease_seconds": 600},
            reviewer_id="user-reviewer-1",
            organization_id="org-1",
        )

        with self.assertRaisesRegex(ValueError, "REVIEW_CLAIM_CONFLICT"):
            service.review_mapping(
                "mapping-1",
                mapping_review_command(),
                reviewer_id="user-reviewer-2",
                organization_id="org-1",
            )

        released = service.release_mapping_claim(
            "mapping-1",
            {},
            reviewer_id="user-reviewer-1",
            organization_id="org-1",
        )
        reviewed = service.review_mapping(
            "mapping-1",
            mapping_review_command(),
            reviewer_id="user-reviewer-2",
            organization_id="org-1",
        )

        self.assertEqual(released.claim_status, "unclaimed")
        self.assertEqual(reviewed.mapping_status, "accepted")

    def test_list_review_queue_rejects_unknown_sort(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        with self.assertRaisesRegex(ValueError, "INVALID_REVIEW_QUEUE_SORT"):
            service.list_review_queue("cycle-1", sort="priority")

    def test_list_review_queue_rejects_unknown_claim_state(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        with self.assertRaisesRegex(ValueError, "INVALID_REVIEW_QUEUE_CLAIM_STATE"):
            service.list_review_queue("cycle-1", claim_state="mine")

    def test_gap_transitions_enforce_stricter_terminal_policy(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        with self.assertRaisesRegex(ValueError, "GAP_STATUS_CONFLICT"):
            service.decide_gap("gap-1", gap_decision_command(decision="acknowledge"))

        with self.assertRaisesRegex(ValueError, "GAP_STATUS_CONFLICT"):
            service.decide_gap("gap-1", gap_decision_command(decision="reopen_gap"))

    def test_cycle_contract_timestamps_advance_after_processing_and_review(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.process_cycle(
            cycle_processing_command(workflow_run_id="auditflow-contract-cycle")
        )
        cycle_after_processing = service.get_cycle_dashboard("cycle-1").cycle
        service.review_mapping("mapping-1", mapping_review_command())
        cycle_after_review = service.get_cycle_dashboard("cycle-1").cycle

        self.assertEqual(cycle_after_processing.current_snapshot_version, 2)
        self.assertIsNotNone(cycle_after_processing.last_mapped_at)
        self.assertIsNotNone(cycle_after_review.last_reviewed_at)
        self.assertEqual(cycle_after_review.current_snapshot_version, 3)

        resolved = service.decide_gap("gap-1", gap_decision_command(decision="resolve_gap"))
        reopened = service.decide_gap("gap-1", gap_decision_command(decision="reopen_gap"))
        cycle_after_reopen = service.get_cycle_dashboard("cycle-1").cycle

        self.assertEqual(resolved.status, "resolved")
        self.assertEqual(reopened.status, "open")
        self.assertEqual(cycle_after_reopen.current_snapshot_version, 5)

    def test_review_and_gap_decisions_rebase_live_rows_to_current_snapshot(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command())

        with service.repository.session_factory() as session:
            cycle_after_review = session.get(AuditCycleRow, "cycle-1")
            mapping_after_review = session.get(MappingRow, "mapping-1")
            gap_after_review = session.get(GapRow, "gap-1")

        self.assertIsNotNone(cycle_after_review)
        self.assertIsNotNone(mapping_after_review)
        self.assertIsNotNone(gap_after_review)
        self.assertEqual(cycle_after_review.current_snapshot_version, 2)
        self.assertEqual(mapping_after_review.snapshot_version, 2)
        self.assertEqual(gap_after_review.snapshot_version, 2)

        service.decide_gap("gap-1", gap_decision_command())

        with service.repository.session_factory() as session:
            cycle_after_gap = session.get(AuditCycleRow, "cycle-1")
            mapping_after_gap = session.get(MappingRow, "mapping-1")
            gap_after_gap = session.get(GapRow, "gap-1")

        self.assertIsNotNone(cycle_after_gap)
        self.assertIsNotNone(mapping_after_gap)
        self.assertIsNotNone(gap_after_gap)
        self.assertEqual(cycle_after_gap.current_snapshot_version, 3)
        self.assertEqual(mapping_after_gap.snapshot_version, 3)
        self.assertEqual(gap_after_gap.snapshot_version, 3)

    def test_review_mapping_is_idempotent_for_repeated_key(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        first = service.review_mapping(
            "mapping-1",
            mapping_review_command(),
            idempotency_key="mapping-review-1",
        )
        second = service.review_mapping(
            "mapping-1",
            mapping_review_command(),
            idempotency_key="mapping-review-1",
        )

        self.assertEqual(first.mapping_status, second.mapping_status)
        self.assertEqual(first.mapping_id, second.mapping_id)

    def test_review_decisions_record_authenticated_reviewer_id(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping(
            "mapping-1",
            mapping_review_command(),
            reviewer_id="user-reviewer-1",
            organization_id="org-1",
        )

        with service.repository.session_factory() as session:
            decision_row = session.scalars(
                select(ReviewDecisionRow)
                .where(ReviewDecisionRow.mapping_id == "mapping-1")
                .order_by(ReviewDecisionRow.created_at.desc())
            ).first()

        self.assertIsNotNone(decision_row)
        self.assertEqual(decision_row.reviewer_id, "user-reviewer-1")

    def test_review_mapping_rejects_stale_snapshot_after_cycle_advances(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        with service.repository.session_factory.begin() as session:
            cycle_row = session.get(AuditCycleRow, "cycle-1")
            self.assertIsNotNone(cycle_row)
            cycle_row.current_snapshot_version = 2
            cycle_row.updated_at = service.repository._utcnow_naive()

        with self.assertRaisesRegex(ValueError, "CONFLICT_STALE_RESOURCE"):
            service.review_mapping(
                "mapping-1",
                mapping_review_command(expected_snapshot_version=1),
            )

    def test_gap_decision_rejects_expected_snapshot_version_conflict(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        with self.assertRaisesRegex(ValueError, "CONFLICT_STALE_RESOURCE"):
            service.decide_gap(
                "gap-1",
                gap_decision_command(
                    decision="resolve_gap",
                    expected_snapshot_version=2,
                ),
            )

    def test_gap_decision_rejects_idempotency_conflict(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.decide_gap(
            "gap-1",
            gap_decision_command(decision="resolve_gap"),
            idempotency_key="gap-decision-conflict",
        )

        with self.assertRaisesRegex(ValueError, "IDEMPOTENCY_CONFLICT"):
            service.decide_gap(
                "gap-1",
                gap_decision_command(decision="acknowledge"),
                idempotency_key="gap-decision-conflict",
            )

    def test_snapshot_ledger_preserves_historical_refs_after_snapshot_advances(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        initial_snapshot = service.repository.read_snapshot_refs(
            "cycle-1",
            working_snapshot_version=1,
        )
        service.review_mapping("mapping-1", mapping_review_command())
        after_review_snapshot = service.repository.read_snapshot_refs(
            "cycle-1",
            working_snapshot_version=2,
        )
        service.decide_gap("gap-1", gap_decision_command())
        after_gap_snapshot = service.repository.read_snapshot_refs(
            "cycle-1",
            working_snapshot_version=3,
        )

        self.assertEqual(initial_snapshot["accepted_mapping_ids"], [])
        self.assertEqual(initial_snapshot["open_gap_ids"], ["gap-1"])
        self.assertEqual(after_review_snapshot["accepted_mapping_ids"], ["mapping-1"])
        self.assertEqual(after_review_snapshot["open_gap_ids"], ["gap-1"])
        self.assertEqual(after_gap_snapshot["accepted_mapping_ids"], ["mapping-1"])
        self.assertEqual(after_gap_snapshot["open_gap_ids"], [])

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

    def test_upload_import_is_idempotent_for_repeated_key(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        first = service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-idempotent",
                display_name="Idempotent Access Review Export",
            ),
            idempotency_key="upload-import-1",
        )
        second = service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-idempotent",
                display_name="Idempotent Access Review Export",
            ),
            idempotency_key="upload-import-1",
        )
        imports = service.list_imports("cycle-1")

        matching = [item for item in imports.items if item.artifact_id == "artifact-upload-idempotent"]
        self.assertEqual(first.workflow_run_id, second.workflow_run_id)
        self.assertEqual(first.evidence_source_ids, second.evidence_source_ids)
        self.assertEqual(len(matching), 1)

    def test_upload_import_emits_accept_and_dispatch_outbox_events(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        accepted = service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-events-1",
                display_name="Outbox Event Export",
            ),
        )

        pending = service.runtime_stores.outbox_store.list_pending()
        matching_events = [
            item.event
            for item in pending
            if item.event.workflow_run_id == accepted.workflow_run_id
        ]
        event_names = [event.event_name for event in matching_events]

        self.assertIn("auditflow.import.accepted", event_names)
        self.assertIn("auditflow.import.requested", event_names)

    def test_import_processing_persists_artifact_backed_evidence_and_chunks(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        accepted = service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-rich-1",
                display_name="Privileged Access Review",
                artifact_text=(
                    "Privileged Access Review\n\n"
                    "Owner: Security Engineering\n"
                    "Review window: 2026-Q1\n"
                    "All production administrators were reviewed.\n\n"
                    "Findings:\n"
                    "- Two stale contractor accounts were removed.\n"
                    "- Break-glass access remained limited to on-call leads."
                ),
            ),
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-rich-1")
            ).first()
            self.assertIsNotNone(evidence_row)
            artifact_row = session.get(ArtifactBlobRow, "artifact-upload-rich-1")
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-rich-1-normalized")
            chunk_rows = session.scalars(
                select(EvidenceChunkRow)
                .where(EvidenceChunkRow.evidence_id == evidence_row.evidence_id)
                .order_by(EvidenceChunkRow.chunk_index.asc())
            ).all()

        self.assertEqual(accepted.accepted_count, 1)
        self.assertIsNotNone(artifact_row)
        self.assertIsNotNone(normalized_row)
        self.assertGreaterEqual(len(chunk_rows), 2)
        self.assertIn("Privileged Access Review", artifact_row.content_text)
        self.assertEqual(evidence_row.normalized_artifact_id, "artifact-upload-rich-1-normalized")

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertEqual(evidence.source["artifact_id"], "artifact-upload-rich-1")
        self.assertEqual(evidence.source["normalized_artifact_id"], "artifact-upload-rich-1-normalized")
        self.assertGreaterEqual(len(evidence.chunks), 2)

    def test_csv_upload_import_uses_structured_parser_metadata(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-csv-1",
                display_name="Quarterly Access Review CSV",
                artifact_text=(
                    "reviewer,system,status\n"
                    "Security Engineering,production-admins,approved\n"
                    "Security Engineering,break-glass,approved\n"
                ),
            ),
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-csv-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-csv-1")
            ).first()
            chunk_rows = session.scalars(
                select(EvidenceChunkRow)
                .where(EvidenceChunkRow.evidence_id == evidence_row.evidence_id)
                .order_by(EvidenceChunkRow.chunk_index.asc())
            ).all()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "csv")
        self.assertEqual(normalized_row.metadata_payload["row_count"], 2)
        self.assertEqual(
            normalized_row.metadata_payload["column_names"],
            ["reviewer", "system", "status"],
        )
        self.assertIn("CSV row 1", normalized_row.content_text)
        self.assertEqual(len(chunk_rows), 2)

    def test_json_upload_import_flattens_nested_fields(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-json-1",
                display_name="Access Review JSON",
                artifact_text=json.dumps(
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
            )
            | {"source_locator": "uploads/access-review.json"},
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-json-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-json-1")
            ).first()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "json")
        self.assertEqual(normalized_row.metadata_payload["top_level_keys"], ["review", "controls"])
        self.assertIn("review.owner: Security Engineering", normalized_row.content_text)
        self.assertIn("controls[0].code: CC6.1", normalized_row.content_text)

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertIn("review.owner: Security Engineering", evidence.summary)

    def test_markdown_upload_import_preserves_section_structure(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-md-1",
                display_name="Access Review Notes",
                artifact_text=(
                    "# Access Review\n\n"
                    "- Reviewer: Security Engineering\n"
                    "- Quarter: 2026-Q1\n\n"
                    "## Findings\n\n"
                    "1. Two stale contractor accounts were removed.\n"
                    "2. Break-glass access remained limited.\n"
                ),
            )
            | {"source_locator": "uploads/access-review.md"},
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-md-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-md-1")
            ).first()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "markdown")
        self.assertEqual(normalized_row.metadata_payload["heading_count"], 2)
        self.assertEqual(normalized_row.metadata_payload["bullet_count"], 4)
        self.assertIn("Access Review", normalized_row.content_text)
        self.assertIn("Findings", normalized_row.content_text)

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertGreaterEqual(len(evidence.chunks), 2)

    def test_html_upload_import_strips_markup_into_text_chunks(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-html-1",
                display_name="Access Review Portal Export",
                artifact_text=(
                    "<html><body><h1>Access Review</h1><p>Reviewer: Security Engineering</p>"
                    "<ul><li>Production admins approved</li><li>Break-glass reviewed</li></ul>"
                    "<p>All actions completed.</p></body></html>"
                ),
            )
            | {"source_locator": "uploads/access-review.html"},
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-html-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-html-1")
            ).first()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "html")
        self.assertEqual(normalized_row.metadata_payload["heading_count"], 1)
        self.assertIn("Access Review", normalized_row.content_text)
        self.assertIn("Production admins approved", normalized_row.content_text)
        self.assertNotIn("<h1>", normalized_row.content_text)

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertGreaterEqual(len(evidence.chunks), 1)

    def test_pdf_upload_import_extracts_text_from_binary_payload(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        pdf_bytes = (
            b"%PDF-1.4\n"
            b"1 0 obj\n<< /Type /Catalog >>\nendobj\n"
            b"2 0 obj\n<< /Length 98 >>\nstream\n"
            b"BT /F1 12 Tf 72 720 Td "
            b"(Access Review Report) Tj "
            b"(Reviewer: Security Engineering) Tj "
            b"(All privileged assignments approved.) Tj "
            b"ET\nendstream\nendobj\n"
        )
        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-pdf-1",
                display_name="Access Review PDF",
                artifact_text=None,
                artifact_bytes_base64=base64.b64encode(pdf_bytes).decode("ascii"),
            )
            | {"source_locator": "uploads/access-review.pdf"},
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-pdf-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-pdf-1")
            ).first()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "pdf_text_extract")
        self.assertEqual(normalized_row.metadata_payload["source_format"], "pdf")
        self.assertIn("Access Review Report", normalized_row.content_text)
        self.assertIn("Security Engineering", normalized_row.content_text)

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertIn("Access Review Report", evidence.summary)

    def test_png_upload_import_extracts_text_from_binary_payload(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        png_bytes = _png_with_text(
            "Reviewer: Security Engineering; Break-glass access reviewed; All actions completed."
        )
        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-png-1",
                display_name="Access Review Screenshot",
                artifact_text=None,
                artifact_bytes_base64=base64.b64encode(png_bytes).decode("ascii"),
            )
            | {"source_locator": "uploads/access-review.png"},
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-png-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-png-1")
            ).first()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "image_ocr_heuristic")
        self.assertEqual(normalized_row.metadata_payload["source_format"], "png")
        self.assertTrue(normalized_row.metadata_payload["ocr_used"])
        self.assertIn("Security Engineering", normalized_row.content_text)
        self.assertIn("Break-glass access reviewed", normalized_row.content_text)

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertIn("Security Engineering", evidence.summary)

    def test_docx_upload_import_extracts_text_from_binary_payload(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        docx_bytes = _docx_with_paragraphs(
            [
                "Access Review Notes",
                "Reviewer: Security Engineering",
                "All privileged assignments approved.",
            ]
        )
        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-docx-1",
                display_name="Access Review DOCX",
                artifact_text=None,
                artifact_bytes_base64=base64.b64encode(docx_bytes).decode("ascii"),
            )
            | {"source_locator": "uploads/access-review.docx"},
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-docx-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-docx-1")
            ).first()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "docx_xml_extract")
        self.assertEqual(normalized_row.metadata_payload["source_format"], "docx")
        self.assertIn("Access Review Notes", normalized_row.content_text)
        self.assertIn("Security Engineering", normalized_row.content_text)

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertIn("Access Review Notes", evidence.summary)

    def test_xlsx_upload_import_extracts_rows_from_binary_payload(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        xlsx_bytes = _xlsx_with_rows(
            [
                ["reviewer", "system", "status"],
                ["Security Engineering", "break-glass", "approved"],
            ]
        )
        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-xlsx-1",
                display_name="Access Review XLSX",
                artifact_text=None,
                artifact_bytes_base64=base64.b64encode(xlsx_bytes).decode("ascii"),
            )
            | {"source_locator": "uploads/access-review.xlsx"},
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-xlsx-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-xlsx-1")
            ).first()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "xlsx_xml_extract")
        self.assertEqual(normalized_row.metadata_payload["source_format"], "xlsx")
        self.assertEqual(normalized_row.metadata_payload["row_count"], 2)
        self.assertIn("sheet1 row 1", normalized_row.content_text)
        self.assertIn("A2: Security Engineering", normalized_row.content_text)

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertIn("sheet1 row 1", evidence.summary)

    def test_zip_upload_import_extracts_textual_entries_from_binary_payload(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        zip_bytes = _zip_with_entries(
            {
                "reviews/access-review.csv": "reviewer,system,status\nSecurity Engineering,break-glass,approved\n",
                "notes/findings.md": "# Findings\n\n- Two stale contractor accounts were removed.\n",
            }
        )
        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-upload-zip-1",
                display_name="Access Review ZIP",
                artifact_text=None,
                artifact_bytes_base64=base64.b64encode(zip_bytes).decode("ascii"),
            )
            | {"source_locator": "uploads/access-review.zip"},
        )
        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            normalized_row = session.get(ArtifactBlobRow, "artifact-upload-zip-1-normalized")
            evidence_row = session.scalars(
                select(EvidenceRow).where(EvidenceRow.source_artifact_id == "artifact-upload-zip-1")
            ).first()

        self.assertIsNotNone(normalized_row)
        self.assertEqual(normalized_row.metadata_payload["parser_kind"], "zip_entry_extract")
        self.assertEqual(normalized_row.metadata_payload["source_format"], "zip")
        self.assertIn("reviews/access-review.csv", normalized_row.content_text)
        self.assertIn("Security Engineering", normalized_row.content_text)
        self.assertIn("notes/findings.md", normalized_row.content_text)

        evidence = service.get_evidence(evidence_row.evidence_id)
        self.assertIn("reviews/access-review.csv", evidence.summary)

    def test_import_processing_indexes_chunks_for_evidence_search(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        xlsx_bytes = _xlsx_with_rows(
            [
                ["reviewer", "system", "status"],
                ["Security Engineering", "break-glass", "approved"],
            ]
        )
        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-search-xlsx-1",
                display_name="Searchable Access Review XLSX",
                artifact_text=None,
                artifact_bytes_base64=base64.b64encode(xlsx_bytes).decode("ascii"),
            )
            | {"source_locator": "uploads/searchable-access-review.xlsx"},
        )
        service.dispatch_import_jobs()
        results = service.search_evidence("cycle-1", query="break glass approved", limit=5)

        with service.repository.session_factory() as session:
            indexed_rows = session.scalars(
                select(EmbeddingChunkRow)
                .where(EmbeddingChunkRow.workspace_id == "audit-ws-1")
                .where(EmbeddingChunkRow.subject_type == "audit_evidence")
                .where(EmbeddingChunkRow.model_name == "lexical-v1")
            ).all()

        self.assertGreaterEqual(len(indexed_rows), 2)
        self.assertGreaterEqual(results.total_count, 1)
        self.assertEqual(results.items[0].title, "Searchable Access Review XLSX")
        self.assertIn("break-glass", results.items[0].text_excerpt)

    def test_hybrid_evidence_search_matches_semantic_control_language(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                artifact_id="artifact-semantic-search-1",
                display_name="Quarterly Entitlement Certification",
                artifact_text=(
                    "Quarterly entitlement recertification completed for production privileges.\n"
                    "Manager signoff recorded after reviewer certification."
                ),
            )
            | {"source_locator": "uploads/entitlement-certification.txt"},
        )
        service.dispatch_import_jobs()
        results = service.search_evidence("cycle-1", query="access review approval", limit=5)

        with service.repository.session_factory() as session:
            semantic_rows = session.scalars(
                select(EmbeddingChunkRow)
                .where(EmbeddingChunkRow.workspace_id == "audit-ws-1")
                .where(EmbeddingChunkRow.subject_type == "audit_evidence")
                .where(EmbeddingChunkRow.model_name == service.repository.semantic_model_name)
            ).all()
            semantic_vector_rows = session.scalars(
                select(SemanticVectorRow)
                .where(SemanticVectorRow.workspace_id == "audit-ws-1")
                .where(SemanticVectorRow.subject_type == "audit_evidence")
                .where(SemanticVectorRow.model_name == service.repository.semantic_model_name)
            ).all()

        self.assertGreaterEqual(len(semantic_rows), 2)
        self.assertGreaterEqual(len(semantic_vector_rows), 2)
        self.assertTrue(
            all(
                isinstance((row.metadata_payload or {}).get("embedding_vector"), list)
                and len((row.metadata_payload or {}).get("embedding_vector"))
                == service.repository.semantic_vector_dimension
                and isinstance((row.metadata_payload or {}).get("ann_bucket_keys"), list)
                and len((row.metadata_payload or {}).get("ann_bucket_keys")) >= 2
                and (row.metadata_payload or {}).get("vector_search_backend") in {"ann-metadata-json", "flat-metadata-json"}
                for row in semantic_rows
            )
        )
        self.assertTrue(
            all(
                isinstance(row.embedding_vector, list)
                and len(row.embedding_vector) == service.repository.semantic_vector_dimension
                and isinstance(row.ann_bucket_keys, list)
                and len(row.ann_bucket_keys) >= 2
                and isinstance(row.semantic_terms, list)
                for row in semantic_vector_rows
            )
        )
        self.assertGreaterEqual(results.total_count, 1)
        self.assertTrue(
            any(item.title == "Quarterly Entitlement Certification" for item in results.items)
        )
        matching_item = next(
            item for item in results.items
            if item.title == "Quarterly Entitlement Certification"
        )
        self.assertIn("signoff", matching_item.text_excerpt.lower())

    def test_semantic_search_uses_candidate_pruning_in_ann_mode(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        for index in range(6):
            service.create_upload_import(
                "cycle-1",
                upload_import_command(
                    artifact_id=f"artifact-semantic-prune-{index}",
                    display_name=f"Entitlement Certification {index}",
                    artifact_text=(
                        f"Quarterly entitlement certification {index}\n\n"
                        "Privileged entitlement attestation completed with documented signoff."
                    ),
                ),
                organization_id="org-1",
            )
        service.dispatch_import_jobs()

        repository = service.repository
        repository.vector_search_mode = "ann"
        repository.semantic_candidate_limit = 2

        semantic_call_count = 0
        original_score_semantic_match = repository._score_semantic_match

        def counting_score_semantic_match(*, query, text_content, metadata_payload, query_vector=None):
            nonlocal semantic_call_count
            semantic_call_count += 1
            return original_score_semantic_match(
                query=query,
                text_content=text_content,
                metadata_payload=metadata_payload,
                query_vector=query_vector,
            )

        repository._score_semantic_match = counting_score_semantic_match

        results = service.search_evidence("cycle-1", query="access review", limit=5)

        self.assertGreaterEqual(results.total_count, 1)
        self.assertLessEqual(semantic_call_count, 2)

    def test_import_processing_populates_workflow_grounding_context(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command(comment="Accepted reviewer pattern."))
        service.decide_gap("gap-1", gap_decision_command(comment="Resolved after refreshed evidence."))

        workflow_run_id = "auditflow-grounding-import-1"
        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                workflow_run_id=workflow_run_id,
                artifact_id="artifact-grounding-1",
                display_name="Grounded Access Review Upload",
                artifact_text=(
                    "Quarterly access review completed for production systems.\n"
                    "Reviewer confirmed all privileged assignments."
                ),
            )
            | {"source_locator": "uploads/grounded-access-review.txt"},
        )
        service.dispatch_import_jobs()
        state = service.get_workflow_state(workflow_run_id)

        self.assertEqual(state.workflow_run_id, workflow_run_id)
        self.assertEqual(state.raw_state["framework_name"], "SOC2")
        self.assertEqual(state.raw_state["freshness_policy"]["max_age_days"], 90)
        self.assertEqual(state.raw_state["in_scope_controls"][0]["control_code"], "CC6.1")
        self.assertTrue(
            any(ref["kind"] == "historical_evidence_chunk" for ref in state.raw_state["evidence_chunk_refs"])
        )
        self.assertTrue(
            any(memory["decision"] == "accept" for memory in state.raw_state["mapping_memory_context"])
        )
        self.assertTrue(
            any(memory["decision"] == "resolve_gap" for memory in state.raw_state["challenge_memory_context"])
        )

    def test_import_processing_preserves_initiating_auth_context_in_workflow_state(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        workflow_run_id = "auditflow-import-auth-context-1"
        service.create_upload_import(
            "cycle-1",
            upload_import_command(
                workflow_run_id=workflow_run_id,
                artifact_id="artifact-auth-context-1",
                display_name="Auth Context Upload",
                artifact_text="Quarterly access review completed for production systems.",
            ),
            auth_context=SimpleNamespace(
                user_id="user-reviewer-1",
                role="reviewer",
                session_id="auth-session-1",
            ),
        )
        service.dispatch_import_jobs()
        state = service.get_workflow_state(workflow_run_id)

        self.assertEqual(state.raw_state["user_id"], "user-reviewer-1")
        self.assertEqual(state.raw_state["role"], "reviewer")
        self.assertEqual(state.raw_state["session_id"], "auth-session-1")

    def test_mapping_and_gap_reviews_record_memory_context(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command(comment="Accepted reviewer pattern."))
        service.decide_gap("gap-1", gap_decision_command(comment="Resolved after new upload."))
        organization_memory = service.list_memory_records(
            "cycle-1",
            scope="organization",
            subject_type="framework_control",
        )
        cycle_memory = service.list_memory_records(
            "cycle-1",
            scope="cycle",
            subject_type="audit_cycle",
        )

        with service.repository.session_factory() as session:
            stored_rows = session.scalars(select(MemoryRecordRow).order_by(MemoryRecordRow.created_at.asc())).all()

        self.assertEqual(len(stored_rows), 3)
        self.assertEqual(organization_memory.total_count, 1)
        self.assertEqual(organization_memory.items[0].memory_key, "mapping:mapping-1")
        self.assertEqual(organization_memory.items[0].value["decision"], "accept")
        self.assertEqual(organization_memory.items[0].value["control_code"], "CC6.1")
        self.assertIn("Quarterly access review", organization_memory.items[0].value["evidence_summary"])
        self.assertEqual(cycle_memory.total_count, 2)
        self.assertTrue(any(item.memory_key == "mapping:mapping-1" for item in cycle_memory.items))
        self.assertTrue(any(item.memory_key == "gap:gap-1" for item in cycle_memory.items))

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

    def test_list_tool_access_audit_filters_by_workflow_user_and_tool(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        tool_executor = service.workflow_api_service.execution_service.tool_executor
        call = SimpleNamespace(
            tool_call_id="tool-audit-service-1",
            tool_name="evidence.search",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-tool-audit-service-1",
            node_name="mapping",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "workspace_id": "audit-ws-1",
                "audit_cycle_id": "cycle-1",
                "query": "access review",
                "limit": 5,
            },
            idempotency_key="wf-tool-audit-service-1:evidence.search",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-1",
                role="reviewer",
                session_id="auth-session-1",
            ),
        )

        tool_executor.execute(call)
        all_rows = service.list_tool_access_audit(organization_id="org-1")
        filtered_rows = service.list_tool_access_audit(
            workflow_run_id="wf-tool-audit-service-1",
            user_id="user-reviewer-1",
            tool_name="evidence.search",
            organization_id="org-1",
        )

        self.assertGreaterEqual(all_rows.total_count, 1)
        self.assertEqual(filtered_rows.total_count, 1)
        self.assertEqual(filtered_rows.items[0].workflow_run_id, "wf-tool-audit-service-1")
        self.assertEqual(filtered_rows.items[0].node_name, "mapping")
        self.assertEqual(filtered_rows.items[0].tool_name, "evidence.search")
        self.assertEqual(filtered_rows.items[0].user_id, "user-reviewer-1")
        self.assertEqual(filtered_rows.items[0].role, "reviewer")
        self.assertEqual(filtered_rows.items[0].session_id, "auth-session-1")
        self.assertEqual(filtered_rows.items[0].arguments["query"], "access review")

    def test_cycle_dashboard_and_cycle_tool_access_endpoint_share_cycle_scoped_audit_view(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        tool_executor = service.workflow_api_service.execution_service.tool_executor
        first_call = SimpleNamespace(
            tool_call_id="tool-audit-cycle-1",
            tool_name="evidence.search",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-cycle-audit-1",
            node_name="mapping",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "workspace_id": "audit-ws-1",
                "audit_cycle_id": "cycle-1",
                "query": "access review",
                "limit": 5,
            },
            idempotency_key="wf-cycle-audit-1:evidence.search",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-1",
                role="reviewer",
                session_id="auth-session-1",
            ),
        )
        second_call = SimpleNamespace(
            tool_call_id="tool-audit-cycle-2",
            tool_name="mapping.read_candidates",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-cycle-audit-2",
            node_name="challenge",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "audit_cycle_id": "cycle-1",
                "evidence_item_id": "evidence-1",
                "control_id": "control-state-1",
            },
            idempotency_key="wf-cycle-audit-2:mapping.read_candidates",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-2",
                role="reviewer",
                session_id="auth-session-2",
            ),
        )

        tool_executor.execute(first_call)
        tool_executor.execute(second_call)

        dashboard = service.get_cycle_dashboard("cycle-1", organization_id="org-1")
        filtered_rows = service.list_cycle_tool_access_audit(
            "cycle-1",
            workflow_run_id="wf-cycle-audit-2",
            tool_name="mapping.read_candidates",
            organization_id="org-1",
        )

        self.assertEqual(dashboard.tool_access_summary.total_count, 2)
        self.assertEqual(dashboard.tool_access_summary.latest_workflow_run_id, "wf-cycle-audit-2")
        self.assertEqual(
            dashboard.tool_access_summary.recent_tool_names,
            ["mapping.read_candidates", "evidence.search"],
        )
        self.assertEqual(dashboard.tool_access_summary.execution_status_counts, {"success": 2})
        self.assertEqual(filtered_rows.total_count, 1)
        self.assertEqual(filtered_rows.items[0].workflow_run_id, "wf-cycle-audit-2")
        self.assertEqual(filtered_rows.items[0].tool_name, "mapping.read_candidates")
        self.assertEqual(filtered_rows.items[0].user_id, "user-reviewer-2")

    def test_control_detail_and_control_tool_access_endpoint_share_control_scoped_audit_view(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        tool_executor = service.workflow_api_service.execution_service.tool_executor
        unrelated_cycle_call = SimpleNamespace(
            tool_call_id="tool-audit-control-1",
            tool_name="evidence.search",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-control-audit-1",
            node_name="mapping",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "workspace_id": "audit-ws-1",
                "audit_cycle_id": "cycle-1",
                "query": "access review",
                "limit": 3,
            },
            idempotency_key="wf-control-audit-1:evidence.search",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-1",
                role="reviewer",
                session_id="auth-session-1",
            ),
        )
        direct_control_call = SimpleNamespace(
            tool_call_id="tool-audit-control-2",
            tool_name="mapping.read_candidates",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-control-audit-2",
            node_name="challenge",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "audit_cycle_id": "cycle-1",
                "evidence_item_id": "evidence-1",
                "control_id": "control-state-1",
            },
            idempotency_key="wf-control-audit-2:mapping.read_candidates",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-2",
                role="reviewer",
                session_id="auth-session-2",
            ),
        )
        mapping_context_call = SimpleNamespace(
            tool_call_id="tool-audit-control-3",
            tool_name="review_decision.read_history",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-control-audit-3",
            node_name="challenge",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "audit_cycle_id": "cycle-1",
                "mapping_id": "mapping-1",
            },
            idempotency_key="wf-control-audit-3:review_decision.read_history",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-3",
                role="reviewer",
                session_id="auth-session-3",
            ),
        )
        other_control_call = SimpleNamespace(
            tool_call_id="tool-audit-control-4",
            tool_name="mapping.read_candidates",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-control-audit-4",
            node_name="challenge",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "audit_cycle_id": "cycle-1",
                "control_id": "control-state-2",
            },
            idempotency_key="wf-control-audit-4:mapping.read_candidates",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-4",
                role="reviewer",
                session_id="auth-session-4",
            ),
        )

        tool_executor.execute(unrelated_cycle_call)
        tool_executor.execute(direct_control_call)
        tool_executor.execute(mapping_context_call)
        tool_executor.execute(other_control_call)

        control_detail = service.get_control_detail("control-state-1", organization_id="org-1")
        filtered_rows = service.list_control_tool_access_audit(
            "control-state-1",
            tool_name="mapping.read_candidates",
            organization_id="org-1",
        )

        self.assertEqual(control_detail.tool_access_summary.total_count, 2)
        self.assertEqual(control_detail.tool_access_summary.latest_workflow_run_id, "wf-control-audit-3")
        self.assertEqual(
            control_detail.tool_access_summary.recent_tool_names,
            ["review_decision.read_history", "mapping.read_candidates"],
        )
        self.assertEqual(control_detail.tool_access_summary.execution_status_counts, {"success": 2})
        self.assertEqual(filtered_rows.total_count, 1)
        self.assertEqual(filtered_rows.items[0].workflow_run_id, "wf-control-audit-2")
        self.assertEqual(filtered_rows.items[0].tool_name, "mapping.read_candidates")
        self.assertEqual(filtered_rows.items[0].arguments["control_id"], "control-state-1")

    def test_review_queue_and_mapping_tool_access_endpoint_share_mapping_scoped_audit_view(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        tool_executor = service.workflow_api_service.execution_service.tool_executor
        unrelated_cycle_call = SimpleNamespace(
            tool_call_id="tool-audit-mapping-1",
            tool_name="evidence.search",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-mapping-audit-1",
            node_name="mapping",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "workspace_id": "audit-ws-1",
                "audit_cycle_id": "cycle-1",
                "query": "access review",
                "limit": 3,
            },
            idempotency_key="wf-mapping-audit-1:evidence.search",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-1",
                role="reviewer",
                session_id="auth-session-1",
            ),
        )
        inferred_mapping_call = SimpleNamespace(
            tool_call_id="tool-audit-mapping-2",
            tool_name="mapping.read_candidates",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-mapping-audit-2",
            node_name="challenge",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "audit_cycle_id": "cycle-1",
                "evidence_item_id": "evidence-1",
                "control_id": "control-state-1",
            },
            idempotency_key="wf-mapping-audit-2:mapping.read_candidates",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-2",
                role="reviewer",
                session_id="auth-session-2",
            ),
        )
        direct_mapping_call = SimpleNamespace(
            tool_call_id="tool-audit-mapping-3",
            tool_name="review_decision.read_history",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-mapping-audit-3",
            node_name="challenge",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "audit_cycle_id": "cycle-1",
                "mapping_id": "mapping-1",
            },
            idempotency_key="wf-mapping-audit-3:review_decision.read_history",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-3",
                role="reviewer",
                session_id="auth-session-3",
            ),
        )
        other_mapping_call = SimpleNamespace(
            tool_call_id="tool-audit-mapping-4",
            tool_name="mapping.read_candidates",
            tool_version="2026-03-16.1",
            workflow_run_id="wf-mapping-audit-4",
            node_name="challenge",
            subject_type="audit_cycle",
            subject_id="cycle-1",
            arguments={
                "audit_cycle_id": "cycle-1",
                "evidence_item_id": "evidence-1",
                "control_id": "control-state-2",
            },
            idempotency_key="wf-mapping-audit-4:mapping.read_candidates",
            authorization_context=SimpleNamespace(
                organization_id="org-1",
                workspace_id="audit-ws-1",
                user_id="user-reviewer-4",
                role="reviewer",
                session_id="auth-session-4",
            ),
        )

        tool_executor.execute(unrelated_cycle_call)
        tool_executor.execute(inferred_mapping_call)
        tool_executor.execute(direct_mapping_call)
        tool_executor.execute(other_mapping_call)

        review_queue = service.list_review_queue("cycle-1", organization_id="org-1")
        filtered_rows = service.list_mapping_tool_access_audit(
            "mapping-1",
            tool_name="mapping.read_candidates",
            organization_id="org-1",
        )

        self.assertEqual(review_queue.total_count, 1)
        self.assertEqual(review_queue.items[0].mapping_id, "mapping-1")
        self.assertEqual(review_queue.items[0].tool_access_summary.total_count, 2)
        self.assertEqual(review_queue.items[0].tool_access_summary.latest_workflow_run_id, "wf-mapping-audit-3")
        self.assertEqual(
            review_queue.items[0].tool_access_summary.recent_tool_names,
            ["review_decision.read_history", "mapping.read_candidates"],
        )
        self.assertEqual(review_queue.items[0].tool_access_summary.execution_status_counts, {"success": 2})
        self.assertEqual(filtered_rows.total_count, 1)
        self.assertEqual(filtered_rows.items[0].workflow_run_id, "wf-mapping-audit-2")
        self.assertEqual(filtered_rows.items[0].tool_name, "mapping.read_candidates")
        self.assertEqual(filtered_rows.items[0].arguments["control_id"], "control-state-1")

    def test_mapping_review_emits_review_recorded_outbox_event(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command(comment="Accepting mapped evidence."))
        pending = service.runtime_stores.outbox_store.list_pending()
        matching = [item.event for item in pending if item.event.event_name == "auditflow.review.recorded"]

        self.assertTrue(any(event.payload.get("mapping_id") == "mapping-1" for event in matching))

    def test_create_export_package_emits_export_progress_and_ready_events(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command())
        service.decide_gap("gap-1", gap_decision_command())
        package = service.create_export_package("cycle-1", export_create_command(workflow_run_id="auditflow-event-export-1"))
        pending = service.runtime_stores.outbox_store.list_pending()
        matching = [
            item.event
            for item in pending
            if item.event.workflow_run_id == "auditflow-event-export-1"
        ]

        self.assertTrue(any(event.event_name == "auditflow.export.progress" for event in matching))
        self.assertTrue(
            any(
                event.event_name == "auditflow.package.ready"
                and event.payload.get("package_id") == package.package_id
                for event in matching
            )
        )

    def test_list_review_decisions_supports_cycle_and_subject_filters(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command(comment="Accepting mapped evidence."))
        service.decide_gap("gap-1", gap_decision_command(comment="Gap is resolved."))

        all_decisions = service.list_review_decisions("cycle-1")
        mapping_decisions = service.list_review_decisions("cycle-1", mapping_id="mapping-1")
        gap_decisions = service.list_review_decisions("cycle-1", gap_id="gap-1")

        self.assertEqual(all_decisions.total_count, 2)
        self.assertEqual(mapping_decisions.total_count, 1)
        self.assertEqual(mapping_decisions.items[0].mapping_id, "mapping-1")
        self.assertIsNone(mapping_decisions.items[0].gap_id)
        self.assertEqual(gap_decisions.total_count, 1)
        self.assertEqual(gap_decisions.items[0].gap_id, "gap-1")
        self.assertIn("decision:resolve_gap", gap_decisions.items[0].feedback_tags)

    def test_process_cycle_and_load_state(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        result = service.process_cycle(cycle_processing_command(workflow_run_id="auditflow-service-cycle-1"))
        state = service.get_workflow_state("auditflow-service-cycle-1")

        self.assertEqual(result.current_state, "human_review")
        self.assertEqual(state.current_state, "human_review")
        self.assertEqual(state.workflow_type, "auditflow_cycle")

    def test_process_cycle_persists_workflow_generated_mapping(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        cycle = service.create_cycle(
            cycle_create_command(workspace_id="audit-ws-1", cycle_name="SOC2 2033")
        )
        control = service.list_controls(cycle.cycle_id)[0]
        service.repository.upsert_artifact_blob(
            artifact_id="artifact-generated-1",
            artifact_type="upload_raw",
            content_text="Quarterly access review evidence for generated workflow mapping.",
            metadata_payload={
                "organization_id": "org-1",
                "workspace_id": "audit-ws-1",
            },
        )

        result = service.process_cycle(
            {
                "workflow_run_id": "auditflow-generated-cycle-1",
                "audit_cycle_id": cycle.cycle_id,
                "audit_workspace_id": "audit-ws-1",
                "organization_id": "org-1",
                "workspace_id": "audit-ws-1",
                "source_id": "source-generated-1",
                "source_type": "upload",
                "artifact_id": "artifact-generated-1",
                "extracted_text_or_summary": "Quarterly access review completed for production systems.",
                "allowed_evidence_types": ["ticket"],
                "evidence_item_id": "evidence-generated-1",
                "evidence_chunk_refs": [{"kind": "evidence_chunk", "id": "chunk-generated-1"}],
                "in_scope_controls": [
                    {
                        "control_state_id": control.control_state_id,
                        "control_code": control.control_code,
                        "title": "Access permissions are scoped and reviewed.",
                    }
                ],
                "framework_name": "SOC2",
                "mapping_payloads": [],
                "mapping_memory_context": [],
                "challenge_memory_context": [],
                "freshness_policy": {"mode": "standard", "max_age_days": 90},
                "control_text": "Review user access quarterly.",
            }
        )
        mappings = service.list_mappings(cycle.cycle_id)

        self.assertEqual(result.current_state, "human_review")
        self.assertEqual(mappings.total_count, 1)
        self.assertEqual(mappings.items[0].control_state_id, control.control_state_id)
        self.assertEqual(mappings.items[0].evidence_item_id, "evidence-generated-1")
        self.assertIn("aligns best", mappings.items[0].rationale_summary.lower())

    def test_process_cycle_emits_mapping_progress_event(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.process_cycle(cycle_processing_command(workflow_run_id="auditflow-progress-cycle-1"))
        pending = service.runtime_stores.outbox_store.list_pending()
        matching = [
            item.event
            for item in pending
            if item.event.workflow_run_id == "auditflow-progress-cycle-1"
            and item.event.event_name == "auditflow.mapping.progress"
        ]

        self.assertTrue(any(event.payload.get("cycle_id") == "cycle-1" for event in matching))

    def test_review_mapping_and_generate_export(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        review_result = service.review_mapping("mapping-1", mapping_review_command())
        service.decide_gap("gap-1", gap_decision_command())
        review_queue = service.list_review_queue("cycle-1")
        result = service.generate_export(
            export_generation_command(
                workflow_run_id="auditflow-service-export-1",
                working_snapshot_version=3,
            ),
            auth_context=SimpleNamespace(
                user_id="user-admin-1",
                role="product_admin",
                session_id="auth-session-export-1",
            ),
        )
        dashboard = service.get_cycle_dashboard("cycle-1")
        export_package = service.get_export_package(dashboard.latest_export_package.package_id)
        narratives = service.list_narratives("cycle-1")

        self.assertEqual(review_result.mapping_status, "accepted")
        self.assertEqual(review_queue.total_count, 0)
        self.assertEqual(result.current_state, "exported")
        self.assertEqual(dashboard.cycle.current_snapshot_version, 3)
        self.assertEqual(export_package.status, "ready")
        self.assertEqual(export_package.package_artifact_id, export_package.artifact_id)
        self.assertIsNotNone(export_package.manifest_artifact_id)
        self.assertIsNotNone(export_package.immutable_at)
        self.assertGreaterEqual(len(narratives), 1)
        self.assertTrue(
            any("Snapshot 3 for cycle `cycle-1` packages accepted mappings" in item.content_markdown for item in narratives)
        )

        with service.repository.session_factory() as session:
            package_row = session.get(ArtifactBlobRow, export_package.package_artifact_id)
            manifest_row = session.get(ArtifactBlobRow, export_package.manifest_artifact_id)
            snapshot_row = session.scalars(
                select(CycleSnapshotRow)
                .where(CycleSnapshotRow.cycle_id == "cycle-1")
                .where(CycleSnapshotRow.snapshot_version == 3)
            ).first()

        self.assertIsNotNone(package_row)
        self.assertIsNotNone(manifest_row)
        self.assertIsNotNone(snapshot_row)
        self.assertIn("manifest_artifact_id", package_row.content_text)
        self.assertIn("tool_access_audit_count", package_row.content_text)
        self.assertIn("narrative_markdown", package_row.content_text)
        self.assertIn("accepted_mappings", manifest_row.content_text)
        self.assertIn("narratives", manifest_row.content_text)
        self.assertIn("tool_access_audit_summary", manifest_row.content_text)
        self.assertIn("tool_access_audit", manifest_row.content_text)
        self.assertIn('"tool_name": "export.snapshot_validate"', manifest_row.content_text)
        self.assertIn('"tool_name": "narrative.snapshot_read"', manifest_row.content_text)
        self.assertIn('"user_id": "user-admin-1"', manifest_row.content_text)
        self.assertEqual(snapshot_row.snapshot_status, "frozen")
        self.assertEqual(snapshot_row.package_id, export_package.package_id)
        self.assertIsNotNone(snapshot_row.frozen_at)

    def test_import_processing_uses_stable_evidence_id_and_workflow_mapping(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        upload = service.create_upload_import(
            "cycle-1",
            upload_import_command(workflow_run_id="auditflow-import-stable-1"),
        )
        source_id = upload.evidence_source_ids[0]

        service.dispatch_import_jobs()

        with service.repository.session_factory() as session:
            evidence_rows = session.scalars(
                select(EvidenceRow).where(EvidenceRow.audit_cycle_id == "cycle-1")
            ).all()
            evidence_row = next(
                row
                for row in evidence_rows
                if isinstance(row.source_payload, dict)
                and row.source_payload.get("evidence_source_id") == source_id
            )
            mapping_rows = session.scalars(
                select(MappingRow)
                .where(MappingRow.cycle_id == "cycle-1")
                .where(MappingRow.evidence_item_id == evidence_row.evidence_id)
            ).all()

        expected_evidence_id = service._stable_import_evidence_id(
            cycle_id="cycle-1",
            evidence_source_id=source_id,
        )
        self.assertEqual(evidence_row.evidence_id, expected_evidence_id)
        self.assertGreaterEqual(len(mapping_rows), 1)
        self.assertTrue(
            all("requires reviewer confirmation" not in row.rationale_summary for row in mapping_rows)
        )

    def test_create_export_package_returns_latest_package(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command())
        service.decide_gap("gap-1", gap_decision_command())
        package = service.create_export_package("cycle-1", export_create_command(workflow_run_id="auditflow-service-export-2"))

        self.assertEqual(package.cycle_id, "cycle-1")
        self.assertEqual(package.snapshot_version, 3)

    def test_list_export_packages_returns_cycle_freeze_history(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command())
        service.decide_gap("gap-1", gap_decision_command())
        first = service.create_export_package(
            "cycle-1",
            export_create_command(workflow_run_id="auditflow-export-ledger-1"),
        )
        second = service.create_export_package(
            "cycle-1",
            export_create_command(workflow_run_id="auditflow-export-ledger-2"),
        )

        packages = service.list_export_packages("cycle-1")
        ready_packages = service.list_export_packages("cycle-1", status="ready")

        self.assertEqual(second.package_id, first.package_id)
        self.assertEqual([item.package_id for item in packages], [first.package_id])
        self.assertEqual(len(ready_packages), 1)
        self.assertTrue(all(item.snapshot_version == first.snapshot_version for item in ready_packages))

    def test_create_export_package_rejects_cycle_not_ready(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        with self.assertRaisesRegex(ValueError, "CYCLE_NOT_READY_FOR_EXPORT"):
            service.create_export_package("cycle-1", export_create_command(workflow_run_id="auditflow-export-blocked"))

    def test_create_export_package_rejects_stale_snapshot(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command())
        service.decide_gap("gap-1", gap_decision_command())
        service.create_export_package("cycle-1", export_create_command(workflow_run_id="auditflow-export-current"))

        with self.assertRaisesRegex(ValueError, "SNAPSHOT_STALE"):
            service.create_export_package(
                "cycle-1",
                export_create_command(
                    workflow_run_id="auditflow-export-stale",
                    snapshot_version=2,
                ),
            )

    def test_create_export_package_rejects_duplicate_running_export(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        service.review_mapping("mapping-1", mapping_review_command())
        service.decide_gap("gap-1", gap_decision_command())
        now = datetime.now(UTC)
        with service.repository.session_factory.begin() as session:
            session.add(
                ExportPackageRow(
                    package_id="pkg-queued-1",
                    cycle_id="cycle-1",
                    snapshot_version=3,
                    status="queued",
                    artifact_id=None,
                    manifest_artifact_id=None,
                    workflow_run_id="auditflow-export-queued",
                    created_at=now,
                    immutable_at=None,
                )
            )

        with self.assertRaisesRegex(ValueError, "EXPORT_ALREADY_RUNNING"):
            service.create_export_package(
                "cycle-1",
                export_create_command(workflow_run_id="auditflow-export-duplicate"),
            )

    def test_sqlalchemy_repository_persists_export_across_service_instances(self) -> None:
        tmp_dir = _create_repo_tempdir("auditflow-db-")
        database_url = f"sqlite+pysqlite:///{(tmp_dir / 'auditflow.db').resolve().as_posix()}"

        service_one = build_app_service(database_url=database_url)
        service_two = None
        try:
            service_one.review_mapping("mapping-1", mapping_review_command())
            service_one.decide_gap("gap-1", gap_decision_command())
            result = service_one.generate_export(
                export_generation_command(
                    workflow_run_id="auditflow-persist-export-1",
                    working_snapshot_version=3,
                )
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
            shutil.rmtree(tmp_dir, ignore_errors=True)
