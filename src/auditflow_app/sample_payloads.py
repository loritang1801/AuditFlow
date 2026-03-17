def cycle_processing_payload() -> dict:
    return {
        "audit_cycle_id": "cycle-1",
        "audit_workspace_id": "audit-ws-1",
        "source_id": "source-1",
        "source_type": "upload",
        "artifact_id": "artifact-1",
        "extracted_text_or_summary": "Quarterly access review completed for production systems.",
        "allowed_evidence_types": ["ticket"],
        "evidence_item_id": "evidence-1",
        "evidence_chunk_refs": [{"kind": "evidence_chunk", "id": "chunk-1"}],
        "in_scope_controls": ["control-1"],
        "framework_name": "SOC2",
        "mapping_payloads": [{"mapping_id": "mapping-1"}],
        "control_text": "Review user access quarterly.",
    }


def export_generation_payload() -> dict:
    return {
        "audit_cycle_id": "cycle-1",
        "audit_workspace_id": "audit-ws-1",
        "working_snapshot_version": 3,
        "accepted_mapping_refs": ["mapping-1"],
        "open_gap_refs": [],
        "export_scope": "cycle_package",
    }


def cycle_processing_request(
    *,
    workflow_run_id: str = "auditflow-demo-cycle",
    state_overrides: dict | None = None,
) -> dict:
    return {
        "workflow_name": "auditflow_cycle_processing",
        "workflow_run_id": workflow_run_id,
        "input_payload": cycle_processing_payload(),
        "state_overrides": state_overrides or {},
    }


def export_generation_request(
    *,
    workflow_run_id: str = "auditflow-demo-export",
    state_overrides: dict | None = None,
) -> dict:
    return {
        "workflow_name": "auditflow_export_generation",
        "workflow_run_id": workflow_run_id,
        "input_payload": export_generation_payload(),
        "state_overrides": state_overrides or {},
    }


def cycle_processing_command(
    *,
    workflow_run_id: str = "auditflow-demo-cycle",
    state_overrides: dict | None = None,
) -> dict:
    payload = cycle_processing_payload()
    payload["workflow_run_id"] = workflow_run_id
    payload["state_overrides"] = state_overrides or {}
    return payload


def export_generation_command(
    *,
    workflow_run_id: str = "auditflow-demo-export",
    state_overrides: dict | None = None,
) -> dict:
    payload = export_generation_payload()
    payload["workflow_run_id"] = workflow_run_id
    payload["state_overrides"] = state_overrides or {}
    return payload


def mapping_review_command(
    *,
    decision: str = "accept",
    comment: str = "Citation is sufficient.",
    target_control_id: str | None = None,
) -> dict:
    return {
        "decision": decision,
        "comment": comment,
        "target_control_id": target_control_id,
    }


def gap_decision_command(*, decision: str = "resolve_gap", comment: str = "Gap resolved with current evidence.") -> dict:
    return {
        "decision": decision,
        "comment": comment,
    }


def upload_import_command(
    *,
    workflow_run_id: str | None = None,
    artifact_id: str = "artifact-upload-1",
    display_name: str = "Quarterly Access Review Export",
    evidence_type_hint: str = "report",
    artifact_text: str | None = (
        "Quarterly Access Review Export\n\n"
        "Control owner: Security Engineering\n"
        "Review period: 2026-Q1\n"
        "Result: All privileged access assignments were reviewed and approved.\n\n"
        "Reviewer notes:\n"
        "- Production admins remain limited to the platform team.\n"
        "- Two stale contractor accounts were removed before sign-off."
    ),
) -> dict:
    return {
        "workflow_run_id": workflow_run_id,
        "artifact_id": artifact_id,
        "display_name": display_name,
        "captured_at": "2026-03-16T09:00:00Z",
        "evidence_type_hint": evidence_type_hint,
        "source_locator": "uploads/q1-access-review.csv",
        "artifact_text": artifact_text,
    }


def external_import_command(
    *,
    workflow_run_id: str | None = None,
    provider: str = "jira",
    upstream_ids: list[str] | None = None,
) -> dict:
    return {
        "workflow_run_id": workflow_run_id,
        "connection_id": f"connection-{provider}-1",
        "provider": provider,
        "upstream_ids": upstream_ids or ["SEC-125", "SEC-126"],
        "query": None,
    }


def export_create_command(
    *,
    workflow_run_id: str = "auditflow-demo-export-create",
    snapshot_version: int = 3,
    format: str = "zip",
) -> dict:
    return {
        "workflow_run_id": workflow_run_id,
        "snapshot_version": snapshot_version,
        "format": format,
    }


def workspace_create_command(
    *,
    workspace_name: str = "AuditFlow Demo Workspace",
    slug: str | None = None,
    default_owner_user_id: str | None = None,
) -> dict:
    return {
        "workspace_name": workspace_name,
        "slug": slug,
        "framework_name": "SOC2",
        "workspace_status": "active",
        "default_owner_user_id": default_owner_user_id,
        "settings": {"freshness_days_default": 90},
    }


def cycle_create_command(
    *,
    workspace_id: str = "audit-ws-1",
    cycle_name: str = "SOC2 Demo Cycle",
    audit_period_start: str = "2026-01-01",
    audit_period_end: str = "2026-12-31",
    owner_user_id: str | None = None,
) -> dict:
    return {
        "workspace_id": workspace_id,
        "cycle_name": cycle_name,
        "framework_name": "SOC2",
        "audit_period_start": audit_period_start,
        "audit_period_end": audit_period_end,
        "owner_user_id": owner_user_id,
        "cycle_status": "draft",
    }
