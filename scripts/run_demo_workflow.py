from __future__ import annotations

import argparse
import json
from uuid import uuid4

from _local_runtime import ensure_src_on_path, resolve_database_url

ensure_src_on_path()

from auditflow_app.bootstrap import build_app_service
from auditflow_app.sample_payloads import (
    cycle_create_command,
    cycle_processing_command,
    export_generation_command,
    upload_import_command,
    workspace_create_command,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the AuditFlow demo workflow against a local database.")
    parser.add_argument("--database-url", help="Optional SQLAlchemy database URL.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    suffix = uuid4().hex[:8]
    service = build_app_service(database_url=resolve_database_url(args.database_url))
    try:
        workspace = service.create_workspace(
            workspace_create_command(
                workspace_name=f"AuditFlow Demo Workspace {suffix}",
                slug=f"auditflow-demo-{suffix}",
            )
        )
        cycle = service.create_cycle(
            cycle_create_command(
                workspace_id=workspace.workspace_id,
                cycle_name=f"SOC2 Demo Cycle {suffix}",
            )
        )
        upload = service.create_upload_import(
            cycle.cycle_id,
            upload_import_command(
                workflow_run_id=f"auditflow-demo-upload-{suffix}",
                artifact_id=f"artifact-upload-{suffix}",
            ),
        )
        service.dispatch_import_jobs()
        processing_command = cycle_processing_command(
            workflow_run_id=f"auditflow-demo-cycle-{suffix}"
        )
        processing_command.update(
            {
                "audit_cycle_id": cycle.cycle_id,
                "audit_workspace_id": workspace.workspace_id,
                "workspace_id": workspace.workspace_id,
                "source_id": upload.evidence_source_ids[0],
                "artifact_id": upload.artifact_id,
                "evidence_item_id": f"evidence-{suffix}",
                "evidence_chunk_refs": [{"kind": "evidence_chunk", "id": f"chunk-{suffix}"}],
                "in_scope_controls": [f"control-{suffix}"],
                "mapping_payloads": [{"mapping_id": f"mapping-{suffix}"}],
            }
        )
        processing = service.process_cycle(
            processing_command
        )
        export_command = export_generation_command(
            workflow_run_id=f"auditflow-demo-export-{suffix}"
        )
        export_command.update(
            {
                "audit_cycle_id": cycle.cycle_id,
                "audit_workspace_id": workspace.workspace_id,
                "workspace_id": workspace.workspace_id,
                "accepted_mapping_refs": [f"mapping-{suffix}"],
            }
        )
        exported = service.generate_export(
            export_command
        )
        print(json.dumps({"processing": processing.model_dump(), "export": exported.model_dump()}, indent=2))
    finally:
        service.close()


if __name__ == "__main__":
    main()
