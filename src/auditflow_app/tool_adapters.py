from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import or_, select

from .repository import (
    ArtifactBlobRow,
    AuditCycleRow,
    ControlCatalogRow,
    ControlCoverageRow,
    EmbeddingChunkRow,
    EvidenceChunkRow,
    EvidenceRow,
    ExportPackageRow,
    GapRow,
    MappingRow,
    NarrativeRow,
    ReviewDecisionRow,
    SEMANTIC_MODEL_NAME,
    SqlAlchemyAuditFlowRepository,
)


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _normalize_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    timestamp = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _tool_error_code(exc: Exception) -> str:
    if isinstance(exc, KeyError):
        return "RESOURCE_NOT_FOUND_OR_SCOPE_DENIED"
    if isinstance(exc, ValueError) and exc.args:
        return str(exc.args[0])
    return exc.__class__.__name__


class _AuditedToolAdapter:
    def __init__(self, repository: SqlAlchemyAuditFlowRepository) -> None:
        self.repository = repository

    def execute(self, *, tool, call, arguments):
        recorded_at = datetime.now(UTC)
        arguments_payload = (
            arguments.model_dump(mode="python")
            if hasattr(arguments, "model_dump")
            else {}
        )
        try:
            result = self._execute_impl(tool=tool, call=call, arguments=arguments)
        except Exception as exc:
            self.repository.record_tool_access(
                tool_call_id=str(call.tool_call_id),
                workflow_run_id=str(call.workflow_run_id),
                node_name=(str(call.node_name) if getattr(call, "node_name", None) else None),
                tool_name=str(tool.tool_name),
                tool_version=str(tool.tool_version),
                adapter_type=str(tool.adapter_type),
                subject_type=str(call.subject_type),
                subject_id=str(call.subject_id),
                organization_id=str(call.authorization_context.organization_id),
                workspace_id=(
                    str(call.authorization_context.workspace_id)
                    if getattr(call.authorization_context, "workspace_id", None) is not None
                    else None
                ),
                user_id=(
                    str(call.authorization_context.user_id)
                    if getattr(call.authorization_context, "user_id", None) is not None
                    else None
                ),
                role=(
                    str(call.authorization_context.role)
                    if getattr(call.authorization_context, "role", None) is not None
                    else None
                ),
                session_id=(
                    str(call.authorization_context.session_id)
                    if getattr(call.authorization_context, "session_id", None) is not None
                    else None
                ),
                connection_id=(
                    str(call.authorization_context.connection_id)
                    if getattr(call.authorization_context, "connection_id", None) is not None
                    else None
                ),
                execution_status="failed",
                error_code=_tool_error_code(exc),
                arguments_payload=arguments_payload,
                source_locator=None,
                recorded_at=recorded_at,
                completed_at=datetime.now(UTC),
            )
            raise

        provenance = result.get("provenance", {}) if isinstance(result, dict) else {}
        self.repository.record_tool_access(
            tool_call_id=str(call.tool_call_id),
            workflow_run_id=str(call.workflow_run_id),
            node_name=(str(call.node_name) if getattr(call, "node_name", None) else None),
            tool_name=str(tool.tool_name),
            tool_version=str(tool.tool_version),
            adapter_type=str(tool.adapter_type),
            subject_type=str(call.subject_type),
            subject_id=str(call.subject_id),
            organization_id=str(call.authorization_context.organization_id),
            workspace_id=(
                str(call.authorization_context.workspace_id)
                if getattr(call.authorization_context, "workspace_id", None) is not None
                else None
            ),
            user_id=(
                str(call.authorization_context.user_id)
                if getattr(call.authorization_context, "user_id", None) is not None
                else None
            ),
            role=(
                str(call.authorization_context.role)
                if getattr(call.authorization_context, "role", None) is not None
                else None
            ),
            session_id=(
                str(call.authorization_context.session_id)
                if getattr(call.authorization_context, "session_id", None) is not None
                else None
            ),
            connection_id=(
                str(call.authorization_context.connection_id)
                if getattr(call.authorization_context, "connection_id", None) is not None
                else None
            ),
            execution_status=str(result.get("status", "success")),
            error_code=None,
            arguments_payload=arguments_payload,
            source_locator=(
                str(provenance.get("source_locator"))
                if provenance.get("source_locator") is not None
                else None
            ),
            recorded_at=recorded_at,
            completed_at=datetime.now(UTC),
        )
        return result

    def _execute_impl(self, *, tool, call, arguments):
        raise NotImplementedError


class AuditFlowArtifactStoreAdapter(_AuditedToolAdapter):
    def __init__(self, repository: SqlAlchemyAuditFlowRepository) -> None:
        self.repository = repository

    def _execute_impl(self, *, tool, call, arguments):
        with self.repository.session_factory() as session:
            artifact_row = session.get(ArtifactBlobRow, arguments.artifact_id)
            if artifact_row is None:
                raise KeyError(arguments.artifact_id)
            evidence_row = session.scalars(
                select(EvidenceRow)
                .where(
                    or_(
                        EvidenceRow.source_artifact_id == arguments.artifact_id,
                        EvidenceRow.normalized_artifact_id == arguments.artifact_id,
                    )
                )
                .order_by(EvidenceRow.captured_at.desc())
            ).first()
            metadata_payload = (
                dict(artifact_row.metadata_payload)
                if isinstance(artifact_row.metadata_payload, dict)
                else {}
            )
            self._assert_artifact_scope(
                call=call,
                evidence_row=evidence_row,
                metadata_payload=metadata_payload,
            )
            chunk_ids: list[str] = []
            if evidence_row is not None:
                chunk_ids = session.scalars(
                    select(EvidenceChunkRow.chunk_id)
                    .where(EvidenceChunkRow.evidence_id == evidence_row.evidence_id)
                    .order_by(EvidenceChunkRow.chunk_index.asc())
                ).all()
            return {
                "status": "success",
                "normalized_payload": {
                    "artifact_id": artifact_row.artifact_id,
                    "artifact_type": artifact_row.artifact_type,
                    "parser_status": str(metadata_payload.get("parser_status") or "completed"),
                    "text_ref_ids": list(chunk_ids),
                    "metadata": metadata_payload,
                },
                "provenance": {
                    "adapter_type": tool.adapter_type,
                    "fetched_at": _utcnow_iso(),
                    "source_locator": artifact_row.artifact_id,
                },
                "raw_ref": {
                    "artifact_id": artifact_row.artifact_id,
                    "kind": "auditflow_artifact_blob",
                },
                "warnings": [],
            }

    def _assert_artifact_scope(self, *, call, evidence_row, metadata_payload: dict[str, object]) -> None:
        organization_id = str(call.authorization_context.organization_id)
        workspace_id = str(call.authorization_context.workspace_id)
        if evidence_row is not None:
            cycle_scope = self.repository.get_cycle_context(
                evidence_row.audit_cycle_id,
                organization_id=organization_id,
            )
            if cycle_scope["workspace_id"] != workspace_id:
                raise KeyError(evidence_row.audit_cycle_id)
            return
        if metadata_payload.get("organization_id") not in {None, organization_id}:
            raise KeyError(str(metadata_payload.get("organization_id")))
        if metadata_payload.get("workspace_id") not in {None, workspace_id}:
            raise KeyError(str(metadata_payload.get("workspace_id")))


class AuditFlowChunkStoreAdapter(_AuditedToolAdapter):
    def __init__(self, repository: SqlAlchemyAuditFlowRepository) -> None:
        self.repository = repository

    def _execute_impl(self, *, tool, call, arguments):
        with self.repository.session_factory() as session:
            evidence_row = session.scalars(
                select(EvidenceRow)
                .where(
                    or_(
                        EvidenceRow.source_artifact_id == arguments.artifact_id,
                        EvidenceRow.normalized_artifact_id == arguments.artifact_id,
                    )
                )
                .limit(1)
            ).first()
            if evidence_row is None:
                raise KeyError(arguments.chunk_id)
            cycle_scope = self.repository.get_cycle_context(
                evidence_row.audit_cycle_id,
                organization_id=call.authorization_context.organization_id,
            )
            if cycle_scope["workspace_id"] != call.authorization_context.workspace_id:
                raise KeyError(arguments.chunk_id)
            chunk_row = session.get(EvidenceChunkRow, arguments.chunk_id)
            if chunk_row is None or chunk_row.evidence_id != evidence_row.evidence_id:
                raise KeyError(arguments.chunk_id)
            return {
                "status": "success",
                "normalized_payload": {
                    "artifact_id": arguments.artifact_id,
                    "chunk_id": chunk_row.chunk_id,
                    "text": chunk_row.text_excerpt,
                    "locator": {
                        "chunk_index": chunk_row.chunk_index,
                        "section_label": chunk_row.section_label,
                    },
                },
                "provenance": {
                    "adapter_type": tool.adapter_type,
                    "fetched_at": _utcnow_iso(),
                    "source_locator": f"{arguments.artifact_id}/{chunk_row.chunk_id}",
                },
                "warnings": [],
            }


class AuditFlowVectorStoreAdapter(_AuditedToolAdapter):
    def __init__(self, repository: SqlAlchemyAuditFlowRepository) -> None:
        self.repository = repository

    def _execute_impl(self, *, tool, call, arguments):
        if arguments.workspace_id != call.authorization_context.workspace_id:
            raise KeyError(arguments.audit_cycle_id)
        response = self.repository.search_evidence(
            cycle_id=arguments.audit_cycle_id,
            query=arguments.query,
            limit=arguments.limit,
            organization_id=call.authorization_context.organization_id,
            workspace_id=call.authorization_context.workspace_id,
        )
        return {
            "status": "success",
            "normalized_payload": {
                "items": [
                    {
                        "evidence_chunk_id": item.evidence_chunk_id,
                        "evidence_item_id": item.evidence_item_id,
                        "score": item.score,
                        "summary": item.summary,
                    }
                    for item in response.items
                ]
            },
            "provenance": {
                "adapter_type": tool.adapter_type,
                "fetched_at": _utcnow_iso(),
                "source_locator": (
                    f"auditflow://cycles/{arguments.audit_cycle_id}/evidence-search?query={arguments.query}"
                ),
            },
            "warnings": [],
        }


class AuditFlowControlCatalogAdapter(_AuditedToolAdapter):
    def __init__(self, repository: SqlAlchemyAuditFlowRepository) -> None:
        self.repository = repository

    def _execute_impl(self, *, tool, call, arguments):
        del call
        with self.repository.session_factory() as session:
            stmt = select(ControlCatalogRow).where(ControlCatalogRow.is_active.is_(True))
            if arguments.framework_name is not None:
                stmt = stmt.where(ControlCatalogRow.framework_name == arguments.framework_name)
            rows = session.scalars(
                stmt.order_by(ControlCatalogRow.sort_order.asc(), ControlCatalogRow.control_code.asc())
            ).all()
            if arguments.control_ids:
                requested_ids = {str(control_id) for control_id in arguments.control_ids}
                rows = [
                    row
                    for row in rows
                    if row.control_code in requested_ids or row.control_id in requested_ids
                ]
            if arguments.search_query:
                normalized_query = arguments.search_query.strip().lower()
                rows = [
                    row
                    for row in rows
                    if normalized_query in row.control_code.lower()
                    or normalized_query in row.title.lower()
                    or normalized_query in row.description.lower()
                ]
            return {
                "status": "success",
                "normalized_payload": {
                    "controls": [
                        {
                            "control_id": row.control_code,
                            "title": row.title,
                            "objective_text": row.description,
                        }
                        for row in rows
                    ]
                },
                "provenance": {
                    "adapter_type": tool.adapter_type,
                    "fetched_at": _utcnow_iso(),
                    "source_locator": f"auditflow://control-catalog/{arguments.framework_name or 'all'}",
                },
                "warnings": [],
            }


class AuditFlowDatabaseAdapter(_AuditedToolAdapter):
    def __init__(self, repository: SqlAlchemyAuditFlowRepository) -> None:
        self.repository = repository

    def _execute_impl(self, *, tool, call, arguments):
        if tool.tool_name == "mapping.read_candidates":
            return self._mapping_candidates(tool=tool, call=call, arguments=arguments)
        if tool.tool_name == "review_decision.read_history":
            return self._review_history(tool=tool, call=call, arguments=arguments)
        raise ValueError(f"Unsupported auditflow database tool: {tool.tool_name}")

    def _mapping_candidates(self, *, tool, call, arguments):
        with self.repository.session_factory() as session:
            cycle_scope = self.repository.get_cycle_context(
                arguments.audit_cycle_id,
                organization_id=call.authorization_context.organization_id,
            )
            if cycle_scope["workspace_id"] != call.authorization_context.workspace_id:
                raise KeyError(arguments.audit_cycle_id)
            stmt = select(MappingRow).where(MappingRow.cycle_id == arguments.audit_cycle_id)
            if arguments.evidence_item_id is not None:
                stmt = stmt.where(MappingRow.evidence_item_id == arguments.evidence_item_id)
            rows = session.scalars(stmt.order_by(MappingRow.updated_at.desc())).all()
            if arguments.control_id is not None:
                control_id = str(arguments.control_id)
                rows = [
                    row
                    for row in rows
                    if row.control_state_id == control_id or row.control_code == control_id
                ]
            return {
                "status": "success",
                "normalized_payload": {
                    "candidates": [
                        {
                            "mapping_id": row.mapping_id,
                            "control_id": row.control_state_id,
                            "status": row.mapping_status,
                            "ranking_score": self._mapping_ranking_score(row),
                        }
                        for row in rows
                    ]
                },
                "provenance": {
                    "adapter_type": tool.adapter_type,
                    "fetched_at": _utcnow_iso(),
                    "source_locator": f"auditflow://cycles/{arguments.audit_cycle_id}/mappings",
                },
                "warnings": [],
            }

    def _review_history(self, *, tool, call, arguments):
        with self.repository.session_factory() as session:
            cycle_scope = self.repository.get_cycle_context(
                arguments.audit_cycle_id,
                organization_id=call.authorization_context.organization_id,
            )
            if cycle_scope["workspace_id"] != call.authorization_context.workspace_id:
                raise KeyError(arguments.audit_cycle_id)
            stmt = select(ReviewDecisionRow).where(ReviewDecisionRow.cycle_id == arguments.audit_cycle_id)
            if arguments.mapping_id is not None:
                stmt = stmt.where(ReviewDecisionRow.mapping_id == arguments.mapping_id)
            rows = session.scalars(stmt.order_by(ReviewDecisionRow.created_at.desc())).all()
            if arguments.control_id is not None:
                control_id = str(arguments.control_id)
                allowed_mapping_ids = {
                    row.mapping_id
                    for row in session.scalars(
                        select(MappingRow)
                        .where(MappingRow.cycle_id == arguments.audit_cycle_id)
                        .where(
                            or_(
                                MappingRow.control_state_id == control_id,
                                MappingRow.control_code == control_id,
                            )
                        )
                    ).all()
                }
                rows = [row for row in rows if row.mapping_id in allowed_mapping_ids]
            return {
                "status": "success",
                "normalized_payload": {
                    "decisions": [
                        {
                            "review_decision_id": row.review_decision_id,
                            "decision": row.decision,
                            "decided_at": row.created_at,
                        }
                        for row in rows
                    ]
                },
                "provenance": {
                    "adapter_type": tool.adapter_type,
                    "fetched_at": _utcnow_iso(),
                    "source_locator": f"auditflow://cycles/{arguments.audit_cycle_id}/review-decisions",
                },
                "warnings": [],
            }

    @staticmethod
    def _mapping_ranking_score(row: MappingRow) -> float:
        base_score = {
            "accepted": 0.95,
            "reassigned": 0.7,
            "proposed": 0.6,
            "rejected": 0.2,
        }.get(row.mapping_status, 0.4)
        citation_bonus = min(len(row.citation_refs or []) * 0.05, 0.2)
        return round(base_score + citation_bonus, 4)


class AuditFlowSnapshotReaderAdapter(_AuditedToolAdapter):
    def __init__(self, repository: SqlAlchemyAuditFlowRepository) -> None:
        self.repository = repository

    def _execute_impl(self, *, tool, call, arguments):
        cycle_scope = self.repository.get_cycle_context(
            arguments.audit_cycle_id,
            organization_id=call.authorization_context.organization_id,
        )
        if cycle_scope["workspace_id"] != call.authorization_context.workspace_id:
            raise KeyError(arguments.audit_cycle_id)
        snapshot_refs = self.repository.read_snapshot_refs(
            arguments.audit_cycle_id,
            working_snapshot_version=arguments.working_snapshot_version,
            organization_id=call.authorization_context.organization_id,
        )
        return {
            "status": "success",
            "normalized_payload": snapshot_refs,
            "provenance": {
                "adapter_type": tool.adapter_type,
                "fetched_at": _utcnow_iso(),
                "source_locator": (
                    f"auditflow://cycles/{arguments.audit_cycle_id}/snapshots/{arguments.working_snapshot_version}"
                ),
            },
            "warnings": [],
        }


class AuditFlowSnapshotValidatorAdapter(_AuditedToolAdapter):
    def __init__(self, repository: SqlAlchemyAuditFlowRepository) -> None:
        self.repository = repository

    def _execute_impl(self, *, tool, call, arguments):
        cycle_scope = self.repository.get_cycle_context(
            arguments.audit_cycle_id,
            organization_id=call.authorization_context.organization_id,
        )
        if cycle_scope["workspace_id"] != call.authorization_context.workspace_id:
            raise KeyError(arguments.audit_cycle_id)
        dashboard = self.repository.get_cycle_dashboard(
            arguments.audit_cycle_id,
            organization_id=call.authorization_context.organization_id,
        )
        blocker_codes: list[str] = []
        if dashboard.accepted_mapping_count == 0:
            blocker_codes.append("no_accepted_mappings")
        if dashboard.review_queue_count > 0:
            blocker_codes.append("review_queue_open")
        if dashboard.open_gap_count > 0:
            blocker_codes.append("open_gaps")
        if dashboard.cycle.current_snapshot_version != arguments.working_snapshot_version:
            blocker_codes.append("snapshot_stale")
        latest_ready_package = next(
            (
                package
                for package in self.repository.list_export_packages(
                    arguments.audit_cycle_id,
                    organization_id=call.authorization_context.organization_id,
                )
                if package.snapshot_version == arguments.working_snapshot_version
                and package.status in {"queued", "building"}
            ),
            None,
        )
        if latest_ready_package is not None:
            blocker_codes.append("export_running")
        return {
            "status": "success",
            "normalized_payload": {
                "eligible": len(blocker_codes) == 0,
                "blocker_codes": blocker_codes,
                "current_snapshot_version": dashboard.cycle.current_snapshot_version,
            },
            "provenance": {
                "adapter_type": tool.adapter_type,
                "fetched_at": _utcnow_iso(),
                "source_locator": (
                    f"auditflow://cycles/{arguments.audit_cycle_id}/exports/validate/{arguments.working_snapshot_version}"
                ),
            },
            "warnings": [],
        }


def register_auditflow_product_tool_adapters(tool_executor, repository: SqlAlchemyAuditFlowRepository) -> None:
    tool_executor.register_adapter("artifact_store", AuditFlowArtifactStoreAdapter(repository))
    tool_executor.register_adapter("chunk_store", AuditFlowChunkStoreAdapter(repository))
    tool_executor.register_adapter("vector_store", AuditFlowVectorStoreAdapter(repository))
    tool_executor.register_adapter("control_catalog", AuditFlowControlCatalogAdapter(repository))
    tool_executor.register_adapter("auditflow_database", AuditFlowDatabaseAdapter(repository))
    tool_executor.register_adapter("snapshot_reader", AuditFlowSnapshotReaderAdapter(repository))
    tool_executor.register_adapter("snapshot_validator", AuditFlowSnapshotValidatorAdapter(repository))
