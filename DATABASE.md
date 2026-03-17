# AuditFlow Database Design

- Version: v0.1
- Date: 2026-03-16
- Scope: `AuditFlow` domain schema on top of the shared platform database

## 1. Database Overview

`AuditFlow` persists audit workspaces, audit cycles, controls, evidence, mappings, gaps, reviewer decisions, narratives, and export packages. All domain tables are tenant-scoped and rely on shared platform tables for auth, workflow, artifact storage, memory, and replay.

### Physical Boundaries

1. Shared platform tables remain in the common application schema.
2. `AuditFlow` tables use the `audit_` prefix except for evidence tables where `evidence_` is clearer.
3. All primary keys use `UUID`.
4. Every table except static catalog rows carries `organization_id`.

## 2. Domain Enumerations

### 2.1 Audit Cycle Status

- `draft`
- `ingesting`
- `mapping`
- `reviewing`
- `ready_for_export`
- `exporting`
- `exported`
- `archived`

### 2.2 Control Coverage Status

- `not_started`
- `needs_evidence`
- `partial`
- `pending_review`
- `covered`
- `at_risk`

### 2.3 Evidence Source Type

- `upload`
- `jira`
- `confluence`
- `manual_note`

### 2.4 Evidence Ingest Status

- `pending`
- `pulling`
- `parsed`
- `failed`
- `superseded`

### 2.5 Evidence Type

- `policy_document`
- `procedure_document`
- `screenshot`
- `ticket`
- `asset_list`
- `log_extract`
- `meeting_note`
- `other`

### 2.6 Mapping Status

- `proposed`
- `accepted`
- `rejected`
- `needs_more_evidence`
- `superseded`

### 2.7 Gap Type

- `missing_evidence`
- `insufficient_coverage`
- `stale_evidence`
- `conflicting_evidence`
- `context_mismatch`

### 2.8 Gap Status

- `open`
- `acknowledged`
- `in_progress`
- `resolved`
- `wont_fix`

### 2.9 Review Decision

- `accept`
- `reassign`
- `reject`
- `needs_more_evidence`
- `resolve_gap`
- `reopen_gap`

### 2.10 Narrative Type

- `control_summary`
- `cycle_summary`
- `gap_summary`
- `export_cover`

### 2.11 Narrative Status

- `draft`
- `final`
- `superseded`

### 2.12 Package Status

- `building`
- `ready`
- `failed`
- `superseded`

## 3. Core Tables

### 3.1 `audit_workspace`

Purpose: `AuditFlow` workspace extension over shared `workspace`.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `workspace_id` | `UUID` | No | PK, FK `workspace.id` |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `default_framework` | `VARCHAR(30)` | No | v1 fixed to `soc2` |
| `default_owner_user_id` | `UUID` | Yes | FK `app_user.id` |
| `workspace_status` | `VARCHAR(30)` | No | `active`, `archived` |
| `settings_json` | `JSONB` | Yes | Defaults for cycle creation |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Indexes:

1. Unique on `workspace_id`
2. Index on `(organization_id, workspace_status)`

### 3.2 `audit_cycle`

Purpose: a single audit preparation period.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_workspace_id` | `UUID` | No | FK `audit_workspace.workspace_id` |
| `cycle_name` | `VARCHAR(200)` | No | User-facing cycle name |
| `framework` | `VARCHAR(30)` | No | v1 `soc2` |
| `audit_period_start` | `DATE` | No | Period start |
| `audit_period_end` | `DATE` | No | Period end |
| `status` | `VARCHAR(30)` | No | Audit cycle status enum |
| `owner_user_id` | `UUID` | Yes | FK `app_user.id` |
| `current_snapshot_version` | `INTEGER` | No | Starts at `1` |
| `last_mapped_at` | `TIMESTAMPTZ` | Yes | Last mapping completion |
| `last_reviewed_at` | `TIMESTAMPTZ` | Yes | Last reviewer action |
| `summary_json` | `JSONB` | Yes | Cached dashboard aggregates |
| `created_by_user_id` | `UUID` | No | FK `app_user.id` |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `archived_at` | `TIMESTAMPTZ` | Yes | Optional archive marker |

Constraints:

1. Check `audit_period_end >= audit_period_start`
2. Unique on `(audit_workspace_id, cycle_name)`

Indexes:

1. Index on `(organization_id, audit_workspace_id, status, created_at DESC)`
2. Index on `(owner_user_id, status)`

### 3.3 `control_catalog`

Purpose: seeded control definitions.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `framework` | `VARCHAR(30)` | No | `soc2` |
| `control_code` | `VARCHAR(80)` | No | Stable external code |
| `domain` | `VARCHAR(80)` | No | Control family/domain |
| `title` | `VARCHAR(255)` | No | Display title |
| `description` | `TEXT` | No | Canonical description |
| `guidance_markdown` | `TEXT` | Yes | Reviewer-facing guidance |
| `common_evidence_json` | `JSONB` | Yes | Example evidence hints |
| `is_active` | `BOOLEAN` | No | Default `true` |
| `sort_order` | `INTEGER` | No | UI ordering |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Constraints:

1. Unique on `(framework, control_code)`

Indexes:

1. Index on `(framework, domain, sort_order)`

### 3.4 `audit_control_state`

Purpose: cycle-specific state for each control.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `control_id` | `UUID` | No | FK `control_catalog.id` |
| `coverage_status` | `VARCHAR(30)` | No | Coverage status enum |
| `risk_level` | `VARCHAR(20)` | No | `low`, `medium`, `high`, `critical` |
| `accepted_mapping_count` | `INTEGER` | No | Cached count |
| `open_gap_count` | `INTEGER` | No | Cached count |
| `last_evaluated_at` | `TIMESTAMPTZ` | Yes | Last rule/agent run |
| `last_reviewed_at` | `TIMESTAMPTZ` | Yes | Last reviewer action |
| `current_snapshot_version` | `INTEGER` | No | Snapshot version backing current state |
| `summary_json` | `JSONB` | Yes | Compact dashboard payload |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Constraints:

1. Unique on `(audit_cycle_id, control_id)`

Indexes:

1. Index on `(audit_cycle_id, coverage_status, risk_level)`
2. Index on `(organization_id, control_id, coverage_status)`

### 3.5 `evidence_source`

Purpose: source-system registration for imported or uploaded evidence.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `source_type` | `VARCHAR(30)` | No | Evidence source type enum |
| `connection_id` | `UUID` | Yes | FK `external_connection.id` |
| `artifact_id` | `UUID` | Yes | FK `artifact.id` for raw upload |
| `upstream_object_id` | `VARCHAR(255)` | Yes | Jira issue key, page id, etc. |
| `source_locator` | `TEXT` | Yes | URL, file path, or provider locator |
| `display_name` | `VARCHAR(255)` | No | Source title/name |
| `ingest_status` | `VARCHAR(30)` | No | Evidence ingest status enum |
| `fingerprint` | `VARCHAR(128)` | Yes | Deduplication hash |
| `captured_at` | `TIMESTAMPTZ` | Yes | Evidence capture time if known |
| `last_synced_at` | `TIMESTAMPTZ` | Yes | Sync marker for external source |
| `metadata_json` | `JSONB` | Yes | Raw source metadata |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Constraints:

1. Unique on `(audit_cycle_id, source_type, upstream_object_id)` when `upstream_object_id` is present

Indexes:

1. Index on `(audit_cycle_id, ingest_status, created_at DESC)`
2. Index on `(organization_id, fingerprint)`

### 3.6 `evidence_item`

Purpose: canonical normalized evidence object used for review and mapping.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `evidence_source_id` | `UUID` | No | FK `evidence_source.id` |
| `source_artifact_id` | `UUID` | Yes | FK `artifact.id`, raw source if file-backed |
| `normalized_artifact_id` | `UUID` | Yes | FK `artifact.id`, extracted text dump |
| `evidence_type` | `VARCHAR(40)` | No | Evidence type enum |
| `title` | `VARCHAR(255)` | No | Derived or source title |
| `summary` | `TEXT` | Yes | Short AI or parser summary |
| `fresh_until` | `TIMESTAMPTZ` | Yes | Freshness cutoff |
| `content_language` | `VARCHAR(20)` | Yes | Default `en`, `zh`, etc. |
| `author_name` | `VARCHAR(255)` | Yes | Optional source author |
| `version_label` | `VARCHAR(80)` | Yes | Version or revision |
| `is_duplicate_of_id` | `UUID` | Yes | FK `evidence_item.id` |
| `parse_status` | `VARCHAR(30)` | No | `pending`, `parsed`, `failed` |
| `captured_at` | `TIMESTAMPTZ` | Yes | Evidence timestamp |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Indexes:

1. Index on `(audit_cycle_id, evidence_type, parse_status)`
2. Index on `(audit_cycle_id, fresh_until)`
3. Index on `(organization_id, evidence_source_id)`

### 3.7 `evidence_chunk`

Purpose: canonical citation unit for evidence.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `evidence_item_id` | `UUID` | No | FK `evidence_item.id` |
| `chunk_index` | `INTEGER` | No | Order within evidence item |
| `text_content` | `TEXT` | No | Canonical chunk text |
| `char_start` | `INTEGER` | Yes | Offset in normalized text |
| `char_end` | `INTEGER` | Yes | Offset in normalized text |
| `page_number` | `INTEGER` | Yes | Document page if known |
| `section_label` | `VARCHAR(255)` | Yes | Heading or region label |
| `embedding_chunk_id` | `UUID` | Yes | FK `embedding_chunk.id` |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |

Constraints:

1. Unique on `(evidence_item_id, chunk_index)`

Indexes:

1. Index on `(audit_cycle_id, evidence_item_id, chunk_index)`
2. Index on `(organization_id, page_number)`

### 3.8 `evidence_mapping`

Purpose: candidate or accepted relationship between one evidence item and one control.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `control_state_id` | `UUID` | No | FK `audit_control_state.id` |
| `evidence_item_id` | `UUID` | No | FK `evidence_item.id` |
| `mapping_status` | `VARCHAR(30)` | No | Mapping status enum |
| `confidence` | `NUMERIC(5,4)` | Yes | AI confidence |
| `ranking_score` | `NUMERIC(8,4)` | Yes | Queue sort hint |
| `rationale` | `TEXT` | Yes | Model or rule explanation |
| `citation_refs_json` | `JSONB` | No | Array of chunk ids and offsets |
| `source_workflow_run_id` | `UUID` | Yes | FK `workflow_run.id` |
| `snapshot_version` | `INTEGER` | No | Version when generated |
| `reviewer_locked` | `BOOLEAN` | No | Prevent auto-supersede after manual decision |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Constraints:

1. Unique on `(control_state_id, evidence_item_id, snapshot_version, source_workflow_run_id)`

Indexes:

1. Index on `(audit_cycle_id, mapping_status, ranking_score DESC)`
2. Index on `(control_state_id, mapping_status, confidence DESC)`
3. Index on `(evidence_item_id, mapping_status)`

### 3.9 `gap_record`

Purpose: unresolved coverage issue for one control in one cycle.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `control_state_id` | `UUID` | No | FK `audit_control_state.id` |
| `related_mapping_id` | `UUID` | Yes | FK `evidence_mapping.id` |
| `gap_type` | `VARCHAR(40)` | No | Gap type enum |
| `severity` | `VARCHAR(20)` | No | `low`, `medium`, `high`, `critical` |
| `status` | `VARCHAR(30)` | No | Gap status enum |
| `title` | `VARCHAR(255)` | No | UI summary |
| `description` | `TEXT` | Yes | Detailed explanation |
| `recommended_action` | `TEXT` | Yes | Suggested next step |
| `owner_hint_user_id` | `UUID` | Yes | FK `app_user.id` |
| `detected_by_run_id` | `UUID` | Yes | FK `workflow_run.id` |
| `resolved_by_user_id` | `UUID` | Yes | FK `app_user.id` |
| `resolved_at` | `TIMESTAMPTZ` | Yes | When terminal resolved/wont_fix |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Indexes:

1. Index on `(audit_cycle_id, status, severity, created_at DESC)`
2. Index on `(control_state_id, status, severity)`
3. Index on `(owner_hint_user_id, status)`

### 3.10 `review_decision`

Purpose: immutable reviewer action log.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `mapping_id` | `UUID` | Yes | FK `evidence_mapping.id` |
| `gap_id` | `UUID` | Yes | FK `gap_record.id` |
| `decision` | `VARCHAR(40)` | No | Review decision enum |
| `from_status` | `VARCHAR(30)` | Yes | Previous status |
| `to_status` | `VARCHAR(30)` | Yes | New status |
| `reviewer_id` | `UUID` | No | FK `app_user.id` |
| `comment` | `TEXT` | Yes | Reviewer explanation |
| `feedback_tags` | `TEXT[]` | Yes | Structured labels |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |

Constraints:

1. At least one of `mapping_id` or `gap_id` must be non-null

Indexes:

1. Index on `(audit_cycle_id, reviewer_id, created_at DESC)`
2. Index on `(mapping_id, created_at DESC)`
3. Index on `(gap_id, created_at DESC)`

### 3.11 `audit_narrative`

Purpose: generated written output for one control or one cycle.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `control_state_id` | `UUID` | Yes | FK `audit_control_state.id`, null for cycle summary |
| `narrative_type` | `VARCHAR(40)` | No | Narrative type enum |
| `status` | `VARCHAR(30)` | No | Narrative status enum |
| `snapshot_version` | `INTEGER` | No | Snapshot basis |
| `content_markdown` | `TEXT` | No | Rendered narrative |
| `source_workflow_run_id` | `UUID` | Yes | FK `workflow_run.id` |
| `artifact_id` | `UUID` | Yes | FK `artifact.id` if exported/stored separately |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Indexes:

1. Index on `(audit_cycle_id, narrative_type, snapshot_version)`
2. Index on `(control_state_id, status, created_at DESC)`

### 3.12 `audit_package`

Purpose: immutable export snapshot for external sharing.

| Column | Type | Null | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | No | PK |
| `organization_id` | `UUID` | No | FK `organization.id` |
| `audit_cycle_id` | `UUID` | No | FK `audit_cycle.id` |
| `snapshot_version` | `INTEGER` | No | Frozen version exported |
| `status` | `VARCHAR(30)` | No | Package status enum |
| `package_artifact_id` | `UUID` | Yes | FK `artifact.id` |
| `manifest_json` | `JSONB` | No | Included narratives, evidence refs, gaps |
| `generated_by_user_id` | `UUID` | Yes | FK `app_user.id` |
| `source_workflow_run_id` | `UUID` | Yes | FK `workflow_run.id` |
| `immutable_at` | `TIMESTAMPTZ` | Yes | Set when ready |
| `created_at` | `TIMESTAMPTZ` | No | Default `now()` |
| `updated_at` | `TIMESTAMPTZ` | No | Default `now()` |

Constraints:

1. Unique on `(audit_cycle_id, snapshot_version)`

Indexes:

1. Index on `(audit_cycle_id, status, created_at DESC)`

## 4. Relationship Rules

### 4.1 Workspace and Cycle

1. One `audit_workspace` has many `audit_cycle`.
2. One `audit_cycle` has one row in `audit_control_state` for every in-scope control.
3. `audit_cycle.current_snapshot_version` is the source of truth for exportable review state.

### 4.2 Evidence and Artifacts

1. `evidence_source.artifact_id` stores raw upload when source is file-based.
2. `evidence_item.normalized_artifact_id` stores extracted full text or OCR dump if persisted externally.
3. `evidence_chunk` is the citation unit.
4. `embedding_chunk` indexes `evidence_chunk`; it does not replace it.

### 4.3 Mapping and Review

1. One `evidence_item` can map to many controls.
2. One `audit_control_state` can have many mappings and many gaps.
3. Reviewer actions never overwrite decision history; they append `review_decision`.
4. Accepted mapping state is materialized back into `audit_control_state`.

### 4.4 Narrative and Export

1. `audit_narrative` is generated from accepted mappings only.
2. `audit_package` references a frozen `snapshot_version`.
3. Re-exporting a changed cycle requires a new snapshot version, not mutation of an old package.

## 5. Query Patterns and Index Intent

These queries must remain index-backed:

1. Review queue by cycle ordered by severity, status, and ranking score
2. Control coverage matrix for one cycle
3. Stale evidence search by `fresh_until`
4. Evidence lookup by source fingerprint or upstream object id
5. Open gaps by cycle and owner hint
6. Latest narrative and package by cycle

## 6. Consistency Rules

1. `audit_control_state.coverage_status` is derived but persisted for fast reads.
2. `accepted_mapping_count` and `open_gap_count` are maintained transactionally when review decisions land.
3. `reviewer_locked = true` prevents background mapping runs from silently superseding a human-accepted mapping.
4. `gap_record.status = resolved` requires `resolved_at`.
5. Export generation reads a fixed `snapshot_version`; no live rows can change what is included mid-build.

## 7. Transaction Boundaries

### 7.1 Evidence Ingestion

Single transaction:

1. Insert or update `evidence_source`
2. Insert raw `artifact` if upload-based
3. Insert `outbox_event` for parse/index jobs

### 7.2 Parse Completion

Single transaction:

1. Update `evidence_source.ingest_status`
2. Insert `evidence_item`
3. Insert `evidence_chunk` rows
4. Insert shared `embedding_chunk` rows or queue embedding jobs

### 7.3 Reviewer Decision

Single transaction:

1. Update target `evidence_mapping` or `gap_record`
2. Append `review_decision`
3. Recompute impacted `audit_control_state`
4. Insert `feedback_event`
5. Insert `outbox_event` for downstream narrative refresh

### 7.4 Package Build

Single transaction to freeze snapshot metadata:

1. Increment or validate `audit_cycle.current_snapshot_version`
2. Insert `audit_package` row with `building`
3. Insert `outbox_event` for build worker

Separate completion transaction:

1. Attach `package_artifact_id`
2. Update `audit_package.status` to `ready`
3. Set `immutable_at`

## 8. Migration and ORM Notes

1. Seed `control_catalog` via migration-managed seed data.
2. Use composite indexes rather than ORM filtering assumptions for review queue performance.
3. Store large extracted text outside the main row if evidence volume grows; v1 allows `normalized_artifact_id`.
4. Treat `summary_json` fields as cacheable projections, not source of truth.

## 9. Implementation Order

1. `audit_workspace`, `audit_cycle`, `control_catalog`, `audit_control_state`
2. `evidence_source`, `evidence_item`, `evidence_chunk`
3. `evidence_mapping`, `gap_record`, `review_decision`
4. `audit_narrative`, `audit_package`
