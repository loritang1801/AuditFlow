# AuditFlow API and Event Contracts

- Version: v0.1
- Date: 2026-03-18
- Scope: `AuditFlow` REST, SSE, and async event contracts

## 1. Contract Summary

This document defines the implementation-grade interface for `AuditFlow`:

1. Audit workspace and cycle APIs
2. Evidence import, evidence detail, and retrieval/memory inspection APIs
3. Control coverage and review queue APIs
4. Gap, narrative, and export APIs
5. AuditFlow-specific SSE and outbox events

Shared authentication, response envelope, and approval APIs are defined in the shared platform contract.

## 2. Domain Design Rules

1. All APIs are tenant-scoped and require `X-Organization-Id`
2. All user-visible conclusions must return citation references
3. Reviewer actions are optimistic-concurrency protected
4. Export actions are snapshot-bound and idempotent
5. Heavy imports and exports always return `202 Accepted` and continue in background workflow

## 3. Domain Resource Shapes

### 3.1 `AuditWorkspaceSummary`

```json
{
  "id": "uuid",
  "name": "Acme SOC2",
  "slug": "acme-soc2",
  "default_framework": "soc2",
  "workspace_status": "active",
  "default_owner_user_id": "uuid",
  "created_at": "2026-03-16T09:00:00Z"
}
```

### 3.2 `AuditCycleSummary`

```json
{
  "id": "uuid",
  "workspace_id": "uuid",
  "cycle_name": "SOC2 2026",
  "framework": "soc2",
  "status": "reviewing",
  "audit_period_start": "2026-01-01",
  "audit_period_end": "2026-12-31",
  "owner_user_id": "uuid",
  "current_snapshot_version": 3,
  "last_mapped_at": "2026-03-16T09:00:00Z",
  "last_reviewed_at": "2026-03-16T10:00:00Z"
}
```

### 3.3 `ControlMatrixRow`

```json
{
  "control_state_id": "uuid",
  "control_code": "CC6.1",
  "title": "Logical access controls",
  "coverage_status": "pending_review",
  "risk_level": "high",
  "accepted_mapping_count": 2,
  "open_gap_count": 1,
  "last_reviewed_at": "2026-03-16T10:00:00Z"
}
```

### 3.4 `EvidenceDetail`

```json
{
  "id": "uuid",
  "audit_cycle_id": "uuid",
  "title": "Jira Access Review Ticket",
  "evidence_type": "ticket",
  "parse_status": "parsed",
  "fresh_until": "2026-06-01T00:00:00Z",
  "captured_at": "2026-03-10T09:00:00Z",
  "summary": "Quarterly access review completed.",
  "source": {
    "source_type": "jira",
    "source_locator": "https://jira.example.com/browse/SEC-123"
  },
  "chunks": [
    {
      "chunk_id": "uuid",
      "chunk_index": 0,
      "page_number": null,
      "section_label": "Description",
      "text_excerpt": "Quarterly access review..."
    }
  ]
}
```

### 3.4.1 `EvidenceSearchResponse`

```json
{
  "cycle_id": "uuid",
  "workspace_id": "uuid",
  "query": "access review",
  "total_count": 2,
  "items": [
    {
      "evidence_chunk_id": "uuid",
      "evidence_item_id": "uuid",
      "score": 1.82,
      "summary": "Quarterly access review completed.",
      "title": "Jira Access Review Ticket",
      "section_label": "Description",
      "text_excerpt": "Quarterly access review completed for production systems."
    }
  ]
}
```

### 3.4.2 `MemoryRecordSummary`

```json
{
  "memory_id": "uuid",
  "scope": "organization",
  "subject_type": "framework_control",
  "subject_id": "SOC2:CC6.1",
  "memory_key": "mapping:mapping-1",
  "memory_type": "pattern",
  "value": {
    "decision": "accept",
    "control_code": "CC6.1"
  },
  "confidence": 1.0,
  "source_kind": "human_feedback",
  "status": "active",
  "created_at": "2026-03-18T09:00:00Z",
  "updated_at": "2026-03-18T09:00:00Z"
}
```

### 3.5 `ReviewQueueItem`

```json
{
  "mapping_id": "uuid",
  "control_state_id": "uuid",
  "control_code": "CC6.1",
  "coverage_status": "pending_review",
  "snapshot_version": 3,
  "evidence_id": "uuid",
  "confidence": 0.84,
  "ranking_score": 0.91,
  "rationale_summary": "Evidence references user access review completion.",
  "claimed_by_user_id": "user-reviewer-1",
  "claimed_at": "2026-03-16T09:00:00Z",
  "claim_expires_at": "2026-03-16T09:15:00Z",
  "claim_status": "claimed_by_me",
  "citation_refs": [
    {
      "chunk_id": "uuid",
      "char_start": 0,
      "char_end": 120
    }
  ],
  "updated_at": "2026-03-16T09:00:00Z"
}
```

### 3.6 `GapSummary`

```json
{
  "id": "uuid",
  "control_state_id": "uuid",
  "gap_type": "stale_evidence",
  "severity": "high",
  "status": "open",
  "snapshot_version": 3,
  "title": "Access review evidence is older than policy threshold",
  "recommended_action": "Upload the latest quarterly access review artifact."
}
```

### 3.7 `AuditPackageSummary`

```json
{
  "id": "uuid",
  "audit_cycle_id": "uuid",
  "snapshot_version": 3,
  "status": "ready",
  "package_artifact_id": "uuid",
  "created_at": "2026-03-16T10:00:00Z",
  "immutable_at": "2026-03-16T10:01:00Z"
}
```

## 4. REST API

### 4.1 `POST /api/v1/auditflow/workspaces`

Purpose: create an AuditFlow workspace.

Auth: `product_admin`

Request body:

```json
{
  "name": "Acme SOC2",
  "slug": "acme-soc2",
  "default_framework": "soc2",
  "default_owner_user_id": "uuid",
  "settings": {
    "freshness_days_default": 90
  }
}
```

Response:

- `201 Created` with `AuditWorkspaceSummary`

Errors:

- `WORKSPACE_SLUG_ALREADY_EXISTS`
- `VALIDATION_ERROR`

### 4.2 `GET /api/v1/auditflow/workspaces/:workspaceId`

Purpose: fetch one workspace.

Auth: `viewer`

Response:

- `200 OK` with `AuditWorkspaceSummary`

### 4.3 `POST /api/v1/auditflow/cycles`

Purpose: create a new audit cycle.

Auth: `reviewer` or stronger

Headers:

- `Idempotency-Key` required

Request body:

```json
{
  "workspace_id": "uuid",
  "cycle_name": "SOC2 2026",
  "audit_period_start": "2026-01-01",
  "audit_period_end": "2026-12-31",
  "owner_user_id": "uuid"
}
```

Response:

- `201 Created` with `AuditCycleSummary`

### 4.4 `GET /api/v1/auditflow/cycles`

Purpose: list audit cycles in one workspace.

Auth: `viewer`

Query params:

- `workspace_id` required
- `status`
- `cursor`
- `limit`

Response:

- `200 OK` with paginated `AuditCycleSummary[]`

### 4.5 `GET /api/v1/auditflow/cycles/:cycleId/dashboard`

Purpose: fetch cycle-level dashboard aggregates.

Auth: `viewer`

Response:

- `200 OK`

```json
{
  "data": {
    "cycle": {
      "id": "uuid",
      "status": "reviewing",
      "current_snapshot_version": 3
    },
    "counts": {
      "controls_total": 64,
      "controls_covered": 30,
      "controls_pending_review": 12,
      "open_gaps": 9,
      "evidence_items": 117
    },
    "updated_at": "2026-03-16T10:00:00Z"
  }
}
```

### 4.6 `POST /api/v1/auditflow/cycles/:cycleId/imports/upload`

Purpose: upload one raw evidence file.

Auth: `reviewer` or stronger

Headers:

- `Idempotency-Key` required
- `Content-Type: multipart/form-data`

Multipart fields:

- `file` required
- `display_name` optional
- `captured_at` optional
- `evidence_type_hint` optional

Response:

- `202 Accepted`

```json
{
  "data": {
    "evidence_source_id": "uuid",
    "artifact_id": "uuid",
    "ingest_status": "pending"
  },
  "meta": {
    "workflow_run_id": "uuid",
    "request_id": "req_123"
  }
}
```

Errors:

- `FILE_TOO_LARGE`
- `UNSUPPORTED_MEDIA_TYPE`
- `IDEMPOTENCY_CONFLICT`

### 4.7 `POST /api/v1/auditflow/cycles/:cycleId/imports/external`

Purpose: trigger import from Jira or Confluence.

Auth: `reviewer` or stronger

Headers:

- `Idempotency-Key` required

Request body:

```json
{
  "connection_id": "uuid",
  "provider": "jira",
  "upstream_ids": ["SEC-123", "SEC-124"],
  "query": null
}
```

Rules:

1. Exactly one of `upstream_ids` or `query` must be supplied
2. `provider` must match the referenced connection
3. Import workers may resolve the external source through env-configured live connector fetch; otherwise they fall back to synthetic normalization using the requested selector/query

Response:

- `202 Accepted`

```json
{
  "data": {
    "accepted_count": 2
  },
  "meta": {
    "workflow_run_id": "uuid"
  }
}
```

### 4.7.1 `GET /api/v1/auditflow/runtime-capabilities`

Purpose: inspect the effective runtime modes currently active for model provider, embedding provider, vector search, and external connectors.

Auth: `product_admin` or stronger

Response:

- `200 OK`

```json
{
  "data": {
    "product": "auditflow",
    "model_provider": {
      "requested_mode": "auto",
      "effective_mode": "local",
      "backend_id": "heuristic-local",
      "fallback_reason": "MODEL_PROVIDER_NOT_CONFIGURED",
      "details": {
        "configured_model": null,
        "fallback_enabled": true
      }
    },
    "embedding_provider": {
      "requested_mode": "auto",
      "effective_mode": "local",
      "backend_id": "semantic-v1",
      "fallback_reason": "OPENAI_EMBEDDING_NOT_CONFIGURED",
      "details": {
        "vector_dimension": 96
      }
    },
    "vector_search": {
      "requested_mode": "auto",
      "effective_mode": "ann",
      "backend_id": "ann-metadata-json",
      "fallback_reason": null,
      "details": {
        "dialect_name": "sqlite",
        "semantic_candidate_limit": 64
      }
    },
    "connectors": {
      "jira": {
        "requested_mode": "auto",
        "effective_mode": "local",
        "backend_id": "jira-synthetic",
        "fallback_reason": "CONNECTOR_HTTP_TEMPLATE_NOT_CONFIGURED",
        "details": {
          "has_url_template": false
        }
      }
    }
  }
}
```

Errors:

- `SOURCE_AUTH_REQUIRED`
- `SOURCE_PROVIDER_MISMATCH`
- `IMPORT_TARGET_NOT_FOUND`

### 4.8 `GET /api/v1/auditflow/cycles/:cycleId/imports`

Purpose: list import sources and statuses for one cycle.

Auth: `viewer`

Query params:

- `status`
- `source_type`
- `cursor`
- `limit`

Response:

- `200 OK`

```json
{
  "data": [
    {
      "evidence_source_id": "uuid",
      "source_type": "jira",
      "display_name": "SEC-123",
      "ingest_status": "parsed",
      "captured_at": "2026-03-10T09:00:00Z",
      "last_synced_at": "2026-03-16T09:00:00Z"
    }
  ]
}
```

### 4.9 `GET /api/v1/auditflow/cycles/:cycleId/controls`

Purpose: fetch the control coverage matrix.

Auth: `viewer`

Query params:

- `coverage_status`
- `risk_level`
- `search`
- `cursor`
- `limit`

Response:

- `200 OK` with paginated `ControlMatrixRow[]`

### 4.9.1 `GET /api/v1/auditflow/cycles/:cycleId/mappings`

Purpose: list evidence mappings for one cycle with optional control/status filters.

Auth: `reviewer` or stronger

Query params:

- `control_state_id`
- `mapping_status`

Response:

- `200 OK` with `MappingSummary[]`

### 4.10 `GET /api/v1/auditflow/cycles/:cycleId/controls/:controlStateId`

Purpose: fetch one control state with accepted mappings, pending mappings, and open gaps.

Auth: `viewer`

Response:

- `200 OK`

```json
{
  "data": {
    "control_state": {
      "id": "uuid",
      "control_code": "CC6.1",
      "coverage_status": "pending_review"
    },
    "accepted_mappings": [],
    "pending_mappings": [],
    "open_gaps": []
  }
}
```

### 4.11 `GET /api/v1/auditflow/evidence/:evidenceId`

Purpose: fetch evidence detail and citation-ready chunks.

Auth: `viewer`

Response:

- `200 OK` with `EvidenceDetail`

### 4.11.A `GET /api/v1/auditflow/cycles/:cycleId/evidence-search`

Purpose: search indexed evidence chunks within one cycle.

Auth: `viewer`

Query params:

- `query` required
- `limit`

Response:

- `200 OK` with `EvidenceSearchResponse`

Errors:

- `INVALID_SEARCH_QUERY`

### 4.11.B `GET /api/v1/auditflow/cycles/:cycleId/memory-records`

Purpose: inspect product-side retrieval memory derived from reviewer outcomes.

Auth: `reviewer` or stronger

Query params:

- `scope`
- `subject_type`
- `subject_id`
- `memory_type`
- `status`
- `cursor`
- `limit`

Response:

- `200 OK` with paginated `MemoryRecordSummary[]`

### 4.11.1 `GET /api/v1/auditflow/cycles/:cycleId/gaps`

Purpose: list gap records for one cycle with optional status/severity filters.

Auth: `reviewer` or stronger

Query params:

- `status`
- `severity`

Response:

- `200 OK` with `GapSummary[]`

### 4.12 `GET /api/v1/auditflow/review-queue`

Purpose: list pending review work for one cycle.

Auth: `reviewer` or stronger

Query params:

- `cycle_id` required
- `control_state_id`
- `severity`
- `claim_state=unclaimed|claimed_by_me|claimed_by_other`
- `sort=ranking|recent|claim`
- `cursor`
- `limit`

Response:

- `200 OK` with paginated `ReviewQueueItem[]`

### 4.12.1 `GET /api/v1/auditflow/cycles/:cycleId/review-decisions`

Purpose: list immutable reviewer decisions already recorded for one cycle.

Auth: `reviewer` or stronger

Query params:

- `mapping_id`
- `gap_id`

Response:

- `200 OK` with `ReviewDecision[]`

### 4.13 `POST /api/v1/auditflow/mappings/:mappingId/review`

Purpose: apply a reviewer decision to one mapping.

Auth: `reviewer` or stronger

Headers:

- `Idempotency-Key` required

Request body:

```json
{
  "decision": "accept",
  "comment": "Citation is sufficient.",
  "target_control_id": null,
  "expected_snapshot_version": 3,
  "expected_updated_at": "2026-03-16T09:00:00Z"
}
```

Rules:

1. `target_control_id` is required only when `decision = reassign`
2. `expected_updated_at` is required for optimistic concurrency
3. If supplied, `expected_snapshot_version` must match the mapping's stored snapshot and the cycle's current snapshot
4. If another reviewer holds an active claim on the mapping, the review is rejected with `REVIEW_CLAIM_CONFLICT`

Response:

- `200 OK`

```json
{
  "data": {
    "mapping_id": "uuid",
    "mapping_status": "accepted",
    "control_state": {
      "id": "uuid",
      "coverage_status": "covered",
      "accepted_mapping_count": 3,
      "open_gap_count": 0
    }
  }
}
```

Errors:

- `MAPPING_REVIEW_CONFLICT`
- `MAPPING_ALREADY_TERMINAL`
- `REVIEW_CLAIM_CONFLICT`
- `TARGET_CONTROL_NOT_FOUND`

### 4.13.1 `POST /api/v1/auditflow/mappings/:mappingId/claim`

Purpose: claim one review-queue mapping for a reviewer lease window.

Auth: `reviewer` or stronger

Headers:

- `Idempotency-Key` required

Request body:

```json
{
  "lease_seconds": 900,
  "expected_updated_at": "2026-03-16T09:00:00Z"
}
```

Response:

- `200 OK`

```json
{
  "data": {
    "mapping_id": "uuid",
    "mapping_status": "proposed",
    "claimed_by_user_id": "user-reviewer-1",
    "claimed_at": "2026-03-16T09:00:00Z",
    "claim_expires_at": "2026-03-16T09:15:00Z",
    "claim_status": "claimed_by_me"
  }
}
```

Errors:

- `REVIEW_CLAIM_CONFLICT`
- `MAPPING_ALREADY_TERMINAL`

### 4.13.2 `POST /api/v1/auditflow/mappings/:mappingId/claim/release`

Purpose: release an active reviewer claim so another reviewer can pick up the mapping.

Auth: `reviewer` or stronger

Headers:

- `Idempotency-Key` required

Request body:

```json
{
  "expected_updated_at": "2026-03-16T09:00:00Z"
}
```

Response:

- `200 OK`

```json
{
  "data": {
    "mapping_id": "uuid",
    "mapping_status": "proposed",
    "claimed_by_user_id": null,
    "claimed_at": null,
    "claim_expires_at": null,
    "claim_status": "unclaimed"
  }
}
```

### 4.14 `POST /api/v1/auditflow/gaps/:gapId/decision`

Purpose: resolve, reopen, or acknowledge a gap.

Auth: `reviewer` or stronger

Headers:

- `Idempotency-Key` required

Request body:

```json
{
  "decision": "resolve_gap",
  "comment": "New evidence uploaded under SEC-125.",
  "expected_snapshot_version": 3,
  "expected_updated_at": "2026-03-16T09:00:00Z"
}
```

Allowed decisions:

- `resolve_gap`
- `reopen_gap`
- `acknowledge`

Rules:

1. `acknowledge` is only valid when the current gap status is `open`
2. `reopen_gap` is only valid when the current gap status is `resolved`
3. If supplied, `expected_snapshot_version` must match the gap's stored snapshot and the cycle's current snapshot

Response:

- `200 OK` with updated `GapSummary`

Errors:

- `GAP_STATUS_CONFLICT`
- `CONFLICT_STALE_RESOURCE`

### 4.15 `GET /api/v1/auditflow/cycles/:cycleId/narratives`

Purpose: list narratives for one cycle and snapshot.

Auth: `viewer`

Query params:

- `snapshot_version`
- `narrative_type`

Response:

- `200 OK`

```json
{
  "data": [
    {
      "id": "uuid",
      "narrative_type": "control_summary",
      "status": "draft",
      "control_state_id": "uuid",
      "snapshot_version": 3
    }
  ]
}
```

### 4.16 `GET /api/v1/auditflow/cycles/:cycleId/exports`

Purpose: list immutable export/freeze records for one cycle.

Auth: `viewer`

Query params:

- `snapshot_version`
- `status`
- `cursor`
- `limit`

Response:

- `200 OK` with paginated `AuditPackageSummary[]`

### 4.17 `POST /api/v1/auditflow/cycles/:cycleId/exports`

Purpose: create an export package for a fixed snapshot.

Auth: `reviewer` or stronger

Headers:

- `Idempotency-Key` required

Request body:

```json
{
  "snapshot_version": 3,
  "format": "zip"
}
```

Rules:

1. If `snapshot_version` is omitted, the server uses `current_snapshot_version`
2. Export always freezes to one snapshot version at submission time
3. Repeating the same cycle + snapshot request returns the existing immutable package once that freeze is ready

Response:

- `202 Accepted`

```json
{
  "data": {
    "package_id": "uuid",
    "status": "building",
    "snapshot_version": 3
  },
  "meta": {
    "workflow_run_id": "uuid"
  }
}
```

Errors:

- `EXPORT_ALREADY_RUNNING`
- `SNAPSHOT_STALE`
- `CYCLE_NOT_READY_FOR_EXPORT`

### 4.18 `GET /api/v1/auditflow/exports/:packageId`

Purpose: fetch export package status and artifact ref.

Auth: `viewer`

Response:

- `200 OK` with `AuditPackageSummary`

## 5. SSE Contract

### 5.1 Topics

Supported `AuditFlow` topics:

1. `auditflow.workspace.{workspaceId}`
2. `auditflow.cycle.{cycleId}`
3. `auditflow.export.{packageId}`

### 5.2 Event Types

#### `auditflow.import.progress`

Payload:

```json
{
  "cycle_id": "uuid",
  "evidence_source_id": "uuid",
  "ingest_status": "parsed"
}
```

#### `auditflow.mapping.progress`

Payload:

```json
{
  "cycle_id": "uuid",
  "mapped_controls": 32,
  "pending_review_count": 11
}
```

#### `auditflow.review_queue.updated`

Payload:

```json
{
  "cycle_id": "uuid",
  "pending_count": 11,
  "updated_control_state_id": "uuid"
}
```

#### `auditflow.export.progress`

Payload:

```json
{
  "package_id": "uuid",
  "status": "building",
  "snapshot_version": 3
}
```

## 6. Async Event Contract

### 6.1 Event Types

#### `auditflow.import.accepted`

Producer: import API

Payload:

```json
{
  "cycle_id": "uuid",
  "evidence_source_id": "uuid",
  "source_type": "upload"
}
```

#### `auditflow.evidence.normalized`

Producer: normalization worker

Payload:

```json
{
  "cycle_id": "uuid",
  "evidence_item_id": "uuid",
  "evidence_type": "ticket",
  "parse_status": "parsed"
}
```

#### `auditflow.mapping.generated`

Producer: mapping worker

Payload:

```json
{
  "cycle_id": "uuid",
  "control_state_id": "uuid",
  "mapping_id": "uuid",
  "confidence": 0.84
}
```

#### `auditflow.mapping.flagged`

Producer: challenge worker

Payload:

```json
{
  "cycle_id": "uuid",
  "mapping_id": "uuid",
  "reason": "conflicting_evidence"
}
```

#### `auditflow.gap.detected`

Producer: gap worker

Payload:

```json
{
  "cycle_id": "uuid",
  "gap_id": "uuid",
  "gap_type": "stale_evidence",
  "severity": "high"
}
```

#### `auditflow.review.recorded`

Producer: review API

Payload:

```json
{
  "cycle_id": "uuid",
  "review_decision_id": "uuid",
  "mapping_id": "uuid",
  "decision": "accept"
}
```

#### `auditflow.package.ready`

Producer: export worker

Payload:

```json
{
  "cycle_id": "uuid",
  "package_id": "uuid",
  "snapshot_version": 3,
  "artifact_id": "uuid"
}
```

## 7. Error Codes

| Code | HTTP | Meaning |
| --- | --- | --- |
| `AUDIT_WORKSPACE_NOT_FOUND` | `404` | Workspace missing or hidden |
| `AUDIT_CYCLE_NOT_FOUND` | `404` | Cycle missing or hidden |
| `CONTROL_STATE_NOT_FOUND` | `404` | Control state not found |
| `EVIDENCE_NOT_FOUND` | `404` | Evidence item not found |
| `INVALID_SEARCH_QUERY` | `400` | Retrieval query is blank or invalid |
| `SOURCE_AUTH_REQUIRED` | `409` | External connector needs re-auth |
| `IMPORT_TARGET_NOT_FOUND` | `404` | Upstream object missing |
| `EVIDENCE_PARSE_FAILED` | `422` | Raw file could not be normalized |
| `MAPPING_REVIEW_CONFLICT` | `409` | Mapping changed since UI last read |
| `GAP_STATUS_CONFLICT` | `409` | Gap action invalid for current status |
| `SNAPSHOT_STALE` | `409` | Requested snapshot is not current or no longer exportable |
| `EXPORT_ALREADY_RUNNING` | `409` | Same cycle/snapshot export already queued |
| `CYCLE_NOT_READY_FOR_EXPORT` | `422` | Cycle state blocks export |

## 8. Authorization Matrix

| Endpoint Family | Minimum Role |
| --- | --- |
| Workspace/cycle read | `viewer` |
| Import upload/external | `reviewer` |
| Control/evidence read | `viewer` |
| Review queue read | `reviewer` |
| Mapping review | `reviewer` |
| Gap decision | `reviewer` |
| Export create | `reviewer` |
| Export download via artifact | `viewer` |

## 9. Mapping to Data Model

1. Workspace endpoints map to `audit_workspace`
2. Cycle endpoints map to `audit_cycle`
3. Import endpoints map to `evidence_source`, `artifact`, `workflow_run`
4. Evidence detail maps to `evidence_item`, `evidence_chunk`
5. Review queue maps to `evidence_mapping`, `audit_control_state`, `gap_record`
6. Review actions append `review_decision` and update `evidence_mapping` / `gap_record`
7. Narrative endpoints map to `audit_narrative`
8. Export endpoints map to `audit_package` and shared `artifact`
