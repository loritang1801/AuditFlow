# AuditFlow Implementation Backlog

Last updated: 2026-03-18

This file tracks the gap between the current AuditFlow demo implementation and
the product/design contracts in `PRD.md`, `ARCHITECTURE.md`, `DATABASE.md`,
`API.md`, `WORKFLOW.md`, and `PROMPT_TOOL.md`.

## Current Gap Map

### 1. Domain Bootstrapping

Status: completed

Implemented:

1. Workspace creation.
2. Cycle creation.
3. SQLAlchemy-backed reusable control catalog seeding.
4. New cycles inherit the active control template set for their framework.

### 2. API Contract Convergence

Status: partially implemented

Missing or partial:

1. Workspace/cycle resource models now carry contract-aligned slug/owner/period/snapshot fields, product read routes plus shared workflow endpoints use shared success envelopes with cursor metadata, but full cross-product parity is still incomplete.
2. Core cycle/import/export routes plus reviewer mapping/gap mutations now enforce persisted idempotency keys, but the remaining mutation surface has not been upgraded yet.
3. Workspace/cycle write endpoints still do not cover the full contract surface beyond create/read plus current query filters.
4. Product SSE is now exposed at `/api/v1/events/stream`, but live coverage is still limited to outbox-backed events and there is still no auth/RBAC integration around the stream.

### 3. Ingestion and Connectors

Status: partially implemented

Implemented:

1. Upload/Jira/Confluence imports enqueue outbox jobs.
2. Import acceptance now emits product-scoped `auditflow.import.accepted` outbox events.
3. Import worker dispatches connector-specific handlers.
4. Duplicate upload and connector sources now collapse at import acceptance time via source fingerprints and stable source keys.
5. Import processing now persists raw/normalized artifact text blobs and multi-chunk evidence rows for downstream review.
6. Upload imports now apply format-aware CSV/JSON/Markdown/HTML/text normalization before chunk materialization.

Missing or partial:

1. No OCR or binary file parser pipeline yet.
2. No embedding/index persistence path yet.
3. External connectors still use synthetic payload capture rather than live pulls.

### 4. Reviewer Workflow and Audit Trail

Status: partially implemented

Implemented:

1. Mapping review decisions.
2. Gap decisions.
3. Basic optimistic concurrency checks.
4. Immutable `review_decision` audit rows appended for mapping and gap decisions.

Missing or partial:

1. Mapping accepted/rejected locks plus gap acknowledge/reopen rules now exist, and reviewer reads/mutations are snapshot-aware, but multi-actor reviewer merge/conflict resolution is still missing.
2. Review queue ordering is still simplified.

### 5. Retrieval, Memory, and Prompt Grounding

Status: not started

Missing or partial:

1. No hybrid retrieval layer wired into the product repository.
2. No organization/cycle memory surfaced from AuditFlow code.
3. Prompt bundles are defined in docs and shared runtime, but product-side
   retrieval inputs are still demo-grade.

### 6. Export and Snapshot Governance

Status: partially implemented

Implemented:

1. Export workflow execution.
2. Narrative row creation.
3. Export package projection.
4. Export manifest structure.
5. Persisted package and manifest artifact packaging.

Missing or partial:

1. Snapshot freeze history plus cycle+snapshot export deduping now exist, but there is still no separate first-class freeze ledger model beyond `audit_package`.

### 7. Operations and Platform Integration

Status: partially implemented

Implemented:

1. Import worker supervision now exists with retry/backoff, idle-stop controls, and heartbeat callbacks/CLI output for long-running polling.
2. Product routes now enforce tenant header plus minimum-role checks through injectable auth/RBAC hooks.
3. Product-scoped replay/evaluation harness now captures the fixed import-to-export demo scenario, emits baseline JSON plus JSON/Markdown comparison reports, and is runnable from `scripts/run_replay_harness.py`.

Missing or partial:

1. Route auth currently uses a local header-based authorizer; shared session/token validation is still not wired through the product layer.
2. SSE forwarding for cycle/workspace live updates now exists, with product events for import acceptance, review recording, and export progress/completion, but event coverage still depends on outbox-backed actions.
3. Replay coverage is still limited to the fixed demo scenario; there is no broader fixture catalog or historical baseline management yet.

## Delivery Order

1. Step 1: workspace/cycle creation plus reusable control-template seeding.
2. Step 2: align core API contract fields and status codes for workspace/cycle/import endpoints.
3. Step 3: replace synthetic import normalization with artifact-backed ingestion inputs.
4. Step 4: add review decision audit records and stronger concurrency handling.
5. Step 5: wire retrieval/memory inputs into mapping and challenge flows.
6. Step 6: tighten snapshot/export invariants and package manifest generation.
7. Step 7: add product-level SSE, auth hooks, and worker supervision.

## Active Step

Step 7 is the current implementation target.
