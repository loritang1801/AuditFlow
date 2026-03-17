# AuditFlow Implementation Backlog

Last updated: 2026-03-17

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

Status: not started

Missing or partial:

1. Request/response shapes still diverge from the full API contract.
2. Some routes use simplified status handling and no idempotency enforcement.
3. Workspace/cycle write endpoints are incomplete.
4. SSE and async event surfaces are not exposed at the product layer.

### 3. Ingestion and Connectors

Status: partially implemented

Implemented:

1. Upload/Jira/Confluence imports enqueue outbox jobs.
2. Import worker dispatches connector-specific handlers.
3. Duplicate upload and connector sources now collapse at import acceptance time via source fingerprints and stable source keys.
4. Import processing now persists raw/normalized artifact text blobs and multi-chunk evidence rows for downstream review.
5. Upload imports now apply format-aware CSV/JSON/text normalization before chunk materialization.

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

1. No richer reviewer conflict resolution or terminal-state policy coverage.
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

Missing or partial:

1. No strict snapshot freeze/validation rules.
2. No export manifest structure.
3. No artifact packaging step beyond projection data.

### 7. Operations and Platform Integration

Status: not started

Missing or partial:

1. No long-running worker supervision strategy in this repo.
2. No auth/RBAC integration at the product route layer.
3. No SSE forwarding for cycle/workspace live updates.
4. No product-scoped replay/evaluation harness.

## Delivery Order

1. Step 1: workspace/cycle creation plus reusable control-template seeding.
2. Step 2: align core API contract fields and status codes for workspace/cycle/import endpoints.
3. Step 3: replace synthetic import normalization with artifact-backed ingestion inputs.
4. Step 4: add review decision audit records and stronger concurrency handling.
5. Step 5: wire retrieval/memory inputs into mapping and challenge flows.
6. Step 6: tighten snapshot/export invariants and package manifest generation.
7. Step 7: add product-level SSE, auth hooks, and worker supervision.

## Active Step

Step 2 is the current implementation target.
