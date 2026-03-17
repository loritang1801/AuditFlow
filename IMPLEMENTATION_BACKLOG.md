# AuditFlow Implementation Backlog

Last updated: 2026-03-16

This file tracks the gap between the current AuditFlow demo implementation and
the product/design contracts in `PRD.md`, `ARCHITECTURE.md`, `DATABASE.md`,
`API.md`, `WORKFLOW.md`, and `PROMPT_TOOL.md`.

## Current Gap Map

### 1. Domain Bootstrapping

Status: in progress

Missing or partial:

1. Workspace creation is not implemented.
2. Cycle creation is not implemented.
3. Control catalog is still implicit demo data instead of a reusable domain layer.
4. New cycles do not yet inherit a formal control template set.

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

Missing or partial:

1. No real artifact-backed parsing pipeline yet.
2. No OCR/chunking/indexing persistence path yet.
3. External connectors use synthetic payload normalization rather than live pulls.
4. Duplicate detection and fingerprinting are not implemented.

### 4. Reviewer Workflow and Audit Trail

Status: partially implemented

Implemented:

1. Mapping review decisions.
2. Gap decisions.
3. Basic optimistic concurrency checks.

Missing or partial:

1. No dedicated `review_decision` domain record.
2. No richer reviewer conflict resolution or terminal-state policy coverage.
3. Review queue ordering is still simplified.

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

Step 1 is the current implementation target.
