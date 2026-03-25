# AuditFlow Implementation Backlog

Last updated: 2026-03-20

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
4. Product SSE is now exposed at `/api/v1/events/stream`, now validates tenant workspace access through the route auth context, but live coverage is still limited to outbox-backed events.

### 3. Ingestion and Connectors

Status: partially implemented

Implemented:

1. Upload/Jira/Confluence imports enqueue outbox jobs.
2. Import acceptance now emits product-scoped `auditflow.import.accepted` outbox events.
3. Import worker dispatches connector-specific handlers.
4. Duplicate upload and connector sources now collapse at import acceptance time via source fingerprints and stable source keys.
5. Import processing now persists raw/normalized artifact text blobs and multi-chunk evidence rows for downstream review.
6. Upload imports now apply format-aware CSV/JSON/Markdown/HTML/text normalization before chunk materialization.
7. Upload imports now also accept base64-backed PDF/image/DOCX/XLSX/ZIP payloads and apply heuristic text extraction with binary parser metadata.

Missing or partial:

1. Binary parsing still lacks production-grade OCR plus broader DOC/XLS/PPT/archive coverage beyond the current heuristic ZIP/OpenXML support.
2. External connectors now support env-configured live HTTP fetch with automatic synthetic fallback, but they still rely on URL-template configuration rather than first-class provider SDK/auth/session management.

### 4. Reviewer Workflow and Audit Trail

Status: partially implemented

Implemented:

1. Mapping review decisions.
2. Gap decisions.
3. Basic optimistic concurrency checks.
4. Immutable `review_decision` audit rows appended for mapping and gap decisions.

Missing or partial:

1. Mapping review now supports reviewer claim/release leases plus cross-reviewer conflict checks, but richer merge/reassignment semantics and broader multi-item coordination are still missing.
2. Review queue ordering/filtering now includes claim state, but prioritization remains simplified.

### 5. Retrieval, Memory, and Prompt Grounding

Status: partially implemented

Implemented:

1. Evidence chunk materialization now also persists a lexical retrieval index in the product repository.
2. Product service/routes now expose cycle-scoped evidence search for retrieval debugging and reviewer workflows.
3. Reviewer mapping and gap decisions now materialize organization/cycle memory records sourced from human feedback.
4. Evidence chunks now also persist semantic retrieval rows, and search now combines lexical and semantic-style ranking.
5. Import-driven cycle processing now builds grounded mapper/skeptic context from cycle control metadata, historical evidence hits, reviewer memory, and workspace freshness policy.
6. Shared workflow prompt assembly now consumes mapper/skeptic memory context through prompt source wiring instead of leaving memory as a read-only side channel.

Missing or partial:

1. Semantic retrieval now supports optional provider-backed embeddings through an env-configured OpenAI path, persists ANN bucket signatures in embedding metadata, and uses two-stage candidate pruning before cosine scoring, but storage/ranking is still product-table metadata search rather than a true `pgvector`/ANN index.
2. Memory compaction, dedupe policy beyond stable keys, and broader subject scopes are still demo-grade.
3. Product runtime can now optionally call a live model provider through an env-configured OpenAI gateway, but the default path still falls back to the local heuristic gateway and there is no provider-neutral abstraction beyond that.

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
2. Product routes now enforce auth/RBAC through injectable hooks, and the real app default is now a shared-style persisted session/token validator instead of the prior header-only authorizer.
3. Product-scoped replay/evaluation harness now captures fixed text/CSV/JSON/Markdown/HTML import-to-export scenarios, emits baseline JSON plus JSON/Markdown comparison reports, and is runnable from `scripts/run_replay_harness.py` with scenario listing, saved baseline/report catalog listing, latest-baseline lookup, and batch capture support.
4. Auth/session routes now exist for login, refresh, current-session revoke, and current-user reads over persisted auth sessions plus signed access tokens.
5. Product tool adapter types are now wired to the AuditFlow repository for artifact reads, evidence search, review history, mapping candidate reads, control lookup, and snapshot validation.
6. Workspace and cycle rows are now organization-scoped, product routes/service propagate the authenticated tenant context into repository reads/writes, and review decisions now record the authenticated reviewer id instead of a fixed demo reviewer.
7. Product admins can now inspect effective runtime modes for model provider, embedding provider, vector search, and connector fetch through a dedicated runtime-capabilities API.

Missing or partial:

1. Auth is now session/token-backed, but it is still a local product implementation rather than a separately shared platform package.
2. SSE forwarding for cycle/workspace live updates now exists, with product events for import acceptance, review recording, and export progress/completion, but event coverage still depends on outbox-backed actions.
3. Replay coverage is still limited to the built-in demo fixture suite; basic saved-baseline/report catalog support now exists, but fixture versioning and curated regression pack management do not.
4. Product runtime no longer boots through the shared demo runtime or static sample gateway, and it now has an optional OpenAI-backed provider path, but connectors and retrieval infra are still not production-grade integrations.

## Delivery Order

1. Step 1: workspace/cycle creation plus reusable control-template seeding.
2. Step 2: align core API contract fields and status codes for workspace/cycle/import endpoints.
3. Step 3: replace synthetic import normalization with artifact-backed ingestion inputs.
4. Step 4: add review decision audit records and stronger concurrency handling.
5. Step 5: wire retrieval/memory inputs into mapping and challenge flows.
6. Step 6: tighten snapshot/export invariants and package manifest generation.
7. Step 7: add product-level SSE, auth hooks, and worker supervision.

## Active Step

Step 5 now centers on turning the optional provider-backed embedding path into durable `pgvector`/ANN infrastructure, while Step 7 focuses on platform extraction, broader live-event coverage, and production connector/model integration.
