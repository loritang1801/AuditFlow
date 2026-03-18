# AuditFlow Agent Context

- Date: 2026-03-17
- Product: SOC 2 evidence management and audit package generation

## Completed Design Layers

- `PRD.md`
- `ARCHITECTURE.md`
- `DATABASE.md`
- `API.md`
- `WORKFLOW.md`
- `PROMPT_TOOL.md`

## Current Implementation State

- Thin product adapters now exist under `src/auditflow_app/`
- `bootstrap.py` filters shared workflows down to AuditFlow-only entries
- `service.py` exposes domain-facing AuditFlow commands over the shared workflow API
- `routes.py` exposes product-specific FastAPI route definitions
- `repository.py` now uses a SQLAlchemy-backed workspace/cycle/import/review/gap/export repository
- Workspace and cycle creation now seed a reusable SOC2 control catalog instead of relying on one implicit demo control row
- `bootstrap.py` defaults to the SQLAlchemy repository over the shared runtime engine/session
- `sample_payloads.py` includes demo payload and request helpers
- `app.py` exposes a FastAPI factory over the shared workflow API
- Implemented product APIs now include controls, control detail, evidence detail, imports, mapping review, gap decisions, narratives, and export submission
- Import submission is now outbox-driven: import requests enqueue `auditflow.import.requested` jobs, and dispatching those jobs triggers the shared `auditflow_cycle_processing` workflow plus evidence/chunk/mapping materialization
- Import acceptance now also emits `auditflow.import.accepted` product outbox events alongside the worker-dispatch import job event
- `worker.py` now provides a dedicated import worker over shared `OutboxDispatcher`, filters unrelated outbox events, and supports connector-specific handlers for `upload`, `jira`, and `confluence`
- `worker.py` now also provides `AuditFlowImportWorkerSupervisor` with retry/backoff, idle-stop, and heartbeat emission, and `scripts/run_import_worker.py` now supports supervised/long-running execution modes
- `replay_harness.py` now provides a product-scoped replay/evaluation harness for fixed text/CSV/JSON/Markdown/HTML import-to-export scenarios, and `scripts/run_replay_harness.py` can list built-in scenarios plus saved baseline/report catalogs, capture baselines, and generate JSON/Markdown comparison reports under `replay_baselines/` and `replay_reports/`
- Import acceptance now collapses duplicate upload and connector requests before enqueueing normalization jobs
- Import processing now persists raw artifact text, normalized artifact text, and multi-chunk evidence rows before reviewer mapping
- Upload imports now normalize CSV, JSON, Markdown, HTML, and plain-text artifacts into structured evidence chunks with parser metadata
- Upload imports now also accept base64-backed binary payloads for PDF, image, DOCX, XLSX, and ZIP evidence, with heuristic text extraction / OCR-style normalization metadata for reviewer workflows
- Import processing now also persists lexical retrieval index rows for evidence chunks, and reviewer decisions now materialize organization/cycle memory records for accepted/rejected mapping and gap outcomes
- Reviewer mutations now emit `auditflow.review.recorded`, and export submission/completion now emit `auditflow.export.progress` plus package-ready outbox events for SSE consumers
- Workflow-backed cycle processing now also emits `auditflow.mapping.progress` using product dashboard counts after materialization completes
- Reviewer actions now append immutable `review_decision` audit rows for mapping and gap decisions
- Cycle-level gap records can now be queried with status/severity filters for reviewer workbench backends
- Review history can now be queried at the cycle level with optional mapping/gap filters for reviewer workbench backends
- Cycle-level mapping records can now be queried with control/state filters for reviewer workbench backends
- Product read APIs now include cycle-scoped evidence search plus reviewer-only memory record inspection for retrieval/prompt-grounding debugging
- Cycle list queries now support `status` filtering and import list routes now accept the contract-level `status` query alias
- Workspace and cycle create/read models now persist contract-facing slug, owner, audit-period, and snapshot timestamp fields while accepting contract request aliases
- Cycle creation plus upload/external import and export submission now support persisted idempotency keys, and cycle/import list routes now emit shared envelope metadata with cursor pagination
- Review mapping and gap decision mutations now also support persisted idempotency keys, and the rest of the product read surface now emits shared envelopes instead of bare payloads
- Mapping, gap, and review-queue reads now surface reviewer `snapshot_version`, and reviewer mutations reject stale snapshot decisions when the cycle has advanced or the caller provides a mismatched `expected_snapshot_version`
- Shared health/workflow endpoints now also emit shared envelopes, and `/api/v1/events/stream` now supports workspace/cycle/export topic filters with payload-backed event context fallback when workflow state is unavailable
- `routes.py` now enforces tenant-scoped minimum-role checks via a header-based authorizer hook, and `build_fastapi_app()` accepts custom authorizer injection for shared-platform integration later
- Control matrix queries now support `coverage_status` and `search` filters at the product layer
- Review queue queries now support `control_state_id`, `severity`, and `sort=recent|ranking` filtering at the product layer
- Gap transitions now enforce a stricter terminal policy: `acknowledge` only from `open`, `reopen_gap` only from `resolved`
- `routes.py` now contains explicit domain-error-to-HTTP mapping logic for product APIs
- Import and export submission routes now return `202 Accepted` for async contract parity, and export submission rejects cycles with no accepted mappings, open review items, stale snapshot requests, and duplicate queued exports
- Export package projection now records immutable package timestamps plus persisted package and manifest artifacts describing controls, accepted mappings, open gaps, and narratives for the frozen snapshot
- Cycle-scoped export history can now be listed as a freeze ledger with `snapshot_version` and `status` filters for package audit/read-side tooling
- Export freezes are now logically keyed by `cycle + snapshot_version`, so repeated export requests for the same approved snapshot return the existing immutable package instead of minting duplicates
- `scripts/run_import_worker.py` now supports single-dispatch and polling modes with optional seeded upload jobs
- Shared runtime foundation lives in `D:\project\SharedAgentCore`
- Future AuditFlow code should consume vendored shared assets instead of re-implementing registries and runtime helpers

## Intended Repo Layout

- `src/`: future AuditFlow backend/app code
- `tests/`: future product-specific tests
- `scripts/`: helper scripts such as shared-core vendoring
- `shared_core/`: vendored copy of `SharedAgentCore` when this becomes a standalone repo

## First Implementation Targets

1. Expand binary parsing beyond the current PDF/image/DOCX/XLSX/ZIP heuristic support into stronger OCR and broader office/archive coverage
2. Replace the current header-based route auth hook with shared session/token validation
3. Upgrade retrieval from the current lexical chunk index into true hybrid retrieval with vector search and direct mapper/skeptic prompt consumption
4. Expand reviewer workbench state/query coverage beyond current review-decision history, evidence search, memory inspection, cycle-level gap/mapping listing, and broader import edge-case coverage
5. Expand the replay/evaluation harness beyond the current built-in fixture suite and basic saved-baseline catalog into fixture versioning and curated regression packs

## Resume Point

- Latest completed commit: `fe01ffa` `Add AuditFlow retrieval and memory foundations`
- Retrieval/memory v1 is now in place: lexical evidence search, persisted chunk index rows, and reviewer-derived organization/cycle memory records
- The next implementation start point should be one of these:
  1. Upgrade retrieval from lexical-only to true hybrid retrieval with vector/semantic ranking
  2. Wire memory/retrieval outputs directly into mapper and skeptic prompt assembly
  3. Replace the local header authorizer with shared session/token validation
- If continuing the current product track, start with item 1 above before expanding more read APIs

## Local Note

The local workspace source of truth for shared assets remains `D:\project\SharedAgentCore`.
