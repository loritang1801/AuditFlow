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
- `worker.py` now provides a dedicated import worker over shared `OutboxDispatcher`, filters unrelated outbox events, and supports connector-specific handlers for `upload`, `jira`, and `confluence`
- Import acceptance now collapses duplicate upload and connector requests before enqueueing normalization jobs
- Import processing now persists raw artifact text, normalized artifact text, and multi-chunk evidence rows before reviewer mapping
- Upload imports now normalize CSV, JSON, and plain-text artifacts into structured evidence chunks with parser metadata
- Reviewer actions now append immutable `review_decision` audit rows for mapping and gap decisions
- Cycle-level gap records can now be queried with status/severity filters for reviewer workbench backends
- Review history can now be queried at the cycle level with optional mapping/gap filters for reviewer workbench backends
- Cycle-level mapping records can now be queried with control/state filters for reviewer workbench backends
- Gap transitions now enforce a stricter terminal policy: `acknowledge` only from `open`, `reopen_gap` only from `resolved`
- `routes.py` now contains explicit domain-error-to-HTTP mapping logic for product APIs
- Export submission now rejects cycles with no accepted mappings, open review items, stale snapshot requests, and duplicate queued exports
- `scripts/run_import_worker.py` now supports single-dispatch and polling modes with optional seeded upload jobs
- Shared runtime foundation lives in `D:\project\SharedAgentCore`
- Future AuditFlow code should consume vendored shared assets instead of re-implementing registries and runtime helpers

## Intended Repo Layout

- `src/`: future AuditFlow backend/app code
- `tests/`: future product-specific tests
- `scripts/`: helper scripts such as shared-core vendoring
- `shared_core/`: vendored copy of `SharedAgentCore` when this becomes a standalone repo

## First Implementation Targets

1. Add OCR and binary file parsers beyond the current CSV/JSON/text-backed import path
2. Expand worker execution from local polling into long-running/background process supervision
3. Expand reviewer concurrency and terminal-state conflict handling beyond the current stricter gap transition policy
4. Expand reviewer workbench state/query coverage beyond current review-decision history, cycle-level gap/mapping listing, and broader import edge-case coverage

## Local Note

The local workspace source of truth for shared assets remains `D:\project\SharedAgentCore`.
