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
- Reviewer actions now append immutable `review_decision` audit rows for mapping and gap decisions
- `scripts/run_import_worker.py` now supports single-dispatch and polling modes with optional seeded upload jobs
- Shared runtime foundation lives in `D:\project\SharedAgentCore`
- Future AuditFlow code should consume vendored shared assets instead of re-implementing registries and runtime helpers

## Intended Repo Layout

- `src/`: future AuditFlow backend/app code
- `tests/`: future product-specific tests
- `scripts/`: helper scripts such as shared-core vendoring
- `shared_core/`: vendored copy of `SharedAgentCore` when this becomes a standalone repo

## First Implementation Targets

1. Add OCR/file-format-specific parsers and artifact adapters beyond the current text-backed import path
2. Expand worker execution from local polling into long-running/background process supervision
3. Add richer reviewer concurrency and terminal-state conflict handling around mappings and gaps
4. Add reviewer workbench backend state/query layer and broader import edge-case coverage

## Local Note

The local workspace source of truth for shared assets remains `D:\project\SharedAgentCore`.
