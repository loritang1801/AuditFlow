# AuditFlow

`AuditFlow` is the product workspace for the SOC 2 evidence management agent.

## Current State

This folder now contains the product specs plus a working product-layer implementation under `src/auditflow_app/`.

Available documents:

- `PRD.md`
- `ARCHITECTURE.md`
- `DATABASE.md`
- `API.md`
- `WORKFLOW.md`
- `PROMPT_TOOL.md`

## Shared Code Strategy

Shared runtime code is maintained centrally in `D:\project\SharedAgentCore`.

Before splitting this project into its own GitHub repository, vendor `SharedAgentCore` into this repo as `shared_core/` by running:

```powershell
.\scripts\vendor_shared_core.ps1
```

## Validation

After vendoring shared code, run tests from `shared_core/`.

## Local Demo

Product-specific thin adapters now live under `src/auditflow_app/`.

- Build a product-scoped API service from `auditflow_app.bootstrap`
- Build a domain-facing application service from `auditflow_app.bootstrap.build_app_service`
- Use `auditflow_app.app:create_app` as a FastAPI factory when `fastapi` is installed
- Build a product-scoped replay harness from `auditflow_app.bootstrap.build_replay_harness`
- Default product repository is now SQLAlchemy-backed and shares the same runtime engine/session as the workflow layer
- Current product API covers workspace/cycle creation with contract-aligned slug/owner/audit-period/snapshot fields, persisted idempotency for cycle/import/export plus reviewer mapping/gap submissions, shared envelopes and cursor metadata across the product read surface plus health/workflow endpoints, `/api/v1/events/stream` with workspace/cycle/export topic filtering, product outbox events for import acceptance plus reviewer/export lifecycle updates and mapping progress, cycle-status filtering, artifact-backed imports with format-aware CSV/JSON/Markdown/HTML/text chunking plus base64-backed PDF/image binary parsing heuristics, contract-aligned async import/export status codes, route-level domain error mapping, control/review query filtering including richer review-queue filters, snapshot-aware reviewer conflict checks and snapshot-version read models, stricter review/gap transition rules plus review-decision history and cycle-level gap/mapping listing, cycle-scoped export freeze history reads, export freezes keyed by cycle plus snapshot, export-readiness validation, narratives, workflow-backed cycle processing, export package projection with immutable snapshot metadata plus persisted package and manifest artifacts, and a product-scoped replay harness that captures baseline/replay reports across fixed text/CSV/JSON/Markdown/HTML import-to-export scenarios
- Run the local workflow smoke script:

```powershell
python .\scripts\run_demo_workflow.py
```

- Run the local replay harness:

```powershell
python .\scripts\run_replay_harness.py
```

- List built-in replay scenarios:

```powershell
python .\scripts\run_replay_harness.py --list-scenarios
```

- Capture all built-in replay baselines:

```powershell
python .\scripts\run_replay_harness.py --all --capture-only
```
