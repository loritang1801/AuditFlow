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
- Default product repository is now SQLAlchemy-backed and shares the same runtime engine/session as the workflow layer
- Current product API covers workspace/cycle creation with contract-aligned slug/owner/audit-period/snapshot fields, cycle-status filtering, artifact-backed imports with format-aware CSV/JSON/text chunking, contract-aligned async import/export status codes, route-level domain error mapping, control/review query filtering including richer review-queue filters, stricter review/gap transition rules plus review-decision history and cycle-level gap/mapping listing, export-readiness validation, narratives, workflow-backed cycle processing, and export package projection with immutable snapshot metadata plus persisted package and manifest artifacts
- Run the local workflow smoke script:

```powershell
python .\scripts\run_demo_workflow.py
```
