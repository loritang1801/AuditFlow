# AuditFlow

`AuditFlow` is the product workspace for the SOC 2 evidence management agent.

## Current State

This folder currently contains decision-complete product and engineering specifications plus an initialized repo skeleton for future implementation.

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
- Current product API covers workspace lookup, cycle list/dashboard, control detail, evidence detail, mapping review, narratives, workflow-backed cycle processing, and export package projection
- Run the local workflow smoke script:

```powershell
python .\scripts\run_demo_workflow.py
```
