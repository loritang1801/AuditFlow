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
- Current product API covers workspace/cycle creation with contract-aligned slug/owner/audit-period/snapshot fields, organization-scoped workspace/cycle enforcement, persisted idempotency for cycle/import/export plus reviewer mapping/gap submissions, mapping claim/release leases with claim-aware review-queue filters, shared envelopes and cursor metadata across the product read surface plus health/workflow/runtime-capability endpoints, `/api/v1/events/stream` with workspace/cycle/export topic filtering, product outbox events for import acceptance plus reviewer/export lifecycle updates and mapping progress, cycle-status filtering, artifact-backed imports with format-aware CSV/JSON/Markdown/HTML/text chunking plus base64-backed PDF/image/DOCX/XLSX/ZIP binary parsing heuristics, lexical evidence search over persisted chunk indexes, reviewer-readable organization/cycle memory records sourced from review outcomes, contract-aligned async import/export status codes, route-level domain error mapping including invalid binary payloads and invalid search queries, control/review query filtering including richer review-queue filters, snapshot-aware reviewer conflict checks and snapshot-version read models, stricter review/gap transition rules plus review-decision history and cycle-level gap/mapping listing, cycle-scoped export freeze history reads, export freezes keyed by cycle plus snapshot, export-readiness validation, narratives, workflow-backed cycle processing, export package projection with immutable snapshot metadata plus persisted package and manifest artifacts, and a product-scoped replay harness that captures baseline/replay reports across fixed text/CSV/JSON/Markdown/HTML import-to-export scenarios
- Current product API also includes shared-style auth/session routes backed by persisted auth sessions and signed access tokens, hybrid lexical+dense-vector evidence search with ANN-style semantic candidate pruning, mapper/skeptic prompt grounding from historical evidence plus reviewer memory, repository-bound product tool adapters for evidence search/artifact reads/control lookup/review history/snapshot validation, a product-specific runtime/model gateway assembly instead of the shared demo bootstrap, optional env-configured OpenAI provider paths for model responses and embeddings with local fallback, and optional env-configured live Jira/Confluence HTTP fetch with synthetic fallback
- Optional live-provider environment variables:
  - `AUDITFLOW_MODEL_PROVIDER=auto|local|openai`
  - `AUDITFLOW_OPENAI_MODEL=<responses-model>`
  - `AUDITFLOW_EMBEDDING_PROVIDER=auto|local|openai`
  - `AUDITFLOW_OPENAI_EMBEDDING_MODEL=<embedding-model>`
  - `AUDITFLOW_VECTOR_SEARCH_MODE=auto|ann|flat|pgvector`
  - `AUDITFLOW_VECTOR_CANDIDATE_LIMIT=<positive-int>`
  - `AUDITFLOW_VECTOR_ANN_BUCKETS=<positive-int>`
  - `OPENAI_API_KEY=<api-key>`
- Optional live connector environment variables:
  - `AUDITFLOW_JIRA_FETCH_MODE=auto|local|http`
  - `AUDITFLOW_JIRA_URL_TEMPLATE=<issue-url-template>`
  - `AUDITFLOW_JIRA_QUERY_URL_TEMPLATE=<query-url-template>`
  - `AUDITFLOW_JIRA_AUTH_TOKEN=<bearer-token>`
  - `AUDITFLOW_CONFLUENCE_FETCH_MODE=auto|local|http`
  - `AUDITFLOW_CONFLUENCE_URL_TEMPLATE=<page-url-template>`
  - `AUDITFLOW_CONFLUENCE_QUERY_URL_TEMPLATE=<query-url-template>`
  - `AUDITFLOW_CONFLUENCE_AUTH_TOKEN=<bearer-token>`
- Run the local workflow smoke script:

```powershell
python .\scripts\run_demo_workflow.py
```

- Run the local replay harness:

```powershell
python .\scripts\run_replay_harness.py
```

- Run the vector-search validation/benchmark harness:

```powershell
python .\scripts\run_vector_search_benchmark.py --mode ann
python .\scripts\run_vector_search_benchmark.py --database-url postgresql+psycopg://... --mode pgvector --expected-effective-mode pgvector
python .\scripts\run_vector_search_benchmark.py --compare-mode ann=ann --compare-mode flat=flat
```

- Query cycle-scoped evidence search or memory records from the FastAPI surface:

```text
GET /api/v1/auditflow/cycles/{cycle_id}/evidence-search?query=access+review
GET /api/v1/auditflow/cycles/{cycle_id}/memory-records
```

- List built-in replay scenarios:

```powershell
python .\scripts\run_replay_harness.py --list-scenarios
```

- List saved replay baselines or reports:

```powershell
python .\scripts\run_replay_harness.py --list-baselines
python .\scripts\run_replay_harness.py --list-reports
```

- Capture all built-in replay baselines:

```powershell
python .\scripts\run_replay_harness.py --all --capture-only
```

- Re-run against the latest saved baseline for one scenario:

```powershell
python .\scripts\run_replay_harness.py --latest-baseline --scenario-name json_access_review
```
