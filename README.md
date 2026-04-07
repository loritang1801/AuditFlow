# AuditFlow

AuditFlow is a Python/FastAPI product repo for audit evidence intake, review workflows, replay validation, and local connector fallback. Detailed design and API documentation live in [docs/PROJECT.md](docs/PROJECT.md).

## Local Run

```powershell
cd D:\project\AuditFlow
python -m pip install --upgrade pip
python -m pip install -e .[api,connectors]
Copy-Item .env.example .env

.\start-local.ps1
python .\scripts\run_demo_workflow.py
python .\scripts\run_import_worker.py --poll --iterations 2 --max-idle-polls 1 --seed-upload
python .\scripts\run_replay_harness.py
python .\scripts\run_runtime_smoke.py
python .\scripts\run_ci_checks.py
```

Local scripts default to [`.local/auditflow.db`](D:\project\AuditFlow\.local\auditflow.db) for persistence. Override that with `AUDITFLOW_DATABASE_URL` or `--database-url`. The repo defaults to vendored [`shared_core`](D:\project\AuditFlow\shared_core); set `AUDITFLOW_SHARED_CORE_SOURCE=workspace` only if you explicitly want to load the sibling `SharedAgentCore` workspace copy.
