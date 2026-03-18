# AuditFlow Source

Product-layer AuditFlow application code lives under `src/auditflow_app/`.

Current implementation includes:

- `bootstrap.py`: shared-runtime wiring and app/worker factories
- `service.py`: domain-facing application service
- `repository.py`: SQLAlchemy-backed product repository
- `routes.py`: FastAPI route layer and shared response envelopes
- `worker.py`: import outbox dispatcher and connector-specific handlers
- `sample_payloads.py`: demo and test payload builders
