# AuditFlow Source

Product-layer AuditFlow application code lives under `src/auditflow_app/`.

Current implementation includes:

- `bootstrap.py`: shared-runtime wiring and app/worker factories
- `service.py`: domain-facing application service
- `service.py`: domain-facing application service plus multi-format import normalization, including PDF/image binary heuristics
- `repository.py`: SQLAlchemy-backed product repository
- `routes.py`: FastAPI route layer and shared response envelopes
- `worker.py`: import outbox dispatcher and connector-specific handlers
- `replay_harness.py`: multi-format demo cycle-to-export replay baseline/evaluation capture
- `sample_payloads.py`: demo and test payload builders
