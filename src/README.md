# AuditFlow Source

Product-layer AuditFlow application code lives under `src/auditflow_app/`.

Current implementation includes:

- `bootstrap.py`: shared-runtime wiring and app/worker factories
- `service.py`: domain-facing application service
- `service.py`: domain-facing application service plus multi-format import normalization, lexical evidence search, and memory record reads
- `repository.py`: SQLAlchemy-backed product repository with evidence chunk indexing plus reviewer-derived memory persistence
- `routes.py`: FastAPI route layer and shared response envelopes, including cycle-scoped evidence search and reviewer memory inspection
- `worker.py`: import outbox dispatcher and connector-specific handlers
- `replay_harness.py`: multi-format demo cycle-to-export replay baseline/evaluation capture plus saved baseline/report catalog helpers
- `sample_payloads.py`: demo and test payload builders
