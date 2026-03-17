# AuditFlow Prompt and Tool Contracts

- Version: v0.1
- Date: 2026-03-16
- Scope: `AuditFlow` prompt bundle templates, agent-specific tool contracts, adapter mappings, and grounding rules

## 1. Contract Summary

This document defines the prompt and tool contracts used by `AuditFlow` specialist agents.

It locks:

1. Prompt bundle ids and template structure per agent
2. Required variables and forbidden context
3. Logical tool names, input/output responsibilities, and adapter mappings
4. Citation and snapshot binding rules
5. Failure handling for stale evidence, invalid citations, and reviewer-locked state

The contracts in this document extend the shared rules in `SharedAgentCore/docs/PROMPT_TOOL.md`.

## 2. Bundle Registry

| Agent | Bundle Id | Default Model Profile | Primary Nodes | Response Schema Ref |
| --- | --- | --- | --- | --- |
| `collector_agent` | `auditflow.collector` | `extraction.standard` | `ingestion`, `normalization` | `auditflow.collector.output.v1` |
| `mapper_agent` | `auditflow.mapper` | `reasoning.standard` | `mapping` | `auditflow.mapper.output.v1` |
| `skeptic_agent` | `auditflow.skeptic` | `reasoning.standard` | `challenge` | `auditflow.skeptic.output.v1` |
| `writer_agent` | `auditflow.writer` | `generation.grounded` | `package_generation` | `auditflow.writer.output.v1` |
| `review_coordinator` | `auditflow.review_coordinator` | `summarization.compact` | `human_review` | `auditflow.review_coordinator.output.v1` |

Every bundle version must be recorded on the workflow run before the first agent invocation in its node.

## 3. Shared AuditFlow Prompt Context

### 3.1 Authoritative Context Sources

AuditFlow bundles may draw from:

1. `AuditCycleWorkflowState`
2. `audit_cycle`, `audit_workspace`, and `audit_control_state`
3. Accepted or pending `evidence_item`, `evidence_chunk`, and `evidence_mapping`
4. `gap_record` and `review_decision`
5. Snapshot metadata and export eligibility state
6. Retrieved organization memory for reviewer preferences and accepted evidence patterns

### 3.2 Forbidden Default Context

The runtime must not inject the following as positive evidence context unless the bundle explicitly asks for challenge/comparison material:

1. Rejected mappings as if they were accepted exemplars
2. Stale evidence marked outside allowed freshness windows
3. Reviewer comments containing secrets or unrelated internal notes
4. Draft export text from a different snapshot version

### 3.3 Context Packing Priority

When token limits force pruning, keep context in this order:

1. Current control text and control scope
2. Evidence chunks referenced by current node inputs
3. Accepted reviewer decisions for the same control
4. Open gaps and freshness warnings
5. Historical summary memory

## 4. Agent Prompt Templates

### 4.1 `collector_agent`

Purpose:

1. Normalize imported evidence into structured metadata
2. Summarize what the artifact contains
3. Emit citation-ready metadata for later nodes

Prompt parts:

1. `system_identity`: evidence normalization specialist for SOC 2 support material
2. `developer_constraints`: do not infer facts absent from content; return only schema fields
3. `runtime_context`: cycle id, source id, workspace policy, allowed evidence types
4. `domain_context`: artifact metadata, extracted text preview, source origin metadata
5. `output_contract`: `auditflow.collector.output.v1`

Required variables:

1. `audit_cycle_id`
2. `source_id`
3. `source_type`
4. `artifact_id`
5. `extracted_text_or_summary`
6. `allowed_evidence_types`

Forbidden context:

1. Existing mapping decisions
2. Reviewer acceptance history
3. Unrelated control text

Allowed tools:

1. `artifact.read`
2. `artifact.preview_chunk`

Output contract:

```json
{
  "normalized_title": "Quarterly Access Review",
  "evidence_type": "ticket",
  "summary": "Quarterly user access review completed for production systems.",
  "captured_at": "2026-03-10T09:00:00Z",
  "fresh_until": "2026-06-10T09:00:00Z",
  "citation_refs": [
    {
      "kind": "artifact",
      "id": "uuid"
    }
  ]
}
```

Rules:

1. `captured_at` may be null if the source does not support a reliable date.
2. `summary` must be grounded in cited content.
3. OCR/parser execution happens before prompt assembly and is not agent-invoked.

### 4.2 `mapper_agent`

Purpose:

1. Propose evidence-to-control mappings
2. Rank mapping candidates
3. Provide rationale anchored to evidence chunks

Prompt parts:

1. `system_identity`: control-evidence mapping specialist
2. `developer_constraints`: only map to in-scope controls; do not treat prior acceptance as proof
3. `runtime_context`: cycle id, control scope, evidence item metadata
4. `domain_context`: evidence chunks, control text, control objectives, candidate control shortlist
5. `memory_context`: accepted reviewer preferences for the same organization and framework
6. `tool_manifest`
7. `output_contract`

Required variables:

1. `audit_cycle_id`
2. `evidence_item_id`
3. `evidence_chunk_refs`
4. `in_scope_controls`
5. `framework_name`

Forbidden context:

1. Rejected mappings presented as accepted exemplars
2. Reviewer comments from unrelated controls
3. Narrative drafts

Allowed tools:

1. `evidence.search`
2. `control_catalog.lookup`
3. `mapping.read_candidates`

Output contract:

```json
{
  "mapping_candidates": [
    {
      "control_id": "uuid",
      "confidence": 0.84,
      "ranking_score": 0.91,
      "rationale": "Evidence shows a completed access review.",
      "citation_refs": [
        {
          "kind": "evidence_chunk",
          "id": "uuid"
        }
      ]
    }
  ]
}
```

Rules:

1. Every candidate must have at least one `citation_ref`.
2. `confidence` and `ranking_score` are independent fields; neither may be omitted.
3. The agent may reuse existing pending candidates for de-duplication, but not auto-accept them.

### 4.3 `skeptic_agent`

Purpose:

1. Challenge weak or conflicting mappings
2. Identify stale or insufficient evidence
3. Suggest gap records grounded in policy or freshness rules

Prompt parts:

1. `system_identity`: evidence quality and contradiction reviewer
2. `developer_constraints`: prefer rejecting weak proof over overclaiming control coverage
3. `runtime_context`: current cycle status, freshness policy, mapping ids under review
4. `domain_context`: proposed mappings, cited evidence chunks, existing gaps, freshness metadata
5. `memory_context`: prior reviewer rejection patterns for same control
6. `tool_manifest`
7. `output_contract`

Required variables:

1. `proposed_mapping_ids`
2. `mapping_payloads`
3. `freshness_policy`
4. `control_text`

Forbidden context:

1. Export package status
2. Draft narratives
3. Reviewer identities beyond decision outcomes

Allowed tools:

1. `evidence.search`
2. `control_catalog.lookup`
3. `mapping.read_candidates`
4. `review_decision.read_history`

Output contract:

```json
{
  "mapping_flags": [
    {
      "mapping_id": "uuid",
      "issue_type": "conflicting_evidence",
      "severity": "high",
      "recommended_action": "Request latest quarterly review evidence."
    }
  ],
  "gaps": [
    {
      "control_state_id": "uuid",
      "gap_type": "stale_evidence",
      "severity": "high",
      "title": "Evidence older than freshness threshold"
    }
  ]
}
```

Rules:

1. A flag without a cited contradictory or insufficient ref is invalid.
2. `gap_type` must map to the domain enum already defined in database contracts.
3. The agent must not downgrade a reviewer-locked accepted mapping.

### 4.4 `writer_agent`

Purpose:

1. Generate export-ready narratives from frozen snapshot data
2. Summarize accepted evidence and unresolved gaps
3. Preserve citation traceability in generated text metadata

Prompt parts:

1. `system_identity`: audit narrative writer operating on frozen snapshot data
2. `developer_constraints`: do not introduce facts outside the frozen snapshot; cite accepted inputs only
3. `runtime_context`: cycle id, `working_snapshot_version`, export request metadata
4. `domain_context`: accepted mappings, accepted gaps, prior accepted narrative fragments for same snapshot
5. `tool_manifest`
6. `output_contract`

Required variables:

1. `audit_cycle_id`
2. `working_snapshot_version`
3. `accepted_mapping_refs`
4. `open_gap_refs`
5. `export_scope`

Forbidden context:

1. Pending or rejected mappings
2. Reviewer draft comments not part of accepted decisions
3. Data from any other snapshot version

Allowed tools:

1. `narrative.snapshot_read`
2. `control_catalog.lookup`
3. `export.snapshot_validate`

Output contract:

```json
{
  "narratives": [
    {
      "control_state_id": "uuid",
      "narrative_type": "control_summary",
      "content_markdown": "Access review is evidenced by the quarterly review artifact.",
      "citation_refs": [
        {
          "kind": "evidence_chunk",
          "id": "uuid"
        }
      ]
    }
  ]
}
```

Rules:

1. The runtime must validate snapshot eligibility before agent invocation.
2. Narrative generation fails closed if `working_snapshot_version` no longer matches current frozen export input.
3. The agent can summarize open gaps but may not phrase them as resolved coverage.

### 4.5 `review_coordinator`

Purpose:

1. Summarize review queue status
2. Predict readiness for export
3. Suggest reviewer focus areas

Prompt parts:

1. `system_identity`: review queue summarizer and readiness coordinator
2. `developer_constraints`: summarize state, do not create final review decisions
3. `runtime_context`: blocker counts, gap counts, snapshot version
4. `domain_context`: pending mappings, unresolved gaps, recent review outcomes
5. `tool_manifest`
6. `output_contract`

Required variables:

1. `review_blocker_count`
2. `pending_mapping_refs`
3. `open_gap_refs`
4. `working_snapshot_version`

Forbidden context:

1. Future export package ids
2. Uncommitted reviewer drafts

Allowed tools:

1. `mapping.read_candidates`
2. `review_decision.read_history`
3. `export.snapshot_validate`

Output contract:

```json
{
  "review_blocker_count": 4,
  "ready_for_export": false,
  "blocking_ids": ["uuid"],
  "recommended_focus": "Resolve high-severity stale evidence gaps first."
}
```

Rules:

1. This agent may recommend ordering but may not write final review decisions.
2. `ready_for_export` is advisory and must still be checked against domain gates.

## 5. AuditFlow Tool Registry

### 5.1 Logical Tools

| Tool | Access | Purpose | Normalized Output |
| --- | --- | --- | --- |
| `artifact.read` | `read_only` | Read artifact metadata and content refs | Artifact metadata, text refs, parser status |
| `artifact.preview_chunk` | `read_only` | Retrieve citation-ready artifact excerpt | Chunk text, locator, stable chunk ref |
| `evidence.search` | `read_only` | Search evidence chunks within workspace/cycle | Ranked chunk refs with evidence metadata |
| `control_catalog.lookup` | `read_only` | Resolve in-scope controls and text | Control ids, titles, objective text |
| `mapping.read_candidates` | `read_only` | Read existing mapping proposals and statuses | Mapping candidate refs and status summaries |
| `review_decision.read_history` | `read_only` | Read reviewer outcomes for same control or mapping | Decision summaries, status, timestamps |
| `narrative.snapshot_read` | `read_only` | Read frozen snapshot inputs for writing | Accepted mappings, gaps, prior narratives |
| `export.snapshot_validate` | `read_only` | Validate export eligibility for snapshot | Eligibility flag, blocker codes, snapshot metadata |

### 5.2 Adapter Mapping

| Tool | Primary Adapter | Notes |
| --- | --- | --- |
| `artifact.read` | Object storage + artifact metadata store | Used for uploads, Jira attachments, Confluence attachments |
| `artifact.preview_chunk` | Chunk store | Returns citation-ready excerpt and locator |
| `evidence.search` | Postgres + vector retrieval | Workspace- and cycle-scoped retrieval only |
| `control_catalog.lookup` | Control catalog database | Read-only framework lookup |
| `mapping.read_candidates` | AuditFlow database | Reads accepted, pending, rejected statuses |
| `review_decision.read_history` | AuditFlow database | Exposes decision outcomes, not raw private reviewer notes |
| `narrative.snapshot_read` | Snapshot reader over audit tables | Must bind to fixed `working_snapshot_version` |
| `export.snapshot_validate` | Snapshot validator service | No external connector |

Connector notes:

1. Jira and Confluence remain connector adapters that feed `artifact` and `evidence` records before agent invocation.
2. OCR and parsing are internal preprocessing services and not agent-callable tools in v1.

## 6. Tool Policies and Guardrails

1. `mapper_agent` and `skeptic_agent` may use only read-only tools.
2. `writer_agent` may operate only on accepted snapshot data and may not call general evidence search.
3. `review_coordinator` may read review history but may not mutate review state.
4. Any tool result that resolves to a rejected or stale artifact must be marked as contextual warning data, not positive proof.
5. Tool results must expose stable ids so reviewer UI and export package generation can reuse the same refs.

## 7. Failure and Recovery Rules

1. If `mapper_agent` returns a candidate without citations, the node fails with `CITATION_REQUIRED`.
2. If a cited `evidence_chunk` no longer exists, fail with `CITATION_REF_INVALID` and do not auto-repair with a different chunk.
3. If a mapping is reviewer-locked after prompt assembly but before writeback, the node must discard the conflicting patch and reload current row state.
4. If `export.snapshot_validate` reports stale snapshot metadata, `writer_agent` must not run.
5. Partial retrieval failure may continue only if cited primary evidence remains available.

## 8. Replay and Version Binding

Each AuditFlow agent run must persist:

1. `bundle_id` and `bundle_version`
2. `response_schema_ref`
3. `tool_policy_id` and `tool_policy_version`
4. `working_snapshot_version` when applicable
5. Evidence chunk refs used for prompt context
6. Tool result `raw_ref` values for replay fixtures

Replay rules:

1. `writer_agent` replay must use the same `working_snapshot_version`.
2. Replay may stub connector-fed artifacts with stored `artifact` and chunk refs.
3. Reviewer history used in replay must be capped to the version visible at the original checkpoint.

## 9. Mapping to Existing Docs

This document extends and must remain consistent with:

1. `WORKFLOW.md` for AuditFlow node sequencing and human review rules
2. `API.md` for evidence detail, review queue, narrative, and export interfaces
3. `DATABASE.md` for evidence, mapping, gap, review, narrative, and package tables
4. `D:/project/SharedAgentCore/docs/PROMPT_TOOL.md` for shared registry and runtime protocol
