from __future__ import annotations

import os
from contextlib import contextmanager
from statistics import fmean
from time import perf_counter
from typing import Iterator
from uuid import uuid4

from .bootstrap import build_app_service
from .sample_payloads import cycle_create_command, upload_import_command, workspace_create_command


DEFAULT_QUERY_SET = (
    "access review approval",
    "joiner mover leaver approval",
    "security alert triage",
    "change approval deployment",
)
_ALLOWED_REQUESTED_VECTOR_MODES = frozenset(("auto", "ann", "flat", "pgvector"))
_ALLOWED_EFFECTIVE_VECTOR_MODES = frozenset(("ann", "flat", "pgvector"))


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    position = max(0, min(len(sorted_values) - 1, int(round((len(sorted_values) - 1) * percentile))))
    return round(sorted_values[position], 4)


@contextmanager
def _temporary_env(overrides: dict[str, str | None]) -> Iterator[None]:
    original_values = {key: os.environ.get(key) for key in overrides}
    try:
        for key, value in overrides.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in original_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _benchmark_documents(corpus_size: int) -> list[dict[str, str]]:
    templates = (
        (
            "Quarterly Access Review",
            "Quarterly access review approval completed for production entitlements and privileged signoff.",
        ),
        (
            "Joiner Mover Leaver Audit",
            "Joiner mover leaver approval ticket shows onboarding, transfer review, and offboarding evidence.",
        ),
        (
            "Security Alert Triage",
            "Security alert triage notes capture monitoring evidence, incident investigation, and response workflow.",
        ),
        (
            "Production Change Approval",
            "Production change approval record shows deployment evidence, rollback planning, and release review.",
        ),
    )
    documents: list[dict[str, str]] = []
    normalized_corpus_size = max(1, corpus_size)
    for index in range(normalized_corpus_size):
        title_prefix, body = templates[index % len(templates)]
        documents.append(
            {
                "artifact_id": f"benchmark-artifact-{index + 1}",
                "display_name": f"{title_prefix} {index + 1}",
                "artifact_text": (
                    f"{title_prefix} {index + 1}\n\n"
                    f"{body}\n"
                    f"Evidence batch marker: {index + 1}."
                ),
                "source_locator": f"benchmarks/{title_prefix.lower().replace(' ', '-')}-{index + 1}.txt",
            }
        )
    return documents


def _normalize_requested_vector_modes(modes: list[str]) -> list[str]:
    normalized_modes: list[str] = []
    seen_modes: set[str] = set()
    for mode in modes:
        normalized_mode = mode.strip().lower()
        if not normalized_mode:
            continue
        if normalized_mode not in _ALLOWED_REQUESTED_VECTOR_MODES:
            raise ValueError(f"Unsupported vector search mode: {normalized_mode}")
        if normalized_mode in seen_modes:
            continue
        normalized_modes.append(normalized_mode)
        seen_modes.add(normalized_mode)
    if not normalized_modes:
        raise ValueError("At least one vector search mode is required")
    return normalized_modes


def _normalize_expected_effective_modes(
    expected_effective_modes: dict[str, str] | None,
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for requested_mode, effective_mode in (expected_effective_modes or {}).items():
        normalized_requested_mode = str(requested_mode).strip().lower()
        normalized_effective_mode = str(effective_mode).strip().lower()
        if normalized_requested_mode not in _ALLOWED_REQUESTED_VECTOR_MODES:
            raise ValueError(f"Unsupported vector search mode: {normalized_requested_mode}")
        if normalized_effective_mode not in _ALLOWED_EFFECTIVE_VECTOR_MODES:
            raise ValueError(f"Unsupported effective vector mode: {normalized_effective_mode}")
        normalized[normalized_requested_mode] = normalized_effective_mode
    return normalized


def _extract_vector_capability(report: dict[str, object]) -> dict[str, object]:
    runtime_capabilities = report.get("runtime_capabilities")
    if not isinstance(runtime_capabilities, dict):
        return {}
    vector_capability = runtime_capabilities.get("vector_search")
    if not isinstance(vector_capability, dict):
        return {}
    return vector_capability


def _extract_vector_details(report: dict[str, object]) -> dict[str, object]:
    details = _extract_vector_capability(report).get("details")
    return dict(details) if isinstance(details, dict) else {}


def run_vector_search_benchmark(
    *,
    database_url: str | None = None,
    vector_search_mode: str | None = None,
    expected_effective_mode: str | None = None,
    corpus_size: int = 40,
    iterations: int = 5,
    queries: list[str] | None = None,
) -> dict[str, object]:
    env_overrides = {
        "AUDITFLOW_VECTOR_SEARCH_MODE": vector_search_mode,
    }
    normalized_iterations = max(1, iterations)
    normalized_queries = [query.strip() for query in (queries or list(DEFAULT_QUERY_SET)) if query.strip()]
    if not normalized_queries:
        raise ValueError("At least one non-empty query is required")
    with _temporary_env(env_overrides):
        service = build_app_service(database_url=database_url)
        try:
            run_token = uuid4().hex[:8]
            organization_id = f"org-benchmark-{run_token}"
            workspace = service.create_workspace(
                workspace_create_command(
                    workspace_name=f"AuditFlow Vector Benchmark {run_token}",
                    slug=f"auditflow-vector-benchmark-{run_token}",
                    default_owner_user_id="benchmark-owner",
                ),
                organization_id=organization_id,
            )
            cycle = service.create_cycle(
                cycle_create_command(
                    workspace_id=workspace.workspace_id,
                    cycle_name=f"Vector Benchmark Cycle {run_token}",
                    owner_user_id="benchmark-owner",
                ),
                organization_id=organization_id,
            )
            for index, document in enumerate(_benchmark_documents(corpus_size), start=1):
                service.create_upload_import(
                    cycle.cycle_id,
                    upload_import_command(
                        artifact_id=document["artifact_id"],
                        display_name=document["display_name"],
                        artifact_text=document["artifact_text"],
                    )
                    | {"source_locator": document["source_locator"]},
                    organization_id=organization_id,
                    idempotency_key=f"benchmark-import-{run_token}-{index}",
                )
            dispatch = service.dispatch_import_jobs()
            capabilities = service.get_runtime_capabilities()
            effective_mode = str(capabilities.vector_search.effective_mode)
            if expected_effective_mode is not None and effective_mode != expected_effective_mode:
                raise ValueError(
                    f"Expected effective vector mode {expected_effective_mode}, got {effective_mode}"
                )
            query_reports: list[dict[str, object]] = []
            aggregate_latencies: list[float] = []
            for query in normalized_queries:
                service.search_evidence(
                    cycle.cycle_id,
                    query=query,
                    limit=5,
                    organization_id=organization_id,
                )
                latencies_ms: list[float] = []
                last_result = None
                for _ in range(normalized_iterations):
                    started_at = perf_counter()
                    last_result = service.search_evidence(
                        cycle.cycle_id,
                        query=query,
                        limit=5,
                        organization_id=organization_id,
                    )
                    latency_ms = round((perf_counter() - started_at) * 1000.0, 4)
                    latencies_ms.append(latency_ms)
                    aggregate_latencies.append(latency_ms)
                top_item = last_result.items[0] if last_result is not None and last_result.items else None
                query_reports.append(
                    {
                        "query": query,
                        "iterations": normalized_iterations,
                        "result_count": int(last_result.total_count if last_result is not None else 0),
                        "top_title": (top_item.title if top_item is not None else None),
                        "top_score": (top_item.score if top_item is not None else None),
                        "latency_ms": {
                            "min": round(min(latencies_ms), 4),
                            "avg": round(fmean(latencies_ms), 4),
                            "p50": _percentile(latencies_ms, 0.50),
                            "p95": _percentile(latencies_ms, 0.95),
                            "max": round(max(latencies_ms), 4),
                        },
                    }
                )
            return {
                "workspace_id": workspace.workspace_id,
                "cycle_id": cycle.cycle_id,
                "organization_id": organization_id,
                "corpus_size": corpus_size,
                "dispatched_import_jobs": dispatch.dispatched_count,
                "requested_mode": vector_search_mode or "auto",
                "effective_mode": effective_mode,
                "runtime_capabilities": capabilities.model_dump(mode="json"),
                "query_reports": query_reports,
                "aggregate_latency_ms": {
                    "min": round(min(aggregate_latencies), 4),
                    "avg": round(fmean(aggregate_latencies), 4),
                    "p50": _percentile(aggregate_latencies, 0.50),
                    "p95": _percentile(aggregate_latencies, 0.95),
                    "max": round(max(aggregate_latencies), 4),
                },
            }
        finally:
            service.close()


def run_vector_search_mode_comparison(
    *,
    modes: list[str],
    expected_effective_modes: dict[str, str] | None = None,
    database_url: str | None = None,
    corpus_size: int = 40,
    iterations: int = 5,
    queries: list[str] | None = None,
) -> dict[str, object]:
    normalized_modes = _normalize_requested_vector_modes(modes)
    normalized_expected_effective_modes = _normalize_expected_effective_modes(expected_effective_modes)
    unexpected_modes = sorted(set(normalized_expected_effective_modes) - set(normalized_modes))
    if unexpected_modes:
        raise ValueError(
            "Expected effective modes configured for unrequested vector modes: "
            + ", ".join(unexpected_modes)
        )
    reports = [
        run_vector_search_benchmark(
            database_url=database_url,
            vector_search_mode=mode,
            expected_effective_mode=normalized_expected_effective_modes.get(mode),
            corpus_size=corpus_size,
            iterations=iterations,
            queries=queries,
        )
        for mode in normalized_modes
    ]
    comparison_rows: list[dict[str, object]] = []
    for report in reports:
        vector_capability = _extract_vector_capability(report)
        vector_details = _extract_vector_details(report)
        aggregate = dict(report.get("aggregate_latency_ms") or {})
        comparison_rows.append(
            {
                "requested_mode": report["requested_mode"],
                "effective_mode": report["effective_mode"],
                "backend_id": vector_capability.get("backend_id"),
                "fallback_reason": vector_capability.get("fallback_reason"),
                "avg_latency_ms": float(aggregate.get("avg") or 0.0),
                "p50_latency_ms": float(aggregate.get("p50") or 0.0),
                "p95_latency_ms": float(aggregate.get("p95") or 0.0),
                "max_latency_ms": float(aggregate.get("max") or 0.0),
                "query_count": len(report.get("query_reports") or []),
                "pgvector_index_ready": bool(vector_details.get("pgvector_index_ready", False)),
                "pgvector_index_reason": vector_details.get("pgvector_index_reason"),
                "pgvector_dimension_supported": bool(
                    vector_details.get("pgvector_dimension_supported", False)
                ),
            }
        )
    sorted_rows = sorted(comparison_rows, key=lambda item: (item["avg_latency_ms"], item["p95_latency_ms"]))
    fastest = sorted_rows[0] if sorted_rows else None
    slowest = sorted_rows[-1] if sorted_rows else None
    fastest_avg_latency = float(fastest["avg_latency_ms"]) if fastest is not None else 0.0
    for index, row in enumerate(sorted_rows, start=1):
        row["rank"] = index
        row["avg_latency_delta_vs_fastest_ms"] = round(float(row["avg_latency_ms"]) - fastest_avg_latency, 4)
    return {
        "modes": normalized_modes,
        "reports": reports,
        "comparison": sorted_rows,
        "summary": {
            "compared_mode_count": len(sorted_rows),
            "unique_effective_modes": list(dict.fromkeys(str(report["effective_mode"]) for report in reports)),
            "fastest_requested_mode": (fastest["requested_mode"] if fastest is not None else None),
            "fastest_effective_mode": (fastest["effective_mode"] if fastest is not None else None),
            "slowest_requested_mode": (slowest["requested_mode"] if slowest is not None else None),
            "slowest_effective_mode": (slowest["effective_mode"] if slowest is not None else None),
            "avg_latency_spread_ms": (
                round(float(slowest["avg_latency_ms"]) - float(fastest["avg_latency_ms"]), 4)
                if fastest is not None and slowest is not None
                else 0.0
            ),
        },
    }
