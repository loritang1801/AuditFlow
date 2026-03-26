from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.vector_benchmark import (
    run_vector_search_benchmark,
    run_vector_search_mode_comparison,
)


_ALLOWED_REQUESTED_MODES = frozenset(("auto", "ann", "flat", "pgvector"))
_ALLOWED_EFFECTIVE_MODES = frozenset(("ann", "flat", "pgvector"))


def _parse_compare_mode_specs(compare_modes: list[str]) -> tuple[list[str], dict[str, str] | None]:
    requested_modes: list[str] = []
    expected_effective_modes: dict[str, str] = {}
    for raw_spec in compare_modes:
        normalized_spec = raw_spec.strip().lower()
        if not normalized_spec:
            continue
        requested_mode, separator, expected_mode = normalized_spec.partition("=")
        if requested_mode not in _ALLOWED_REQUESTED_MODES:
            raise ValueError(f"Unsupported compare mode: {requested_mode}")
        if requested_mode not in requested_modes:
            requested_modes.append(requested_mode)
        if separator:
            if expected_mode not in _ALLOWED_EFFECTIVE_MODES:
                raise ValueError(f"Unsupported expected effective mode: {expected_mode}")
            expected_effective_modes[requested_mode] = expected_mode
    if not requested_modes:
        raise ValueError("At least one compare mode is required")
    return requested_modes, (expected_effective_modes or None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate AuditFlow vector search runtime mode and benchmark evidence search latency.",
    )
    parser.add_argument("--database-url", default=None, help="SQLAlchemy database URL. Use PostgreSQL for pgvector validation.")
    parser.add_argument(
        "--mode",
        default=None,
        choices=("auto", "ann", "flat", "pgvector"),
        help="Requested AuditFlow vector search mode.",
    )
    parser.add_argument(
        "--expected-effective-mode",
        default=None,
        choices=("ann", "flat", "pgvector"),
        help="Fail if the effective runtime mode differs from this value.",
    )
    parser.add_argument("--corpus-size", type=int, default=40, help="Number of synthetic benchmark evidence documents.")
    parser.add_argument("--iterations", type=int, default=5, help="Search iterations per query.")
    parser.add_argument(
        "--compare-mode",
        action="append",
        dest="compare_modes",
        default=None,
        help="Run multiple requested modes and emit a comparison report. Repeat for multiple modes, optionally as requested=effective.",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        default=None,
        help="Benchmark query to execute. Repeat for multiple queries.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        if args.compare_modes:
            if args.mode is not None:
                raise ValueError("--mode cannot be combined with --compare-mode")
            if args.expected_effective_mode is not None:
                raise ValueError("--expected-effective-mode cannot be combined with --compare-mode")
            compare_modes, expected_effective_modes = _parse_compare_mode_specs(args.compare_modes)
            report = run_vector_search_mode_comparison(
                modes=compare_modes,
                expected_effective_modes=expected_effective_modes,
                database_url=args.database_url,
                corpus_size=args.corpus_size,
                iterations=args.iterations,
                queries=args.queries,
            )
        else:
            report = run_vector_search_benchmark(
                database_url=args.database_url,
                vector_search_mode=args.mode,
                expected_effective_mode=args.expected_effective_mode,
                corpus_size=args.corpus_size,
                iterations=args.iterations,
                queries=args.queries,
            )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
