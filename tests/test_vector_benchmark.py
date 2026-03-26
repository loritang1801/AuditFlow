from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.vector_benchmark import run_vector_search_benchmark, run_vector_search_mode_comparison


class AuditFlowVectorBenchmarkTests(unittest.TestCase):
    def test_benchmark_runs_and_reports_effective_mode(self) -> None:
        report = run_vector_search_benchmark(
            vector_search_mode="ann",
            expected_effective_mode="ann",
            corpus_size=4,
            iterations=1,
            queries=["access review approval", "change approval deployment"],
        )

        self.assertEqual(report["requested_mode"], "ann")
        self.assertEqual(report["effective_mode"], "ann")
        self.assertEqual(report["dispatched_import_jobs"], 4)
        self.assertEqual(len(report["query_reports"]), 2)
        self.assertGreaterEqual(report["query_reports"][0]["result_count"], 1)
        self.assertIn("aggregate_latency_ms", report)

    def test_benchmark_raises_when_effective_mode_does_not_match_expectation(self) -> None:
        with self.assertRaisesRegex(ValueError, "Expected effective vector mode pgvector"):
            run_vector_search_benchmark(
                vector_search_mode="ann",
                expected_effective_mode="pgvector",
                corpus_size=2,
                iterations=1,
                queries=["access review approval"],
            )

    def test_mode_comparison_reports_ranked_rows_and_summary(self) -> None:
        report = run_vector_search_mode_comparison(
            modes=[" ann ", "flat", "ann"],
            expected_effective_modes={"ann": "ann", "flat": "flat"},
            corpus_size=4,
            iterations=1,
            queries=["access review approval"],
        )

        self.assertEqual(report["modes"], ["ann", "flat"])
        self.assertEqual(report["summary"]["compared_mode_count"], 2)
        self.assertEqual(report["summary"]["unique_effective_modes"], ["ann", "flat"])
        comparison = report["comparison"]
        self.assertEqual([row["rank"] for row in comparison], [1, 2])
        self.assertEqual(comparison[0]["avg_latency_delta_vs_fastest_ms"], 0.0)
        self.assertEqual(
            [row["avg_latency_ms"] for row in comparison],
            sorted(row["avg_latency_ms"] for row in comparison),
        )
        self.assertTrue(all("backend_id" in row for row in comparison))
        self.assertTrue(all("pgvector_index_ready" in row for row in comparison))

    def test_mode_comparison_raises_when_expected_mode_does_not_match(self) -> None:
        with self.assertRaisesRegex(ValueError, "Expected effective vector mode pgvector"):
            run_vector_search_mode_comparison(
                modes=["ann"],
                expected_effective_modes={"ann": "pgvector"},
                corpus_size=2,
                iterations=1,
                queries=["access review approval"],
            )


if __name__ == "__main__":
    unittest.main()
