from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_replay_harness
from auditflow_app.replay_harness import load_replay_baseline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the AuditFlow replay harness.")
    parser.add_argument(
        "--scenario-name",
        default="demo_cycle_export",
        help="Scenario name to capture when no baseline file is provided.",
    )
    parser.add_argument(
        "--baseline",
        help="Existing baseline JSON artifact path to evaluate against.",
    )
    parser.add_argument(
        "--capture-only",
        action="store_true",
        help="Capture a baseline and stop without running evaluation.",
    )
    parser.add_argument(
        "--database-url",
        help="Optional SQLAlchemy database URL used for each harness service instance.",
    )
    parser.add_argument(
        "--baseline-root",
        help="Optional directory for captured baseline artifacts.",
    )
    parser.add_argument(
        "--report-root",
        help="Optional directory for generated replay reports.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    harness = build_replay_harness(
        database_url=args.database_url,
        baseline_root=args.baseline_root,
        report_root=args.report_root,
    )
    if args.baseline:
        baseline = load_replay_baseline(args.baseline)
    else:
        baseline = harness.capture_demo_baseline(scenario_name=args.scenario_name)
    if args.capture_only:
        print(json.dumps({"baseline": baseline.model_dump(mode="json")}, indent=2))
        return
    report = harness.evaluate_demo_scenario(baseline)
    print(
        json.dumps(
            {
                "baseline": baseline.model_dump(mode="json"),
                "report": report.model_dump(mode="json"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
