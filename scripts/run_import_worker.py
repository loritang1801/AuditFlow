from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_import_worker
from auditflow_app.sample_payloads import upload_import_command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the AuditFlow import worker.")
    parser.add_argument("--poll", action="store_true", help="Run polling mode instead of a single dispatch.")
    parser.add_argument(
        "--supervise",
        action="store_true",
        help="Run the worker under a supervisor loop with retries and heartbeat output.",
    )
    parser.add_argument(
        "--forever",
        action="store_true",
        help="Remove the iteration cap. Combine with --max-idle-polls 0 for a long-running worker.",
    )
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds.")
    parser.add_argument("--iterations", type=int, default=1, help="Maximum polling iterations.")
    parser.add_argument(
        "--max-idle-polls",
        type=int,
        default=1,
        help="Stop after this many idle polls. Use 0 to disable the idle stop condition.",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=3,
        help="Supervisor-only: stop after this many consecutive dispatch exceptions.",
    )
    parser.add_argument(
        "--failure-backoff",
        type=float,
        default=5.0,
        help="Supervisor-only: seconds to wait before retrying after a dispatch exception.",
    )
    parser.add_argument(
        "--heartbeat-every",
        type=int,
        default=1,
        help="Supervisor-only: emit an idle heartbeat every N iterations.",
    )
    parser.add_argument("--seed-upload", action="store_true", help="Seed one demo upload before running.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    worker = build_import_worker()
    try:
        if args.seed_upload:
            worker.app_service.create_upload_import("cycle-1", upload_import_command())
        max_iterations = None if args.forever else args.iterations
        max_idle_polls = None if args.max_idle_polls <= 0 else args.max_idle_polls
        if args.supervise:
            supervisor = worker.build_supervisor()

            def emit_heartbeat(heartbeat) -> None:
                print(json.dumps(heartbeat.to_dict()))

            heartbeats = supervisor.run(
                poll_interval_seconds=args.interval,
                max_iterations=max_iterations,
                max_idle_polls=max_idle_polls,
                max_consecutive_failures=args.max_consecutive_failures,
                failure_backoff_seconds=args.failure_backoff,
                heartbeat_every_iterations=args.heartbeat_every,
                heartbeat_callback=emit_heartbeat,
            )
            print(json.dumps({"heartbeat_count": len(heartbeats)}))
        elif args.poll:
            results = worker.run_polling(
                poll_interval_seconds=args.interval,
                max_iterations=max_iterations,
                max_idle_polls=max_idle_polls,
            )
            print(json.dumps([result.model_dump() for result in results], indent=2))
        else:
            result = worker.dispatch_once()
            print(json.dumps(result.model_dump(), indent=2))
    finally:
        worker.app_service.close()


if __name__ == "__main__":
    main()
