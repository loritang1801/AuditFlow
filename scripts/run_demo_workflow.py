from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_app_service
from auditflow_app.sample_payloads import cycle_processing_command, export_generation_command


def main() -> None:
    service = build_app_service()
    processing = service.process_cycle(cycle_processing_command(workflow_run_id="auditflow-demo-1"))
    exported = service.generate_export(export_generation_command(workflow_run_id="auditflow-demo-2"))
    print(json.dumps({"processing": processing.model_dump(), "export": exported.model_dump()}, indent=2))


if __name__ == "__main__":
    main()
