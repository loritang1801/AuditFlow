from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared_core.agent_platform.product_ci import ProductCiConfig


def build_ci_config() -> ProductCiConfig:
    return ProductCiConfig(
        workflow_name="AuditFlow CI",
        workflow_filename=".github/workflows/auditflow-ci.yml",
        install_spec=".[api,connectors]",
        schema_generator_script="scripts/generate_connector_schemas.py",
        schema_paths=("schemas/connector_contracts",),
        smoke_commands=(
            ("{python}", "scripts/run_demo_workflow.py"),
            ("{python}", "scripts/run_runtime_smoke.py"),
            (
                "{python}",
                "scripts/run_import_worker.py",
                "--poll",
                "--iterations",
                "2",
                "--max-idle-polls",
                "1",
                "--seed-upload",
            ),
            ("{python}", "scripts/run_replay_harness.py"),
        ),
        path_filters=(
            ".github/workflows/auditflow-ci.yml",
            "pyproject.toml",
            "scripts/**",
            "schemas/**",
            "src/**",
            "shared_core/**",
            "tests/**",
            "README.md",
            "PROMPT_TOOL.md",
        ),
    )
