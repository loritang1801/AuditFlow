from __future__ import annotations

import argparse
import json

from _local_runtime import ensure_src_on_path, resolve_database_url

ensure_src_on_path()

from auditflow_app.bootstrap import build_app_service
from auditflow_app.connectors import EnvConfiguredConnectorResolver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local runtime capability smoke checks for AuditFlow.")
    parser.add_argument("--database-url", help="Optional SQLAlchemy database URL.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    resolver = EnvConfiguredConnectorResolver()
    service = build_app_service(database_url=resolve_database_url(args.database_url))
    try:
        capabilities = service.get_runtime_capabilities()
        connector_probes = {
            "jira": resolver.fetch(
                "jira",
                selector="SEC-LOCAL-SMOKE",
                query=None,
                display_name="Local Jira Smoke",
                source_locator="jira://SEC-LOCAL-SMOKE",
                connection_id="smoke-jira",
            ),
            "confluence": resolver.fetch(
                "confluence",
                selector="CONF-LOCAL-SMOKE",
                query=None,
                display_name="Local Confluence Smoke",
                source_locator="confluence://CONF-LOCAL-SMOKE",
                connection_id="smoke-confluence",
            ),
        }
        payload = {
            "product": capabilities.product,
            "model_provider": capabilities.model_provider.model_dump(mode="json"),
            "embedding_provider": capabilities.embedding_provider.model_dump(mode="json"),
            "vector_search": capabilities.vector_search.model_dump(mode="json"),
            "connectors": {
                name: capabilities.connectors[name].model_dump(mode="json")
                for name in ("jira", "confluence")
            },
            "connector_probes": {
                name: {
                    "returned_payload": result is not None,
                    "status": (
                        "local_fallback_ready"
                        if capabilities.connectors[name].effective_mode == "local"
                        else "remote_http_configured"
                    ),
                }
                for name, result in connector_probes.items()
            },
        }
        print(json.dumps(payload, indent=2))
        return 0
    finally:
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())
