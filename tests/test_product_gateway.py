from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.bootstrap import build_app_service
from auditflow_app.connectors import EnvConfiguredConnectorResolver
from auditflow_app.product_gateway import (
    AuditFlowProductModelGateway,
    _OpenAIResponsesAuditFlowProductGateway,
)
from auditflow_app.shared_runtime import load_shared_agent_platform


def _assembled_prompt(bundle_id: str = "auditflow.collector"):
    return SimpleNamespace(
        bundle_id=bundle_id,
        bundle_version="2026-03-16.1",
        agent_name="auditflow-test-agent",
        workflow_type="auditflow_cycle",
        citation_policy_id="required",
        response_schema_ref="agent_output",
        resolved_variables={
            "artifact_id": "artifact-1",
            "allowed_evidence_types": ["ticket"],
            "extracted_text_or_summary": "Quarterly access review completed for production systems.",
            "evidence_chunk_refs": [{"kind": "evidence_chunk", "id": "chunk-1"}],
            "in_scope_controls": [{"control_state_id": "control-state-1", "control_code": "CC6.1"}],
            "mapping_payloads": [{"mapping_id": "mapping-1"}],
            "audit_cycle_id": "cycle-1",
            "working_snapshot_version": 1,
            "accepted_mapping_refs": ["mapping-1"],
            "open_gap_refs": [],
            "framework_name": "SOC2",
        },
        tool_manifest=[
            SimpleNamespace(tool_name="artifact.read", tool_version="2026-03-16.1"),
            SimpleNamespace(tool_name="narrative.snapshot_read", tool_version="2026-03-16.1"),
            SimpleNamespace(tool_name="export.snapshot_validate", tool_version="2026-03-16.1"),
        ],
        parts=[
            SimpleNamespace(
                name="runtime_context",
                description="Current workflow state",
                instructions=["Stay grounded in the provided evidence."],
                variables={"artifact_id": "artifact-1", "audit_cycle_id": "cycle-1"},
            )
        ],
    )


class _BrokenGateway:
    def generate(self, *, assembled_prompt):
        raise RuntimeError("boom")


class _FakeResponsesClient:
    def __init__(self, parsed_payload) -> None:
        self._parsed_payload = parsed_payload
        self.responses = SimpleNamespace(parse=self._parse)

    def _parse(self, **kwargs):
        return SimpleNamespace(output_parsed=self._parsed_payload)


class _FakeEmbeddingClient:
    def __init__(self, vector, *, should_fail: bool = False) -> None:
        self._vector = vector
        self._should_fail = should_fail
        self.embeddings = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        if self._should_fail:
            raise RuntimeError("embedding failure")
        return SimpleNamespace(data=[SimpleNamespace(embedding=list(self._vector))])


class AuditFlowProductGatewayTests(unittest.TestCase):
    def test_shared_runtime_remote_mode_helper_requires_strict_remote_configuration(self) -> None:
        shared_platform = load_shared_agent_platform()

        with self.assertRaisesRegex(ValueError, "AUDITFLOW_OPENAI_MODEL"):
            shared_platform.resolve_remote_mode(
                requested_mode="openai",
                allowed_modes=("auto", "local", "openai"),
                local_mode="local",
                remote_mode="openai",
                has_remote_configuration=False,
                strict_remote_mode="openai",
                strict_missing_error="AUDITFLOW_OPENAI_MODEL",
                auto_fallback_reason="MODEL_PROVIDER_NOT_CONFIGURED",
            )

    def test_connector_capability_uses_shared_runtime_mode_resolution(self) -> None:
        resolver = EnvConfiguredConnectorResolver()

        capability = resolver.describe_capability("jira")

        self.assertEqual(capability["requested_mode"], "auto")
        self.assertEqual(capability["effective_mode"], "local")
        self.assertEqual(capability["fallback_reason"], "CONNECTOR_HTTP_TEMPLATE_NOT_CONFIGURED")
        self.assertEqual(capability["backend_id"], "jira-synthetic")

    def test_gateway_falls_back_to_heuristic_when_primary_fails(self) -> None:
        gateway = AuditFlowProductModelGateway(
            primary_gateway=_BrokenGateway(),
            allow_fallback=True,
        )

        response = gateway.generate(assembled_prompt=_assembled_prompt())

        self.assertEqual(response.agent_output.status, "success")
        self.assertEqual(response.agent_output.citations[0].id, "artifact-1")

    def test_openai_gateway_accepts_parsed_model_gateway_response(self) -> None:
        shared_platform = load_shared_agent_platform()
        parsed_payload = shared_platform.ModelGatewayResponse.model_validate(
            {
                "agent_output": {
                    "status": "success",
                    "summary": "Generated response.",
                    "structured_output": {"mapping_candidates": []},
                    "citations": [{"kind": "artifact", "id": "artifact-1"}],
                },
                "planned_tool_calls": [],
            }
        )
        gateway = _OpenAIResponsesAuditFlowProductGateway(
            model_name="test-model",
            api_key="test-key",
            client=_FakeResponsesClient(parsed_payload),
        )

        response = gateway.generate(assembled_prompt=_assembled_prompt("auditflow.mapper"))

        self.assertEqual(response.agent_output.summary, "Generated response.")
        self.assertEqual(response.agent_output.citations[0].id, "artifact-1")

    def test_repository_uses_external_embedding_client_when_present(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)
        repository = service.repository
        repository.embedding_provider_mode = "openai"
        repository._allow_embedding_fallback = False
        repository.semantic_model_name = "openai:test-embedding"
        repository.semantic_vector_dimension = 4
        repository._openai_embedding_client = _FakeEmbeddingClient([3.0, 4.0, 0.0, 0.0])

        payload = repository._build_semantic_embedding_payload(
            text_content="Quarterly access review completed.",
            metadata_payload={"title": "Access Review Export", "summary": "Quarterly review summary"},
        )

        self.assertEqual(payload["embedding_model_name"], "openai:test-embedding")
        self.assertEqual(len(payload["embedding_vector"]), 4)
        self.assertAlmostEqual(payload["embedding_vector"][0], 0.6, places=2)
        self.assertAlmostEqual(payload["embedding_vector"][1], 0.8, places=2)

    def test_repository_falls_back_to_local_embedding_when_auto_provider_fails(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)
        repository = service.repository
        repository.embedding_provider_mode = "auto"
        repository._allow_embedding_fallback = True
        repository.semantic_model_name = "openai:test-embedding"
        repository.semantic_vector_dimension = 4
        repository._openai_embedding_client = _FakeEmbeddingClient([1.0, 0.0, 0.0, 0.0], should_fail=True)

        payload = repository._build_semantic_embedding_payload(
            text_content="Quarterly access review completed.",
            metadata_payload={"title": "Access Review Export", "summary": "Quarterly review summary"},
        )

        self.assertEqual(len(payload["embedding_vector"]), 96)
        self.assertEqual(payload["embedding_provider"], "openai")

    def test_service_runtime_capabilities_report_default_fallbacks(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        capabilities = service.get_runtime_capabilities()

        self.assertEqual(capabilities.product, "auditflow")
        self.assertEqual(capabilities.model_provider.effective_mode, "local")
        self.assertEqual(capabilities.embedding_provider.effective_mode, "local")
        self.assertEqual(capabilities.vector_search.backend_id, "ann-metadata-json")
        self.assertIn("jira", capabilities.connectors)

    def test_repository_reports_pgvector_request_fallback_when_backend_unavailable(self) -> None:
        with patch.dict(
            "os.environ",
            {"AUDITFLOW_VECTOR_SEARCH_MODE": "pgvector"},
            clear=False,
        ):
            service = build_app_service()
        self.addCleanup(service.close)

        capabilities = service.get_runtime_capabilities()

        self.assertEqual(capabilities.vector_search.requested_mode, "pgvector")
        self.assertEqual(capabilities.vector_search.effective_mode, "ann")
        self.assertEqual(capabilities.vector_search.fallback_reason, "PGVECTOR_BACKEND_NOT_AVAILABLE")


if __name__ == "__main__":
    unittest.main()
