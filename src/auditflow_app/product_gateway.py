from __future__ import annotations

import json
import re
from typing import Any

from .shared_runtime import load_shared_agent_platform


class _HeuristicAuditFlowProductModelGateway:
    def __init__(self) -> None:
        self._shared_platform = load_shared_agent_platform()

    def generate(self, *, assembled_prompt) -> Any:
        bundle_id = str(assembled_prompt.bundle_id)
        if bundle_id == "auditflow.collector":
            payload = self._collector_response(assembled_prompt)
        elif bundle_id == "auditflow.mapper":
            payload = self._mapper_response(assembled_prompt)
        elif bundle_id == "auditflow.skeptic":
            payload = self._skeptic_response(assembled_prompt)
        elif bundle_id == "auditflow.writer":
            payload = self._writer_response(assembled_prompt)
        else:
            raise ValueError(f"Unsupported AuditFlow product bundle: {bundle_id}")
        return self._shared_platform.ModelGatewayResponse.model_validate(payload)

    @staticmethod
    def _tool_names(assembled_prompt) -> set[str]:
        return {str(tool.tool_name) for tool in assembled_prompt.tool_manifest}

    @staticmethod
    def _tokenize(value: str) -> set[str]:
        return set(re.findall(r"[a-z0-9]+", value.lower()))

    @classmethod
    def _best_title(cls, value: str, *, fallback: str) -> str:
        for line in value.splitlines():
            candidate = line.strip()
            if candidate:
                return candidate[:120]
        return fallback

    @staticmethod
    def _citation_id(ref: dict[str, Any]) -> str | None:
        for key in ("id", "evidence_chunk_id", "artifact_id", "mapping_id", "control_state_id"):
            if ref.get(key):
                return str(ref[key])
        return None

    @classmethod
    def _citation_ref(cls, ref: dict[str, Any], *, fallback_kind: str, fallback_id: str) -> dict[str, str]:
        citation_id = cls._citation_id(ref) or fallback_id
        citation_kind = str(ref.get("kind") or fallback_kind)
        return {"kind": citation_kind, "id": citation_id}

    @classmethod
    def _collector_response(cls, assembled_prompt) -> dict[str, Any]:
        variables = assembled_prompt.resolved_variables
        artifact_id = str(variables.get("artifact_id") or "artifact-unknown")
        allowed_types = list(variables.get("allowed_evidence_types") or ["document"])
        summary_source = str(variables.get("extracted_text_or_summary") or artifact_id)
        tool_calls: list[dict[str, Any]] = []
        if "artifact.read" in cls._tool_names(assembled_prompt):
            tool_calls.append(
                {
                    "tool_name": "artifact.read",
                    "tool_version": "2026-03-16.1",
                    "arguments": {"artifact_id": artifact_id},
                }
            )
        return {
            "agent_output": {
                "status": "success",
                "summary": "Collected evidence from the product runtime.",
                "structured_output": {
                    "normalized_title": cls._best_title(summary_source, fallback=artifact_id),
                    "evidence_type": str(allowed_types[0] if allowed_types else "document"),
                    "summary": summary_source[:500],
                    "captured_at": None,
                    "fresh_until": None,
                    "citation_refs": [{"kind": "artifact", "id": artifact_id}],
                },
                "citations": [{"kind": "artifact", "id": artifact_id}],
            },
            "planned_tool_calls": tool_calls,
        }

    @classmethod
    def _mapper_response(cls, assembled_prompt) -> dict[str, Any]:
        variables = assembled_prompt.resolved_variables
        controls = list(variables.get("in_scope_controls") or [])
        evidence_refs = list(variables.get("evidence_chunk_refs") or [])
        memories = list(variables.get("accepted_pattern_memories") or [])
        framework_name = str(variables.get("framework_name") or "framework")
        evidence_text = "\n".join(
            str(ref.get("text_excerpt") or ref.get("summary") or ref.get("id") or "")
            for ref in evidence_refs
        )
        evidence_tokens = cls._tokenize(evidence_text)

        best_control = None
        best_score = -1.0
        for control in controls:
            if isinstance(control, str):
                control = {"control_id": control, "control_code": control}
            control_text = "\n".join(
                str(control.get(key) or "")
                for key in ("control_code", "title", "description", "guidance_markdown")
            )
            control_tokens = cls._tokenize(control_text)
            overlap = len(evidence_tokens & control_tokens)
            memory_bonus = 0.0
            control_code = str(control.get("control_code") or control.get("control_state_id") or control.get("control_id") or "")
            for memory in memories:
                if str(memory.get("control_code") or "") == control_code:
                    memory_bonus += float(memory.get("confidence") or 0.0) * 0.1
            score = overlap + memory_bonus
            if score > best_score:
                best_control = control
                best_score = score

        selected_control = best_control or (
            controls[0] if controls else {"control_state_id": f"{framework_name}-control", "control_code": framework_name}
        )
        if isinstance(selected_control, str):
            selected_control = {"control_state_id": selected_control, "control_code": selected_control}
        first_ref = evidence_refs[0] if evidence_refs else {"kind": "evidence_chunk", "id": "chunk-unknown"}
        citation = cls._citation_ref(first_ref, fallback_kind="evidence_chunk", fallback_id="chunk-unknown")
        rationale = (
            f"Evidence language aligns best with {selected_control.get('control_code') or selected_control.get('control_state_id')}."
        )
        ranking_score = round(min(0.55 + (max(best_score, 0.0) * 0.08), 0.98), 4)
        return {
            "agent_output": {
                "status": "success",
                "summary": "Generated grounded mapping candidates from current cycle context.",
                "structured_output": {
                    "mapping_candidates": [
                        {
                            "control_id": str(
                                selected_control.get("control_state_id")
                                or selected_control.get("control_id")
                                or selected_control.get("control_code")
                                or "control-unknown"
                            ),
                            "confidence": round(min(ranking_score, 0.95), 4),
                            "ranking_score": ranking_score,
                            "rationale": rationale,
                            "citation_refs": [citation],
                        }
                    ]
                },
                "citations": [citation],
            }
        }

    @classmethod
    def _skeptic_response(cls, assembled_prompt) -> dict[str, Any]:
        variables = assembled_prompt.resolved_variables
        mapping_payloads = list(variables.get("mapping_payloads") or [])
        freshness_policy = dict(variables.get("freshness_policy") or {})
        challenge_memories = list(variables.get("challenge_pattern_memories") or [])
        mapping_id = "mapping-1"
        if mapping_payloads and isinstance(mapping_payloads[0], dict) and mapping_payloads[0].get("mapping_id"):
            mapping_id = str(mapping_payloads[0]["mapping_id"])
        severity = "low"
        if int(freshness_policy.get("max_age_days") or 90) <= 30 or challenge_memories:
            severity = "medium"
        first_ref = {"kind": "mapping", "id": mapping_id}
        return {
            "agent_output": {
                "status": "success",
                "summary": "Reviewed mapping risk against freshness policy and prior reviewer feedback.",
                "structured_output": {
                    "mapping_flags": [
                        {
                            "mapping_id": mapping_id,
                            "issue_type": "needs_reviewer_confirmation",
                            "severity": severity,
                            "recommended_action": "Confirm the mapping against the most recent evidence and reviewer history.",
                        }
                    ],
                    "gaps": [],
                },
                "citations": [first_ref],
            }
        }

    @classmethod
    def _writer_response(cls, assembled_prompt) -> dict[str, Any]:
        variables = assembled_prompt.resolved_variables
        cycle_id = str(variables.get("audit_cycle_id") or "cycle-unknown")
        snapshot_version = int(variables.get("working_snapshot_version") or 0)
        accepted_mapping_refs = list(variables.get("accepted_mapping_refs") or [])
        open_gap_refs = list(variables.get("open_gap_refs") or [])
        citation_id = str(accepted_mapping_refs[0] if accepted_mapping_refs else open_gap_refs[0] if open_gap_refs else cycle_id)
        control_state_id = (
            str(accepted_mapping_refs[0]) if accepted_mapping_refs else f"{cycle_id}-snapshot-{snapshot_version}"
        )
        tool_calls: list[dict[str, Any]] = []
        tool_names = cls._tool_names(assembled_prompt)
        if "narrative.snapshot_read" in tool_names:
            tool_calls.append(
                {
                    "tool_name": "narrative.snapshot_read",
                    "tool_version": "2026-03-16.1",
                    "arguments": {
                        "audit_cycle_id": cycle_id,
                        "working_snapshot_version": snapshot_version,
                    },
                }
            )
        if "export.snapshot_validate" in tool_names:
            tool_calls.append(
                {
                    "tool_name": "export.snapshot_validate",
                    "tool_version": "2026-03-16.1",
                    "arguments": {
                        "audit_cycle_id": cycle_id,
                        "working_snapshot_version": snapshot_version,
                    },
                }
            )
        return {
            "agent_output": {
                "status": "success",
                "summary": "Generated frozen-snapshot package narratives from live product state.",
                "structured_output": {
                    "narratives": [
                        {
                            "control_state_id": control_state_id,
                            "narrative_type": "control_summary",
                            "content_markdown": (
                                f"Snapshot {snapshot_version} for cycle `{cycle_id}` packages accepted mappings "
                                f"and remaining gaps using repository-backed product data."
                            ),
                            "citation_refs": [{"kind": "mapping", "id": citation_id}],
                        }
                    ]
                },
                "citations": [{"kind": "mapping", "id": citation_id}],
            },
            "planned_tool_calls": tool_calls,
        }


class _OpenAIResponsesAuditFlowProductGateway:
    def __init__(
        self,
        *,
        model_name: str,
        api_key: str,
        base_url: str | None = None,
        timeout_seconds: float = 30.0,
        client=None,
    ) -> None:
        self._shared_platform = load_shared_agent_platform()
        self._response_model = self._shared_platform.ModelGatewayResponse
        if client is None:
            from openai import OpenAI

            client = OpenAI(
                api_key=api_key,
                base_url=base_url,
                timeout=timeout_seconds,
            )
        self._client = client
        self.model_name = model_name

    def generate(self, *, assembled_prompt) -> Any:
        response = self._client.responses.parse(
            model=self.model_name,
            instructions=self._build_instructions(assembled_prompt),
            input=self._render_prompt(assembled_prompt),
            text_format=self._response_model,
            max_output_tokens=1200,
        )
        parsed = getattr(response, "output_parsed", None)
        if parsed is None:
            raise ValueError("AUDITFLOW_MODEL_RESPONSE_EMPTY")
        if isinstance(parsed, self._response_model):
            return parsed
        return self._response_model.model_validate(parsed)

    @staticmethod
    def _build_instructions(assembled_prompt) -> str:
        tool_lines = [
            f"- {tool.tool_name}@{tool.tool_version}"
            for tool in assembled_prompt.tool_manifest
        ]
        tool_manifest = "\n".join(tool_lines) if tool_lines else "- none"
        return (
            "Return strictly valid JSON matching the provided schema. "
            "Do not invent tool names, IDs, or citations not grounded in the prompt. "
            "If you plan a tool call, it must come from the allowed tool manifest.\n\n"
            f"Allowed tool manifest:\n{tool_manifest}"
        )

    @staticmethod
    def _render_prompt(assembled_prompt) -> str:
        rendered_parts: list[str] = [
            f"bundle_id: {assembled_prompt.bundle_id}",
            f"bundle_version: {assembled_prompt.bundle_version}",
            f"agent_name: {assembled_prompt.agent_name}",
            f"workflow_type: {assembled_prompt.workflow_type}",
            f"citation_policy_id: {assembled_prompt.citation_policy_id}",
            f"response_schema_ref: {assembled_prompt.response_schema_ref}",
        ]
        for part in assembled_prompt.parts:
            rendered_parts.append(f"\n## {part.name}")
            rendered_parts.append(part.description)
            if part.instructions:
                rendered_parts.append("instructions:")
                rendered_parts.extend(f"- {instruction}" for instruction in part.instructions)
            rendered_parts.append("variables:")
            rendered_parts.append(json.dumps(part.variables, ensure_ascii=True, indent=2, default=str))
        return "\n".join(rendered_parts)


class AuditFlowProductModelGateway:
    def __init__(
        self,
        *,
        primary_gateway=None,
        fallback_gateway=None,
        allow_fallback: bool | None = None,
    ) -> None:
        self._shared_platform = load_shared_agent_platform()
        self._fallback_gateway = fallback_gateway or _HeuristicAuditFlowProductModelGateway()
        self._requested_provider_mode = self._shared_platform.normalize_requested_mode(
            self._env_value("AUDITFLOW_MODEL_PROVIDER"),
            allowed_modes=("auto", "local", "openai"),
            default="auto",
        )
        self._configured_model_name = self._env_value("AUDITFLOW_OPENAI_MODEL")
        self._provider_mode_decision = None
        if primary_gateway is None:
            primary_gateway, default_allow_fallback = self._build_primary_gateway()
        else:
            default_allow_fallback = True
        self._primary_gateway = primary_gateway
        self._allow_fallback = default_allow_fallback if allow_fallback is None else allow_fallback

    @staticmethod
    def _env_value(name: str) -> str | None:
        return load_shared_agent_platform().env_value(name)

    def _build_primary_gateway(self):
        provider = self._requested_provider_mode
        api_key = self._env_value("OPENAI_API_KEY")
        model_name = self._env_value("AUDITFLOW_OPENAI_MODEL")
        decision = self._shared_platform.resolve_remote_mode(
            requested_mode=provider,
            allowed_modes=("auto", "local", "openai"),
            local_mode="local",
            remote_mode="openai",
            has_remote_configuration=api_key is not None and model_name is not None,
            strict_remote_mode="openai",
            strict_missing_error="AUDITFLOW_OPENAI_MODEL",
            auto_fallback_reason="MODEL_PROVIDER_NOT_CONFIGURED",
        )
        self._provider_mode_decision = decision
        if not decision.use_remote:
            return None, decision.allow_fallback
        try:
            gateway = _OpenAIResponsesAuditFlowProductGateway(
                model_name=str(model_name),
                api_key=str(api_key),
                base_url=self._env_value("AUDITFLOW_OPENAI_BASE_URL"),
                timeout_seconds=float(self._env_value("AUDITFLOW_OPENAI_TIMEOUT_SECONDS") or 30.0),
            )
        except Exception:
            if provider == "openai":
                raise
            self._provider_mode_decision = self._shared_platform.RuntimeModeDecision(
                requested_mode=decision.requested_mode,
                effective_mode="local",
                use_remote=False,
                allow_fallback=True,
                fallback_reason="MODEL_PROVIDER_INIT_FAILED",
            )
            return None, True
        return gateway, decision.allow_fallback

    def generate(self, *, assembled_prompt) -> Any:
        if self._primary_gateway is None:
            return self._fallback_gateway.generate(assembled_prompt=assembled_prompt)
        try:
            return self._primary_gateway.generate(assembled_prompt=assembled_prompt)
        except Exception:
            if not self._allow_fallback:
                raise
            return self._fallback_gateway.generate(assembled_prompt=assembled_prompt)

    def describe_capability(self) -> dict[str, object]:
        decision = self._provider_mode_decision or self._shared_platform.RuntimeModeDecision(
            requested_mode=self._requested_provider_mode,
            effective_mode="local" if self._primary_gateway is None else "openai",
            use_remote=self._primary_gateway is not None,
            allow_fallback=self._allow_fallback,
            fallback_reason=(
                "MODEL_PROVIDER_NOT_CONFIGURED"
                if self._primary_gateway is None and self._requested_provider_mode == "auto"
                else None
            ),
        )
        return self._shared_platform.RuntimeCapabilityDescriptor(
            requested_mode=decision.requested_mode,
            effective_mode=decision.effective_mode,
            backend_id="openai-responses" if decision.effective_mode == "openai" else "heuristic-local",
            fallback_reason=decision.fallback_reason,
            details={
                "configured_model": self._configured_model_name,
                "fallback_enabled": self._allow_fallback,
            },
        ).as_dict()
