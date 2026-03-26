from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from .auth import AuditFlowAuthorizer, SqlAlchemyAuditFlowAuthService
from .product_gateway import AuditFlowProductModelGateway
from .replay_harness import AuditFlowReplayHarness
from .repository import SqlAlchemyAuditFlowRepository
from .shared_runtime import load_shared_agent_platform
from .service import AuditFlowAppService
from .tool_adapters import register_auditflow_product_tool_adapters
from .worker import AuditFlowImportWorker, AuditFlowImportWorkerSupervisor

SUPPORTED_WORKFLOW_NAMES = (
    "auditflow_cycle_processing",
    "auditflow_export_generation",
)


def _build_registry():
    ap = load_shared_agent_platform()
    base_registry = ap.build_workflow_registry()
    registry = ap.WorkflowRegistry()
    for workflow_name in SUPPORTED_WORKFLOW_NAMES:
        registry.register(base_registry.get(workflow_name))
    return ap, registry


def list_supported_workflows() -> tuple[str, ...]:
    return SUPPORTED_WORKFLOW_NAMES


def _create_runtime_engine(database_url: str | None = None):
    resolved_database_url = database_url or "sqlite+pysqlite:///:memory:"
    if resolved_database_url.startswith("sqlite"):
        engine_kwargs: dict[str, object] = {
            "connect_args": {"check_same_thread": False},
        }
        if ":memory:" in resolved_database_url or "mode=memory" in resolved_database_url:
            engine_kwargs["poolclass"] = StaticPool
        return create_engine(resolved_database_url, **engine_kwargs)
    return create_engine(resolved_database_url)


def build_runtime_components(*, database_url: str | None = None) -> dict[str, Any]:
    ap, registry = _build_registry()
    catalog = ap.build_default_runtime_catalog()
    prompt_service = ap.PromptAssemblyService(catalog)
    tool_executor = ap.ToolExecutor(catalog)
    runtime_engine = _create_runtime_engine(database_url)
    runtime_stores = ap.create_sqlalchemy_runtime_stores(engine=runtime_engine)
    repository = SqlAlchemyAuditFlowRepository.from_runtime_stores(runtime_stores)
    auth_service = SqlAlchemyAuditFlowAuthService.from_runtime_stores(runtime_stores)
    register_auditflow_product_tool_adapters(
        tool_executor,
        repository,
    )
    model_gateway = AuditFlowProductModelGateway()
    execution_service = ap.WorkflowExecutionService(
        prompt_service,
        model_gateway=model_gateway,
        tool_executor=tool_executor,
        state_store=runtime_stores.state_store,
        checkpoint_store=runtime_stores.checkpoint_store,
        replay_store=runtime_stores.replay_store,
        outbox_store=runtime_stores.outbox_store,
    )
    api_service = ap.WorkflowApiService(
        registry,
        execution_service,
        runtime_stores=runtime_stores,
    )
    return {
        "catalog": catalog,
        "prompt_service": prompt_service,
        "workflow_registry": registry,
        "tool_executor": tool_executor,
        "runtime_stores": runtime_stores,
        "repository": repository,
        "auth_service": auth_service,
        "model_gateway": model_gateway,
        "execution_service": execution_service,
        "api_service": api_service,
    }


def build_execution_service(*, database_url: str | None = None):
    components = build_runtime_components(database_url=database_url)
    return components["execution_service"]


def build_api_service(*, database_url: str | None = None):
    components = build_runtime_components(database_url=database_url)
    return components["api_service"]


def build_app_service(*, database_url: str | None = None) -> AuditFlowAppService:
    components = build_runtime_components(database_url=database_url)
    return AuditFlowAppService(
        components["api_service"],
        repository=components["repository"],
        runtime_stores=components["runtime_stores"],
        auth_service=components["auth_service"],
    )


def build_fastapi_app(*, database_url: str | None = None, authorizer: AuditFlowAuthorizer | None = None):
    from .routes import create_fastapi_app

    service = build_app_service(database_url=database_url)
    try:
        return create_fastapi_app(service, authorizer=authorizer)
    except Exception:
        service.close()
        raise


def build_import_worker(*, database_url: str | None = None) -> AuditFlowImportWorker:
    return AuditFlowImportWorker(build_app_service(database_url=database_url))


def build_import_worker_supervisor(*, database_url: str | None = None) -> AuditFlowImportWorkerSupervisor:
    return build_import_worker(database_url=database_url).build_supervisor()


def build_replay_harness(
    *,
    database_url: str | None = None,
    baseline_root: str | None = None,
    report_root: str | None = None,
) -> AuditFlowReplayHarness:
    return AuditFlowReplayHarness(
        service_factory=lambda: build_app_service(database_url=database_url),
        baseline_root=baseline_root,
        report_root=report_root,
    )
