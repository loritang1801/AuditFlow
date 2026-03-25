from __future__ import annotations

from typing import Protocol

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .shared_runtime import load_shared_agent_platform

_AP = load_shared_agent_platform()

ROLE_PRIORITY = {
    "viewer": 1,
    "reviewer": 2,
    "product_admin": 3,
}

ROLE_ALIASES = {
    "org_admin": "product_admin",
}

DEFAULT_AUTH_SECRET = "auditflow-dev-secret"
DEFAULT_ACCESS_TTL_SECONDS = 60 * 60
DEFAULT_REFRESH_TTL_DAYS = 30

AuditFlowAccessContext = _AP.AuthAccessContext
SessionUser = _AP.SessionUser
SessionOrganization = _AP.SessionOrganization
SessionMembership = _AP.SessionMembership
SessionResponse = _AP.SessionResponse
CurrentUserResponse = _AP.CurrentUserResponse
SessionCreateCommand = _AP.SessionCreateCommand
AuthSessionIssue = _AP.AuthSessionIssue
AuthBase = _AP.AuthBase
OrganizationRow = _AP.OrganizationRow
AppUserRow = _AP.AppUserRow
OrganizationMembershipRow = _AP.OrganizationMembershipRow
AuthSessionRow = _AP.AuthSessionRow


class AuditFlowAuthorizationError(_AP.SharedAuthorizationError):
    pass


class AuditFlowAuthorizer(Protocol):
    def authorize(
        self,
        *,
        required_role: str,
        authorization: str | None,
        organization_id: str | None,
        user_id: str | None = None,
        user_role: str | None = None,
    ) -> AuditFlowAccessContext: ...


class HeaderAuditFlowAuthorizer(_AP.HeaderRoleAuthorizer):
    def __init__(self, *, default_role: str = "viewer") -> None:
        super().__init__(
            default_role=default_role,
            role_priority=ROLE_PRIORITY,
            role_aliases=ROLE_ALIASES,
            error_type=AuditFlowAuthorizationError,
        )


class SessionTokenAuditFlowAuthorizer(_AP.SessionTokenAuthorizer):
    pass


class AccessTokenCodec(_AP.AccessTokenCodec):
    def __init__(self, secret: str, *, ttl_seconds: int = DEFAULT_ACCESS_TTL_SECONDS) -> None:
        super().__init__(
            secret,
            ttl_seconds=ttl_seconds,
            error_type=AuditFlowAuthorizationError,
        )


def create_auth_tables(engine: Engine) -> None:
    _AP.create_auth_tables(engine)


class SqlAlchemyAuditFlowAuthService(_AP.SqlAlchemyPlatformAuthService):
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        engine: Engine,
        *,
        auth_secret: str | None = None,
        access_ttl_seconds: int = DEFAULT_ACCESS_TTL_SECONDS,
        refresh_ttl_days: int = DEFAULT_REFRESH_TTL_DAYS,
    ) -> None:
        super().__init__(
            session_factory,
            engine,
            auth_secret=auth_secret,
            auth_secret_env_name="AUDITFLOW_AUTH_SECRET",
            default_auth_secret=DEFAULT_AUTH_SECRET,
            access_ttl_seconds=access_ttl_seconds,
            refresh_ttl_days=refresh_ttl_days,
            role_priority=ROLE_PRIORITY,
            role_aliases=ROLE_ALIASES,
            error_type=AuditFlowAuthorizationError,
            seed_organizations=(
                _AP.SeedOrganization(
                    organization_id="org-1",
                    name="Acme",
                    slug="acme",
                    status="active",
                    settings_json={},
                ),
            ),
            seed_user_memberships=(
                _AP.SeedUserMembership(
                    user_id="user-viewer-1",
                    email="viewer@example.com",
                    display_name="Audit Viewer",
                    password="auditflow-demo",
                    organization_id="org-1",
                    role="viewer",
                ),
                _AP.SeedUserMembership(
                    user_id="user-reviewer-1",
                    email="reviewer@example.com",
                    display_name="Audit Reviewer",
                    password="auditflow-demo",
                    organization_id="org-1",
                    role="reviewer",
                ),
                _AP.SeedUserMembership(
                    user_id="user-admin-1",
                    email="admin@example.com",
                    display_name="Audit Admin",
                    password="auditflow-demo",
                    organization_id="org-1",
                    role="org_admin",
                ),
            ),
        )

    @classmethod
    def from_runtime_stores(cls, runtime_stores) -> "SqlAlchemyAuditFlowAuthService":
        return cls(runtime_stores.session_factory, runtime_stores.engine)

    def build_authorizer(self) -> SessionTokenAuditFlowAuthorizer:
        return SessionTokenAuditFlowAuthorizer(self)
