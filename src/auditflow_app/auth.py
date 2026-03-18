from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

ROLE_PRIORITY = {
    "viewer": 1,
    "reviewer": 2,
    "product_admin": 3,
}

ROLE_ALIASES = {
    "org_admin": "product_admin",
}


class AuditFlowAuthorizationError(Exception):
    def __init__(self, *, code: str, message: str, status_code: int) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(slots=True, frozen=True)
class AuditFlowAccessContext:
    organization_id: str
    user_id: str
    role: str


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


class HeaderAuditFlowAuthorizer:
    def __init__(self, *, default_role: str = "viewer") -> None:
        self.default_role = self._normalize_role(default_role)

    def authorize(
        self,
        *,
        required_role: str,
        authorization: str | None,
        organization_id: str | None,
        user_id: str | None = None,
        user_role: str | None = None,
    ) -> AuditFlowAccessContext:
        required = self._normalize_role(required_role)
        org_id = (organization_id or "").strip()
        token = (authorization or "").strip()
        if not org_id:
            raise AuditFlowAuthorizationError(
                code="TENANT_CONTEXT_REQUIRED",
                message="X-Organization-Id header is required.",
                status_code=400,
            )
        if not token:
            raise AuditFlowAuthorizationError(
                code="AUTH_REQUIRED",
                message="Authorization header is required.",
                status_code=401,
            )
        if not token.lower().startswith("bearer "):
            raise AuditFlowAuthorizationError(
                code="AUTH_INVALID_CREDENTIALS",
                message="Authorization header must use the Bearer scheme.",
                status_code=401,
            )
        role = self._normalize_role(user_role or self.default_role)
        if ROLE_PRIORITY[role] < ROLE_PRIORITY[required]:
            raise AuditFlowAuthorizationError(
                code="AUTH_FORBIDDEN",
                message=f"Role '{role}' does not satisfy required role '{required}'.",
                status_code=403,
            )
        normalized_user_id = (user_id or "demo-user").strip() or "demo-user"
        return AuditFlowAccessContext(
            organization_id=org_id,
            user_id=normalized_user_id,
            role=role,
        )

    @staticmethod
    def _normalize_role(role: str) -> str:
        normalized = ROLE_ALIASES.get(role.strip().lower(), role.strip().lower())
        if normalized not in ROLE_PRIORITY:
            raise AuditFlowAuthorizationError(
                code="AUTH_CONTEXT_INVALID",
                message=f"Unsupported role '{role}'.",
                status_code=400,
            )
        return normalized
