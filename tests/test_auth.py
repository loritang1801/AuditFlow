from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auditflow_app.auth import AuditFlowAuthorizationError
from auditflow_app.bootstrap import build_app_service
from auditflow_app.routes import create_fastapi_app
from auditflow_app.shared_runtime import load_shared_agent_platform

try:
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover - exercised only when fastapi is absent
    TestClient = None


class AuditFlowAuthServiceTests(unittest.TestCase):
    def test_shared_auth_primitives_support_role_policy_and_password_hashing(self) -> None:
        shared_platform = load_shared_agent_platform()
        password_hash = shared_platform.hash_password_pbkdf2("auditflow-demo")
        normalized_role = shared_platform.require_role(
            required_role="product_admin",
            actual_role="org_admin",
            role_priority={"viewer": 1, "reviewer": 2, "product_admin": 3},
            role_aliases={"org_admin": "product_admin"},
            error_type=AuditFlowAuthorizationError,
        )

        self.assertTrue(shared_platform.verify_password_pbkdf2("auditflow-demo", password_hash))
        self.assertFalse(shared_platform.verify_password_pbkdf2("wrong-password", password_hash))
        self.assertEqual(normalized_role, "product_admin")

    def test_shared_access_token_codec_uses_auditflow_error_contract(self) -> None:
        codec = load_shared_agent_platform().AccessTokenCodec(
            "auditflow-test-secret",
            error_type=AuditFlowAuthorizationError,
        )

        with self.assertRaises(AuditFlowAuthorizationError) as context:
            codec.parse("invalid-token")

        self.assertEqual(context.exception.code, "AUTH_INVALID_CREDENTIALS")
        self.assertEqual(context.exception.status_code, 401)

    def test_create_session_issue_access_token_and_authorize(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        issue = service.auth_service.create_session(
            {
                "email": "reviewer@example.com",
                "password": "auditflow-demo",
                "organization_slug": "acme",
            }
        )
        context = service.auth_service.build_authorizer().authorize(
            required_role="viewer",
            authorization=f"Bearer {issue.response.access_token}",
            organization_id=None,
        )

        self.assertEqual(issue.response.user.email, "reviewer@example.com")
        self.assertEqual(issue.response.active_organization.slug, "acme")
        self.assertEqual(context.organization_id, "org-1")
        self.assertEqual(context.user_id, "user-reviewer-1")
        self.assertEqual(context.role, "reviewer")
        self.assertIsNotNone(context.session_id)

    def test_authorizer_rejects_insufficient_role_from_session_token(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        issue = service.auth_service.create_session(
            {
                "email": "viewer@example.com",
                "password": "auditflow-demo",
                "organization_slug": "acme",
            }
        )

        with self.assertRaises(AuditFlowAuthorizationError) as context:
            service.auth_service.build_authorizer().authorize(
                required_role="reviewer",
                authorization=f"Bearer {issue.response.access_token}",
                organization_id="org-1",
            )

        self.assertEqual(context.exception.code, "AUTH_FORBIDDEN")
        self.assertEqual(context.exception.status_code, 403)

    def test_refresh_session_rotates_refresh_token_and_revoke_invalidates_access(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        issue = service.auth_service.create_session(
            {
                "email": "admin@example.com",
                "password": "auditflow-demo",
                "organization_slug": "acme",
            }
        )
        context = service.auth_service.build_authorizer().authorize(
            required_role="product_admin",
            authorization=f"Bearer {issue.response.access_token}",
            organization_id="org-1",
        )
        refreshed = service.auth_service.refresh_session(issue.refresh_token)

        self.assertNotEqual(refreshed.refresh_token, issue.refresh_token)
        self.assertNotEqual(refreshed.response.access_token, issue.response.access_token)

        service.auth_service.revoke_session(context.session_id)

        with self.assertRaises(AuditFlowAuthorizationError) as revoked_context:
            service.auth_service.build_authorizer().authorize(
                required_role="viewer",
                authorization=f"Bearer {issue.response.access_token}",
                organization_id="org-1",
            )

        self.assertEqual(revoked_context.exception.code, "AUTH_SESSION_REVOKED")

    def test_get_current_user_returns_memberships(self) -> None:
        service = build_app_service()
        self.addCleanup(service.close)

        issue = service.auth_service.create_session(
            {
                "email": "admin@example.com",
                "password": "auditflow-demo",
                "organization_slug": "acme",
            }
        )
        context = service.auth_service.build_authorizer().authorize(
            required_role="viewer",
            authorization=f"Bearer {issue.response.access_token}",
            organization_id=None,
        )
        current_user = service.auth_service.get_current_user(context)

        self.assertEqual(current_user.user.display_name, "Audit Admin")
        self.assertEqual(current_user.active_organization.id, "org-1")
        self.assertEqual(current_user.memberships[0].role, "org_admin")


@unittest.skipIf(TestClient is None, "fastapi test client unavailable")
class AuditFlowAuthRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = build_app_service()
        self.addCleanup(self.service.close)
        self.app = create_fastapi_app(self.service)
        self.client = TestClient(self.app)

    def test_session_routes_issue_cookie_and_authorize_me(self) -> None:
        login_response = self.client.post(
            "/api/v1/auth/session",
            json={
                "email": "reviewer@example.com",
                "password": "auditflow-demo",
                "organization_slug": "acme",
            },
        )

        self.assertEqual(login_response.status_code, 200)
        self.assertIn("refresh_token", login_response.cookies)
        access_token = login_response.json()["data"]["access_token"]

        me_response = self.client.get(
            "/api/v1/me",
            headers={"Authorization": f"Bearer {access_token}"},
        )

        self.assertEqual(me_response.status_code, 200)
        self.assertEqual(me_response.json()["data"]["active_organization"]["slug"], "acme")

    def test_refresh_and_revoke_current_session(self) -> None:
        login_response = self.client.post(
            "/api/v1/auth/session",
            json={
                "email": "admin@example.com",
                "password": "auditflow-demo",
                "organization_slug": "acme",
            },
        )
        access_token = login_response.json()["data"]["access_token"]

        refresh_response = self.client.post("/api/v1/auth/session/refresh")
        self.assertEqual(refresh_response.status_code, 200)
        refreshed_access_token = refresh_response.json()["data"]["access_token"]
        self.assertNotEqual(refreshed_access_token, access_token)

        revoke_response = self.client.delete(
            "/api/v1/auth/session/current",
            headers={"Authorization": f"Bearer {refreshed_access_token}"},
        )
        self.assertEqual(revoke_response.status_code, 204)

        blocked_response = self.client.get(
            "/api/v1/me",
            headers={"Authorization": f"Bearer {refreshed_access_token}"},
        )
        self.assertEqual(blocked_response.status_code, 401)
        self.assertEqual(blocked_response.json()["error"]["code"], "AUTH_SESSION_REVOKED")


if __name__ == "__main__":
    unittest.main()
