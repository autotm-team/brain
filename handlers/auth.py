"""Authentication handlers for Brain UI endpoints."""

from __future__ import annotations

from datetime import datetime
import os
from typing import Any, Dict

from aiohttp import web

from handlers.base import BaseHandler
from auth_service import AuthError


class AuthHandler(BaseHandler):
    """Handle /api/v1/ui/auth/* endpoints."""

    REFRESH_COOKIE_NAME = os.getenv("BRAIN_AUTH_REFRESH_COOKIE_NAME", "autotm_refresh_token")
    REFRESH_COOKIE_PATH = os.getenv("BRAIN_AUTH_REFRESH_COOKIE_PATH", "/api/v1/ui/auth")
    REFRESH_COOKIE_SAMESITE = os.getenv("BRAIN_AUTH_REFRESH_COOKIE_SAMESITE", "Lax")

    @staticmethod
    def _is_secure_request(request: web.Request) -> bool:
        if request.secure:
            return True
        forwarded_proto = request.headers.get("X-Forwarded-Proto", "")
        return forwarded_proto.lower() == "https"

    def _set_refresh_cookie(self, request: web.Request, response: web.Response, refresh_token: str, max_age: int) -> None:
        domain = os.getenv("BRAIN_AUTH_REFRESH_COOKIE_DOMAIN") or None
        secure = self._is_secure_request(request)
        response.set_cookie(
            self.REFRESH_COOKIE_NAME,
            refresh_token,
            max_age=max_age,
            path=self.REFRESH_COOKIE_PATH,
            httponly=True,
            secure=secure,
            samesite=self.REFRESH_COOKIE_SAMESITE,
            domain=domain,
        )

    def _clear_refresh_cookie(self, request: web.Request, response: web.Response) -> None:
        domain = os.getenv("BRAIN_AUTH_REFRESH_COOKIE_DOMAIN") or None
        response.del_cookie(
            self.REFRESH_COOKIE_NAME,
            path=self.REFRESH_COOKIE_PATH,
            domain=domain,
        )

    @staticmethod
    def _extract_ip(request: web.Request) -> str:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.remote or ""

    @staticmethod
    def _extract_user_agent(request: web.Request) -> str:
        return request.headers.get("User-Agent", "")

    async def login(self, request: web.Request) -> web.Response:
        try:
            payload = await self.get_request_json(request)
            if not isinstance(payload, dict):
                return self.error_response("Request body must be a JSON object", 400)
            username = str(payload.get("username") or "").strip()
            password = str(payload.get("password") or "")
            if not username or not password:
                return self.error_response("Missing username or password", 400)

            auth_service = self.get_app_component(request, "auth_service")
            bundle = await auth_service.login(
                username=username,
                password=password,
                ip_address=self._extract_ip(request),
                user_agent=self._extract_user_agent(request),
            )
            response = self.success_response(
                {
                    "access_token": bundle.access_token,
                    "token_type": bundle.token_type,
                    "access_expires_in": bundle.access_expires_in,
                    "refresh_expires_in": bundle.refresh_expires_in,
                    "user": bundle.user,
                },
                "Login successful",
            )
            self._set_refresh_cookie(request, response, bundle.refresh_token, bundle.refresh_expires_in)
            return response
        except AuthError as exc:
            return web.json_response(
                {
                    "success": False,
                    "error": exc.message,
                    "error_code": exc.code,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=exc.status,
            )
        except Exception as exc:
            self.logger.error(f"Login failed: {exc}")
            return self.error_response("Login failed", 500)

    async def refresh(self, request: web.Request) -> web.Response:
        try:
            payload: Dict[str, Any] = {}
            if request.can_read_body:
                payload = await self.get_request_json(request)
            if not isinstance(payload, dict):
                return self.error_response("Request body must be a JSON object", 400)
            refresh_token = str(payload.get("refresh_token") or request.cookies.get(self.REFRESH_COOKIE_NAME) or "").strip()
            if not refresh_token:
                return self.error_response("Missing refresh_token", 400)

            auth_service = self.get_app_component(request, "auth_service")
            bundle = await auth_service.refresh(
                refresh_token=refresh_token,
                ip_address=self._extract_ip(request),
                user_agent=self._extract_user_agent(request),
            )
            response = self.success_response(
                {
                    "access_token": bundle.access_token,
                    "token_type": bundle.token_type,
                    "access_expires_in": bundle.access_expires_in,
                    "refresh_expires_in": bundle.refresh_expires_in,
                    "user": bundle.user,
                },
                "Token refreshed",
            )
            self._set_refresh_cookie(request, response, bundle.refresh_token, bundle.refresh_expires_in)
            return response
        except AuthError as exc:
            return web.json_response(
                {
                    "success": False,
                    "error": exc.message,
                    "error_code": exc.code,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=exc.status,
            )
        except Exception as exc:
            self.logger.error(f"Refresh token failed: {exc}")
            return self.error_response("Refresh token failed", 500)

    async def logout(self, request: web.Request) -> web.Response:
        try:
            payload: Dict[str, Any] = {}
            if request.can_read_body:
                payload = await self.get_request_json(request)
            if not isinstance(payload, dict):
                return self.error_response("Request body must be a JSON object", 400)

            refresh_token = str(payload.get("refresh_token") or request.cookies.get(self.REFRESH_COOKIE_NAME) or "").strip() or None
            revoke_all = bool(payload.get("revoke_all", False))
            auth_header = request.headers.get("Authorization", "")
            access_token = auth_header[7:] if auth_header.startswith("Bearer ") else None

            auth_service = self.get_app_component(request, "auth_service")
            await auth_service.logout(refresh_token=refresh_token, access_token=access_token, revoke_all=revoke_all)
            response = self.success_response({"revoked": True}, "Logout successful")
            self._clear_refresh_cookie(request, response)
            return response
        except AuthError as exc:
            return web.json_response(
                {
                    "success": False,
                    "error": exc.message,
                    "error_code": exc.code,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=exc.status,
            )
        except Exception as exc:
            self.logger.error(f"Logout failed: {exc}")
            return self.error_response("Logout failed", 500)

    async def me(self, request: web.Request) -> web.Response:
        try:
            current_user = request.get("current_user")
            if not isinstance(current_user, dict):
                return self.error_response("Unauthorized", 401, "UNAUTHORIZED")
            return self.success_response(current_user)
        except Exception as exc:
            self.logger.error(f"Get current user failed: {exc}")
            return self.error_response("Failed to fetch current user", 500)
