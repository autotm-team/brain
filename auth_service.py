"""Brain authentication service with JWT + refresh token sessions."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import secrets
import uuid
from typing import Any, Dict, Optional

try:
    import bcrypt
except ImportError:
    bcrypt = None

_ECONDB_IMPORT_ERROR: Exception | None = None
try:
    from econdb import (  # type: ignore
        UISystemDataAPI,
        SystemAuditDTO,
        SystemAuthSessionDTO,
        create_database_manager,
    )
except Exception as exc:  # pragma: no cover - surfaced only in misconfigured runtime
    UISystemDataAPI = None  # type: ignore[assignment]
    SystemAuditDTO = None  # type: ignore[assignment]
    SystemAuthSessionDTO = None  # type: ignore[assignment]
    create_database_manager = None  # type: ignore[assignment]
    _ECONDB_IMPORT_ERROR = exc


class AuthError(RuntimeError):
    def __init__(self, message: str, status: int = 401, code: str = "AUTH_ERROR"):
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code


@dataclass
class TokenBundle:
    access_token: str
    refresh_token: str
    token_type: str
    access_expires_in: int
    refresh_expires_in: int
    user: Dict[str, Any]


class AuthService:
    """Auth service backed by econdb + optional Redis revocation index."""

    def __init__(self, config, redis_client=None):
        if UISystemDataAPI is None or SystemAuditDTO is None or SystemAuthSessionDTO is None or create_database_manager is None:
            raise RuntimeError(
                "econdb runtime dependencies are unavailable; install service dependencies before using AuthService"
            ) from _ECONDB_IMPORT_ERROR
        self._config = config
        self._redis = redis_client
        self._db_manager = create_database_manager()
        self._api = UISystemDataAPI(self._db_manager)

        service_cfg = getattr(config, "service", None)
        self._issuer = getattr(service_cfg, "auth_issuer", "autotm-brain")
        self._secret = getattr(service_cfg, "auth_jwt_secret", None) or os.getenv("BRAIN_AUTH_JWT_SECRET") or secrets.token_urlsafe(48)
        self._access_ttl = int(getattr(service_cfg, "auth_access_token_ttl_seconds", 900) or 900)
        self._refresh_ttl = int(getattr(service_cfg, "auth_refresh_token_ttl_seconds", 604800) or 604800)
        self._lock_enabled = bool(getattr(service_cfg, "auth_lock_enabled", False))
        self._lock_threshold = int(getattr(service_cfg, "auth_lock_threshold", 5) or 5)
        self._lock_seconds = int(getattr(service_cfg, "auth_lock_seconds", 600) or 600)

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _to_iso(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _parse_time(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _b64url_encode(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    @staticmethod
    def _b64url_decode(raw: str) -> bytes:
        padding = "=" * (-len(raw) % 4)
        return base64.urlsafe_b64decode((raw + padding).encode("ascii"))

    def _jwt_encode(self, payload: Dict[str, Any]) -> str:
        header = {"alg": "HS256", "typ": "JWT"}
        header_segment = self._b64url_encode(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        payload_segment = self._b64url_encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
        signature = hmac.new(self._secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        return f"{header_segment}.{payload_segment}.{self._b64url_encode(signature)}"

    def _jwt_decode(self, token: str, expected_type: Optional[str] = None, verify_exp: bool = True) -> Dict[str, Any]:
        parts = token.split(".")
        if len(parts) != 3:
            raise AuthError("Invalid token format", 401, "INVALID_TOKEN")
        header_segment, payload_segment, signature_segment = parts
        signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
        expected_signature = hmac.new(self._secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        provided_signature = self._b64url_decode(signature_segment)
        if not hmac.compare_digest(expected_signature, provided_signature):
            raise AuthError("Token signature verification failed", 401, "INVALID_TOKEN")

        payload = json.loads(self._b64url_decode(payload_segment).decode("utf-8"))
        token_type = payload.get("type")
        if expected_type and token_type != expected_type:
            raise AuthError("Token type mismatch", 401, "INVALID_TOKEN")

        if verify_exp:
            exp = int(payload.get("exp") or 0)
            if exp <= int(self._utc_now().timestamp()):
                raise AuthError("Token expired", 401, "TOKEN_EXPIRED")
        return payload

    @staticmethod
    def _hash_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @staticmethod
    def _verify_password(password: str, encoded_hash: Optional[str]) -> bool:
        if not encoded_hash:
            return False
        # Support bcrypt format (e.g., $2b$12$...)
        if encoded_hash.startswith(("$2a$", "$2b$", "$2y$", "$2x$")):
            if bcrypt is None:
                return False
            try:
                return bcrypt.checkpw(password.encode("utf-8"), encoded_hash.encode("ascii"))
            except Exception:
                return False
        # Support pbkdf2_sha256 format
        try:
            algorithm, iter_raw, salt_raw, digest_raw = encoded_hash.split("$", 3)
            if algorithm != "pbkdf2_sha256":
                return False
            iterations = int(iter_raw)
            salt = base64.b64decode(salt_raw.encode("ascii"))
            expected = base64.b64decode(digest_raw.encode("ascii"))
            derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
            return hmac.compare_digest(expected, derived)
        except Exception:
            return False

    @staticmethod
    def _sanitize_user(user: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(user)
        result.pop("password_hash", None)
        return result

    async def initialize(self) -> None:
        allow_runtime_ddl = os.getenv("DB_ALLOW_RUNTIME_DDL", "false").lower() in {"1", "true", "yes", "on"}
        if allow_runtime_ddl:
            await asyncio.to_thread(self._api.ensure_system_schema)
        await asyncio.to_thread(self._api.seed_defaults)
        admin = await asyncio.to_thread(self._api.get_user_by_username, "admin")
        if not self._lock_enabled:
            if hasattr(self._api, "reset_all_auth_states"):
                await asyncio.to_thread(
                    self._api.reset_all_auth_states,
                    "system",
                    "auth_lock_disabled_bootstrap",
                )
            elif admin and hasattr(self._api, "reset_user_auth_state"):
                await asyncio.to_thread(
                    self._api.reset_user_auth_state,
                    str(admin.get("id")),
                    "system",
                    "auth_lock_disabled_bootstrap",
                )
            elif admin and hasattr(self._api, "record_login_success"):
                # Backward-compatible fallback for older econdb baselines.
                await asyncio.to_thread(self._api.record_login_success, str(admin.get("id")), None)

    async def _cache_session_hash(self, session_id: str, refresh_hash: str) -> None:
        if not self._redis:
            return
        try:
            key = f"auth:session:{session_id}"
            set_result = self._redis.set(key, refresh_hash, ex=self._refresh_ttl)
            if asyncio.iscoroutine(set_result):
                await set_result
        except Exception:
            return

    async def _drop_cached_session(self, session_id: str) -> None:
        if not self._redis:
            return
        try:
            key = f"auth:session:{session_id}"
            del_result = self._redis.delete(key)
            if asyncio.iscoroutine(del_result):
                await del_result
        except Exception:
            return

    async def _read_cached_session(self, session_id: str) -> Optional[str]:
        if not self._redis:
            return None
        try:
            key = f"auth:session:{session_id}"
            value = self._redis.get(key)
            if asyncio.iscoroutine(value):
                value = await value
            if isinstance(value, bytes):
                return value.decode("utf-8")
            return value
        except Exception:
            return None

    def _build_access_claims(self, user: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        now = self._utc_now()
        exp = now + timedelta(seconds=self._access_ttl)
        return {
            "iss": self._issuer,
            "sub": user["id"],
            "sid": session_id,
            "type": "access",
            "username": user.get("username"),
            "roles": [role.get("name") for role in (user.get("roles") or [])],
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
            "jti": str(uuid.uuid4()),
        }

    def _build_refresh_claims(self, user: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        now = self._utc_now()
        exp = now + timedelta(seconds=self._refresh_ttl)
        return {
            "iss": self._issuer,
            "sub": user["id"],
            "sid": session_id,
            "type": "refresh",
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
            "jti": str(uuid.uuid4()),
        }

    async def _issue_tokens(self, user: Dict[str, Any], session_id: Optional[str] = None, ip_address: Optional[str] = None, user_agent: Optional[str] = None) -> TokenBundle:
        sid = session_id or str(uuid.uuid4())
        access_payload = self._build_access_claims(user, sid)
        refresh_payload = self._build_refresh_claims(user, sid)
        access_token = self._jwt_encode(access_payload)
        refresh_token = self._jwt_encode(refresh_payload)
        refresh_hash = self._hash_token(refresh_token)
        await asyncio.to_thread(
            self._api.create_auth_session,
            SystemAuthSessionDTO(
                id=sid,
                user_id=str(user["id"]),
                refresh_token_hash=refresh_hash,
                status="active",
                issued_at=self._to_iso(self._utc_now()),
                expires_at=self._to_iso(datetime.fromtimestamp(int(refresh_payload["exp"]), tz=timezone.utc)),
                ip_address=ip_address,
                user_agent=user_agent,
                metadata={"issuer": self._issuer},
            ),
        )
        await self._cache_session_hash(sid, refresh_hash)
        return TokenBundle(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="Bearer",
            access_expires_in=self._access_ttl,
            refresh_expires_in=self._refresh_ttl,
            user=self._sanitize_user(user),
        )

    async def login(self, username: str, password: str, ip_address: Optional[str] = None, user_agent: Optional[str] = None) -> TokenBundle:
        identifier = str(username or "").strip()
        if hasattr(self._api, "get_user_by_identifier"):
            user = await asyncio.to_thread(self._api.get_user_by_identifier, identifier)
        else:
            user = await asyncio.to_thread(self._api.get_user_by_username, identifier)
        if not user:
            raise AuthError("Invalid username or password", 401, "INVALID_CREDENTIALS")
        if str(user.get("status") or "").lower() != "active":
            raise AuthError("User is disabled", 403, "ACCOUNT_DISABLED")

        if self._lock_enabled:
            locked_until = self._parse_time(user.get("locked_until"))
            if locked_until and locked_until > self._utc_now():
                raise AuthError("Account is temporarily locked", 423, "ACCOUNT_LOCKED")

        if not self._verify_password(password, user.get("password_hash")):
            failed_count = int(user.get("failed_login_count") or 0) + 1
            lock_seconds = self._lock_seconds if self._lock_enabled and failed_count >= self._lock_threshold else 0
            await asyncio.to_thread(self._api.record_login_failure, str(user["id"]), lock_seconds)
            await asyncio.to_thread(
                self._api.append_audit_log,
                SystemAuditDTO(
                    id=str(uuid.uuid4()),
                    actor_id=str(user.get("id")),
                    action="auth.login_failed",
                    target_type="user",
                    target_id=str(user.get("id")),
                    payload={"failed_count": failed_count, "ip": ip_address},
                    created_at=self._to_iso(self._utc_now()),
                ),
            )
            raise AuthError("Invalid username or password", 401, "INVALID_CREDENTIALS")

        await asyncio.to_thread(self._api.record_login_success, str(user["id"]), ip_address)
        fresh_user = await asyncio.to_thread(self._api.get_user, str(user["id"]))
        bundle = await self._issue_tokens(fresh_user or user, ip_address=ip_address, user_agent=user_agent)
        await asyncio.to_thread(
            self._api.append_audit_log,
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=str(user.get("id")),
                action="auth.login_success",
                target_type="user",
                target_id=str(user.get("id")),
                payload={"ip": ip_address},
                created_at=self._to_iso(self._utc_now()),
            ),
        )
        return bundle

    async def refresh(self, refresh_token: str, ip_address: Optional[str] = None, user_agent: Optional[str] = None) -> TokenBundle:
        claims = self._jwt_decode(refresh_token, expected_type="refresh", verify_exp=True)
        session_id = str(claims.get("sid") or "")
        user_id = str(claims.get("sub") or "")
        if not session_id or not user_id:
            raise AuthError("Invalid refresh token claims", 401, "INVALID_TOKEN")

        refresh_hash = self._hash_token(refresh_token)
        session_payload = await asyncio.to_thread(self._api.get_auth_session, session_id)
        if not session_payload:
            raise AuthError("Session not found", 401, "SESSION_NOT_FOUND")
        if str(session_payload.get("status") or "") != "active":
            raise AuthError("Session is not active", 401, "SESSION_REVOKED")

        expires_at = self._parse_time(session_payload.get("expires_at"))
        if expires_at and expires_at <= self._utc_now():
            await asyncio.to_thread(self._api.revoke_auth_session, session_id, "expired")
            await self._drop_cached_session(session_id)
            raise AuthError("Refresh token expired", 401, "TOKEN_EXPIRED")

        db_hash = str(session_payload.get("refresh_token_hash") or "")
        redis_hash = await self._read_cached_session(session_id)
        if db_hash != refresh_hash:
            await asyncio.to_thread(self._api.revoke_auth_session, session_id, "token_mismatch")
            await self._drop_cached_session(session_id)
            raise AuthError("Refresh token mismatch", 401, "INVALID_TOKEN")
        if redis_hash is not None and redis_hash != refresh_hash:
            await asyncio.to_thread(self._api.revoke_auth_session, session_id, "redis_mismatch")
            await self._drop_cached_session(session_id)
            raise AuthError("Refresh token revoked", 401, "SESSION_REVOKED")

        await asyncio.to_thread(self._api.revoke_auth_session, session_id, "rotated")
        await self._drop_cached_session(session_id)

        user = await asyncio.to_thread(self._api.get_user, user_id)
        if not user:
            raise AuthError("User not found", 401, "USER_NOT_FOUND")
        if str(user.get("status") or "").lower() != "active":
            raise AuthError("User is disabled", 403, "ACCOUNT_DISABLED")

        bundle = await self._issue_tokens(user, ip_address=ip_address, user_agent=user_agent)
        await asyncio.to_thread(
            self._api.append_audit_log,
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=user_id,
                action="auth.refresh",
                target_type="session",
                target_id=session_id,
                payload={"ip": ip_address},
                created_at=self._to_iso(self._utc_now()),
            ),
        )
        return bundle

    async def logout(self, refresh_token: Optional[str] = None, access_token: Optional[str] = None, revoke_all: bool = False) -> None:
        user_id: Optional[str] = None
        session_id: Optional[str] = None

        if access_token:
            try:
                access_claims = self._jwt_decode(access_token, expected_type="access", verify_exp=False)
                user_id = str(access_claims.get("sub") or "") or user_id
            except AuthError:
                pass

        if refresh_token:
            try:
                refresh_claims = self._jwt_decode(refresh_token, expected_type="refresh", verify_exp=False)
                user_id = str(refresh_claims.get("sub") or "") or user_id
                session_id = str(refresh_claims.get("sid") or "") or session_id
            except AuthError:
                session_hash = self._hash_token(refresh_token)
                sess = await asyncio.to_thread(self._api.get_auth_session_by_token_hash, session_hash)
                if sess:
                    session_id = str(sess.get("id") or "")
                    user_id = str(sess.get("user_id") or "")

        if session_id:
            await asyncio.to_thread(self._api.revoke_auth_session, session_id, "logout")
            await self._drop_cached_session(session_id)

        if revoke_all and user_id:
            await asyncio.to_thread(self._api.revoke_user_sessions, user_id, "logout_all")

        if user_id:
            await asyncio.to_thread(
                self._api.append_audit_log,
                SystemAuditDTO(
                    id=str(uuid.uuid4()),
                    actor_id=user_id,
                    action="auth.logout",
                    target_type="user",
                    target_id=user_id,
                    payload={"revoke_all": revoke_all, "session_id": session_id},
                    created_at=self._to_iso(self._utc_now()),
                ),
            )

    async def resolve_access_token(self, token: str) -> Dict[str, Any]:
        claims = self._jwt_decode(token, expected_type="access", verify_exp=True)
        user_id = str(claims.get("sub") or "")
        if not user_id:
            raise AuthError("Invalid access token claims", 401, "INVALID_TOKEN")
        user = await asyncio.to_thread(self._api.get_user, user_id)
        if not user:
            raise AuthError("User not found", 401, "USER_NOT_FOUND")
        if str(user.get("status") or "").lower() != "active":
            raise AuthError("User is disabled", 403, "ACCOUNT_DISABLED")
        return {
            "claims": claims,
            "user": self._sanitize_user(user),
        }
