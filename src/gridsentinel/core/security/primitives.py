"""Use maintained cryptographic primitives without custom crypto protocols."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import re
import secrets
from typing import Any

from gridsentinel.core.exceptions import AuthenticationError, AuthorizationError, ValidationError

TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{43}$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}$")


def validate_identifier(value: Any, max_length: int = 128) -> str:
    """Accept bounded identifiers without control or separator characters."""
    if not isinstance(value, str) or len(value) > max_length or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValidationError("Invalid identifier")
    return value


def new_token() -> str:
    """Generate an opaque 256-bit URL-safe credential."""
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> bytes:
    """Hash only correctly shaped credentials before database lookup."""
    if not isinstance(token, str) or not TOKEN_PATTERN.fullmatch(token):
        raise AuthenticationError("Invalid or expired credentials")
    return hashlib.sha256(token.encode("ascii")).digest()


def require_permission(principal: dict[str, Any], permission: str,
                       tenant_id: str | None = None) -> None:
    """Deny by default, including tenant mismatches and missing identities."""
    if not principal or not principal.get("tenant_id") or not principal.get("id"):
        raise AuthorizationError("Permission denied")
    if tenant_id is not None and principal["tenant_id"] != tenant_id:
        raise AuthorizationError("Permission denied")
    if permission not in principal.get("permissions", ()):
        raise AuthorizationError("Permission denied")


@dataclass(frozen=True)
class PasswordHasher:
    """PBKDF2-HMAC-SHA256 with random salt and a bounded work factor."""

    iterations: int = 600000
    minimum_length: int = 12

    def __post_init__(self) -> None:
        if not 600000 <= self.iterations <= 5000000 or not 12 <= self.minimum_length <= 128:
            raise ValidationError("Invalid password hashing policy")

    def validate(self, password: str) -> None:
        """Require a bounded, nontrivial passphrase without truncation."""
        if not isinstance(password, str) or not self.minimum_length <= len(password) <= 128:
            raise ValidationError(f"Password must contain {self.minimum_length} to 128 characters")
        if password.isspace() or len(set(password)) < 5 or any(ord(char) < 32 for char in password):
            raise ValidationError("Password does not satisfy the passphrase policy")

    def hash(self, password: str) -> tuple[bytes, bytes, int]:
        """Return salt, digest and iteration count for a validated password."""
        self.validate(password)
        salt = secrets.token_bytes(32)
        return salt, hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, self.iterations), self.iterations

    def verify(self, password: str, salt: bytes, expected: bytes, iterations: int) -> bool:
        """Verify without comparing secret bytes using ordinary equality."""
        if not isinstance(password, str) or len(password) > 128 or not 600000 <= iterations <= 5000000:
            return False
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return hmac.compare_digest(actual, expected)
