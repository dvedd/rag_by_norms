import base64
import hashlib
import hmac
import secrets

PASSWORD_ALGORITHM = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 600_000


def normalize_email(email: str) -> str:
    """Normalize an email for case-insensitive account lookup."""
    return email.strip().lower()


def is_valid_email(email: str) -> bool:
    """Apply the account email format check."""
    normalized = normalize_email(email=email)

    return "@" in normalized and "." in normalized.rsplit("@", 1)[-1]


def hash_password(password: str) -> str:
    """Encode a salted PBKDF2 hash; callers must avoid the event loop."""
    salt = secrets.token_bytes(nbytes=16)
    digest = _password_digest(
        password=password,
        salt=salt,
        iterations=PASSWORD_ITERATIONS,
    )

    return "$".join(
        [
            PASSWORD_ALGORITHM,
            str(PASSWORD_ITERATIONS),
            _b64encode(value=salt),
            _b64encode(value=digest),
        ]
    )


def verify_password(password: str, encoded_hash: str) -> bool:
    """Verify a stored hash; callers must avoid the event loop."""
    try:
        algorithm, iterations, salt, expected = encoded_hash.split("$", 3)
    except (ValueError, TypeError):
        return False

    if algorithm != PASSWORD_ALGORITHM:
        return False

    try:
        digest = _password_digest(
            password=password,
            salt=_b64decode(value=salt),
            iterations=int(iterations),
        )
        matches = hmac.compare_digest(_b64encode(value=digest), expected)
    except (ValueError, TypeError, OverflowError):
        return False

    return matches


def create_session_token() -> str:
    """Generate a bearer token with 256 bits of randomness."""
    return secrets.token_urlsafe(nbytes=32)


def hash_token(token: str) -> str:
    """Digest a bearer token for storage without the original secret."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def extract_bearer_token(authorization: str | None) -> str | None:
    """Extract a token from a bearer authorization header."""
    if not authorization:
        return None

    scheme, _, token = authorization.partition(" ")

    if scheme.lower() != "bearer" or not token:
        return None

    return token


def _password_digest(password: str, salt: bytes, iterations: int) -> bytes:
    return hashlib.pbkdf2_hmac(
        hash_name="sha256",
        password=password.encode("utf-8"),
        salt=salt,
        iterations=iterations,
    )


def _b64encode(value: bytes) -> str:
    return base64.b64encode(s=value).decode("ascii")


def _b64decode(value: str) -> bytes:
    return base64.b64decode(s=value.encode("ascii"))
