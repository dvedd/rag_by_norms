import logging
import os
from typing import Any

import requests
from shared.logging.failures import log_failure

DEFAULT_USER_API_URL = "http://127.0.0.1:9130"
DEFAULT_TIMEOUT = (3, 30)
logger = logging.getLogger(__name__)


class UserApiError(Exception):
    """Expose a safe service error and its optional HTTP status."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        """Store a displayable message without a transport exception."""
        super().__init__(message)
        self.status_code = status_code


class UserApiClient:
    """Read and update account data from Streamlit's synchronous script."""

    def __init__(self, base_url: str | None = None) -> None:
        """Resolve the service address without opening a connection."""
        self.base_url = (
            base_url or os.getenv("USER_API_URL") or DEFAULT_USER_API_URL
        ).rstrip("/")

    def register(self, email: str, password: str, display_name: str | None) -> dict:
        """Register an account and return its session."""
        return self._request(
            method="POST",
            path="/auth/register",
            json={
                "email": email,
                "password": password,
                "display_name": display_name,
            },
        )

    def login(self, email: str, password: str) -> dict:
        """Authenticate an account and return its session."""
        return self._request(
            method="POST",
            path="/auth/login",
            json={"email": email, "password": password},
        )

    def get_me(self, token: str) -> dict:
        """Return the account associated with a session."""
        return self._request(method="GET", path="/me", token=token)

    def logout(self, token: str) -> None:
        """Revoke the current session."""
        self._request(method="POST", path="/auth/logout", token=token)

    def get_preferences(self, token: str) -> dict:
        """Return saved account preferences."""
        return self._request(method="GET", path="/me/preferences", token=token)

    def update_preferences(self, token: str, theme_key: str) -> dict:
        """Save an account theme preference."""
        return self._request(
            method="PATCH",
            path="/me/preferences",
            token=token,
            json={"theme_key": theme_key},
        )

    def list_searches(self, token: str, limit: int = 100) -> list[dict]:
        """Return the most recent saved searches."""
        return self._request(
            method="GET",
            path="/me/searches",
            token=token,
            params={"limit": limit},
            response_type=list,
        )

    def create_search(self, token: str, query: str, response: dict) -> dict:
        """Persist a completed search in account history."""
        return self._request(
            method="POST",
            path="/me/searches",
            token=token,
            json={"query": query, "response": response},
        )

    def update_search(
        self,
        token: str,
        search_id: str,
        query: str,
        response: dict,
    ) -> dict:
        """Replace a saved search and its answer."""
        return self._request(
            method="PATCH",
            path=f"/me/searches/{search_id}",
            token=token,
            json={"query": query, "response": response},
        )

    def delete_search(self, token: str, search_id: str) -> None:
        """Delete an account history entry."""
        self._request(method="DELETE", path=f"/me/searches/{search_id}", token=token)

    def _request(
        self,
        method: str,
        path: str,
        token: str | None = None,
        response_type: type[dict] | type[list] = dict,
        **kwargs: Any,
    ) -> Any:
        headers = kwargs.pop("headers", {})
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.request(
                method=method,
                url=f"{self.base_url}{path}",
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
                **kwargs,
            )
        except requests.RequestException as error:
            log_failure(logger=logger, operation="Account service request", error=error)
            raise UserApiError(message="User API is temporarily unavailable") from None

        with response:
            return self._decode_response(response=response, response_type=response_type)

    def _decode_response(
        self,
        response: requests.Response,
        response_type: type[dict] | type[list],
    ) -> Any:
        if response.status_code >= 400:
            raise UserApiError(
                message=_error_message(response=response),
                status_code=response.status_code,
            )

        if response.status_code == 204:
            return None

        try:
            payload = response.json()
        except ValueError as error:
            log_failure(
                logger=logger, operation="Decode account service response", error=error
            )
            raise UserApiError(message="User API returned an invalid response") from None

        if not isinstance(payload, response_type):
            raise UserApiError(message="User API returned an invalid response")

        return payload


def _error_message(response: requests.Response) -> str:
    if response.status_code >= 500:
        return "User API is temporarily unavailable"

    try:
        payload = response.json()
    except ValueError:
        return "User API request failed"

    detail = payload.get("detail") if isinstance(payload, dict) else None
    messages = {
        "Authentication required",
        "Invalid email or password",
        "Invalid or expired session",
        "Not authenticated",
        "Nothing to update",
        "Search not found",
        "User already exists",
    }

    return (
        detail
        if isinstance(detail, str) and detail in messages
        else "User API request failed"
    )
