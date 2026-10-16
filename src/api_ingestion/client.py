
"""GitHub REST API client with retries and pagination."""

import logging
import random
import time

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import requests

from .pagination import get_next_url


logger = logging.getLogger(__name__)


class GitHubAPIClient:
    """Reusable GitHub API client."""

    RETRYABLE_STATUSES = {408, 500, 502, 503, 504}

    def __init__(
        self,
        base_url: str,
        token: str,
        timeout: int = 30,
        api_version: str = "2026-03-10",
        max_attempts: int = 4,
        initial_backoff: float = 1.0,
        max_rate_limit_wait: float = 120.0,
    ):
        if not token:
            raise ValueError("GITHUB_TOKEN is required")

        self.base_url = base_url.rstrip("/")
        parsed = urlparse(self.base_url)

        if parsed.scheme != "https" or parsed.hostname != "api.github.com":
            raise ValueError("Only https://api.github.com is supported")

        self.timeout = timeout
        self.max_attempts = max_attempts
        self.initial_backoff = initial_backoff
        self.max_rate_limit_wait = max_rate_limit_wait

        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": api_version,
            "User-Agent": "python-rest-api-fabric-ingestion",
        })

        self.request_count = 0
        self.retry_count = 0

    def _is_rate_limited(self, response) -> bool:
        """Recognize primary and secondary GitHub rate limits."""

        if response.status_code == 429:
            return True

        if response.status_code != 403:
            return False

        if response.headers.get("x-ratelimit-remaining") == "0":
            return True

        try:
            message = response.json().get("message", "").lower()
        except (ValueError, AttributeError):
            message = ""

        return (
            "secondary rate limit" in message
            or "rate limit exceeded" in message
        )

    def _rate_limit_wait(self, response) -> float:
        """Calculate the minimum rate-limit wait."""

        retry_after = response.headers.get("retry-after")

        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                try:
                    retry_time = parsedate_to_datetime(retry_after)
                    return max(
                        0.0,
                        (
                            retry_time
                            - datetime.now(timezone.utc)
                        ).total_seconds(),
                    )
                except (ValueError, TypeError, OverflowError):
                    pass

        remaining = response.headers.get("x-ratelimit-remaining")
        reset = response.headers.get("x-ratelimit-reset")

        if remaining == "0" and reset:
            try:
                return max(0.0, float(reset) - time.time() + 1)
            except ValueError:
                pass

        # GitHub recommends at least 60 seconds for secondary
        # rate limits without a usable Retry-After header.
        return 60.0

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with small random jitter."""

        base = self.initial_backoff * (2 ** (attempt - 1))
        return min(30.0, base + random.uniform(0, 0.5))

    def _request(self, url: str, params=None):
        """Execute GET with bounded retries."""

        for attempt in range(1, self.max_attempts + 1):
            self.request_count += 1

            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                    allow_redirects=False,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt == self.max_attempts:
                    logger.error(
                        "HTTP transport failed | attempts=%d | type=%s",
                        attempt,
                        type(exc).__name__,
                    )
                    raise

                wait = self._backoff(attempt)
                self.retry_count += 1

                logger.warning(
                    "Transport retry | attempt=%d | wait=%.2f",
                    attempt,
                    wait,
                )
                time.sleep(wait)
                continue

            status = response.status_code

            if status == 200:
                return response

            if self._is_rate_limited(response):
                wait = self._rate_limit_wait(response)

                if attempt == self.max_attempts:
                    response.raise_for_status()

                if wait > self.max_rate_limit_wait:
                    raise RuntimeError(
                        "Rate-limit wait exceeds configured maximum: "
                        f"{wait:.1f} seconds"
                    )

                self.retry_count += 1
                logger.warning(
                    "Rate limit encountered | status=%d | wait=%.1f",
                    status,
                    wait,
                )
                time.sleep(wait)
                continue

            if status in self.RETRYABLE_STATUSES:
                if attempt == self.max_attempts:
                    response.raise_for_status()

                wait = self._backoff(attempt)
                self.retry_count += 1

                logger.warning(
                    "Server retry | status=%d | attempt=%d | wait=%.2f",
                    status,
                    attempt,
                    wait,
                )
                time.sleep(wait)
                continue

            # Authentication, permission, validation, and
            # other unexpected statuses are not retried.
            response.raise_for_status()
            raise RuntimeError(f"Unexpected HTTP status: {status}")

        raise RuntimeError("Request attempts exhausted")

    def verify_authentication(self) -> dict:
        """Verify the configured GitHub token."""

        response = self._request(f"{self.base_url}/user")
        return response.json()

    def get_issues(
        self,
        owner: str,
        repository: str,
        per_page: int = 100,
        state: str = "all",
        since: str | None = None,
        max_pages: int = 100,
    ):
        """Yield complete pages of repository issues."""

        url = (
            f"{self.base_url}/repos/"
            f"{owner}/{repository}/issues"
        )

        params = {
            "state": state,
            "per_page": per_page,
            "sort": "updated",
            "direction": "asc",
        }

        if since:
            params["since"] = since

        page_number = 0
        seen_urls = set()

        while url:
            if url in seen_urls:
                raise RuntimeError("Repeated pagination URL detected")

            seen_urls.add(url)

            page_number += 1

            response = self._request(url, params=params)
            records = response.json()

            if not isinstance(records, list):
                raise ValueError("Expected a JSON list of issues")

            next_url = get_next_url(
                response.headers.get("Link")
            )

            if next_url:
                parsed = urlparse(next_url)

                # GitHub may return pagination URLs using either
                # /repos/{owner}/{repo}/issues or
                # /repositories/{numeric_id}/issues.

                named_path = f"/repos/{owner}/{repository}/issues"

                path_parts = parsed.path.strip("/").split("/")

                is_named_repository = (
                    parsed.path == named_path
                )

                is_numeric_repository = (
                    len(path_parts) == 3
                    and path_parts[0] == "repositories"
                    and path_parts[1].isdigit()
                    and path_parts[2] == "issues"
                )

                if (
                    parsed.scheme != "https"
                    or parsed.netloc != "api.github.com"
                    or parsed.username is not None
                    or parsed.password is not None
                    or parsed.fragment
                    or not (
                        is_named_repository
                        or is_numeric_repository
                    )
                ):
                    raise ValueError("Untrusted pagination URL")

            logger.info(
                "Page retrieved | page=%d | records=%d",
                page_number,
                len(records),
            )

            yield records

            if next_url and page_number >= max_pages:
                raise RuntimeError(
                    f"Pagination exceeded max_pages={max_pages}"
                )

            url = next_url
            params = None  # Next URL already has query parameters.

    def close(self):
        self.session.close()
