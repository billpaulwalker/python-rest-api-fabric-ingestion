
"""GitHub REST API client."""

import requests


class GitHubAPIClient:
    """Handles GitHub API connectivity and authentication."""

    def __init__(
        self,
        base_url: str,
        token: str,
        timeout: int = 30,
        api_version: str = "2026-03-10",
    ):
        if not token:
            raise ValueError("GITHUB_TOKEN is required")

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": api_version,
                "User-Agent": "python-rest-api-fabric-ingestion",
            }
        )

    def verify_authentication(self) -> dict:
        """Verify token validity using the authenticated user endpoint."""

        url = f"{self.base_url}/user"

        response = self.session.get(
            url,
            timeout=self.timeout,
        )
        response.raise_for_status()

        return response.json()

    def get_issues(
        self,
        owner: str,
        repository: str,
        per_page: int = 25,
        state: str = "all",
    ) -> list:
        """Retrieve the first page of repository issues."""

        url = (
            f"{self.base_url}/repos/"
            f"{owner}/{repository}/issues"
        )

        params = {
            "state": state,
            "per_page": per_page,
            "page": 1,
        }

        response = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
        )

        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise ValueError(
                "Expected a JSON list from the issues endpoint"
            )

        return data

    def close(self):
        """Close the underlying HTTP session."""
        self.session.close()