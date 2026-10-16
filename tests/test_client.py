

"""Unit tests for GitHub API connectivity, retries, and pagination."""

import json

from unittest.mock import patch

import pytest
import requests

from api_ingestion.client import GitHubAPIClient


# ---------------------------------------------------------
# Shared test utilities
# ---------------------------------------------------------

def make_response(status, data=None, headers=None):
    """Create a mocked HTTP response."""

    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(
        data if data is not None else {}
    ).encode("utf-8")

    response.headers.update(headers or {})
    response.url = "https://api.github.com/test"

    return response


@pytest.fixture
def client():
    """Create a test client without real credentials."""

    api = GitHubAPIClient(
        base_url="https://api.github.com",
        token="fake-token",
        max_attempts=3,
        initial_backoff=0,
        max_rate_limit_wait=120,
    )

    yield api
    api.close()


# ---------------------------------------------------------
# 1. Successful authentication / HTTP response
# ---------------------------------------------------------

def test_successful_request(client):
    response = make_response(
        200,
        {"login": "tester"},
    )

    with patch.object(
        client.session,
        "get",
        return_value=response,
    ):
        user = client.verify_authentication()

    assert user["login"] == "tester"
    assert client.request_count == 1
    assert client.retry_count == 0


# ---------------------------------------------------------
# 2. HTTP 401 - Do not retry authentication failures
# ---------------------------------------------------------

def test_unauthorized_does_not_retry(client):
    response = make_response(
        401,
        {"message": "Bad credentials"},
    )

    with patch.object(
        client.session,
        "get",
        return_value=response,
    ) as mock_get:

        with pytest.raises(requests.HTTPError):
            client.verify_authentication()

    assert mock_get.call_count == 1
    assert client.retry_count == 0


# ---------------------------------------------------------
# 3. HTTP 503 - Retry and recover
# ---------------------------------------------------------

@patch("api_ingestion.client.time.sleep")
def test_server_error_then_success(mock_sleep, client):
    responses = [
        make_response(503),
        make_response(200, {"login": "tester"}),
    ]

    with patch.object(
        client.session,
        "get",
        side_effect=responses,
    ):
        result = client.verify_authentication()

    assert result["login"] == "tester"
    assert client.request_count == 2
    assert client.retry_count == 1
    mock_sleep.assert_called_once()


# ---------------------------------------------------------
# 4. HTTP 429 - Respect Retry-After
# ---------------------------------------------------------

@patch("api_ingestion.client.time.sleep")
def test_rate_limit_retry_after(mock_sleep, client):
    responses = [
        make_response(
            429,
            headers={"Retry-After": "2"},
        ),
        make_response(200, {"login": "tester"}),
    ]

    with patch.object(
        client.session,
        "get",
        side_effect=responses,
    ):
        client.verify_authentication()

    assert client.retry_count == 1
    mock_sleep.assert_called_once_with(2.0)


# ---------------------------------------------------------
# 5. HTTP 429 - Reject excessive wait times
# ---------------------------------------------------------

def test_rate_limit_wait_too_long(client):
    response = make_response(
        429,
        headers={"Retry-After": "600"},
    )

    with patch.object(
        client.session,
        "get",
        return_value=response,
    ):

        with pytest.raises(
            RuntimeError,
            match="maximum",
        ):
            client.verify_authentication()

    assert client.retry_count == 0


# ---------------------------------------------------------
# 6. Timeout - Retry and recover
# ---------------------------------------------------------

@patch("api_ingestion.client.time.sleep")
def test_timeout_then_success(mock_sleep, client):
    responses = [
        requests.Timeout("Simulated timeout"),
        make_response(200, {"login": "tester"}),
    ]

    with patch.object(
        client.session,
        "get",
        side_effect=responses,
    ):
        result = client.verify_authentication()

    assert result["login"] == "tester"
    assert client.request_count == 2
    assert client.retry_count == 1
    mock_sleep.assert_called_once()


# ---------------------------------------------------------
# 7. Pagination - Standard GitHub repository URL
# ---------------------------------------------------------

def test_pagination_follows_link_header(client):
    next_url = (
        "https://api.github.com/repos/"
        "microsoft/vscode/issues?page=2"
    )

    first = make_response(
        200,
        [{"id": 1}],
        headers={
            "Link": f'<{next_url}>; rel="next"'
        },
    )

    second = make_response(
        200,
        [{"id": 2}],
    )

    with patch.object(
        client.session,
        "get",
        side_effect=[first, second],
    ) as mock_get:

        pages = list(
            client.get_issues(
                owner="microsoft",
                repository="vscode",
            )
        )

    assert len(pages) == 2
    assert mock_get.call_count == 2

    assert pages[0][0]["id"] == 1
    assert pages[1][0]["id"] == 2


# ---------------------------------------------------------
# 8. Pagination - Maximum page limit
# ---------------------------------------------------------

def test_pagination_limit_fails(client):
    next_url = (
        "https://api.github.com/repos/"
        "microsoft/vscode/issues?page=2"
    )

    first = make_response(
        200,
        [{"id": 1}],
        headers={
            "Link": f'<{next_url}>; rel="next"'
        },
    )

    with patch.object(
        client.session,
        "get",
        return_value=first,
    ):

        with pytest.raises(
            RuntimeError,
            match="max_pages",
        ):
            list(
                client.get_issues(
                    owner="microsoft",
                    repository="vscode",
                    max_pages=1,
                )
            )


# ---------------------------------------------------------
# 9. Pagination - Numeric GitHub repository ID
# ---------------------------------------------------------

def test_pagination_with_numeric_repository_id(client):
    """Accept GitHub's numeric repository pagination URLs."""

    next_url = (
        "https://api.github.com/repositories/"
        "41881900/issues?per_page=100&page=2"
    )

    first = make_response(
        200,
        [{"id": 1}],
        headers={
            "Link": f'<{next_url}>; rel="next"'
        },
    )

    second = make_response(
        200,
        [{"id": 2}],
    )

    with patch.object(
        client.session,
        "get",
        side_effect=[first, second],
    ) as mock_get:

        pages = list(
            client.get_issues(
                owner="microsoft",
                repository="vscode",
            )
        )

    assert len(pages) == 2
    assert mock_get.call_count == 2

    assert pages[0][0]["id"] == 1
    assert pages[1][0]["id"] == 2
