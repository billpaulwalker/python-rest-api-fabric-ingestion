
"""GitHub REST API pagination helpers."""

from requests.utils import parse_header_links


def get_next_url(link_header: str | None) -> str | None:
    """Extract the next-page URL from an HTTP Link header."""

    if not link_header:
        return None

    for link in parse_header_links(link_header):
        if link.get("rel") == "next":
            return link.get("url")

    return None
