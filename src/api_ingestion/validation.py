
"""Validation rules for GitHub issue records."""

from datetime import datetime, timezone
from typing import Literal, overload


class RecordValidationError(ValueError):
    """A source record does not satisfy Silver requirements."""


def require_integer(record: dict, field: str) -> int:
    value = record.get(field)

    if type(value) is not int or value <= 0:
        raise RecordValidationError(
            f"{field} must be a positive integer"
        )

    return value


def require_string(record: dict, field: str) -> str:
    value = record.get(field)

    if not isinstance(value, str) or not value.strip():
        raise RecordValidationError(
            f"{field} must be a nonempty string"
        )

    return value.strip()


@overload
def parse_utc_timestamp(
    value: str | None,
    field: str,
    required: Literal[True] = True,
) -> datetime:
    ...


@overload
def parse_utc_timestamp(
    value: str | None,
    field: str,
    required: Literal[False],
) -> datetime | None:
    ...


def parse_utc_timestamp(
    value: str | None,
    field: str,
    required: bool = True,
) -> datetime | None:
    """Parse a timestamp into a UTC-aware datetime."""

    if value is None:
        if not required:
            return None

        raise RecordValidationError(
            f"{field} is required"
        )

    if not isinstance(value, str):
        raise RecordValidationError(
            f"{field} must be a timestamp string"
        )

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise RecordValidationError(
            f"{field} contains an invalid timestamp"
        ) from exc

    if parsed.tzinfo is None:
        raise RecordValidationError(
            f"{field} must include a timezone"
        )

    return parsed.astimezone(timezone.utc)


def validate_source_issue(record: dict) -> None:
    """Validate required source fields and chronology."""

    if not isinstance(record, dict):
        raise RecordValidationError(
            "Source record must be a JSON object"
        )

    require_integer(record, "id")
    require_integer(record, "number")
    require_string(record, "title")

    state = require_string(record, "state").lower()

    if state not in {"open", "closed"}:
        raise RecordValidationError(
            f"Unsupported issue state: {state}"
        )

    created = parse_utc_timestamp(
        record.get("created_at"), "created_at"
    )
    updated = parse_utc_timestamp(
        record.get("updated_at"), "updated_at"
    )
    closed = parse_utc_timestamp(
        record.get("closed_at"),
        "closed_at",
        required=False,
    )

    if updated < created:
        raise RecordValidationError(
            "updated_at precedes created_at"
        )

    if closed is not None and closed < created:
        raise RecordValidationError(
            "closed_at precedes created_at"
        )

    user = record.get("user")

    if user is not None and not isinstance(user, dict):
        raise RecordValidationError(
            "user must be an object or null"
        )

    if isinstance(user, dict) and user.get("login") is not None:
        require_string(user, "login")

    labels = record.get("labels", [])

    if not isinstance(labels, list):
        raise RecordValidationError(
            "labels must be an array"
        )

    for label in labels:
        if not isinstance(label, dict):
            raise RecordValidationError(
                "Each label must be an object"
            )

        require_string(label, "name")
