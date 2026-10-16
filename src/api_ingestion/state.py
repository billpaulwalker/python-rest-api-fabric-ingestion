
"""Local extraction state management."""

import json
import os

from pathlib import Path


def load_state(path: Path) -> dict:
    """Load previously committed extraction state."""

    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_state(path: Path, state: dict) -> None:
    """Atomically replace an extraction state file."""

    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_suffix(".tmp")

    with temporary.open("w", encoding="utf-8") as file:
        json.dump(state, file, indent=2)
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary, path)
