"""Read required env vars, then report every missing one at once.

os.environ["X"] fails on the first missing var and hides the rest, which
turns a bad deploy into a serial guessing game (see SPEC.md).
"""
import os
import sys

_missing: list[str] = []


def require(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        _missing.append(name)
        return ""
    return value


def seal() -> None:
    if _missing:
        sys.exit("missing required environment variables: " + ", ".join(sorted(_missing)))
