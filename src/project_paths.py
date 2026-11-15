"""Project-relative path helpers."""

from __future__ import annotations

from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent
SRC_ROOT = PACKAGE_ROOT.parent
PROJECT_ROOT = SRC_ROOT.parent
VAR_ROOT = PROJECT_ROOT / "var"
DEFAULT_NETWORK_ANALYSIS_DIR = VAR_ROOT / "analysis"


def resolve_project_path(*parts: str | Path) -> Path:
    """Resolve a path relative to the repository root when not absolute."""

    path = Path(*parts).expanduser()
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def resolve_config_path(value: str | None, default: str) -> Path:
    """Resolve a config path value relative to the repository root."""

    raw_value = value if value not in (None, "") else default
    return resolve_project_path(raw_value)


def find_example_env_path() -> Path:
    """Return the preferred example env file path, supporting legacy names."""

    for filename in ("example.env", "example_config.env"):
        candidate = PROJECT_ROOT / filename
        if candidate.exists():
            return candidate
    return PROJECT_ROOT / "example.env"
