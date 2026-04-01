from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import yaml

# Canonical commands that both websocket and webhook ingestion paths can route.
ROUTED_COMMANDS: tuple[str, ...] = (
    "request",
    "random_request",
    "playlist_request",
    "prioritize",
    "points",
    "remove",
    "archive",
)

# Bot-parity defaults used when `commands.yml` is missing or invalid.
DEFAULT_COMMANDS_MAP: dict[str, list[str]] = {
    "prefix": ["!"],
    "request": ["request", "req", "r", "sr"],
    "prioritize": ["prioritize", "prio", "bump"],
    "points": ["points", "pp"],
    "remove": ["remove", "undo", "del"],
    "archive": ["archive"],
    "random_request": ["random", "rr", "randomrequest"],
    "playlist_request": ["playlist", "pl"],
}


def default_commands_map() -> dict[str, list[str]]:
    """Return a detached copy of canonical bot/webhook command defaults.

    Dependencies: module-level `DEFAULT_COMMANDS_MAP`. Code customers:
    websocket bot startup and webhook command parse fallback paths. Used
    variables/origin: defaults mirror `bot/commands.yml` aliases.
    """

    return {key: list(values) for key, values in DEFAULT_COMMANDS_MAP.items()}


def normalize_commands_map(raw: Mapping[str, Any], base: Optional[Mapping[str, Any]] = None) -> dict[str, list[str]]:
    """Normalize command config into lowercase list values keyed by canonical name.

    Dependencies: Python scalar/list coercion only. Code customers:
    `load_commands_map` and tests that need stable alias normalization.
    Used variables/origin: `raw` and optional `base` are loaded from YAML and
    in-process defaults.
    """

    merged: dict[str, Any] = {}
    if base:
        merged.update(dict(base))
    merged.update(dict(raw or {}))

    normalized: dict[str, list[str]] = {}
    for key, value in merged.items():
        if isinstance(value, list):
            normalized[key] = [str(item).strip().lower() for item in value if str(item).strip()]
        elif value is None:
            normalized[key] = []
        else:
            scalar = str(value).strip().lower()
            normalized[key] = [scalar] if scalar else []

    if not normalized.get("prefix"):
        normalized["prefix"] = ["!"]
    return normalized


def resolve_commands_file(command_file: Optional[str] = None) -> Path:
    """Resolve command YAML path with container and local-repo fallbacks.

    Dependencies: environment variable lookup via `os.getenv` and filesystem
    probing with `pathlib.Path`. Code customers: shared command-map loader.
    Used variables/origin: defaults to `COMMANDS_FILE` or `/bot/commands.yml`,
    then falls back to repository `bot/commands.yml` for local execution.
    """

    raw_path = command_file or os.getenv("COMMANDS_FILE", "/bot/commands.yml")
    primary = Path(raw_path)
    if primary.exists():
        return primary
    repo_path = Path(__file__).resolve().parent / "bot" / "commands.yml"
    if repo_path.exists():
        return repo_path
    return primary


def load_commands_map(command_file: Optional[str] = None, logger: Any = None) -> dict[str, list[str]]:
    """Load and normalize canonical command aliases from YAML with safe fallback.

    Dependencies: `resolve_commands_file`, `yaml.safe_load`, and
    `normalize_commands_map`. Code customers: websocket runtime startup and
    webhook parser cache warm path. Used variables/origin: source file comes
    from `COMMANDS_FILE` and/or repository fallback path.
    """

    defaults = default_commands_map()
    file_path = resolve_commands_file(command_file)
    payload: dict[str, Any] = {}

    try:
        with file_path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
            if isinstance(loaded, dict):
                payload = loaded
    except FileNotFoundError:
        if logger is not None:
            logger.info("Command map file not found; using defaults", extra={"path": str(file_path)})
    except Exception:
        if logger is not None:
            logger.warning("Failed to parse command map; using defaults", extra={"path": str(file_path)}, exc_info=True)

    return normalize_commands_map(payload, defaults)


def resolve_prefixed_command(
    message_text: str,
    commands_map: Mapping[str, Sequence[str]],
    routed_commands: Sequence[str] = ROUTED_COMMANDS,
) -> dict[str, Optional[str]]:
    """Resolve a chat message into alias/canonical command metadata.

    Dependencies: canonical alias map from `commands.yml` and message string
    tokenization. Code customers: websocket parser (`parse_chat_command`) and
    webhook parser (`_extract_eventsub_chat_command`). Used variables/origin:
    `message_text` comes from inbound Twitch chat payloads.
    """

    content = (message_text or "").strip()
    prefixes = list(commands_map.get("prefix") or ["!"])
    prefix = next((item.strip() for item in prefixes if str(item).strip()), "!")
    if not content.startswith(prefix):
        return {"alias": None, "canonical": None, "args": None, "parse_reason": "non_command_message"}

    command_blob = content[len(prefix):]
    command_token, _, remainder = command_blob.partition(" ")
    alias = command_token.strip().lower()
    args = remainder.strip() if remainder else ""
    if not alias:
        return {"alias": None, "canonical": None, "args": None, "parse_reason": "non_command_message"}

    canonical = None
    for name in routed_commands:
        alias_list = [str(item).strip().lower() for item in (commands_map.get(name) or []) if str(item).strip()]
        if alias in alias_list:
            canonical = name
            break

    parse_reason = "ok" if canonical else "unknown_alias"
    parse_detail = f"alias '{alias}' is not in configured commands map" if parse_reason == "unknown_alias" else None
    feedback_message = f"Unknown command alias '{alias}'." if parse_reason == "unknown_alias" else None

    return {
        "alias": alias,
        "canonical": canonical,
        "args": args,
        "parse_reason": parse_reason,
        "parse_detail": parse_detail,
        "feedback_message": feedback_message,
    }
