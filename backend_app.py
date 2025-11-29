from __future__ import annotations
import math
import contextlib
from typing import Optional, List, Any, Dict, Mapping, Iterable, Literal, Sequence
from threading import Lock
import os
import json
import time
import hmac
import hashlib
import logging
import re
import secrets
import html
import random
from urllib.parse import quote, urlparse, urlunparse, parse_qs
from datetime import datetime, timedelta
import asyncio
import requests

try:
    from ytmusicapi import YTMusic  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    YTMusic = None  # type: ignore[assignment]
from sse_starlette.sse import EventSourceResponse
from fastapi.middleware.cors import CORSMiddleware

from fastapi import (
    FastAPI,
    HTTPException,
    Depends,
    Header,
    Query,
    Path,
    APIRouter,
    Request as FastAPIRequest,
    Response,
    Cookie,
    Body,
    WebSocket,
)
from fastapi import WebSocketDisconnect
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from starlette.datastructures import URL
from pydantic import BaseModel, Field, ValidationError, model_validator
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, Boolean,
    ForeignKey, UniqueConstraint, func, select, and_, or_, inspect, text,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session, joinedload, selectinload
from sqlalchemy.exc import IntegrityError

# =====================================
# Config
# =====================================
# SQLite database lives in the container's /data directory; the path is
# fixed so the stack does not require an environment variable for it.
DB_URL = "sqlite:////data/db.sqlite"

# Authentication token for admin endpoints; can still be overridden via
# environment variable when the backend starts.
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "change-me")

ADMIN_SESSION_COOKIE = "admin_oauth_token"

YTMUSIC_AUTH_FILE = os.getenv("YTMUSIC_AUTH_FILE")

SETTINGS_ENV_MAP: Dict[str, str] = {
    "twitch_client_id": "TWITCH_CLIENT_ID",
    "twitch_client_secret": "TWITCH_CLIENT_SECRET",
    "twitch_redirect_uri": "TWITCH_REDIRECT_URI",
    "bot_redirect_uri": "BOT_TWITCH_REDIRECT_URI",
    "twitch_scopes": "TWITCH_SCOPES",
    "bot_app_scopes": "BOT_APP_SCOPES",
}

SETTINGS_DEFAULTS: Dict[str, Optional[str]] = {
    "twitch_scopes": "channel:bot channel:read:subscriptions channel:read:vips bits:read moderator:read:followers",
    "bot_app_scopes": "user:read:chat user:write:chat user:bot",
}

SETUP_REQUIRED_KEYS = ("twitch_client_id", "twitch_client_secret")

DEFAULT_TWITCH_SCOPES = SETTINGS_DEFAULTS["twitch_scopes"].split()
DEFAULT_BOT_APP_SCOPES = SETTINGS_DEFAULTS["bot_app_scopes"].split()

API_VERSION = "0.1.0"


def _env_flag(value: Optional[str]) -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


DEV_MODE = True

APP_ACCESS_TOKEN: Optional[str] = None
APP_TOKEN_EXPIRES = 0
BOT_USER_ID: Optional[str] = None

EVENTSUB_EVENT_MAP: dict[str, str] = {
    "channel.follow": "follow",
    "channel.raid": "raid",
    "channel.cheer": "bits",
    "channel.subscribe": "sub",
    "channel.subscription.gift": "gift_sub",
}

_bot_log_listeners: set[asyncio.Queue[str]] = set()
_bot_oauth_states: dict[str, Dict[str, Any]] = {}

logger = logging.getLogger(__name__)

engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

class AppSetting(Base):
    __tablename__ = "app_settings"

    key = Column(String, primary_key=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

_ytmusic_client: Optional[YTMusic] = None
_ytmusic_lock = Lock()


def generate_channel_key() -> str:
    """Create a per-channel secret using `secrets.token_urlsafe` for authenticated access.

    Dependencies: Relies on the standard-library `secrets` module for entropy.
    Code customers: Channel creation flows and migration helpers call this utility.
    Used variables/origin: No external inputs; uses `secrets.token_urlsafe(32)` to produce the key material.
    """

    return secrets.token_urlsafe(32)


def ensure_channel_key_schema() -> None:
    """Ensure the `channel_key` column exists in the `active_channels` table before startup.

    Dependencies: Uses SQLAlchemy `inspect` and a raw `ALTER TABLE` executed via `engine.begin()`.
    Code customers: Invoked at module import to keep persisted databases aligned with the model.
    Used variables/origin: Reads from the global `engine` metadata and mutates the `active_channels` table when needed.
    """

    inspector = inspect(engine)
    columns = {col["name"] for col in inspector.get_columns("active_channels")}
    if "channel_key" not in columns:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE active_channels ADD COLUMN channel_key VARCHAR"))


def ensure_channel_settings_schema() -> None:
    """Backfill new columns on `channel_settings` for legacy databases.

    Dependencies: Uses SQLAlchemy inspection against the global ``engine`` and
    executes raw ALTER TABLE statements when fields are missing.
    Code customers: Startup bootstrap that needs the latest settings columns
    before serving traffic.
    Used variables/origin: Operates on the ``channel_settings`` table and adds
    the ``full_auto_priority_mode`` flag plus priority pricing columns with
    defaults when absent.
    """

    inspector = inspect(engine)
    if "channel_settings" not in inspector.get_table_names():
        return

    columns = {col["name"] for col in inspector.get_columns("channel_settings")}
    required_columns = {
        "full_auto_priority_mode": "ALTER TABLE channel_settings ADD COLUMN full_auto_priority_mode INTEGER DEFAULT 0",
        "prio_follow_enabled": "ALTER TABLE channel_settings ADD COLUMN prio_follow_enabled INTEGER NOT NULL DEFAULT 1",
        "prio_raid_enabled": "ALTER TABLE channel_settings ADD COLUMN prio_raid_enabled INTEGER NOT NULL DEFAULT 1",
        "prio_bits_per_point": "ALTER TABLE channel_settings ADD COLUMN prio_bits_per_point INTEGER NOT NULL DEFAULT 200",
        "prio_gifts_per_point": "ALTER TABLE channel_settings ADD COLUMN prio_gifts_per_point INTEGER NOT NULL DEFAULT 5",
        "prio_sub_tier1_points": "ALTER TABLE channel_settings ADD COLUMN prio_sub_tier1_points INTEGER NOT NULL DEFAULT 0",
        "prio_sub_tier2_points": "ALTER TABLE channel_settings ADD COLUMN prio_sub_tier2_points INTEGER NOT NULL DEFAULT 0",
        "prio_sub_tier3_points": "ALTER TABLE channel_settings ADD COLUMN prio_sub_tier3_points INTEGER NOT NULL DEFAULT 0",
        "prio_reset_points_tier1": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_tier1 INTEGER NOT NULL DEFAULT 0",
        "prio_reset_points_tier2": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_tier2 INTEGER NOT NULL DEFAULT 0",
        "prio_reset_points_tier3": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_tier3 INTEGER NOT NULL DEFAULT 0",
        "prio_reset_points_vip": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_vip INTEGER NOT NULL DEFAULT 0",
        "prio_reset_points_mod": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_mod INTEGER NOT NULL DEFAULT 0",
        "free_mod_priority_requests": "ALTER TABLE channel_settings ADD COLUMN free_mod_priority_requests INTEGER NOT NULL DEFAULT 0",
    }
    missing = {name: ddl for name, ddl in required_columns.items() if name not in columns}
    if not missing:
        return
    with engine.begin() as conn:
        for ddl in missing.values():
            conn.execute(text(ddl))


def backfill_missing_channel_keys() -> None:
    """Assign generated keys to any existing channels lacking a `channel_key` value.

    Dependencies: Opens a database session via `SessionLocal` and uses `generate_channel_key` for new secrets.
    Code customers: Runs during startup to keep legacy rows compatible with the channel-key auth dependency.
    Used variables/origin: Reads `ActiveChannel.channel_key` and writes generated values back to the same records.
    """

    db = SessionLocal()
    try:
        missing = db.query(ActiveChannel).filter(
            (ActiveChannel.channel_key.is_(None)) | (ActiveChannel.channel_key == "")
        ).all()
        for channel in missing:
            channel.channel_key = generate_channel_key()
        if missing:
            db.commit()
    finally:
        db.close()
def _load_settings_from_db() -> Dict[str, Optional[str]]:
    db = SessionLocal()
    try:
        rows = db.query(AppSetting).all()
        values = {row.key: row.value for row in rows}
    finally:
        db.close()
    for key, default in SETTINGS_DEFAULTS.items():
        values.setdefault(key, default)
    return values


class SettingsStore:
    __slots__ = ("_cache", "_lock")

    def __init__(self) -> None:
        self._cache: Optional[Dict[str, Optional[str]]] = None
        self._lock = Lock()

    def snapshot(self) -> Dict[str, Optional[str]]:
        with self._lock:
            if self._cache is None:
                self._cache = _load_settings_from_db()
            return dict(self._cache)

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        value = self.snapshot().get(key, default)
        if value is None:
            return default
        value_str = str(value).strip()
        return value_str or default

    def get_list(self, key: str) -> list[str]:
        raw = self.get(key)
        if not raw:
            return []
        parts = re.split(r"[\s,]+", raw)
        return [part for part in parts if part]

    def invalidate(self) -> None:
        with self._lock:
            self._cache = None


settings_store = SettingsStore()


def _settings_requirements_met(values: Mapping[str, Optional[str]]) -> bool:
    for key in SETUP_REQUIRED_KEYS:
        if not str(values.get(key) or "").strip():
            return False
    return True


def bootstrap_settings_from_env() -> None:
    db = SessionLocal()
    try:
        existing_rows = {row.key: row for row in db.query(AppSetting).all()}
        values: Dict[str, Optional[str]] = {key: row.value for key, row in existing_rows.items()}
        changed = False

        for key, env_name in SETTINGS_ENV_MAP.items():
            env_value = os.getenv(env_name)
            if env_value is None:
                continue
            trimmed = env_value.strip()
            if not trimmed:
                continue
            if key in existing_rows:
                if not existing_rows[key].value:
                    existing_rows[key].value = trimmed
                    values[key] = trimmed
                    changed = True
            else:
                db.add(AppSetting(key=key, value=trimmed))
                values[key] = trimmed
                changed = True

        for key, default in SETTINGS_DEFAULTS.items():
            if key not in values:
                db.add(AppSetting(key=key, value=default))
                values[key] = default
                changed = True

        setup_value = values.get("setup_complete")
        if setup_value is None:
            complete = "1" if _settings_requirements_met(values) else "0"
            db.add(AppSetting(key="setup_complete", value=complete))
            values["setup_complete"] = complete
            changed = True

        if changed:
            db.commit()
        else:
            db.rollback()
    finally:
        db.close()
    settings_store.invalidate()


def _persist_settings(db: Session, updates: Mapping[str, Optional[str]]) -> None:
    for key, value in updates.items():
        row = db.get(AppSetting, key)
        if value is None or (isinstance(value, str) and not value.strip()):
            normalized = None
        else:
            normalized = value.strip() if isinstance(value, str) else str(value)
        if row:
            row.value = normalized
        else:
            db.add(AppSetting(key=key, value=normalized))


def set_settings(db: Session, updates: Mapping[str, Optional[str]]) -> Dict[str, Optional[str]]:
    _persist_settings(db, updates)
    db.commit()
    settings_store.invalidate()
    return settings_store.snapshot()


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    return settings_store.get(key, default)


def get_scopes_setting(key: str, default: Optional[list[str]] = None) -> list[str]:
    scopes = settings_store.get_list(key)
    if scopes:
        return scopes
    return list(default or [])


def is_setup_complete() -> bool:
    value = get_setting("setup_complete", "0")
    return str(value).strip() in {"1", "true", "yes", "on"}


def get_twitch_client_id() -> Optional[str]:
    value = get_setting("twitch_client_id")
    return value.strip() if value else None


def get_twitch_client_secret() -> Optional[str]:
    value = get_setting("twitch_client_secret")
    return value.strip() if value else None


def get_twitch_redirect_uri() -> Optional[str]:
    value = get_setting("twitch_redirect_uri")
    return value.strip() if value else None


def get_bot_redirect_uri() -> Optional[str]:
    value = get_setting("bot_redirect_uri")
    return value.strip() if value else None


def get_twitch_scopes() -> list[str]:
    """
    Return the channel-level Twitch OAuth scopes required by the Queue Manager.

    Dependencies: uses ``get_scopes_setting`` to read persisted overrides and the
    ``DEFAULT_TWITCH_SCOPES`` constant for defaults. Code customers include the
    ``/system/config`` payload that powers the Queue Manager login flow. Used
    variables/origin: combines the stored ``twitch_scopes`` setting (if any)
    with ``DEFAULT_TWITCH_SCOPES`` to ensure the pricing-related bits and
    follower permissions are always requested.
    """
    scopes = get_scopes_setting("twitch_scopes", DEFAULT_TWITCH_SCOPES)
    return scopes or list(DEFAULT_TWITCH_SCOPES)


def get_bot_app_scopes() -> list[str]:
    scopes = get_scopes_setting("bot_app_scopes", DEFAULT_BOT_APP_SCOPES)
    return scopes or list(DEFAULT_BOT_APP_SCOPES)


def _system_config_payload() -> Dict[str, Any]:
    return {
        "setup_complete": is_setup_complete(),
        "twitch_client_id": get_twitch_client_id(),
        "twitch_client_secret_set": bool(get_twitch_client_secret()),
        "twitch_redirect_uri": get_twitch_redirect_uri(),
        "bot_redirect_uri": get_bot_redirect_uri(),
        "twitch_scopes": get_twitch_scopes(),
        "bot_app_scopes": get_bot_app_scopes(),
    }


def get_app_access_token() -> str:
    global APP_ACCESS_TOKEN, APP_TOKEN_EXPIRES
    if not APP_ACCESS_TOKEN or time.time() > APP_TOKEN_EXPIRES:
        client_id = get_twitch_client_id()
        client_secret = get_twitch_client_secret()
        if not client_id or not client_secret:
            raise RuntimeError("twitch oauth credentials are not configured")
        response = requests.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
            timeout=5,
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise requests.HTTPError(
                "Twitch app access token response was not valid JSON",
                response=response,
            ) from exc
        token = payload.get("access_token")
        if not token:
            message = payload.get("message") or payload
            raise requests.HTTPError(
                f"Twitch app access token response missing access_token: {message}",
                response=response,
            )
        APP_ACCESS_TOKEN = token
        expires_in = int(payload.get("expires_in", 3600))
        APP_TOKEN_EXPIRES = time.time() + expires_in - 60
    return APP_ACCESS_TOKEN


def get_bot_user_id() -> Optional[str]:
    global BOT_USER_ID
    if BOT_USER_ID:
        return BOT_USER_ID
    login: Optional[str] = None
    db = SessionLocal()
    try:
        cfg = db.query(BotConfig).order_by(BotConfig.id.asc()).first()
        if cfg and cfg.login:
            login = cfg.login
    finally:
        db.close()
    if not login:
        return None
    client_id = get_twitch_client_id()
    if not client_id:
        return None
    token = get_app_access_token()
    headers = {"Authorization": f"Bearer {token}", "Client-Id": client_id}
    resp = requests.get(
        "https://api.twitch.tv/helix/users",
        params={"login": login},
        headers=headers,
    ).json()
    data = resp.get("data", [])
    if data:
        BOT_USER_ID = data[0]["id"]
    return BOT_USER_ID


def _eventsub_headers(access_token: str) -> dict[str, str]:
    """Return Twitch Helix headers for EventSub requests using the channel token.

    Dependencies: Reads the configured Twitch client ID for the application.
    Code customers: EventSub subscription management and health probes use these
    headers to authenticate against Twitch APIs with the channel owner token.
    Used variables/origin: Combines the provided ``access_token`` with the
    configured client ID to build the Authorization and Client-Id headers.
    """

    client_id = get_twitch_client_id()
    if not client_id:
        raise RuntimeError("twitch oauth credentials are not configured")
    return {"Authorization": f"Bearer {access_token}", "Client-Id": client_id}


def ensure_eventsub_subscriptions(request: FastAPIRequest, channel_pk: int, db: Session) -> None:
    """Ensure required EventSub subscriptions are registered for a channel.

    Dependencies: Issues HTTPS calls to Twitch Helix using the channel owner's
    OAuth token and configured client ID. Persists subscription rows via the
    provided SQLAlchemy ``Session``.
    Code customers: The Twitch OAuth callback invokes this to keep pricing
    events flowing automatically without extra setup.
    Used variables/origin: ``channel_pk`` resolves the broadcaster row; the
    callback URL derives from ``request.url_for('eventsub_callback')`` and the
    function records the Twitch subscription identifiers plus HMAC secrets in
    ``EventSubscription`` rows.
    """

    channel = db.get(ActiveChannel, channel_pk)
    if not channel:
        logger.warning("Cannot create EventSub subscriptions; channel %s missing", channel_pk)
        return
    owner = channel.owner
    if not owner or not owner.access_token:
        logger.info("Skipping EventSub setup for %s; owner token missing", channel.channel_name)
        return
    try:
        headers = _eventsub_headers(owner.access_token)
    except RuntimeError as exc:
        logger.warning("Skipping EventSub setup for %s: %s", channel.channel_name, exc)
        return

    callback = str(request.url_for("eventsub_callback"))
    broadcaster_id = channel.channel_id
    moderator_id = owner.twitch_id
    desired: list[tuple[str, dict[str, str]]] = [
        ("channel.follow", {"broadcaster_user_id": broadcaster_id, "moderator_user_id": moderator_id}),
        ("channel.raid", {"to_broadcaster_user_id": broadcaster_id}),
        ("channel.cheer", {"broadcaster_user_id": broadcaster_id}),
        ("channel.subscribe", {"broadcaster_user_id": broadcaster_id}),
        ("channel.subscription.gift", {"broadcaster_user_id": broadcaster_id}),
    ]
    for event_type, condition in desired:
        if None in condition.values():
            logger.info(
                "Skipping EventSub %s for %s due to incomplete condition %s",
                event_type,
                channel.channel_name,
                condition,
            )
            continue
        existing = (
            db.query(EventSubscription)
            .filter(EventSubscription.channel_id == channel_pk, EventSubscription.type == event_type)
            .one_or_none()
        )
        if existing and existing.status == "enabled" and existing.callback == callback:
            continue
        secret = secrets.token_urlsafe(32)
        payload = {
            "type": event_type,
            "version": "1",
            "condition": condition,
            "transport": {
                "method": "webhook",
                "callback": callback,
                "secret": secret,
            },
        }
        try:
            resp = requests.post(
                "https://api.twitch.tv/helix/eventsub/subscriptions",
                json=payload,
                headers=headers,
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json().get("data") or []
            twitch_sub = data[0] if data else resp.json().get("subscription", {})
            twitch_id = twitch_sub.get("id")
            status = twitch_sub.get("status") or "pending"
        except Exception as exc:  # pragma: no cover - depends on live Twitch
            logger.warning(
                "Failed to register EventSub %s for %s: %s", event_type, channel.channel_name, exc
            )
            continue
        if not twitch_id:
            logger.warning(
                "EventSub %s for %s returned no subscription id; response=%s",
                event_type,
                channel.channel_name,
                resp.text if "resp" in locals() else "<no response>",
            )
            continue
        meta_str = json.dumps({"condition": condition})
        if existing:
            existing.twitch_subscription_id = twitch_id
            existing.status = status
            existing.secret = secret
            existing.callback = callback
            existing.meta = meta_str
            if status == "enabled":
                existing.last_verified_at = datetime.utcnow()
        else:
            db.add(
                EventSubscription(
                    channel_id=channel_pk,
                    twitch_subscription_id=twitch_id,
                    type=event_type,
                    status=status,
                    secret=secret,
                    callback=callback,
                    meta=meta_str,
                    last_verified_at=datetime.utcnow() if status == "enabled" else None,
                )
            )
        db.commit()


def _verify_eventsub_signature(secret: str, message_id: str, timestamp: str, body: bytes, provided: str) -> bool:
    """Validate the HMAC signature on an EventSub webhook payload.

    Dependencies: Uses the standard library ``hmac`` and ``hashlib`` modules to
    recompute the Twitch signature.
    Code customers: The EventSub callback uses this guard before trusting any
    inbound payloads.
    Used variables/origin: Combines the ``message_id``, ``timestamp``, and raw
    request body bytes to mirror Twitch's signature creation routine.
    """

    digest = hmac.new(secret.encode("utf-8"), msg=(message_id + timestamp).encode("utf-8") + body, digestmod=hashlib.sha256)
    expected = f"sha256={digest.hexdigest()}"
    return hmac.compare_digest(expected, provided or "")


def _process_eventsub_notification(db: Session, subscription: EventSubscription, message: dict[str, Any]) -> None:
    """Translate an EventSub notification into a stored event and priority rewards.

    Dependencies: Uses ``_persist_channel_event`` to write the event and award
    points, and ``_get_or_create_channel_user`` to make sure Twitch users are
    represented locally.
    Code customers: Invoked exclusively by the EventSub webhook handler so
    pricing rewards trigger for follows, raids, cheers, and subscriptions.
    Used variables/origin: Reads the Twitch event payload to determine the
    originating channel, the triggering user, and any numeric metadata (bits
    amounts or gift counts).
    """

    twitch_type = subscription.type
    internal_type = EVENTSUB_EVENT_MAP.get(twitch_type)
    if not internal_type:
        logger.debug("Ignoring unhandled EventSub type %s", twitch_type)
        return
    channel = db.get(ActiveChannel, subscription.channel_id)
    if not channel:
        logger.warning("Received EventSub %s for missing channel %s", twitch_type, subscription.channel_id)
        return
    event_payload = message.get("event") or {}
    broadcaster_id = (message.get("subscription") or {}).get("condition", {}).get("broadcaster_user_id")
    if broadcaster_id and broadcaster_id != channel.channel_id:
        logger.warning(
            "EventSub %s target mismatch: payload broadcaster %s vs channel %s",
            twitch_type,
            broadcaster_id,
            channel.channel_id,
        )
        return

    twitch_user_id = event_payload.get("user_id") or event_payload.get("from_broadcaster_user_id")
    username = event_payload.get("user_login") or event_payload.get("from_broadcaster_user_login") or twitch_user_id
    meta: dict[str, Any] = {}
    if twitch_type == "channel.raid":
        meta["viewers"] = event_payload.get("viewers")
    elif twitch_type == "channel.cheer":
        meta["amount"] = event_payload.get("bits") or 0
    elif twitch_type == "channel.subscription.gift":
        meta["count"] = event_payload.get("total") or event_payload.get("total_subs") or 1
        meta["tier"] = event_payload.get("tier")
    elif twitch_type == "channel.subscribe":
        meta["tier"] = event_payload.get("tier")
        meta["count"] = 1

    user_id: Optional[int] = None
    if twitch_user_id:
        user = _get_or_create_channel_user(db, channel.id, twitch_user_id, username or twitch_user_id)
        db.flush()
        user_id = user.id

    _persist_channel_event(
        db,
        channel.id,
        EventIn(type=internal_type, user_id=user_id, meta=meta),
    )
    subscription.last_notified_at = datetime.utcnow()
    db.commit()


def _fetch_remote_eventsubs(channel: ActiveChannel) -> list[dict[str, Any]]:
    """Return live EventSub subscription data from Twitch for the channel's owner.

    Dependencies: Makes a Helix request using the channel owner's access token
    and the configured client ID via ``_eventsub_headers``.
    Code customers: Health endpoints use this to cross-check persisted
    subscriptions with Twitch state.
    Used variables/origin: Reads the channel's ``owner.access_token`` and
    ``channel.channel_id`` to scope the results to relevant subscriptions.
    """

    owner = channel.owner
    if not owner or not owner.access_token:
        return []
    try:
        headers = _eventsub_headers(owner.access_token)
    except RuntimeError:
        return []
    try:
        resp = requests.get(
            "https://api.twitch.tv/helix/eventsub/subscriptions",
            headers=headers,
            timeout=5,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data") or []
    except Exception:
        logger.warning("Failed to fetch remote EventSub subscriptions for %s", channel.channel_name, exc_info=True)
        return []


def get_ytmusic_client() -> YTMusic:
    global _ytmusic_client
    if YTMusic is None:
        raise RuntimeError("ytmusicapi dependency is not installed")
    if _ytmusic_client is not None:
        return _ytmusic_client
    with _ytmusic_lock:
        if _ytmusic_client is not None:
            return _ytmusic_client
        try:
            if YTMUSIC_AUTH_FILE:
                client = YTMusic(YTMUSIC_AUTH_FILE)
            else:
                client = YTMusic()
        except Exception as exc:
            logger.exception("Failed to initialize YTMusic client")
            raise RuntimeError("YTMusic client initialization failed") from exc
        _ytmusic_client = client
    return _ytmusic_client


# =====================================
# Models
# =====================================
class ActiveChannel(Base):
    __tablename__ = "active_channels"
    id = Column(Integer, primary_key=True)
    channel_id = Column(String, unique=True, nullable=False)  # Twitch channel ID
    channel_name = Column(String, nullable=False)
    channel_key = Column(String, nullable=True, unique=True)
    join_active = Column(Integer, default=1)
    authorized = Column(Boolean, default=False)
    owner_id = Column(Integer, ForeignKey("twitch_users.id"))

    owner = relationship("TwitchUser", back_populates="owned_channels")
    settings = relationship("ChannelSettings", back_populates="channel", uselist=False, cascade="all, delete-orphan")
    songs = relationship("Song", back_populates="channel", cascade="all, delete-orphan")
    users = relationship("User", back_populates="channel", cascade="all, delete-orphan")
    moderators = relationship("ChannelModerator", back_populates="channel", cascade="all, delete-orphan")
    bot_state = relationship(
        "ChannelBotState",
        back_populates="channel",
        uselist=False,
        cascade="all, delete-orphan",
    )
    playlists = relationship("Playlist", back_populates="channel", cascade="all, delete-orphan")

    @property
    def bot_active(self) -> bool:
        state = self.bot_state
        return bool(state and state.active)

    @property
    def bot_last_error(self) -> Optional[str]:
        state = self.bot_state
        return state.last_error if state else None


class ChannelBotState(Base):
    __tablename__ = "channel_bot_state"
    id = Column(Integer, primary_key=True)
    channel_id = Column(
        Integer,
        ForeignKey("active_channels.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    active = Column(Boolean, default=False, nullable=False)
    last_error = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    channel = relationship("ActiveChannel", back_populates="bot_state")

class ChannelSettings(Base):
    __tablename__ = "channel_settings"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    max_requests_per_user = Column(Integer, default=-1)
    prio_only = Column(Integer, default=0)
    queue_closed = Column(Integer, default=0)
    allow_bumps = Column(Integer, default=1)
    full_auto_priority_mode = Column(Integer, default=0)
    other_flags = Column(Text)
    max_prio_points = Column(Integer, default=10)
    overall_queue_cap = Column(Integer, default=100)
    nonpriority_queue_cap = Column(Integer, default=100)
    prio_follow_enabled = Column(Integer, default=1)
    prio_raid_enabled = Column(Integer, default=1)
    prio_bits_per_point = Column(Integer, default=200)
    prio_gifts_per_point = Column(Integer, default=5)
    prio_sub_tier1_points = Column(Integer, default=0)
    prio_sub_tier2_points = Column(Integer, default=0)
    prio_sub_tier3_points = Column(Integer, default=0)
    prio_reset_points_tier1 = Column(Integer, default=0)
    prio_reset_points_tier2 = Column(Integer, default=0)
    prio_reset_points_tier3 = Column(Integer, default=0)
    prio_reset_points_vip = Column(Integer, default=0)
    prio_reset_points_mod = Column(Integer, default=0)
    free_mod_priority_requests = Column(Integer, default=0)

    channel = relationship("ActiveChannel", back_populates="settings")

class Song(Base):
    __tablename__ = "songs"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    artist = Column(String, nullable=False)
    title = Column(String, nullable=False)
    youtube_link = Column(Text)
    date_first_played = Column(DateTime)
    date_last_played = Column(DateTime)
    total_played = Column(Integer, default=0)
    mixed_tags = Column(Text)  # JSON or CSV
    is_banned = Column(Integer, default=0)
    is_inactive = Column(Integer, default=0)

    channel = relationship("ActiveChannel", back_populates="songs")

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    twitch_id = Column(String, nullable=False)
    username = Column(String, nullable=False)
    amount_requested = Column(Integer, default=0)
    prio_points = Column(Integer, default=0)

    channel = relationship("ActiveChannel", back_populates="users")

class StreamSession(Base):
    __tablename__ = "stream_sessions"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    ended_at = Column(DateTime)

class UserStreamState(Base):
    __tablename__ = "user_stream_state"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    stream_id = Column(Integer, ForeignKey("stream_sessions.id", ondelete="CASCADE"), nullable=False)
    sub_free_used = Column(Integer, default=0)
    __table_args__ = (UniqueConstraint('user_id', 'stream_id', name='uq_user_stream'),)

class Request(Base):
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    stream_id = Column(Integer, ForeignKey("stream_sessions.id", ondelete="SET NULL"))
    song_id = Column(Integer, ForeignKey("songs.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    request_time = Column(DateTime, default=datetime.utcnow)
    is_priority = Column(Integer, default=0)
    bumped = Column(Integer, default=0)
    played = Column(Integer, default=0)
    priority_source = Column(String)  # 'points' | 'sub_free' | 'admin' | None
    position = Column(Integer, nullable=False, default=0, index=True)

class Event(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    event_type = Column(String, nullable=False)  # raid, sub, gift_sub, follow
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"))
    meta = Column(Text)  # JSON string
    event_time = Column(DateTime, default=datetime.utcnow)


class EventSubscription(Base):
    __tablename__ = "event_subscriptions"

    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    twitch_subscription_id = Column(String, unique=True, nullable=False)
    type = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")
    secret = Column(String, nullable=False)
    callback = Column(Text, nullable=False)
    transport = Column(String, nullable=False, default="webhook")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_notified_at = Column(DateTime)
    last_verified_at = Column(DateTime)
    meta = Column(Text)

    __table_args__ = (
        UniqueConstraint("channel_id", "type", name="uq_channel_eventsub"),
    )

class TwitchUser(Base):
    __tablename__ = "twitch_users"
    id = Column(Integer, primary_key=True)
    twitch_id = Column(String, unique=True, nullable=False)
    username = Column(String, nullable=False)
    access_token = Column(String, nullable=False)
    refresh_token = Column(String)
    scopes = Column(Text)

    owned_channels = relationship("ActiveChannel", back_populates="owner")

class ChannelModerator(Base):
    __tablename__ = "channel_moderators"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("twitch_users.id", ondelete="CASCADE"), nullable=False)
    channel = relationship("ActiveChannel", back_populates="moderators")
    user = relationship("TwitchUser")
    __table_args__ = (UniqueConstraint('channel_id', 'user_id', name='uq_channel_mod'),)

class BotConfig(Base):
    __tablename__ = "bot_config"
    id = Column(Integer, primary_key=True)
    login = Column(String, nullable=True)
    display_name = Column(String, nullable=True)
    scopes = Column(Text, nullable=True)
    access_token = Column(Text, nullable=True)
    refresh_token = Column(Text, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    enabled = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Playlist(Base):
    __tablename__ = "playlists"
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey("active_channels.id", ondelete="CASCADE"), nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    playlist_id = Column(String, nullable=True)
    url = Column(Text, nullable=True)
    source = Column(String, nullable=False, default="youtube")
    visibility = Column(String, nullable=False, default="public")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    channel = relationship("ActiveChannel", back_populates="playlists")
    keywords = relationship("PlaylistKeyword", back_populates="playlist", cascade="all, delete-orphan")
    items = relationship("PlaylistItem", back_populates="playlist", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("channel_id", "playlist_id", name="uq_playlists_channel_playlist"),
    )


class PlaylistKeyword(Base):
    __tablename__ = "playlist_keywords"
    id = Column(Integer, primary_key=True)
    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False)
    keyword = Column(String, nullable=False)

    playlist = relationship("Playlist", back_populates="keywords")

    __table_args__ = (
        UniqueConstraint("playlist_id", "keyword", name="uq_playlist_keyword"),
    )


class PlaylistItem(Base):
    __tablename__ = "playlist_items"
    id = Column(Integer, primary_key=True)
    playlist_id = Column(Integer, ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False)
    position = Column(Integer, default=0, nullable=False)
    video_id = Column(String, nullable=True)
    title = Column(String, nullable=False)
    artist = Column(String, nullable=True)
    duration_seconds = Column(Integer)
    url = Column(Text, nullable=True)

    playlist = relationship("Playlist", back_populates="items")

    __table_args__ = (
        UniqueConstraint("playlist_id", "video_id", name="uq_playlist_item_video"),
    )

# =====================================
# DB bootstrap
# =====================================
Base.metadata.create_all(bind=engine)


def _ensure_channel_settings_schema() -> None:
    """Ensure channel settings tables include queue capacity columns for legacy DBs.

    Dependencies: Relies on the module-level SQLAlchemy ``engine`` and ``inspect``
    helpers to introspect the ``channel_settings`` table and execute ``ALTER``
    statements when columns are missing.
    Code customers: Runtime settings reads/writes, queue enforcement, event
    emitters, and tests that assume queue capacity fields exist.
    Used variables/origin: Reads the discovered column names from the inspector
    and applies a default of ``100`` for both ``overall_queue_cap`` and
    ``nonpriority_queue_cap`` when adding or backfilling those fields.
    """

    with engine.begin() as connection:
        inspector = inspect(connection)
        if "channel_settings" not in inspector.get_table_names():
            return

        columns = {column["name"] for column in inspector.get_columns("channel_settings")}
        pricing_columns = {
            "prio_follow_enabled": "ALTER TABLE channel_settings ADD COLUMN prio_follow_enabled INTEGER NOT NULL DEFAULT 1",
            "prio_raid_enabled": "ALTER TABLE channel_settings ADD COLUMN prio_raid_enabled INTEGER NOT NULL DEFAULT 1",
            "prio_bits_per_point": "ALTER TABLE channel_settings ADD COLUMN prio_bits_per_point INTEGER NOT NULL DEFAULT 200",
            "prio_gifts_per_point": "ALTER TABLE channel_settings ADD COLUMN prio_gifts_per_point INTEGER NOT NULL DEFAULT 5",
            "prio_sub_tier1_points": "ALTER TABLE channel_settings ADD COLUMN prio_sub_tier1_points INTEGER NOT NULL DEFAULT 0",
            "prio_sub_tier2_points": "ALTER TABLE channel_settings ADD COLUMN prio_sub_tier2_points INTEGER NOT NULL DEFAULT 0",
            "prio_sub_tier3_points": "ALTER TABLE channel_settings ADD COLUMN prio_sub_tier3_points INTEGER NOT NULL DEFAULT 0",
            "prio_reset_points_tier1": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_tier1 INTEGER NOT NULL DEFAULT 0",
            "prio_reset_points_tier2": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_tier2 INTEGER NOT NULL DEFAULT 0",
            "prio_reset_points_tier3": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_tier3 INTEGER NOT NULL DEFAULT 0",
            "prio_reset_points_vip": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_vip INTEGER NOT NULL DEFAULT 0",
            "prio_reset_points_mod": "ALTER TABLE channel_settings ADD COLUMN prio_reset_points_mod INTEGER NOT NULL DEFAULT 0",
            "free_mod_priority_requests": "ALTER TABLE channel_settings ADD COLUMN free_mod_priority_requests INTEGER NOT NULL DEFAULT 0",
        }

        if "overall_queue_cap" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE channel_settings "
                    "ADD COLUMN overall_queue_cap INTEGER NOT NULL DEFAULT 100"
                )
            )
        else:
            connection.execute(
                text(
                    "UPDATE channel_settings SET overall_queue_cap = 100 "
                    "WHERE overall_queue_cap IS NULL"
                )
            )

        if "nonpriority_queue_cap" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE channel_settings "
                    "ADD COLUMN nonpriority_queue_cap INTEGER NOT NULL DEFAULT 100"
                )
            )
        else:
            connection.execute(
                text(
                    "UPDATE channel_settings SET nonpriority_queue_cap = 100 "
                    "WHERE nonpriority_queue_cap IS NULL"
                )
            )

        for name, ddl in pricing_columns.items():
            if name not in columns:
                connection.execute(text(ddl))


def _ensure_playlist_schema() -> None:
    """Ensure legacy databases have the latest playlist columns."""

    with engine.begin() as connection:
        inspector = inspect(connection)
        if "playlists" not in inspector.get_table_names():
            return

        columns = {column["name"] for column in inspector.get_columns("playlists")}

        if "description" not in columns:
            connection.execute(text("ALTER TABLE playlists ADD COLUMN description TEXT"))

        added_source = False
        if "source" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE playlists ADD COLUMN source VARCHAR NOT NULL DEFAULT 'youtube'"
                )
            )
            added_source = True

        added_visibility = False
        if "visibility" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE playlists ADD COLUMN visibility VARCHAR NOT NULL DEFAULT 'public'"
                )
            )
            added_visibility = True

        if added_source:
            connection.execute(
                text("UPDATE playlists SET source = 'youtube' WHERE source IS NULL OR source = ''")
            )

        if added_visibility:
            connection.execute(
                text(
                    "UPDATE playlists SET visibility = 'public' WHERE visibility IS NULL OR visibility = ''"
                )
            )

        inspector = inspect(connection)
        playlist_columns = {
            column["name"]: column for column in inspector.get_columns("playlists")
        }
        playlist_id_column = playlist_columns.get("playlist_id")
        if playlist_id_column and not playlist_id_column["nullable"]:
            connection.execute(text("DROP TABLE IF EXISTS playlists_tmp"))
            connection.execute(
                text(
                    """
                    CREATE TABLE playlists_tmp (
                        id INTEGER PRIMARY KEY,
                        channel_id INTEGER NOT NULL REFERENCES active_channels(id) ON DELETE CASCADE,
                        title VARCHAR NOT NULL,
                        description TEXT,
                        playlist_id VARCHAR,
                        url TEXT,
                        source VARCHAR NOT NULL DEFAULT 'youtube',
                        visibility VARCHAR NOT NULL DEFAULT 'public',
                        created_at DATETIME NOT NULL,
                        updated_at DATETIME NOT NULL,
                        CONSTRAINT uq_playlists_channel_playlist UNIQUE (channel_id, playlist_id)
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO playlists_tmp (
                        id, channel_id, title, description, playlist_id, url, source,
                        visibility, created_at, updated_at
                    )
                    SELECT
                        id, channel_id, title, description, playlist_id, url, source,
                        visibility, created_at, updated_at
                    FROM playlists
                    """
                )
            )
            connection.execute(text("DROP TABLE playlists"))
            connection.execute(text("ALTER TABLE playlists_tmp RENAME TO playlists"))


_ensure_channel_settings_schema()
_ensure_playlist_schema()
bootstrap_settings_from_env()

# =====================================
# Schemas
# =====================================
class ChannelIn(BaseModel):
    channel_name: str
    channel_id: str
    join_active: int = 1

class ChannelOut(BaseModel):
    id: int
    channel_name: str
    channel_id: str
    join_active: int
    authorized: bool
    bot_active: bool
    bot_last_error: Optional[str] = None

    class Config:
        from_attributes = True


class ChannelLiveStatusOut(BaseModel):
    channel_name: str
    channel_id: str
    is_live: bool


class ChannelOAuthOut(BaseModel):
    channel_name: str
    authorized: bool
    owner_login: Optional[str] = None
    scopes: List[str] = Field(default_factory=list)


class ChannelKeyOut(BaseModel):
    channel_id: int
    channel_name: str
    channel_key: str

class ChannelSettingsBase(BaseModel):
    max_requests_per_user: int = -1
    prio_only: int = 0
    queue_closed: int = 0
    allow_bumps: int = 1
    full_auto_priority_mode: int = 0
    other_flags: Optional[str] = None
    max_prio_points: int = 10
    overall_queue_cap: int = Field(100, ge=0, le=100)
    nonpriority_queue_cap: int = Field(100, ge=0, le=100)
    prio_follow_enabled: int = 1
    prio_raid_enabled: int = 1
    prio_bits_per_point: int = Field(200, ge=0)
    prio_gifts_per_point: int = Field(5, ge=0)
    prio_sub_tier1_points: int = Field(0, ge=0)
    prio_sub_tier2_points: int = Field(0, ge=0)
    prio_sub_tier3_points: int = Field(0, ge=0)
    prio_reset_points_tier1: int = Field(0, ge=0)
    prio_reset_points_tier2: int = Field(0, ge=0)
    prio_reset_points_tier3: int = Field(0, ge=0)
    prio_reset_points_vip: int = Field(0, ge=0)
    prio_reset_points_mod: int = Field(0, ge=0)
    free_mod_priority_requests: int = 0


class ChannelSettingsUpdate(BaseModel):
    max_requests_per_user: Optional[int] = None
    prio_only: Optional[int] = None
    queue_closed: Optional[int] = None
    allow_bumps: Optional[int] = None
    full_auto_priority_mode: Optional[int] = None
    other_flags: Optional[str] = None
    max_prio_points: Optional[int] = None
    overall_queue_cap: Optional[int] = Field(None, ge=0, le=100)
    nonpriority_queue_cap: Optional[int] = Field(None, ge=0, le=100)
    prio_follow_enabled: Optional[int] = None
    prio_raid_enabled: Optional[int] = None
    prio_bits_per_point: Optional[int] = Field(None, ge=0)
    prio_gifts_per_point: Optional[int] = Field(None, ge=0)
    prio_sub_tier1_points: Optional[int] = Field(None, ge=0)
    prio_sub_tier2_points: Optional[int] = Field(None, ge=0)
    prio_sub_tier3_points: Optional[int] = Field(None, ge=0)
    prio_reset_points_tier1: Optional[int] = Field(None, ge=0)
    prio_reset_points_tier2: Optional[int] = Field(None, ge=0)
    prio_reset_points_tier3: Optional[int] = Field(None, ge=0)
    prio_reset_points_vip: Optional[int] = Field(None, ge=0)
    prio_reset_points_mod: Optional[int] = Field(None, ge=0)
    free_mod_priority_requests: Optional[int] = None


class ChannelSettingsOut(ChannelSettingsBase):
    channel_id: int

class AuthUrlOut(BaseModel):
    auth_url: str

class AuthCallbackOut(BaseModel):
    success: bool

class SessionOut(BaseModel):
    login: str

class MeOut(BaseModel):
    login: str
    display_name: Optional[str] = None
    profile_image_url: Optional[str] = None


class LogoutOut(BaseModel):
    success: bool

class ChannelAccessOut(BaseModel):
    channel_name: str
    role: str

class ModIn(BaseModel):
    twitch_id: str
    username: str


class ChannelBotStatusIn(BaseModel):
    active: bool
    error: Optional[str] = None


class BotConfigOut(BaseModel):
    login: Optional[str]
    display_name: Optional[str]
    scopes: List[str] = Field(default_factory=list)
    enabled: bool
    expires_at: Optional[datetime]
    token_present: bool = False
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    bot_user_id: Optional[str] = None


class BotConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    scopes: Optional[List[str]] = None
    display_name: Optional[str] = None
    login: Optional[str] = None


class BotTokenUpdateIn(BaseModel):
    access_token: str
    refresh_token: str
    expires_at: Optional[datetime] = None
    scopes: List[str] = Field(default_factory=list)


class BotLogEventIn(BaseModel):
    message: str
    level: str = Field(default="info")
    source: Optional[str] = None
    timestamp: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class BotLogAckOut(BaseModel):
    success: bool


class BotOAuthStartIn(BaseModel):
    return_url: Optional[str] = None


class SystemConfigOut(BaseModel):
    setup_complete: bool
    twitch_client_id: Optional[str]
    twitch_client_secret_set: bool
    twitch_redirect_uri: Optional[str]
    bot_redirect_uri: Optional[str]
    twitch_scopes: List[str]
    bot_app_scopes: List[str]


class SystemConfigUpdate(BaseModel):
    twitch_client_id: Optional[str] = None
    twitch_client_secret: Optional[str] = None
    twitch_redirect_uri: Optional[str] = None
    bot_redirect_uri: Optional[str] = None
    twitch_scopes: Optional[List[str]] = None
    bot_app_scopes: Optional[List[str]] = None
    setup_complete: Optional[bool] = None


class SystemStatusOut(BaseModel):
    setup_complete: bool


class SystemMetaOut(BaseModel):
    version: str
    dev_mode: bool


class ManualPlaylistCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=2048)


class PlaylistCreate(BaseModel):
    url: Optional[str] = Field(default=None, min_length=1, max_length=1024)
    manual: Optional[ManualPlaylistCreate] = None
    keywords: List[str] = Field(default_factory=list)
    visibility: Optional[str] = Field(default="public")

    @model_validator(mode="after")
    def _validate_choice(self) -> "PlaylistCreate":
        if bool(self.url) == bool(self.manual):
            raise ValueError("provide either url or manual playlist data")
        return self


class PlaylistUpdate(BaseModel):
    keywords: Optional[List[str]] = None
    visibility: Optional[str] = None
    slug: Optional[str] = Field(default=None, max_length=255)


class PlaylistOut(BaseModel):
    id: int
    title: str
    description: Optional[str]
    playlist_id: Optional[str]
    url: Optional[str]
    source: str
    visibility: str
    keywords: List[str] = Field(default_factory=list)
    item_count: int


class PlaylistItemCreate(BaseModel):
    title: Optional[str] = Field(default=None, min_length=1, max_length=255)
    artist: Optional[str] = Field(default=None, max_length=255)
    video_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    url: Optional[str] = Field(default=None, min_length=1, max_length=1024)
    duration_seconds: Optional[int] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _validate_payload(self) -> "PlaylistItemCreate":
        if not (self.title or self.video_id or self.url):
            raise ValueError("playlist items require a title, video_id, or url")
        return self


class PlaylistItemOut(BaseModel):
    id: int
    title: str
    artist: Optional[str]
    video_id: Optional[str]
    url: Optional[str]
    position: int
    duration_seconds: Optional[int] = None


class PublicPlaylistItemOut(BaseModel):
    id: int
    title: str
    artist: Optional[str]
    video_id: Optional[str]
    url: Optional[str]
    position: int
    duration_seconds: Optional[int] = None


class PublicPlaylistOut(BaseModel):
    id: int
    title: str
    description: Optional[str]
    slug: str
    source: str
    visibility: str
    url: Optional[str]
    keywords: List[str] = Field(default_factory=list)
    item_count: int
    items: List[PublicPlaylistItemOut] = Field(default_factory=list)


class PlaylistQueueIn(BaseModel):
    item_id: int
    bumped: bool = False


class PlaylistSongPick(BaseModel):
    id: int
    artist: str
    title: str
    youtube_link: Optional[str] = None


class PlaylistRequestIn(BaseModel):
    identifier: str = Field(..., min_length=1, max_length=255)
    index: int = Field(..., ge=1)


class PlaylistRequestOut(BaseModel):
    request_id: int
    playlist_item_id: int
    song: PlaylistSongPick


class RandomPlaylistRequestIn(BaseModel):
    keyword: Optional[str] = None
    twitch_id: str = Field(..., min_length=1)
    username: str = Field(..., min_length=1)
    is_subscriber: bool = False


class RandomPlaylistRequestOut(BaseModel):
    request_id: int
    song: PlaylistSongPick
    playlist_item_id: int
    keyword: str

class SongIn(BaseModel):
    artist: str
    title: str
    youtube_link: Optional[str] = None
    mixed_tags: Optional[str] = None
    is_banned: int = 0
    is_inactive: int = 0

class SongOut(BaseModel):
    id: int
    artist: str
    title: str
    youtube_link: Optional[str]
    date_first_played: Optional[datetime]
    date_last_played: Optional[datetime]
    total_played: int
    mixed_tags: Optional[str]
    is_banned: int
    is_inactive: int

    class Config:
        from_attributes = True


class YTMusicThumbnail(BaseModel):
    url: str
    width: Optional[int] = None
    height: Optional[int] = None


class YTMusicSearchResult(BaseModel):
    title: str
    video_id: Optional[str] = None
    playlist_id: Optional[str] = None
    browse_id: Optional[str] = None
    result_type: Optional[str] = None
    artists: List[str] = Field(default_factory=list)
    album: Optional[str] = None
    duration: Optional[str] = None
    thumbnails: List[YTMusicThumbnail] = Field(default_factory=list)
    link: Optional[str] = None

class UserIn(BaseModel):
    twitch_id: str
    username: str

class UserOut(BaseModel):
    id: int
    twitch_id: str
    username: str
    amount_requested: int
    prio_points: int

    class Config:
        from_attributes = True


class UserWithRoles(UserOut):
    is_vip: bool = False
    is_subscriber: bool = False
    subscriber_tier: Optional[str] = None


class RequestCreate(BaseModel):
    song_id: int
    user_id: int
    want_priority: bool = False
    prefer_sub_free: bool = True
    is_subscriber: bool = False  # caller must tell us subscriber status
    is_mod: bool = False

class RequestUpdate(BaseModel):
    played: Optional[int] = None
    bumped: Optional[int] = None
    is_priority: Optional[int] = None  # admin-only toggle

class RequestOut(BaseModel):
    id: int
    song_id: int
    user_id: int
    request_time: datetime
    is_priority: int
    bumped: int
    played: int
    priority_source: Optional[str]

    class Config:
        from_attributes = True


class QueueItemFull(BaseModel):
    request: RequestOut
    song: SongOut
    user: UserWithRoles


class MoveRequestIn(BaseModel):
    direction: Literal["up", "down"]

class EventIn(BaseModel):
    type: str
    user_id: Optional[int] = None
    meta: Optional[dict[str, Any]] = None

class EventOut(BaseModel):
    id: int
    type: str = Field(validation_alias="event_type", serialization_alias="type")
    user_id: Optional[int]
    meta: Optional[str]
    event_time: datetime

    class Config:
        from_attributes = True

class StreamOut(BaseModel):
    id: int
    started_at: datetime
    ended_at: Optional[datetime]

    class Config:
        from_attributes = True

# =====================================
# FastAPI app and deps
# =====================================
app = FastAPI(title="Twitch Song Request Backend", version=API_VERSION)

DEFAULT_CORS_ALLOW_ORIGIN_REGEX = r"https?://.*"


def _parse_cors_origins(raw: str) -> list[str]:
    """Return a list of origins from an environment variable value.

    The original implementation only supported comma separated values, but the
    deployment environment may provide a space separated list (e.g. when the
    value comes from Terraform or certain container schedulers). In that case
    the string would be treated as a single origin and the request's `Origin`
    header would not match, resulting in missing CORS headers. By splitting on
    both commas and whitespace we gracefully handle either format.
    """

    if not raw:
        return []

    origins: list[str] = []
    for part in re.split(r"[\s,]+", raw):
        origin = part.strip()
        if not origin:
            continue

        # Deployment manifests sometimes include a trailing slash when
        # specifying origins (e.g. ``https://example.com/``). Browsers omit the
        # trailing slash in the ``Origin`` header, so we normalise the value to
        # avoid mismatches that would prevent CORS headers from being returned
        # (and therefore block credentialed requests such as the admin login
        # flow).
        origin = origin.rstrip("/")
        if not origin:
            continue

        origins.append(origin)

    return origins


def _separate_cors_origins(origins: list[str]) -> tuple[list[str], list[str]]:
    """Split origins into explicit values and wildcard fragments."""

    explicit: list[str] = []
    wildcard_fragments: list[str] = []

    for origin in origins:
        if "*" not in origin:
            explicit.append(origin)
            continue

        escaped = re.escape(origin)
        # Replace the escaped wildcard with a pattern that matches at least one
        # character but stops at the path separator so that
        # ``https://*.example.com`` matches ``https://foo.example.com`` but
        # not ``https://example.com``.
        fragment = escaped.replace(r"\*", r"[^/]+")
        wildcard_fragments.append(fragment)

    return explicit, wildcard_fragments


def _cors_settings_from_env(env: Mapping[str, str]) -> tuple[list[str], Optional[str]]:
    origins = _parse_cors_origins(env.get("CORS_ALLOW_ORIGINS", ""))
    allow_origins, wildcard_fragments = _separate_cors_origins(origins)

    regex_fragments: list[str] = list(wildcard_fragments)

    configured_regex = env.get("CORS_ALLOW_ORIGIN_REGEX", "")
    if configured_regex:
        regex_fragments.append(configured_regex)
    elif not allow_origins and not regex_fragments:
        regex_fragments.append(DEFAULT_CORS_ALLOW_ORIGIN_REGEX)

    allow_origin_regex = None
    if regex_fragments:
        allow_origin_regex = f"^(?:{'|'.join(regex_fragments)})$"

    return allow_origins, allow_origin_regex


allow_origins, allow_origin_regex = _cors_settings_from_env(os.environ)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_origin_regex=allow_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SETUP_BYPASS_PREFIXES = ("/system", "/docs", "/openapi", "/redoc", "/twitch/eventsub")


@app.middleware("http")
async def enforce_initial_setup(request: FastAPIRequest, call_next):
    if is_setup_complete():
        return await call_next(request)

    path = request.url.path
    for prefix in SETUP_BYPASS_PREFIXES:
        if path.startswith(prefix):
            return await call_next(request)

    token = request.headers.get("X-Admin-Token")
    if token and secrets.compare_digest(token, ADMIN_TOKEN):
        return await call_next(request)

    return JSONResponse({"detail": "setup incomplete"}, status_code=503)

class _ChannelBroker:
    __slots__ = ("channel_pk", "listeners")

    def __init__(self, channel_pk: int) -> None:
        self.channel_pk = channel_pk
        self.listeners: set[asyncio.Queue[str]] = set()

    def subscribe(self) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)
        self.listeners.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self.listeners.discard(queue)

    def _broadcast(self, message: str) -> None:
        if not self.listeners:
            return
        stale: list[asyncio.Queue[str]] = []
        for queue in list(self.listeners):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                stale.append(queue)
                logger.warning(
                    "queue change notification dropped for channel %s",
                    self.channel_pk,
                )
            except Exception:
                stale.append(queue)
                logger.exception(
                    "failed to enqueue queue change notification for channel %s",
                    self.channel_pk,
                )
        for queue in stale:
            self.listeners.discard(queue)

    def put_nowait(self, message: str) -> None:
        self._broadcast(message)

    def has_listeners(self) -> bool:
        return bool(self.listeners)


_brokers: dict[int, _ChannelBroker] = {}


def _broker(channel_pk: int) -> _ChannelBroker:
    broker = _brokers.get(channel_pk)
    if broker is None:
        broker = _ChannelBroker(channel_pk)
        _brokers[channel_pk] = broker
    return broker


def _subscribe_queue(channel_pk: int) -> asyncio.Queue[str]:
    return _broker(channel_pk).subscribe()


def _unsubscribe_queue(channel_pk: int, queue: asyncio.Queue[str]) -> None:
    broker = _brokers.get(channel_pk)
    if not broker:
        return
    broker.unsubscribe(queue)
    if not broker.has_listeners():
        _brokers.pop(channel_pk, None)


def publish_queue_changed(channel_pk: int) -> None:
    """Notify listeners that the active queue for a channel changed."""
    broker = _brokers.get(channel_pk)
    if not broker:
        return
    broker.put_nowait("changed")
    if not broker.has_listeners():
        _brokers.pop(channel_pk, None)


class _ChannelEventBroker:
    __slots__ = ("channel_pk", "listeners")

    def __init__(self, channel_pk: int) -> None:
        self.channel_pk = channel_pk
        self.listeners: set[asyncio.Queue[str]] = set()

    def subscribe(self) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)
        self.listeners.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self.listeners.discard(queue)

    def put_nowait(self, message: str) -> None:
        if not self.listeners:
            return
        stale: list[asyncio.Queue[str]] = []
        for queue in list(self.listeners):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                stale.append(queue)
                logger.warning(
                    "channel event notification dropped for channel %s",
                    self.channel_pk,
                )
            except Exception:
                stale.append(queue)
                logger.exception(
                    "failed to enqueue channel event for channel %s",
                    self.channel_pk,
                )
        for queue in stale:
            self.listeners.discard(queue)

    def has_listeners(self) -> bool:
        return bool(self.listeners)


_event_brokers: dict[int, _ChannelEventBroker] = {}


def _event_broker(channel_pk: int) -> _ChannelEventBroker:
    broker = _event_brokers.get(channel_pk)
    if broker is None:
        broker = _ChannelEventBroker(channel_pk)
        _event_brokers[channel_pk] = broker
    return broker


def _subscribe_channel_events(channel_pk: int) -> asyncio.Queue[str]:
    return _event_broker(channel_pk).subscribe()


def _unsubscribe_channel_events(channel_pk: int, queue: asyncio.Queue[str]) -> None:
    broker = _event_brokers.get(channel_pk)
    if not broker:
        return
    broker.unsubscribe(queue)
    if not broker.has_listeners():
        _event_brokers.pop(channel_pk, None)


def publish_channel_event(
    channel_pk: int, event_type: str, payload: Optional[Mapping[str, Any]]
) -> None:
    broker = _event_brokers.get(channel_pk)
    if not broker:
        return
    event_payload = {
        "type": event_type,
        "payload": payload,
        "timestamp": datetime.utcnow(),
    }
    try:
        message = json.dumps(event_payload, default=_json_default)
    except TypeError:
        logger.exception("failed to serialize channel event for channel %s", channel_pk)
        return
    broker.put_nowait(message)
    if not broker.has_listeners():
        _event_brokers.pop(channel_pk, None)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Type {type(value)!r} not serializable")


def _broadcast_bot_log(event: Dict[str, Any]) -> None:
    payload = json.dumps(event, default=_json_default)
    stale: list[asyncio.Queue[str]] = []
    for queue in list(_bot_log_listeners):
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            stale.append(queue)
    for queue in stale:
        _bot_log_listeners.discard(queue)


def _serialize_user_summary(user: User) -> Dict[str, Any]:
    return {
        "id": user.id,
        "username": user.username,
    }


def _serialize_request_event(db: Session, request: Request) -> Dict[str, Any]:
    song = db.get(Song, request.song_id)
    user = db.get(User, request.user_id)
    song_payload = {
        "title": None,
        "artist": None,
        "youtube_link": None,
    }
    if song:
        song_payload.update(
            {
                "title": song.title,
                "artist": song.artist,
                "youtube_link": song.youtube_link,
            }
        )
    requester_payload: Dict[str, Any] = {
        "id": None,
        "username": None,
    }
    if user:
        requester_payload.update(_serialize_user_summary(user))
    return {
        "id": request.id,
        "song": song_payload,
        "requester": requester_payload,
        "is_priority": bool(request.is_priority),
        "bumped": bool(request.bumped),
        "priority_source": request.priority_source,
    }


def _next_pending_request(
    db: Session, channel_pk: int, stream_id: Optional[int]
) -> Optional[Dict[str, Any]]:
    if stream_id is None:
        return None
    next_req = (
        db.query(Request)
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == stream_id,
            Request.played == 0,
        )
        .order_by(
            Request.is_priority.desc(),
            Request.position.asc(),
            Request.request_time.asc(),
            Request.id.asc(),
        )
        .first()
    )
    if not next_req:
        return None
    return _serialize_request_event(db, next_req)


def _next_nonpriority_request(
    db: Session, channel_pk: int, stream_id: Optional[int]
) -> Optional[Dict[str, Any]]:
    """Return the next unplayed non-priority request ordered by bumps and position.

    Dependencies: Relies on an active SQLAlchemy session and current stream identifier
    supplied by `current_stream`. Code customers: public queue readers needing a
    predictable "next up" view without exposing priority-only selections. Used
    variables/origin: Filters `Request` rows scoped to `channel_pk` and `stream_id`
    where `played==0` and `is_priority==0`, then joins `Song` and `User` for
    serialization.
    """

    if stream_id is None:
        return None
    row = (
        db.query(Request, Song, User)
        .join(Song, Song.id == Request.song_id)
        .join(User, User.id == Request.user_id)
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == stream_id,
            Request.played == 0,
            Request.is_priority == 0,
        )
        .order_by(
            Request.bumped.desc(),
            Request.position.asc(),
            Request.request_time.asc(),
            Request.id.asc(),
        )
        .first()
    )
    if not row:
        return None
    req, song, user = row
    return {
        "request": RequestOut.model_validate(req).model_dump(),
        "song": SongOut.model_validate(song).model_dump(),
        "user": UserOut.model_validate(user).model_dump(),
    }


def _next_priority_request(
    db: Session, channel_pk: int, stream_id: Optional[int]
) -> Optional[Dict[str, Any]]:
    """Return the next unplayed priority request ordered by bumps and position.

    Dependencies: Requires an active SQLAlchemy session and stream identifier
    from `current_stream`. Code customers: public queue readers that surface the
    next VIP pick without authentication. Used variables/origin: Filters
    `Request` rows by `channel_pk` and `stream_id` where `played==0` and
    `is_priority==1`, joining `Song` and `User` to serialize the response.
    """

    if stream_id is None:
        return None
    row = (
        db.query(Request, Song, User)
        .join(Song, Song.id == Request.song_id)
        .join(User, User.id == Request.user_id)
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == stream_id,
            Request.played == 0,
            Request.is_priority == 1,
        )
        .order_by(
            Request.bumped.desc(),
            Request.position.asc(),
            Request.request_time.asc(),
            Request.id.asc(),
        )
        .first()
    )
    if not row:
        return None
    req, song, user = row
    return {
        "request": RequestOut.model_validate(req).model_dump(),
        "song": SongOut.model_validate(song).model_dump(),
        "user": UserOut.model_validate(user).model_dump(),
    }


def _serialize_settings_event(settings: ChannelSettings) -> Dict[str, Any]:
    return {
        "max_requests_per_user": settings.max_requests_per_user,
        "prio_only": settings.prio_only,
        "queue_closed": settings.queue_closed,
        "allow_bumps": settings.allow_bumps,
        "full_auto_priority_mode": settings.full_auto_priority_mode,
        "other_flags": settings.other_flags,
        "max_prio_points": settings.max_prio_points,
        "overall_queue_cap": settings.overall_queue_cap,
        "nonpriority_queue_cap": settings.nonpriority_queue_cap,
        "prio_follow_enabled": settings.prio_follow_enabled,
        "prio_raid_enabled": settings.prio_raid_enabled,
        "prio_bits_per_point": settings.prio_bits_per_point,
        "prio_gifts_per_point": settings.prio_gifts_per_point,
        "prio_sub_tier1_points": settings.prio_sub_tier1_points,
        "prio_sub_tier2_points": settings.prio_sub_tier2_points,
        "prio_sub_tier3_points": settings.prio_sub_tier3_points,
        "prio_reset_points_tier1": settings.prio_reset_points_tier1,
        "prio_reset_points_tier2": settings.prio_reset_points_tier2,
        "prio_reset_points_tier3": settings.prio_reset_points_tier3,
        "prio_reset_points_vip": settings.prio_reset_points_vip,
        "prio_reset_points_mod": settings.prio_reset_points_mod,
        "free_mod_priority_requests": settings.free_mod_priority_requests,
    }


def _apply_settings_patch(settings: ChannelSettings, payload: ChannelSettingsUpdate) -> Dict[str, Any]:
    """Merge a partial ``ChannelSettingsUpdate`` payload into persisted settings.

    Dependencies: Expects a mutable ``ChannelSettings`` ORM entity and a
    validated ``ChannelSettingsUpdate`` model supplied by FastAPI. Code
    customers: ``set_channel_settings`` leverages this helper to avoid wiping
    unspecified fields when the admin UI only transmits edited controls. Used
    variables/origin: iterates through ``payload`` values produced by
    ``model_dump(exclude_unset=True)`` and writes them onto the provided
    ``settings`` instance, ignoring ``None`` for numeric fields while honoring
    explicit nulls for ``other_flags``.
    """

    updates = payload.model_dump(exclude_unset=True)
    for field, value in updates.items():
        if value is None and field != "other_flags":
            continue
        setattr(settings, field, value)
    return updates


def emit_queue_status_event(
    channel_pk: int, closed: bool, status: str, reason: Optional[str] = None
) -> None:
    """Broadcast the queue availability state to active event listeners.

    Dependencies: Relies on the in-memory channel event broker created by
    ``publish_channel_event`` and therefore only emits when listeners have
    subscribed to the `/events` WebSocket.
    Code customers: OBS overlays and moderator dashboards that react to
    `queue.status` notifications when intake pauses or resumes.
    Used variables/origin: Accepts `channel_pk` from the calling route,
    a boolean ``closed`` flag, a ``status`` label such as ``"closed"`` or
    ``"limited"``, and an optional human-readable ``reason`` string.
    """

    payload: Dict[str, Any] = {"closed": bool(closed), "status": status}
    if reason:
        payload["reason"] = reason
    publish_channel_event(channel_pk, "queue.status", payload)


def _normalize_return_url(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        parsed = urlparse(value)
    except ValueError:
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    if not parsed.netloc:
        return None
    sanitized = parsed._replace(fragment="")
    return urlunparse(sanitized)


_FORWARDED_PAIR_RE = re.compile(r"(?P<key>[a-zA-Z-]+)=(?P<value>\"[^\"]*\"|[^;]+)")
_FORWARDED_HOST_RE = re.compile(r"^[A-Za-z0-9_.-]+(:\d+)?$")


def _parse_forwarded_header(raw_value: str) -> dict[str, str]:
    result: dict[str, str] = {}
    if not raw_value:
        return result
    first_value = raw_value.split(",", 1)[0]
    for match in _FORWARDED_PAIR_RE.finditer(first_value):
        key = match.group("key").strip().lower()
        value = match.group("value").strip()
        if value.startswith("\"") and value.endswith("\""):
            value = value[1:-1]
        result[key] = value
    return result


def _apply_forwarded_headers(request: FastAPIRequest, url: URL) -> URL:
    forwarded = _parse_forwarded_header(request.headers.get("forwarded", ""))
    proto = forwarded.get("proto")
    host = forwarded.get("host")
    port = forwarded.get("port")

    xf_proto = request.headers.get("x-forwarded-proto")
    xf_host = request.headers.get("x-forwarded-host")
    xf_port = request.headers.get("x-forwarded-port")

    if xf_proto:
        proto = proto or xf_proto.split(",", 1)[0].strip()
    if xf_host:
        host = host or xf_host.split(",", 1)[0].strip()
    if xf_port:
        port = port or xf_port.split(",", 1)[0].strip()

    if proto:
        proto = proto.lower()
        if proto in ("http", "https"):
            url = url.replace(scheme=proto)

    hostname: Optional[str] = None
    if host and _FORWARDED_HOST_RE.match(host):
        hostname = host
    if hostname and ":" in hostname and not port:
        hostname, _, port_candidate = hostname.partition(":")
        if port_candidate:
            port = port_candidate
    if hostname:
        url = url.replace(hostname=hostname)

    if port:
        try:
            port_int = int(port)
        except ValueError:
            port_int = None
        if port_int:
            url = url.replace(port=port_int)
        else:
            url = url.replace(port=None)

    prefix = request.headers.get("x-forwarded-prefix") or ""
    if prefix:
        prefix = prefix.split(",", 1)[0].strip()
        if prefix:
            if not prefix.startswith("/"):
                prefix = "/" + prefix
            prefix = prefix.rstrip("/")
            if prefix and not url.path.startswith(prefix):
                url = url.replace(path=prefix + url.path)

    return url


def _bot_redirect_uri(request: FastAPIRequest) -> str:
    configured = get_bot_redirect_uri()
    if configured:
        return configured
    url = URL(str(request.url_for("bot_oauth_callback")))
    adjusted = _apply_forwarded_headers(request, url)
    return str(adjusted)


def _cleanup_bot_oauth_states() -> None:
    cutoff = time.time() - 600
    stale = [key for key, meta in _bot_oauth_states.items() if meta.get("created_at", 0) < cutoff]
    for key in stale:
        _bot_oauth_states.pop(key, None)


def _bot_oauth_html_response(success: bool, message: str, *, redirect_url: Optional[str] = None, status_code: int = 200) -> HTMLResponse:
    payload = {"type": "bot-oauth-complete", "success": success}
    if not success:
        payload["error"] = message
    script_payload = json.dumps(payload)
    message_text = html.escape(message or "")
    redirect_script = ""
    if redirect_url:
        redirect_script = f"\n          setTimeout(function() {{ window.location.replace('{html.escape(redirect_url)}'); }}, 1200);"
    body = f"""<!DOCTYPE html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <title>Bot Authorization</title>
    <style>
      body {{ font-family: sans-serif; margin: 2rem; }}
    </style>
  </head>
  <body>
    <h1>{'Success' if success else 'Authorization Failed'}</h1>
    <p>{message_text or ('Authorization completed successfully. You can close this window.' if success else 'Unable to complete bot authorization.')}</p>
    <script>
      (function() {{
        var payload = {script_payload};
        try {{
          if (window.opener) {{
            window.opener.postMessage(payload, '*');
          }} else if (window.parent && window.parent !== window) {{
            window.parent.postMessage(payload, '*');
          }}
        }} catch (err) {{ /* ignore */ }}
        setTimeout(function() {{
          try {{ window.close(); }} catch (err) {{ /* ignore */ }}
        }}, 1500);{redirect_script}
      }})();
    </script>
  </body>
</html>
"""
    return HTMLResponse(content=body, status_code=status_code)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _normalize_scope_list(scopes: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for scope in scopes:
        scope_value = scope.strip()
        if scope_value and scope_value not in seen:
            seen.add(scope_value)
            result.append(scope_value)
    return result


def _ensure_bot_config_scopes(cfg: "BotConfig") -> bool:
    current = _normalize_scope_list((cfg.scopes or "").split())
    required = _normalize_scope_list(get_bot_app_scopes())
    missing = [scope for scope in required if scope not in current]
    if missing:
        current.extend(missing)
        cfg.scopes = " ".join(current) if current else None
        return True
    return False


def _get_bot_config(db: Session) -> BotConfig:
    cfg = db.query(BotConfig).order_by(BotConfig.id.asc()).first()
    if not cfg:
        default_scopes = _normalize_scope_list(get_bot_app_scopes())
        scopes = " ".join(default_scopes) if default_scopes else ""
        cfg = BotConfig(scopes=scopes or None, enabled=False)
        db.add(cfg)
        db.commit()
        db.refresh(cfg)
    else:
        if _ensure_bot_config_scopes(cfg):
            db.commit()
            db.refresh(cfg)
    return cfg


def _serialize_bot_config(cfg: BotConfig, *, include_tokens: bool = False) -> Dict[str, Any]:
    scopes = (cfg.scopes or "").split()
    data: Dict[str, Any] = {
        "login": cfg.login,
        "display_name": cfg.display_name,
        "scopes": scopes,
        "enabled": bool(cfg.enabled),
        "expires_at": cfg.expires_at,
        "token_present": bool(cfg.access_token and cfg.refresh_token),
    }
    if include_tokens:
        data["access_token"] = cfg.access_token
        data["refresh_token"] = cfg.refresh_token
        client_id = get_twitch_client_id()
        client_secret = get_twitch_client_secret()
        if client_id:
            data["client_id"] = client_id
        if client_secret:
            data["client_secret"] = client_secret
        try:
            data["bot_user_id"] = get_bot_user_id()
        except (requests.RequestException, RuntimeError):
            data["bot_user_id"] = None
    return data

def _resolve_user_from_token(
    token: str,
    db: Session,
    *,
    force_validate: bool = False,
) -> tuple[TwitchUser, Optional[dict[str, Any]]]:
    """Return the Twitch user associated with the OAuth token."""
    user = db.query(TwitchUser).filter_by(access_token=token).one_or_none()
    data: Optional[dict[str, Any]] = None
    must_validate = force_validate or user is None
    if must_validate:
        try:
            resp = requests.get(
                "https://id.twitch.tv/oauth2/validate",
                headers={"Authorization": f"OAuth {token}"},
            )
        except requests.RequestException as exc:
            # Surfacing the failure as an HTTPException keeps the request inside
            # FastAPI's normal response handling flow so middleware such as CORS
            # can still attach the proper headers. Without this the browser sees
            # the low-level network exception as a CORS failure.
            raise HTTPException(status_code=502, detail="twitch validation failed") from exc
        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail="invalid token")
        try:
            data = resp.json()
        except ValueError as exc:
            raise HTTPException(status_code=502, detail="invalid response from twitch") from exc
        login = data.get("login")
        twitch_id = data.get("user_id")
        if not login or not twitch_id:
            raise HTTPException(status_code=401, detail="invalid token")
        scopes = " ".join(data.get("scopes", []))
        existing = (
            db.query(TwitchUser)
            .filter(func.lower(TwitchUser.username) == login.lower())
            .one_or_none()
        )
        if existing and existing is not user:
            user = existing
        if not user:
            user = TwitchUser(
                twitch_id=twitch_id,
                username=login,
                access_token=token,
                refresh_token="",
                scopes=scopes,
            )
            db.add(user)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                user = (
                    db.query(TwitchUser)
                    .filter(TwitchUser.twitch_id == twitch_id)
                    .one()
                )
            else:
                db.refresh(user)
        else:
            updated = False
            if user.twitch_id != twitch_id and twitch_id:
                user.twitch_id = twitch_id
                updated = True
            if user.username != login:
                user.username = login
                updated = True
            if user.access_token != token:
                user.access_token = token
                updated = True
            if user.scopes != scopes:
                user.scopes = scopes
                updated = True
            if updated:
                db.commit()
                db.refresh(user)
    return user, data


def _auto_register_channel_from_token(user: TwitchUser, data: dict[str, Any], db: Session) -> None:
    scopes = set(data.get("scopes") or [])
    login = data.get("login")
    user_id = data.get("user_id")
    if "channel:bot" not in scopes or not login or not user_id:
        return
    channel_id = str(user_id)
    channel = (
        db.query(ActiveChannel)
        .filter(ActiveChannel.channel_id == channel_id)
        .one_or_none()
    )
    if not channel:
        channel = (
            db.query(ActiveChannel)
            .filter(func.lower(ActiveChannel.channel_name) == login.lower())
            .one_or_none()
        )
    if channel:
        changed = False
        if channel.channel_id != channel_id:
            channel.channel_id = channel_id
            changed = True
        if (channel.channel_name or "").lower() != login.lower():
            channel.channel_name = login
            changed = True
        if channel.owner_id != user.id:
            channel.owner_id = user.id
            changed = True
        if not channel.channel_key:
            channel.channel_key = generate_channel_key()
            changed = True
        if not channel.authorized:
            channel.authorized = True
            changed = True
        if changed:
            db.commit()
        get_or_create_settings(db, channel.id)
    else:
        channel = ActiveChannel(
            channel_id=channel_id,
            channel_name=login,
            channel_key=generate_channel_key(),
            join_active=1,
            authorized=True,
            owner_id=user.id,
        )
        db.add(channel)
        db.commit()
        db.refresh(channel)
        get_or_create_settings(db, channel.id)


def get_current_user(
    authorization: str = Header(None),
    admin_session: Optional[str] = Cookie(None, alias=ADMIN_SESSION_COOKIE),
    db: Session = Depends(get_db),
) -> TwitchUser:
    token: Optional[str] = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1]
    elif admin_session:
        token = admin_session
    if not token:
        raise HTTPException(status_code=401, detail="missing token")
    user, _ = _resolve_user_from_token(token, db)
    return user

def _user_has_access(user: TwitchUser, channel_pk: int, db: Session) -> bool:
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        return False
    if ch.owner_id == user.id:
        return True
    mod = (
        db.query(ChannelModerator)
        .filter_by(channel_id=channel_pk, user_id=user.id)
        .one_or_none()
    )
    return mod is not None

def require_token(
    channel: Optional[str] = None,
    x_admin_token: str = Header(None),
    authorization: str = Header(None),
    admin_session: Optional[str] = Cookie(None, alias=ADMIN_SESSION_COOKIE),
    db: Session = Depends(get_db),
):
    if x_admin_token == ADMIN_TOKEN:
        return
    token: Optional[str] = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1]
    elif admin_session:
        token = admin_session
    if token:
        user, _ = _resolve_user_from_token(token, db)
        if not channel:
            return
        channel_pk = get_channel_pk(channel, db)
        if _user_has_access(user, channel_pk, db):
            return
    raise HTTPException(status_code=401, detail="invalid admin token")


def require_channel_key(
    channel: str,
    x_channel_key: Optional[str] = Header(None, alias="X-Channel-Key"),
    channel_key_query: Optional[str] = Query(None, alias="channel_key"),
    x_admin_token: str = Header(None),
    authorization: str = Header(None),
    admin_session: Optional[str] = Cookie(None, alias=ADMIN_SESSION_COOKIE),
    db: Session = Depends(get_db),
):
    """Validate channel-level access using the shared key or existing admin/OAuth credentials.

    Dependencies: Reads `ActiveChannel` records via `get_channel_pk`/`Session` and reuses `require_token` for admin flows.
    Code customers: Queue, playlist, and other channel-safe endpoints inject this dependency for authentication.
    Used variables/origin: Accepts the `X-Channel-Key` header or `channel_key` query param and compares against `channel.channel_key`.
    """

    channel_pk = get_channel_pk(channel, db)
    provided_key = x_channel_key or channel_key_query
    if provided_key:
        channel_obj = db.get(ActiveChannel, channel_pk)
        stored_key = channel_obj.channel_key if channel_obj else None
        if stored_key and hmac.compare_digest(stored_key, provided_key):
            return

    try:
        require_token(
            channel=channel,
            x_admin_token=x_admin_token,
            authorization=authorization,
            admin_session=admin_session,
            db=db,
        )
        return
    except HTTPException:
        pass

    raise HTTPException(status_code=401, detail="invalid channel key")

def get_channel_pk(channel: str, db: Session) -> int:
    """Return the primary key for a channel, matching name case-insensitively."""
    ch = (
        db.query(ActiveChannel)
        .filter(func.lower(ActiveChannel.channel_name) == channel.lower())
        .one_or_none()
    )
    if not ch:
        raise HTTPException(status_code=404, detail="channel not found")
    return ch.id

@app.get("/auth/login", response_model=AuthUrlOut)
def auth_login(
    channel: str,
    request: FastAPIRequest,
    return_url: Optional[str] = Query(None),
):
    client_id = get_twitch_client_id()
    client_secret = get_twitch_client_secret()
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Twitch OAuth not configured")
    scope = "+".join(get_twitch_scopes())
    redirect_uri = get_twitch_redirect_uri() or str(request.url_for("auth_callback"))
    state_payload = {"channel": channel}
    if return_url:
        state_payload["return_url"] = return_url
    state_param = quote(json.dumps(state_payload, separators=(",", ":")), safe="")
    url = (
        "https://id.twitch.tv/oauth2/authorize"
        f"?response_type=code&client_id={client_id}"
        f"&redirect_uri={redirect_uri}&scope={scope}&state={state_param}"
    )
    return {"auth_url": url}

@app.get("/auth/callback", response_model=AuthCallbackOut)
def auth_callback(
    code: str,
    state: str,
    request: FastAPIRequest,
    db: Session = Depends(get_db),
):
    client_id = get_twitch_client_id()
    client_secret = get_twitch_client_secret()
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Twitch OAuth not configured")
    redirect_uri = get_twitch_redirect_uri() or str(request.url_for("auth_callback"))
    token_resp = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        },
    ).json()
    access_token = token_resp["access_token"]
    refresh_token = token_resp.get("refresh_token")
    scopes_list = token_resp.get("scope", [])
    if "channel:bot" not in scopes_list:
        raise HTTPException(status_code=400, detail="channel:bot scope required")
    scopes = " ".join(scopes_list)
    channel_name = state
    return_to: Optional[str] = None
    try:
        state_data = json.loads(state)
    except json.JSONDecodeError:
        pass
    else:
        if isinstance(state_data, dict):
            channel_name = state_data.get("channel", channel_name)
            return_to = state_data.get("return_url")
        elif isinstance(state_data, str):
            channel_name = state_data
    headers = {"Authorization": f"Bearer {access_token}", "Client-Id": client_id}
    user_info = requests.get("https://api.twitch.tv/helix/users", headers=headers).json()["data"][0]
    user = db.query(TwitchUser).filter_by(twitch_id=user_info["id"]).one_or_none()
    if not user:
        user = TwitchUser(
            twitch_id=user_info["id"],
            username=user_info["login"],
            access_token=access_token,
            refresh_token=refresh_token,
            scopes=scopes,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    else:
        user.username = user_info["login"]
        user.access_token = access_token
        user.refresh_token = refresh_token
        user.scopes = scopes
        db.commit()
    ch = (
        db.query(ActiveChannel)
        .filter(func.lower(ActiveChannel.channel_name) == channel_name.lower())
        .one_or_none()
    )
    if not ch:
        ch = ActiveChannel(
            channel_id=user_info["id"],
            channel_name=user_info["login"],
            channel_key=generate_channel_key(),
            join_active=1,
            authorized=True,
            owner_id=user.id,
        )
        db.add(ch)
    else:
        ch.owner_id = user.id
        ch.authorized = True
        if not ch.channel_key:
            ch.channel_key = generate_channel_key()
    db.commit()
    try:
        ensure_eventsub_subscriptions(request, ch.id, db)
    except Exception:
        logger.warning("EventSub bootstrap failed for %s", ch.channel_name, exc_info=True)
    if return_to:
        parsed = urlparse(return_to)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return RedirectResponse(return_to)
    return {"success": True}


@app.post("/auth/session", response_model=SessionOut)
def auth_session(
    response: Response,
    authorization: str = Header(None),
    db: Session = Depends(get_db),
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user, data = _resolve_user_from_token(token, db, force_validate=True)
    if data:
        _auto_register_channel_from_token(user, data, db)
    max_age: Optional[int] = None
    if data and isinstance(data.get("expires_in"), int):
        max_age = data["expires_in"]
    cookie_kwargs = {
        "httponly": True,
        "samesite": "lax",
        "path": "/",
    }
    if max_age:
        cookie_kwargs["max_age"] = max_age
    response.set_cookie(ADMIN_SESSION_COOKIE, token, **cookie_kwargs)
    return {"login": user.username}


@app.post("/auth/logout", response_model=LogoutOut)
def auth_logout(response: Response):
    response.delete_cookie(ADMIN_SESSION_COOKIE, path="/")
    return {"success": True}


@app.delete("/auth/session", response_model=LogoutOut)
def auth_session_delete(
    response: Response,
    current: TwitchUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user = db.get(TwitchUser, current.id)
    if user is None:
        response.delete_cookie(ADMIN_SESSION_COOKIE, path="/")
        return {"success": True}

    owned_channels = list(user.owned_channels)
    for channel in owned_channels:
        db.delete(channel)

    db.query(ChannelModerator).filter_by(user_id=user.id).delete(synchronize_session=False)
    db.delete(user)
    db.commit()
    response.delete_cookie(ADMIN_SESSION_COOKIE, path="/")
    return {"success": True}


@app.get(
    "/bot/config",
    response_model=BotConfigOut,
    response_model_exclude_none=True,
    dependencies=[Depends(require_token)],
)
def bot_config(
    db: Session = Depends(get_db),
    x_admin_token: Optional[str] = Header(None),
):
    cfg = _get_bot_config(db)
    include_tokens = x_admin_token == ADMIN_TOKEN
    return _serialize_bot_config(cfg, include_tokens=include_tokens)


@app.put(
    "/bot/config",
    response_model=BotConfigOut,
    response_model_exclude_none=True,
    dependencies=[Depends(require_token)],
)
def update_bot_config(
    payload: BotConfigUpdate,
    db: Session = Depends(get_db),
    x_admin_token: Optional[str] = Header(None),
):
    cfg = _get_bot_config(db)
    data = payload.model_dump(exclude_none=True)
    changed = False
    if "enabled" in data and cfg.enabled != data["enabled"]:
        cfg.enabled = bool(data["enabled"])
        changed = True
    if "scopes" in data:
        scopes_in = data["scopes"] or []
        scopes_clean = [scope.strip() for scope in scopes_in if scope.strip()]
        # Preserve order while removing duplicates
        seen: set[str] = set()
        unique_scopes: list[str] = []
        for scope in scopes_clean:
            if scope not in seen:
                seen.add(scope)
                unique_scopes.append(scope)
        scopes_value = " ".join(unique_scopes) if unique_scopes else None
        if cfg.scopes != scopes_value:
            cfg.scopes = scopes_value
            changed = True
    if "display_name" in data:
        display_val = data["display_name"]
        if isinstance(display_val, str):
            display_val = display_val.strip() or None
        if cfg.display_name != display_val:
            cfg.display_name = display_val
            changed = True
    if "login" in data:
        login_val = data["login"]
        if isinstance(login_val, str):
            login_val = login_val.strip() or None
        if cfg.login != login_val:
            cfg.login = login_val
            changed = True
    if changed:
        cfg.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(cfg)
    include_tokens = x_admin_token == ADMIN_TOKEN
    return _serialize_bot_config(cfg, include_tokens=include_tokens)


@app.post(
    "/bot/config/tokens",
    response_model=BotConfigOut,
    response_model_exclude_none=True,
    dependencies=[Depends(require_token)],
)
def update_bot_tokens(
    payload: BotTokenUpdateIn,
    db: Session = Depends(get_db),
    x_admin_token: Optional[str] = Header(None),
):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="invalid admin token")
    cfg = _get_bot_config(db)
    cfg.access_token = payload.access_token
    cfg.refresh_token = payload.refresh_token
    cfg.expires_at = payload.expires_at
    scopes_value = " ".join(_normalize_scope_list(payload.scopes)) or None
    if scopes_value:
        cfg.scopes = scopes_value
    cfg.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(cfg)
    return _serialize_bot_config(cfg, include_tokens=True)


@app.post("/bot/config/oauth", response_model=AuthUrlOut, dependencies=[Depends(require_token)])
def bot_oauth_start(
    request: FastAPIRequest,
    payload: Optional[BotOAuthStartIn] = Body(default=None),
    db: Session = Depends(get_db),
): 
    client_id = get_twitch_client_id()
    client_secret = get_twitch_client_secret()
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Twitch OAuth not configured")
    cfg = _get_bot_config(db)
    configured_scopes = (cfg.scopes or "").split()
    seen: set[str] = set()
    scopes: list[str] = []
    default_scopes = get_bot_app_scopes()
    for scope in configured_scopes or default_scopes:
        scope_value = scope.strip()
        if scope_value and scope_value not in seen:
            seen.add(scope_value)
            scopes.append(scope_value)
    if not scopes:
        scopes = list(default_scopes)
    redirect_uri = _bot_redirect_uri(request)
    nonce = secrets.token_urlsafe(24)
    _cleanup_bot_oauth_states()
    return_url = _normalize_return_url(payload.return_url) if payload else None
    state_payload: Dict[str, Any] = {"nonce": nonce}
    if return_url:
        state_payload["return_url"] = return_url
    state_param = quote(json.dumps(state_payload, separators=(",", ":")), safe="")
    scope_param = quote(" ".join(scopes), safe="")
    client_id_param = quote(client_id, safe="")
    redirect_param = quote(redirect_uri, safe="")
    auth_url = (
        "https://id.twitch.tv/oauth2/authorize"
        f"?response_type=code&client_id={client_id_param}"
        f"&redirect_uri={redirect_param}&scope={scope_param}&state={state_param}"
    )
    _bot_oauth_states[nonce] = {
        "return_url": return_url,
        "scopes": scopes,
        "created_at": time.time(),
    }
    return {"auth_url": auth_url}


@app.get("/bot/config/oauth/callback")
def bot_oauth_callback(
    code: str,
    state: str,
    request: FastAPIRequest,
    db: Session = Depends(get_db),
):
    pending: Optional[Dict[str, Any]] = None
    try:
        client_id = get_twitch_client_id()
        client_secret = get_twitch_client_secret()
        if not client_id or not client_secret:
            raise HTTPException(status_code=500, detail="Twitch OAuth not configured")
        if not state:
            raise HTTPException(status_code=400, detail="missing state")
        try:
            state_data = json.loads(state)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="invalid state") from exc
        nonce = state_data.get("nonce") if isinstance(state_data, dict) else None
        if not nonce or not isinstance(nonce, str):
            raise HTTPException(status_code=400, detail="invalid state")
        pending = _bot_oauth_states.pop(nonce, None)
        if not pending:
            raise HTTPException(status_code=400, detail="state expired or invalid")
        redirect_url = pending.get("return_url") if pending else None
        expected_scopes = pending.get("scopes") or get_bot_app_scopes()
        redirect_uri = _bot_redirect_uri(request)
        try:
            token_response = requests.post(
                "https://id.twitch.tv/oauth2/token",
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": redirect_uri,
                },
                timeout=10,
            )
            token_response.raise_for_status()
        except requests.RequestException as exc:
            logger.exception("failed to exchange bot oauth code: %s", exc)
            return _bot_oauth_html_response(
                False,
                "Failed to exchange authorization code with Twitch.",
                redirect_url=redirect_url,
                status_code=502,
            )
        try:
            token_payload = token_response.json()
        except ValueError:
            return _bot_oauth_html_response(
                False,
                "Invalid response from Twitch during authorization.",
                redirect_url=redirect_url,
                status_code=502,
            )
        access_token = token_payload.get("access_token")
        if not access_token:
            message = token_payload.get("message") or "Authorization response missing access token."
            return _bot_oauth_html_response(
                False,
                str(message),
                redirect_url=redirect_url,
                status_code=502,
            )
        refresh_token = token_payload.get("refresh_token")
        scopes_list = token_payload.get("scope") or []
        if isinstance(scopes_list, str):
            scopes_list = [scopes_list]
        scopes_list = [scope for scope in scopes_list if isinstance(scope, str) and scope]
        expected_set = {scope for scope in expected_scopes if isinstance(scope, str) and scope}
        if expected_set and not expected_set.issubset(set(scopes_list)):
            missing = sorted(expected_set.difference(set(scopes_list)))
            message = "Missing required scopes: " + ", ".join(missing)
            return _bot_oauth_html_response(
                False,
                message,
                redirect_url=redirect_url,
                status_code=400,
            )
        expires_at: Optional[datetime] = None
        expires_in = token_payload.get("expires_in")
        if isinstance(expires_in, (int, float)) and expires_in > 0:
            expires_at = datetime.utcnow() + timedelta(seconds=int(expires_in))
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Client-Id": client_id,
        }
        try:
            user_response = requests.get(
                "https://api.twitch.tv/helix/users",
                headers=headers,
                timeout=10,
            )
            user_response.raise_for_status()
            user_payload = user_response.json()
        except (requests.RequestException, ValueError) as exc:
            logger.exception("failed to fetch bot user profile: %s", exc)
            return _bot_oauth_html_response(
                False,
                "Failed to fetch bot account information from Twitch.",
                redirect_url=redirect_url,
                status_code=502,
            )
        data_list = user_payload.get("data") if isinstance(user_payload, dict) else None
        user_info = data_list[0] if data_list else None
        if not user_info or not isinstance(user_info, dict):
            return _bot_oauth_html_response(
                False,
                "Twitch response did not include bot account information.",
                redirect_url=redirect_url,
                status_code=502,
            )
        login = user_info.get("login")
        display_name = user_info.get("display_name")
        if not login:
            return _bot_oauth_html_response(
                False,
                "Twitch response missing bot account login.",
                redirect_url=redirect_url,
                status_code=502,
            )
        cfg = _get_bot_config(db)
        cfg.login = login
        cfg.display_name = display_name or login
        cfg.access_token = access_token
        cfg.refresh_token = refresh_token
        cfg.scopes = " ".join(scopes_list) if scopes_list else None
        cfg.expires_at = expires_at
        cfg.enabled = True
        cfg.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(cfg)
        global BOT_USER_ID
        BOT_USER_ID = user_info.get("id")
        _broadcast_bot_log(
            {
                "type": "oauth_complete",
                "level": "info",
                "message": f"Bot app access token acquired for {login}",
                "timestamp": datetime.utcnow(),
            }
        )
        return _bot_oauth_html_response(
            True,
            "Bot authorization completed successfully.",
            redirect_url=redirect_url,
            status_code=200,
        )
    except HTTPException as exc:
        redirect_url = None
        if pending:
            redirect_url = pending.get("return_url")
        return _bot_oauth_html_response(
            False,
            str(exc.detail),
            redirect_url=redirect_url,
            status_code=exc.status_code,
        )
    except Exception:
        logger.exception("unexpected error during bot oauth callback")
        redirect_url = None
        if pending:
            redirect_url = pending.get("return_url")
        return _bot_oauth_html_response(
            False,
            "Unexpected error completing bot authorization.",
            redirect_url=redirect_url,
            status_code=500,
        )


@app.post("/bot/logs", response_model=BotLogAckOut, dependencies=[Depends(require_token)])
def push_bot_log(event: BotLogEventIn):
    timestamp = event.timestamp or datetime.utcnow()
    payload = {
        "type": "log",
        "level": event.level,
        "message": event.message,
        "source": event.source,
        "timestamp": timestamp,
        "metadata": event.metadata or {},
    }
    _broadcast_bot_log(payload)
    return {"success": True}


@app.get("/bot/logs/stream", dependencies=[Depends(require_token)])
async def stream_bot_logs():
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)
    _bot_log_listeners.add(queue)

    async def event_stream():
        try:
            yield "event: log\ndata: {\"type\": \"ready\"}\n\n"
            while True:
                msg = await queue.get()
                yield f"event: log\ndata: {msg}\n\n"
        finally:
            _bot_log_listeners.discard(queue)

    return EventSourceResponse(
        event_stream(),
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/me", response_model=MeOut)
def me(current: TwitchUser = Depends(get_current_user)):
    payload: Dict[str, Optional[str]] = {
        "login": current.username,
        "display_name": current.username,
        "profile_image_url": None,
    }
    client_id = get_twitch_client_id()
    if client_id and current.access_token:
        try:
            headers = {
                "Authorization": f"Bearer {current.access_token}",
                "Client-Id": client_id,
            }
            resp = requests.get(
                "https://api.twitch.tv/helix/users",
                headers=headers,
                timeout=5,
            )
            if resp.ok:
                data = resp.json().get("data") or []
                if data:
                    info = data[0]
                    payload["display_name"] = info.get("display_name") or info.get("login") or current.username
                    payload["profile_image_url"] = info.get("profile_image_url")
        except Exception:
            # Best effort; fall back to stored username if Twitch API lookup fails.
            pass
    if not payload.get("display_name"):
        payload["display_name"] = current.username
    return payload

@app.get("/me/channels", response_model=List[ChannelAccessOut])
def my_channels(current: TwitchUser = Depends(get_current_user), db: Session = Depends(get_db)):
    owned = db.query(ActiveChannel).filter_by(owner_id=current.id).all()
    mod_links = db.query(ChannelModerator).filter_by(user_id=current.id).all()
    res = [{"channel_name": c.channel_name, "role": "owner"} for c in owned]
    for link in mod_links:
        ch = db.get(ActiveChannel, link.channel_id)
        if ch:
            res.append({"channel_name": ch.channel_name, "role": "moderator"})
    return res

@app.post("/channels/{channel}/mods", dependencies=[Depends(require_token)])
def add_mod(channel: str, payload: ModIn, db: Session = Depends(get_db), authorization: str = Header(None)):
    channel_pk = get_channel_pk(channel, db)
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1]
        current = db.query(TwitchUser).filter_by(access_token=token).one()
        ch = db.get(ActiveChannel, channel_pk)
        if ch.owner_id != current.id:
            raise HTTPException(status_code=403, detail="only owner can add moderators")
    user = db.query(TwitchUser).filter_by(twitch_id=payload.twitch_id).one_or_none()
    if not user:
        user = TwitchUser(
            twitch_id=payload.twitch_id,
            username=payload.username,
            access_token="",
            refresh_token="",
            scopes="",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    link = (
        db.query(ChannelModerator)
        .filter_by(channel_id=channel_pk, user_id=user.id)
        .one_or_none()
    )
    if not link:
        link = ChannelModerator(channel_id=channel_pk, user_id=user.id)
        db.add(link)
        db.commit()
    return {"success": True}

# =====================================
# Helpers / Services
# =====================================

def get_or_create_settings(db: Session, channel_pk: int) -> ChannelSettings:
    """Return channel settings, creating or backfilling defaults when missing.

    Dependencies: Uses the provided ``Session`` to query ``ChannelSettings`` by
    ``channel_pk`` and will commit changes when default queue caps are applied.
    Code customers: Any route or helper that needs consistent settings, such as
    queue enforcement, playlist helpers, and channel metadata endpoints.
    Used variables/origin: Receives ``channel_pk`` from upstream path parameters
    or ownership checks, and writes default values onto the settings row when
    ``overall_queue_cap`` or ``nonpriority_queue_cap`` are unset.
    """

    st = db.query(ChannelSettings).filter(ChannelSettings.channel_id == channel_pk).one_or_none()
    created = False
    if not st:
        st = ChannelSettings(channel_id=channel_pk)
        db.add(st)
        created = True

    backfilled = False
    if st.overall_queue_cap is None:
        st.overall_queue_cap = 100
        backfilled = True
    if st.nonpriority_queue_cap is None:
        st.nonpriority_queue_cap = 100
        backfilled = True
    default_pricing = {
        "prio_follow_enabled": 1,
        "prio_raid_enabled": 1,
        "prio_bits_per_point": 200,
        "prio_gifts_per_point": 5,
        "prio_sub_tier1_points": 0,
        "prio_sub_tier2_points": 0,
        "prio_sub_tier3_points": 0,
        "prio_reset_points_tier1": 0,
        "prio_reset_points_tier2": 0,
        "prio_reset_points_tier3": 0,
        "prio_reset_points_vip": 0,
        "prio_reset_points_mod": 0,
        "free_mod_priority_requests": 0,
    }
    for field, default_value in default_pricing.items():
        if getattr(st, field, None) is None:
            setattr(st, field, default_value)
            backfilled = True

    if created or backfilled:
        db.commit()
        db.refresh(st)
    return st


def get_or_create_bot_state(db: Session, channel_pk: int) -> ChannelBotState:
    state = (
        db.query(ChannelBotState)
        .filter(ChannelBotState.channel_id == channel_pk)
        .one_or_none()
    )
    if not state:
        state = ChannelBotState(channel_id=channel_pk, active=False)
        db.add(state)
        db.commit()
        db.refresh(state)
    return state


def _normalize_keyword(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized or None


def _normalize_keywords(keywords: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for raw in keywords:
        key = _normalize_keyword(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return normalized


def _replace_playlist_keywords(playlist: Playlist, keywords: Iterable[str]) -> List[str]:
    normalized = _normalize_keywords(keywords)
    existing_by_keyword = {kw.keyword: kw for kw in playlist.keywords}

    # Reuse existing keyword objects when possible to avoid violating the
    # (playlist_id, keyword) unique constraint during flush.
    playlist.keywords[:] = [
        existing_by_keyword.get(keyword, PlaylistKeyword(keyword=keyword))
        for keyword in normalized
    ]
    return normalized


def _normalize_visibility(value: Optional[str]) -> str:
    if not value:
        return "public"
    token = value.strip().lower()
    if token in {"public"}:
        return "public"
    if token in {"unlisted", "notlisted", "not_listed"}:
        return "unlisted"
    raise HTTPException(status_code=400, detail="invalid visibility")


def _extract_playlist_id(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    query = parse_qs(parsed.query)
    candidates = query.get("list")
    if candidates:
        identifier = (candidates[0] or "").strip()
        if identifier:
            return identifier
    path = (parsed.path or "").strip("/")
    if "playlist" in path:
        parts = path.split("/")
        if parts and parts[-1]:
            return parts[-1]
    return None


def _build_playlist_url(playlist_id: str) -> str:
    return f"https://www.youtube.com/playlist?list={playlist_id}"


def _extract_video_id(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    if parsed.netloc in {"youtu.be"}:
        candidate = parsed.path.strip("/")
        return candidate or None
    if parsed.netloc.endswith("youtube.com"):
        if parsed.path == "/watch":
            query = parse_qs(parsed.query)
            values = query.get("v")
            if values:
                token = (values[0] or "").strip()
                if token:
                    return token
        parts = [part for part in parsed.path.split("/") if part]
        if parts and parts[0] == "shorts" and len(parts) > 1:
            return parts[1]
    return None


def _build_video_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _canonicalize_video_url(url: Optional[str], video_id: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if video_id:
        normalized_id = video_id.strip()
    else:
        normalized_id = _extract_video_id(url or "")
    normalized_url: Optional[str] = None
    if url:
        normalized_url = url.strip()
    if normalized_id:
        normalized_url = _build_video_url(normalized_id)
    return normalized_url, normalized_id


def _parse_duration_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        parts = value.split(":")
        total = 0
        try:
            for part in parts:
                total = total * 60 + int(part)
            return total
        except ValueError:
            return None
    return None


def _fetch_playlist_tracks(playlist_id: str) -> tuple[str, List[Dict[str, Any]]]:
    try:
        client = get_ytmusic_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail="youtube music unavailable") from exc
    try:
        raw = client.get_playlist(playlist_id, limit=500)
    except Exception as exc:
        logger.exception("Failed to load playlist %s", playlist_id)
        raise HTTPException(status_code=502, detail="failed to load playlist") from exc
    title = raw.get("title") or playlist_id
    tracks = raw.get("tracks") or []
    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for idx, track in enumerate(tracks, start=1):
        video_id = track.get("videoId")
        if not video_id or video_id in seen:
            continue
        seen.add(video_id)
        item_title = track.get("title") or f"Video {video_id}"
        artist_names = []
        for artist in track.get("artists") or []:
            name = artist.get("name") if isinstance(artist, Mapping) else None
            if name:
                artist_names.append(name)
        artist = ", ".join(artist_names) if artist_names else "Unknown"
        duration = track.get("duration_seconds")
        if duration is not None:
            try:
                duration_int = int(duration)
            except (TypeError, ValueError):
                duration_int = None
        else:
            duration_int = _parse_duration_seconds(track.get("duration"))
        items.append(
            {
                "position": idx,
                "video_id": video_id,
                "title": item_title,
                "artist": artist,
                "duration_seconds": duration_int,
                "url": f"https://www.youtube.com/watch?v={video_id}",
            }
        )
    if not items:
        raise HTTPException(status_code=404, detail="playlist contains no playable tracks")
    return title, items


def _get_or_create_channel_user(db: Session, channel_pk: int, twitch_id: str, username: str) -> User:
    user = (
        db.query(User)
        .filter(User.channel_id == channel_pk, User.twitch_id == twitch_id)
        .one_or_none()
    )
    if user:
        if username and user.username != username:
            user.username = username
        return user
    user = User(channel_id=channel_pk, twitch_id=twitch_id, username=username or twitch_id)
    db.add(user)
    db.flush()
    return user


def _get_playlist_user(db: Session, channel_pk: int) -> User:
    return _get_or_create_channel_user(db, channel_pk, "__playlist__", "Playlist")


def _award_reset_priority_points(db: Session, channel_pk: int, settings: ChannelSettings) -> None:
    """Grant configured reset priority points to subscribers, VIPs, and mods.

    Dependencies: Reuses ``_collect_channel_roles`` for live role lookups,
    channel moderator links on ``ActiveChannel``, and ``award_prio_points`` to
    persist capped balances.
    Code customers: ``current_stream`` calls this helper once when a new stream
    (and therefore queue) is created so automated perks align with configured
    tiers.
    Used variables/origin: ``channel_pk`` scopes the affected channel; role
    identifiers originate from Twitch API responses or local moderator records;
    per-role point values originate from ``settings``.
    """

    reset_values = {
        "tier1": max(settings.prio_reset_points_tier1 or 0, 0),
        "tier2": max(settings.prio_reset_points_tier2 or 0, 0),
        "tier3": max(settings.prio_reset_points_tier3 or 0, 0),
        "vip": max(settings.prio_reset_points_vip or 0, 0),
        "mod": max(settings.prio_reset_points_mod or 0, 0),
    }
    if not any(reset_values.values()):
        return

    channel_obj = db.get(ActiveChannel, channel_pk)
    if not channel_obj:
        return

    role_collector = globals().get("_collect_channel_roles")
    if callable(role_collector):
        vip_ids, subs = role_collector(channel_obj)
    else:  # pragma: no cover - defensive fallback for legacy deployments
        vip_ids, subs = set(), {}

    moderator_ids: dict[str, str] = {}
    for mod_link in channel_obj.moderators:
        if mod_link.user and mod_link.user.twitch_id:
            moderator_ids[mod_link.user.twitch_id] = mod_link.user.username or mod_link.user.twitch_id

    existing_users = {
        u.twitch_id: u
        for u in db.query(User).filter(User.channel_id == channel_pk).all()
        if u.twitch_id
    }

    def resolve_user(twitch_id: str, username: Optional[str] = None) -> Optional[User]:
        if not twitch_id:
            return None
        cached = existing_users.get(twitch_id)
        if cached:
            return cached
        user = _get_or_create_channel_user(db, channel_pk, twitch_id, username or twitch_id)
        existing_users[twitch_id] = user
        return user

    pending_awards: dict[int, int] = {}
    for twitch_id, tier in subs.items():
        points = 0
        if tier == "1000" or str(tier).lower() == "prime":
            points = reset_values["tier1"]
        elif str(tier) == "2000":
            points = reset_values["tier2"]
        elif str(tier) == "3000":
            points = reset_values["tier3"]
        if points > 0:
            user = resolve_user(twitch_id)
            if user:
                pending_awards[user.id] = pending_awards.get(user.id, 0) + points

    for twitch_id in vip_ids:
        if reset_values["vip"] > 0:
            user = resolve_user(twitch_id)
            if user:
                pending_awards[user.id] = pending_awards.get(user.id, 0) + reset_values["vip"]

    for twitch_id, username in moderator_ids.items():
        if reset_values["mod"] > 0:
            user = resolve_user(twitch_id, username)
            if user:
                pending_awards[user.id] = pending_awards.get(user.id, 0) + reset_values["mod"]

    for user_id, delta in pending_awards.items():
        try:
            award_prio_points(db, channel_pk, user_id, delta)
        except HTTPException:
            logger.info("Skipped reset award for user %s in channel %s", user_id, channel_pk)


def _ensure_playlist_song(db: Session, channel_pk: int, item: PlaylistItem) -> Song:
    url, video_id = _canonicalize_video_url(item.url, item.video_id)
    if not url and not video_id:
        raise HTTPException(status_code=400, detail="playlist item missing video reference")
    if url and not video_id:
        video_id = _extract_video_id(url)
    if url and item.url != url:
        item.url = url
    if video_id and item.video_id != video_id:
        item.video_id = video_id
    song = (
        db.query(Song)
        .filter(Song.channel_id == channel_pk, Song.youtube_link == url)
        .one_or_none()
    )
    if song:
        return song
    song = Song(
        channel_id=channel_pk,
        artist=item.artist or "Unknown",
        title=item.title or "Unknown",
        youtube_link=url,
    )
    db.add(song)
    db.flush()
    return song


def _create_request_entry(
    db: Session,
    channel_pk: int,
    user_id: int,
    song_id: int,
    *,
    bumped: bool = False,
) -> Request:
    stream_id = current_stream(db, channel_pk)
    max_pos = (
        db.query(func.coalesce(func.max(Request.position), 0))
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == stream_id,
            Request.played == 0,
        )
        .scalar()
    )
    new_position = (max_pos or 0) + 1
    req = Request(
        channel_id=channel_pk,
        stream_id=stream_id,
        song_id=song_id,
        user_id=user_id,
        position=new_position,
    )
    if bumped:
        req.bumped = 1
        req.is_priority = 1
        req.priority_source = "admin"
    db.add(req)
    user = db.get(User, user_id)
    if user:
        user.amount_requested = (user.amount_requested or 0) + 1
    db.flush()
    return req


def _playlists_with_keyword(db: Session, channel_pk: int, keyword: str) -> List[Playlist]:
    return (
        db.query(Playlist)
        .join(PlaylistKeyword)
        .options(selectinload(Playlist.items))
        .filter(
            Playlist.channel_id == channel_pk,
            PlaylistKeyword.keyword == keyword,
            Playlist.visibility == "public",
        )
        .all()
    )


def _aggregate_playlist_items(playlists: Iterable[Playlist]) -> List[PlaylistItem]:
    collected: List[PlaylistItem] = []
    seen: set[str] = set()
    for playlist in playlists:
        for item in playlist.items:
            url, video_id = _canonicalize_video_url(item.url, item.video_id)
            if not url and not video_id:
                continue
            key = video_id or url
            if key in seen:
                continue
            seen.add(key)
            if url and item.url != url:
                item.url = url
            if video_id and item.video_id != video_id:
                item.video_id = video_id
            collected.append(item)
    return collected


def _create_default_favorites_playlist(db: Session, channel_pk: int) -> Playlist:
    existing = (
        db.query(Playlist)
        .filter(
            Playlist.channel_id == channel_pk,
            Playlist.source == "manual",
            Playlist.title == "Favorites",
        )
        .one_or_none()
    )
    if existing:
        return existing
    playlist = Playlist(
        channel_id=channel_pk,
        title="Favorites",
        description="Default favorites playlist",
        source="manual",
        visibility="public",
    )
    db.add(playlist)
    db.flush()
    for keyword in ("default", "favorite"):
        playlist.keywords.append(PlaylistKeyword(keyword=keyword))
    url, video_id = _canonicalize_video_url(
        "https://www.youtube.com/watch?v=9Pzj6U5c2cs",
        "9Pzj6U5c2cs",
    )
    item = PlaylistItem(
        playlist_id=playlist.id,
        title="Default Favorite",
        artist="Unknown",
        position=1,
        video_id=video_id,
        url=url,
    )
    db.add(item)
    db.flush()
    return playlist


def current_stream(db: Session, channel_pk: int) -> int:
    """Return an active stream id, creating a fresh stream and reset rewards when none exists.

    Dependencies: Relies on the shared database ``Session`` to look up or
    insert ``StreamSession`` rows, and calls ``get_or_create_settings`` plus the
    reset award helper to initialize per-stream perks.
    Code customers: Queue intake, playlist utilities, and stats endpoints call
    this to scope actions to the current broadcast's queue.
    Used variables/origin: ``channel_pk`` identifies the channel; the function
    allocates a new stream and emits a queue change when no active session is
    present.
    """

    s = (
        db.query(StreamSession)
        .filter(StreamSession.channel_id == channel_pk, StreamSession.ended_at.is_(None))
        .one_or_none()
    )
    if s:
        return s.id
    s = StreamSession(channel_id=channel_pk)
    db.add(s)
    db.commit()
    settings = get_or_create_settings(db, channel_pk)
    _award_reset_priority_points(db, channel_pk, settings)
    publish_queue_changed(channel_pk)
    return s.id


def ensure_user_stream_state(db: Session, user_id: int, stream_id: int):
    exists = (
        db.query(UserStreamState)
        .filter(UserStreamState.user_id == user_id, UserStreamState.stream_id == stream_id)
        .one_or_none()
    )
    if not exists:
        db.add(UserStreamState(user_id=user_id, stream_id=stream_id, sub_free_used=0))
        db.commit()


def try_use_sub_free(db: Session, user_id: int, stream_id: int, is_subscriber: bool) -> bool:
    if not is_subscriber:
        return False
    ensure_user_stream_state(db, user_id, stream_id)
    st = (
        db.query(UserStreamState)
        .filter(UserStreamState.user_id == user_id, UserStreamState.stream_id == stream_id)
        .one()
    )
    if st.sub_free_used:
        return False
    st.sub_free_used = 1
    db.commit()
    return True


def _priority_spending_enabled(settings: ChannelSettings) -> bool:
    """Return whether the channel allows spending points to elevate requests.

    Dependencies: Reads the in-memory ``ChannelSettings`` instance provided by
    callers.
    Code customers: Priority automation helpers gate their behaviour on this
    signal before consuming points.
    Used variables/origin: Evaluates the ``allow_bumps`` flag on ``settings``.
    """

    return bool(settings.allow_bumps)


def _priority_points_for_tier(settings: ChannelSettings, tier: Optional[str]) -> int:
    """Map Twitch subscription tiers to configured per-sub priority point values.

    Dependencies: Reads ``ChannelSettings`` provided by callers.
    Code customers: Event ingestion and queue-reset reward helpers reuse this
    mapping to grant tier-weighted points consistently.
    Used variables/origin: ``tier`` originates from Twitch event metadata (e.g.,
    ``"1000"`` for tier 1). Missing or unknown tiers yield zero.
    """

    tier_str = (tier or "").strip()
    if tier_str == "1000" or tier_str.lower() == "prime":
        return max(settings.prio_sub_tier1_points or 0, 0)
    if tier_str == "2000":
        return max(settings.prio_sub_tier2_points or 0, 0)
    if tier_str == "3000":
        return max(settings.prio_sub_tier3_points or 0, 0)
    return 0


def _apply_priority_to_new_request(
    db: Session,
    settings: ChannelSettings,
    user_id: int,
    stream_id: int,
    *,
    want_priority: bool,
    prefer_sub_free: bool,
    is_subscriber: bool,
    is_mod: bool,
) -> tuple[int, Optional[str]]:
    """Try to make an incoming request priority by consuming freebies or points.

    Dependencies: Uses ``try_use_sub_free`` for subscriber perks and fetches the
    ``User`` row from the provided database session.
    Code customers: The queue intake endpoint calls this helper to consistently
    enforce new priority automation rules.
    Used variables/origin: ``settings`` supplies channel toggles, ``user_id`` and
    ``stream_id`` originate from the intake payload, ``want_priority`` mirrors
    the caller's explicit intent before any auto-upgrade logic applies, and
    ``is_mod`` originates from chat context to allow moderator freebies when
    configured.
    """

    if not _priority_spending_enabled(settings):
        if want_priority:
            raise HTTPException(409, detail="Priority spending disabled")
        return 0, None

    priority_source: Optional[str] = None
    if want_priority and settings.free_mod_priority_requests and is_mod:
        return 1, "mod_free"
    if want_priority and prefer_sub_free and try_use_sub_free(db, user_id, stream_id, is_subscriber):
        return 1, "sub_free"

    user = db.get(User, user_id)
    if not user:
        raise HTTPException(404, "user not found")

    auto_requested = settings.full_auto_priority_mode and not want_priority
    if (want_priority or auto_requested) and (user.prio_points or 0) > 0:
        user.prio_points = (user.prio_points or 0) - 1
        priority_source = "points" if want_priority else "auto_points"
        return 1, priority_source

    if want_priority:
        raise HTTPException(409, detail="No priority available")

    return 0, None


def _auto_upgrade_pending_requests(
    db: Session,
    channel_pk: int,
    user_id: int,
    settings: ChannelSettings,
    stream_id: Optional[int] = None,
) -> int:
    """Promote pending requests when full-auto priority mode and points allow it.

    Dependencies: Consults ``current_stream`` for default stream scoping and
    mutates ``Request`` plus ``User`` rows within the provided ``Session``.
    Code customers: ``award_prio_points`` invokes this helper after adding
    points so that overlays and bots immediately reflect automatic upgrades.
    Used variables/origin: ``channel_pk`` and ``user_id`` come from the request
    context; ``settings`` supplies channel-wide toggles and ``stream_id`` may be
    supplied or derived on-demand.
    """

    if not (_priority_spending_enabled(settings) and settings.full_auto_priority_mode):
        return 0

    sid = stream_id if stream_id is not None else current_stream(db, channel_pk)
    if sid is None:
        return 0

    user = db.query(User).filter(User.id == user_id, User.channel_id == channel_pk).one_or_none()
    if not user or (user.prio_points or 0) <= 0:
        return 0

    pending: list[Request] = (
        db.query(Request)
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == sid,
            Request.user_id == user_id,
            Request.played == 0,
            Request.is_priority == 0,
        )
        .order_by(
            Request.position.asc(),
            Request.request_time.asc(),
            Request.id.asc(),
        )
        .all()
    )

    upgraded: list[Request] = []
    for req in pending:
        if (user.prio_points or 0) <= 0:
            break
        user.prio_points = max((user.prio_points or 0) - 1, 0)
        req.is_priority = 1
        if not req.priority_source:
            req.priority_source = "auto_points"
        upgraded.append(req)

    if not upgraded:
        return 0

    db.commit()
    for req in upgraded:
        db.refresh(req)
        publish_channel_event(channel_pk, "request.bumped", _serialize_request_event(db, req))
    publish_queue_changed(channel_pk)
    return len(upgraded)


def award_prio_points(db: Session, channel_pk: int, user_id: int, delta: int):
    """Award and potentially auto-spend priority points for a channel user.

    Dependencies: Leverages ``get_or_create_settings`` for channel caps and the
    auto-upgrade helper to keep queues synchronized.
    Code customers: Event ingestion (follows, bits, subs) and admin tooling rely
    on this helper to centralize balance updates.
    Used variables/origin: ``channel_pk`` and ``user_id`` come from callers;
    ``delta`` represents the requested adjustment; database persistence happens
    through the shared ``Session``.
    """

    user = db.query(User).filter(User.id == user_id, User.channel_id == channel_pk).one_or_none()
    if not user:
        raise HTTPException(404, detail="user not found in channel")
    settings = get_or_create_settings(db, channel_pk)
    cap = settings.max_prio_points or 10
    old_val = user.prio_points or 0
    new_val = min(cap, old_val + delta)
    user.prio_points = new_val
    db.commit()
    db.refresh(user)

    _auto_upgrade_pending_requests(db, channel_pk, user_id, settings)
    db.refresh(user)

    applied_delta = (user.prio_points or 0) - old_val
    if applied_delta > 0:
        publish_channel_event(
            channel_pk,
            "user.bump_awarded",
            {
                "user": _serialize_user_summary(user),
                "delta": applied_delta,
                "prio_points": user.prio_points,
            },
        )
    return user.prio_points


def enforce_queue_limits(db: Session, channel_pk: int, user_id: int, want_priority: bool):
    """Block queue intake when channel or capacity limits are exceeded.

    Dependencies: Pulls channel settings via ``get_or_create_settings``, uses the
    active stream id from ``current_stream``, and queries ``Request`` rows to
    measure queue depth.
    Code customers: The queue intake endpoint (`POST /channels/{channel}/queue`)
    calls this guard before persisting a request.
    Used variables/origin: ``channel_pk`` is derived from the path parameter,
    ``user_id`` and ``want_priority`` come from the request payload, and the
    database session executes queue-count queries.
    """

    settings = get_or_create_settings(db, channel_pk)
    if settings.queue_closed:
        raise HTTPException(409, detail="queue closed")
    if settings.prio_only and not want_priority:
        raise HTTPException(409, detail="priority requests only")

    stream_id = current_stream(db, channel_pk)
    base_filters = [
        Request.channel_id == channel_pk,
        Request.stream_id == stream_id,
        Request.played == 0,
    ]

    overall_cap = settings.overall_queue_cap
    if overall_cap is not None and overall_cap >= 0:
        queue_depth = db.query(Request).filter(*base_filters).count()
        if queue_depth >= overall_cap:
            if not settings.queue_closed:
                settings.queue_closed = 1
                db.commit()
                db.refresh(settings)
                emit_queue_status_event(
                    channel_pk,
                    True,
                    "closed",
                    "Queue reached the overall capacity limit.",
                )
                publish_channel_event(
                    channel_pk, "settings.updated", _serialize_settings_event(settings)
                )
            raise HTTPException(409, detail="queue capacity reached")

    if not want_priority:
        nonpriority_cap = settings.nonpriority_queue_cap
        if nonpriority_cap is not None and nonpriority_cap >= 0:
            nonpriority_depth = (
                db.query(Request)
                .filter(*base_filters, Request.is_priority == 0)
                .count()
            )
            if nonpriority_depth >= nonpriority_cap:
                emit_queue_status_event(
                    channel_pk,
                    False,
                    "limited",
                    "Non-priority queue capacity reached.",
                )
                raise HTTPException(409, detail="non-priority queue capacity reached")

    if settings.max_requests_per_user and settings.max_requests_per_user >= 0:
        count = (
            db.query(Request)
            .filter(
                Request.channel_id == channel_pk,
                Request.stream_id == stream_id,
                Request.user_id == user_id,
                Request.played == 0,
            )
            .count()
        )
        if count >= settings.max_requests_per_user:
            raise HTTPException(409, detail="user request limit reached")


def seed_default_data():
    db = SessionLocal()
    try:
        existing = (
            db.query(ActiveChannel)
            .filter(func.lower(ActiveChannel.channel_name) == "example_channel")
            .one_or_none()
        )
        if existing:
            return

        channel = ActiveChannel(
            channel_id="example-channel-id",
            channel_name="example_channel",
            channel_key=generate_channel_key(),
            join_active=1,
            authorized=True,
        )
        db.add(channel)
        db.commit()
        db.refresh(channel)

        get_or_create_settings(db, channel.id)

        now = datetime.utcnow()
        archive_started = now - timedelta(days=1, hours=2)
        archive_ended = archive_started + timedelta(hours=2)

        queue_song = Song(
            channel_id=channel.id,
            artist="Daft Punk",
            title="Veridis Quo",
            total_played=0,
        )

        archive_songs = [
            Song(
                channel_id=channel.id,
                artist="Queen",
                title="Don't Stop Me Now",
                total_played=1,
                date_first_played=archive_started + timedelta(minutes=15),
                date_last_played=archive_started + timedelta(minutes=15),
            ),
            Song(
                channel_id=channel.id,
                artist="David Bowie",
                title="Heroes",
                total_played=1,
                date_first_played=archive_started + timedelta(minutes=40),
                date_last_played=archive_started + timedelta(minutes=40),
            ),
            Song(
                channel_id=channel.id,
                artist="CHVRCHES",
                title="The Mother We Share",
                total_played=1,
                date_first_played=archive_started + timedelta(minutes=65),
                date_last_played=archive_started + timedelta(minutes=65),
            ),
        ]

        db.add(queue_song)
        db.add_all(archive_songs)
        db.commit()
        db.refresh(queue_song)
        for song in archive_songs:
            db.refresh(song)

        user = User(
            channel_id=channel.id,
            twitch_id="example-user-id",
            username="example_user",
            amount_requested=len(archive_songs) + 1,
            prio_points=0,
        )
        db.add(user)
        db.commit()
        db.refresh(user)

        active_stream = StreamSession(channel_id=channel.id)
        archived_stream = StreamSession(
            channel_id=channel.id,
            started_at=archive_started,
            ended_at=archive_ended,
        )
        db.add_all([active_stream, archived_stream])
        db.commit()
        db.refresh(active_stream)
        db.refresh(archived_stream)

        queue_request = Request(
            channel_id=channel.id,
            stream_id=active_stream.id,
            song_id=queue_song.id,
            user_id=user.id,
            position=1,
            played=0,
        )
        db.add(queue_request)

        for idx, song in enumerate(archive_songs, start=1):
            played_at = archive_started + timedelta(minutes=idx * 20)
            req = Request(
                channel_id=channel.id,
                stream_id=archived_stream.id,
                song_id=song.id,
                user_id=user.id,
                request_time=played_at,
                position=idx,
                played=1,
            )
            db.add(req)

        db.commit()
    finally:
        db.close()


ensure_channel_key_schema()
ensure_channel_settings_schema()
backfill_missing_channel_keys()
seed_default_data()

# =====================================
# Routes: System
# =====================================
@app.get("/system/status", response_model=SystemStatusOut)
def system_status():
    return {"setup_complete": is_setup_complete()}


@app.get("/system/meta", response_model=SystemMetaOut)
def system_meta():
    return {"version": API_VERSION, "dev_mode": DEV_MODE}


@app.get("/system/config", response_model=SystemConfigOut)
def system_config():
    return _system_config_payload()


@app.put("/system/config", response_model=SystemConfigOut)
def update_system_config(
    payload: SystemConfigUpdate,
    x_admin_token: Optional[str] = Header(None),
):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="invalid admin token")

    updates: Dict[str, Optional[str]] = {}
    credentials_changed = False

    if payload.twitch_client_id is not None:
        updates["twitch_client_id"] = payload.twitch_client_id.strip() or None
        credentials_changed = True
    if payload.twitch_client_secret is not None:
        updates["twitch_client_secret"] = payload.twitch_client_secret.strip() or None
        credentials_changed = True
    if payload.twitch_redirect_uri is not None:
        updates["twitch_redirect_uri"] = payload.twitch_redirect_uri.strip() or None
    if payload.bot_redirect_uri is not None:
        updates["bot_redirect_uri"] = payload.bot_redirect_uri.strip() or None
    if payload.twitch_scopes is not None:
        normalized_scopes = _normalize_scope_list(payload.twitch_scopes)
        updates["twitch_scopes"] = " ".join(normalized_scopes) if normalized_scopes else None
    if payload.bot_app_scopes is not None:
        normalized_bot_scopes = _normalize_scope_list(payload.bot_app_scopes)
        updates["bot_app_scopes"] = " ".join(normalized_bot_scopes) if normalized_bot_scopes else None

    current = settings_store.snapshot()
    merged: Dict[str, Optional[str]] = dict(current)
    for key, value in updates.items():
        merged[key] = value
    if payload.setup_complete is not None:
        merged["setup_complete"] = "1" if payload.setup_complete else "0"

    requirements_met = _settings_requirements_met(merged)
    if payload.setup_complete and not requirements_met:
        raise HTTPException(
            status_code=400,
            detail="Twitch client ID and secret must be configured before completing setup",
        )

    if payload.setup_complete is not None:
        updates["setup_complete"] = "1" if payload.setup_complete else "0"
    elif not requirements_met:
        updates["setup_complete"] = "0"

    if updates:
        db = SessionLocal()
        try:
            set_settings(db, updates)
        finally:
            db.close()
    if credentials_changed:
        global APP_ACCESS_TOKEN, APP_TOKEN_EXPIRES, BOT_USER_ID
        APP_ACCESS_TOKEN = None
        APP_TOKEN_EXPIRES = 0
        BOT_USER_ID = None
    return _system_config_payload()


@app.get("/system/health")
def health():
    try:
        with engine.connect() as _:
            pass
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

# =====================================
# Routes: Channels
# =====================================
@app.get("/channels", response_model=List[ChannelOut])
def list_channels(db: Session = Depends(get_db)):
    channels = db.query(ActiveChannel).all()
    for channel in channels:
        if not channel.bot_state:
            channel.bot_state = get_or_create_bot_state(db, channel.id)
    return channels


def _chunk(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


@app.get("/channels/live_status", response_model=List[ChannelLiveStatusOut])
def get_channel_live_status(db: Session = Depends(get_db)):
    client_id = get_twitch_client_id()
    if not client_id:
        raise HTTPException(status_code=503, detail="twitch client id not configured")

    try:
        token = get_app_access_token()
    except RuntimeError as exc:  # pragma: no cover - defensive, validated via tests
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    channels = db.query(ActiveChannel.channel_id, ActiveChannel.channel_name).all()
    if not channels:
        return []

    headers = {"Authorization": f"Bearer {token}", "Client-Id": client_id}
    channel_ids = [channel.channel_id for channel in channels if channel.channel_id]
    live_map: dict[str, bool] = {channel.channel_id: False for channel in channels if channel.channel_id}

    for batch in _chunk(channel_ids, 100):
        params = [("user_id", channel_id) for channel_id in batch]
        try:
            response = requests.get(
                "https://api.twitch.tv/helix/streams",
                headers=headers,
                params=params,
                timeout=5,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise HTTPException(status_code=502, detail="twitch live status request failed") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise HTTPException(status_code=502, detail="invalid twitch response") from exc

        data = payload.get("data")
        if isinstance(data, list):
            for row in data:
                user_id = row.get("user_id")
                if not isinstance(user_id, str):
                    continue
                live_map[user_id] = (row.get("type") or "").lower() == "live"

    return [
        ChannelLiveStatusOut(
            channel_name=channel.channel_name,
            channel_id=channel.channel_id,
            is_live=live_map.get(channel.channel_id, False),
        )
        for channel in channels
    ]


@app.post("/channels", response_model=ChannelOut, dependencies=[Depends(require_token)])
def add_channel(payload: ChannelIn, db: Session = Depends(get_db)):
    ch = ActiveChannel(
        channel_id=payload.channel_id,
        channel_name=payload.channel_name,
        channel_key=generate_channel_key(),
        join_active=payload.join_active,
    )
    db.add(ch)
    db.commit()
    db.refresh(ch)
    get_or_create_settings(db, ch.id)
    get_or_create_bot_state(db, ch.id)
    _create_default_favorites_playlist(db, ch.id)
    db.commit()
    channel_pk = ch.id
    publish_queue_changed(channel_pk)
    return ch

@app.put("/channels/{channel}", dependencies=[Depends(require_token)])
def update_channel_status(channel: str, join_active: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        raise HTTPException(404, "channel not found")
    ch.join_active = join_active
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}


@app.post("/channels/{channel}/bot_status", dependencies=[Depends(require_token)])
def set_channel_bot_status(channel: str, payload: ChannelBotStatusIn, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        raise HTTPException(404, "channel not found")
    state = get_or_create_bot_state(db, channel_pk)
    state.active = bool(payload.active)
    state.last_error = payload.error
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.delete("/channels/{channel}", dependencies=[Depends(require_token)])
def delete_channel(channel: str, db: Session = Depends(get_db), authorization: str = Header(None)):
    channel_pk = get_channel_pk(channel, db)
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1]
        user = db.query(TwitchUser).filter_by(access_token=token).one_or_none()
        ch = db.get(ActiveChannel, channel_pk)
        if not user or ch.owner_id != user.id:
            raise HTTPException(status_code=403, detail="only owner can unregister")
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        raise HTTPException(status_code=404, detail="channel not found")
    db.delete(ch)
    db.commit()
    return {"success": True}

@app.get("/channels/{channel}/settings", response_model=ChannelSettingsOut)
def get_channel_settings(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    st = get_or_create_settings(db, channel_pk)
    return ChannelSettingsOut(
        channel_id=st.channel_id,
        max_requests_per_user=st.max_requests_per_user,
        prio_only=st.prio_only,
        queue_closed=st.queue_closed,
        allow_bumps=st.allow_bumps,
        full_auto_priority_mode=st.full_auto_priority_mode,
        other_flags=st.other_flags,
        max_prio_points=st.max_prio_points,
        overall_queue_cap=st.overall_queue_cap,
        nonpriority_queue_cap=st.nonpriority_queue_cap,
        prio_follow_enabled=st.prio_follow_enabled,
        prio_raid_enabled=st.prio_raid_enabled,
        prio_bits_per_point=st.prio_bits_per_point,
        prio_gifts_per_point=st.prio_gifts_per_point,
        prio_sub_tier1_points=st.prio_sub_tier1_points,
        prio_sub_tier2_points=st.prio_sub_tier2_points,
        prio_sub_tier3_points=st.prio_sub_tier3_points,
        prio_reset_points_tier1=st.prio_reset_points_tier1,
        prio_reset_points_tier2=st.prio_reset_points_tier2,
        prio_reset_points_tier3=st.prio_reset_points_tier3,
        prio_reset_points_vip=st.prio_reset_points_vip,
        prio_reset_points_mod=st.prio_reset_points_mod,
        free_mod_priority_requests=st.free_mod_priority_requests,
    )


@app.get("/channels/{channel}/oauth", response_model=ChannelOAuthOut)
def get_channel_oauth(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        raise HTTPException(status_code=404, detail="channel not found")
    owner_login: Optional[str] = None
    scopes: List[str] = []
    owner = ch.owner
    if owner:
        owner_login = owner.username
        if owner.scopes:
            scopes = owner.scopes.split()
    authorized = bool(ch.authorized and owner and owner.access_token)
    return ChannelOAuthOut(
        channel_name=ch.channel_name,
        authorized=authorized,
        owner_login=owner_login,
        scopes=scopes,
    )


@app.get("/channels/{channel}/key", response_model=ChannelKeyOut)
def get_channel_key(
    channel: str,
    current: TwitchUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return the current channel key to authorized owners and moderators.

    Dependencies: Uses `get_current_user` plus `Session` to confirm channel-level roles via `_user_has_access`.
    Code customers: Control panels that need to display or share the active key with trusted operators.
    Used variables/origin: Reads `ActiveChannel.channel_key` for the requested `channel` path parameter.
    """

    channel_pk = get_channel_pk(channel, db)
    if not _user_has_access(current, channel_pk, db):
        raise HTTPException(status_code=403, detail="not authorized for channel")
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        raise HTTPException(status_code=404, detail="channel not found")
    if not ch.channel_key:
        ch.channel_key = generate_channel_key()
        db.commit()
        db.refresh(ch)
    return ChannelKeyOut(channel_id=ch.id, channel_name=ch.channel_name, channel_key=ch.channel_key)


@app.post("/channels/{channel}/key/regenerate", response_model=ChannelKeyOut)
def regenerate_channel_key(
    channel: str,
    current: TwitchUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Replace and return the channel key for owners or moderators who need to rotate secrets.

    Dependencies: Shares the same authentication stack as `get_channel_key`, using `get_current_user` and the database session.
    Code customers: Interfaces that allow moderators to cycle compromised keys without database access.
    Used variables/origin: Writes a new `generate_channel_key` output onto the matched `ActiveChannel` row.
    """

    channel_pk = get_channel_pk(channel, db)
    if not _user_has_access(current, channel_pk, db):
        raise HTTPException(status_code=403, detail="not authorized for channel")
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        raise HTTPException(status_code=404, detail="channel not found")
    ch.channel_key = generate_channel_key()
    db.commit()
    db.refresh(ch)
    return ChannelKeyOut(channel_id=ch.id, channel_name=ch.channel_name, channel_key=ch.channel_key)

@app.put("/channels/{channel}/settings", dependencies=[Depends(require_token)])
def set_channel_settings(
    channel: str, payload: ChannelSettingsUpdate, db: Session = Depends(get_db)
):
    """Apply partial channel settings without resetting unspecified fields to defaults.

    Dependencies: Relies on ``get_channel_pk`` for lookup and ``get_or_create_settings``
    for fetching the persisted settings row. Code customers: the admin UI and any
    automation calling ``/channels/{channel}/settings`` use this route to tweak
    queue behavior incrementally. Used variables/origin: merges ``payload`` values
    into the ``ChannelSettings`` instance via ``_apply_settings_patch`` and emits
    queue status events when ``queue_closed`` flips.
    """

    channel_pk = get_channel_pk(channel, db)
    st = get_or_create_settings(db, channel_pk)
    prev_queue_closed = bool(st.queue_closed)
    _apply_settings_patch(st, payload)
    db.commit()
    db.refresh(st)
    updated_settings = _serialize_settings_event(st)
    if bool(st.queue_closed) != prev_queue_closed:
        emit_queue_status_event(
            channel_pk,
            bool(st.queue_closed),
            "closed" if st.queue_closed else "open",
            "Queue closed via settings update." if st.queue_closed else "Queue reopened via settings update.",
        )
    publish_channel_event(channel_pk, "settings.updated", updated_settings)
    publish_queue_changed(channel_pk)
    return {"success": True}

# =====================================
# Routes: YouTube Music search
# =====================================


@app.get("/ytmusic/search", response_model=List[YTMusicSearchResult])
def search_ytmusic(query: str = Query(..., min_length=1, max_length=200)):
    q = query.strip()
    if not q:
        raise HTTPException(status_code=400, detail="query required")

    try:
        client = get_ytmusic_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail="youtube music unavailable") from exc

    try:
        raw_results = client.search(q, limit=10)
    except Exception as exc:
        logger.exception("YouTube Music search failed for query %s", q)
        raise HTTPException(status_code=502, detail="youtube music search failed") from exc

    results: List[YTMusicSearchResult] = []
    for item in raw_results:
        normalized = _normalize_ytmusic_result(item)
        if not normalized:
            continue
        results.append(normalized)
        if len(results) >= 5:
            break

    return results

# =====================================
# Routes: Songs
# =====================================


@app.get("/channels/{channel}/playlists", response_model=List[PlaylistOut], dependencies=[Depends(require_channel_key)])
def list_playlists(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    playlists = (
        db.query(Playlist)
        .options(selectinload(Playlist.keywords), selectinload(Playlist.items))
        .filter(Playlist.channel_id == channel_pk)
        .order_by(Playlist.title.asc())
        .all()
    )
    results: List[PlaylistOut] = []
    for playlist in playlists:
        keywords = sorted(kw.keyword for kw in playlist.keywords)
        results.append(
            PlaylistOut(
                id=playlist.id,
                title=playlist.title,
                description=playlist.description,
                playlist_id=playlist.playlist_id,
                url=playlist.url,
                source=playlist.source,
                visibility=playlist.visibility,
                keywords=keywords,
                item_count=len(playlist.items),
            )
        )
    return results


@app.get(
    "/channels/{channel}/public/playlists",
    response_model=List[PublicPlaylistOut],
)
def list_public_playlists(
    channel: str,
    response: Response,
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    rows: List[Playlist] = (
        db.query(Playlist)
        .options(selectinload(Playlist.items), selectinload(Playlist.keywords))
        .filter(Playlist.channel_id == channel_pk, Playlist.visibility == "public")
        .order_by(Playlist.title.asc(), Playlist.id.asc())
        .all()
    )
    payload: List[PublicPlaylistOut] = []
    for playlist in rows:
        keywords = sorted(
            {kw.keyword for kw in playlist.keywords if kw.keyword},
        )
        ordered_items = sorted(
            playlist.items,
            key=lambda entry: ((entry.position or 0), entry.id),
        )
        slug = playlist.playlist_id or str(playlist.id)
        payload.append(
            PublicPlaylistOut(
                id=playlist.id,
                title=playlist.title,
                description=playlist.description,
                slug=slug,
                source=playlist.source,
                visibility=playlist.visibility,
                url=playlist.url,
                keywords=keywords,
                item_count=len(ordered_items),
                items=[
                    PublicPlaylistItemOut(
                        id=item.id,
                        title=item.title,
                        artist=item.artist,
                        video_id=item.video_id,
                        url=item.url,
                        position=item.position,
                        duration_seconds=item.duration_seconds,
                    )
                    for item in ordered_items
                ],
            )
        )
    response.headers.setdefault("Cache-Control", "public, max-age=30")
    return payload


@app.post(
    "/channels/{channel}/playlists",
    response_model=dict,
    dependencies=[Depends(require_channel_key)],
)
def create_playlist(channel: str, payload: PlaylistCreate, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    visibility = _normalize_visibility(payload.visibility)
    normalized_keywords = _normalize_keywords(payload.keywords)
    if payload.manual:
        manual = payload.manual
        title = manual.title.strip()
        if not title:
            raise HTTPException(status_code=400, detail="playlist title is required")
        description = None
        if manual.description:
            desc = manual.description.strip()
            description = desc or None
        playlist = Playlist(
            channel_id=channel_pk,
            title=title,
            description=description,
            source="manual",
            visibility=visibility,
        )
        db.add(playlist)
        db.flush()
        for keyword in normalized_keywords:
            db.add(PlaylistKeyword(playlist_id=playlist.id, keyword=keyword))
        db.commit()
        return {"id": playlist.id}

    playlist_url = payload.url or ""
    playlist_id = _extract_playlist_id(playlist_url)
    if not playlist_id:
        raise HTTPException(status_code=400, detail="invalid playlist url")
    existing = (
        db.query(Playlist)
        .filter(Playlist.channel_id == channel_pk, Playlist.playlist_id == playlist_id)
        .one_or_none()
    )
    if existing:
        raise HTTPException(status_code=409, detail="playlist already added")
    title, tracks = _fetch_playlist_tracks(playlist_id)
    playlist = Playlist(
        channel_id=channel_pk,
        title=title,
        playlist_id=playlist_id,
        url=_build_playlist_url(playlist_id),
        visibility=visibility,
        source="youtube",
    )
    db.add(playlist)
    db.flush()
    for keyword in normalized_keywords:
        db.add(PlaylistKeyword(playlist_id=playlist.id, keyword=keyword))
    for item in tracks:
        db.add(
            PlaylistItem(
                playlist_id=playlist.id,
                position=item["position"],
                video_id=item["video_id"],
                title=item["title"],
                artist=item["artist"],
                duration_seconds=item["duration_seconds"],
                url=item["url"],
            )
        )
    db.commit()
    return {"id": playlist.id}


@app.put(
    "/channels/{channel}/playlists/{playlist_id}",
    response_model=PlaylistOut,
    dependencies=[Depends(require_channel_key)],
)
def update_playlist(
    channel: str,
    playlist_id: int,
    payload: PlaylistUpdate,
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    playlist = (
        db.query(Playlist)
        .options(selectinload(Playlist.keywords), selectinload(Playlist.items))
        .filter(Playlist.channel_id == channel_pk, Playlist.id == playlist_id)
        .one_or_none()
    )
    if not playlist:
        raise HTTPException(status_code=404, detail="playlist not found")
    slug_included = "slug" in payload.model_fields_set
    if slug_included:
        if playlist.source != "manual" and playlist.playlist_id not in (None, ""):
            raise HTTPException(status_code=400, detail="slug can only be changed for manual playlists")
        if payload.slug is None:
            slug_value: Optional[str] = None
        else:
            slug_value = payload.slug.strip()
            if not slug_value:
                slug_value = None
        if slug_value:
            existing = (
                db.query(Playlist)
                .filter(Playlist.channel_id == channel_pk)
                .filter(Playlist.id != playlist.id)
                .filter(Playlist.playlist_id == slug_value)
                .one_or_none()
            )
            if existing:
                raise HTTPException(status_code=409, detail="playlist slug already in use")
        playlist.playlist_id = slug_value

    if payload.visibility is not None:
        playlist.visibility = _normalize_visibility(payload.visibility)
    if payload.keywords is not None:
        _replace_playlist_keywords(playlist, payload.keywords)
    db.commit()
    db.refresh(playlist)
    keywords = sorted(kw.keyword for kw in playlist.keywords)
    return PlaylistOut(
        id=playlist.id,
        title=playlist.title,
        description=playlist.description,
        playlist_id=playlist.playlist_id,
        url=playlist.url,
        source=playlist.source,
        visibility=playlist.visibility,
        keywords=keywords,
        item_count=len(playlist.items),
    )


@app.delete(
    "/channels/{channel}/playlists/{playlist_id}",
    status_code=204,
    dependencies=[Depends(require_channel_key)],
)
def delete_playlist(channel: str, playlist_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    playlist = (
        db.query(Playlist)
        .filter(Playlist.channel_id == channel_pk, Playlist.id == playlist_id)
        .one_or_none()
    )
    if not playlist:
        raise HTTPException(status_code=404, detail="playlist not found")
    db.delete(playlist)
    db.commit()
    return Response(status_code=204)


@app.get(
    "/channels/{channel}/playlists/{playlist_id}/items",
    response_model=List[PlaylistItemOut],
    dependencies=[Depends(require_channel_key)],
)
def list_playlist_items(channel: str, playlist_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    playlist = (
        db.query(Playlist)
        .options(selectinload(Playlist.items))
        .filter(Playlist.channel_id == channel_pk, Playlist.id == playlist_id)
        .one_or_none()
    )
    if not playlist:
        raise HTTPException(status_code=404, detail="playlist not found")
    items = sorted(playlist.items, key=lambda entry: entry.position)
    results: List[PlaylistItemOut] = []
    for item in items:
        url, video_id = _canonicalize_video_url(item.url, item.video_id)
        if url != item.url:
            item.url = url
        if video_id != item.video_id:
            item.video_id = video_id
        results.append(
            PlaylistItemOut(
                id=item.id,
                title=item.title,
                artist=item.artist,
                video_id=video_id,
                url=url,
                position=item.position,
                duration_seconds=item.duration_seconds,
            )
        )
    return results


@app.post(
    "/channels/{channel}/playlists/{playlist_id}/items",
    response_model=PlaylistItemOut,
    dependencies=[Depends(require_token)],
)
def create_playlist_item(
    channel: str,
    playlist_id: int,
    payload: PlaylistItemCreate,
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    playlist = (
        db.query(Playlist)
        .options(selectinload(Playlist.items))
        .filter(Playlist.channel_id == channel_pk, Playlist.id == playlist_id)
        .one_or_none()
    )
    if not playlist:
        raise HTTPException(status_code=404, detail="playlist not found")
    if playlist.source != "manual":
        raise HTTPException(
            status_code=400,
            detail="playlist items can only be managed for manual playlists",
        )
    normalized_url, normalized_video_id = _canonicalize_video_url(payload.url, payload.video_id)
    item_title = (payload.title or "Untitled").strip()
    item_artist = payload.artist.strip() if payload.artist else None
    normalized_title_key = item_title.lower()
    normalized_artist_key = (item_artist or "").lower()
    if not payload.title and not normalized_video_id:
        raise HTTPException(status_code=400, detail="playlist item title or video is required")
    for existing in playlist.items:
        existing_url, existing_video_id = _canonicalize_video_url(existing.url, existing.video_id)
        if normalized_video_id and normalized_video_id == existing_video_id:
            raise HTTPException(status_code=409, detail="playlist item already exists")
        if normalized_url and existing_url and normalized_url == existing_url:
            raise HTTPException(status_code=409, detail="playlist item already exists")
        if (
            not normalized_video_id
            and not normalized_url
            and existing.title
            and existing.title.strip().lower() == normalized_title_key
            and (existing.artist or "").strip().lower() == normalized_artist_key
        ):
            raise HTTPException(status_code=409, detail="playlist item already exists")
    max_position = max((item.position for item in playlist.items), default=0)
    item = PlaylistItem(
        playlist_id=playlist.id,
        title=item_title,
        artist=item_artist,
        position=max_position + 1,
        duration_seconds=payload.duration_seconds,
        video_id=normalized_video_id,
        url=normalized_url,
    )
    db.add(item)
    db.flush()
    db.commit()
    db.refresh(item)
    return PlaylistItemOut(
        id=item.id,
        title=item.title,
        artist=item.artist,
        video_id=item.video_id,
        url=item.url,
        position=item.position,
        duration_seconds=item.duration_seconds,
    )


@app.delete(
    "/channels/{channel}/playlists/{playlist_id}/items/{item_id}",
    status_code=204,
    dependencies=[Depends(require_token)],
)
def delete_playlist_item(
    channel: str,
    playlist_id: int,
    item_id: int,
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    playlist = (
        db.query(Playlist)
        .filter(Playlist.channel_id == channel_pk, Playlist.id == playlist_id)
        .one_or_none()
    )
    if not playlist:
        raise HTTPException(status_code=404, detail="playlist not found")
    if playlist.source != "manual":
        raise HTTPException(
            status_code=400,
            detail="playlist items can only be managed for manual playlists",
        )
    item = (
        db.query(PlaylistItem)
        .filter(PlaylistItem.id == item_id, PlaylistItem.playlist_id == playlist.id)
        .one_or_none()
    )
    if not item:
        raise HTTPException(status_code=404, detail="playlist item not found")
    db.delete(item)
    db.commit()
    return Response(status_code=204)


@app.post(
    "/channels/{channel}/playlists/{playlist_id}/queue",
    response_model=dict,
    dependencies=[Depends(require_channel_key)],
)
def queue_playlist_item(
    channel: str,
    playlist_id: int,
    payload: PlaylistQueueIn,
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    playlist = (
        db.query(Playlist)
        .filter(Playlist.channel_id == channel_pk, Playlist.id == playlist_id)
        .one_or_none()
    )
    if not playlist:
        raise HTTPException(status_code=404, detail="playlist not found")
    item = (
        db.query(PlaylistItem)
        .filter(PlaylistItem.id == payload.item_id, PlaylistItem.playlist_id == playlist.id)
        .one_or_none()
    )
    if not item:
        raise HTTPException(status_code=404, detail="playlist item not found")
    playlist_user = _get_playlist_user(db, channel_pk)
    song = _ensure_playlist_song(db, channel_pk, item)
    req = _create_request_entry(db, channel_pk, playlist_user.id, song.id, bumped=payload.bumped)
    db.commit()
    db.refresh(req)
    payload_data = _serialize_request_event(db, req)
    publish_channel_event(channel_pk, "request.added", payload_data)
    if req.is_priority or req.bumped:
        publish_channel_event(channel_pk, "request.bumped", payload_data)
    publish_queue_changed(channel_pk)
    return {"request_id": req.id}


@app.post(
    "/channels/{channel}/playlists/request",
    response_model=PlaylistRequestOut,
    dependencies=[Depends(require_token)],
)
def request_playlist_item(
    channel: str,
    payload: PlaylistRequestIn,
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    identifier = payload.identifier.strip()
    if not identifier:
        raise HTTPException(status_code=400, detail="playlist identifier required")
    identifier_lower = identifier.lower()
    filters = [func.lower(Playlist.title) == identifier_lower, Playlist.playlist_id == identifier]
    numeric_id: Optional[int]
    try:
        numeric_id = int(identifier)
    except ValueError:
        numeric_id = None
    if numeric_id is not None:
        filters.append(Playlist.id == numeric_id)
    playlist = (
        db.query(Playlist)
        .options(selectinload(Playlist.items))
        .filter(Playlist.channel_id == channel_pk)
        .filter(or_(*filters))
        .one_or_none()
    )
    if not playlist:
        raise HTTPException(status_code=404, detail="playlist not found")
    if playlist.visibility != "public":
        raise HTTPException(status_code=404, detail="playlist not found")
    items = sorted(playlist.items, key=lambda entry: (entry.position or 0, entry.id))
    if not items:
        raise HTTPException(status_code=404, detail="playlist contains no items")
    if payload.index > len(items):
        raise HTTPException(status_code=400, detail="index out of range")
    item = items[payload.index - 1]
    playlist_user = _get_playlist_user(db, channel_pk)
    song = _ensure_playlist_song(db, channel_pk, item)
    req = _create_request_entry(db, channel_pk, playlist_user.id, song.id, bumped=False)
    db.commit()
    db.refresh(req)
    payload_data = _serialize_request_event(db, req)
    publish_channel_event(channel_pk, "request.added", payload_data)
    if req.is_priority or req.bumped:
        publish_channel_event(channel_pk, "request.bumped", payload_data)
    publish_queue_changed(channel_pk)
    pick = PlaylistSongPick(
        id=song.id,
        artist=song.artist,
        title=song.title,
        youtube_link=song.youtube_link,
    )
    return PlaylistRequestOut(
        request_id=req.id,
        playlist_item_id=item.id,
        song=pick,
    )


@app.post(
    "/channels/{channel}/playlists/random_request",
    response_model=RandomPlaylistRequestOut,
    dependencies=[Depends(require_channel_key)],
)
def random_playlist_request(
    channel: str,
    payload: RandomPlaylistRequestIn,
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    keyword = _normalize_keyword(payload.keyword) if payload.keyword else None
    playlists: List[Playlist] = []
    resolved_keyword = keyword or ""
    if keyword:
        playlists = _playlists_with_keyword(db, channel_pk, keyword)
    if not playlists:
        playlists = _playlists_with_keyword(db, channel_pk, "default")
        if playlists:
            resolved_keyword = "default"
    if not playlists:
        raise HTTPException(status_code=404, detail="no playlist for keyword")
    pool = _aggregate_playlist_items(playlists)
    if not pool:
        raise HTTPException(status_code=404, detail="no playlist items available")
    choice = random.choice(pool)
    user = _get_or_create_channel_user(db, channel_pk, payload.twitch_id, payload.username)
    db.flush()
    enforce_queue_limits(db, channel_pk, user.id, want_priority=False)
    stream_id = current_stream(db, channel_pk)
    existing_req = (
        db.query(Request.id)
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == stream_id,
            Request.user_id == user.id,
        )
        .first()
    )
    if not existing_req and payload.is_subscriber:
        try:
            award_prio_points(db, channel_pk, user.id, 1)
        except HTTPException:
            pass
    song = _ensure_playlist_song(db, channel_pk, choice)
    req = _create_request_entry(db, channel_pk, user.id, song.id, bumped=False)
    db.commit()
    db.refresh(req)
    event_payload = _serialize_request_event(db, req)
    publish_channel_event(channel_pk, "request.added", event_payload)
    publish_queue_changed(channel_pk)
    pick = PlaylistSongPick(
        id=song.id,
        artist=song.artist,
        title=song.title,
        youtube_link=song.youtube_link,
    )
    return RandomPlaylistRequestOut(
        request_id=req.id,
        song=pick,
        playlist_item_id=choice.id,
        keyword=resolved_keyword,
    )


@app.get("/channels/{channel}/songs", response_model=List[SongOut])
def search_songs(channel: str, search: Optional[str] = Query(None), db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    q = db.query(Song).filter(Song.channel_id == channel_pk)
    if search:
        like = f"%{search}%"
        q = q.filter(
            (
                (Song.artist.ilike(like))
                | (Song.title.ilike(like))
                | (Song.youtube_link.ilike(like))
            )
        )
    return q.order_by(Song.artist.asc(), Song.title.asc()).all()

@app.post("/channels/{channel}/songs", response_model=dict, dependencies=[Depends(require_token)])
def add_song(channel: str, payload: SongIn, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    payload_data = payload.model_dump()
    youtube_link = payload_data.get("youtube_link")
    normalized_link: Optional[str] = None
    if youtube_link:
        normalized_link, _ = _canonicalize_video_url(youtube_link, None)
        target_link = normalized_link or youtube_link.strip()
        if target_link:
            existing = (
                db.query(Song)
                .filter(Song.channel_id == channel_pk, Song.youtube_link == target_link)
                .one_or_none()
            )
            if existing:
                return {"id": existing.id}
        if normalized_link:
            payload_data["youtube_link"] = normalized_link
        elif youtube_link:
            payload_data["youtube_link"] = youtube_link.strip()
    song = Song(channel_id=channel_pk, **payload_data)
    db.add(song)
    db.commit()
    publish_queue_changed(channel_pk)
    return {"id": song.id}

@app.get("/channels/{channel}/songs/{song_id}", response_model=SongOut)
def get_song(channel: str, song_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    song = db.query(Song).filter(Song.id == song_id, Song.channel_id == channel_pk).one_or_none()
    if not song:
        raise HTTPException(404, "song not found")
    return song

@app.put("/channels/{channel}/songs/{song_id}", dependencies=[Depends(require_token)])
def update_song(channel: str, song_id: int, payload: SongIn, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    song = db.query(Song).filter(Song.id == song_id, Song.channel_id == channel_pk).one_or_none()
    if not song:
        raise HTTPException(404, "song not found")
    for k, v in payload.model_dump().items():
        setattr(song, k, v)
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.delete("/channels/{channel}/songs/{song_id}", dependencies=[Depends(require_token)])
def delete_song(channel: str, song_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    song = db.query(Song).filter(Song.id == song_id, Song.channel_id == channel_pk).one_or_none()
    if not song:
        raise HTTPException(404, "song not found")
    db.delete(song)
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

# =====================================
# Routes: Users
# =====================================
@app.get("/channels/{channel}/users", response_model=List[UserOut])
def search_users(channel: str, search: Optional[str] = Query(None), db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    q = db.query(User).filter(User.channel_id == channel_pk)
    if search:
        like = f"%{search}%"
        q = q.filter(User.username.ilike(like))
    return q.order_by(User.username.asc()).all()

@app.post("/channels/{channel}/users", response_model=dict, dependencies=[Depends(require_token)])
def get_or_create_user(channel: str, payload: UserIn, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    u = (
        db.query(User)
        .filter(User.channel_id == channel_pk, User.twitch_id == payload.twitch_id)
        .one_or_none()
    )
    if u:
        u.username = payload.username  # update latest name
        db.commit()
        publish_queue_changed(channel_pk)
        return {"id": u.id}
    u = User(channel_id=channel_pk, twitch_id=payload.twitch_id, username=payload.username)
    db.add(u)
    db.commit()
    publish_queue_changed(channel_pk)
    return {"id": u.id}

@app.get("/channels/{channel}/users/{user_id}", response_model=UserOut)
def get_user(channel: str, user_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    u = db.query(User).filter(User.id == user_id, User.channel_id == channel_pk).one_or_none()
    if not u:
        raise HTTPException(404, "user not found")
    return u

@app.put("/channels/{channel}/users/{user_id}", dependencies=[Depends(require_token)])
def update_user(channel: str, user_id: int, prio_points: Optional[int] = None, amount_requested: Optional[int] = None, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    u = db.query(User).filter(User.id == user_id, User.channel_id == channel_pk).one_or_none()
    if not u:
        raise HTTPException(404, "user not found")
    if prio_points is not None:
        st = get_or_create_settings(db, channel_pk)
        u.prio_points = max(0, min(st.max_prio_points or 10, prio_points))
    if amount_requested is not None:
        u.amount_requested = max(0, amount_requested)
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.get("/channels/{channel}/users/{user_id}/stream_state")
def get_user_stream_state(channel: str, user_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    ensure_user_stream_state(db, user_id, sid)
    st = (
        db.query(UserStreamState)
        .filter(UserStreamState.user_id == user_id, UserStreamState.stream_id == sid)
        .one()
    )
    return {"stream_id": sid, "sub_free_used": int(st.sub_free_used)}

@app.get("/channels/{channel}/users", dependencies=[Depends(require_token)])
def list_users(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    return db.query(User).filter(User.channel_id==channel_pk).all()

@app.put("/channels/{channel}/users/{user_id}/points", dependencies=[Depends(require_token)])
def set_points(channel: str, user_id: int, payload: dict, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    u = db.get(User, user_id)
    if not u or u.channel_id != channel_pk:
        raise HTTPException(404)
    u.prio_points = int(payload.get("prio_points", 0))
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

# =====================================
# Routes: Queue
# =====================================


def _iter_twitch_collection(url: str, headers: Mapping[str, str], params: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield paginated Twitch API collections safely.

    Dependencies: Performs HTTP GET requests via ``requests`` using the provided
    headers and query params, retrying pagination cursors until exhausted.
    Code customers: Used by Twitch role helpers to enumerate VIPs and
    subscribers without duplicating pagination logic.
    Used variables/origin: ``url`` targets the collection endpoint while
    ``headers`` and ``params`` originate from the caller's OAuth token and
    broadcaster context.
    """

    query: dict[str, Any] = dict(params)
    while True:
        try:
            resp = requests.get(url, headers=headers, params=query, timeout=10)
        except requests.RequestException as exc:
            logger.warning("failed to fetch %s: %s", url, exc)
            return
        if resp.status_code in (401, 403):
            logger.warning(
                "twitch request to %s returned %s; channel authorization may be missing scopes",
                url,
                resp.status_code,
            )
            return
        if not resp.ok:
            logger.warning(
                "twitch request to %s failed: status=%s body=%s",
                url,
                resp.status_code,
                resp.text,
            )
            return
        try:
            payload = resp.json()
        except ValueError as exc:
            logger.warning("invalid JSON from twitch %s: %s", url, exc)
            return
        data = payload.get("data")
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item
        pagination = payload.get("pagination")
        cursor = None
        if isinstance(pagination, dict):
            cursor = pagination.get("cursor")
        if cursor:
            query = dict(query)
            query["after"] = cursor
        else:
            return


def _collect_channel_roles(channel_obj: ActiveChannel) -> tuple[set[str], dict[str, Optional[str]]]:
    """Collect VIP and subscription roles for a channel via Twitch API.

    Dependencies: Requires a valid ``ActiveChannel`` with an ``owner`` that has
    an OAuth access token, Twitch client ID via ``get_twitch_client_id``, and
    uses ``_iter_twitch_collection`` for pagination.
    Code customers: Role-aware queue APIs such as ``get_queue_full`` and
    ``_award_reset_priority_points`` call this to enrich user payloads and reset
    rewards.
    Used variables/origin: Pulls the broadcaster ID from ``channel_obj`` to
    scope VIP and subscriber lookups; role IDs and tiers originate from Helix
    responses.
    """

    if not channel_obj or not channel_obj.owner or not channel_obj.channel_id:
        return set(), {}
    owner = channel_obj.owner
    client_id = get_twitch_client_id()
    if not client_id or not owner.access_token:
        return set(), {}
    headers = {
        "Authorization": f"Bearer {owner.access_token}",
        "Client-Id": client_id,
    }
    params = {"broadcaster_id": channel_obj.channel_id}
    vip_ids: set[str] = set()
    subs: dict[str, Optional[str]] = {}
    for row in _iter_twitch_collection("https://api.twitch.tv/helix/channels/vips", headers, params):
        user_id = row.get("user_id")
        if isinstance(user_id, str):
            vip_ids.add(user_id)
    for row in _iter_twitch_collection("https://api.twitch.tv/helix/subscriptions", headers, params):
        user_id = row.get("user_id")
        tier = row.get("tier") if isinstance(row, dict) else None
        if isinstance(user_id, str):
            subs[user_id] = tier if isinstance(tier, str) else None
    return vip_ids, subs


def _coerce_int(value: Any, *, default: int = 0) -> int:
    """Best-effort conversion of arbitrary values to integers.

    User metadata stored in the database predates the new Pydantic response
    models and can therefore contain unexpected types (e.g. stringified
    numbers). Rather than returning a 500 error when validation fails we coerce
    the value into a safe integer and fall back to ``default`` when conversion
    is not possible.
    """

    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return default
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            return int(stripped, 10)
        except ValueError:
            try:
                float_value = float(stripped)
            except ValueError:
                return default
            if not math.isfinite(float_value):
                return default
            return int(float_value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_str(value: Any, *, default: str = "") -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return default
    return str(value)


def _build_queue_user_payload(
    user: User,
    vip_ids: set[str],
    subs: dict[str, Optional[str]],
) -> Optional[UserWithRoles]:
    base_data = {
        "id": _coerce_int(getattr(user, "id", 0), default=0),
        "twitch_id": _coerce_str(getattr(user, "twitch_id", "")),
        "username": _coerce_str(getattr(user, "username", "")),
        "amount_requested": _coerce_int(getattr(user, "amount_requested", 0), default=0),
        "prio_points": _coerce_int(getattr(user, "prio_points", 0), default=0),
    }
    try:
        base_user = UserOut.model_validate(base_data)
    except ValidationError:
        logger.warning(
            "skipping user %s in queue due to invalid stored data",
            getattr(user, "id", None),
            exc_info=True,
        )
        return None
    twitch_id = base_user.twitch_id
    return UserWithRoles(
        **base_user.model_dump(),
        is_vip=twitch_id in vip_ids,
        is_subscriber=twitch_id in subs,
        subscriber_tier=subs.get(twitch_id),
    )


def _normalize_ytmusic_result(item: Mapping[str, Any]) -> Optional[YTMusicSearchResult]:
    if not isinstance(item, Mapping):
        return None

    title_value = item.get("title")
    if isinstance(title_value, Mapping):
        title = title_value.get("text") or ""
    elif isinstance(title_value, str):
        title = title_value
    else:
        title = ""

    artists: List[str] = []
    for artist in item.get("artists", []) or []:
        if isinstance(artist, Mapping):
            name = artist.get("name")
            if isinstance(name, str):
                artists.append(name)

    album_name: Optional[str] = None
    album = item.get("album")
    if isinstance(album, Mapping):
        raw_album_name = album.get("name")
        if isinstance(raw_album_name, str):
            album_name = raw_album_name

    duration_value = item.get("duration")
    if isinstance(duration_value, Mapping):
        duration = duration_value.get("text")
    elif isinstance(duration_value, str):
        duration = duration_value
    else:
        duration = None
    if duration is not None and not isinstance(duration, str):
        duration = str(duration)

    thumbnails: List[YTMusicThumbnail] = []
    raw_thumbnails = item.get("thumbnails") or []
    for thumb in raw_thumbnails:
        if not isinstance(thumb, Mapping):
            continue
        url = thumb.get("url")
        if not isinstance(url, str):
            continue
        width_val = thumb.get("width")
        height_val = thumb.get("height")
        width = None
        height = None
        try:
            if width_val is not None:
                width = int(width_val)
        except (TypeError, ValueError):
            width = None
        try:
            if height_val is not None:
                height = int(height_val)
        except (TypeError, ValueError):
            height = None
        thumbnails.append(YTMusicThumbnail(url=url, width=width, height=height))

    video_id = item.get("videoId")
    if not isinstance(video_id, str):
        video_id = None

    playlist_id = item.get("playlistId")
    if not isinstance(playlist_id, str):
        playlist_id = None

    browse_id = item.get("browseId")
    if not isinstance(browse_id, str):
        browse_id = None

    result_type = item.get("resultType") or item.get("category")
    if isinstance(result_type, str):
        normalized_result_type = (
            result_type.strip().lower().replace(" ", "_").replace("-", "_")
        )
        if not normalized_result_type:
            normalized_result_type = None
    else:
        result_type = None
        normalized_result_type = None

    link = item.get("link")
    if not isinstance(link, str):
        link = None
    if not link:
        if video_id:
            link = f"https://www.youtube.com/watch?v={video_id}"
        elif playlist_id:
            link = f"https://www.youtube.com/playlist?list={playlist_id}"
        elif browse_id:
            link = f"https://music.youtube.com/browse/{browse_id}"

    if not video_id:
        return None

    allowed_result_types = {"song", "songs", "video", "videos", "music_video", "musicvideo"}
    if normalized_result_type and normalized_result_type not in allowed_result_types:
        return None

    return YTMusicSearchResult(
        title=title or "",
        video_id=video_id,
        playlist_id=playlist_id,
        browse_id=browse_id,
        result_type=result_type,
        artists=artists,
        album=album_name,
        duration=duration if isinstance(duration, str) else None,
        thumbnails=thumbnails,
        link=link,
    )


@app.get(
    "/channels/{channel}/queue/full",
    response_model=List[QueueItemFull],
)
def get_queue_full(
    channel: str,
    db: Session = Depends(get_db),
):
    """Return the full queue with request, song, and user metadata for the current stream.

    Dependencies: Relies on the `current_stream` helper and DB models (`Request`, `Song`, `User`) to assemble enriched rows.
    Code customers: Used by overlays and moderator dashboards that need a single call for the complete queue view.
    Used variables/origin: Pulls the path `channel` parameter, derives `channel_pk` via `get_channel_pk`, and reuses globally cached
    role data through `_collect_channel_roles` when available.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    rows: list[Request] = (
        db.query(Request)
        .filter(Request.channel_id == channel_pk, Request.stream_id == sid)
        .order_by(
            Request.played.asc(),
            Request.is_priority.desc(),
            Request.position.asc(),
            Request.request_time.asc(),
            Request.id.asc(),
        )
        .all()
    )
    song_ids = {row.song_id for row in rows}
    user_ids = {row.user_id for row in rows}
    songs: dict[int, Song] = {}
    if song_ids:
        for song in db.query(Song).filter(Song.id.in_(song_ids)).all():
            songs[song.id] = song
    users: dict[int, User] = {}
    if user_ids:
        for user in db.query(User).filter(User.id.in_(user_ids)).all():
            users[user.id] = user
    channel_obj = db.get(ActiveChannel, channel_pk)
    role_collector = globals().get("_collect_channel_roles")
    if callable(role_collector):
        vip_ids, subs = role_collector(channel_obj)
    else:  # pragma: no cover - defensive fallback for legacy deployments
        logger.warning("_collect_channel_roles helper missing; skipping role lookup")
        vip_ids, subs = set(), {}
    result: list[QueueItemFull] = []
    for row in rows:
        song = songs.get(row.song_id)
        user = users.get(row.user_id)
        if not song or not user:
            continue
        user_payload = _build_queue_user_payload(user, vip_ids, subs)
        if not user_payload:
            logger.warning(
                "skipping request %s due to invalid user payload",
                getattr(row, "id", None),
            )
            continue
        try:
            request_payload = RequestOut.model_validate(row)
        except ValidationError:
            logger.warning(
                "skipping request %s due to invalid request data",
                getattr(row, "id", None),
                exc_info=True,
            )
            continue
        try:
            song_payload = SongOut.model_validate(song)
        except ValidationError:
            logger.warning(
                "skipping request %s due to invalid song data",
                getattr(row, "id", None),
                exc_info=True,
            )
            continue
        result.append(
            QueueItemFull(
                request=request_payload,
                song=song_payload,
                user=user_payload,
            )
        )
    return result


@app.get("/channels/{channel}/queue/stream")
async def stream_queue(channel: str, db: Session = Depends(get_db)):
    """Stream queue updates for a channel via SSE without requiring authentication.

    Dependencies: database session provided by `get_db` for channel lookup.
    Code consumers: overlays and bot listeners use this endpoint for live updates.
    Variables: `channel` path parameter identifies the channel; `db` supplies DB access.
    """
    channel_pk = get_channel_pk(channel, db)
    q = _subscribe_queue(channel_pk)

    async def gen():
        # initial tick so clients render immediately
        try:
            yield {"event": "queue", "data": "init"}
            while True:
                msg = await q.get()
                yield {"event": "queue", "data": msg}
        finally:
            _unsubscribe_queue(channel_pk, q)

    return EventSourceResponse(
        gen(),
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
        },
    )


@app.websocket("/channels/{channel}/events")
async def channel_event_stream(channel: str, websocket: WebSocket) -> None:
    db = SessionLocal()
    try:
        try:
            channel_pk = get_channel_pk(channel, db)
        except HTTPException:
            await websocket.accept()
            await websocket.close(code=1008)
            return
        queue = _subscribe_channel_events(channel_pk)
        await websocket.accept()
        send_task: asyncio.Task[str] = asyncio.create_task(queue.get())
        receive_task = asyncio.create_task(websocket.receive_text())
        try:
            while True:
                done, _ = await asyncio.wait(
                    {send_task, receive_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if receive_task in done:
                    # connection closed by client
                    break
                if send_task in done:
                    try:
                        message = send_task.result()
                        await websocket.send_text(message)
                    except WebSocketDisconnect:
                        break
                    send_task = asyncio.create_task(queue.get())
        except WebSocketDisconnect:
            pass
        finally:
            send_task.cancel()
            receive_task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(send_task, receive_task, return_exceptions=True)
            _unsubscribe_channel_events(channel_pk, queue)
    finally:
        db.close()


@app.get("/channels/{channel}/queue", response_model=List[RequestOut])
def get_queue(channel: str, db: Session = Depends(get_db)):
    """Return the current queue for the active stream without requiring authentication.

    Dependencies: database session via `get_db` for channel resolution and queries.
    Code consumers: overlays, public embeds, and bots fetch the queue for display.
    Variables: `channel` path parameter selects the channel; `db` executes queries.
    """
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    return (
        db.query(Request)
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == sid,            
        )
        .order_by(
            Request.played.asc(),
            Request.is_priority.desc(),
            Request.position.asc(),      # <- use new position column
            Request.request_time.asc(),  # tie-breaker
            Request.id.asc(),            # final tie-breaker
        )
        .all()
    )

@app.get("/channels/{channel}/streams/{stream_id}/queue", response_model=List[RequestOut])
def get_stream_queue(channel: str, stream_id: int, db: Session = Depends(get_db)):
    """Return the queue for a specific stream without requiring authentication.

    Dependencies: database session via `get_db` for channel verification and queries.
    Code consumers: overlays and diagnostic tools that inspect past or specific streams.
    Variables: `channel` path parameter selects the channel; `stream_id` identifies the
    stream; `db` executes the database query.
    """
    channel_pk = get_channel_pk(channel, db)
    return (
        db.query(Request)
        .filter(Request.channel_id == channel_pk, Request.stream_id == stream_id)
        .order_by(
            Request.played.asc(),
            Request.is_priority.desc(),
            Request.position.asc(),
            Request.request_time.asc(),
            Request.id.asc(),
        )
        .all()
    )

@app.post("/channels/{channel}/queue", response_model=dict, dependencies=[Depends(require_channel_key)])
def add_request(channel: str, payload: RequestCreate, db: Session = Depends(get_db)):
    """Create a queue request, optionally spending points automatically.

    Dependencies: Resolves ``channel_pk`` via ``get_channel_pk``, loads
    ``ChannelSettings`` for gating, enforces queue limits, and uses
    ``current_stream`` for stream scoping.
    Code customers: Public intake endpoints and bots call this to add requests
    while keeping overlays synchronized through emitted events.
    Used variables/origin: Path parameter ``channel`` maps to ``channel_pk``;
    the payload contributes ``user_id``, ``song_id``, and priority preferences;
    moderator intent arrives via ``is_mod``; the database session persists both
    the new ``Request`` and any point spends.
    """

    channel_pk = get_channel_pk(channel, db)
    settings = get_or_create_settings(db, channel_pk)
    prioritized_intake = payload.want_priority or (
        settings.full_auto_priority_mode and _priority_spending_enabled(settings)
    )
    enforce_queue_limits(db, channel_pk, payload.user_id, prioritized_intake)
    sid = current_stream(db, channel_pk)

    # If subscriber's first request this stream, award a prio point
    existing_req = (
        db.query(Request.id)
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == sid,
            Request.user_id == payload.user_id,
        )
        .first()
    )
    if not existing_req and payload.is_subscriber:
        award_prio_points(db, channel_pk, payload.user_id, 1)

    max_pos = db.query(func.coalesce(func.max(Request.position), 0))\
        .filter(Request.channel_id == channel_pk,
                Request.stream_id == sid,
                Request.played == 0)\
        .scalar()
    
    new_position = (max_pos or 0) + 1

    is_priority, priority_source = _apply_priority_to_new_request(
        db,
        settings,
        payload.user_id,
        sid,
        want_priority=payload.want_priority,
        prefer_sub_free=payload.prefer_sub_free,
        is_subscriber=payload.is_subscriber,
        is_mod=payload.is_mod,
    )

    req = Request(
        channel_id=channel_pk,
        stream_id=sid,
        song_id=payload.song_id,
        user_id=payload.user_id,
        is_priority=is_priority,
        priority_source=priority_source,
        position=new_position,
    )
    db.add(req)

    # Update user stats
    u = db.get(User, payload.user_id)
    if u:
        u.amount_requested = (u.amount_requested or 0) + 1

    db.commit()
    db.refresh(req)
    event_payload = _serialize_request_event(db, req)
    publish_channel_event(channel_pk, "request.added", event_payload)
    if req.is_priority or req.bumped:
        publish_channel_event(channel_pk, "request.bumped", event_payload)

    publish_queue_changed(channel_pk)
    return {"request_id": req.id}

REQUEST_IDENTIFIER_PATTERN = r"(?i)^(?:\d+|top|previous|last|random)$"


def resolve_queue_request(db: Session, channel_pk: int, identifier: str | int) -> Request:
    """
    Locate a queue request by numeric id or keyword for a given channel.

    Dependencies: database session access via ``db``.
    Code customers: queue update endpoints relying on flexible identifiers.
    Variables origin: ``identifier`` comes from path parameters supplied by clients.
    """
    normalized = str(identifier or "").strip()
    if not normalized:
        raise HTTPException(status_code=404, detail="request not found")

    if normalized.isdigit():
        req = db.execute(
            select(Request).where(
                and_(Request.id == int(normalized), Request.channel_id == channel_pk)
            )
        ).scalar_one_or_none()
        if req:
            return req
        raise HTTPException(status_code=404, detail="request not found")

    keyword = normalized.lower()
    stream_id = current_stream(db, channel_pk)
    base_query = (
        db.query(Request)
        .filter(Request.channel_id == channel_pk, Request.stream_id == stream_id)
    )

    if keyword == "top":
        req = (
            base_query.filter(Request.played == 0)
            .order_by(
                Request.is_priority.desc(),
                Request.position.asc(),
                Request.request_time.asc(),
                Request.id.asc(),
            )
            .first()
        )
    elif keyword == "previous":
        req = (
            base_query.filter(Request.played == 1)
            .order_by(
                Request.request_time.desc(),
                Request.id.desc(),
            )
            .first()
        )
    elif keyword == "last":
        req = (
            base_query.filter(Request.played == 0)
            .order_by(
                Request.position.desc(),
                Request.request_time.desc(),
                Request.id.desc(),
            )
            .first()
        )
    elif keyword == "random":
        req = base_query.filter(Request.played == 0).order_by(func.random()).first()
    else:
        raise HTTPException(status_code=404, detail="request not found")

    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    return req


@app.put("/channels/{channel}/queue/{request_id}", dependencies=[Depends(require_token)])
def update_request(
    channel: str,
    payload: RequestUpdate,
    request_id: str = Path(..., pattern=REQUEST_IDENTIFIER_PATTERN),
    db: Session = Depends(get_db),
):
    """
    Update queue request status or priority attributes for a channel request.

    Dependencies: ``require_token`` authentication and database session ``db``.
    Code customers: channel moderation tools invoking request status changes.
    Variables origin: ``request_id`` from path, ``payload`` from request body, ``channel`` from path.
    """
    channel_pk = get_channel_pk(channel, db)
    req = resolve_queue_request(db, channel_pk, request_id)
    prev_played = bool(req.played)
    prev_bumped = bool(req.bumped)
    prev_priority = bool(req.is_priority)
    played_now = False
    became_bumped = False
    became_priority = False
    if payload.played is not None:
        req.played = 1 if payload.played else 0
        if req.played and not prev_played:
            played_now = True
        if req.played:
            # Update song stats
            s = db.get(Song, req.song_id)
            now = datetime.utcnow()
            if s:
                if not s.date_first_played:
                    s.date_first_played = now
                s.date_last_played = now
                s.total_played = (s.total_played or 0) + 1
    if payload.bumped is not None:
        req.bumped = 1 if payload.bumped else 0
        if req.bumped and not prev_bumped:
            became_bumped = True
    if payload.is_priority is not None:
        req.is_priority = 1 if payload.is_priority else 0
        if req.is_priority and not req.priority_source:
            req.priority_source = 'admin'
        if req.is_priority and not prev_priority:
            became_priority = True
    db.commit()
    db.refresh(req)
    event_payload = _serialize_request_event(db, req)
    if played_now:
        up_next = _next_pending_request(db, channel_pk, req.stream_id)
        publish_channel_event(
            channel_pk,
            "request.played",
            {
                "request": event_payload,
                "up_next": up_next,
            },
        )
    if became_bumped or became_priority:
        publish_channel_event(channel_pk, "request.bumped", event_payload)
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.delete("/channels/{channel}/queue/{request_id}", dependencies=[Depends(require_token)])
def remove_request(
    channel: str,
    request_id: str = Path(..., pattern=REQUEST_IDENTIFIER_PATTERN),
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    req = resolve_queue_request(db, channel_pk, request_id)
    db.delete(req)
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.post("/channels/{channel}/queue/clear", dependencies=[Depends(require_channel_key)])
def clear_queue(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    db.query(Request).filter(Request.channel_id == channel_pk, Request.stream_id == sid, Request.played == 0).delete()
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.get(
    "/channels/{channel}/queue/random_nonpriority",
)
def random_nonpriority(channel: str, db: Session = Depends(get_db)):
    """Fetch a random non-priority pending request for the current stream.

    Dependencies: Uses SQLAlchemy joins across `Request`, `Song`, and `User` plus `current_stream` for stream scoping.
    Code customers: Supports overlays and automation that highlight random viewer picks without privileged credentials.
    Used variables/origin: Accepts `channel` from the path, resolves `channel_pk` via `get_channel_pk`, and filters by `sid`,
    `played`, and `is_priority` fields to isolate eligible rows.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    row = (
        db.query(Request, Song, User)
        .join(Song, Song.id == Request.song_id)
        .join(User, User.id == Request.user_id)
        .filter(Request.channel_id == channel_pk,
                Request.stream_id == sid,
                Request.played == 0,
                Request.is_priority == 0)
        .order_by(func.random())
        .first()
    )
    if not row:
        return None
    r, s, u = row
    return {
        "request_id": r.id,
        "song": {"id": s.id, "artist": s.artist, "title": s.title},
        "user": {"id": u.id, "username": u.username}
    }


@app.get(
    "/channels/{channel}/queue/next_nonpriority",
)
def next_nonpriority(channel: str, db: Session = Depends(get_db)):
    """Return the next queued non-priority request, preferring bumped entries first.

    Dependencies: Opens a database session via `get_db` and consults
    `current_stream` for the active stream id. Code customers: overlays or bots
    that need a deterministic next-up item without requiring authentication.
    Used variables/origin: Resolves `channel_pk` from the path, filters the
    queue by `played==0` and `is_priority==0`, and orders by `bumped`,
    `position`, then `request_time` to surface bumped picks.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    return _next_nonpriority_request(db, channel_pk, sid)


@app.get(
    "/channels/{channel}/queue/next_priority",
)
def next_priority(channel: str, db: Session = Depends(get_db)):
    """Return the next queued priority request, honoring bumped entries first.

    Dependencies: Uses the shared database session from `get_db` and active
    stream resolution via `current_stream`. Code customers: public overlays
    surfacing VIP or priority picks. Used variables/origin: Resolves
    `channel_pk` from the path and filters pending requests where
    `played==0` and `is_priority==1`, ordering by `bumped` then position and
    request time.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    return _next_priority_request(db, channel_pk, sid)


@app.get(
    "/channels/{channel}/queue/next_song",
)
def next_song(channel: str, db: Session = Depends(get_db)):
    """Return the next queued song, preferring priority picks when available.

    Dependencies: Shares the `get_db` database session and `current_stream`
    lookup. Code customers: widgets that need a single endpoint for the next
    song regardless of priority status. Used variables/origin: Resolves
    `channel_pk` from the path, attempts `_next_priority_request`, and falls
    back to `_next_nonpriority_request` when no priority items remain.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    return _next_priority_request(db, channel_pk, sid) or _next_nonpriority_request(
        db, channel_pk, sid
    )


def _queue_stats_for_stream(db: Session, channel_pk: int, stream_id: Optional[int]) -> Dict[str, int]:
    """Compute queue counters for the given channel stream without authentication.

    Dependencies: SQLAlchemy session scoped to the app's engine. Code customers:
    public statistics endpoints summarizing the active queue state. Used
    variables/origin: Filters `Request` rows by `channel_pk` and `stream_id`,
    counting played/unplayed and priority/non-priority subsets. When no stream
    id exists, returns zeroed counters.
    """

    if stream_id is None:
        return {
            "total_unplayed": 0,
            "total_priority": 0,
            "total_nonpriority": 0,
            "total_played": 0,
        }

    base_query = db.query(Request).filter(
        Request.channel_id == channel_pk, Request.stream_id == stream_id
    )
    total_unplayed = base_query.filter(Request.played == 0).count()
    total_priority = base_query.filter(Request.played == 0, Request.is_priority == 1).count()
    total_nonpriority = base_query.filter(Request.played == 0, Request.is_priority == 0).count()
    total_played = base_query.filter(Request.played == 1).count()
    return {
        "total_unplayed": total_unplayed,
        "total_priority": total_priority,
        "total_nonpriority": total_nonpriority,
        "total_played": total_played,
    }


@app.get(
    "/channels/{channel}/queue/stats",
)
@app.get(
    "/channels/{channel}/queue/queue_stats",
)
def queue_stats(channel: str, db: Session = Depends(get_db)):
    """Return aggregate queue statistics for the active stream (public access).

    Dependencies: Uses the shared database session from `get_db` and the
    `current_stream` helper to scope counts. Code customers: dashboards wanting
    a single call for queue totals. Used variables/origin: Resolves `channel_pk`
    and forwards to `_queue_stats_for_stream` to gather counts for unplayed and
    played requests split by priority.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    return _queue_stats_for_stream(db, channel_pk, sid)


@app.get(
    "/channels/{channel}/queue/stats/total_priority",
)
def queue_total_priority(channel: str, db: Session = Depends(get_db)):
    """Expose only the unplayed priority count for the active stream (public).

    Dependencies: Shared FastAPI session dependency `get_db` plus `current_stream`.
    Code customers: lightweight widgets that only need the priority backlog size.
    Used variables/origin: Path `channel` resolves to `channel_pk`, which scopes
    `_queue_stats_for_stream` to return integer totals.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    stats = _queue_stats_for_stream(db, channel_pk, sid)
    return stats["total_priority"]


@app.get(
    "/channels/{channel}/queue/stats/total_nonpriority",
)
def queue_total_nonpriority(channel: str, db: Session = Depends(get_db)):
    """Expose only the unplayed non-priority count for the active stream (public).

    Dependencies: Database session from `get_db` and active stream lookup via
    `current_stream`. Code customers: counters that highlight the non-priority
    backlog size. Used variables/origin: Resolves `channel_pk` from the path and
    reads non-priority totals from `_queue_stats_for_stream`.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    stats = _queue_stats_for_stream(db, channel_pk, sid)
    return stats["total_nonpriority"]


@app.get(
    "/channels/{channel}/queue/stats/total_unplayed",
)
def queue_total_unplayed(channel: str, db: Session = Depends(get_db)):
    """Expose only the total unplayed queue count for the active stream (public).

    Dependencies: Database session via `get_db` and stream scoping via
    `current_stream`. Code customers: widgets needing the combined pending
    length. Used variables/origin: Resolves `channel_pk` then reads the
    aggregated unplayed count from `_queue_stats_for_stream`.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    stats = _queue_stats_for_stream(db, channel_pk, sid)
    return stats["total_unplayed"]


@app.get(
    "/channels/{channel}/queue/stats/total_played",
)
def queue_total_played(channel: str, db: Session = Depends(get_db)):
    """Expose only the played request count for the active stream (public).

    Dependencies: Shares the `get_db` session and `current_stream` lookup.
    Code customers: analytics views that chart how many songs have been played.
    Used variables/origin: Extracts `channel_pk` from the path and pulls the
    played total via `_queue_stats_for_stream`.
    """

    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    stats = _queue_stats_for_stream(db, channel_pk, sid)
    return stats["total_played"]

def _mark_state_change_no_cache(response: Response) -> None:
    """Add headers preventing caches from storing mutation responses.

    Dependencies: None beyond the provided `Response` object from FastAPI.
    Code customers: State-changing GET endpoints call this helper to protect
    against intermediary caching.
    Used variables/origin: Mutates the passed-in `response.headers` with
    `Cache-Control: no-store, max-age=0` and `Pragma: no-cache`.
    """

    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"


@app.api_route(
    "/channels/{channel}/queue/{request_id}/bump_admin",
    methods=["POST", "GET"],
    dependencies=[Depends(require_channel_key)],
)
def bump_admin(
    channel: str,
    request: FastAPIRequest,
    response: Response,
    request_id: str = Path(..., pattern=REQUEST_IDENTIFIER_PATTERN),
    db: Session = Depends(get_db),
):
    """Elevate a queue request to admin-driven priority status.

    Dependencies: Channel-key or admin auth via `require_channel_key`, database
    session from `get_db`, and request resolution through `resolve_queue_request`.
    Code customers: Queue manager UI actions and automation scripts that
    immediately promote a request.
    Used variables/origin: Path `channel` and `request_id` identify the record;
    `request.method` governs cache headers for GET; the injected `response`
    carries cache-busting headers when needed; `req.priority_source` tracks why
    the entry became priority.
    """

    if request.method == "GET":
        _mark_state_change_no_cache(response)
    channel_pk = get_channel_pk(channel, db)
    req = resolve_queue_request(db, channel_pk, request_id)
    req.is_priority = 1
    req.priority_source = 'admin'
    db.commit()
    db.refresh(req)
    payload = _serialize_request_event(db, req)
    publish_channel_event(channel_pk, "request.bumped", payload)
    publish_queue_changed(channel_pk)
    return {"success": True}

def _get_req(db, channel_pk: int, request_id: str | int):
    """Legacy helper to fetch a request by id; currently unused and slated for cleanup."""

    # TODO: remove or repurpose; superseded by resolve_queue_request.
    req = db.execute(
        select(Request).where(and_(Request.id == request_id,
                                        Request.channel_id == channel_pk))
    ).scalar_one_or_none()
    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    return req

@app.api_route(
    "/channels/{channel}/queue/{request_id}/move",
    methods=["POST", "GET"],
    dependencies=[Depends(require_channel_key)],
)
def move_request(
    channel: str,
    request: FastAPIRequest,
    response: Response,
    request_id: str = Path(..., pattern=REQUEST_IDENTIFIER_PATTERN),
    payload: Optional[MoveRequestIn] = Body(None),
    direction_query: Optional[str] = Query(
        None,
        alias="direction",
        description="Direction to move the request when using GET: 'up' or 'down'.",
    ),
    db: Session = Depends(get_db),
):
    """Reorder a queue entry relative to neighbors within the same stream.

    Dependencies: Channel-key/admin auth via `require_channel_key`, database
    session from `get_db`, and request resolution handled by
    `resolve_queue_request`.
    Code customers: Moderator tools invoking bump/skip behaviors and automated
    bots that adjust ordering.
    Used variables/origin: Path `channel`/`request_id` locate the entry; body
    `payload.direction` is honored for POST while `direction_query` is required
    for GET; `request.method` controls cache headers, with the injected
    `response` carrying the cache-busting headers.
    """

    if request.method == "GET":
        _mark_state_change_no_cache(response)
    allowed_directions = {"up", "down"}
    if request.method == "GET":
        direction = direction_query
    else:
        direction = payload.direction if payload else None
    if not direction:
        raise HTTPException(
            status_code=400,
            detail="direction is required (POST body or GET query parameter)",
        )
    if direction not in allowed_directions:
        raise HTTPException(status_code=400, detail="direction must be 'up' or 'down'")
    channel_pk = get_channel_pk(channel, db)
    req = resolve_queue_request(db, channel_pk, request_id)
    # find neighbor within same stream
    if direction == "up":
        neighbor = db.execute(
            select(Request).where(and_(
                Request.channel_id == channel_pk,
                Request.stream_id == req.stream_id,
                Request.played == 0,
                Request.position < req.position
            )).order_by(Request.position.desc()).limit(1)
        ).scalar_one_or_none()
    else:
        neighbor = db.execute(
            select(Request).where(and_(
                Request.channel_id == channel_pk,
                Request.stream_id == req.stream_id,
                Request.played == 0,
                Request.position > req.position
            )).order_by(Request.position.asc()).limit(1)
        ).scalar_one_or_none()
    if not neighbor:
        return {"success": True}  # nothing to move
    req.position, neighbor.position = neighbor.position, req.position
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.api_route(
    "/channels/{channel}/queue/{request_id}/skip",
    methods=["POST", "GET"],
    dependencies=[Depends(require_channel_key)],
)
def skip_request(
    channel: str,
    request: FastAPIRequest,
    response: Response,
    request_id: str = Path(..., pattern=REQUEST_IDENTIFIER_PATTERN),
    db: Session = Depends(get_db),
):
    """Send a pending request to the back of the queue for the current stream.

    Dependencies: Auth via `require_channel_key`, DB session from `get_db`, and
    queue lookup through `resolve_queue_request`.
    Code customers: Moderator skip controls and automated workflows that defer a
    request.
    Used variables/origin: Path parameters provide the target request; cache
    headers applied when `request.method` is GET; `max_pos` is derived from
    pending request ordering.
    """

    if request.method == "GET":
        _mark_state_change_no_cache(response)
    channel_pk = get_channel_pk(channel, db)
    req = resolve_queue_request(db, channel_pk, request_id)
    # move to bottom of pending
    max_pos = (
        db.query(func.coalesce(func.max(Request.position), 0))
        .filter(
            Request.channel_id == channel_pk,
            Request.stream_id == req.stream_id,
            Request.played == 0,
        )
        .scalar()
    )
    req.position = (max_pos or 0) + 1
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.api_route(
    "/channels/{channel}/queue/{request_id}/priority",
    methods=["POST", "GET"],
    dependencies=[Depends(require_channel_key)],
)
def set_priority(
    channel: str,
    request: FastAPIRequest,
    response: Response,
    request_id: str = Path(..., pattern=REQUEST_IDENTIFIER_PATTERN),
    enabled_body: Optional[bool] = Body(None, embed=True),
    enabled_query: Optional[bool] = Query(
        None, alias="enabled", description="Whether to enable priority when using GET"
    ),
    db: Session = Depends(get_db),
):
    """Toggle the priority flag for a queue request.

    Dependencies: Channel-key/admin auth via `require_channel_key`, DB session
    via `get_db`, and request resolution with `resolve_queue_request`.
    Code customers: Queue UI toggles, chat bot commands, and scheduled scripts
    that adjust priority eligibility.
    Used variables/origin: Path params target the request; POST primarily uses
    `enabled_body` (with query fallback) while GET requires `enabled_query`;
    `request.method` gates cache headers applied to the injected `response` for
    GET responses.
    """

    if request.method == "GET":
        _mark_state_change_no_cache(response)
    enabled_value: Optional[bool]
    if request.method == "GET":
        enabled_value = enabled_query
    else:
        enabled_value = enabled_body if enabled_body is not None else enabled_query
    if enabled_value is None:
        raise HTTPException(
            status_code=400,
            detail="enabled is required as a boolean (body for POST, query for GET)",
        )
    channel_pk = get_channel_pk(channel, db)
    req = resolve_queue_request(db, channel_pk, request_id)
    was_priority = bool(req.is_priority)
    # optional: refund or spend points can be inserted here
    req.is_priority = 1 if enabled_value else 0
    if req.is_priority and not req.priority_source:
        req.priority_source = 'admin'
    db.commit()
    db.refresh(req)
    if req.is_priority and not was_priority:
        payload = _serialize_request_event(db, req)
        publish_channel_event(channel_pk, "request.bumped", payload)
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.api_route(
    "/channels/{channel}/queue/{request_id}/played",
    methods=["POST", "GET"],
    dependencies=[Depends(require_channel_key)],
)
def mark_played(
    channel: str,
    request: FastAPIRequest,
    response: Response,
    request_id: str = Path(..., pattern=REQUEST_IDENTIFIER_PATTERN),
    db: Session = Depends(get_db),
):
    """Mark a queue entry as played and broadcast the next-up entry.

    Dependencies: Channel-key/admin auth enforced by `require_channel_key`, DB
    session via `get_db`, and request resolution handled by
    `resolve_queue_request`.
    Code customers: Playback UIs, overlays, and automation that finalize song
    requests.
    Used variables/origin: Path parameters locate the request; GET responses
    receive cache-busting headers; `req.played` persists the state change and
    `up_next` is derived via `_next_pending_request`.
    """

    if request.method == "GET":
        _mark_state_change_no_cache(response)
    channel_pk = get_channel_pk(channel, db)
    req = resolve_queue_request(db, channel_pk, request_id)
    req.played = 1
    # optionally push it out of visible order by setting a sentinel position
    db.commit()
    db.refresh(req)
    payload = _serialize_request_event(db, req)
    up_next = _next_pending_request(db, channel_pk, req.stream_id)
    publish_channel_event(
        channel_pk,
        "request.played",
        {
            "request": payload,
            "up_next": up_next,
        },
    )
    publish_queue_changed(channel_pk)
    return {"success": True}

# =====================================
# Event logging helpers
# =====================================

def _persist_channel_event(db: Session, channel_pk: int, payload: EventIn) -> Event:
    """Persist and reward a channel event in a single reusable helper.

    Dependencies: Requires an active database ``Session`` plus the existing
    channel settings fetched via ``get_or_create_settings`` to compute reward
    points. ``award_prio_points`` handles the balance mutations when a user is
    eligible. ``publish_queue_changed`` broadcasts state updates so overlays and
    pricing displays stay current.
    Code customers: The `/channels/{channel}/events` endpoint and the Twitch
    EventSub webhook both call this helper to ensure consistent reward handling
    regardless of the ingress path.
    Used variables/origin: ``channel_pk`` targets the owning channel; ``payload``
    supplies the event type, ``user_id`` to reward (internal DB identifier), and
    the free-form ``meta`` dictionary used for bits, gifts, and tier data.
    """

    meta = payload.meta or {}
    meta_str = json.dumps(meta)
    ev = Event(channel_id=channel_pk, event_type=payload.type, user_id=payload.user_id, meta=meta_str)
    db.add(ev)
    db.commit()

    points = 0
    settings = get_or_create_settings(db, channel_pk)
    if payload.type == "follow" and settings.prio_follow_enabled:
        points = 1
    elif payload.type == "raid" and settings.prio_raid_enabled:
        points = 1
    elif payload.type == "gift_sub":
        count = max(_coerce_int(meta.get("count"), default=1), 0)
        tier_points = _priority_points_for_tier(settings, meta.get("tier"))
        gift_threshold = max(settings.prio_gifts_per_point or 0, 0)
        threshold_points = count // gift_threshold if gift_threshold else 0
        points = threshold_points + (tier_points * count)
    elif payload.type == "bits":
        amount = max(_coerce_int(meta.get("amount"), default=0), 0)
        per_point = max(settings.prio_bits_per_point or 0, 0)
        points = amount // per_point if per_point else 0
    elif payload.type == "sub":
        count = max(_coerce_int(meta.get("count"), default=1), 1)
        tier_points = _priority_points_for_tier(settings, meta.get("tier"))
        points = tier_points * count

    if payload.user_id and points > 0:
        try:
            award_prio_points(db, channel_pk, payload.user_id, points)
            logger.info(
                "Awarded %s priority points to user %s for %s in channel %s",
                points,
                payload.user_id,
                payload.type,
                channel_pk,
            )
        except HTTPException:
            logger.info("Skipping award for event %s in channel %s", payload.type, channel_pk)
    else:
        logger.debug(
            "Recorded event %s for channel %s with payload %s but no reward points",
            payload.type,
            channel_pk,
            meta,
        )
    publish_queue_changed(channel_pk)
    return ev

# =====================================
# Routes: EventSub
# =====================================

@app.post("/twitch/eventsub/callback", name="eventsub_callback")
async def eventsub_callback(request: FastAPIRequest, db: Session = Depends(get_db)):
    """Handle Twitch EventSub verification pings and live notifications.

    Dependencies: Uses the request body and headers to validate EventSub HMAC
    signatures via ``_verify_eventsub_signature`` and persists state with the
    injected database ``Session``. Event persistence flows through
    ``_process_eventsub_notification`` for reward calculations.
    Code customers: Twitch's EventSub delivery system posts challenges and event
    notifications to this endpoint whenever pricing-relevant actions occur.
    Used variables/origin: Reads the Twitch headers (`Twitch-Eventsub-*`) for
    signature and message metadata plus the JSON payload to identify the
    subscription record and event content.
    """

    body = await request.body()
    headers = request.headers
    message_id = headers.get("Twitch-Eventsub-Message-Id") or ""
    timestamp = headers.get("Twitch-Eventsub-Message-Timestamp") or ""
    signature = headers.get("Twitch-Eventsub-Message-Signature") or ""
    message_type = headers.get("Twitch-Eventsub-Message-Type") or ""

    if not message_id or not timestamp or not signature:
        raise HTTPException(status_code=400, detail="missing signature headers")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON")

    sub_info = payload.get("subscription") or {}
    sub_id = sub_info.get("id")
    if not sub_id:
        raise HTTPException(status_code=400, detail="subscription id missing")

    subscription = (
        db.query(EventSubscription)
        .filter(EventSubscription.twitch_subscription_id == sub_id)
        .one_or_none()
    )
    if not subscription:
        logger.warning("Ignoring callback for unknown EventSub %s", sub_id)
        return JSONResponse(status_code=202, content={"detail": "unknown subscription"})

    if not _verify_eventsub_signature(subscription.secret, message_id, timestamp, body, signature):
        logger.warning("EventSub signature mismatch for %s", sub_id)
        raise HTTPException(status_code=403, detail="invalid signature")

    subscription.status = sub_info.get("status") or subscription.status
    subscription.updated_at = datetime.utcnow()
    db.flush()

    if message_type == "webhook_callback_verification":
        subscription.last_verified_at = datetime.utcnow()
        db.commit()
        challenge = payload.get("challenge") or ""
        return Response(content=challenge, media_type="text/plain")

    if message_type == "notification":
        _process_eventsub_notification(db, subscription, payload)
        return JSONResponse({"success": True})

    db.commit()
    return JSONResponse({"detail": "ignored"})

# =====================================
# Routes: Events
# =====================================
@app.post("/channels/{channel}/events", response_model=dict, dependencies=[Depends(require_channel_key)])
def log_event(channel: str, payload: EventIn, db: Session = Depends(get_db)):
    """Record a channel event and apply configured priority point rewards.

    Dependencies: Uses the database ``Session`` for persistence and
    ``get_or_create_settings`` to read reward knobs. ``award_prio_points``
    handles balance updates with caps.
    Code customers: Webhooks or bot workers forward Twitch events here so
    overlays and queues stay in sync.
    Used variables/origin: ``payload`` supplies the event type, optional
    ``user_id`` to reward, and metadata such as gift counts, bits amounts, or
    subscription tiers.
    """

    channel_pk = get_channel_pk(channel, db)
    ev = _persist_channel_event(db, channel_pk, payload)
    return {"event_id": ev.id}

@app.get("/channels/{channel}/events", response_model=List[EventOut])
def list_events(channel: str, type: Optional[str] = None, since: Optional[str] = None, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    q = db.query(Event).filter(Event.channel_id == channel_pk)
    if type:
        q = q.filter(Event.event_type == type)
    if since:
        try:
            dt = datetime.fromisoformat(since)
            q = q.filter(Event.event_time >= dt)
        except ValueError:
            raise HTTPException(400, detail="invalid since timestamp")
    events = q.order_by(Event.event_time.desc()).all()
    result: list[dict[str, Any]] = []
    for ev in events:
        data = EventOut.model_validate(ev).model_dump(by_alias=True)
        data.setdefault("event_type", data.get("type"))
        result.append(data)
    return result


@app.get("/channels/{channel}/eventsub/health", response_model=Dict[str, Any], dependencies=[Depends(require_token)])
def eventsub_health(channel: str, db: Session = Depends(get_db)):
    """Summarize EventSub subscription state for diagnostics.

    Dependencies: Enforces admin/OAuth access via ``require_token`` and uses the
    database ``Session`` to read persisted subscriptions. Remote status is
    pulled from Twitch through ``_fetch_remote_eventsubs`` for comparison.
    Code customers: Operations dashboards and support tooling call this endpoint
    to verify that follow/raid/cheer/subscription hooks are active.
    Used variables/origin: ``channel`` path param resolves the broadcaster; the
    response includes local subscription rows (type, status, timestamps) and any
    remote data Twitch returns.
    """

    channel_pk = get_channel_pk(channel, db)
    channel_obj = db.get(ActiveChannel, channel_pk)
    if not channel_obj:
        raise HTTPException(status_code=404, detail="channel not found")
    local = [
        {
            "type": sub.type,
            "status": sub.status,
            "last_verified_at": sub.last_verified_at,
            "last_notified_at": sub.last_notified_at,
            "callback": sub.callback,
        }
        for sub in db.query(EventSubscription).filter(EventSubscription.channel_id == channel_pk)
    ]
    remote = _fetch_remote_eventsubs(channel_obj)
    if not remote:
        logger.info("Remote EventSub data unavailable for %s; check token/scopes", channel)
    return {"local": local, "remote": remote}

# =====================================
# Routes: Streams
# =====================================
@app.get("/channels/{channel}/streams", response_model=List[StreamOut])
def list_streams(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    return (
        db.query(StreamSession)
        .filter(StreamSession.channel_id == channel_pk)
        .order_by(StreamSession.started_at.asc())
        .all()
    )

@app.post("/channels/{channel}/streams/start", response_model=dict, dependencies=[Depends(require_channel_key)])
def start_stream(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    return {"stream_id": sid}

@app.post("/channels/{channel}/streams/archive", response_model=dict, dependencies=[Depends(require_channel_key)])
def archive_stream(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    # close current
    cur = (
        db.query(StreamSession)
        .filter(StreamSession.channel_id == channel_pk, StreamSession.ended_at.is_(None))
        .one_or_none()
    )
    now = datetime.utcnow()
    archived_stream_id: Optional[int] = None
    if cur:
        archived_stream_id = cur.id
        cur.ended_at = now
        db.commit()
    # start new
    new_sid = current_stream(db, channel_pk)
    publish_channel_event(
        channel_pk,
        "queue.archived",
        {
            "archived_stream_id": archived_stream_id,
            "new_stream_id": new_sid,
        },
    )
    publish_queue_changed(channel_pk)
    return {"new_stream_id": new_sid}

# =====================================
# Routes: Stats
# =====================================
@app.get("/channels/{channel}/stats/general")
def stats_general(channel: str, since: Optional[str] = None, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    rq = db.query(Request).filter(Request.channel_id == channel_pk, Request.stream_id == sid)
    if since:
        try:
            dt = datetime.fromisoformat(since)
            rq = rq.filter(Request.request_time >= dt)
        except ValueError:
            raise HTTPException(400, detail="invalid since timestamp")
    total_requests = rq.count()
    unique_songs = rq.with_entities(Request.song_id).distinct().count()
    unique_users = rq.with_entities(Request.user_id).distinct().count()
    return {"total_requests": total_requests, "unique_songs": unique_songs, "unique_users": unique_users}

@app.get("/channels/{channel}/stats/songs")
def stats_top_songs(channel: str, top: int = 10, since: Optional[str] = None, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    rq = db.query(Request.song_id, func.count(Request.id).label("cnt")).\
        filter(Request.channel_id == channel_pk, Request.stream_id == sid).group_by(Request.song_id)
    if since:
        try:
            dt = datetime.fromisoformat(since)
            rq = rq.filter(Request.request_time >= dt)
        except ValueError:
            raise HTTPException(400, detail="invalid since timestamp")
    rows = rq.order_by(func.count(Request.id).desc()).limit(top).all()
    return [{"song_id": r[0], "count": r[1]} for r in rows]

@app.get("/channels/{channel}/stats/users")
def stats_top_users(channel: str, top: int = 10, since: Optional[str] = None, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    rq = db.query(Request.user_id, func.count(Request.id).label("cnt")).\
        filter(Request.channel_id == channel_pk, Request.stream_id == sid).group_by(Request.user_id)
    if since:
        try:
            dt = datetime.fromisoformat(since)
            rq = rq.filter(Request.request_time >= dt)
        except ValueError:
            raise HTTPException(400, detail="invalid since timestamp")
    rows = rq.order_by(func.count(Request.id).desc()).limit(top).all()
    return [{"user_id": r[0], "count": r[1]} for r in rows]
