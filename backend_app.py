from __future__ import annotations
from typing import Optional, List, Any, Dict, Mapping, Iterable, Literal
import os
import json
import time
import hmac
import hashlib
import logging
import re
import secrets
import html
from urllib.parse import quote, urlparse, urlunparse
from datetime import datetime, timedelta
import asyncio
import requests
from sse_starlette.sse import EventSourceResponse
from fastapi.middleware.cors import CORSMiddleware

from fastapi import (
    FastAPI,
    HTTPException,
    Depends,
    Header,
    Query,
    APIRouter,
    Request as FastAPIRequest,
    Response,
    Cookie,
    Body,
)
from fastapi.responses import RedirectResponse, HTMLResponse
from starlette.datastructures import URL
from pydantic import BaseModel, Field
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, Boolean,
    ForeignKey, UniqueConstraint, func, select, and_
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session
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

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_REDIRECT_URI = os.getenv("TWITCH_REDIRECT_URI")
BOT_TWITCH_REDIRECT_URI = os.getenv("BOT_TWITCH_REDIRECT_URI")
TWITCH_SCOPES = os.getenv(
    "TWITCH_SCOPES", "channel:bot channel:read:subscriptions channel:read:vips"
).split()
BOT_NICK = os.getenv("BOT_NICK")

BOT_APP_SCOPES = os.getenv(
    "BOT_APP_SCOPES", "user:read:chat user:write:chat user:bot"
).split()

ADMIN_SESSION_COOKIE = "admin_oauth_token"

APP_ACCESS_TOKEN: Optional[str] = None
APP_TOKEN_EXPIRES = 0
BOT_USER_ID: Optional[str] = None

_bot_log_listeners: set[asyncio.Queue[str]] = set()
_bot_oauth_states: dict[str, Dict[str, Any]] = {}

logger = logging.getLogger(__name__)

engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


def get_app_access_token() -> str:
    global APP_ACCESS_TOKEN, APP_TOKEN_EXPIRES
    if not APP_ACCESS_TOKEN or time.time() > APP_TOKEN_EXPIRES:
        response = requests.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
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
    login = BOT_NICK
    if not login:
        db = SessionLocal()
        try:
            cfg = db.query(BotConfig).order_by(BotConfig.id.asc()).first()
            if cfg and cfg.login:
                login = cfg.login
        finally:
            db.close()
    if not login:
        return None
    token = get_app_access_token()
    headers = {"Authorization": f"Bearer {token}", "Client-Id": TWITCH_CLIENT_ID}
    resp = requests.get(
        "https://api.twitch.tv/helix/users",
        params={"login": login},
        headers=headers,
    ).json()
    data = resp.get("data", [])
    if data:
        BOT_USER_ID = data[0]["id"]
    return BOT_USER_ID


# =====================================
# Models
# =====================================
class ActiveChannel(Base):
    __tablename__ = "active_channels"
    id = Column(Integer, primary_key=True)
    channel_id = Column(String, unique=True, nullable=False)  # Twitch channel ID
    channel_name = Column(String, nullable=False)
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
    other_flags = Column(Text)
    max_prio_points = Column(Integer, default=10)

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

# =====================================
# DB bootstrap
# =====================================
Base.metadata.create_all(bind=engine)

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


class ChannelOAuthOut(BaseModel):
    channel_name: str
    authorized: bool
    owner_login: Optional[str] = None
    scopes: List[str] = Field(default_factory=list)

class ChannelSettingsIn(BaseModel):
    max_requests_per_user: int = -1
    prio_only: int = 0
    queue_closed: int = 0
    allow_bumps: int = 1
    other_flags: Optional[str] = None
    max_prio_points: int = 10

class ChannelSettingsOut(ChannelSettingsIn):
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
    type: str = Field(serialization_alias="event_type")
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
app = FastAPI(title="Twitch Song Request Backend", version="1.0.0")

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

_brokers: dict[int, set[asyncio.Queue[str]]] = {}


def _broker_queues(channel_pk: int) -> set[asyncio.Queue[str]]:
    return _brokers.setdefault(channel_pk, set())


def _subscribe_queue(channel_pk: int) -> asyncio.Queue[str]:
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)
    _broker_queues(channel_pk).add(queue)
    return queue


def _unsubscribe_queue(channel_pk: int, queue: asyncio.Queue[str]) -> None:
    listeners = _brokers.get(channel_pk)
    if not listeners:
        return
    listeners.discard(queue)
    if not listeners:
        _brokers.pop(channel_pk, None)


def publish_queue_changed(channel_pk: int) -> None:
    """Notify listeners that the active queue for a channel changed."""
    listeners = _brokers.get(channel_pk)
    if not listeners:
        return
    stale: list[asyncio.Queue[str]] = []
    for queue in list(listeners):
        try:
            queue.put_nowait("changed")
        except asyncio.QueueFull:
            stale.append(queue)
            logger.warning("queue change notification dropped for channel %s", channel_pk)
        except Exception:
            stale.append(queue)
            logger.exception(
                "failed to enqueue queue change notification for channel %s",
                channel_pk,
            )
    if stale:
        for queue in stale:
            listeners.discard(queue)
        if not listeners:
            _brokers.pop(channel_pk, None)


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
    if BOT_TWITCH_REDIRECT_URI:
        return BOT_TWITCH_REDIRECT_URI
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
    required = _normalize_scope_list(BOT_APP_SCOPES)
    missing = [scope for scope in required if scope not in current]
    if missing:
        current.extend(missing)
        cfg.scopes = " ".join(current) if current else None
        return True
    return False


def _get_bot_config(db: Session) -> BotConfig:
    cfg = db.query(BotConfig).order_by(BotConfig.id.asc()).first()
    if not cfg:
        default_scopes = _normalize_scope_list(BOT_APP_SCOPES)
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
    }
    if include_tokens:
        data["access_token"] = cfg.access_token
        data["refresh_token"] = cfg.refresh_token
        data["client_id"] = TWITCH_CLIENT_ID
        data["client_secret"] = TWITCH_CLIENT_SECRET
        try:
            data["bot_user_id"] = get_bot_user_id()
        except requests.RequestException:
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
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Twitch OAuth not configured")
    scope = "+".join(TWITCH_SCOPES)
    redirect_uri = TWITCH_REDIRECT_URI or str(request.url_for("auth_callback"))
    state_payload = {"channel": channel}
    if return_url:
        state_payload["return_url"] = return_url
    state_param = quote(json.dumps(state_payload, separators=(",", ":")), safe="")
    url = (
        "https://id.twitch.tv/oauth2/authorize"
        f"?response_type=code&client_id={TWITCH_CLIENT_ID}"
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
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Twitch OAuth not configured")
    redirect_uri = TWITCH_REDIRECT_URI or str(request.url_for("auth_callback"))
    token_resp = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
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
    headers = {"Authorization": f"Bearer {access_token}", "Client-Id": TWITCH_CLIENT_ID}
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
            join_active=1,
            authorized=True,
            owner_id=user.id,
        )
        db.add(ch)
    else:
        ch.owner_id = user.id
        ch.authorized = True
    db.commit()
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
    owned = db.query(ActiveChannel).filter_by(owner_id=current.id).all()
    for channel in owned:
        db.delete(channel)
    db.query(ChannelModerator).filter_by(user_id=current.id).delete()
    db.delete(current)
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
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Twitch OAuth not configured")
    cfg = _get_bot_config(db)
    configured_scopes = (cfg.scopes or "").split()
    seen: set[str] = set()
    scopes: list[str] = []
    for scope in configured_scopes or BOT_APP_SCOPES:
        scope_value = scope.strip()
        if scope_value and scope_value not in seen:
            seen.add(scope_value)
            scopes.append(scope_value)
    if not scopes:
        scopes = BOT_APP_SCOPES[:]
    redirect_uri = _bot_redirect_uri(request)
    nonce = secrets.token_urlsafe(24)
    _cleanup_bot_oauth_states()
    return_url = _normalize_return_url(payload.return_url) if payload else None
    state_payload: Dict[str, Any] = {"nonce": nonce}
    if return_url:
        state_payload["return_url"] = return_url
    state_param = quote(json.dumps(state_payload, separators=(",", ":")), safe="")
    scope_param = quote(" ".join(scopes), safe="")
    client_id_param = quote(TWITCH_CLIENT_ID, safe="")
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
        if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
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
        expected_scopes = pending.get("scopes") or BOT_APP_SCOPES
        redirect_uri = _bot_redirect_uri(request)
        try:
            token_response = requests.post(
                "https://id.twitch.tv/oauth2/token",
                data={
                    "client_id": TWITCH_CLIENT_ID,
                    "client_secret": TWITCH_CLIENT_SECRET,
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
            "Client-Id": TWITCH_CLIENT_ID,
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
    if TWITCH_CLIENT_ID and current.access_token:
        try:
            headers = {
                "Authorization": f"Bearer {current.access_token}",
                "Client-Id": TWITCH_CLIENT_ID,
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
    st = db.query(ChannelSettings).filter(ChannelSettings.channel_id == channel_pk).one_or_none()
    if not st:
        st = ChannelSettings(channel_id=channel_pk)
        db.add(st)
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


def current_stream(db: Session, channel_pk: int) -> int:
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


def award_prio_points(db: Session, channel_pk: int, user_id: int, delta: int):
    user = db.query(User).filter(User.id == user_id, User.channel_id == channel_pk).one_or_none()
    if not user:
        raise HTTPException(404, detail="user not found in channel")
    settings = get_or_create_settings(db, channel_pk)
    cap = settings.max_prio_points or 10
    new_val = min(cap, (user.prio_points or 0) + delta)
    user.prio_points = new_val
    db.commit()


def enforce_queue_limits(db: Session, channel_pk: int, user_id: int, want_priority: bool):
    settings = get_or_create_settings(db, channel_pk)
    if settings.queue_closed:
        raise HTTPException(409, detail="queue closed")
    if settings.prio_only and not want_priority:
        raise HTTPException(409, detail="priority requests only")
    if settings.max_requests_per_user and settings.max_requests_per_user >= 0:
        stream_id = current_stream(db, channel_pk)
        count = (
            db.query(Request)
            .filter(Request.channel_id == channel_pk,
                    Request.stream_id == stream_id,
                    Request.user_id == user_id,
                    Request.played == 0)
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


seed_default_data()

# =====================================
# Routes: System
# =====================================
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

@app.post("/channels", response_model=ChannelOut, dependencies=[Depends(require_token)])
def add_channel(payload: ChannelIn, db: Session = Depends(get_db)):
    ch = ActiveChannel(channel_id=payload.channel_id, channel_name=payload.channel_name, join_active=payload.join_active)
    db.add(ch)
    db.commit()
    db.refresh(ch)
    get_or_create_settings(db, ch.id)
    get_or_create_bot_state(db, ch.id)
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
        other_flags=st.other_flags,
        max_prio_points=st.max_prio_points,
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

@app.put("/channels/{channel}/settings", dependencies=[Depends(require_token)])
def set_channel_settings(channel: str, payload: ChannelSettingsIn, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    st = get_or_create_settings(db, channel_pk)
    st.max_requests_per_user = payload.max_requests_per_user
    st.prio_only = payload.prio_only
    st.queue_closed = payload.queue_closed
    st.allow_bumps = payload.allow_bumps
    st.other_flags = payload.other_flags
    st.max_prio_points = payload.max_prio_points
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

# =====================================
# Routes: Songs
# =====================================
@app.get("/channels/{channel}/songs", response_model=List[SongOut])
def search_songs(channel: str, search: Optional[str] = Query(None), db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    q = db.query(Song).filter(Song.channel_id == channel_pk)
    if search:
        like = f"%{search}%"
        q = q.filter((Song.artist.ilike(like)) | (Song.title.ilike(like)))
    return q.order_by(Song.artist.asc(), Song.title.asc()).all()

@app.post("/channels/{channel}/songs", response_model=dict, dependencies=[Depends(require_token)])
def add_song(channel: str, payload: SongIn, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    song = Song(channel_id=channel_pk, **payload.model_dump())
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
    if not channel_obj or not channel_obj.owner or not channel_obj.channel_id:
        return set(), {}
    owner = channel_obj.owner
    if not TWITCH_CLIENT_ID or not owner.access_token:
        return set(), {}
    headers = {
        "Authorization": f"Bearer {owner.access_token}",
        "Client-Id": TWITCH_CLIENT_ID,
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


@app.get("/channels/{channel}/queue/full", response_model=List[QueueItemFull])
def get_queue_full(
    channel: str,
    current: TwitchUser = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    channel_pk = get_channel_pk(channel, db)
    if not _user_has_access(current, channel_pk, db):
        raise HTTPException(status_code=403, detail="not authorized for channel")
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
    vip_ids, subs = _collect_channel_roles(channel_obj)
    result: list[QueueItemFull] = []
    for row in rows:
        song = songs.get(row.song_id)
        user = users.get(row.user_id)
        if not song or not user:
            continue
        base_user = UserOut.model_validate(user)
        user_payload = UserWithRoles(
            **base_user.model_dump(),
            is_vip=user.twitch_id in vip_ids,
            is_subscriber=user.twitch_id in subs,
            subscriber_tier=subs.get(user.twitch_id),
        )
        result.append(
            QueueItemFull(
                request=RequestOut.model_validate(row),
                song=SongOut.model_validate(song),
                user=user_payload,
            )
        )
    return result


@app.get("/channels/{channel}/queue/stream")
async def stream_queue(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    q = _subscribe_queue(channel_pk)

    async def gen():
        # initial tick so clients render immediately
        try:
            yield "event: queue\ndata: init\n\n"
            while True:
                msg = await q.get()
                yield f"event: queue\ndata: {msg}\n\n"
        finally:
            _unsubscribe_queue(channel_pk, q)

    return EventSourceResponse(
        gen(),
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
        },
    )

@app.get("/channels/{channel}/queue", response_model=List[RequestOut])
def get_queue(channel: str, db: Session = Depends(get_db)):
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

@app.post("/channels/{channel}/queue", response_model=dict, dependencies=[Depends(require_token)])
def add_request(channel: str, payload: RequestCreate, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    # Checks
    enforce_queue_limits(db, channel_pk, payload.user_id, payload.want_priority)
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

    is_priority = 0
    priority_source = None

    if payload.want_priority:
        if payload.prefer_sub_free and try_use_sub_free(db, payload.user_id, sid, payload.is_subscriber):
            is_priority = 1
            priority_source = 'sub_free'
        else:
            u = db.get(User, payload.user_id)
            if not u:
                raise HTTPException(404, "user not found")
            if (u.prio_points or 0) > 0:
                u.prio_points -= 1
                is_priority = 1
                priority_source = 'points'
                db.commit()
                publish_queue_changed(channel_pk)
            else:
                raise HTTPException(409, detail="No priority available")

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

    publish_queue_changed(channel_pk)
    return {"request_id": req.id}

@app.put("/channels/{channel}/queue/{request_id}", dependencies=[Depends(require_token)])
def update_request(channel: str, request_id: int, payload: RequestUpdate, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    r = db.query(Request).filter(Request.id == request_id, Request.channel_id == channel_pk).one_or_none()
    if not r:
        raise HTTPException(404, "request not found")
    if payload.played is not None:
        r.played = 1 if payload.played else 0
        if r.played:
            # Update song stats
            s = db.get(Song, r.song_id)
            now = datetime.utcnow()
            if s:
                if not s.date_first_played:
                    s.date_first_played = now
                s.date_last_played = now
                s.total_played = (s.total_played or 0) + 1
    if payload.bumped is not None:
        r.bumped = 1 if payload.bumped else 0
    if payload.is_priority is not None:
        r.is_priority = 1 if payload.is_priority else 0
        if r.is_priority and not r.priority_source:
            r.priority_source = 'admin'
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.delete("/channels/{channel}/queue/{request_id}", dependencies=[Depends(require_token)])
def remove_request(channel: str, request_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    r = db.query(Request).filter(Request.id == request_id, Request.channel_id == channel_pk).one_or_none()
    if not r:
        raise HTTPException(404, "request not found")
    db.delete(r)
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.post("/channels/{channel}/queue/clear", dependencies=[Depends(require_token)])
def clear_queue(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    db.query(Request).filter(Request.channel_id == channel_pk, Request.stream_id == sid, Request.played == 0).delete()
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.get("/channels/{channel}/queue/random_nonpriority")
def random_nonpriority(channel: str, db: Session = Depends(get_db)):
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

@app.post("/channels/{channel}/queue/{request_id}/bump_admin", dependencies=[Depends(require_token)])
def bump_admin(channel: str, request_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    r = db.query(Request).filter(Request.id == request_id, Request.channel_id == channel_pk).one_or_none()
    if not r:
        raise HTTPException(404, "request not found")
    r.is_priority = 1
    r.priority_source = 'admin'
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

def _get_req(db, channel_pk: int, request_id: int):
    req = db.execute(
        select(Request).where(and_(Request.id == request_id,
                                        Request.channel_id == channel_pk))
    ).scalar_one_or_none()
    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    return req

@app.post("/channels/{channel}/queue/{request_id}/move", dependencies=[Depends(require_token)])
def move_request(channel: str, request_id: int, payload: MoveRequestIn, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    direction = payload.direction
    req = _get_req(db, channel_pk, request_id)
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

@app.post("/channels/{channel}/queue/{request_id}/skip", dependencies=[Depends(require_token)])
def skip_request(channel: str, request_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    req = _get_req(db, channel_pk, request_id)
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

@app.post("/channels/{channel}/queue/{request_id}/priority", dependencies=[Depends(require_token)])
def set_priority(channel: str, request_id: int, enabled: bool, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    req = _get_req(db, channel_pk, request_id)
    # optional: refund or spend points can be inserted here
    req.is_priority = 1 if enabled else 0
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

@app.post("/channels/{channel}/queue/{request_id}/played", dependencies=[Depends(require_token)])
def mark_played(channel: str, request_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    req = _get_req(db, channel_pk, request_id)
    req.played = 1
    # optionally push it out of visible order by setting a sentinel position
    db.commit()
    publish_queue_changed(channel_pk)
    return {"success": True}

# =====================================
# Routes: Events
# =====================================
@app.post("/channels/{channel}/events", response_model=dict, dependencies=[Depends(require_token)])
def log_event(channel: str, payload: EventIn, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    meta = payload.meta or {}
    meta_str = json.dumps(meta)
    ev = Event(channel_id=channel_pk, event_type=payload.type, user_id=payload.user_id, meta=meta_str)
    db.add(ev)
    db.commit()

    # Award points based on rules
    if payload.type in {"follow", "raid"}:
        if payload.user_id:
            award_prio_points(db, channel_pk, payload.user_id, 1)
    elif payload.type == "gift_sub":
        # metadata expects {"count": N}
        count = int(meta.get("count", 1))
        points = count // 5
        if payload.user_id and points > 0:  # gifter
            award_prio_points(db, channel_pk, payload.user_id, count)
    elif payload.type == "bits":
        amount = int(meta.get("amount", 0))
        if payload.user_id and amount >= 200:
            award_prio_points(db, channel_pk, payload.user_id, 1)
    elif payload.type == "sub":
        # no automatic points; handled via free-per-stream when requesting
        pass
    publish_queue_changed(channel_pk)
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
    return q.order_by(Event.event_time.desc()).all()

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

@app.post("/channels/{channel}/streams/start", response_model=dict, dependencies=[Depends(require_token)])
def start_stream(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    return {"stream_id": sid}

@app.post("/channels/{channel}/streams/archive", response_model=dict, dependencies=[Depends(require_token)])
def archive_stream(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    # close current
    cur = (
        db.query(StreamSession)
        .filter(StreamSession.channel_id == channel_pk, StreamSession.ended_at.is_(None))
        .one_or_none()
    )
    now = datetime.utcnow()
    if cur:
        cur.ended_at = now
        db.commit()
    # start new
    new_sid = current_stream(db, channel_pk)
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
