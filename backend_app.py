from __future__ import annotations
from typing import Optional, List, Any
import os
import json
import time
import hmac
import hashlib
from datetime import datetime
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
)
from pydantic import BaseModel, Field
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, Boolean,
    ForeignKey, UniqueConstraint, func, select, and_, text
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session

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
TWITCH_REDIRECT_URI = os.getenv("TWITCH_REDIRECT_URI", "http://localhost:8000/auth/callback")
TWITCH_SCOPES = os.getenv("TWITCH_SCOPES", "chat:read chat:edit channel:bot").split()
TWITCH_EVENTSUB_CALLBACK = os.getenv("TWITCH_EVENTSUB_CALLBACK", "http://localhost:8000/eventsub/callback")
TWITCH_EVENTSUB_SECRET = os.getenv("TWITCH_EVENTSUB_SECRET", "change-me")
BOT_NICK = os.getenv("BOT_NICK")

APP_ACCESS_TOKEN: Optional[str] = None
APP_TOKEN_EXPIRES = 0
BOT_USER_ID: Optional[str] = None

engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


def get_app_access_token() -> str:
    global APP_ACCESS_TOKEN, APP_TOKEN_EXPIRES
    if not APP_ACCESS_TOKEN or time.time() > APP_TOKEN_EXPIRES:
        resp = requests.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
                "grant_type": "client_credentials",
            },
        ).json()
        APP_ACCESS_TOKEN = resp["access_token"]
        APP_TOKEN_EXPIRES = time.time() + resp.get("expires_in", 3600) - 60
    return APP_ACCESS_TOKEN


def get_bot_user_id() -> Optional[str]:
    global BOT_USER_ID
    if BOT_USER_ID:
        return BOT_USER_ID
    if not BOT_NICK:
        return None
    token = get_app_access_token()
    headers = {"Authorization": f"Bearer {token}", "Client-Id": TWITCH_CLIENT_ID}
    resp = requests.get(
        "https://api.twitch.tv/helix/users",
        params={"login": BOT_NICK},
        headers=headers,
    ).json()
    data = resp.get("data", [])
    if data:
        BOT_USER_ID = data[0]["id"]
    return BOT_USER_ID


def subscribe_chat_eventsub(broadcaster_id: str) -> None:
    token = get_app_access_token()
    bot_id = get_bot_user_id()
    if not bot_id:
        return
    headers = {
        "Authorization": f"Bearer {token}",
        "Client-Id": TWITCH_CLIENT_ID,
        "Content-Type": "application/json",
    }
    payload = {
        "type": "channel.chat.message",
        "version": "1",
        "condition": {
            "broadcaster_user_id": broadcaster_id,
            "user_id": bot_id,
        },
        "transport": {
            "method": "webhook",
            "callback": TWITCH_EVENTSUB_CALLBACK,
            "secret": TWITCH_EVENTSUB_SECRET,
        },
    }
    requests.post(
        "https://api.twitch.tv/helix/eventsub/subscriptions",
        headers=headers,
        data=json.dumps(payload),
    )


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

    class Config:
        from_attributes = True

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

class ChannelAccessOut(BaseModel):
    channel_name: str
    role: str

class ModIn(BaseModel):
    twitch_id: str
    username: str

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

_brokers: dict[int, asyncio.Queue[str]] = {}

def _broker(channel_pk: int) -> asyncio.Queue[str]:
    return _brokers.setdefault(channel_pk, asyncio.Queue(maxsize=1000))

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(authorization: str = Header(None), db: Session = Depends(get_db)) -> TwitchUser:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1]
    user = db.query(TwitchUser).filter_by(access_token=token).one_or_none()
    if user:
        return user
    resp = requests.get(
        "https://id.twitch.tv/oauth2/validate",
        headers={"Authorization": f"OAuth {token}"},
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="invalid token")
    login = resp.json().get("login")
    if not login:
        raise HTTPException(status_code=401, detail="invalid token")
    user = db.query(TwitchUser).filter(func.lower(TwitchUser.username) == login.lower()).one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="invalid token")
    user.access_token = token
    db.commit()
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

def require_token(channel: Optional[str] = None, x_admin_token: str = Header(None), authorization: str = Header(None), db: Session = Depends(get_db)):
    if x_admin_token == ADMIN_TOKEN:
        return
    if channel and authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1]
        user = db.query(TwitchUser).filter_by(access_token=token).one_or_none()
        if user:
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
def auth_login(channel: str):
    scope = "+".join(TWITCH_SCOPES)
    url = (
        "https://id.twitch.tv/oauth2/authorize"
        f"?response_type=code&client_id={TWITCH_CLIENT_ID}"
        f"&redirect_uri={TWITCH_REDIRECT_URI}&scope={scope}&state={channel}"
    )
    return {"auth_url": url}

@app.get("/auth/callback", response_model=AuthCallbackOut)
def auth_callback(code: str, state: str, db: Session = Depends(get_db)):
    token_resp = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": TWITCH_REDIRECT_URI,
        },
    ).json()
    access_token = token_resp["access_token"]
    refresh_token = token_resp.get("refresh_token")
    scopes_list = token_resp.get("scope", [])
    if "channel:bot" not in scopes_list:
        raise HTTPException(status_code=400, detail="channel:bot scope required")
    scopes = " ".join(scopes_list)
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
    channel_name = state
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
    subscribe_chat_eventsub(user_info["id"])
    return {"success": True}


@app.post("/eventsub/callback")
async def eventsub_callback(
    request: FastAPIRequest,
    twitch_eventsub_message_type: str = Header(None),
    twitch_eventsub_message_id: str = Header(None),
    twitch_eventsub_message_timestamp: str = Header(None),
    twitch_eventsub_message_signature: str = Header(None),
):
    body = await request.body()
    if twitch_eventsub_message_type == "webhook_callback_verification":
        data = json.loads(body)
        return Response(content=data.get("challenge", ""))
    message = (
        twitch_eventsub_message_id + twitch_eventsub_message_timestamp + body.decode()
    )
    sig = hmac.new(
        TWITCH_EVENTSUB_SECRET.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    if twitch_eventsub_message_signature != f"sha256={sig}":
        raise HTTPException(status_code=403, detail="invalid signature")
    return {"ok": True}

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
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
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
    return db.query(ActiveChannel).all()

@app.post("/channels", response_model=ChannelOut, dependencies=[Depends(require_token)])
def add_channel(payload: ChannelIn, db: Session = Depends(get_db)):
    ch = ActiveChannel(channel_id=payload.channel_id, channel_name=payload.channel_name, join_active=payload.join_active)
    db.add(ch)
    db.commit()
    db.refresh(ch)
    get_or_create_settings(db, ch.id)
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
    return ch

@app.put("/channels/{channel}", dependencies=[Depends(require_token)])
def update_channel_status(channel: str, join_active: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        raise HTTPException(404, "channel not found")
    ch.join_active = join_active
    db.commit()
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
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
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
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
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
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
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
    return {"success": True}

@app.delete("/channels/{channel}/songs/{song_id}", dependencies=[Depends(require_token)])
def delete_song(channel: str, song_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    song = db.query(Song).filter(Song.id == song_id, Song.channel_id == channel_pk).one_or_none()
    if not song:
        raise HTTPException(404, "song not found")
    db.delete(song)
    db.commit()
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
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
        try: _broker(channel_pk).put_nowait("changed")
        except: pass
        return {"id": u.id}
    u = User(channel_id=channel_pk, twitch_id=payload.twitch_id, username=payload.username)
    db.add(u)
    db.commit()
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
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
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
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
    if not u or u.channel_id != channel_pk: raise HTTPException(404)
    u.prio_points = int(payload.get("prio_points", 0))
    db.commit()
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
    return {"success": True}

# =====================================
# Routes: Queue
# =====================================
@app.get("/channels/{channel}/queue/stream")
async def stream_queue(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    q = _broker(channel_pk)
    async def gen():
        # initial tick so clients render immediately
        yield "event: queue\ndata: init\n\n"
        while True:
            msg = await q.get()
            yield f"event: queue\ndata: {msg}\n\n"
    return EventSourceResponse(
    gen(),
    headers={
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
    }
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
                try: _broker(channel_pk).put_nowait("changed")
                except: pass
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

    try:
        _broker(channel_pk).put_nowait("changed")
    except asyncio.QueueFull:
        pass
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
    try:
        _broker(channel_pk).put_nowait("changed")
    except asyncio.QueueFull:
        pass
    return {"success": True}

@app.delete("/channels/{channel}/queue/{request_id}", dependencies=[Depends(require_token)])
def remove_request(channel: str, request_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    r = db.query(Request).filter(Request.id == request_id, Request.channel_id == channel_pk).one_or_none()
    if not r:
        raise HTTPException(404, "request not found")
    db.delete(r)
    db.commit()
    try:
        _broker(channel_pk).put_nowait("changed")
    except asyncio.QueueFull:
        pass
    return {"success": True}

@app.post("/channels/{channel}/queue/clear", dependencies=[Depends(require_token)])
def clear_queue(channel: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    sid = current_stream(db, channel_pk)
    db.query(Request).filter(Request.channel_id == channel_pk, Request.stream_id == sid, Request.played == 0).delete()
    db.commit()
    try:
        _broker(channel_pk).put_nowait("changed")
    except asyncio.QueueFull:
        pass
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
    try:
        _broker(channel_pk).put_nowait("changed")
    except asyncio.QueueFull:
        pass
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
def move_request(channel: str, request_id: int, direction: str, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    if direction not in ("up", "down"):
        raise HTTPException(400, "direction must be 'up' or 'down'")
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
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
    return {"success": True}

@app.post("/channels/{channel}/queue/{request_id}/skip", dependencies=[Depends(require_token)])
def skip_request(channel: str, request_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    req = _get_req(db, channel_pk, request_id)
    # move to bottom of pending
    max_pos = db.execute(
        text("SELECT COALESCE(MAX(position), 0) FROM queue_requests WHERE channel_id=:c AND stream_id=:s AND played=0"),
        {"c": channel_pk, "s": req.stream_id}
    ).scalar_one()
    req.position = max_pos + 1
    db.commit()
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
    return {"success": True}

@app.post("/channels/{channel}/queue/{request_id}/priority", dependencies=[Depends(require_token)])
def set_priority(channel: str, request_id: int, enabled: bool, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    req = _get_req(db, channel_pk, request_id)
    # optional: refund or spend points can be inserted here
    req.is_priority = 1 if enabled else 0
    db.commit()
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
    return {"success": True}

@app.post("/channels/{channel}/queue/{request_id}/played", dependencies=[Depends(require_token)])
def mark_played(channel: str, request_id: int, db: Session = Depends(get_db)):
    channel_pk = get_channel_pk(channel, db)
    req = _get_req(db, channel_pk, request_id)
    req.played = 1
    # optionally push it out of visible order by setting a sentinel position
    db.commit()
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
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
    try: _broker(channel_pk).put_nowait("changed")
    except: pass    
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
    try:
        _broker(channel_pk).put_nowait("changed")
    except asyncio.QueueFull:
        pass
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
