from __future__ import annotations
from typing import Optional, List, Any
import os
import json
from datetime import datetime
import asyncio
from sse_starlette.sse import EventSourceResponse
from fastapi.middleware.cors import CORSMiddleware

from fastapi import FastAPI, HTTPException, Depends, Header, Query, APIRouter
from pydantic import BaseModel, Field
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, Boolean,
    ForeignKey, UniqueConstraint, func, select, and_, text
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session

# =====================================
# Config
# =====================================
DB_URL = os.getenv("DB_URL", "sqlite:///./db.sqlite")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "defaultpw")

engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()



# =====================================
# Models
# =====================================
class ActiveChannel(Base):
    __tablename__ = "active_channels"
    id = Column(Integer, primary_key=True)
    channel_id = Column(String, unique=True, nullable=False)  # Twitch channel ID
    channel_name = Column(String, nullable=False)
    join_active = Column(Integer, default=1)

    settings = relationship("ChannelSettings", back_populates="channel", uselist=False, cascade="all, delete-orphan")
    songs = relationship("Song", back_populates="channel", cascade="all, delete-orphan")
    users = relationship("User", back_populates="channel", cascade="all, delete-orphan")

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
    return _brokers.setdefault(channel_pk, asyncio.Queue(maxsize=100))

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def require_token(x_admin_token: str = Header(None)):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="invalid admin token")

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
    return ch

@app.put("/channels/{channel_pk}", dependencies=[Depends(require_token)])
def update_channel_status(channel_pk: int, join_active: int, db: Session = Depends(get_db)):
    ch = db.get(ActiveChannel, channel_pk)
    if not ch:
        raise HTTPException(404, "channel not found")
    ch.join_active = join_active
    db.commit()
    return {"success": True}

@app.get("/channels/{channel_pk}/settings", response_model=ChannelSettingsOut)
def get_channel_settings(channel_pk: int, db: Session = Depends(get_db)):
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

@app.put("/channels/{channel_pk}/settings", dependencies=[Depends(require_token)])
def set_channel_settings(channel_pk: int, payload: ChannelSettingsIn, db: Session = Depends(get_db)):
    st = get_or_create_settings(db, channel_pk)
    st.max_requests_per_user = payload.max_requests_per_user
    st.prio_only = payload.prio_only
    st.queue_closed = payload.queue_closed
    st.allow_bumps = payload.allow_bumps
    st.other_flags = payload.other_flags
    st.max_prio_points = payload.max_prio_points
    db.commit()
    return {"success": True}

# =====================================
# Routes: Songs
# =====================================
@app.get("/channels/{channel_pk}/songs", response_model=List[SongOut])
def search_songs(channel_pk: int, search: Optional[str] = Query(None), db: Session = Depends(get_db)):
    q = db.query(Song).filter(Song.channel_id == channel_pk)
    if search:
        like = f"%{search}%"
        q = q.filter((Song.artist.ilike(like)) | (Song.title.ilike(like)))
    return q.order_by(Song.artist.asc(), Song.title.asc()).all()

@app.post("/channels/{channel_pk}/songs", response_model=dict, dependencies=[Depends(require_token)])
def add_song(channel_pk: int, payload: SongIn, db: Session = Depends(get_db)):
    song = Song(channel_id=channel_pk, **payload.model_dump())
    db.add(song)
    db.commit()
    return {"id": song.id}

@app.get("/channels/{channel_pk}/songs/{song_id}", response_model=SongOut)
def get_song(channel_pk: int, song_id: int, db: Session = Depends(get_db)):
    song = db.query(Song).filter(Song.id == song_id, Song.channel_id == channel_pk).one_or_none()
    if not song:
        raise HTTPException(404, "song not found")
    return song

@app.put("/channels/{channel_pk}/songs/{song_id}", dependencies=[Depends(require_token)])
def update_song(channel_pk: int, song_id: int, payload: SongIn, db: Session = Depends(get_db)):
    song = db.query(Song).filter(Song.id == song_id, Song.channel_id == channel_pk).one_or_none()
    if not song:
        raise HTTPException(404, "song not found")
    for k, v in payload.model_dump().items():
        setattr(song, k, v)
    db.commit()
    return {"success": True}

@app.delete("/channels/{channel_pk}/songs/{song_id}", dependencies=[Depends(require_token)])
def delete_song(channel_pk: int, song_id: int, db: Session = Depends(get_db)):
    song = db.query(Song).filter(Song.id == song_id, Song.channel_id == channel_pk).one_or_none()
    if not song:
        raise HTTPException(404, "song not found")
    db.delete(song)
    db.commit()
    return {"success": True}

# =====================================
# Routes: Users
# =====================================
@app.get("/channels/{channel_pk}/users", response_model=List[UserOut])
def search_users(channel_pk: int, search: Optional[str] = Query(None), db: Session = Depends(get_db)):
    q = db.query(User).filter(User.channel_id == channel_pk)
    if search:
        like = f"%{search}%"
        q = q.filter(User.username.ilike(like))
    return q.order_by(User.username.asc()).all()

@app.post("/channels/{channel_pk}/users", response_model=dict, dependencies=[Depends(require_token)])
def get_or_create_user(channel_pk: int, payload: UserIn, db: Session = Depends(get_db)):
    u = (
        db.query(User)
        .filter(User.channel_id == channel_pk, User.twitch_id == payload.twitch_id)
        .one_or_none()
    )
    if u:
        u.username = payload.username  # update latest name
        db.commit()
        return {"id": u.id}
    u = User(channel_id=channel_pk, twitch_id=payload.twitch_id, username=payload.username)
    db.add(u)
    db.commit()
    return {"id": u.id}

@app.get("/channels/{channel_pk}/users/{user_id}", response_model=UserOut)
def get_user(channel_pk: int, user_id: int, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.id == user_id, User.channel_id == channel_pk).one_or_none()
    if not u:
        raise HTTPException(404, "user not found")
    return u

@app.put("/channels/{channel_pk}/users/{user_id}", dependencies=[Depends(require_token)])
def update_user(channel_pk: int, user_id: int, prio_points: Optional[int] = None, amount_requested: Optional[int] = None, db: Session = Depends(get_db)):
    u = db.query(User).filter(User.id == user_id, User.channel_id == channel_pk).one_or_none()
    if not u:
        raise HTTPException(404, "user not found")
    if prio_points is not None:
        st = get_or_create_settings(db, channel_pk)
        u.prio_points = max(0, min(st.max_prio_points or 10, prio_points))
    if amount_requested is not None:
        u.amount_requested = max(0, amount_requested)
    db.commit()
    return {"success": True}

@app.get("/channels/{channel_pk}/users/{user_id}/stream_state")
def get_user_stream_state(channel_pk: int, user_id: int, db: Session = Depends(get_db)):
    sid = current_stream(db, channel_pk)
    ensure_user_stream_state(db, user_id, sid)
    st = (
        db.query(UserStreamState)
        .filter(UserStreamState.user_id == user_id, UserStreamState.stream_id == sid)
        .one()
    )
    return {"stream_id": sid, "sub_free_used": int(st.sub_free_used)}

@app.get("/channels/{channel_pk}/users", dependencies=[Depends(require_token)])
def list_users(channel_pk: int, db: Session = Depends(get_db)):
    return db.query(User).filter(User.channel_id==channel_pk).all()

@app.put("/channels/{channel_pk}/users/{user_id}/points", dependencies=[Depends(require_token)])
def set_points(channel_pk: int, user_id: int, payload: dict, db: Session = Depends(get_db)):
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
@app.get("/channels/{channel_pk}/queue/stream")
async def stream_queue(channel_pk: int):
    q = _broker(channel_pk)
    async def gen():
        # initial tick so clients render immediately
        yield {"event": "queue", "data": "init"}
        while True:
            msg = await q.get()
            yield {"event": "queue", "data": msg}
    return EventSourceResponse(gen())

@app.get("/channels/{channel_pk}/queue", response_model=List[RequestOut])
def get_queue(channel_pk: int, db: Session = Depends(get_db)):
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

@app.get("/channels/{channel_pk}/streams/{stream_id}/queue", response_model=List[RequestOut])
def get_stream_queue(channel_pk: int, stream_id: int, db: Session = Depends(get_db)):
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

@app.post("/channels/{channel_pk}/queue", response_model=dict, dependencies=[Depends(require_token)])
def add_request(channel_pk: int, payload: RequestCreate, db: Session = Depends(get_db)):
    # Checks
    enforce_queue_limits(db, channel_pk, payload.user_id, payload.want_priority)
    sid = current_stream(db, channel_pk)

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

@app.put("/channels/{channel_pk}/queue/{request_id}", dependencies=[Depends(require_token)])
def update_request(channel_pk: int, request_id: int, payload: RequestUpdate, db: Session = Depends(get_db)):
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

@app.delete("/channels/{channel_pk}/queue/{request_id}", dependencies=[Depends(require_token)])
def remove_request(channel_pk: int, request_id: int, db: Session = Depends(get_db)):
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

@app.post("/channels/{channel_pk}/queue/clear", dependencies=[Depends(require_token)])
def clear_queue(channel_pk: int, db: Session = Depends(get_db)):
    sid = current_stream(db, channel_pk)
    db.query(Request).filter(Request.channel_id == channel_pk, Request.stream_id == sid, Request.played == 0).delete()
    db.commit()
    try:
        _broker(channel_pk).put_nowait("changed")
    except asyncio.QueueFull:
        pass
    return {"success": True}

@app.get("/channels/{channel_pk}/queue/random_nonpriority")
def random_nonpriority(channel_pk: int, db: Session = Depends(get_db)):
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

@app.post("/channels/{channel_pk}/queue/{request_id}/bump_admin", dependencies=[Depends(require_token)])
def bump_admin(channel_pk: int, request_id: int, db: Session = Depends(get_db)):
    r = db.query(Request).filter(Request.id == request_id, Request.channel_id == channel_pk).one_or_none()
    if not r:
        raise HTTPException(404, "request not found")
    r.is_priority = 1
    r.priority_source = 'admin'
    db.commit()
    return {"success": True}

def _get_req(db, channel_pk: int, request_id: int):
    req = db.execute(
        select(Request).where(and_(Request.id == request_id,
                                        Request.channel_id == channel_pk))
    ).scalar_one_or_none()
    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    return req

@app.post("/channels/{channel_pk}/queue/{request_id}/move", dependencies=[Depends(require_token)])
def move_request(channel_pk: int, request_id: int, direction: str, db: Session = Depends(get_db)):
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

@app.post("/channels/{channel_pk}/queue/{request_id}/skip", dependencies=[Depends(require_token)])
def skip_request(channel_pk: int, request_id: int, db: Session = Depends(get_db)):
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

@app.post("/channels/{channel_pk}/queue/{request_id}/priority", dependencies=[Depends(require_token)])
def set_priority(channel_pk: int, request_id: int, enabled: bool, db: Session = Depends(get_db)):
    req = _get_req(db, channel_pk, request_id)
    # optional: refund or spend points can be inserted here
    req.is_priority = 1 if enabled else 0
    db.commit()
    try: _broker(channel_pk).put_nowait("changed")
    except: pass
    return {"success": True}

@app.post("/channels/{channel_pk}/queue/{request_id}/played", dependencies=[Depends(require_token)])
def mark_played(channel_pk: int, request_id: int, db: Session = Depends(get_db)):
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
@app.post("/channels/{channel_pk}/events", response_model=dict, dependencies=[Depends(require_token)])
def log_event(channel_pk: int, payload: EventIn, db: Session = Depends(get_db)):
    meta_str = json.dumps(payload.meta or {})
    ev = Event(channel_id=channel_pk, event_type=payload.type, user_id=payload.user_id, meta=meta_str)
    db.add(ev)
    db.commit()

    # Award points based on rules
    if payload.type in {"follow", "raid"}:
        if payload.user_id:
            award_prio_points(db, channel_pk, payload.user_id, 1)
    elif payload.type == "gift_sub":
        # meta expects {"count": N}
        count = int((payload.meta or {}).get("count", 1))
        points = count // 5
        if payload.user_id and points > 0:  # gifter
            award_prio_points(db, channel_pk, payload.user_id, points)
    elif payload.type == "sub":
        # no automatic points; handled via free-per-stream when requesting
        pass

    return {"event_id": ev.id}

@app.get("/channels/{channel_pk}/events", response_model=List[EventOut])
def list_events(channel_pk: int, type: Optional[str] = None, since: Optional[str] = None, db: Session = Depends(get_db)):
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
@app.post("/channels/{channel_pk}/streams/start", response_model=dict, dependencies=[Depends(require_token)])
def start_stream(channel_pk: int, db: Session = Depends(get_db)):
    sid = current_stream(db, channel_pk)
    return {"stream_id": sid}

@app.post("/channels/{channel_pk}/streams/archive", response_model=dict, dependencies=[Depends(require_token)])
def archive_stream(channel_pk: int, db: Session = Depends(get_db)):
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
@app.get("/channels/{channel_pk}/stats/general")
def stats_general(channel_pk: int, since: Optional[str] = None, db: Session = Depends(get_db)):
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

@app.get("/channels/{channel_pk}/stats/songs")
def stats_top_songs(channel_pk: int, top: int = 10, since: Optional[str] = None, db: Session = Depends(get_db)):
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

@app.get("/channels/{channel_pk}/stats/users")
def stats_top_users(channel_pk: int, top: int = 10, since: Optional[str] = None, db: Session = Depends(get_db)):
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