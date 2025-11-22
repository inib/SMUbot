import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

import backend_app


def _wipe_db() -> None:
    """Clear all tables touched by queue tests to isolate scenarios.

    Dependencies: a writable SQLAlchemy session. Code customers: test setUp and
    tearDown routines rely on this reset to avoid crosstalk. Variables originate
    from the ORM models imported from `backend_app`.
    """
    db = backend_app.SessionLocal()
    try:
        for model in [
            backend_app.Request,
            backend_app.Song,
            backend_app.User,
            backend_app.StreamSession,
            backend_app.PlaylistItem,
            backend_app.PlaylistKeyword,
            backend_app.Playlist,
            backend_app.ChannelSettings,
            backend_app.ChannelModerator,
            backend_app.ActiveChannel,
            backend_app.TwitchUser,
        ]:
            db.query(model).delete()
        db.commit()
    finally:
        db.close()


def _seed_queue_fixture(
    db: backend_app.Session, *, amount_requested: object = 0, prio_points: object = 0
) -> tuple[str, str]:
    """Seed a minimal channel, stream, and queue entry for API tests.

    Dependencies: expects an open database session using the same engine as the
    FastAPI app. Code customers: queue API tests rely on this record scaffold to
    generate predictable identifiers and authentication tokens. Variables such as
    `amount_requested` and `prio_points` originate from test parameters to emulate
    edge-case user metadata.
    """
    owner = backend_app.TwitchUser(
        twitch_id="owner",
        username="owner",
        access_token="",
        refresh_token="",
        scopes="",
    )
    db.add(owner)
    db.commit()
    db.refresh(owner)

    channel = backend_app.ActiveChannel(
        channel_id="123",
        channel_name="itsalpine",
        owner_id=owner.id,
        authorized=True,
    )
    db.add(channel)
    db.commit()
    db.refresh(channel)

    stream = backend_app.StreamSession(channel_id=channel.id)
    db.add(stream)
    db.commit()
    db.refresh(stream)

    song = backend_app.Song(
        channel_id=channel.id,
        title="Song",
        artist="Artist",
    )
    db.add(song)
    db.commit()
    db.refresh(song)

    user = backend_app.User(
        channel_id=channel.id,
        twitch_id="user1",
        username="user1",
        amount_requested=amount_requested,
        prio_points=prio_points,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    request = backend_app.Request(
        channel_id=channel.id,
        stream_id=stream.id,
        song_id=song.id,
        user_id=user.id,
    )
    db.add(request)
    db.commit()

    admin = backend_app.TwitchUser(
        twitch_id="admin",
        username="admin",
        access_token="session-token",
        refresh_token="",
        scopes="",
    )
    db.add(admin)
    db.commit()
    db.refresh(admin)

    link = backend_app.ChannelModerator(channel_id=channel.id, user_id=admin.id)
    db.add(link)
    db.commit()

    return channel.channel_name, "session-token"


def _add_request(
    db: backend_app.Session,
    *,
    channel_id: int,
    stream_id: int,
    song_id: int,
    user_id: int,
    position: int,
    played: int = 0,
    is_priority: int = 0,
    bumped: int = 0,
    request_time: datetime | None = None,
) -> backend_app.Request:
    """Create a queue request tailored for resolver tests.

    Dependencies: active SQLAlchemy session, foreign keys for channel, stream, song,
    and user identifiers. Code customers: queue identifier keyword tests rely on this
    builder to craft pending and played records with explicit positions. Variables
    originate from seeded fixtures to mirror realistic queue rows.
    """

    req = backend_app.Request(
        channel_id=channel_id,
        stream_id=stream_id,
        song_id=song_id,
        user_id=user_id,
        position=position,
        played=played,
        is_priority=is_priority,
        bumped=bumped,
        request_time=request_time or datetime.utcnow(),
    )
    db.add(req)
    db.commit()
    db.refresh(req)
    return req


class QueueApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._settings_snapshot = backend_app.settings_store.snapshot()
        _wipe_db()
        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.AppSetting).delete()
            backend_app.set_settings(
                db,
                {
                    "twitch_client_id": "client",
                    "twitch_client_secret": "secret",
                    "setup_complete": "1",
                    "twitch_scopes": "channel:bot channel:read:subscriptions channel:read:vips",
                    "bot_app_scopes": "user:read:chat user:write:chat user:bot",
                },
            )
        finally:
            db.close()
        backend_app.APP_ACCESS_TOKEN = None
        backend_app.APP_TOKEN_EXPIRES = 0
        backend_app.BOT_USER_ID = None
        self._client = TestClient(backend_app.app)

    def tearDown(self) -> None:
        self._client.close()
        _wipe_db()
        backend_app.APP_ACCESS_TOKEN = None
        backend_app.APP_TOKEN_EXPIRES = 0
        backend_app.BOT_USER_ID = None
        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.AppSetting).delete()
            if self._settings_snapshot:
                backend_app.set_settings(db, self._settings_snapshot)
        finally:
            db.close()

    def test_channel_live_status(self) -> None:
        db = backend_app.SessionLocal()
        try:
            channel_name, _ = _seed_queue_fixture(db)
        finally:
            db.close()

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "data": [
                {"user_id": "123", "type": "live"},
            ]
        }

        with patch("backend_app.get_app_access_token", return_value="token"), patch(
            "backend_app.requests.get", return_value=mock_response
        ):
            response = self._client.get("/channels/live_status")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            response.json(),
            [
                {
                    "channel_name": channel_name,
                    "channel_id": "123",
                    "is_live": True,
                }
            ],
        )

    def test_queue_full_coerces_invalid_user_counts(self) -> None:
        db = backend_app.SessionLocal()
        try:
            channel_name, token = _seed_queue_fixture(
                db, amount_requested="abc", prio_points="xyz"
            )
        finally:
            db.close()

        response = self._client.get(
            f"/channels/{channel_name}/queue/full",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertTrue(payload)
        user_payload = payload[0]["user"]
        self.assertEqual(user_payload["amount_requested"], 0)
        self.assertEqual(user_payload["prio_points"], 0)

    def test_queue_full_handles_missing_role_collector_helper(self) -> None:
        db = backend_app.SessionLocal()
        try:
            channel_name, token = _seed_queue_fixture(db)
        finally:
            db.close()

        original_helper = getattr(backend_app, "_collect_channel_roles", None)
        try:
            if hasattr(backend_app, "_collect_channel_roles"):
                delattr(backend_app, "_collect_channel_roles")
            response = self._client.get(
                f"/channels/{channel_name}/queue/full",
                headers={"Authorization": f"Bearer {token}"},
            )
            self.assertEqual(response.status_code, 200, response.text)
            payload = response.json()
            self.assertTrue(payload)
        finally:
            if original_helper is not None:
                backend_app._collect_channel_roles = original_helper  # type: ignore[attr-defined]

    def test_keyword_request_identifiers_cover_queue_edges(self) -> None:
        db = backend_app.SessionLocal()
        try:
            channel_name, token = _seed_queue_fixture(db)
            channel = db.query(backend_app.ActiveChannel).filter_by(channel_name=channel_name).one()
            stream = db.query(backend_app.StreamSession).filter_by(channel_id=channel.id).one()
            song = db.query(backend_app.Song).filter_by(channel_id=channel.id).first()
            user = db.query(backend_app.User).filter_by(channel_id=channel.id).first()
            base_request = db.query(backend_app.Request).filter_by(channel_id=channel.id).first()

            priority_pending = _add_request(
                db,
                channel_id=channel.id,
                stream_id=stream.id,
                song_id=song.id,
                user_id=user.id,
                position=2,
                is_priority=1,
            )
            trailing_pending = _add_request(
                db,
                channel_id=channel.id,
                stream_id=stream.id,
                song_id=song.id,
                user_id=user.id,
                position=9,
            )
            recent_played = _add_request(
                db,
                channel_id=channel.id,
                stream_id=stream.id,
                song_id=song.id,
                user_id=user.id,
                position=4,
                played=1,
                is_priority=1,
                request_time=datetime.utcnow() + timedelta(seconds=1),
            )
            base_request_id = base_request.id
            priority_pending_id = priority_pending.id
            trailing_pending_id = trailing_pending.id
            recent_played_id = recent_played.id
        finally:
            db.close()

        response = self._client.post(
            f"/channels/{channel_name}/queue/top/played",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            refreshed_priority = db.get(backend_app.Request, priority_pending_id)
            self.assertIsNotNone(refreshed_priority)
            assert refreshed_priority  # narrow type
            self.assertEqual(refreshed_priority.played, 1)
        finally:
            db.close()

        response = self._client.post(
            f"/channels/{channel_name}/queue/previous/priority",
            params={"enabled": False},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            refreshed_previous = db.get(backend_app.Request, recent_played_id)
            self.assertIsNotNone(refreshed_previous)
            assert refreshed_previous
            self.assertEqual(refreshed_previous.is_priority, 0)
        finally:
            db.close()

        response = self._client.delete(
            f"/channels/{channel_name}/queue/last",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            self.assertIsNone(db.get(backend_app.Request, trailing_pending_id))
        finally:
            db.close()

        response = self._client.post(
            f"/channels/{channel_name}/queue/{base_request.id}/played",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            refreshed_base = db.get(backend_app.Request, base_request_id)
            self.assertIsNotNone(refreshed_base)
            assert refreshed_base
            self.assertEqual(refreshed_base.played, 1)
        finally:
            db.close()

    def test_random_keyword_uses_single_pending_request(self) -> None:
        db = backend_app.SessionLocal()
        try:
            channel_name, token = _seed_queue_fixture(db)
            base_request = db.query(backend_app.Request).first()
        finally:
            db.close()

        response = self._client.post(
            f"/channels/{channel_name}/queue/random/played",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            refreshed_request = db.get(backend_app.Request, base_request.id)
            self.assertIsNotNone(refreshed_request)
            assert refreshed_request
            self.assertEqual(refreshed_request.played, 1)
        finally:
            db.close()

