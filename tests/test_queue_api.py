import unittest

from fastapi.testclient import TestClient

import backend_app


def _wipe_db() -> None:
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


class QueueApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._client = TestClient(backend_app.app)
        self._original_client_id = backend_app.TWITCH_CLIENT_ID
        backend_app.TWITCH_CLIENT_ID = None
        _wipe_db()

    def tearDown(self) -> None:
        self._client.close()
        backend_app.TWITCH_CLIENT_ID = self._original_client_id
        _wipe_db()

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

