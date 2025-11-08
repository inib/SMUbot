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


def _create_channel(db: backend_app.Session) -> str:
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
    return channel.channel_name


class SongApiTests(unittest.TestCase):
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

    def test_add_song_reuses_existing_youtube_link(self) -> None:
        db = backend_app.SessionLocal()
        try:
            channel_name = _create_channel(db)
        finally:
            db.close()

        headers = {"X-Admin-Token": backend_app.ADMIN_TOKEN}
        payload = {
            "artist": "Artist",
            "title": "Title",
            "youtube_link": "https://youtu.be/abc123def45",
        }
        response = self._client.post(
            f"/channels/{channel_name}/songs",
            json=payload,
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        first_id = response.json()["id"]

        duplicate = dict(payload)
        duplicate["youtube_link"] = (
            "https://www.youtube.com/watch?v=abc123def45&feature=share"
        )
        response = self._client.post(
            f"/channels/{channel_name}/songs",
            json=duplicate,
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["id"], first_id)

        db = backend_app.SessionLocal()
        try:
            songs = (
                db.query(backend_app.Song)
                .filter(backend_app.Song.channel_id == backend_app.get_channel_pk(channel_name, db))
                .all()
            )
            self.assertEqual(len(songs), 1)
            self.assertEqual(
                songs[0].youtube_link,
                "https://www.youtube.com/watch?v=abc123def45",
            )
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
