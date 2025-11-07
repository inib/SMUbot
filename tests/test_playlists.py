from __future__ import annotations

import unittest
from unittest import mock

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


class FakeYTMusic:
    def get_playlist(self, playlistId, limit=500):  # noqa: N802 - external API casing
        if playlistId != "PL123":
            raise AssertionError(f"unexpected playlist id {playlistId}")
        return {
            "title": "Test Playlist",
            "tracks": [
                {
                    "videoId": "vid1",
                    "title": "Track One",
                    "artists": [{"name": "Artist A"}],
                    "duration_seconds": 215,
                },
                {
                    "videoId": "vid2",
                    "title": "Track Two",
                    "artists": [{"name": "Artist B"}],
                    "duration": "3:45",
                },
            ],
        }


class PlaylistApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(backend_app.app)
        self._original_get_client = backend_app.get_ytmusic_client
        self._fake_client = FakeYTMusic()
        backend_app.get_ytmusic_client = lambda: self._fake_client  # type: ignore[assignment]
        _wipe_db()
        db = backend_app.SessionLocal()
        try:
            owner = backend_app.TwitchUser(
                twitch_id="owner",
                username="owner",
                access_token="token",
                refresh_token="",
                scopes="",
            )
            db.add(owner)
            db.commit()
            db.refresh(owner)

            channel = backend_app.ActiveChannel(
                channel_id="chan123",
                channel_name="itsalpine",
                owner_id=owner.id,
                authorized=True,
            )
            db.add(channel)
            db.commit()
            db.refresh(channel)
            backend_app.get_or_create_settings(db, channel.id)
            self.channel_name = channel.channel_name
        finally:
            db.close()

    def tearDown(self) -> None:
        backend_app.get_ytmusic_client = self._original_get_client  # type: ignore[assignment]
        self.client.close()
        _wipe_db()

    def _admin_headers(self) -> dict[str, str]:
        return {"X-Admin-Token": backend_app.ADMIN_TOKEN}

    def _create_sample_playlist(self) -> int:
        response = self.client.post(
            f"/channels/{self.channel_name}/playlists",
            json={
                "url": "https://www.youtube.com/playlist?list=PL123",
                "keywords": ["Default", " chill "],
                "visibility": "notlisted",
            },
            headers=self._admin_headers(),
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        return int(payload["id"])

    def test_create_playlist_and_fetch_items(self) -> None:
        playlist_id = self._create_sample_playlist()

        list_response = self.client.get(
            f"/channels/{self.channel_name}/playlists",
            headers=self._admin_headers(),
        )
        self.assertEqual(list_response.status_code, 200, list_response.text)
        data = list_response.json()
        self.assertEqual(len(data), 1)
        playlist = data[0]
        self.assertEqual(playlist["playlist_id"], "PL123")
        self.assertEqual(playlist["visibility"], "unlisted")
        self.assertEqual(playlist["keywords"], ["chill", "default"])
        self.assertEqual(playlist["item_count"], 2)
        self.assertEqual(playlist["source"], "youtube")
        self.assertIsNone(playlist["description"])

        items_response = self.client.get(
            f"/channels/{self.channel_name}/playlists/{playlist_id}/items",
            headers=self._admin_headers(),
        )
        self.assertEqual(items_response.status_code, 200, items_response.text)
        items = items_response.json()
        self.assertEqual(len(items), 2)
        first = items[0]
        self.assertEqual(first["video_id"], "vid1")
        self.assertEqual(first["artist"], "Artist A")
        self.assertEqual(first["duration_seconds"], 215)
        self.assertEqual(first["url"], "https://www.youtube.com/watch?v=vid1")

    def test_random_request_uses_default_keyword(self) -> None:
        self._create_sample_playlist()
        with mock.patch("backend_app.random.choice", side_effect=lambda seq: seq[0]):
            response = self.client.post(
                f"/channels/{self.channel_name}/playlists/random_request",
                json={
                    "twitch_id": "viewer1",
                    "username": "Viewer",
                    "is_subscriber": False,
                },
                headers=self._admin_headers(),
            )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["keyword"], "default")
        self.assertEqual(payload["song"]["title"], "Track One")

        db = backend_app.SessionLocal()
        try:
            requests = db.query(backend_app.Request).all()
            self.assertEqual(len(requests), 1)
            req = requests[0]
            self.assertEqual(req.is_priority, 0)
            user = db.get(backend_app.User, req.user_id)
            self.assertIsNotNone(user)
            self.assertEqual(user.twitch_id, "viewer1")
            song = db.get(backend_app.Song, req.song_id)
            self.assertIsNotNone(song)
            if song:
                self.assertEqual(song.youtube_link, "https://www.youtube.com/watch?v=vid1")
        finally:
            db.close()

    def test_queue_playlist_item_bumped_sets_priority(self) -> None:
        playlist_id = self._create_sample_playlist()
        items_response = self.client.get(
            f"/channels/{self.channel_name}/playlists/{playlist_id}/items",
            headers=self._admin_headers(),
        )
        self.assertEqual(items_response.status_code, 200, items_response.text)
        item_id = items_response.json()[0]["id"]

        queue_response = self.client.post(
            f"/channels/{self.channel_name}/playlists/{playlist_id}/queue",
            json={"item_id": item_id, "bumped": True},
            headers=self._admin_headers(),
        )
        self.assertEqual(queue_response.status_code, 200, queue_response.text)

        db = backend_app.SessionLocal()
        try:
            req = db.query(backend_app.Request).one()
            self.assertEqual(req.is_priority, 1)
            self.assertEqual(req.priority_source, "admin")
            self.assertEqual(req.bumped, 1)
            user = db.get(backend_app.User, req.user_id)
            self.assertIsNotNone(user)
            if user:
                self.assertEqual(user.twitch_id, "__playlist__")
        finally:
            db.close()

    def test_create_manual_playlist_and_items(self) -> None:
        response = self.client.post(
            f"/channels/{self.channel_name}/playlists",
            json={
                "manual": {"title": "Manual Mix", "description": "chill vibes"},
                "keywords": ["Default", "Favorites"],
                "visibility": "public",
            },
            headers=self._admin_headers(),
        )
        self.assertEqual(response.status_code, 200, response.text)
        playlist_id = response.json()["id"]

        add_item = self.client.post(
            f"/channels/{self.channel_name}/playlists/{playlist_id}/items",
            json={"title": "Song A", "artist": "Artist A", "video_id": "abc123"},
            headers=self._admin_headers(),
        )
        self.assertEqual(add_item.status_code, 200, add_item.text)
        payload = add_item.json()
        self.assertEqual(payload["position"], 1)
        self.assertEqual(payload["video_id"], "abc123")
        self.assertEqual(payload["url"], "https://www.youtube.com/watch?v=abc123")

        add_second = self.client.post(
            f"/channels/{self.channel_name}/playlists/{playlist_id}/items",
            json={"title": "Song B", "url": "https://youtu.be/xyz789"},
            headers=self._admin_headers(),
        )
        self.assertEqual(add_second.status_code, 200, add_second.text)
        second = add_second.json()
        self.assertEqual(second["position"], 2)
        self.assertEqual(second["video_id"], "xyz789")

        list_response = self.client.get(
            f"/channels/{self.channel_name}/playlists/{playlist_id}/items",
            headers=self._admin_headers(),
        )
        self.assertEqual(list_response.status_code, 200, list_response.text)
        items = list_response.json()
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["video_id"], "abc123")

        delete_response = self.client.delete(
            f"/channels/{self.channel_name}/playlists/{playlist_id}/items/{second['id']}",
            headers=self._admin_headers(),
        )
        self.assertEqual(delete_response.status_code, 204, delete_response.text)

        dup_response = self.client.post(
            f"/channels/{self.channel_name}/playlists/{playlist_id}/items",
            json={"title": "Duplicate", "video_id": "abc123"},
            headers=self._admin_headers(),
        )
        self.assertEqual(dup_response.status_code, 409, dup_response.text)

    def test_add_channel_seeds_favorites_playlist(self) -> None:
        _wipe_db()
        self.client.post(
            "/channels",
            json={
                "channel_name": "manualtester",
                "channel_id": "manual123",
                "join_active": 1,
            },
            headers=self._admin_headers(),
        )
        db = backend_app.SessionLocal()
        try:
            channel = db.query(backend_app.ActiveChannel).filter_by(channel_name="manualtester").one()
            playlists = db.query(backend_app.Playlist).filter_by(channel_id=channel.id).all()
            self.assertTrue(any(pl.source == "manual" and pl.title == "Favorites" for pl in playlists))
            favorites = next(pl for pl in playlists if pl.title == "Favorites")
            keywords = sorted(kw.keyword for kw in favorites.keywords)
            self.assertEqual(keywords, ["default", "favorite"])
            items = db.query(backend_app.PlaylistItem).filter_by(playlist_id=favorites.id).all()
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].video_id, "9Pzj6U5c2cs")
            self.assertEqual(items[0].position, 1)
        finally:
            db.close()

    def test_update_playlist_keywords_and_visibility(self) -> None:
        playlist_id = self._create_sample_playlist()
        response = self.client.put(
            f"/channels/{self.channel_name}/playlists/{playlist_id}",
            json={"keywords": ["Focus", "", "LoFi"], "visibility": "PUBLIC"},
            headers=self._admin_headers(),
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["visibility"], "public")
        self.assertEqual(payload["keywords"], ["focus", "lofi"])

        db = backend_app.SessionLocal()
        try:
            playlist = db.get(backend_app.Playlist, playlist_id)
            self.assertIsNotNone(playlist)
            if playlist:
                self.assertEqual(playlist.visibility, "public")
                stored_keywords = sorted(kw.keyword for kw in playlist.keywords)
                self.assertEqual(stored_keywords, ["focus", "lofi"])
        finally:
            db.close()

    def test_delete_playlist_removes_related_rows(self) -> None:
        playlist_id = self._create_sample_playlist()
        response = self.client.delete(
            f"/channels/{self.channel_name}/playlists/{playlist_id}",
            headers=self._admin_headers(),
        )
        self.assertEqual(response.status_code, 204, response.text)

        db = backend_app.SessionLocal()
        try:
            playlists = db.query(backend_app.Playlist).all()
            self.assertFalse(playlists)
            self.assertFalse(db.query(backend_app.PlaylistItem).all())
            self.assertFalse(db.query(backend_app.PlaylistKeyword).all())
        finally:
            db.close()
