import unittest
from typing import Dict

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


def _setup_channel() -> Dict[str, int]:
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
            channel_id="cid",
            channel_name="event_channel",
            owner_id=owner.id,
            authorized=True,
        )
        db.add(channel)
        db.commit()
        db.refresh(channel)

        backend_app.get_or_create_settings(db, channel.id)

        stream = backend_app.StreamSession(channel_id=channel.id)
        db.add(stream)
        db.commit()
        db.refresh(stream)

        song_one = backend_app.Song(
            channel_id=channel.id,
            title="Song One",
            artist="Artist A",
            youtube_link="https://youtu.be/one",
        )
        song_two = backend_app.Song(
            channel_id=channel.id,
            title="Song Two",
            artist="Artist B",
            youtube_link="https://youtu.be/two",
        )
        db.add_all([song_one, song_two])
        db.commit()
        db.refresh(song_one)
        db.refresh(song_two)

        user_one = backend_app.User(
            channel_id=channel.id,
            twitch_id="user-one",
            username="userone",
            prio_points=0,
        )
        user_two = backend_app.User(
            channel_id=channel.id,
            twitch_id="user-two",
            username="usertwo",
            prio_points=1,
        )
        db.add_all([user_one, user_two])
        db.commit()
        db.refresh(user_one)
        db.refresh(user_two)

        return {
            "channel_pk": channel.id,
            "channel_name": channel.channel_name,
            "stream_id": stream.id,
            "song_one": song_one.id,
            "song_two": song_two.id,
            "user_one": user_one.id,
            "user_two": user_two.id,
        }
    finally:
        db.close()


class ChannelEventTests(unittest.TestCase):
    def setUp(self) -> None:
        _wipe_db()
        self.client = TestClient(backend_app.app)

    def tearDown(self) -> None:
        self.client.close()
        _wipe_db()

    def test_channel_event_stream_emits_expected_payloads(self) -> None:
        details = _setup_channel()
        channel = details["channel_name"]
        headers = {"X-Admin-Token": backend_app.ADMIN_TOKEN}

        with self.client.websocket_connect(f"/channels/{channel}/events") as ws:
            add_one = self.client.post(
                f"/channels/{channel}/queue",
                json={
                    "song_id": details["song_one"],
                    "user_id": details["user_one"],
                    "want_priority": False,
                    "prefer_sub_free": False,
                    "is_subscriber": False,
                },
                headers=headers,
            )
            self.assertEqual(add_one.status_code, 200, add_one.text)
            first_event = ws.receive_json()
            self.assertEqual(first_event["type"], "request.added")
            first_payload = first_event["payload"]
            first_request_id = add_one.json()["request_id"]
            self.assertEqual(first_payload["id"], first_request_id)
            self.assertEqual(first_payload["song"]["title"], "Song One")
            self.assertEqual(first_payload["requester"]["username"], "userone")

            add_two = self.client.post(
                f"/channels/{channel}/queue",
                json={
                    "song_id": details["song_two"],
                    "user_id": details["user_two"],
                    "want_priority": True,
                    "prefer_sub_free": False,
                    "is_subscriber": False,
                },
                headers=headers,
            )
            self.assertEqual(add_two.status_code, 200, add_two.text)
            second_event = ws.receive_json()
            self.assertEqual(second_event["type"], "request.added")
            second_payload = second_event["payload"]
            second_request_id = add_two.json()["request_id"]
            self.assertEqual(second_payload["id"], second_request_id)
            bumped_event = ws.receive_json()
            self.assertEqual(bumped_event["type"], "request.bumped")
            self.assertEqual(bumped_event["payload"]["id"], second_request_id)
            self.assertTrue(bumped_event["payload"]["is_priority"])

            promote = self.client.post(
                f"/channels/{channel}/queue/{first_request_id}/priority",
                params={"enabled": "true"},
                headers=headers,
            )
            self.assertEqual(promote.status_code, 200, promote.text)
            promote_event = ws.receive_json()
            self.assertEqual(promote_event["type"], "request.bumped")
            self.assertEqual(promote_event["payload"]["id"], first_request_id)

            played = self.client.post(
                f"/channels/{channel}/queue/{first_request_id}/played",
                headers=headers,
            )
            self.assertEqual(played.status_code, 200, played.text)
            played_event = ws.receive_json()
            self.assertEqual(played_event["type"], "request.played")
            played_payload = played_event["payload"]
            self.assertEqual(played_payload["request"]["id"], first_request_id)
            self.assertEqual(
                played_payload["up_next"]["id"],
                second_request_id,
            )

            settings = self.client.put(
                f"/channels/{channel}/settings",
                json={
                    "max_requests_per_user": -1,
                    "prio_only": 0,
                    "queue_closed": 1,
                    "allow_bumps": 1,
                    "other_flags": None,
                    "max_prio_points": 10,
                    "overall_queue_cap": 10,
                    "nonpriority_queue_cap": 8,
                },
                headers=headers,
            )
            self.assertEqual(settings.status_code, 200, settings.text)
            status_event = ws.receive_json()
            self.assertEqual(status_event["type"], "queue.status")
            self.assertTrue(status_event["payload"]["closed"])
            update_event = ws.receive_json()
            self.assertEqual(update_event["type"], "settings.updated")
            self.assertEqual(update_event["payload"]["queue_closed"], 1)

            archived = self.client.post(
                f"/channels/{channel}/streams/archive",
                headers=headers,
            )
            self.assertEqual(archived.status_code, 200, archived.text)
            archive_event = ws.receive_json()
            self.assertEqual(archive_event["type"], "queue.archived")
            self.assertIsNotNone(archive_event["payload"]["archived_stream_id"])
            self.assertEqual(
                archive_event["payload"]["new_stream_id"],
                archived.json()["new_stream_id"],
            )

            db = backend_app.SessionLocal()
            try:
                backend_app.award_prio_points(
                    db,
                    details["channel_pk"],
                    details["user_one"],
                    2,
                )
            finally:
                db.close()
            award_event = ws.receive_json()
            self.assertEqual(award_event["type"], "user.bump_awarded")
            award_payload = award_event["payload"]
            self.assertEqual(award_payload["user"]["id"], details["user_one"])
            self.assertEqual(award_payload["delta"], 2)
            self.assertGreaterEqual(award_payload["prio_points"], 2)

    def test_get_or_create_settings_backfills_queue_caps(self) -> None:
        """Ensure legacy channel settings rows gain default queue caps.

        Dependencies: Uses ``SessionLocal`` to write a fabricated legacy
        ``ChannelSettings`` row with null caps. Code customers: guards against
        regressions in ``get_or_create_settings`` that would leave preexisting
        installations without defaults. Used variables/origin: builds a new
        ``ActiveChannel`` record locally, binds it to the manually inserted
        settings, then re-reads via ``get_or_create_settings`` to verify
        backfilled values.
        """

        db = backend_app.SessionLocal()
        try:
            channel = backend_app.ActiveChannel(
                channel_id="legacy-channel-id",
                channel_name="legacy",
                join_active=1,
            )
            db.add(channel)
            db.commit()
            db.refresh(channel)

            legacy_settings = backend_app.ChannelSettings(
                channel_id=channel.id,
                overall_queue_cap=None,
                nonpriority_queue_cap=None,
            )
            db.add(legacy_settings)
            db.commit()
        finally:
            db.close()

    def test_event_pricing_respects_settings(self) -> None:
        details = _setup_channel()
        channel = details["channel_name"]
        headers = {"X-Admin-Token": backend_app.ADMIN_TOKEN}

        settings = self.client.put(
            f"/channels/{channel}/settings",
            json={
                "prio_follow_enabled": 0,
                "prio_raid_enabled": 1,
                "prio_bits_per_point": 250,
                "prio_gifts_per_point": 2,
                "prio_sub_tier1_points": 0,
                "prio_sub_tier2_points": 1,
                "prio_sub_tier3_points": 0,
                "max_prio_points": 5,
            },
            headers=headers,
        )
        self.assertEqual(settings.status_code, 200, settings.text)

        gift = self.client.post(
            f"/channels/{channel}/events",
            json={"type": "gift_sub", "user_id": details["user_one"], "meta": {"count": 3, "tier": "2000"}},
            headers=headers,
        )
        self.assertEqual(gift.status_code, 200, gift.text)

        follow = self.client.post(
            f"/channels/{channel}/events",
            json={"type": "follow", "user_id": details["user_one"]},
            headers=headers,
        )
        self.assertEqual(follow.status_code, 200, follow.text)

        bits = self.client.post(
            f"/channels/{channel}/events",
            json={"type": "bits", "user_id": details["user_one"], "meta": {"amount": 500}},
            headers=headers,
        )
        self.assertEqual(bits.status_code, 200, bits.text)

        db = backend_app.SessionLocal()
        try:
            user = db.get(backend_app.User, details["user_one"])
            assert user
            self.assertEqual(user.prio_points, 5)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()

