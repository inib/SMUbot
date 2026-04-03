import unittest
from unittest import mock
from typing import Dict
import tempfile
import os

from fastapi.testclient import TestClient
import hashlib
import hmac
import json
import requests

import backend_app


def _wipe_db() -> None:
    db = backend_app.SessionLocal()
    try:
        for model in [
            backend_app.Request,
            backend_app.Song,
            backend_app.User,
            backend_app.StreamSession,
            backend_app.Event,
            backend_app.EventSubscription,
            backend_app.EventSubMessageDedupe,
            backend_app.TwitchConduitShard,
            backend_app.TwitchConduit,
            backend_app.PlaylistItem,
            backend_app.PlaylistKeyword,
            backend_app.Playlist,
            backend_app.ChannelSettings,
            backend_app.ChannelModerator,
            backend_app.ActiveChannel,
            backend_app.TwitchUser,
            backend_app.BotConfig,
        ]:
            db.query(model).delete()
        db.commit()
    finally:
        db.close()
    with backend_app._BOT_TOKEN_REFRESH_LOCK:
        backend_app._BOT_TOKEN_REFRESH_STATE.update(
            {
                "last_attempt_at": None,
                "last_success_at": None,
                "last_failure_at": None,
                "last_error": None,
                "failure_count": 0,
            }
        )


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


def _create_chat_conduit_subscription(
    channel_pk: int,
    *,
    subscription_id: str,
    conduit_id: str,
    shard_id: str,
    secret: str,
) -> None:
    """Persist a conduit-backed chat subscription fixture for callback tests.

    Dependencies: EventSub conduit ORM rows and ``EventSubscription`` storage.
    Code customers: EventSub chat callback tests validating authoritative versus
    shadow behavior. Used variables/origin: identifiers and secrets are supplied
    per test to isolate signed payload fixtures.
    """

    db = backend_app.SessionLocal()
    try:
        conduit = backend_app.TwitchConduit(conduit_id=conduit_id, status="enabled")
        db.add(conduit)
        db.commit()
        db.refresh(conduit)
        db.add(
            backend_app.TwitchConduitShard(
                conduit_fk=conduit.id,
                shard_id=shard_id,
                transport_callback="https://example/callback",
                transport_secret=secret,
                status="enabled",
            )
        )
        db.add(
            backend_app.EventSubscription(
                channel_id=channel_pk,
                twitch_subscription_id=subscription_id,
                type="channel.chat.message",
                status="enabled",
                secret="legacy-subscription-secret",
                callback="https://example/callback",
                transport="conduit",
                conduit_id=conduit_id,
                shard_id=shard_id,
                meta=json.dumps(
                    {
                        "transport": {"method": "conduit", "conduit_id": conduit_id},
                        "conduit_shard": {"conduit_id": conduit_id, "shard_id": shard_id},
                        "shard_id": shard_id,
                    }
                ),
            )
        )
        db.commit()
    finally:
        db.close()


def _signed_eventsub_headers(secret: str, message_id: str, timestamp: str, raw_payload: bytes) -> Dict[str, str]:
    """Build Twitch EventSub callback headers with a valid HMAC signature.

    Dependencies: standard ``hashlib``/``hmac`` helpers.
    Code customers: webhook callback integration tests.
    Used variables/origin: per-test secret/message identifiers and serialized
    request body bytes.
    """

    digest = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw_payload, digestmod=hashlib.sha256)
    return {
        "Twitch-Eventsub-Message-Id": message_id,
        "Twitch-Eventsub-Message-Timestamp": timestamp,
        "Twitch-Eventsub-Message-Signature": f"sha256={digest.hexdigest()}",
        "Twitch-Eventsub-Message-Type": "notification",
    }


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

    def test_eventsub_callback_logs_events(self) -> None:
        details = _setup_channel()
        channel = details["channel_name"]
        secret = "abc123secret"
        db = backend_app.SessionLocal()
        try:
            sub = backend_app.EventSubscription(
                channel_id=details["channel_pk"],
                twitch_subscription_id="sub-1",
                type="channel.follow",
                status="enabled",
                secret=secret,
                callback="https://example/callback",
            )
            db.add(sub)
            db.commit()
        finally:
            db.close()

        body = {
            "subscription": {
                "id": "sub-1",
                "type": "channel.follow",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid", "moderator_user_id": "owner"},
            },
            "event": {
                "user_id": "user-three",
                "user_login": "userthree",
                "broadcaster_user_id": "cid",
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-1"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        headers = {
            "Twitch-Eventsub-Message-Id": message_id,
            "Twitch-Eventsub-Message-Timestamp": timestamp,
            "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
            "Twitch-Eventsub-Message-Type": "notification",
        }

        resp = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
        self.assertEqual(resp.status_code, 200, resp.text)

        events = self.client.get(
            f"/channels/{channel}/events",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
        ).json()
        self.assertEqual(events[0]["type"], "follow")
        db = backend_app.SessionLocal()
        try:
            user = (
                db.query(backend_app.User)
                .filter(backend_app.User.channel_id == details["channel_pk"], backend_app.User.twitch_id == "user-three")
                .one()
            )
            self.assertEqual(user.prio_points, 1)
        finally:
            db.close()

    def test_eventsub_callback_dedupes_notification_retries(self) -> None:
        """Ensure EventSub retries do not replay reward/command side effects.

        Dependencies: Uses the EventSub callback route plus the
        ``eventsub_message_dedupe`` table for idempotency checks. Code
        customers: webhook retry behavior from Twitch and chat/event command
        ingress processing. Used variables/origin: signs two identical payload
        deliveries with the same ``Twitch-Eventsub-Message-Id``.
        """

        details = _setup_channel()
        secret = "abc123secret"
        db = backend_app.SessionLocal()
        try:
            sub = backend_app.EventSubscription(
                channel_id=details["channel_pk"],
                twitch_subscription_id="sub-dedupe",
                type="channel.follow",
                status="enabled",
                secret=secret,
                callback="https://example/callback",
            )
            db.add(sub)
            db.commit()
        finally:
            db.close()

        body = {
            "subscription": {
                "id": "sub-dedupe",
                "type": "channel.follow",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid", "moderator_user_id": "owner"},
            },
            "event": {
                "user_id": "retry-user",
                "user_login": "retryuser",
                "broadcaster_user_id": "cid",
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-retry-1"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        headers = {
            "Twitch-Eventsub-Message-Id": message_id,
            "Twitch-Eventsub-Message-Timestamp": timestamp,
            "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
            "Twitch-Eventsub-Message-Type": "notification",
        }

        first = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
        second = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertTrue(second.json().get("deduped"))

        db = backend_app.SessionLocal()
        try:
            self.assertEqual(
                db.query(backend_app.Event).filter(backend_app.Event.channel_id == details["channel_pk"]).count(),
                1,
            )
            self.assertEqual(
                db.query(backend_app.EventSubMessageDedupe).filter(backend_app.EventSubMessageDedupe.message_id == message_id).count(),
                1,
            )
        finally:
            db.close()

    def test_eventsub_verification_accepts_subscription_shape(self) -> None:
        """Ensure classic verification payloads succeed via subscription secret.

        Dependencies: Persists an ``EventSubscription`` row and exercises
        ``/twitch/eventsub/callback`` signature validation.
        Code customers: Twitch classic webhook verification.
        Used variables/origin: Uses payload ``subscription.id`` and challenge.
        """

        details = _setup_channel()
        secret = "verify-sub-secret"
        db = backend_app.SessionLocal()
        try:
            db.add(
                backend_app.EventSubscription(
                    channel_id=details["channel_pk"],
                    twitch_subscription_id="sub-verify-1",
                    type="channel.follow",
                    status="enabled",
                    secret=secret,
                    callback="https://example/callback",
                )
            )
            db.commit()
        finally:
            db.close()

        body = {
            "subscription": {"id": "sub-verify-1", "status": "enabled", "type": "channel.follow", "version": "1"},
            "challenge": "challenge-sub",
        }
        raw = json.dumps(body).encode()
        message_id = "msg-verify-sub"
        timestamp = "2023-01-01T00:00:00Z"
        digest = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        response = self.client.post(
            "/twitch/eventsub/callback",
            data=raw,
            headers={
                "Twitch-Eventsub-Message-Id": message_id,
                "Twitch-Eventsub-Message-Timestamp": timestamp,
                "Twitch-Eventsub-Message-Signature": f"sha256={digest.hexdigest()}",
                "Twitch-Eventsub-Message-Type": "webhook_callback_verification",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.text, "challenge-sub")

    def test_eventsub_verification_accepts_conduit_shard_shape_without_subscription(self) -> None:
        """Ensure conduit-shard verification works without subscription payload.

        Dependencies: Persists conduit + shard metadata including
        ``transport_secret`` and calls callback route.
        Code customers: Twitch conduit shard verification callbacks.
        Used variables/origin: Uses payload ``conduit_shard.shard`` and
        ``conduit_shard.conduit_id`` context.
        """

        shard_secret = "verify-conduit-secret"
        db = backend_app.SessionLocal()
        try:
            conduit = backend_app.TwitchConduit(conduit_id="conduit-1", status="enabled")
            db.add(conduit)
            db.commit()
            db.refresh(conduit)
            db.add(
                backend_app.TwitchConduitShard(
                    conduit_fk=conduit.id,
                    shard_id="0",
                    transport_callback="https://example/callback",
                    transport_secret=shard_secret,
                    status="enabled",
                )
            )
            db.commit()
        finally:
            db.close()

        body = {
            "conduit_shard": {"shard": "0", "conduit_id": "conduit-1"},
            "challenge": "challenge-conduit",
        }
        raw = json.dumps(body).encode()
        message_id = "msg-verify-conduit"
        timestamp = "2023-01-01T00:00:00Z"
        digest = hmac.new(shard_secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        response = self.client.post(
            "/twitch/eventsub/callback",
            data=raw,
            headers={
                "Twitch-Eventsub-Message-Id": message_id,
                "Twitch-Eventsub-Message-Timestamp": timestamp,
                "Twitch-Eventsub-Message-Signature": f"sha256={digest.hexdigest()}",
                "Twitch-Eventsub-Message-Type": "webhook_callback_verification",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.text, "challenge-conduit")

    def test_eventsub_conduit_verification_refreshes_shard_status_from_twitch(self) -> None:
        """Refresh shard status after successful conduit verification callbacks.

        Dependencies: Uses callback signature verification plus mocked
        ``_eventsub_app_headers`` and Twitch shard list HTTP response.
        Code customers: EventSub health diagnostics that should stop showing
        stale ``webhook_callback_verification_pending`` shard status.
        Used variables/origin: Starts local shard status as pending and uses
        mocked Helix response data to transition it to enabled.
        """

        shard_secret = "verify-refresh-secret"
        db = backend_app.SessionLocal()
        try:
            conduit = backend_app.TwitchConduit(conduit_id="conduit-refresh", status="enabled")
            db.add(conduit)
            db.commit()
            db.refresh(conduit)
            db.add(
                backend_app.TwitchConduitShard(
                    conduit_fk=conduit.id,
                    shard_id="0",
                    transport_callback="https://example/callback",
                    transport_secret=shard_secret,
                    status="webhook_callback_verification_pending",
                )
            )
            db.commit()
        finally:
            db.close()

        body = {
            "conduit_shard": {"shard": "0", "conduit_id": "conduit-refresh"},
            "challenge": "challenge-refresh",
        }
        raw = json.dumps(body).encode()
        message_id = "msg-verify-refresh"
        timestamp = "2023-01-01T00:00:00Z"
        digest = hmac.new(shard_secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "_eventsub_app_headers", return_value={"Client-Id": "cid"}):
            fake_response = mock.Mock()
            fake_response.raise_for_status.return_value = None
            fake_response.json.return_value = {
                "data": [
                    {
                        "id": "0",
                        "status": "enabled",
                        "transport": {"method": "webhook", "callback": "https://example/callback"},
                    }
                ]
            }
            with mock.patch("backend_app.requests.get", return_value=fake_response):
                response = self.client.post(
                    "/twitch/eventsub/callback",
                    data=raw,
                    headers={
                        "Twitch-Eventsub-Message-Id": message_id,
                        "Twitch-Eventsub-Message-Timestamp": timestamp,
                        "Twitch-Eventsub-Message-Signature": f"sha256={digest.hexdigest()}",
                        "Twitch-Eventsub-Message-Type": "webhook_callback_verification",
                    },
                )
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            shard_row = (
                db.query(backend_app.TwitchConduitShard)
                .join(backend_app.TwitchConduit, backend_app.TwitchConduitShard.conduit_fk == backend_app.TwitchConduit.id)
                .filter(
                    backend_app.TwitchConduit.conduit_id == "conduit-refresh",
                    backend_app.TwitchConduitShard.shard_id == "0",
                )
                .one()
            )
            self.assertEqual(shard_row.status, "enabled")
            self.assertIsNotNone(shard_row.last_sync_at)
        finally:
            db.close()

    def test_eventsub_verification_rejects_missing_conduit_shard_field_shard(self) -> None:
        """Reject conduit verification payloads that omit ``conduit_shard.shard``.

        Dependencies: Persists conduit + shard metadata and calls callback route.
        Code customers: Twitch conduit shard verification payload validation.
        Used variables/origin: Sends a conduit payload with only
        ``conduit_shard.conduit_id`` to assert reason-code enforcement.
        """

        shard_secret = "verify-conduit-secret"
        db = backend_app.SessionLocal()
        try:
            conduit = backend_app.TwitchConduit(conduit_id="conduit-2", status="enabled")
            db.add(conduit)
            db.commit()
            db.refresh(conduit)
            db.add(
                backend_app.TwitchConduitShard(
                    conduit_fk=conduit.id,
                    shard_id="7",
                    transport_callback="https://example/callback",
                    transport_secret=shard_secret,
                    status="enabled",
                )
            )
            db.commit()
        finally:
            db.close()

        body = {"conduit_shard": {"conduit_id": "conduit-2"}, "challenge": "challenge-missing-shard"}
        raw = json.dumps(body).encode()
        message_id = "msg-verify-missing-shard"
        timestamp = "2023-01-01T00:00:00Z"
        digest = hmac.new(shard_secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        response = self.client.post(
            "/twitch/eventsub/callback",
            data=raw,
            headers={
                "Twitch-Eventsub-Message-Id": message_id,
                "Twitch-Eventsub-Message-Timestamp": timestamp,
                "Twitch-Eventsub-Message-Signature": f"sha256={digest.hexdigest()}",
                "Twitch-Eventsub-Message-Type": "webhook_callback_verification",
            },
        )
        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(response.json(), {"detail": "missing_conduit_shard_field_shard"})

    def test_eventsub_verification_rejects_invalid_signature_for_subscription_and_conduit(self) -> None:
        """Ensure invalid signatures are rejected for both verification shapes.

        Dependencies: Persists subscription and conduit shard secrets then hits
        callback route with intentionally mismatched HMAC signatures.
        Code customers: Signature integrity checks for callback verification.
        Used variables/origin: Uses wrong local signing secret for each shape.
        """

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            db.add(
                backend_app.EventSubscription(
                    channel_id=details["channel_pk"],
                    twitch_subscription_id="sub-verify-bad",
                    type="channel.follow",
                    status="enabled",
                    secret="good-sub-secret",
                    callback="https://example/callback",
                )
            )
            conduit = backend_app.TwitchConduit(conduit_id="conduit-bad", status="enabled")
            db.add(conduit)
            db.commit()
            db.refresh(conduit)
            db.add(
                backend_app.TwitchConduitShard(
                    conduit_fk=conduit.id,
                    shard_id="1",
                    transport_callback="https://example/callback",
                    transport_secret="good-conduit-secret",
                    status="enabled",
                )
            )
            db.commit()
        finally:
            db.close()

        sub_body = {"subscription": {"id": "sub-verify-bad", "type": "channel.follow"}, "challenge": "x"}
        sub_raw = json.dumps(sub_body).encode()
        sub_digest = hmac.new(
            b"wrong-sub-secret",
            msg=("msg-verify-bad-sub" + "2023-01-01T00:00:00Z").encode() + sub_raw,
            digestmod=hashlib.sha256,
        )
        sub_resp = self.client.post(
            "/twitch/eventsub/callback",
            data=sub_raw,
            headers={
                "Twitch-Eventsub-Message-Id": "msg-verify-bad-sub",
                "Twitch-Eventsub-Message-Timestamp": "2023-01-01T00:00:00Z",
                "Twitch-Eventsub-Message-Signature": f"sha256={sub_digest.hexdigest()}",
                "Twitch-Eventsub-Message-Type": "webhook_callback_verification",
            },
        )
        self.assertEqual(sub_resp.status_code, 403, sub_resp.text)

        conduit_body = {"conduit_shard": {"shard": "1", "conduit_id": "conduit-bad"}, "challenge": "y"}
        conduit_raw = json.dumps(conduit_body).encode()
        conduit_digest = hmac.new(
            b"wrong-conduit-secret",
            msg=("msg-verify-bad-conduit" + "2023-01-01T00:00:00Z").encode() + conduit_raw,
            digestmod=hashlib.sha256,
        )
        conduit_resp = self.client.post(
            "/twitch/eventsub/callback",
            data=conduit_raw,
            headers={
                "Twitch-Eventsub-Message-Id": "msg-verify-bad-conduit",
                "Twitch-Eventsub-Message-Timestamp": "2023-01-01T00:00:00Z",
                "Twitch-Eventsub-Message-Signature": f"sha256={conduit_digest.hexdigest()}",
                "Twitch-Eventsub-Message-Type": "webhook_callback_verification",
            },
        )
        self.assertEqual(conduit_resp.status_code, 403, conduit_resp.text)

    def test_eventsub_chat_notification_shadow_mode_records_dedupe(self) -> None:
        """Verify chat webhook notifications in shadow mode stay non-authoritative.

        Dependencies: EventSub callback routing, system settings persistence, and
        dedupe storage. Code customers: dual-path webhook/websocket ingress
        rollout validation. Used variables/origin: toggles
        ``chat_ingress_shadow_mode`` and sends a signed ``channel.chat.message``
        payload.
        """

        details = _setup_channel()
        secret = "chatsecret"
        conduit_id = "conduit-chat-shadow"
        shard_id = "2"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "websocket", "chat_ingress_shadow_mode": "1"})
            conduit = backend_app.TwitchConduit(conduit_id=conduit_id, status="enabled")
            db.add(conduit)
            db.commit()
            db.refresh(conduit)
            db.add(
                backend_app.TwitchConduitShard(
                    conduit_fk=conduit.id,
                    shard_id=shard_id,
                    transport_callback="https://example/callback",
                    transport_secret=secret,
                    status="enabled",
                )
            )
            chat_sub = backend_app.EventSubscription(
                channel_id=details["channel_pk"],
                twitch_subscription_id="sub-chat-shadow",
                type="channel.chat.message",
                status="enabled",
                secret="legacy-subscription-secret",
                callback="https://example/callback",
                transport="conduit",
                conduit_id=conduit_id,
                shard_id=shard_id,
                meta=json.dumps(
                    {
                        "transport": {"method": "conduit", "conduit_id": conduit_id},
                        "conduit_shard": {"conduit_id": conduit_id, "shard_id": shard_id},
                        "shard_id": shard_id,
                    }
                ),
            )
            db.add(chat_sub)
            db.commit()
        finally:
            db.close()

        body = {
            "subscription": {
                "id": "sub-chat-shadow",
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "chat-user-1",
                "chatter_user_login": "chatuser",
                "message": {"text": "!request artist - title"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-chat-shadow"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        headers = {
            "Twitch-Eventsub-Message-Id": message_id,
            "Twitch-Eventsub-Message-Timestamp": timestamp,
            "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
            "Twitch-Eventsub-Message-Type": "notification",
        }
        response = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            self.assertEqual(
                db.query(backend_app.EventSubMessageDedupe).filter(backend_app.EventSubMessageDedupe.message_id == message_id).count(),
                1,
            )
            sub_row = (
                db.query(backend_app.EventSubscription)
                .filter(backend_app.EventSubscription.twitch_subscription_id == "sub-chat-shadow")
                .one()
            )
            self.assertIsNotNone(sub_row.last_notified_at)
            self.assertEqual(db.query(backend_app.Event).filter(backend_app.Event.channel_id == details["channel_pk"]).count(), 0)
        finally:
            db.close()

    def test_eventsub_chat_notification_authoritative_request_mutates_queue(self) -> None:
        """Execute authoritative webhook request command, mutate queue, and send reply."""

        details = _setup_channel()
        secret = "chatsecret-authoritative"
        conduit_id = "conduit-chat-authoritative"
        shard_id = "3"
        subscription_id = "sub-chat-authoritative"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-1",
                "chatter_user_login": "webhookuser",
                "message": {"text": "!request Artist C - Song Three"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-chat-authoritative"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
            backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"
        ), mock.patch("backend_app.requests.post") as mock_send:
            mock_send.return_value.raise_for_status.return_value = None
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers={
                    "Twitch-Eventsub-Message-Id": message_id,
                    "Twitch-Eventsub-Message-Timestamp": timestamp,
                    "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                    "Twitch-Eventsub-Message-Type": "notification",
                },
            )
            self.assertEqual(mock_send.call_count, 1)
            payload = mock_send.call_args.kwargs["json"]
            self.assertEqual(payload["broadcaster_id"], "cid")
            self.assertEqual(payload["sender_id"], "bot-user-1")
            self.assertEqual(payload["message"], "Added: Artist C - Song Three")
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            rows = db.query(backend_app.Request).filter(backend_app.Request.channel_id == details["channel_pk"]).all()
            self.assertEqual(len(rows), 1)
            song = db.get(backend_app.Song, rows[0].song_id)
            self.assertIsNotNone(song)
            self.assertEqual(song.artist, "Artist C")
            self.assertEqual(song.title, "Song Three")
        finally:
            db.close()

    def test_eventsub_chat_notification_authoritative_request_accepts_youtube_url(self) -> None:
        """Accept direct YouTube URLs and derive artist/title from oEmbed title text."""

        details = _setup_channel()
        secret = "chatsecret-authoritative-url"
        conduit_id = "conduit-chat-authoritative-url"
        shard_id = "4"
        subscription_id = "sub-chat-authoritative-url"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-url",
                "chatter_user_login": "webhookuserurl",
                "message": {"text": "!request https://youtu.be/TvZskcqdYcE"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-chat-authoritative-url"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "_fetch_youtube_oembed_title", return_value="Artist D - Song Four"), mock.patch.object(
            backend_app, "get_bot_user_id", return_value="bot-user-1"
        ), mock.patch.object(backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"), mock.patch(
            "backend_app.requests.post"
        ) as mock_send:
            mock_send.return_value.raise_for_status.return_value = None
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers={
                    "Twitch-Eventsub-Message-Id": message_id,
                    "Twitch-Eventsub-Message-Timestamp": timestamp,
                    "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                    "Twitch-Eventsub-Message-Type": "notification",
                },
            )
            self.assertEqual(mock_send.call_count, 1)
            payload = mock_send.call_args.kwargs["json"]
            self.assertEqual(payload["message"], "Added: Artist D - Song Four")
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            rows = db.query(backend_app.Request).filter(backend_app.Request.channel_id == details["channel_pk"]).all()
            self.assertEqual(len(rows), 1)
            song = db.get(backend_app.Song, rows[0].song_id)
            self.assertIsNotNone(song)
            self.assertEqual(song.artist, "Artist D")
            self.assertEqual(song.title, "Song Four")
            self.assertEqual(song.youtube_link, "https://www.youtube.com/watch?v=TvZskcqdYcE")
        finally:
            db.close()

    def test_eventsub_chat_notification_authoritative_request_extracts_embedded_youtube_url(self) -> None:
        """Extract YouTube URLs from mixed request text like websocket command mode."""

        details = _setup_channel()
        secret = "chatsecret-authoritative-url-embedded"
        conduit_id = "conduit-chat-authoritative-url-embedded"
        shard_id = "14"
        subscription_id = "sub-chat-authoritative-url-embedded"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-url-embedded",
                "chatter_user_login": "webhookuserurlembedded",
                "message": {"text": "!request check this out https://youtu.be/TvZskcqdYcE"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-chat-authoritative-url-embedded"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "_fetch_youtube_oembed_title", return_value="Artist D - Song Four"), mock.patch.object(
            backend_app, "get_bot_user_id", return_value="bot-user-1"
        ), mock.patch.object(backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"), mock.patch(
            "backend_app.requests.post"
        ) as mock_send:
            mock_send.return_value.raise_for_status.return_value = None
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers={
                    "Twitch-Eventsub-Message-Id": message_id,
                    "Twitch-Eventsub-Message-Timestamp": timestamp,
                    "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                    "Twitch-Eventsub-Message-Type": "notification",
                },
            )
            self.assertEqual(mock_send.call_count, 1)
            payload = mock_send.call_args.kwargs["json"]
            self.assertEqual(payload["message"], "Added: Artist D - Song Four")
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            rows = db.query(backend_app.Request).filter(backend_app.Request.channel_id == details["channel_pk"]).all()
            self.assertEqual(len(rows), 1)
            song = db.get(backend_app.Song, rows[0].song_id)
            self.assertIsNotNone(song)
            self.assertEqual(song.artist, "Artist D")
            self.assertEqual(song.title, "Song Four")
            self.assertEqual(song.youtube_link, "https://www.youtube.com/watch?v=TvZskcqdYcE")
        finally:
            db.close()

    def test_eventsub_chat_notification_authoritative_request_plain_title_fallback(self) -> None:
        """Treat plain non-URL request text as valid and fall back to Unknown artist."""

        details = _setup_channel()
        secret = "chatsecret-authoritative-fallback"
        conduit_id = "conduit-chat-authoritative-fallback"
        shard_id = "5"
        subscription_id = "sub-chat-authoritative-fallback"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-fallback",
                "chatter_user_login": "webhookuserfallback",
                "message": {"text": "!request Song Without Dash"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-chat-authoritative-fallback"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
            backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"
        ), mock.patch("backend_app.requests.post") as mock_send:
            mock_send.return_value.raise_for_status.return_value = None
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers={
                    "Twitch-Eventsub-Message-Id": message_id,
                    "Twitch-Eventsub-Message-Timestamp": timestamp,
                    "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                    "Twitch-Eventsub-Message-Type": "notification",
                },
            )
            self.assertEqual(mock_send.call_count, 1)
            payload = mock_send.call_args.kwargs["json"]
            self.assertEqual(payload["message"], "Added: Unknown - Song Without Dash")
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            rows = db.query(backend_app.Request).filter(backend_app.Request.channel_id == details["channel_pk"]).all()
            self.assertEqual(len(rows), 1)
            song = db.get(backend_app.Song, rows[0].song_id)
            self.assertIsNotNone(song)
            self.assertEqual(song.artist, "Unknown")
            self.assertEqual(song.title, "Song Without Dash")
            self.assertIsNone(song.youtube_link)
        finally:
            db.close()

    def test_eventsub_chat_notification_reply_parent_uses_event_message_id(self) -> None:
        """Send replies threaded to ``event.message_id`` instead of header message id."""

        details = _setup_channel()
        secret = "chatsecret-event-message-id"
        conduit_id = "conduit-chat-event-message-id"
        shard_id = "13"
        subscription_id = "sub-chat-event-message-id"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "message_id": "event-message-id-123",
                "chatter_user_id": "webhook-user-2",
                "chatter_user_login": "webhookuser2",
                "message": {"text": "!request Artist E - Song Five"},
            },
        }
        raw = json.dumps(body).encode()
        header_message_id = "header-message-id-999"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(header_message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
            backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"
        ), mock.patch("backend_app.requests.post") as mock_send:
            mock_send.return_value.raise_for_status.return_value = None
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers={
                    "Twitch-Eventsub-Message-Id": header_message_id,
                    "Twitch-Eventsub-Message-Timestamp": timestamp,
                    "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                    "Twitch-Eventsub-Message-Type": "notification",
                },
            )
            payload = mock_send.call_args.kwargs["json"]
            self.assertEqual(payload["reply_parent_message_id"], "event-message-id-123")
        self.assertEqual(response.status_code, 200, response.text)

    def test_eventsub_chat_notification_reply_parent_omitted_when_event_message_id_missing(self) -> None:
        """Omit reply threading when ``event.message_id`` is absent."""

        details = _setup_channel()
        secret = "chatsecret-no-event-message-id"
        conduit_id = "conduit-chat-no-event-message-id"
        shard_id = "14"
        subscription_id = "sub-chat-no-event-message-id"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-3",
                "chatter_user_login": "webhookuser3",
                "message": {"text": "!request Artist F - Song Six"},
            },
        }
        raw = json.dumps(body).encode()
        header_message_id = "header-message-id-omit-reply-parent"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(header_message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
            backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"
        ), mock.patch("backend_app.requests.post") as mock_send:
            mock_send.return_value.raise_for_status.return_value = None
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers={
                    "Twitch-Eventsub-Message-Id": header_message_id,
                    "Twitch-Eventsub-Message-Timestamp": timestamp,
                    "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                    "Twitch-Eventsub-Message-Type": "notification",
                },
            )
            payload = mock_send.call_args.kwargs["json"]
            self.assertNotIn("reply_parent_message_id", payload)
        self.assertEqual(response.status_code, 200, response.text)

    def test_eventsub_chat_notification_preflight_sender_subject_mismatch_skips_send(self) -> None:
        """Skip deterministic bad payload sends when sender and token subject mismatch."""

        details = _setup_channel()
        secret = "chatsecret-sender-mismatch"
        conduit_id = "conduit-chat-sender-mismatch"
        shard_id = "15"
        subscription_id = "sub-chat-sender-mismatch"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "message_id": "event-message-id-sender-mismatch",
                "chatter_user_id": "webhook-user-4",
                "chatter_user_login": "webhookuser4",
                "message": {"text": "!request Artist G - Song Seven"},
            },
        }
        raw = json.dumps(body).encode()
        header_message_id = "header-message-id-sender-mismatch"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(header_message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
            backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-2"
        ), mock.patch("backend_app.requests.post") as mock_send:
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers={
                    "Twitch-Eventsub-Message-Id": header_message_id,
                    "Twitch-Eventsub-Message-Timestamp": timestamp,
                    "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                    "Twitch-Eventsub-Message-Type": "notification",
                },
            )
            self.assertEqual(mock_send.call_count, 0)
        self.assertEqual(response.status_code, 200, response.text)

    def test_send_eventsub_chat_reply_preflight_invalid_message_length_skips_request(self) -> None:
        """Skip Send Chat API call when rendered message violates Twitch length limits."""

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            channel = db.get(backend_app.ActiveChannel, details["channel_pk"])
            self.assertIsNotNone(channel)
            over_limit_message = "x" * (backend_app.TWITCH_SEND_CHAT_MESSAGE_MAX_LENGTH + 1)
            reply = {
                "status": "success",
                "template_key": over_limit_message,
                "template_vars": {},
                "visibility": "normal",
            }
            with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
                backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"
            ), mock.patch("backend_app.requests.post") as mock_send:
                sent = backend_app._send_eventsub_chat_reply(
                    db,
                    channel,
                    reply,
                    reply_parent_message_id="event-message-id-over-limit",
                )
                self.assertFalse(sent)
                self.assertEqual(mock_send.call_count, 0)
        finally:
            db.close()

    def test_preflight_eventsub_chat_reply_payload_uses_app_auth_policy_headers(self) -> None:
        """Use app-auth headers for Send Chat when auth policy selects app mode."""

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            channel = db.get(backend_app.ActiveChannel, details["channel_pk"])
            self.assertIsNotNone(channel)
            with mock.patch.object(
                backend_app,
                "get_twitch_send_chat_auth_mode",
                return_value=backend_app.TWITCH_SEND_CHAT_AUTH_MODE_APP,
            ), mock.patch.object(
                backend_app,
                "_eventsub_app_headers",
                return_value={"Authorization": "Bearer app-token", "Client-Id": "cid"},
            ) as mock_app_headers, mock.patch.object(
                backend_app,
                "_resolve_twitch_token_subject_id",
            ) as mock_subject:
                payload, reason_code, headers = backend_app._preflight_eventsub_chat_reply_payload(
                    db,
                    channel=channel,
                    sender_id="bot-user-1",
                    message="hello from webhook",
                    reply_parent_message_id="parent-msg-id",
                )
            self.assertIsNone(reason_code)
            self.assertEqual(payload["sender_id"], "bot-user-1")
            self.assertEqual(headers, {"Authorization": "Bearer app-token", "Client-Id": "cid"})
            self.assertEqual(mock_app_headers.call_count, 1)
            self.assertEqual(mock_subject.call_count, 0)
        finally:
            db.close()

    def test_preflight_eventsub_chat_reply_payload_uses_app_headers_when_auth_mode_unset(self) -> None:
        """Default missing auth-mode setting to app headers during preflight."""

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.AppSetting).filter(backend_app.AppSetting.key == "twitch_send_chat_auth_mode").delete()
            db.commit()
            backend_app.settings_store.invalidate()
            channel = db.get(backend_app.ActiveChannel, details["channel_pk"])
            self.assertIsNotNone(channel)
            with mock.patch.object(
                backend_app,
                "_eventsub_app_headers",
                return_value={"Authorization": "Bearer app-token", "Client-Id": "cid"},
            ) as mock_app_headers, mock.patch.object(
                backend_app,
                "_resolve_twitch_token_subject_id",
            ) as mock_subject:
                payload, reason_code, headers = backend_app._preflight_eventsub_chat_reply_payload(
                    db,
                    channel=channel,
                    sender_id="bot-user-1",
                    message="hello from webhook",
                    reply_parent_message_id=None,
                )
            self.assertIsNone(reason_code)
            self.assertIsNotNone(payload)
            self.assertEqual(headers, {"Authorization": "Bearer app-token", "Client-Id": "cid"})
            self.assertEqual(mock_app_headers.call_count, 1)
            self.assertEqual(mock_subject.call_count, 0)
        finally:
            db.close()

    def test_preflight_eventsub_chat_reply_payload_uses_app_headers_when_default_selected(self) -> None:
        """Treat explicit default auth-mode value as app-header policy."""

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"twitch_send_chat_auth_mode": backend_app.TWITCH_SEND_CHAT_AUTH_MODE_DEFAULT})
            channel = db.get(backend_app.ActiveChannel, details["channel_pk"])
            self.assertIsNotNone(channel)
            with mock.patch.object(
                backend_app,
                "_eventsub_app_headers",
                return_value={"Authorization": "Bearer app-token", "Client-Id": "cid"},
            ) as mock_app_headers, mock.patch.object(
                backend_app,
                "_resolve_twitch_token_subject_id",
            ) as mock_subject:
                payload, reason_code, headers = backend_app._preflight_eventsub_chat_reply_payload(
                    db,
                    channel=channel,
                    sender_id="bot-user-1",
                    message="hello from webhook",
                    reply_parent_message_id=None,
                )
            self.assertIsNone(reason_code)
            self.assertIsNotNone(payload)
            self.assertEqual(headers, {"Authorization": "Bearer app-token", "Client-Id": "cid"})
            self.assertEqual(mock_app_headers.call_count, 1)
            self.assertEqual(mock_subject.call_count, 0)
        finally:
            db.close()

    def test_preflight_eventsub_chat_reply_payload_skips_user_subject_checks_in_app_mode(self) -> None:
        """Avoid user-token-only preflight failures when app-auth mode is selected."""

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            channel = db.get(backend_app.ActiveChannel, details["channel_pk"])
            self.assertIsNotNone(channel)
            with mock.patch.object(
                backend_app,
                "get_twitch_send_chat_auth_mode",
                return_value=backend_app.TWITCH_SEND_CHAT_AUTH_MODE_APP,
            ), mock.patch.object(
                backend_app,
                "_eventsub_app_headers",
                return_value={"Authorization": "Bearer app-token", "Client-Id": "cid"},
            ), mock.patch.object(
                backend_app,
                "_resolve_twitch_token_subject_id",
                return_value=None,
            ):
                payload, reason_code, headers = backend_app._preflight_eventsub_chat_reply_payload(
                    db,
                    channel=channel,
                    sender_id="bot-user-1",
                    message="hello",
                    reply_parent_message_id=None,
                )
            self.assertIsNone(reason_code)
            self.assertIsNotNone(payload)
            self.assertEqual(headers, {"Authorization": "Bearer app-token", "Client-Id": "cid"})
        finally:
            db.close()

    def test_send_eventsub_chat_reply_400_maps_reason_and_skips_retry(self) -> None:
        """Map deterministic Twitch 400 reply diagnostics and avoid retries."""

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            channel = db.get(backend_app.ActiveChannel, details["channel_pk"])
            self.assertIsNotNone(channel)
            reply = {"status": "success", "template_key": "queue_open", "template_vars": {}, "visibility": "normal"}

            response = requests.Response()
            response.status_code = 400
            response._content = b'{"error":"Bad Request","status":400,"message":"reply_parent_message_id is invalid"}'
            error = requests.HTTPError(response=response)
            with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
                backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"
            ), mock.patch("backend_app.requests.post", side_effect=error) as mock_send:
                sent = backend_app._send_eventsub_chat_reply(
                    db,
                    channel,
                    reply,
                    reply_parent_message_id="bad-parent-id",
                )
            self.assertFalse(sent)
            self.assertEqual(mock_send.call_count, 1)
            snapshot = backend_app._ingress_metrics_snapshot(backend_app.datetime.utcnow())
            reasons = snapshot.get("send_api_failure_reasons") or {}
            self.assertGreaterEqual(reasons.get("invalid_reply_parent_message_id", 0), 1)
        finally:
            db.close()

    def test_send_eventsub_chat_reply_5xx_retries_and_tracks_transient_reason(self) -> None:
        """Retry transient Send Chat API failures and classify reason as transient 5xx."""

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            channel = db.get(backend_app.ActiveChannel, details["channel_pk"])
            self.assertIsNotNone(channel)
            reply = {"status": "success", "template_key": "queue_open", "template_vars": {}, "visibility": "normal"}

            response = requests.Response()
            response.status_code = 503
            response._content = b'{"error":"Service Unavailable","status":503,"message":"server overloaded"}'
            error = requests.HTTPError(response=response)
            with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
                backend_app, "_resolve_twitch_token_subject_id", return_value="bot-user-1"
            ), mock.patch("backend_app.requests.post", side_effect=error) as mock_send, mock.patch(
                "backend_app.time.sleep", return_value=None
            ):
                sent = backend_app._send_eventsub_chat_reply(
                    db,
                    channel,
                    reply,
                    reply_parent_message_id="parent-id",
                )
            self.assertFalse(sent)
            self.assertEqual(mock_send.call_count, 3)
            snapshot = backend_app._ingress_metrics_snapshot(backend_app.datetime.utcnow())
            reasons = snapshot.get("send_api_failure_reasons") or {}
            self.assertGreaterEqual(reasons.get("transient_5xx", 0), 1)
        finally:
            db.close()

    def test_eventsub_chat_notification_shadow_mode_request_does_not_mutate_queue(self) -> None:
        """Keep webhook chat commands parse-only when shadow mode is enabled."""

        details = _setup_channel()
        secret = "chatsecret-shadow-no-mutate"
        conduit_id = "conduit-chat-shadow-no-mutate"
        shard_id = "4"
        subscription_id = "sub-chat-shadow-no-mutate"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "1"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )
        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-shadow",
                "chatter_user_login": "webhookshadow",
                "message": {"text": "!request Artist D - Song Four"},
            },
        }
        raw = json.dumps(body).encode()
        response = self.client.post(
            "/twitch/eventsub/callback",
            data=raw,
            headers=_signed_eventsub_headers(secret, "msg-chat-shadow-no-mutate", "2023-01-01T00:00:00Z", raw),
        )
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            rows = db.query(backend_app.Request).filter(backend_app.Request.channel_id == details["channel_pk"]).all()
            self.assertEqual(len(rows), 0)
        finally:
            db.close()

    def test_eventsub_chat_notification_authoritative_points_sends_reply(self) -> None:
        """Reply to ``!points`` in authoritative mode without mutating the queue."""

        details = _setup_channel()
        secret = "chatsecret-points"
        conduit_id = "conduit-chat-points"
        shard_id = "8"
        subscription_id = "sub-chat-points"
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
            db.add(
                backend_app.User(
                    channel_id=details["channel_pk"],
                    twitch_id="webhook-user-points",
                    username="pointsuser",
                    prio_points=7,
                )
            )
            db.commit()
        finally:
            db.close()
        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-points",
                "chatter_user_login": "pointsuser",
                "message": {"text": "!points"},
            },
        }
        raw = json.dumps(body).encode()
        with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
            backend_app, "_eventsub_bot_headers", return_value={"Authorization": "Bearer token", "Client-Id": "cid"}
        ), mock.patch("backend_app.requests.post") as mock_send:
            mock_send.return_value.raise_for_status.return_value = None
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers=_signed_eventsub_headers(secret, "msg-chat-points", "2023-01-01T00:00:00Z", raw),
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(mock_send.call_count, 1)
            self.assertEqual(
                mock_send.call_args.kwargs["json"]["message"],
                "pointsuser has 7 points",
            )

    def test_eventsub_chat_notification_authoritative_reply_respects_mute_policy(self) -> None:
        """Suppress webhook reply sends when the channel bot message level is mute."""

        details = _setup_channel()
        secret = "chatsecret-mute"
        conduit_id = "conduit-chat-mute"
        shard_id = "6"
        subscription_id = "sub-chat-mute"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(
                db,
                {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"},
            )
            settings = backend_app.get_or_create_settings(db, details["channel_pk"])
            settings.bot_message_level = "mute"
            db.commit()
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-4",
                "chatter_user_login": "webhookuser4",
                "message": {"text": "!request Artist E - Song Five"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-chat-mute"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
            backend_app, "_eventsub_bot_headers", return_value={"Authorization": "Bearer token", "Client-Id": "cid"}
        ), mock.patch("backend_app.requests.post") as mock_send:
            response = self.client.post(
                "/twitch/eventsub/callback",
                data=raw,
                headers={
                    "Twitch-Eventsub-Message-Id": message_id,
                    "Twitch-Eventsub-Message-Timestamp": timestamp,
                    "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                    "Twitch-Eventsub-Message-Type": "notification",
                },
            )
            self.assertEqual(mock_send.call_count, 0)
        self.assertEqual(response.status_code, 200, response.text)

    def test_eventsub_chat_notification_retry_does_not_duplicate_reply(self) -> None:
        """Ignore deduped retries so webhook replies are not sent twice."""

        details = _setup_channel()
        secret = "chatsecret-retry-reply"
        conduit_id = "conduit-chat-retry-reply"
        shard_id = "7"
        subscription_id = "sub-chat-retry-reply"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-5",
                "chatter_user_login": "webhookuser5",
                "message": {"text": "!request Artist F - Song Six"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-chat-retry-reply"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        headers = {
            "Twitch-Eventsub-Message-Id": message_id,
            "Twitch-Eventsub-Message-Timestamp": timestamp,
            "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
            "Twitch-Eventsub-Message-Type": "notification",
        }
        with mock.patch.object(backend_app, "get_bot_user_id", return_value="bot-user-1"), mock.patch.object(
            backend_app, "_eventsub_bot_headers", return_value={"Authorization": "Bearer token", "Client-Id": "cid"}
        ), mock.patch("backend_app.requests.post") as mock_send:
            mock_send.return_value.raise_for_status.return_value = None
            first = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
            second = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
            self.assertEqual(first.status_code, 200, first.text)
            self.assertEqual(second.status_code, 200, second.text)
            self.assertEqual(mock_send.call_count, 1)
        db = backend_app.SessionLocal()
        try:
            requests_for_channel = db.query(backend_app.Request).filter(backend_app.Request.channel_id == details["channel_pk"]).count()
            self.assertEqual(requests_for_channel, 1)
        finally:
            db.close()

    def test_eventsub_chat_notification_non_command_is_ignored(self) -> None:
        """Ignore non-command chat lines and keep queue state unchanged."""

        details = _setup_channel()
        secret = "chatsecret-non-command"
        conduit_id = "conduit-chat-non-command"
        shard_id = "5"
        subscription_id = "sub-chat-non-command"
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit", "chat_ingress_shadow_mode": "0"})
        finally:
            db.close()
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id=subscription_id,
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret=secret,
        )

        body = {
            "subscription": {
                "id": subscription_id,
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "webhook-user-3",
                "chatter_user_login": "webhookuser3",
                "message": {"text": "hello chat"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-chat-non-command"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(secret.encode(), msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        response = self.client.post(
            "/twitch/eventsub/callback",
            data=raw,
            headers={
                "Twitch-Eventsub-Message-Id": message_id,
                "Twitch-Eventsub-Message-Timestamp": timestamp,
                "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                "Twitch-Eventsub-Message-Type": "notification",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

        db = backend_app.SessionLocal()
        try:
            rows = db.query(backend_app.Request).filter(backend_app.Request.channel_id == details["channel_pk"]).all()
            self.assertEqual(len(rows), 0)
        finally:
            db.close()

    def test_extract_eventsub_chat_command_uses_commands_yml_aliases(self) -> None:
        """Parse webhook aliases from the same canonical command map as websocket mode.

        Dependencies: ``COMMANDS_FILE`` env wiring and webhook parser alias map
        cache invalidation. Code customers: EventSub webhook parse parity.
        Used variables/origin: temporary YAML file defines aliases that must map
        to canonical commands in ``_extract_eventsub_chat_command``.
        """

        custom_commands = """
prefix: "!"
request:
  - request
  - req
  - sr
points:
  - points
  - pp
remove:
  - remove
  - undo
  - del
"""
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yml", delete=False) as handle:
            handle.write(custom_commands)
            custom_path = handle.name
        previous = os.environ.get("COMMANDS_FILE")
        try:
            os.environ["COMMANDS_FILE"] = custom_path
            backend_app._eventsub_chat_command_aliases.cache_clear()
            self.assertEqual(backend_app._extract_eventsub_chat_command("!req Artist - Song")["canonical"], "request")
            self.assertEqual(backend_app._extract_eventsub_chat_command("!sr Artist - Song")["canonical"], "request")
            self.assertEqual(backend_app._extract_eventsub_chat_command("!pp")["canonical"], "points")
            self.assertEqual(backend_app._extract_eventsub_chat_command("!undo")["canonical"], "remove")
            self.assertEqual(backend_app._extract_eventsub_chat_command("!del")["canonical"], "remove")
        finally:
            if previous is None:
                os.environ.pop("COMMANDS_FILE", None)
            else:
                os.environ["COMMANDS_FILE"] = previous
            backend_app._eventsub_chat_command_aliases.cache_clear()
            os.unlink(custom_path)

    def test_extract_eventsub_chat_command_unknown_alias_returns_reason_and_feedback(self) -> None:
        """Return explicit unknown-alias parse metadata for webhook diagnostics."""

        backend_app._eventsub_chat_command_aliases.cache_clear()
        parsed = backend_app._extract_eventsub_chat_command("!notacommand 123")
        self.assertEqual(parsed["parse_reason"], "unknown_alias")
        self.assertEqual(parsed["canonical"], None)
        self.assertEqual(parsed["alias"], "notacommand")
        self.assertIn("notacommand", str(parsed["parse_detail"]))
        self.assertEqual(parsed["feedback_message"], "Unknown command alias 'notacommand'.")

    def test_eventsub_conduit_chat_notification_missing_shard_secret_returns_503(self) -> None:
        """Fail conduit chat notifications when persisted shard secret is missing.

        Dependencies: EventSub callback signature validation, conduit/shard
        persistence, and HTTP error diagnostics. Code customers: operators
        diagnosing conduit reconciliation/state drift. Used variables/origin:
        creates a conduit chat subscription pointing at a shard row without a
        stored secret.
        """

        details = _setup_channel()
        conduit_id = "conduit-missing-secret"
        shard_id = "7"
        db = backend_app.SessionLocal()
        try:
            conduit = backend_app.TwitchConduit(conduit_id=conduit_id, status="enabled")
            db.add(conduit)
            db.commit()
            db.refresh(conduit)
            db.add(
                backend_app.TwitchConduitShard(
                    conduit_fk=conduit.id,
                    shard_id=shard_id,
                    transport_callback="https://example/callback",
                    transport_secret=None,
                    status="enabled",
                )
            )
            db.add(
                backend_app.EventSubscription(
                    channel_id=details["channel_pk"],
                    twitch_subscription_id="sub-chat-missing-secret",
                    type="channel.chat.message",
                    status="enabled",
                    secret="legacy-secret-unused",
                    callback="https://example/callback",
                    transport="conduit",
                    conduit_id=conduit_id,
                    shard_id=shard_id,
                )
            )
            db.commit()
        finally:
            db.close()

        body = {
            "subscription": {
                "id": "sub-chat-missing-secret",
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": "cid"},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {
                "chatter_user_id": "chat-user-1",
                "chatter_user_login": "chatuser",
                "message": {"text": "!request song"},
            },
        }
        raw = json.dumps(body).encode()
        message_id = "msg-missing-shard-secret"
        timestamp = "2023-01-01T00:00:00Z"
        signature = hmac.new(b"legacy-secret-unused", msg=(message_id + timestamp).encode() + raw, digestmod=hashlib.sha256)
        response = self.client.post(
            "/twitch/eventsub/callback",
            data=raw,
            headers={
                "Twitch-Eventsub-Message-Id": message_id,
                "Twitch-Eventsub-Message-Timestamp": timestamp,
                "Twitch-Eventsub-Message-Signature": f"sha256={signature.hexdigest()}",
                "Twitch-Eventsub-Message-Type": "notification",
            },
        )
        self.assertEqual(response.status_code, 503, response.text)
        payload = response.json().get("detail") or {}
        self.assertEqual(payload.get("reason_code"), "missing_shard_secret")
        self.assertEqual(payload.get("conduit_id"), conduit_id)
        self.assertEqual(payload.get("shard_id"), shard_id)

    def test_eventsub_conduit_notification_signature_passes_with_current_shard_secret(self) -> None:
        """Accept conduit notifications signed by the active shard secret.

        Dependencies: callback HMAC verification and conduit shard secret lookup.
        Code customers: webhook-conduit notification path.
        Used variables/origin: payload transport + subscription metadata provide
        conduit/shard identity while shard row ``current_secret`` is canonical.
        """

        details = _setup_channel()
        conduit_id = "conduit-current-secret"
        shard_id = "2"
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id="sub-current-secret",
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret="shard-current",
        )
        body = {
            "subscription": {
                "id": "sub-current-secret",
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": details["channel_name"]},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {"chatter_user_id": "u1", "chatter_user_login": "u1", "message": {"text": "hello"}},
        }
        raw = json.dumps(body).encode()
        headers = _signed_eventsub_headers("shard-current", "msg-current-secret", "2023-01-01T00:00:00Z", raw)
        response = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
        self.assertEqual(response.status_code, 200, response.text)

    def test_eventsub_conduit_notification_stale_secret_fails(self) -> None:
        """Reject conduit notifications signed with an expired previous secret.

        Dependencies: shard rotation fields and callback signature diagnostics.
        Code customers: operators triaging drift after rotation grace expires.
        Used variables/origin: subscription legacy secret intentionally matches
        stale signing key to assert ``stale_shard_secret`` reason reporting.
        """

        details = _setup_channel()
        conduit_id = "conduit-stale-secret"
        shard_id = "3"
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id="sub-stale-secret",
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret="shard-current-new",
        )
        db = backend_app.SessionLocal()
        try:
            row = (
                db.query(backend_app.TwitchConduitShard)
                .join(backend_app.TwitchConduit, backend_app.TwitchConduitShard.conduit_fk == backend_app.TwitchConduit.id)
                .filter(
                    backend_app.TwitchConduit.conduit_id == conduit_id,
                    backend_app.TwitchConduitShard.shard_id == shard_id,
                )
                .one()
            )
            row.current_secret = "shard-current-new"
            row.previous_secret = "legacy-subscription-secret"
            row.previous_secret_valid_until = backend_app.datetime.utcnow() - backend_app.timedelta(seconds=30)
            row.transport_secret = "shard-current-new"
            db.commit()
        finally:
            db.close()
        body = {
            "subscription": {
                "id": "sub-stale-secret",
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": details["channel_name"]},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {"chatter_user_id": "u2", "chatter_user_login": "u2", "message": {"text": "hello"}},
        }
        raw = json.dumps(body).encode()
        headers = _signed_eventsub_headers("legacy-subscription-secret", "msg-stale-secret", "2023-01-01T00:00:00Z", raw)
        response = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
        self.assertEqual(response.status_code, 403, response.text)
        detail = response.json().get("detail") or {}
        self.assertEqual(detail.get("reason_code"), "stale_shard_secret")

    def test_eventsub_conduit_notification_rotation_grace_accepts_previous_secret(self) -> None:
        """Accept conduit notifications signed with previous secret in grace.

        Dependencies: shard rotation grace window and callback verification.
        Code customers: zero-downtime shard secret rotation in conduit ingress.
        Used variables/origin: previous secret is explicitly persisted with a
        future ``previous_secret_valid_until`` timestamp.
        """

        details = _setup_channel()
        conduit_id = "conduit-rotation-grace"
        shard_id = "4"
        _create_chat_conduit_subscription(
            details["channel_pk"],
            subscription_id="sub-rotation-grace",
            conduit_id=conduit_id,
            shard_id=shard_id,
            secret="rotated-current-secret",
        )
        db = backend_app.SessionLocal()
        try:
            row = (
                db.query(backend_app.TwitchConduitShard)
                .join(backend_app.TwitchConduit, backend_app.TwitchConduitShard.conduit_fk == backend_app.TwitchConduit.id)
                .filter(
                    backend_app.TwitchConduit.conduit_id == conduit_id,
                    backend_app.TwitchConduitShard.shard_id == shard_id,
                )
                .one()
            )
            row.current_secret = "rotated-current-secret"
            row.previous_secret = "legacy-subscription-secret"
            row.previous_secret_valid_until = backend_app.datetime.utcnow() + backend_app.timedelta(minutes=2)
            row.transport_secret = "rotated-current-secret"
            db.commit()
        finally:
            db.close()
        body = {
            "subscription": {
                "id": "sub-rotation-grace",
                "type": "channel.chat.message",
                "status": "enabled",
                "version": "1",
                "condition": {"broadcaster_user_id": details["channel_name"]},
                "transport": {"method": "conduit", "conduit_id": conduit_id},
            },
            "event": {"chatter_user_id": "u3", "chatter_user_login": "u3", "message": {"text": "hello"}},
        }
        raw = json.dumps(body).encode()
        headers = _signed_eventsub_headers("legacy-subscription-secret", "msg-rotation-grace", "2023-01-01T00:00:00Z", raw)
        response = self.client.post("/twitch/eventsub/callback", data=raw, headers=headers)
        self.assertEqual(response.status_code, 200, response.text)

    def test_system_health_reports_ingress_summary_metrics(self) -> None:
        """Expose compact ingress summary with callback throughput and counters."""

        _setup_channel()
        db = backend_app.SessionLocal()
        try:
            backend_app._record_ingress_metric("callback_status", key="2xx")
            backend_app._record_ingress_metric("callback_status", key="4xx")
            backend_app._record_ingress_metric("signature_failure", key="invalid_signature")
            backend_app._record_ingress_metric("send_api_failure_reason", key="unknown_400")
            backend_app._record_ingress_metric("send_api_failure_reason", key="preflight_token_subject_unresolved")
        finally:
            db.close()

        response = self.client.get("/system/health")
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json().get("eventsub") or {}
        summary = payload.get("ingress_summary") or {}
        self.assertIn("recent_callback_throughput", summary)
        self.assertGreaterEqual((summary.get("callback_status") or {}).get("2xx", 0), 1)
        self.assertGreaterEqual((summary.get("callback_status") or {}).get("4xx", 0), 1)
        self.assertGreaterEqual(summary.get("signature_failures", 0), 1)
        self.assertGreaterEqual((summary.get("send_api_failure_reasons") or {}).get("unknown_400", 0), 1)
        last_errors = summary.get("last_errors") or {}
        self.assertEqual(last_errors.get("signature_failure_reason_code"), "invalid_signature")
        self.assertEqual(
            last_errors.get("reply_preflight_failure_reason_code"),
            "preflight_token_subject_unresolved",
        )

    def test_runtime_invariant_detects_unresolved_conduit_assignment(self) -> None:
        """Report unresolved conduit shard secret path in runtime invariants.

        Dependencies: Persists conduit chat assignment rows without matching
        shard metadata and calls runtime invariant evaluator directly.
        Code customers: ingress guard startup/runtime invariant diagnostics.
        Used variables/origin: assignment conduit/shard IDs come from persisted
        ``event_subscriptions`` rows and are expected to fail secret resolution.
        """

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            db.add(
                backend_app.EventSubscription(
                    channel_id=details["channel_pk"],
                    twitch_subscription_id="sub-invariant-missing-secret",
                    type="channel.chat.message",
                    status="enabled",
                    secret=backend_app.EVENTSUB_CONDUIT_SECRET_PLACEHOLDER,
                    callback="https://example/callback",
                    transport="conduit",
                    conduit_id="conduit-invariant-missing",
                    shard_id="9",
                )
            )
            db.commit()
            invariants = backend_app._evaluate_ingress_runtime_invariants(db, now=backend_app.datetime.utcnow())
            self.assertFalse(invariants.get("conduit_signature_secret_resolvable"))
            self.assertGreaterEqual(invariants.get("unresolved_assignment_count", 0), 1)
            self.assertFalse(invariants.get("sender_token_subject_resolvable"))
        finally:
            db.close()

    def test_ingress_guard_degradation_can_auto_fallback_websocket_mode(self) -> None:
        """Auto-fallback to websocket mode when authoritative ingress is degraded."""

        _setup_channel()
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(
                db,
                {
                    "chat_ingress_mode": "webhook_conduit",
                    "chat_ingress_guard_auto_fallback_enabled": "1",
                    "chat_ingress_guard_callback_error_threshold": "1",
                    "chat_ingress_guard_min_healthy_shards": "1",
                },
            )
            backend_app._record_ingress_metric("callback_status", key="5xx")
            guard = backend_app._evaluate_ingress_guard(db, backend_app.datetime.utcnow(), apply_fallback=True)
            self.assertTrue(guard["degraded"])
            self.assertTrue(guard["auto_fallback_applied"])
            self.assertEqual(backend_app.get_chat_ingress_mode(), "websocket")
        finally:
            db.close()

    def test_backend_token_refresh_worker_persists_refreshed_bot_tokens(self) -> None:
        """Refresh due bot credentials and persist replacement token fields.

        Dependencies: BotConfig persistence, Twitch refresh-token grant API,
        and ``_refresh_bot_access_token_once`` scheduler routine.
        Code customers: backend token refresh worker in webhook_conduit mode.
        Used variables/origin: stored ``BotConfig`` refresh credentials and
        ``expires_at`` trigger refresh before expiry (T-5m).
        """

        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.BotConfig).delete()
            cfg = backend_app.BotConfig(
                login="botlogin",
                display_name="Bot Login",
                access_token="old-access",
                refresh_token="old-refresh",
                scopes="user:bot",
                expires_at=backend_app.datetime.utcnow() + backend_app.timedelta(minutes=3),
                enabled=True,
            )
            db.add(cfg)
            db.commit()
        finally:
            db.close()

        response_mock = mock.Mock()
        response_mock.raise_for_status.return_value = None
        response_mock.json.return_value = {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
            "scope": ["user:bot", "user:read:chat"],
        }
        with mock.patch.object(backend_app, "get_twitch_client_id", return_value="cid"), mock.patch.object(
            backend_app, "get_twitch_client_secret", return_value="secret"
        ), mock.patch.object(backend_app.requests, "post", return_value=response_mock):
            result = backend_app._refresh_bot_access_token_once(now=backend_app.datetime.utcnow())
        self.assertEqual(result.get("status"), "refreshed")

        verify_db = backend_app.SessionLocal()
        try:
            refreshed = verify_db.query(backend_app.BotConfig).order_by(backend_app.BotConfig.id.asc()).first()
            self.assertIsNotNone(refreshed)
            assert refreshed
            self.assertEqual(refreshed.access_token, "new-access")
            self.assertEqual(refreshed.refresh_token, "new-refresh")
            self.assertIn("user:read:chat", str(refreshed.scopes))
            self.assertIsNotNone(refreshed.expires_at)
        finally:
            verify_db.close()

    def test_ingress_guard_runtime_invariants_report_refresh_health(self) -> None:
        """Expose token refresh worker health separately from callback health.

        Dependencies: in-memory refresh worker state, ingress runtime invariant
        evaluator, and guard degradation reason composition.
        Code customers: `/system/health` operator diagnostics and guard alerting.
        Used variables/origin: refresh failure timestamps in
        ``_BOT_TOKEN_REFRESH_STATE`` are checked via
        ``runtime_invariants.token_refresh_health``.
        """

        _setup_channel()
        db = backend_app.SessionLocal()
        now = backend_app.datetime.utcnow()
        try:
            backend_app._record_bot_token_refresh_result(success=False, now=now, error="synthetic_failure")
            invariants = backend_app._evaluate_ingress_runtime_invariants(db, now=now)
            self.assertFalse(invariants.get("token_refresh_healthy"))
            self.assertEqual((invariants.get("token_refresh_health") or {}).get("last_error"), "synthetic_failure")
            guard = backend_app._evaluate_ingress_guard(db, now, apply_fallback=False)
            self.assertIn("token_refresh_unhealthy", guard.get("reasons", []))
        finally:
            db.close()

    def test_ingress_guard_repair_action_selection_respects_cooldown_and_caps(self) -> None:
        """Select next repair action using least-disruptive order and guard rails.

        Dependencies: in-memory ``_INGRESS_GUARD_REPAIR_STATE`` and
        ``_select_ingress_guard_repair_action`` policy evaluator.
        Code customers: ingress guard repair watcher planning path.
        Used variables/origin: degraded guard reasons and synthetic timestamps
        model watcher cycles across cooldown and max-attempt limits.
        """

        now = backend_app.datetime.utcnow()
        with backend_app._INGRESS_GUARD_REPAIR_LOCK:
            for action in backend_app.INGRESS_GUARD_REPAIR_ACTION_ORDER:
                backend_app._INGRESS_GUARD_REPAIR_STATE[action] = {"last_attempt_at": None, "attempt_count": 0}

        first_action, _, _ = backend_app._select_ingress_guard_repair_action(
            reasons=["callback_errors_spike"],
            now=now,
        )
        self.assertEqual(first_action, "reconcile")

        second_action, second_skip_reason, _ = backend_app._select_ingress_guard_repair_action(
            reasons=["callback_errors_spike"],
            now=now + backend_app.timedelta(seconds=5),
        )
        self.assertEqual(second_action, "rebuild")
        self.assertIsNone(second_skip_reason)

        for attempt in range(backend_app.INGRESS_GUARD_REPAIR_MAX_ATTEMPTS["rebuild"]):
            _ = backend_app._select_ingress_guard_repair_action(
                reasons=["callback_errors_spike"],
                now=now + backend_app.timedelta(seconds=1200 + (attempt * 1200)),
            )
        blocked_action, blocked_reason, _ = backend_app._select_ingress_guard_repair_action(
            reasons=["callback_errors_spike"],
            now=now + backend_app.timedelta(seconds=9999),
        )
        self.assertIsNone(blocked_action)
        self.assertEqual(blocked_reason, "cooldown_or_attempt_cap")

    def test_ingress_guard_repair_cycle_emits_action_metrics(self) -> None:
        """Record structured repair metrics when watcher executes an action.

        Dependencies: ``run_ingress_guard_repair_cycle`` plus ingress telemetry
        counters returned by ``_ingress_metrics_snapshot``.
        Code customers: watcher observability and system-health dashboards.
        Used variables/origin: mocked degraded guard reasons and repair action
        outcome drive metric key increments.
        """

        _setup_channel()
        cycle_now = backend_app.datetime.utcnow()
        db = backend_app.SessionLocal()
        try:
            backend_app.set_settings(db, {"chat_ingress_mode": "webhook_conduit"})
        finally:
            db.close()
        with backend_app._INGRESS_GUARD_REPAIR_LOCK:
            for action in backend_app.INGRESS_GUARD_REPAIR_ACTION_ORDER:
                backend_app._INGRESS_GUARD_REPAIR_STATE[action] = {"last_attempt_at": None, "attempt_count": 0}
        with mock.patch.object(
            backend_app,
            "_evaluate_ingress_guard",
            return_value={"degraded": True, "reasons": ["callback_errors_spike"]},
        ), mock.patch.object(
            backend_app,
            "_run_ingress_guard_repair_action",
            return_value={"status": "ok", "errors": []},
        ):
            outcome = backend_app.run_ingress_guard_repair_cycle(now=cycle_now)
        self.assertEqual(outcome.get("status"), "executed")
        snapshot = backend_app._ingress_metrics_snapshot(cycle_now + backend_app.timedelta(seconds=1))
        self.assertGreaterEqual((snapshot.get("guard_repair_actions") or {}).get("reconcile", 0), 1)
        self.assertGreaterEqual((snapshot.get("guard_repair_outcomes") or {}).get("reconcile:ok", 0), 1)

    def test_cleanup_conduit_subscription_secret_semantics_sets_placeholder(self) -> None:
        """Normalize conduit chat subscription secrets to non-authoritative placeholder."""

        details = _setup_channel()
        db = backend_app.SessionLocal()
        try:
            db.add(
                backend_app.EventSubscription(
                    channel_id=details["channel_pk"],
                    twitch_subscription_id="sub-conduit-cleanup",
                    type="channel.chat.message",
                    status="enabled",
                    secret="legacy-secret",
                    callback="https://example/callback",
                    transport="conduit",
                    conduit_id="conduit-cleanup",
                    shard_id="0",
                )
            )
            db.commit()
        finally:
            db.close()

        backend_app.cleanup_conduit_subscription_secret_semantics()
        db = backend_app.SessionLocal()
        try:
            row = (
                db.query(backend_app.EventSubscription)
                .filter(backend_app.EventSubscription.twitch_subscription_id == "sub-conduit-cleanup")
                .one()
            )
            self.assertEqual(row.secret, backend_app.EVENTSUB_CONDUIT_SECRET_PLACEHOLDER)
        finally:
            db.close()

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

    def test_partial_settings_updates_preserve_existing_values(self) -> None:
        details = _setup_channel()
        channel = details["channel_name"]
        headers = {"X-Admin-Token": backend_app.ADMIN_TOKEN}

        db = backend_app.SessionLocal()
        try:
            settings = backend_app.get_or_create_settings(db, details["channel_pk"])
            settings.allow_bumps = 0
            settings.max_prio_points = 42
            settings.overall_queue_cap = 77
            db.commit()
        finally:
            db.close()

        patch = self.client.put(
            f"/channels/{channel}/settings",
            json={"queue_closed": 1},
            headers=headers,
        )
        self.assertEqual(patch.status_code, 200, patch.text)

        updated = self.client.get(
            f"/channels/{channel}/settings", headers=headers
        ).json()
        self.assertEqual(updated["queue_closed"], 1)
        self.assertEqual(updated["allow_bumps"], 0)
        self.assertEqual(updated["max_prio_points"], 42)
        self.assertEqual(updated["overall_queue_cap"], 77)

    def test_bot_message_level_defaults_updates_and_rejects_invalid_values(self) -> None:
        details = _setup_channel()
        channel = details["channel_name"]
        headers = {"X-Admin-Token": backend_app.ADMIN_TOKEN}

        initial = self.client.get(f"/channels/{channel}/settings", headers=headers)
        self.assertEqual(initial.status_code, 200, initial.text)
        self.assertEqual(initial.json()["bot_message_level"], "normal")

        update = self.client.put(
            f"/channels/{channel}/settings",
            json={"bot_message_level": "debug"},
            headers=headers,
        )
        self.assertEqual(update.status_code, 200, update.text)

        after_update = self.client.get(f"/channels/{channel}/settings", headers=headers)
        self.assertEqual(after_update.status_code, 200, after_update.text)
        self.assertEqual(after_update.json()["bot_message_level"], "debug")

        invalid = self.client.put(
            f"/channels/{channel}/settings",
            json={"bot_message_level": "loud"},
            headers=headers,
        )
        self.assertEqual(invalid.status_code, 422, invalid.text)

    def test_channel_status_route_controls_join_active_and_rejects_invalid_values(self) -> None:
        """Connect/disconnect should use `/channels/{channel}` with strict join_active validation."""

        details = _setup_channel()
        channel = details["channel_name"]
        headers = {"X-Admin-Token": backend_app.ADMIN_TOKEN}

        disconnect = self.client.put(
            f"/channels/{channel}",
            params={"join_active": 0},
            headers=headers,
        )
        self.assertEqual(disconnect.status_code, 200, disconnect.text)

        with backend_app.SessionLocal() as db:
            stored = db.query(backend_app.ActiveChannel).filter_by(channel_name=channel).one()
            self.assertEqual(stored.join_active, 0)

        reconnect = self.client.put(
            f"/channels/{channel}",
            params={"join_active": 1},
            headers=headers,
        )
        self.assertEqual(reconnect.status_code, 200, reconnect.text)

        invalid = self.client.put(
            f"/channels/{channel}",
            params={"join_active": 2},
            headers=headers,
        )
        self.assertEqual(invalid.status_code, 422, invalid.text)

    def test_settings_route_ignores_join_active_and_only_updates_message_level(self) -> None:
        """Settings updates should keep join state untouched while still applying bot message level."""

        details = _setup_channel()
        channel = details["channel_name"]
        headers = {"X-Admin-Token": backend_app.ADMIN_TOKEN}

        update = self.client.put(
            f"/channels/{channel}/settings",
            json={"join_active": 0, "bot_message_level": "verbose"},
            headers=headers,
        )
        self.assertEqual(update.status_code, 200, update.text)

        with backend_app.SessionLocal() as db:
            stored_channel = db.query(backend_app.ActiveChannel).filter_by(channel_name=channel).one()
            stored_settings = backend_app.get_or_create_settings(db, stored_channel.id)
            self.assertEqual(stored_channel.join_active, 1)
            self.assertEqual(stored_settings.bot_message_level, "verbose")


if __name__ == "__main__":
    unittest.main()
