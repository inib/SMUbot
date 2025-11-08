import os
import sys
import uuid
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

os.makedirs("/data", exist_ok=True)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import backend_app


class AuthSessionDeleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self._settings_snapshot = backend_app.settings_store.snapshot()
        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.ChannelModerator).delete()
            db.query(backend_app.ActiveChannel).delete()
            db.query(backend_app.TwitchUser).delete()
            db.query(backend_app.BotConfig).delete()
            db.query(backend_app.AppSetting).delete()
            backend_app.set_settings(
                db,
                {
                    "setup_complete": "1",
                    "twitch_client_id": "client",
                    "twitch_client_secret": "secret",
                },
            )
        finally:
            db.close()
        backend_app.settings_store.invalidate()
        backend_app.APP_ACCESS_TOKEN = None
        backend_app.APP_TOKEN_EXPIRES = 0
        backend_app.BOT_USER_ID = None
        backend_app._bot_oauth_states.clear()
        self.client = TestClient(backend_app.app)

    def tearDown(self) -> None:
        self.client.close()
        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.ChannelModerator).delete()
            db.query(backend_app.ActiveChannel).delete()
            db.query(backend_app.TwitchUser).delete()
            db.query(backend_app.BotConfig).delete()
            db.query(backend_app.AppSetting).delete()
            if self._settings_snapshot:
                backend_app.set_settings(db, self._settings_snapshot)
        finally:
            db.close()
        backend_app.settings_store.invalidate()
        backend_app.APP_ACCESS_TOKEN = None
        backend_app.APP_TOKEN_EXPIRES = 0
        backend_app.BOT_USER_ID = None
        backend_app._bot_oauth_states.clear()

    def test_delete_session_removes_user_channel_and_cookie(self) -> None:
        db = backend_app.SessionLocal()
        try:
            user = backend_app.TwitchUser(
                twitch_id=str(uuid.uuid4()),
                username="owner",
                access_token="access",
                refresh_token="refresh",
                scopes="channel:bot",
            )
            db.add(user)
            db.commit()
            db.refresh(user)

            owned_channel = backend_app.ActiveChannel(
                channel_id=str(uuid.uuid4()),
                channel_name="ownerchannel",
                join_active=1,
                authorized=True,
                owner_id=user.id,
            )
            db.add(owned_channel)
            db.commit()
            db.refresh(owned_channel)

            mod_channel = backend_app.ActiveChannel(
                channel_id=str(uuid.uuid4()),
                channel_name="modded",
                join_active=1,
                authorized=True,
            )
            db.add(mod_channel)
            db.commit()
            db.refresh(mod_channel)

            moderator = backend_app.ChannelModerator(
                channel_id=mod_channel.id,
                user_id=user.id,
            )
            db.add(moderator)
            db.commit()

            user_stub = backend_app.TwitchUser(
                id=user.id,
                twitch_id=user.twitch_id,
                username=user.username,
                access_token=user.access_token,
                refresh_token=user.refresh_token,
                scopes=user.scopes,
            )
        finally:
            db.close()

        self.client.cookies.set(backend_app.ADMIN_SESSION_COOKIE, "session-token")
        with patch.object(backend_app, "_resolve_user_from_token", return_value=(user_stub, None)):
            response = self.client.delete(
                "/auth/session",
                headers={"Authorization": "Bearer session-token"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"success": True})

        set_cookie_header = response.headers.get("set-cookie", "")
        self.assertIn(f"{backend_app.ADMIN_SESSION_COOKIE}=", set_cookie_header)
        self.assertIn("Max-Age=0", set_cookie_header)

        db = backend_app.SessionLocal()
        try:
            self.assertIsNone(
                db.query(backend_app.TwitchUser)
                .filter_by(id=user_stub.id)
                .one_or_none()
            )
            self.assertEqual(
                db.query(backend_app.ActiveChannel)
                .filter_by(owner_id=user_stub.id)
                .count(),
                0,
            )
            self.assertEqual(
                db.query(backend_app.ChannelModerator)
                .filter_by(user_id=user_stub.id)
                .count(),
                0,
            )
        finally:
            db.close()
