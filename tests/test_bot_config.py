import os
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

os.makedirs("/data", exist_ok=True)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import backend_app


class BotConfigApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._client_id = backend_app.TWITCH_CLIENT_ID
        self._client_secret = backend_app.TWITCH_CLIENT_SECRET
        self._redirect_uri = backend_app.TWITCH_REDIRECT_URI
        self._bot_nick = backend_app.BOT_NICK
        self._bot_user_id = backend_app.BOT_USER_ID
        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.BotConfig).delete()
            db.commit()
        finally:
            db.close()
        backend_app.BOT_NICK = None
        backend_app.BOT_USER_ID = None
        self.client = TestClient(backend_app.app)

    def tearDown(self) -> None:
        self.client.close()
        backend_app.TWITCH_CLIENT_ID = self._client_id
        backend_app.TWITCH_CLIENT_SECRET = self._client_secret
        backend_app.TWITCH_REDIRECT_URI = self._redirect_uri
        backend_app.BOT_NICK = self._bot_nick
        backend_app.BOT_USER_ID = self._bot_user_id

    def test_fetch_default_config(self) -> None:
        response = self.client.get("/bot/config", headers={"X-Admin-Token": backend_app.ADMIN_TOKEN})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsNone(data["login"])
        self.assertFalse(data["enabled"])
        self.assertEqual(data["scopes"], backend_app.BOT_APP_SCOPES)

    def test_update_config_scope_and_enabled(self) -> None:
        payload = {"enabled": True, "scopes": ["chat:read", "channel:bot"]}
        response = self.client.put(
            "/bot/config",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
            json=payload,
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["enabled"])
        self.assertEqual(data["scopes"], payload["scopes"])

    def test_client_credentials_flow_persists_tokens(self) -> None:
        backend_app.TWITCH_CLIENT_ID = "client"
        backend_app.TWITCH_CLIENT_SECRET = "secret"
        backend_app.TWITCH_REDIRECT_URI = None

        class FakeTokenResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {
                    "access_token": "access",
                    "expires_in": 3600,
                }

        with patch.object(backend_app.requests, "post", return_value=FakeTokenResponse()) as mock_post:
            auth_start = self.client.post(
            "/bot/config/oauth",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
        )
        self.assertEqual(auth_start.status_code, 200)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://id.twitch.tv/oauth2/token")
        self.assertEqual(kwargs["data"]["grant_type"], "client_credentials")
        data = auth_start.json()
        self.assertTrue(data["enabled"])
        self.assertEqual(data["scopes"], backend_app.BOT_APP_SCOPES)

        data = self.client.get(
            "/bot/config",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
        ).json()
        self.assertTrue(data["enabled"])
        db = backend_app.SessionLocal()
        try:
            cfg = backend_app._get_bot_config(db)
            self.assertEqual(cfg.access_token, "access")
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
