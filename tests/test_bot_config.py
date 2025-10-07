import os
import sys
import unittest
from pathlib import Path
from urllib.parse import urlparse, parse_qs
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
        backend_app._bot_oauth_states.clear()
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
        self.assertEqual(data["scopes"], backend_app.TWITCH_SCOPES)

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

    def test_oauth_flow_persists_tokens(self) -> None:
        backend_app.TWITCH_CLIENT_ID = "client"
        backend_app.TWITCH_CLIENT_SECRET = "secret"
        backend_app.TWITCH_REDIRECT_URI = None

        auth_start = self.client.post(
            "/bot/config/oauth",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
        )
        self.assertEqual(auth_start.status_code, 200)
        auth_url = auth_start.json()["auth_url"]
        parsed = urlparse(auth_url)
        params = parse_qs(parsed.query)
        state = params["state"][0]

        class FakeTokenResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {
                    "access_token": "access",
                    "refresh_token": "refresh",
                    "scope": ["chat:read", "chat:edit"],
                    "expires_in": 3600,
                }

        class FakeUserResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "id": "1234",
                            "login": "botuser",
                            "display_name": "BotUser",
                        }
                    ]
                }

        with patch.object(backend_app.requests, "post", return_value=FakeTokenResponse()), \
            patch.object(backend_app.requests, "get", return_value=FakeUserResponse()):
            callback = self.client.get(
                "/bot/config/oauth/callback",
                params={"code": "abc", "state": state},
            )

        self.assertEqual(callback.status_code, 200)
        data = self.client.get(
            "/bot/config",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
        ).json()
        self.assertEqual(data["login"], "botuser")
        self.assertTrue(data["enabled"])
        self.assertIn("chat:read", data["scopes"])


if __name__ == "__main__":
    unittest.main()
