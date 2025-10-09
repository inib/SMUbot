import os
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, unquote, urlparse

from fastapi.testclient import TestClient

os.makedirs("/data", exist_ok=True)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import backend_app


class BotConfigApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._client_id = backend_app.TWITCH_CLIENT_ID
        self._client_secret = backend_app.TWITCH_CLIENT_SECRET
        self._redirect_uri = backend_app.TWITCH_REDIRECT_URI
        self._bot_redirect_uri = backend_app.BOT_TWITCH_REDIRECT_URI
        self._bot_nick = backend_app.BOT_NICK
        self._bot_user_id = backend_app.BOT_USER_ID
        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.BotConfig).delete()
            db.query(backend_app.TwitchUser).delete()
            db.commit()
        finally:
            db.close()
        backend_app.BOT_NICK = None
        backend_app.BOT_USER_ID = None
        backend_app._bot_oauth_states.clear()
        self.client = TestClient(backend_app.app)

    def tearDown(self) -> None:
        self.client.close()
        backend_app.TWITCH_CLIENT_ID = self._client_id
        backend_app.TWITCH_CLIENT_SECRET = self._client_secret
        backend_app.TWITCH_REDIRECT_URI = self._redirect_uri
        backend_app.BOT_TWITCH_REDIRECT_URI = self._bot_redirect_uri
        backend_app.BOT_NICK = self._bot_nick
        backend_app.BOT_USER_ID = self._bot_user_id
        backend_app._bot_oauth_states.clear()

    def test_fetch_default_config(self) -> None:
        response = self.client.get("/bot/config", headers={"X-Admin-Token": backend_app.ADMIN_TOKEN})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsNone(data.get("login"))
        self.assertFalse(data["enabled"])
        self.assertEqual(data["scopes"], backend_app.BOT_APP_SCOPES)

    def test_existing_config_missing_required_scopes_is_healed(self) -> None:
        db = backend_app.SessionLocal()
        try:
            cfg = backend_app.BotConfig(scopes="chat:read chat:edit channel:bot")
            db.add(cfg)
            db.commit()
        finally:
            db.close()

        response = self.client.get("/bot/config", headers={"X-Admin-Token": backend_app.ADMIN_TOKEN})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        for scope in backend_app.BOT_APP_SCOPES:
            self.assertIn(scope, data["scopes"])

        db = backend_app.SessionLocal()
        try:
            cfg = backend_app._get_bot_config(db)
            stored_scopes = (cfg.scopes or "").split()
            for scope in backend_app.BOT_APP_SCOPES:
                self.assertIn(scope, stored_scopes)
        finally:
            db.close()

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

    def test_fetch_config_includes_tokens_for_admin_header(self) -> None:
        db = backend_app.SessionLocal()
        try:
            cfg = backend_app._get_bot_config(db)
            cfg.login = "botnick"
            cfg.access_token = "stored-access"
            cfg.refresh_token = "stored-refresh"
            cfg.enabled = True
            db.commit()
        finally:
            db.close()

        response = self.client.get(
            "/bot/config",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["access_token"], "stored-access")
        self.assertEqual(data["refresh_token"], "stored-refresh")

    def test_fetch_config_hides_tokens_for_admin_session(self) -> None:
        db = backend_app.SessionLocal()
        try:
            cfg = backend_app._get_bot_config(db)
            cfg.login = "botnick"
            cfg.access_token = "stored-access"
            cfg.refresh_token = "stored-refresh"
            cfg.enabled = True
            user = backend_app.TwitchUser(
                twitch_id="u1",
                username="owner",
                access_token="session-token",
                refresh_token="",
                scopes="",
            )
            db.add(user)
            db.commit()
        finally:
            db.close()

        response = self.client.get(
            "/bot/config",
            cookies={backend_app.ADMIN_SESSION_COOKIE: "session-token"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotIn("access_token", data)
        self.assertNotIn("refresh_token", data)

    def test_bot_oauth_start_returns_authorize_url(self) -> None:
        backend_app.TWITCH_CLIENT_ID = "client"
        backend_app.TWITCH_CLIENT_SECRET = "secret"
        backend_app.TWITCH_REDIRECT_URI = "https://irrelevant.example/old"
        backend_app.BOT_TWITCH_REDIRECT_URI = None

        response = self.client.post(
            "/bot/config/oauth",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
            json={"return_url": "https://admin.example.com/dashboard"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("auth_url", data)
        parsed = urlparse(data["auth_url"])
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "id.twitch.tv")
        params = parse_qs(parsed.query)
        self.assertEqual(params.get("response_type", [None])[0], "code")
        self.assertEqual(params.get("client_id", [None])[0], "client")
        redirect_param = params.get("redirect_uri", [None])[0]
        self.assertEqual(redirect_param, "http://testserver/bot/config/oauth/callback")
        scope_param = params.get("scope", [""])[0]
        for scope in backend_app.BOT_APP_SCOPES:
            self.assertIn(scope, scope_param)
        state_value = params.get("state", [None])[0]
        self.assertIsNotNone(state_value)
        state_payload = json.loads(unquote(state_value))
        nonce = state_payload["nonce"]
        self.assertIn(nonce, backend_app._bot_oauth_states)
        self.assertEqual(
            backend_app._bot_oauth_states[nonce]["return_url"],
            "https://admin.example.com/dashboard",
        )

    def test_bot_oauth_callback_persists_tokens(self) -> None:
        backend_app.TWITCH_CLIENT_ID = "client"
        backend_app.TWITCH_CLIENT_SECRET = "secret"
        backend_app.TWITCH_REDIRECT_URI = None
        backend_app.BOT_TWITCH_REDIRECT_URI = None

        start_response = self.client.post(
            "/bot/config/oauth",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
        )
        self.assertEqual(start_response.status_code, 200)
        start_data = start_response.json()
        params = parse_qs(urlparse(start_data["auth_url"]).query)
        state_value = params.get("state", [None])[0]
        self.assertIsNotNone(state_value)
        state_payload = json.loads(unquote(state_value))
        nonce = state_payload["nonce"]

        class FakeTokenResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {
                    "access_token": "bot-access",
                    "refresh_token": "bot-refresh",
                    "expires_in": 3600,
                    "scope": backend_app.BOT_APP_SCOPES,
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
                            "login": "botaccount",
                            "display_name": "BotAccount",
                        }
                    ]
                }

        with patch.object(backend_app.requests, "post", return_value=FakeTokenResponse()) as mock_post, patch.object(
            backend_app.requests, "get", return_value=FakeUserResponse()
        ) as mock_get:
            callback = self.client.get(
                "/bot/config/oauth/callback",
                params={"code": "abc123", "state": state_value},
            )

        self.assertEqual(callback.status_code, 200)
        self.assertIn("bot-oauth-complete", callback.text)
        db = backend_app.SessionLocal()
        try:
            cfg = backend_app._get_bot_config(db)
            self.assertEqual(cfg.login, "botaccount")
            self.assertEqual(cfg.display_name, "BotAccount")
            self.assertEqual(cfg.access_token, "bot-access")
            self.assertEqual(cfg.refresh_token, "bot-refresh")
            self.assertTrue(cfg.enabled)
        finally:
            db.close()
        self.assertNotIn(nonce, backend_app._bot_oauth_states)
        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        self.assertEqual(kwargs["data"]["grant_type"], "authorization_code")
        mock_get.assert_called_once()

    def test_bot_oauth_start_respects_override_redirect(self) -> None:
        backend_app.TWITCH_CLIENT_ID = "client"
        backend_app.TWITCH_CLIENT_SECRET = "secret"
        backend_app.BOT_TWITCH_REDIRECT_URI = "https://admin.example.com/bot/callback"

        response = self.client.post(
            "/bot/config/oauth",
            headers={"X-Admin-Token": backend_app.ADMIN_TOKEN},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        params = parse_qs(urlparse(data["auth_url"]).query)
        redirect_param = params.get("redirect_uri", [None])[0]
        self.assertEqual(redirect_param, "https://admin.example.com/bot/callback")

    def test_bot_oauth_start_uses_forwarded_proto_and_host(self) -> None:
        backend_app.TWITCH_CLIENT_ID = "client"
        backend_app.TWITCH_CLIENT_SECRET = "secret"
        backend_app.BOT_TWITCH_REDIRECT_URI = None

        response = self.client.post(
            "/bot/config/oauth",
            headers={
                "X-Admin-Token": backend_app.ADMIN_TOKEN,
                "X-Forwarded-Proto": "https",
                "X-Forwarded-Host": "qapi.alpen.bot",
            },
        )

        self.assertEqual(response.status_code, 200)
        params = parse_qs(urlparse(response.json()["auth_url"]).query)
        redirect_param = params.get("redirect_uri", [None])[0]
        self.assertEqual(
            redirect_param,
            "https://qapi.alpen.bot/bot/config/oauth/callback",
        )

    def test_bot_oauth_start_honors_forwarded_header(self) -> None:
        backend_app.TWITCH_CLIENT_ID = "client"
        backend_app.TWITCH_CLIENT_SECRET = "secret"
        backend_app.BOT_TWITCH_REDIRECT_URI = None

        response = self.client.post(
            "/bot/config/oauth",
            headers={
                "X-Admin-Token": backend_app.ADMIN_TOKEN,
                "Forwarded": 'proto=https;host=secure.example.com',
            },
        )

        self.assertEqual(response.status_code, 200)
        params = parse_qs(urlparse(response.json()["auth_url"]).query)
        redirect_param = params.get("redirect_uri", [None])[0]
        self.assertEqual(
            redirect_param,
            "https://secure.example.com/bot/config/oauth/callback",
        )


if __name__ == "__main__":
    unittest.main()
