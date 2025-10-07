import os
import re
import unittest
import uuid
from unittest.mock import patch

import requests
from fastapi.testclient import TestClient

os.makedirs("/data", exist_ok=True)

import backend_app


class CorsConfigTests(unittest.TestCase):
    def test_wildcard_origins_expand_to_regex(self) -> None:
        allow_origins, allow_regex = backend_app._cors_settings_from_env(
            {"CORS_ALLOW_ORIGINS": "https://*.alpen.bot"}
        )

        self.assertEqual(allow_origins, [])
        self.assertIsNotNone(allow_regex)

        pattern = re.compile(allow_regex or "")
        self.assertIsNotNone(pattern.fullmatch("https://qadmin.alpen.bot"))
        self.assertIsNone(pattern.fullmatch("https://alpen.bot"))
        self.assertIsNone(pattern.fullmatch("https://example.com"))

    def test_mixed_wildcard_and_explicit_origins(self) -> None:
        allow_origins, allow_regex = backend_app._cors_settings_from_env(
            {
                "CORS_ALLOW_ORIGINS": "https://qadmin.alpen.bot https://*.alpen.bot",
            }
        )

        self.assertEqual(allow_origins, ["https://qadmin.alpen.bot"])
        self.assertIsNotNone(allow_regex)

        pattern = re.compile(allow_regex or "")
        self.assertIsNotNone(pattern.fullmatch("https://qstats.alpen.bot"))
        self.assertIsNone(pattern.fullmatch("https://example.com"))

    def test_trailing_slash_is_ignored(self) -> None:
        allow_origins, allow_regex = backend_app._cors_settings_from_env(
            {
                "CORS_ALLOW_ORIGINS": "https://qadmin.alpen.bot/ https://*.alpen.bot/",
            }
        )

        self.assertEqual(allow_origins, ["https://qadmin.alpen.bot"])
        self.assertIsNotNone(allow_regex)

        pattern = re.compile(allow_regex or "")
        self.assertIsNotNone(pattern.fullmatch("https://qstats.alpen.bot"))
        self.assertIsNone(pattern.fullmatch("https://example.com"))

    def test_default_regex_retained_when_no_overrides(self) -> None:
        allow_origins, allow_regex = backend_app._cors_settings_from_env({})

        self.assertEqual(allow_origins, [])
        self.assertIsNotNone(allow_regex)

        pattern = re.compile(allow_regex or "")
        self.assertIsNotNone(pattern.fullmatch("https://anywhere.example"))


class GetAppAccessTokenTests(unittest.TestCase):
    def setUp(self) -> None:
        self._client_id = backend_app.TWITCH_CLIENT_ID
        self._client_secret = backend_app.TWITCH_CLIENT_SECRET
        backend_app.TWITCH_CLIENT_ID = "client"
        backend_app.TWITCH_CLIENT_SECRET = "secret"
        backend_app.APP_ACCESS_TOKEN = None
        backend_app.APP_TOKEN_EXPIRES = 0

    def tearDown(self) -> None:
        backend_app.TWITCH_CLIENT_ID = self._client_id
        backend_app.TWITCH_CLIENT_SECRET = self._client_secret
        backend_app.APP_ACCESS_TOKEN = None
        backend_app.APP_TOKEN_EXPIRES = 0

    def test_missing_access_token_raises_http_error(self) -> None:
        class FakeResponse:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, str]:
                return {"message": "invalid"}

        with patch.object(backend_app.requests, "post", return_value=FakeResponse()):
            with self.assertRaises(requests.HTTPError):
                backend_app.get_app_access_token()


class AuthSessionCORSTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(backend_app.app)

    def tearDown(self) -> None:
        self.client.close()

    def test_auth_session_network_error_still_returns_cors_headers(self) -> None:
        origin = "https://qadmin.alpen.bot"
        with patch.object(backend_app.requests, "get", side_effect=requests.RequestException):
            response = self.client.post(
                "/auth/session",
                headers={
                    "Origin": origin,
                    "Authorization": "Bearer test-token",
                },
            )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.headers.get("access-control-allow-origin"), origin)
        self.assertEqual(response.headers.get("access-control-allow-credentials"), "true")

    def test_auth_session_eventsub_failure_still_returns_cors_headers(self) -> None:
        origin = "https://qadmin.alpen.bot"
        twitch_id = str(uuid.uuid4())

        db = backend_app.SessionLocal()
        user = backend_app.TwitchUser(
            twitch_id=twitch_id,
            username="tester",
            access_token="token",
            refresh_token="",
            scopes="channel:bot",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        db.close()

        data = {
            "login": "tester",
            "user_id": twitch_id,
            "scopes": ["channel:bot"],
        }

        with patch.object(backend_app, "_resolve_user_from_token", return_value=(user, data)), \
            patch.object(backend_app, "get_app_access_token", return_value="app-token"), \
            patch.object(backend_app, "get_bot_user_id", return_value="bot-id"), \
            patch.object(backend_app.requests, "post", side_effect=requests.RequestException):
            response = self.client.post(
                "/auth/session",
                headers={
                    "Origin": origin,
                    "Authorization": "Bearer test-token",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"login": "tester"})
        self.assertEqual(response.headers.get("access-control-allow-origin"), origin)
        self.assertEqual(response.headers.get("access-control-allow-credentials"), "true")

    def test_auth_session_skips_eventsub_without_app_credentials(self) -> None:
        origin = "https://qadmin.alpen.bot"
        twitch_id = str(uuid.uuid4())

        db = backend_app.SessionLocal()
        user = backend_app.TwitchUser(
            twitch_id=twitch_id,
            username="tester",
            access_token="token",
            refresh_token="",
            scopes="channel:bot",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        db.close()

        data = {
            "login": "tester",
            "user_id": twitch_id,
            "scopes": ["channel:bot"],
        }

        client_id = backend_app.TWITCH_CLIENT_ID
        client_secret = backend_app.TWITCH_CLIENT_SECRET
        backend_app.TWITCH_CLIENT_ID = None
        backend_app.TWITCH_CLIENT_SECRET = None
        try:
            with patch.object(backend_app, "_resolve_user_from_token", return_value=(user, data)), \
                patch.object(backend_app, "get_app_access_token") as token_mock, \
                patch.object(backend_app, "get_bot_user_id") as bot_mock:
                response = self.client.post(
                    "/auth/session",
                    headers={
                        "Origin": origin,
                        "Authorization": "Bearer test-token",
                    },
                )
        finally:
            backend_app.TWITCH_CLIENT_ID = client_id
            backend_app.TWITCH_CLIENT_SECRET = client_secret

        token_mock.assert_not_called()
        bot_mock.assert_not_called()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"login": "tester"})
        self.assertEqual(response.headers.get("access-control-allow-origin"), origin)
        self.assertEqual(response.headers.get("access-control-allow-credentials"), "true")

    def test_auth_session_cookie_available_for_other_endpoints(self) -> None:
        origin = "https://qadmin.alpen.bot"
        twitch_id = str(uuid.uuid4())

        db = backend_app.SessionLocal()
        user = backend_app.TwitchUser(
            twitch_id=twitch_id,
            username="tester",
            access_token="token",
            refresh_token="",
            scopes="channel:bot",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        db.close()

        data = {
            "login": "tester",
            "user_id": twitch_id,
            "scopes": ["channel:bot"],
        }

        with patch.object(backend_app, "_resolve_user_from_token", return_value=(user, data)), \
            patch.object(backend_app, "subscribe_chat_eventsub"):
            response = self.client.post(
                "/auth/session",
                headers={
                    "Origin": origin,
                    "Authorization": "Bearer test-token",
                },
            )

            self.assertEqual(response.status_code, 200)
            set_cookie = response.headers.get("set-cookie") or ""
            self.assertIn("Path=/", set_cookie)

            me_resp = self.client.get(
                "/me",
                headers={"Origin": origin},
            )

        self.assertEqual(me_resp.status_code, 200)
        self.assertEqual(me_resp.json(), {
            "login": "tester",
            "display_name": "tester",
            "profile_image_url": None,
        })

    def test_auth_session_handles_missing_app_access_token(self) -> None:
        origin = "https://qadmin.alpen.bot"
        twitch_id = str(uuid.uuid4())

        db = backend_app.SessionLocal()
        user = backend_app.TwitchUser(
            twitch_id=twitch_id,
            username="tester",
            access_token="token",
            refresh_token="",
            scopes="channel:bot",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        db.close()

        data = {
            "login": "tester",
            "user_id": twitch_id,
            "scopes": ["channel:bot"],
        }

        with patch.object(backend_app, "_resolve_user_from_token", return_value=(user, data)), \
            patch.object(backend_app, "get_app_access_token", side_effect=requests.HTTPError("boom")):
            response = self.client.post(
                "/auth/session",
                headers={
                    "Origin": origin,
                    "Authorization": "Bearer test-token",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"login": "tester"})


if __name__ == "__main__":
    unittest.main()
