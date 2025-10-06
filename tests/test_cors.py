import os
import unittest
import uuid
from unittest.mock import patch

import requests
from fastapi.testclient import TestClient

os.makedirs("/data", exist_ok=True)

import backend_app


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


if __name__ == "__main__":
    unittest.main()
