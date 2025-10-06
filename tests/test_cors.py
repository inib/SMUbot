import os
import unittest
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


if __name__ == "__main__":
    unittest.main()
