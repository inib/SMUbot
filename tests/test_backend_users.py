import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bot.bot_app as bot_app


class BackendFindOrCreateUserTests(unittest.IsolatedAsyncioTestCase):
    async def test_mixed_type_users_payload_falls_back_to_create(self) -> None:
        """
        Verify mixed-type search responses trigger anomaly logging and fallback creation.

        Dependencies: relies on AsyncMock to emulate backend `_req` calls and MagicMock to intercept logging.
        Code customers: protects callers of `find_or_create_user` so they always receive a user ID despite malformed data.
        Variables/origin: `mixed_users` simulates a corrupted list payload, while channel and identity values mimic Twitch inputs.
        """
        backend = bot_app.Backend("http://api", "token")
        mixed_users = ["oops", {"twitch_id": "123", "id": 7}]
        backend._req = AsyncMock(side_effect=[mixed_users, {"id": 55}])
        backend._log_anomalous_users_response = MagicMock()

        user_id = await backend.find_or_create_user("channel", "123", "viewer")

        self.assertEqual(user_id, 55)
        backend._log_anomalous_users_response.assert_called_once_with("channel", "viewer", mixed_users)
        self.assertEqual(backend._req.await_count, 2)
        first_call = backend._req.await_args_list[0]
        self.assertEqual(first_call.args[0], "GET")
        self.assertIn("/channels/channel/users", first_call.args[1])
        self.assertEqual(backend._req.await_args_list[1].args, (
            "POST",
            "/channels/channel/users",
            {"twitch_id": "123", "username": "viewer"},
        ))
