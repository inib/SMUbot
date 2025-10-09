import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bot.bot_app as bot_app


class BotServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._original_backend = bot_app.backend
        self.backend = AsyncMock()
        self.backend.push_bot_log = AsyncMock()
        self.backend.get_bot_config = AsyncMock()
        self.backend.set_bot_status = AsyncMock()
        bot_app.backend = self.backend
        self.created_bots: list[tuple[MagicMock, dict]] = []

        def _bot_factory(**kwargs):
            bot = MagicMock()
            bot.start = AsyncMock()
            bot.close = AsyncMock()
            bot.shutdown = AsyncMock()
            bot.update_enabled = AsyncMock()
            ready_event = asyncio.Event()
            ready_event.set()
            bot.ready_event = ready_event
            self.created_bots.append((bot, kwargs))
            return bot

        self.bot_factory = _bot_factory

    async def asyncTearDown(self) -> None:
        bot_app.backend = self._original_backend

    async def test_apply_settings_uses_backend_credentials(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        settings = bot_app.BotSettings(
            token="backend-token",
            refresh_token="refresh",
            login="botnick",
            client_id="client",
            client_secret="secret",
            bot_user_id="1234",
            scopes=["user:bot"],
            enabled=True,
        )

        await service.apply_settings(settings)

        self.assertEqual(len(self.created_bots), 1)
        bot, kwargs = self.created_bots[0]
        self.assertEqual(kwargs["token"], "backend-token")
        self.assertEqual(kwargs["refresh_token"], "refresh")
        self.assertEqual(kwargs["client_id"], "client")
        self.assertEqual(kwargs["client_secret"], "secret")
        self.assertEqual(kwargs["bot_id"], "1234")
        self.assertEqual(kwargs["login"], "botnick")
        self.assertEqual(kwargs["scopes"], ["user:bot"])
        self.assertTrue(kwargs["enabled"])
        bot.start.assert_called()

    async def test_run_fetches_backend_credentials(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        config = {
            "access_token": "fetched-token",
            "refresh_token": "fetched-refresh",
            "login": "botnick",
            "client_id": "client",
            "client_secret": "secret",
            "bot_user_id": "1234",
            "scopes": ["user:bot"],
            "enabled": True,
        }
        self.backend.get_bot_config = AsyncMock(return_value=config)
        service.apply_settings = AsyncMock()
        sleep_mock = AsyncMock(side_effect=asyncio.CancelledError())

        with patch.object(bot_app.asyncio, "sleep", sleep_mock):
            with self.assertRaises(asyncio.CancelledError):
                await service.run()

        service.apply_settings.assert_awaited_once()
        args, kwargs = service.apply_settings.call_args
        settings = args[0]
        self.assertIsInstance(settings, bot_app.BotSettings)
        self.assertEqual(settings.token, "fetched-token")
        self.assertEqual(settings.refresh_token, "fetched-refresh")
        self.assertEqual(settings.client_id, "client")
        self.assertEqual(settings.client_secret, "secret")
        self.assertEqual(settings.bot_user_id, "1234")
        self.assertEqual(settings.scopes, ["user:bot"])
        self.assertTrue(settings.enabled)

    async def test_settings_missing_credentials_disable_bot(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        settings = service._settings_from_config({})
        self.assertIsNone(settings.token)
        self.assertIsNone(settings.login)
        self.assertFalse(settings.enabled)
        self.assertEqual(
            settings.error,
            "Missing bot credentials: access_token, refresh_token, login, client_id, client_secret, bot_user_id",
        )

    async def test_missing_credentials_idle_bot(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        settings = service._settings_from_config({})
        with patch.object(bot_app, "push_console_event", AsyncMock()) as push_event:
            await service.apply_settings(settings)

        self.assertEqual(self.created_bots, [])
        push_event.assert_awaited_once()
        args, kwargs = push_event.call_args
        self.assertEqual(args[0], "error")
        self.assertIn("Missing bot credentials", args[1])
        self.assertEqual(kwargs.get("event"), "startup")
        self.assertEqual(kwargs.get("metadata"), {"error": settings.error})

    async def test_disable_stops_running_bot(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        await service.apply_settings(
            bot_app.BotSettings(
                token="abc",
                refresh_token="ref",
                login="nick",
                client_id="client",
                client_secret="secret",
                bot_user_id="1",
                scopes=["scope"],
                enabled=True,
            )
        )
        bot = self.created_bots[0][0]
        bot.start.assert_called()

        await service.apply_settings(
            bot_app.BotSettings(
                token="abc",
                refresh_token="ref",
                login="nick",
                client_id="client",
                client_secret="secret",
                bot_user_id="1",
                scopes=["scope"],
                enabled=False,
            )
        )
        bot.shutdown.assert_awaited()
        bot.close.assert_not_awaited()

    async def test_sync_channels_subscribes_backend_channels(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.channel_map = {}
        song_bot.state = {}
        song_bot.listeners = {}
        song_bot.joined = set()
        song_bot._sync_lock = asyncio.Lock()
        song_bot.enabled = True
        song_bot._announce_joined = AsyncMock()
        song_bot._announce_left = AsyncMock()
        song_bot.listen_backend = AsyncMock(return_value=None)
        song_bot._subscribe_for_channel = AsyncMock()
        song_bot._unsubscribe_channel = AsyncMock()
        song_bot._send_message = AsyncMock()

        channel_rows = [
            {"channel_name": "Foo", "channel_id": "1", "authorized": True, "join_active": 1},
            {"channel_name": "Bar", "channel_id": "2", "authorized": True, "join_active": 1},
        ]
        self.backend.get_channels = AsyncMock(return_value=channel_rows)
        self.backend.get_queue = AsyncMock(return_value=[])

        create_tasks: list = []

        def fake_create_task(coro):
            create_tasks.append(coro)
            coro.close()
            return MagicMock()

        push_event = AsyncMock()
        with patch.object(bot_app.asyncio, "create_task", fake_create_task), \
            patch.object(bot_app, "push_console_event", push_event):
            await song_bot.sync_channels()

        self.assertIn("foo", song_bot.channel_map)
        self.assertIn("bar", song_bot.channel_map)
        self.assertIn("foo", song_bot.state)
        self.assertIn("bar", song_bot.state)
        self.assertIn("foo", song_bot.joined)
        self.assertIn("bar", song_bot.joined)
        self.backend.set_bot_status.assert_any_await("Foo", True)
        self.backend.set_bot_status.assert_any_await("Bar", True)
        song_bot._subscribe_for_channel.assert_any_await("1")
        song_bot._subscribe_for_channel.assert_any_await("2")
        song_bot.listen_backend.assert_any_call("Foo")
        song_bot.listen_backend.assert_any_call("Bar")
        song_bot._announce_joined.assert_any_call("foo")
        song_bot._announce_joined.assert_any_call("bar")
        self.assertEqual(len(create_tasks), 4)

    async def test_sync_channels_logs_subscription_errors(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.channel_map = {}
        song_bot.state = {}
        song_bot.listeners = {}
        song_bot.joined = set()
        song_bot._sync_lock = asyncio.Lock()
        song_bot.enabled = True
        song_bot._subscribe_for_channel = AsyncMock(side_effect=RuntimeError("boom"))
        song_bot._unsubscribe_channel = AsyncMock()
        song_bot.listen_backend = AsyncMock()
        song_bot._announce_joined = AsyncMock()
        song_bot._announce_left = AsyncMock()

        channel_rows = [
            {"channel_name": "Foo", "channel_id": "1", "authorized": True, "join_active": 1},
        ]
        self.backend.get_channels = AsyncMock(return_value=channel_rows)
        self.backend.get_queue = AsyncMock()

        push_event = AsyncMock()
        def fake_create_task(coro):
            coro.close()
            return MagicMock()

        with patch.object(bot_app.asyncio, "create_task", fake_create_task), \
            patch.object(bot_app, "push_console_event", push_event):
            await song_bot.sync_channels()

        push_event.assert_awaited_once()
        args, kwargs = push_event.call_args
        self.assertEqual(args[0], "error")
        self.assertIn("Failed to subscribe channel Foo", args[1])
        self.assertEqual(kwargs.get("metadata"), {"channel": "Foo", "error": "boom"})
        self.assertEqual(kwargs.get("event"), "join_error")
        self.assertNotIn("foo", song_bot.joined)
        self.backend.set_bot_status.assert_awaited_once_with("Foo", False, "boom")
        song_bot._announce_joined.assert_not_called()

    async def test_songbot_does_not_assign_readonly_nick(self) -> None:
        commands_map = {k: ([v] if not isinstance(v, list) else v) for k, v in bot_app.DEFAULT_COMMANDS.items()}
        with patch.object(bot_app.commands.Bot, "__init__", return_value=None):
            with patch.object(bot_app, "load_commands", return_value=commands_map):
                with patch.object(bot_app, "load_messages", return_value=bot_app.DEFAULT_MESSAGES):
                    bot = bot_app.SongBot(
                        client_id="client",
                        client_secret="secret",
                        bot_id="1",
                        token="abc",
                        refresh_token="ref",
                        login="botnick",
                        scopes=["scope"],
                        enabled=True,
                    )

        self.assertTrue(bot.enabled)
        self.assertEqual(bot.configured_login, "botnick")


if __name__ == "__main__":
    unittest.main()
