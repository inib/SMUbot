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
        bot_app.backend = self.backend
        self.created_bots: list[tuple[MagicMock, dict]] = []

        def _bot_factory(**kwargs):
            bot = MagicMock()
            bot.start = AsyncMock()
            bot.close = AsyncMock()
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
            enabled=True,
        )

        await service.apply_settings(settings)

        self.assertEqual(len(self.created_bots), 1)
        bot, kwargs = self.created_bots[0]
        self.assertEqual(kwargs["token"], "oauth:backend-token")
        self.assertEqual(kwargs["nick"], "botnick")
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
        self.assertTrue(settings.enabled)

    async def test_settings_fall_back_to_environment(self) -> None:
        with patch.object(bot_app, "ENV_BOT_TOKEN", "env-token"), \
             patch.object(bot_app, "ENV_BOT_NICK", "envnick"):
            service = bot_app.BotService(
                self.backend,
                bot_factory=self.bot_factory,
                task_factory=asyncio.create_task,
            )
            settings = service._settings_from_config({})
        self.assertEqual(settings.token, "env-token")
        self.assertEqual(settings.login, "envnick")
        self.assertTrue(settings.enabled)

    async def test_missing_credentials_idle_bot(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        await service.apply_settings(
            bot_app.BotSettings(token=None, refresh_token=None, login=None, enabled=False)
        )
        self.assertEqual(self.created_bots, [])
        self.backend.push_bot_log.assert_awaited()

    async def test_disable_stops_running_bot(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        await service.apply_settings(
            bot_app.BotSettings(token="abc", refresh_token=None, login="nick", enabled=True)
        )
        bot = self.created_bots[0][0]
        bot.start.assert_called()

        await service.apply_settings(
            bot_app.BotSettings(token="abc", refresh_token=None, login="nick", enabled=False)
        )
        bot.close.assert_awaited()


if __name__ == "__main__":
    unittest.main()
