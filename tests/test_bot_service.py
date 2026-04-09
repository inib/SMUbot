import asyncio
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bot.bot_app as bot_app
import backend_app

os.makedirs("/data", exist_ok=True)


class BotServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._original_backend = bot_app.backend
        self.backend = AsyncMock()
        self.backend.push_bot_log = AsyncMock()
        self.backend.get_bot_config = AsyncMock()
        self.backend.set_bot_status = AsyncMock()
        self.backend.get_system_config = AsyncMock(return_value={"chat_ingress_mode": "websocket"})
        self.backend.list_playlists = AsyncMock()
        self.backend.playlist_request = AsyncMock()
        bot_app.backend = self.backend
        self.created_bots: list[tuple[MagicMock, dict]] = []

        def _bot_factory(**kwargs):
            bot = MagicMock()
            bot.start = AsyncMock()
            bot.close = AsyncMock()
            bot.shutdown = AsyncMock()
            bot.update_enabled = AsyncMock()
            bot.refresh_runtime_tokens = AsyncMock()
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

    async def test_settings_require_app_credentials(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        config = {
            "access_token": "token",
            "refresh_token": "refresh",
            "login": "botnick",
            "enabled": True,
        }
        settings = service._settings_from_config(config)
        self.assertIsNone(settings.client_id)
        self.assertIsNone(settings.client_secret)
        self.assertIsNone(settings.bot_user_id)
        self.assertFalse(settings.enabled)
        self.assertIn("client_id", settings.error or "")
        self.assertIn("client_secret", settings.error or "")
        self.assertIn("bot_user_id", settings.error or "")

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
        self.assertGreaterEqual(push_event.await_count, 1)
        args, kwargs = push_event.await_args_list[0]
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

    async def test_apply_settings_token_delta_hot_swaps_without_restart(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        bot = MagicMock()
        bot.update_enabled = AsyncMock()
        bot.refresh_runtime_tokens = AsyncMock()
        service._bot = bot
        service._current_token = "old-token"
        service._current_refresh = "old-refresh"
        service._current_login = "nick"
        service._current_client_id = "client"
        service._current_client_secret = "secret"
        service._current_bot_id = "1"
        service._current_scopes = ["scope"]
        service._restart_bot = AsyncMock()

        await service.apply_settings(
            bot_app.BotSettings(
                token="new-token",
                refresh_token="new-refresh",
                login="nick",
                client_id="client",
                client_secret="secret",
                bot_user_id="1",
                scopes=["scope"],
                enabled=True,
            )
        )

        service._restart_bot.assert_not_awaited()
        bot.refresh_runtime_tokens.assert_awaited_once()
        self.assertEqual(service._current_token, "new-token")
        self.assertEqual(service._current_refresh, "new-refresh")

    async def test_apply_settings_identity_change_still_restarts(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        bot = MagicMock()
        bot.update_enabled = AsyncMock()
        bot.refresh_runtime_tokens = AsyncMock()
        service._bot = bot
        service._current_token = "token"
        service._current_refresh = "refresh"
        service._current_login = "nick"
        service._current_client_id = "client"
        service._current_client_secret = "secret"
        service._current_bot_id = "1"
        service._current_scopes = ["scope"]
        service._restart_bot = AsyncMock()

        await service.apply_settings(
            bot_app.BotSettings(
                token="token",
                refresh_token="refresh",
                login="newnick",
                client_id="client",
                client_secret="secret",
                bot_user_id="1",
                scopes=["scope"],
                enabled=True,
            )
        )

        service._restart_bot.assert_awaited_once()
        bot.refresh_runtime_tokens.assert_not_awaited()

    async def test_apply_settings_hot_swap_failure_falls_back_to_restart(self) -> None:
        service = bot_app.BotService(
            self.backend,
            bot_factory=self.bot_factory,
            task_factory=asyncio.create_task,
        )
        bot = MagicMock()
        bot.update_enabled = AsyncMock()
        bot.refresh_runtime_tokens = AsyncMock(side_effect=RuntimeError("boom"))
        service._bot = bot
        service._current_token = "token"
        service._current_refresh = "refresh"
        service._current_login = "nick"
        service._current_client_id = "client"
        service._current_client_secret = "secret"
        service._current_bot_id = "1"
        service._current_scopes = ["scope"]
        service._restart_bot = AsyncMock()

        await service.apply_settings(
            bot_app.BotSettings(
                token="token-new",
                refresh_token="refresh-new",
                login="nick",
                client_id="client",
                client_secret="secret",
                bot_user_id="1",
                scopes=["scope"],
                enabled=True,
            )
        )

        bot.refresh_runtime_tokens.assert_awaited_once()
        service._restart_bot.assert_awaited_once()

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
        song_bot.listen_backend.assert_any_call("Foo")
        song_bot.listen_backend.assert_any_call("Bar")
        song_bot._announce_joined.assert_any_call("foo")
        song_bot._announce_joined.assert_any_call("bar")
        self.assertEqual(len(create_tasks), 4)

    async def test_sync_channels_emits_join_events_without_subscription_hooks(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.channel_map = {}
        song_bot.state = {}
        song_bot.listeners = {}
        song_bot.joined = set()
        song_bot._sync_lock = asyncio.Lock()
        song_bot.enabled = True
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

        self.assertGreaterEqual(push_event.await_count, 1)
        join_event = push_event.await_args_list[0]
        self.assertEqual(join_event.args[0], "info")
        self.assertIn("Subscribed channel Foo", join_event.args[1])
        self.assertEqual(join_event.kwargs.get("metadata"), {"channel": "Foo"})
        self.assertEqual(join_event.kwargs.get("event"), "join")
        self.assertIn("foo", song_bot.joined)
        self.backend.set_bot_status.assert_awaited_once_with("Foo", True)
        song_bot._announce_joined.assert_called_once_with("foo")

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

    async def test_cancel_refresher_task(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)

        async def never_complete() -> None:
            await asyncio.Future()

        refresher = asyncio.create_task(never_complete())
        song_bot._refresher_task = refresher
        await song_bot._cancel_refresher()
        self.assertTrue(refresher.cancelled())
        self.assertIsNone(song_bot._refresher_task)

    async def test_songbot_shutdown_closes_resources(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot._cancel_refresher = AsyncMock()
        song_bot._disable_all_channels = AsyncMock()
        with patch.object(bot_app.commands.Bot, "close", AsyncMock()) as base_close:
            await song_bot.shutdown()

        song_bot._cancel_refresher.assert_awaited()
        song_bot._disable_all_channels.assert_awaited()
        base_close.assert_awaited()

    async def test_announce_backend_runtime_event_logs_when_backend_reports_unsent(self) -> None:
        """Log unsent backend runtime announcements with backend reason metadata.

        Dependencies: ``SongBot._announce_backend_runtime_event`` and backend
        ``announce_runtime_event`` response contract.
        Code customers: runtime queue/event producers that rely on backend
        delivery for non-chat announcements.
        Used variables/origin: backend response fields ``sent``,
        ``message_id``, ``visibility``, and ``reason_code``.
        """

        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot._send_catalog_message = AsyncMock()
        self.backend.announce_runtime_event = AsyncMock(
            return_value={
                "success": True,
                "sent": False,
                "channel": "ChannelOne",
                "message_id": "queue_position_changed",
                "visibility": "verbose",
                "reason_code": "suppressed_by_level",
            }
        )

        push_event = AsyncMock()
        with patch.object(bot_app, "push_console_event", push_event):
            await song_bot._announce_backend_runtime_event(
                login="channelone",
                channel="ChannelOne",
                message_id="queue_position_changed",
            )

        self.backend.announce_runtime_event.assert_awaited_once()
        song_bot._send_catalog_message.assert_not_awaited()
        push_event.assert_awaited_once()
        args, kwargs = push_event.await_args
        self.assertEqual(args[0], "warning")
        self.assertIn("suppressed", args[1].lower())
        self.assertEqual(kwargs.get("event"), "runtime_announcement_suppressed")
        self.assertEqual(kwargs.get("metadata", {}).get("message_id"), "queue_position_changed")
        self.assertEqual(kwargs.get("metadata", {}).get("visibility"), "verbose")
        self.assertEqual(kwargs.get("metadata", {}).get("reason_code"), "suppressed_by_level")

    async def test_announce_backend_runtime_event_no_websocket_fallback_on_error(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot._send_catalog_message = AsyncMock()
        self.backend.announce_runtime_event = AsyncMock(side_effect=RuntimeError("network"))

        push_event = AsyncMock()
        with patch.object(bot_app, "push_console_event", push_event):
            await song_bot._announce_backend_runtime_event(
                login="channelone",
                channel="ChannelOne",
                message_id="queue_position_changed",
            )

        song_bot._send_catalog_message.assert_not_awaited()
        push_event.assert_awaited_once()
        args, kwargs = push_event.await_args
        self.assertEqual(args[0], "error")
        self.assertEqual(kwargs.get("event"), "runtime_announcement_failed")

    async def test_handle_playlist_request_success(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.channel_map = {'channelname': {'channel_name': 'ChannelName'}}
        song_bot.messages = bot_app.DEFAULT_MESSAGES.copy()
        song_bot._send_message = AsyncMock()
        song_bot._channel_login = bot_app.SongBot._channel_login.__get__(song_bot, bot_app.SongBot)
        msg = SimpleNamespace(
            broadcaster=SimpleNamespace(name='ChannelName', display_name='ChannelName'),
            chatter=SimpleNamespace(name='viewer', display_name='Viewer', id='42', subscriber=False),
            text='!playlist Chill Mix 1',
            id='msg123',
        )
        self.backend.list_playlists.return_value = [{'id': 10, 'title': 'Chill Mix'}]
        self.backend.playlist_request.return_value = {
            'request_id': 5,
            'playlist_item_id': 77,
            'song': {'artist': 'Artist B', 'title': 'Track Two'},
        }

        await song_bot.handle_playlist_request(msg, 'CHILL mix 1')

        self.backend.list_playlists.assert_awaited_once_with('ChannelName')
        self.backend.playlist_request.assert_awaited_once()
        args, kwargs = self.backend.playlist_request.call_args
        self.assertEqual(args[0], 'ChannelName')
        self.assertEqual(kwargs['identifier'], '10')
        self.assertEqual(kwargs['index'], 1)
        song_bot._send_message.assert_awaited_once()
        sent_args, sent_kwargs = song_bot._send_message.call_args
        self.assertEqual(sent_args[0], 'channelname')
        self.assertIn('Track Two', sent_args[1])
        self.assertEqual(sent_kwargs.get('metadata', {}).get('playlist'), 'Chill Mix')

    async def test_handle_playlist_request_missing_playlist(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.channel_map = {'channelname': {'channel_name': 'ChannelName'}}
        song_bot.messages = bot_app.DEFAULT_MESSAGES.copy()
        song_bot._send_message = AsyncMock()
        song_bot._channel_login = bot_app.SongBot._channel_login.__get__(song_bot, bot_app.SongBot)
        msg = SimpleNamespace(
            broadcaster=SimpleNamespace(name='ChannelName', display_name='ChannelName'),
            chatter=SimpleNamespace(name='viewer', display_name='Viewer', id='42', subscriber=False),
            text='!playlist Missing 1',
            id='msg124',
        )
        self.backend.list_playlists.return_value = [{'id': 2, 'title': 'Other'}]

        await song_bot.handle_playlist_request(msg, 'Missing 1')

        self.backend.playlist_request.assert_not_called()
        song_bot._send_message.assert_awaited_once()
        sent_args, _ = song_bot._send_message.call_args
        self.assertIn('not found', sent_args[1].lower())

    async def test_handle_playlist_request_index_error(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.channel_map = {'channelname': {'channel_name': 'ChannelName'}}
        song_bot.messages = bot_app.DEFAULT_MESSAGES.copy()
        song_bot._send_message = AsyncMock()
        song_bot._channel_login = bot_app.SongBot._channel_login.__get__(song_bot, bot_app.SongBot)
        msg = SimpleNamespace(
            broadcaster=SimpleNamespace(name='ChannelName', display_name='ChannelName'),
            chatter=SimpleNamespace(name='viewer', display_name='Viewer', id='42', subscriber=False),
            text='!playlist Chill Mix 2',
            id='msg125',
        )
        self.backend.list_playlists.return_value = [{'id': 3, 'title': 'Chill Mix'}]
        self.backend.playlist_request.side_effect = bot_app.BackendError(400, 'index out of range')

        with patch.object(bot_app, 'push_console_event', AsyncMock()) as push_event:
            await song_bot.handle_playlist_request(msg, 'Chill Mix 2')

        self.backend.playlist_request.assert_awaited_once()
        song_bot._send_message.assert_awaited_once()
        sent_args, sent_kwargs = song_bot._send_message.call_args
        self.assertIn('no song', sent_args[1].lower())
        push_event.assert_not_awaited()

    async def test_event_message_routes_playlist_command(self) -> None:
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        commands_map = {k: ([v] if not isinstance(v, list) else v) for k, v in bot_app.DEFAULT_COMMANDS.items()}
        song_bot.commands_map = commands_map
        song_bot.enabled = True
        song_bot.bot_user_id = 'bot'
        song_bot.channel_map = {'channelname': {'channel_name': 'ChannelName'}}
        song_bot.messages = bot_app.DEFAULT_MESSAGES.copy()
        song_bot.currency_plural = 'points'
        song_bot._send_message = AsyncMock()
        song_bot._channel_login = bot_app.SongBot._channel_login.__get__(song_bot, bot_app.SongBot)
        msg = SimpleNamespace(
            text='!playlist mix 2',
            broadcaster=SimpleNamespace(name='ChannelName'),
            chatter=SimpleNamespace(id='user', name='viewer', display_name='Viewer', subscriber=False),
            id='msg126',
        )
        with patch.object(bot_app.backend, "list_playlists", AsyncMock(return_value=[])) as list_playlists:
            await song_bot.event_message(msg)

        list_playlists.assert_awaited_once_with('ChannelName')
        song_bot._send_message.assert_awaited_once()
        sent_args, _ = song_bot._send_message.call_args
        self.assertIn('not found', sent_args[1].lower())

    async def test_send_bot_message_policy_matrix(self) -> None:
        """Verify per-channel thresholds include lower-severity messages."""
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.messages = bot_app.DEFAULT_MESSAGES.copy()
        song_bot.channel_map = {
            "mutech": {"channel_name": "MuteCh", "bot_message_level": "mute"},
            "normalch": {"channel_name": "NormalCh", "bot_message_level": "normal"},
            "verbosech": {"channel_name": "VerboseCh", "bot_message_level": "verbose"},
            "debugch": {"channel_name": "DebugCh", "bot_message_level": "debug"},
        }
        song_bot._channel_login = bot_app.SongBot._channel_login.__get__(song_bot, bot_app.SongBot)
        song_bot._send_message = AsyncMock()

        with patch.object(bot_app, "push_console_event", AsyncMock()) as push_event:
            # Mute sends none.
            await song_bot._send_bot_message("mutech", "mute-normal", level=bot_app.BotMessageLevel.NORMAL)
            await song_bot._send_bot_message("mutech", "mute-verbose", level=bot_app.BotMessageLevel.VERBOSE)

            # Normal sends only normal.
            await song_bot._send_bot_message("normalch", "normal-normal", level=bot_app.BotMessageLevel.NORMAL)
            await song_bot._send_bot_message("normalch", "normal-verbose", level=bot_app.BotMessageLevel.VERBOSE)
            await song_bot._send_bot_message("normalch", "normal-debug", level=bot_app.BotMessageLevel.DEBUG)

            # Verbose sends normal + verbose.
            await song_bot._send_bot_message("verbosech", "verbose-normal", level=bot_app.BotMessageLevel.NORMAL)
            await song_bot._send_bot_message("verbosech", "verbose-verbose", level=bot_app.BotMessageLevel.VERBOSE)
            await song_bot._send_bot_message("verbosech", "verbose-debug", level=bot_app.BotMessageLevel.DEBUG)

            # Debug sends normal + verbose + debug.
            await song_bot._send_bot_message("debugch", "debug-normal", level=bot_app.BotMessageLevel.NORMAL)
            await song_bot._send_bot_message("debugch", "debug-verbose", level=bot_app.BotMessageLevel.VERBOSE)
            await song_bot._send_bot_message("debugch", "debug-debug", level=bot_app.BotMessageLevel.DEBUG)

        sent_calls = [call.args[1] for call in song_bot._send_message.await_args_list]
        self.assertEqual(
            sent_calls,
            [
                "normal-normal",
                "verbose-normal",
                "verbose-verbose",
                "debug-normal",
                "debug-verbose",
                "debug-debug",
            ],
        )
        suppressed_logs = [
            call
            for call in push_event.await_args_list
            if call.kwargs.get("event") == "message_suppressed"
        ]
        self.assertEqual(len(suppressed_logs), 5)

    async def test_request_added_message_visible_at_verbose_and_debug(self) -> None:
        """Regression: command responses remain visible for verbose/debug channels."""
        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.messages = bot_app.DEFAULT_MESSAGES.copy()
        song_bot.message_catalog = bot_app.DEFAULT_MESSAGE_CATALOG.copy()
        song_bot.channel_map = {
            "verbosech": {"channel_name": "VerboseCh", "bot_message_level": "verbose"},
            "debugch": {"channel_name": "DebugCh", "bot_message_level": "debug"},
        }
        song_bot._channel_login = bot_app.SongBot._channel_login.__get__(song_bot, bot_app.SongBot)
        song_bot._send_message = AsyncMock()

        with patch.object(bot_app, "push_console_event", AsyncMock()):
            await song_bot._send_catalog_message(
                "verbosech",
                "request_added",
                template_vars={"artist": "Artist", "title": "Title"},
            )
            await song_bot._send_catalog_message(
                "debugch",
                "request_added",
                template_vars={"artist": "Artist", "title": "Title"},
            )

        sent_calls = [call.args[1] for call in song_bot._send_message.await_args_list]
        self.assertEqual(sent_calls, ["Added: Artist - Title", "Added: Artist - Title"])

    async def test_channel_template_resolution_prefers_channel_db_then_defaults(self) -> None:
        """Catalog rendering should use channel DB template overrides before global defaults."""

        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.messages = bot_app.DEFAULT_MESSAGES.copy()
        song_bot.message_catalog = bot_app.DEFAULT_MESSAGE_CATALOG.copy()
        song_bot.channel_map = {
            "chan": {
                "channel_name": "Chan",
                "bot_message_level": "debug",
                "bot_message_templates": {
                    "request_added": {
                        "message_id": "request_added",
                        "template": "DB Added: {artist}::{title}",
                        "enabled": True,
                    }
                },
            },
            "plain": {"channel_name": "Plain", "bot_message_level": "debug", "bot_message_templates": {}},
        }
        song_bot._channel_login = bot_app.SongBot._channel_login.__get__(song_bot, bot_app.SongBot)
        song_bot._send_message = AsyncMock()

        with patch.object(bot_app, "push_console_event", AsyncMock()):
            await song_bot._send_catalog_message(
                "chan",
                "request_added",
                template_vars={"artist": "A", "title": "T"},
            )
            await song_bot._send_catalog_message(
                "plain",
                "request_added",
                template_vars={"artist": "A", "title": "T"},
            )

        sent_calls = [call.args[1] for call in song_bot._send_message.await_args_list]
        self.assertEqual(sent_calls, ["DB Added: A::T", "Added: A - T"])

    async def test_webhook_and_chat_template_rendering_match_for_optional_vars(self) -> None:
        """Webhook/backend and websocket/chat paths should format templates identically."""

        db = backend_app.SessionLocal()
        try:
            db.query(backend_app.ChannelBotMessage).delete()
            db.query(backend_app.ChannelSettings).delete()
            db.query(backend_app.ActiveChannel).delete()
            channel = backend_app.ActiveChannel(channel_id="8811", channel_name="ParityCh")
            db.add(channel)
            db.commit()
            db.refresh(channel)
            backend_app._seed_channel_bot_messages(db, channel.id)
            db.commit()
            row = (
                db.query(backend_app.ChannelBotMessage)
                .filter(
                    backend_app.ChannelBotMessage.channel_id == channel.id,
                    backend_app.ChannelBotMessage.message_id == "request_added",
                )
                .one()
            )
            row.template = "Added: {artist}-{title}|u:{username}|s:{settings_prio_bits_per_point}"
            db.commit()

            reply = backend_app._eventsub_response_contract(
                "success",
                template_key="request_added",
                template_vars={"artist": "A", "title": "T"},
            )
            backend_rendered = backend_app._render_eventsub_reply_text(db, channel, reply)
        finally:
            db.close()

        song_bot = bot_app.SongBot.__new__(bot_app.SongBot)
        song_bot.messages = bot_app.DEFAULT_MESSAGES.copy()
        song_bot.message_catalog = bot_app.DEFAULT_MESSAGE_CATALOG.copy()
        song_bot.channel_map = {
            "paritych": {
                "channel_name": "ParityCh",
                "settings": {"prio_bits_per_point": 200},
                "bot_message_level": "debug",
                "bot_message_templates": {
                    "request_added": {
                        "message_id": "request_added",
                        "template": "Added: {artist}-{title}|u:{username}|s:{settings_prio_bits_per_point}",
                        "enabled": True,
                    }
                },
            }
        }
        song_bot._channel_login = bot_app.SongBot._channel_login.__get__(song_bot, bot_app.SongBot)
        song_bot._send_message = AsyncMock()

        with patch.object(bot_app, "push_console_event", AsyncMock()):
            await song_bot._send_catalog_message(
                "paritych",
                "request_added",
                template_vars={"artist": "A", "title": "T"},
            )

        chat_rendered = song_bot._send_message.await_args_list[0].args[1]
        self.assertEqual(backend_rendered, chat_rendered)

    def test_default_message_catalog_levels_match_intended_groups(self) -> None:
        """Ensure command/background/diagnostic messages retain expected severities."""
        catalog = bot_app.DEFAULT_MESSAGE_CATALOG
        command_ids = [
            "request_added",
            "random_request_added",
            "playlist_request_added",
            "remove_success",
            "archive_success",
        ]
        verbose_background_ids = [
            "bot_joined",
            "played_next",
            "award_follow",
            "queue_position_changed",
        ]
        diagnostic_ids = ["token_refreshed", "action_failed_debug"]

        for message_id in command_ids:
            self.assertEqual(catalog[message_id].level, bot_app.BotMessageLevel.NORMAL)
        for message_id in verbose_background_ids:
            self.assertEqual(catalog[message_id].level, bot_app.BotMessageLevel.VERBOSE)
        for message_id in diagnostic_ids:
            self.assertEqual(catalog[message_id].level, bot_app.BotMessageLevel.DEBUG)

if __name__ == "__main__":
    unittest.main()
