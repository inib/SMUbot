from __future__ import annotations
import os, re, asyncio, json, yaml, logging
from typing import Optional, Dict, List, Tuple, Callable, Awaitable, Set, Any
from enum import IntEnum
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta
from command_resolution import default_commands_map, load_commands_map

import aiohttp
from twitchio import eventsub
from twitchio.ext import commands
from twitchio.payloads import TokenRefreshedPayload
if __package__:
    from .chat_command_core import (
        ChatCommandContext,
        NormalizedChatInput,
        dispatch_chat_command,
        execute_playlist_request,
        execute_points,
        execute_prioritize,
        execute_random_request,
        execute_remove,
        execute_request,
        parse_chat_command,
    )
else:
    from chat_command_core import (
        ChatCommandContext,
        NormalizedChatInput,
        dispatch_chat_command,
        execute_playlist_request,
        execute_points,
        execute_prioritize,
        execute_random_request,
        execute_remove,
        execute_request,
        parse_chat_command,
    )

# ---- Env ----
# Configure logging before other components so that early startup messages are visible.
LOG_LEVEL = os.getenv("BOT_LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("songbot.bot")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.propagate = False

# Full URL of the backend API, defaulting to the docker-compose service name.
BACKEND_URL = os.getenv('BACKEND_URL', 'http://api:7070')
# Token used for privileged requests to the backend.
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', 'change-me')
MESSAGES_PATH = Path(os.getenv("BOT_MESSAGES_PATH", "/bot/messages.yml"))
COMMANDS_FILE = os.getenv('COMMANDS_FILE', '/bot/commands.yml')
DEFAULT_COMMANDS = default_commands_map()


DEFAULT_MESSAGES = {
    'currency_singular': 'point',
    'currency_plural': 'points',
    'channel_not_registered': 'Channel not registered in backend',
    'request_added': 'Added: {artist} - {title}',
    'random_request_added': 'Random pick: {artist} - {title}',
    'random_not_found': 'No playlist found for "{keyword}"',
    'prioritize_limit': 'Limit reached: 3 prioritized songs per stream',
    'prioritize_no_target': 'No eligible request to prioritize',
    'prioritize_success': 'Prioritized request #{request_id}',
    'points': '{username}, {points} {currency_plural}',
    'remove_no_pending': 'You have no pending requests',
    'remove_success': 'Removed your latest request #{request_id}',
    'failed': 'Failed: {error}',
    'archive_success': 'Archived current queue and started new stream',
    'archive_denied': 'Only channel owner or moderators can archive the queue',
    'played_next': 'This was {artist} - {title} requested by {user}. Next up {next_artist} - {next_title} requested by {next_user}',
    'played_last': 'This was {artist} - {title} requested by {user}. @{channel} no more bumped songs',
    'bump_free': '{artist} - {title} got a free bump, congrats {user}',
    'award_follow': 'Thx for following {username}, take {word} - you have now {points} {currency_plural}',
    'award_raid': 'Thx for raiding {username}, take {word} - you have now {points} {currency_plural}',
    'award_gift_sub': 'Thx for gifting {count} subs {username}, take {word} - you have now {points} {currency_plural}',
    'award_bits': 'Thx for cheering {amount} bits {username}, take {word} - you have now {points} {currency_plural}',
    'bot_joined': 'Song queue bot connected to chat.',
    'bot_left': 'Song queue bot disconnected from chat.',
    'playlist_request_added': 'Added from {playlist}: {artist} - {title}',
    'playlist_not_found': 'Playlist "{playlist}" not found',
    'playlist_song_missing': 'Playlist "{playlist}" has no song #{index}',
    'playlist_usage': 'Usage: !playlist <name> <index>',
    'vip_points_awarded': 'Thx for becoming a VIP {username}, take {word} - you have now {points} {currency_plural}',
    'queue_position_changed': 'Request #{request_id} moved from #{old_position} to #{new_position}',
    'token_refreshed': 'Bot token refreshed successfully.',
    'action_failed_debug': 'Debug failure in {action}: {error}',
}



# Channels that should never trigger Twitch joins/subscriptions even if they
# appear in the backend (e.g. seeded test data).
EXCLUDED_CHANNEL_LOGINS = {
    "example channel",
    "example_channel",
}

YOUTUBE_PATTERNS = [
    re.compile(r"https?://(www\.)?youtube\.com/watch\?v=([\w-]{11})", re.I),
    re.compile(r"https?://(music\.)?youtube\.com/watch\?v=([\w-]{11})", re.I),
    re.compile(r"https?://youtu\.be/([\w-]{11})", re.I),
]

# ---- Backend client ----
class BackendError(RuntimeError):
    def __init__(self, status: int, detail: object):
        message = detail if isinstance(detail, str) else str(detail)
        super().__init__(message)
        self.status = status
        self.detail = message

class Backend:
    def __init__(self, base_url: str, admin_token: str):
        self.base = base_url.rstrip('/')
        self.headers = { 'X-Admin-Token': admin_token, 'Content-Type': 'application/json' }
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def _req(self, method: str, path: str, payload: Optional[dict] = None):
        if not self.session:
            await self.start()
        url = f"{self.base}{path}"
        async with self.session.request(method, url, headers=self.headers, data=json.dumps(payload) if payload else None) as r:
            content_type = r.headers.get('content-type', '')
            is_json = content_type.startswith('application/json')
            if r.status >= 400:
                detail: object = ''
                if is_json:
                    try:
                        data = await r.json()
                    except Exception:
                        data = None
                    if isinstance(data, dict) and 'detail' in data:
                        detail = data['detail']
                    else:
                        detail = data or ''
                if not detail:
                    try:
                        detail = await r.text()
                    except Exception:
                        detail = ''
                if isinstance(detail, list):
                    detail = ', '.join(str(item) for item in detail)
                raise BackendError(r.status, detail or f"{method} {path} failed")
            if is_json:
                return await r.json()
            return await r.text()

    async def get_channels(self):
        return await self._req('GET', "/channels")

    async def get_system_config(self) -> dict:
        """Return backend system config used for ingress runtime gating.

        Dependencies: calls the backend ``/system/config`` endpoint.
        Code customers: ``SongBot.sync_channels`` websocket fallback decisions.
        Used variables/origin: uses admin-authenticated API response fields such
        as ``chat_ingress_mode`` and ``chat_websocket_fallback_legacy_enabled``.
        """

        payload = await self._req('GET', "/system/config")
        return payload if isinstance(payload, dict) else {}

    async def add_channel(self, channel_name: str, channel_id: str, join_active: int = 1):
        return await self._req('POST', "/channels", {
            'channel_name': channel_name,
            'channel_id': channel_id,
            'join_active': join_active,
        })

    async def set_bot_status(self, channel: str, active: bool, error: Optional[str] = None):
        payload = {'active': bool(active), 'error': error}
        try:
            return await self._req('POST', f"/channels/{channel}/bot_status", payload)
        except Exception:
            # Status updates are advisory; swallow errors to avoid breaking runtime behaviour.
            return None

    async def find_or_create_user(self, channel: str, twitch_id: str, username: str) -> int:
        """Find an existing channel user or create one using the paginated search response.

        Dependencies: consumes the `/channels/{channel}/users` paginated payload
        with an `items` list defined by `backend_app.UserPage`. Code customers:
        chat event handlers that map Twitch chatters to backend users. Used
        variables/origin: caller-provided `twitch_id`/`username` parameters and
        `items` entries returned by the backend search, which may be absent when
        no matches exist.
        """

        users_response = await self._req('GET', f"/channels/{channel}/users?search={username}")
        items = []
        if isinstance(users_response, dict):
            raw_items = users_response.get('items') or []
            if isinstance(raw_items, list):
                items = raw_items
        elif isinstance(users_response, list):  # backward compatibility if pagination is bypassed
            items = users_response

        for user in items:
            if user.get('twitch_id') == twitch_id and user.get('id') is not None:
                return user['id']

        resp = await self._req('POST', f"/channels/{channel}/users", { 'twitch_id': twitch_id, 'username': username })
        return resp['id']

    async def search_song(self, channel: str, query: str) -> Optional[dict]:
        songs = await self._req('GET', f"/channels/{channel}/songs?search={query}")
        return songs[0] if songs else None

    async def song_by_link(self, channel: str, link: str) -> Optional[dict]:
        songs = await self._req('GET', f"/channels/{channel}/songs?search={link}")
        for s in songs:
            if s.get('youtube_link') == link:
                return s
        return None

    async def add_song(self, channel: str, artist: str, title: str, link: Optional[str]) -> int:
        resp = await self._req('POST', f"/channels/{channel}/songs", {
            'artist': artist, 'title': title, 'youtube_link': link
        })
        return resp['id']

    async def add_request(self, channel: str, song_id: int, user_id: int,
                          want_priority: bool, prefer_sub_free: bool, is_subscriber: bool, is_mod: bool):
        return await self._req('POST', f"/channels/{channel}/queue", {
            'song_id': song_id, 'user_id': user_id,
            'want_priority': want_priority,
            'prefer_sub_free': prefer_sub_free,
            'is_subscriber': is_subscriber,
            'is_mod': is_mod,
        })

    async def get_queue(self, channel: str, include_played: bool = False):
        path = f"/channels/{channel}/queue"
        if include_played:
            path += "?include_played=1"
        return await self._req('GET', path)

    async def list_playlists(self, channel: str) -> List[dict]:
        return await self._req('GET', f"/channels/{channel}/playlists")

    async def delete_request(self, channel: str, request_id: int):
        return await self._req('DELETE', f"/channels/{channel}/queue/{request_id}")

    async def archive_stream(self, channel: str):
        return await self._req('POST', f"/channels/{channel}/streams/archive")

    async def get_user(self, channel: str, user_id: int):
        return await self._req('GET', f"/channels/{channel}/users/{user_id}")

    async def get_song(self, channel: str, song_id: int):
        return await self._req('GET', f"/channels/{channel}/songs/{song_id}")

    async def get_events(self, channel: str, since: Optional[str] = None):
        path = f"/channels/{channel}/events"
        if since:
            path += f"?since={since}"
        return await self._req('GET', path)

    async def get_bot_config(self) -> Dict[str, object]:
        return await self._req('GET', "/bot/config")

    async def update_bot_tokens(
        self,
        *,
        access_token: str,
        refresh_token: str,
        expires_at: Optional[str],
        scopes: List[str],
    ) -> Dict[str, object]:
        payload = {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'expires_at': expires_at,
            'scopes': scopes,
        }
        return await self._req('POST', "/bot/config/tokens", payload)

    async def push_bot_log(
        self,
        *,
        level: str = 'info',
        message: str,
        metadata: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        payload = {
            'level': level,
            'message': message,
            'metadata': metadata or {},
            'source': 'bot',
        }
        return await self._req('POST', "/bot/logs", payload)

    async def announce_runtime_event(
        self,
        *,
        channel: str,
        message_id: str,
        template_vars: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        """Send a runtime event announcement through backend Send Chat pipeline.

        Dependencies: calls backend ``POST /bot/runtime/announcements`` with
        admin-token auth.
        Code customers: ``SongBot`` non-chat queue/event producer methods
        (``check_played``, ``check_bumps``, ``check_queue_position_changes``,
        ``announce_event``).
        Used variables/origin: ``channel`` comes from backend queue polling
        state, ``message_id`` selects the catalog entry, and ``template_vars``
        carries formatter values from queue/event payload fields.
        """

        payload = {
            'channel': channel,
            'message_id': message_id,
            'template_vars': template_vars or {},
        }
        return await self._req('POST', "/bot/runtime/announcements", payload)

    async def random_playlist_request(
        self,
        channel: str,
        *,
        keyword: Optional[str],
        twitch_id: str,
        username: str,
        is_subscriber: bool,
    ) -> Dict[str, object]:
        payload: Dict[str, object] = {
            'twitch_id': twitch_id,
            'username': username,
            'is_subscriber': bool(is_subscriber),
        }
        if keyword:
            payload['keyword'] = keyword
        return await self._req('POST', f"/channels/{channel}/playlists/random_request", payload)

    async def playlist_request(
        self,
        channel: str,
        *,
        identifier: str,
        index: int,
    ) -> Dict[str, object]:
        payload = {'identifier': identifier, 'index': index}
        return await self._req('POST', f"/channels/{channel}/playlists/request", payload)


backend = Backend(BACKEND_URL, ADMIN_TOKEN)


@dataclass
class BotSettings:
    token: Optional[str]
    refresh_token: Optional[str]
    login: Optional[str]
    client_id: Optional[str]
    client_secret: Optional[str]
    bot_user_id: Optional[str]
    scopes: List[str]
    enabled: bool
    error: Optional[str] = None


class BotMessageLevel(IntEnum):
    """Enumerates chat visibility tiers used by channel-level message filtering.

    Dependencies: the enum values are persisted in backend channel settings via
    `bot_message_level` (string form) and evaluated by `SongBot._send_bot_message`.
    Code customers: command handlers, queue event announcers, and channel
    join/part notifications that annotate each outgoing message category.
    Used variables/origin: members are fixed constants where smaller values are
    less visible (`MUTE`) and higher values are more verbose (`DEBUG`).
    """

    MUTE = 0
    NORMAL = 1
    VERBOSE = 2
    DEBUG = 3


@dataclass(frozen=True)
class BotMessageCatalogEntry:
    """Defines a logical bot message identity and default presentation behavior."""

    template_key: str
    level: BotMessageLevel
    description: str
    group: Optional[str] = None


DEFAULT_MESSAGE_CATALOG: Dict[str, BotMessageCatalogEntry] = {
    'channel_not_registered': BotMessageCatalogEntry('channel_not_registered', BotMessageLevel.NORMAL, 'Command target channel is not registered.', 'commands'),
    'request_added': BotMessageCatalogEntry('request_added', BotMessageLevel.NORMAL, 'User song request accepted.', 'commands'),
    'random_request_added': BotMessageCatalogEntry('random_request_added', BotMessageLevel.NORMAL, 'Random playlist request accepted.', 'commands'),
    'random_not_found': BotMessageCatalogEntry('random_not_found', BotMessageLevel.NORMAL, 'Random playlist keyword did not match any playlist.', 'commands'),
    'playlist_request_added': BotMessageCatalogEntry('playlist_request_added', BotMessageLevel.NORMAL, 'Playlist song request accepted.', 'commands'),
    'playlist_not_found': BotMessageCatalogEntry('playlist_not_found', BotMessageLevel.NORMAL, 'Playlist command referenced a missing playlist.', 'commands'),
    'playlist_song_missing': BotMessageCatalogEntry('playlist_song_missing', BotMessageLevel.NORMAL, 'Playlist command referenced an out-of-range song index.', 'commands'),
    'playlist_usage': BotMessageCatalogEntry('playlist_usage', BotMessageLevel.NORMAL, 'Playlist command usage guidance.', 'commands'),
    'prioritize_limit': BotMessageCatalogEntry('prioritize_limit', BotMessageLevel.NORMAL, 'User reached prioritize limit.', 'commands'),
    'prioritize_no_target': BotMessageCatalogEntry('prioritize_no_target', BotMessageLevel.NORMAL, 'No eligible request to prioritize.', 'commands'),
    'prioritize_success': BotMessageCatalogEntry('prioritize_success', BotMessageLevel.NORMAL, 'Request prioritized successfully.', 'commands'),
    'points': BotMessageCatalogEntry('points', BotMessageLevel.NORMAL, 'Points command response.', 'commands'),
    'remove_no_pending': BotMessageCatalogEntry('remove_no_pending', BotMessageLevel.NORMAL, 'Remove command found no pending requests.', 'commands'),
    'remove_success': BotMessageCatalogEntry('remove_success', BotMessageLevel.NORMAL, 'Remove command deleted latest request.', 'commands'),
    'archive_success': BotMessageCatalogEntry('archive_success', BotMessageLevel.NORMAL, 'Queue archive command succeeded.', 'commands'),
    'archive_denied': BotMessageCatalogEntry('archive_denied', BotMessageLevel.NORMAL, 'Archive command denied due to missing permissions.', 'commands'),
    'failed': BotMessageCatalogEntry('failed', BotMessageLevel.NORMAL, 'Command failed with a user-facing error.', 'errors'),
    'bot_joined': BotMessageCatalogEntry('bot_joined', BotMessageLevel.VERBOSE, 'Bot joined channel chat.', 'lifecycle'),
    'bot_left': BotMessageCatalogEntry('bot_left', BotMessageLevel.VERBOSE, 'Bot left channel chat.', 'lifecycle'),
    'played_next': BotMessageCatalogEntry('played_next', BotMessageLevel.VERBOSE, 'Playback advanced and next prioritized song announced.', 'queue'),
    'played_last': BotMessageCatalogEntry('played_last', BotMessageLevel.VERBOSE, 'Playback advanced and no prioritized songs remain.', 'queue'),
    'bump_free': BotMessageCatalogEntry('bump_free', BotMessageLevel.VERBOSE, 'Automatic free bump was granted.', 'rewards'),
    'award_follow': BotMessageCatalogEntry('award_follow', BotMessageLevel.VERBOSE, 'Follow reward points announcement.', 'rewards'),
    'award_raid': BotMessageCatalogEntry('award_raid', BotMessageLevel.VERBOSE, 'Raid reward points announcement.', 'rewards'),
    'award_gift_sub': BotMessageCatalogEntry('award_gift_sub', BotMessageLevel.VERBOSE, 'Gifted subscription reward points announcement.', 'rewards'),
    'award_bits': BotMessageCatalogEntry('award_bits', BotMessageLevel.VERBOSE, 'Bits reward points announcement.', 'rewards'),
    'vip_points_awarded': BotMessageCatalogEntry('vip_points_awarded', BotMessageLevel.VERBOSE, 'VIP reward points announcement.', 'rewards'),
    'queue_position_changed': BotMessageCatalogEntry('queue_position_changed', BotMessageLevel.VERBOSE, 'Queue item moved to a new position.', 'queue'),
    'token_refreshed': BotMessageCatalogEntry('token_refreshed', BotMessageLevel.DEBUG, 'Token refresh succeeded.', 'lifecycle'),
    'action_failed_debug': BotMessageCatalogEntry('action_failed_debug', BotMessageLevel.DEBUG, 'Internal action failure diagnostic.', 'errors'),
}


def _format_token(token: str) -> str:
    return token.removeprefix('oauth:') if token else token


async def push_console_event(
    level: str,
    message: str,
    *,
    event: Optional[str] = None,
    metadata: Optional[Dict[str, object]] = None,
):
    level_key = (level or "").lower()
    log_level = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "warn": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }.get(level_key, logging.INFO)
    extra = ""
    if metadata:
        try:
            extra = f" | {json.dumps(metadata, sort_keys=True)}"
        except Exception:
            extra = f" | {metadata}"
    logger.log(log_level, f"{message}{extra}")
    meta = dict(metadata or {})
    if event:
        meta.setdefault('event', event)
    try:
        await backend.push_bot_log(level=level, message=message, metadata=meta)
    except Exception:
        # Console streaming is best-effort; avoid crashing the bot when the
        # backend is temporarily unavailable.
        pass

# ---- helpers ----
async def fetch_youtube_oembed_title(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    oembed_url = f"https://www.youtube.com/oembed?url={url}&format=json"
    try:
        async with session.get(oembed_url, timeout=8) as r:
            if r.status == 200:
                data = await r.json()
                return data.get('title')
    except Exception:
        return None
    return None

def parse_artist_title(raw: str) -> Tuple[str, str]:
    if ' - ' in raw:
        a, t = raw.split(' - ', 1)
        return a.strip(), t.strip()
    return 'Unknown', raw.strip()

def extract_youtube_url(text: str) -> Optional[str]:
    for pat in YOUTUBE_PATTERNS:
        m = pat.search(text)
        if m:
            vid = m.group(m.lastindex)
            return f"https://www.youtube.com/watch?v={vid}"
    return None

def load_commands(path: str) -> Dict[str, List[str]]:
    """Load normalized command aliases from YAML with bot-parity defaults.

    Dependencies: shared `load_commands_map` utility and `COMMANDS_FILE`
    filesystem path. Code customers: `SongBot` startup initialization. Used
    variables/origin: `path` is sourced from `COMMANDS_FILE` env/default.
    """

    return load_commands_map(path, logger=logger)


def load_messages(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = DEFAULT_MESSAGES.copy()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
            cfg.update(data)
    except FileNotFoundError:
        pass
    return cfg

# ---- bot ----
class SongBot(commands.Bot):
    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        bot_id: str,
        token: str,
        refresh_token: str,
        login: str,
        scopes: List[str],
        enabled: bool = True,
    ):
        if not token or not refresh_token or not login or not bot_id:
            raise RuntimeError('token, refresh_token, login, and bot_id are required')
        self.commands_map = load_commands(COMMANDS_FILE)
        self.messages = load_messages(MESSAGES_PATH)
        self.message_catalog = DEFAULT_MESSAGE_CATALOG.copy()
        self.currency_singular = self.messages.get('currency_singular', 'point')
        self.currency_plural = self.messages.get('currency_plural', 'points')
        prefix = self.commands_map['prefix'][0]
        super().__init__(
            client_id=client_id,
            client_secret=client_secret,
            bot_id=str(bot_id),
            prefix=prefix,
            fetch_client_user=False,
        )
        self.channel_map: Dict[str, Dict] = {}
        self.listeners: Dict[str, asyncio.Task] = {}
        self.state: Dict[str, Dict] = {}
        self.joined: Set[str] = set()
        self._sync_lock = asyncio.Lock()
        self.ready_event = asyncio.Event()
        self.enabled = enabled
        self._configured_login = login
        self.bot_user_id = str(bot_id)
        self._user_token = token
        self._refresh_token = refresh_token
        self._scopes = list(scopes or [])
        self._subscription_ids: Dict[str, str] = {}
        self._update_locks: Dict[str, asyncio.Lock] = {}
        self._refresher_task: Optional[asyncio.Task] = None
        self._websocket_fallback_enabled = False

    @property
    def configured_login(self) -> Optional[str]:
        return self._configured_login

    async def load_tokens(self, path: Optional[str] = None) -> None:
        if not self._user_token or not self._refresh_token:
            raise RuntimeError('Bot credentials are unavailable')
        payload = await super().add_token(self._user_token, self._refresh_token)
        self._scopes = list(payload.scopes)
        await self._persist_tokens(
            access_token=self._user_token,
            refresh_token=self._refresh_token,
            expires_in=payload.expires_in,
            scopes=self._scopes,
        )

    async def save_tokens(self, path: Optional[str] = None) -> None:
        # Tokens are persisted to the backend, so skip file writes.
        return None

    async def _persist_tokens(
        self,
        *,
        access_token: str,
        refresh_token: str,
        expires_in: Optional[int],
        scopes: List[str],
    ) -> None:
        expires_at_str: Optional[str] = None
        if expires_in is not None:
            try:
                expires_at = datetime.utcnow() + timedelta(seconds=int(expires_in))
                expires_at_str = expires_at.isoformat()
            except Exception:
                expires_at_str = None
        try:
            await backend.update_bot_tokens(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=expires_at_str,
                scopes=scopes,
            )
        except Exception:
            # Persisting tokens should not crash the bot if the backend is unavailable.
            pass

    async def event_token_refreshed(self, payload: TokenRefreshedPayload) -> None:
        # cleanup_candidate: token refresh events sourced from live websocket
        # session lifecycle are now legacy because backend_app runs an
        # authoritative refresh worker for webhook_conduit mode. Keep this path
        # as rollback compatibility until worker stability is confirmed.
        self._user_token = payload.token
        self._refresh_token = payload.refresh_token
        self._scopes = list(payload.scopes)
        await self._persist_tokens(
            access_token=payload.token,
            refresh_token=payload.refresh_token,
            expires_in=payload.expires_in,
            scopes=self._scopes,
        )
        for login in list(self.joined):
            await self._send_catalog_message(
                login,
                'token_refreshed',
                metadata={'event': 'token_refresh'},
            )

    async def event_ready(self) -> None:
        if self.enabled:
            await self.sync_channels()
        self._ensure_refresher_running()
        self.ready_event.set()

    def _ensure_refresher_running(self) -> None:
        task = self._refresher_task
        if task and not task.done():
            return
        self._refresher_task = asyncio.create_task(self.channel_refresher())

    async def _cancel_refresher(self) -> None:
        task = self._refresher_task
        if not task:
            return
        cancel = getattr(task, 'cancel', None)
        if callable(cancel):
            cancel()
        if isinstance(task, asyncio.Task):
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._refresher_task = None

    async def channel_refresher(self) -> None:
        try:
            while True:
                await asyncio.sleep(60)
                if self.enabled:
                    await self.sync_channels()
        except asyncio.CancelledError:
            raise
        finally:
            self._refresher_task = None

    def _channel_login(self, name: str) -> str:
        return name.lower()

    def _channel_info(self, login: str) -> Optional[Dict]:
        return self.channel_map.get(self._channel_login(login))

    async def sync_channels(self) -> None:
        """Synchronize channel listeners and optional websocket rollback hooks.

        Description: refresh channel authorization state, keep backend queue
        listeners aligned, and only enable websocket chat subscriptions when an
        explicit rollback flag allows legacy ingress.
        Dependencies: backend ``/system/config`` and ``/channels`` APIs plus
        SongBot listener/subscription lifecycle helpers.
        Code customers: ``event_ready``, periodic ``channel_refresher``, and
        runtime enable/disable transitions.
        Used variables/origin: ``chat_ingress_mode`` and
        ``chat_websocket_fallback_legacy_enabled`` from system config drive
        websocket fallback gating; channel rows from ``backend.get_channels``.
        """

        if not self.enabled:
            await self._disable_all_channels()
            return
        async with self._sync_lock:
            config = await backend.get_system_config()
            ingress_mode = str(config.get("chat_ingress_mode") or "").strip().lower()
            rollback_enabled = bool(config.get("chat_websocket_fallback_legacy_enabled"))
            self._websocket_fallback_enabled = rollback_enabled
            if ingress_mode == "webhook_conduit":
                logger.info(
                    "Canonical chat ingress is webhook_conduit; websocket EventSub chat subscriptions are rollback-only (enabled=%s)",
                    rollback_enabled,
                )
            rows = await backend.get_channels()
            allowed: Dict[str, Dict] = {}
            for row in rows:
                login = self._channel_login(row['channel_name'])
                if login in EXCLUDED_CHANNEL_LOGINS:
                    continue
                if row.get('authorized') and row.get('join_active'):
                    allowed[login] = row
            current_keys = set(self.channel_map.keys())
            allowed_keys = set(allowed.keys())
            added_preview = sorted(allowed_keys - current_keys)
            removed_preview = sorted(current_keys - allowed_keys)
            logger.info(
                "CHANNEL_SYNC_SUMMARY considered=%s listened=%s rollback_websocket=%s added=%s removed=%s",
                len(rows),
                len(current_keys),
                rollback_enabled,
                added_preview,
                removed_preview,
            )

            removed = current_keys - allowed_keys
            for key in removed:
                row = self.channel_map.pop(key)
                channel_name = row['channel_name']
                broadcaster_id = str(row.get('channel_id') or '')
                task = self.listeners.pop(key, None)
                if task:
                    task.cancel()
                await self._unsubscribe_channel(broadcaster_id)
                if key in self.joined:
                    await self._announce_left(key)
                    self.joined.discard(key)
                await backend.set_bot_status(channel_name, False)
                await push_console_event(
                    'info',
                    f'Parted channel {channel_name}',
                    event='part',
                    metadata={'channel': channel_name},
                )
                self.state.pop(key, None)
                self._update_locks.pop(key, None)

            for key in allowed_keys & current_keys:
                self.channel_map[key] = allowed[key]

            new_keys = allowed_keys - current_keys
            for key in new_keys:
                row = allowed[key]
                channel_name = row['channel_name']
                broadcaster_id = str(row.get('channel_id') or '')
                try:
                    await self._subscribe_for_channel(broadcaster_id)
                except Exception as exc:
                    await backend.set_bot_status(channel_name, False, str(exc))
                    await push_console_event(
                        'error',
                        f'Failed to subscribe channel {channel_name}: {exc}',
                        event='join_error',
                        metadata={'channel': channel_name, 'error': str(exc)},
                    )
                    await self._announce_debug_failure(
                        key,
                        action='subscribe_channel',
                        error=exc,
                        metadata={'channel': channel_name},
                    )
                    self.channel_map.pop(key, None)
                    continue
                await backend.set_bot_status(channel_name, True)
                await push_console_event(
                    'info',
                    f'Subscribed channel {channel_name}',
                    event='join',
                    metadata={'channel': channel_name},
                )
                self.joined.add(key)
                asyncio.create_task(self._announce_joined(key))
                initial_queue = await backend.get_queue(channel_name, include_played=True)
                self.state[key] = {
                    'channel_name': channel_name,
                    'queue': initial_queue,
                    'last_event': datetime.utcnow().isoformat(),
                }
                self.channel_map[key] = row
                self.listeners[key] = asyncio.create_task(self.listen_backend(channel_name))

            for key, row in self.channel_map.items():
                channel_name = row['channel_name']
                if key not in self.listeners:
                    self.listeners[key] = asyncio.create_task(self.listen_backend(channel_name))
                if key not in self.state:
                    initial_queue = await backend.get_queue(channel_name, include_played=True)
                    self.state[key] = {
                        'channel_name': channel_name,
                        'queue': initial_queue,
                        'last_event': datetime.utcnow().isoformat(),
                    }
                await self._subscribe_for_channel(str(row.get('channel_id') or ''))

    def _extract_subscription_id(self, response: object) -> Optional[str]:
        """[deprecated/unused] Extract websocket subscription IDs from TwitchIO responses.

        Description: retained temporary parser for old websocket recovery flows.
        Dependencies: TwitchIO response payload shape.
        Code customers: none in authoritative webhook-conduit mode.
        Used variables/origin: ``response`` object from historical
        ``subscribe_websocket`` calls.
        TODO(removal): delete when websocket rollback harness is retired.
        """

        if not response:
            return None
        if isinstance(response, dict):
            data = response.get('data')
            if isinstance(data, list) and data:
                first = data[0]
                if isinstance(first, dict):
                    sub_id = first.get('id')
                    if sub_id:
                        return str(sub_id)
        subscription = getattr(response, 'subscription', None)
        sub_id = getattr(subscription, 'id', None)
        if sub_id:
            return str(sub_id)
        sub_id = getattr(response, 'id', None)
        if sub_id:
            return str(sub_id)
        return None

    async def _find_existing_subscription_id(self, broadcaster_id: str) -> Optional[str]:
        """[deprecated/unused] Lookup existing websocket chat subscription ID.

        Description: legacy helper formerly used by subscription recovery loops.
        Dependencies: TwitchIO websocket/EventSub subscription fetch APIs.
        Code customers: none after removing websocket reuse/recovery loops.
        Used variables/origin: ``broadcaster_id`` from backend channel rows and
        ``self.bot_user_id`` from bot credentials.
        TODO(removal): remove with websocket fallback end-of-life.
        """

        for existing_id, details in self.websocket_subscriptions().items():
            condition = getattr(details, 'condition', {}) or {}
            sub_type = getattr(details, 'type', None)
            if (
                sub_type == eventsub.SubscriptionType.ChannelChatMessage
                and condition.get('broadcaster_user_id') == broadcaster_id
                and condition.get('user_id') == self.bot_user_id
            ):
                return str(existing_id)
        try:
            events = await self.fetch_eventsub_subscriptions(
                token_for=self.bot_user_id,
                type=eventsub.SubscriptionType.ChannelChatMessage.value,
            )
        except Exception:
            return None
        if not events:
            return None
        async for subscription in events.subscriptions:
            condition = getattr(subscription, 'condition', {}) or {}
            if (
                condition.get('broadcaster_user_id') == broadcaster_id
                and condition.get('user_id') == self.bot_user_id
            ):
                sub_id = getattr(subscription, 'id', None)
                if sub_id:
                    return str(sub_id)
        return None

    async def _subscribe_for_channel(self, broadcaster_id: str) -> None:
        """Create websocket chat subscription only for explicit rollback mode.

        Description: authoritative ``webhook_conduit`` mode never depends on
        websocket ingress; this hook exists solely for emergency rollback.
        Dependencies: TwitchIO ``subscribe_websocket`` API and
        ``self._websocket_fallback_enabled`` from ``sync_channels`` policy.
        Code customers: ``sync_channels`` channel lifecycle during rollback.
        Used variables/origin: ``broadcaster_id`` from backend channel config
        and ``self.bot_user_id`` from loaded bot credentials.
        """

        if not broadcaster_id:
            raise RuntimeError('Channel missing broadcaster id')
        if not self._websocket_fallback_enabled:
            return
        if broadcaster_id in self._subscription_ids:
            return
        payload = eventsub.ChatMessageSubscription(
            broadcaster_user_id=broadcaster_id,
            user_id=self.bot_user_id,
        )
        response = await self.subscribe_websocket(payload=payload, as_bot=True)
        sub_id = self._extract_subscription_id(response)
        if not sub_id:
            raise RuntimeError('Subscription id unavailable')
        self._subscription_ids[broadcaster_id] = sub_id
        logger.info(
            "Created new websocket subscription %s for broadcaster %s",
            sub_id,
            broadcaster_id,
        )

    async def _unsubscribe_channel(self, broadcaster_id: str) -> None:
        """Delete a rollback websocket subscription for the given broadcaster.

        Description: best-effort cleanup for legacy websocket fallback only.
        Dependencies: TwitchIO ``delete_websocket_subscription`` API.
        Code customers: channel removal and bot shutdown flows.
        Used variables/origin: ``broadcaster_id`` from channel config and
        ``self._subscription_ids`` populated by ``_subscribe_for_channel``.
        """

        sub_id = self._subscription_ids.pop(broadcaster_id, None)
        if not sub_id:
            return
        try:
            await self.delete_websocket_subscription(sub_id, force=True)
        except Exception:
            pass

    async def _disable_all_channels(self) -> None:
        listener_tasks = list(self.listeners.values())
        for task in listener_tasks:
            cancel = getattr(task, 'cancel', None)
            if callable(cancel):
                cancel()
        awaitables = [task for task in listener_tasks if isinstance(task, asyncio.Task)]
        if awaitables:
            await asyncio.gather(*awaitables, return_exceptions=True)
        self.listeners.clear()
        for key, row in list(self.channel_map.items()):
            await self._unsubscribe_channel(str(row.get('channel_id') or ''))
            if key in self.joined:
                await self._announce_left(key)
                self.joined.discard(key)
            await backend.set_bot_status(row['channel_name'], False)
        self.channel_map.clear()
        self.state.clear()
        self._update_locks.clear()
        await self._cancel_refresher()

    async def shutdown(self) -> None:
        await self._cancel_refresher()
        await self._disable_all_channels()
        shutdown = getattr(super(), 'shutdown', None)
        if callable(shutdown):
            await shutdown()
        else:
            await super().close()
        await backend.close()

    async def _announce_joined(self, login: str) -> None:
        info = self._channel_info(login)
        channel_label = info.get('channel_name') if info else login
        await self._send_catalog_message(
            login,
            'bot_joined',
            metadata={'channel': channel_label, 'event': 'bot_join'},
        )

    async def _announce_left(self, login: str) -> None:
        info = self._channel_info(login)
        channel_label = info.get('channel_name') if info else login
        await self._send_catalog_message(
            login,
            'bot_left',
            metadata={'channel': channel_label, 'event': 'bot_part'},
        )

    def _coerce_message_level(self, raw_level: object) -> BotMessageLevel:
        """Normalize backend/channel message-level inputs to enum values.

        Dependencies: backend channel payloads may provide `bot_message_level`
        as strings, integers, or missing values. Code customers: message policy
        resolver and unit tests asserting level normalization. Used
        variables/origin: `raw_level` comes from `channel_map` rows or callers.
        """

        if isinstance(raw_level, BotMessageLevel):
            return raw_level
        if isinstance(raw_level, str):
            value = raw_level.strip().lower()
            by_name = {
                'mute': BotMessageLevel.MUTE,
                'normal': BotMessageLevel.NORMAL,
                'verbose': BotMessageLevel.VERBOSE,
                'debug': BotMessageLevel.DEBUG,
            }
            if value in by_name:
                return by_name[value]
            if value.isdigit():
                raw_level = int(value)
        if isinstance(raw_level, (int, float)):
            by_value = {
                0: BotMessageLevel.MUTE,
                1: BotMessageLevel.NORMAL,
                2: BotMessageLevel.VERBOSE,
                3: BotMessageLevel.DEBUG,
            }
            return by_value.get(int(raw_level), BotMessageLevel.NORMAL)
        return BotMessageLevel.NORMAL

    def _resolve_channel_message_threshold(self, channel_login: str) -> BotMessageLevel:
        """Resolve per-channel chat threshold from cached channel settings.

        Dependencies: reads `self.channel_map` entries synchronized from backend
        `/channels` responses. Code customers: `_send_bot_message` visibility
        gate for all outbound chat. Used variables/origin: `channel_login`
        derives from Twitch channel logins and maps to lowercase dict keys.
        """

        channel_info = (getattr(self, 'channel_map', {}) or {}).get(self._channel_login(channel_login), {})
        raw_level = None
        if isinstance(channel_info, dict):
            raw_level = channel_info.get('bot_message_level')
            settings = channel_info.get('settings')
            if raw_level is None and isinstance(settings, dict):
                raw_level = settings.get('bot_message_level')
        return self._coerce_message_level(raw_level)

    def _resolve_message_catalog_entry(
        self,
        channel_login: str,
        message_id: str,
    ) -> BotMessageCatalogEntry:
        """Resolve message catalog entries with optional per-channel overrides."""

        catalog = getattr(self, 'message_catalog', DEFAULT_MESSAGE_CATALOG) or DEFAULT_MESSAGE_CATALOG
        entry = catalog.get(message_id)
        if not entry:
            return BotMessageCatalogEntry(
                template_key=message_id,
                level=BotMessageLevel.NORMAL,
                description='Ad-hoc message key fallback.',
            )
        channel_info = (getattr(self, 'channel_map', {}) or {}).get(self._channel_login(channel_login), {})
        overrides: Dict[str, Dict[str, Any]] = {}
        if isinstance(channel_info, dict):
            raw_overrides = channel_info.get('bot_message_overrides') or channel_info.get('message_overrides')
            if isinstance(raw_overrides, dict):
                overrides = raw_overrides
        override = overrides.get(message_id, {}) if isinstance(overrides, dict) else {}
        if not isinstance(override, dict):
            override = {}
        template_key = str(override.get('template_key') or entry.template_key)
        level_value = self._coerce_message_level(override.get('level', entry.level))
        return BotMessageCatalogEntry(
            template_key=template_key,
            level=level_value,
            description=entry.description,
            group=entry.group,
        )

    async def _send_catalog_message(
        self,
        channel_login: str,
        message_id: str,
        *,
        template_vars: Optional[Dict[str, object]] = None,
        metadata: Optional[Dict[str, object]] = None,
        reply_to: Optional[str] = None,
        fallback_partial: Optional[object] = None,
    ) -> None:
        """Send a logical message by catalog ID with templating and level policy."""

        entry = self._resolve_message_catalog_entry(channel_login, message_id)
        message_templates = getattr(self, 'messages', DEFAULT_MESSAGES) or DEFAULT_MESSAGES
        template = message_templates.get(entry.template_key)
        if not template:
            return
        values = template_vars or {}
        try:
            message_text = template.format(**values)
        except Exception:
            message_text = template
        enriched_meta = {
            **(metadata or {}),
            'message_id': message_id,
            'template_key': entry.template_key,
            'group': entry.group,
        }
        await self._send_bot_message(
            channel_login,
            message_text,
            level=entry.level,
            metadata=enriched_meta,
            reply_to=reply_to,
            fallback_partial=fallback_partial,
        )

    async def _send_bot_message(
        self,
        channel_login: str,
        message_or_key: str,
        *,
        level: BotMessageLevel = BotMessageLevel.NORMAL,
        metadata: Optional[Dict[str, object]] = None,
        reply_to: Optional[str] = None,
        fallback_partial: Optional[object] = None,
        message_key: bool = False,
    ) -> None:
        """Send a chat message only when it passes channel visibility policy.

        Dependencies: uses `self.channel_map` message threshold settings and
        delegates delivery to `_send_message`; always emits backend console logs
        through `push_console_event` for observability. Code customers: all bot
        command and event handlers. Used variables/origin: message content comes
        from `message_or_key` (literal text or `self.messages` key), while
        `level` is declared at each call site. Visibility rule: `MUTE`
        channels always suppress chat, otherwise messages are allowed when the
        resolved message level is less than or equal to the configured channel
        threshold (`resolved_level <= threshold`).
        """

        resolved_level = self._coerce_message_level(level)
        threshold = self._resolve_channel_message_threshold(channel_login)
        allow_chat = threshold != BotMessageLevel.MUTE and resolved_level <= threshold
        message_text = self.messages.get(message_or_key, '') if message_key else message_or_key
        if not message_text:
            return
        policy_meta = {
            **(metadata or {}),
            'channel_login': channel_login,
            'message_level': int(resolved_level),
            'message_level_name': resolved_level.name.lower(),
            'channel_threshold': int(threshold),
            'channel_threshold_name': threshold.name.lower(),
        }
        if not allow_chat:
            await push_console_event(
                'info',
                f'Suppressed chat message for {channel_login}',
                event='message_suppressed',
                metadata={**policy_meta, 'suppressed_text': message_text},
            )
            return
        await self._send_message(
            channel_login,
            message_text,
            metadata=policy_meta,
            reply_to=reply_to,
            fallback_partial=fallback_partial,
        )

    async def _announce_debug_failure(
        self,
        channel_login: str,
        *,
        action: str,
        error: object,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        """Emit debug-tier failure diagnostics using catalog templates."""

        await self._send_catalog_message(
            channel_login,
            'action_failed_debug',
            template_vars={'action': action, 'error': error},
            metadata={**(metadata or {}), 'event': 'action_failed_debug'},
        )

    async def _send_message(
        self,
        channel_login: str,
        message: str,
        *,
        metadata: Optional[Dict[str, object]] = None,
        reply_to: Optional[str] = None,
        fallback_partial: Optional[object] = None,
    ) -> None:
        """Send a websocket-runtime chat message using bot-user token contract.

        Description: rollback-only websocket egress helper; authoritative
        webhook/conduit ingress should use backend Send Chat transport instead.
        Dependencies: TwitchIO ``PartialUser.send_message`` and runtime
        ``self._websocket_fallback_enabled`` policy flag.
        Code customers: websocket ingress command handlers and debug/catalog
        responses emitted during rollback operation.
        Used variables/origin: ``channel_login`` and ``message`` come from
        command dispatch flows; ``self.bot_user_id`` is persisted bot identity
        used for both ``sender`` and ``token_for`` in TwitchIO sends.
        """

        if not self._websocket_fallback_enabled:
            await push_console_event(
                'warning',
                f'Skipped websocket send for {channel_login}: fallback mode disabled',
                event='message_skipped',
                metadata={**(metadata or {}), 'channel': channel_login, 'reason_code': 'websocket_fallback_disabled'},
            )
            return
        info = self._channel_info(channel_login)
        partial = None
        channel_label = channel_login
        if info:
            channel_label = info.get('channel_name') or channel_login
            try:
                partial = self.create_partialuser(info.get('channel_id'), info.get('channel_name'))
            except Exception:
                partial = None
        if partial is None and fallback_partial is not None:
            partial = fallback_partial
            channel_label = getattr(fallback_partial, 'display_name', None) or getattr(fallback_partial, 'name', channel_login)
        if partial is None:
            return
        if not self.bot_user_id:
            await push_console_event(
                'error',
                f'Failed to send message to {channel_label}: bot_user_id is missing',
                event='message',
                metadata={**(metadata or {}), 'channel': channel_label, 'reason_code': 'missing_bot_user_id'},
            )
            return
        try:
            await partial.send_message(
                message,
                sender=self.bot_user_id,
                token_for=self.bot_user_id,
                reply_to_message_id=reply_to,
            )
            await push_console_event(
                'info',
                f'Sent message to {channel_label}',
                event='message',
                metadata={**(metadata or {}), 'sent_text': message, 'channel': channel_label},
            )
        except Exception as exc:
            await push_console_event(
                'error',
                f'Failed to send message to {channel_label}: {exc}',
                event='message',
                metadata={**(metadata or {}), 'channel': channel_label, 'error': str(exc)},
            )
            if (metadata or {}).get('event') != 'action_failed_debug':
                await self._announce_debug_failure(
                    channel_login,
                    action='send_message',
                    error=exc,
                    metadata={'channel': channel_label},
                )

    async def _announce_backend_runtime_event(
        self,
        *,
        login: str,
        channel: str,
        message_id: str,
        template_vars: Optional[Dict[str, object]] = None,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        """Publish catalog announcements via backend authoritative transport.

        Dependencies: backend ``announce_runtime_event`` endpoint.
        Code customers: transitional runtime producers while backend mutation
        endpoints are being validated end-to-end.
        Used variables/origin: arguments originate from queue/event diffs and
        are forwarded unchanged to backend templating payload.
        """

        try:
            response = await backend.announce_runtime_event(
                channel=channel,
                message_id=message_id,
                template_vars=template_vars or {},
            )
            if not bool((response or {}).get('sent', True)):
                await push_console_event(
                    'warning',
                    f"Backend runtime announcement suppressed for {channel} "
                    f"(message_id={(response or {}).get('message_id', message_id)}, "
                    f"visibility={(response or {}).get('visibility', 'unknown')})",
                    event='runtime_announcement_suppressed',
                    metadata={
                        **(metadata or {}),
                        'channel': channel,
                        'message_id': (response or {}).get('message_id', message_id),
                        'visibility': (response or {}).get('visibility'),
                        'reason_code': (response or {}).get('reason_code'),
                    },
                )
            return
        except Exception as exc:
            await push_console_event(
                'error',
                f'Backend runtime announcement failed for {channel}: {exc}',
                event='runtime_announcement_failed',
                metadata={**(metadata or {}), 'channel': channel, 'message_id': message_id, 'error': str(exc)},
            )

    async def update_enabled(self, enabled: bool) -> None:
        if self.enabled == enabled:
            return
        self.enabled = enabled
        await self.ready_event.wait()
        if not enabled:
            await push_console_event('info', 'Disabling bot', event='lifecycle')
            await self._disable_all_channels()
        else:
            await push_console_event('info', 'Enabling bot', event='lifecycle')
            await self.sync_channels()
            self._ensure_refresher_running()

    def _normalize_twitch_chat_input(self, message) -> NormalizedChatInput:
        """Convert TwitchIO chat payloads into normalized command DTOs.

        Dependencies: TwitchIO `event_message` payload fields (`broadcaster`,
        `chatter`, and `text`). Code customers: command dispatcher entrypoint
        and compatibility wrappers used in unit tests. Used variables/origin:
        IDs/roles/time values are copied from incoming payload attributes.
        """

        chatter = getattr(message, 'chatter', None)
        broadcaster = getattr(message, 'broadcaster', None)
        return NormalizedChatInput(
            channel_login=self._channel_login(getattr(broadcaster, 'name', '')),
            user_id=str(getattr(chatter, 'id', '')),
            username=(getattr(chatter, 'display_name', None) or getattr(chatter, 'name', '')),
            text=(getattr(message, 'text', None) or ''),
            is_subscriber=bool(getattr(chatter, 'subscriber', False)),
            is_moderator=bool(getattr(chatter, 'moderator', False)),
            is_broadcaster=bool(getattr(chatter, 'broadcaster', False)),
            message_id=getattr(message, 'id', None),
            message_timestamp=getattr(message, 'timestamp', None),
        )

    async def _fetch_youtube_title_by_url(self, url: str) -> Optional[str]:
        """Fetch YouTube title metadata with shared backend aiohttp session.

        Dependencies: global `backend` HTTP client session and
        `fetch_youtube_oembed_title`. Code customers: shared command core via
        `ChatCommandContext.fetch_youtube_title`. Used variables/origin: `url`
        comes from parsed chat command arguments.
        """

        if backend.session is None:
            await backend.start()
        return await fetch_youtube_oembed_title(backend.session, url)

    async def _log_command_error(self, message: str, metadata: Dict[str, object]) -> None:
        """Log command execution failures through consolidated console events.

        Dependencies: `push_console_event` logger bridge. Code customers:
        command core execution handlers through `ChatCommandContext.log_error`.
        Used variables/origin: parameters are composed in shared command logic.
        """

        await push_console_event('error', message, metadata=metadata)

    async def _send_command_reply(
        self,
        login: str,
        message: str,
        *,
        command: str,
        channel: str,
        reply_to: Optional[str] = None,
        **metadata: object,
    ) -> None:
        """Send command replies with standard bot message policy metadata.

        Dependencies: `_send_bot_message` visibility policy and channel map for
        fallback partial lookup. Code customers: reusable command core handlers
        via `ChatCommandContext.send_reply`. Used variables/origin:
        `command/channel/reply_to` are passed through from command execution.
        """

        info = self.channel_map.get(login) or {}
        fallback_partial = None
        channel_id = info.get('channel_id')
        channel_name = info.get('channel_name') or channel
        if channel_id:
            try:
                fallback_partial = self.create_partialuser(channel_id, channel_name)
            except Exception:
                fallback_partial = None
        await self._send_bot_message(
            login,
            message,
            level=BotMessageLevel.NORMAL,
            metadata={'channel': channel, 'command': command, **metadata},
            reply_to=reply_to,
            fallback_partial=fallback_partial,
        )

    def _build_chat_command_context(self) -> ChatCommandContext:
        """Build command-core dependency adapters from the current bot state.

        Dependencies: runtime command config, message templates, backend client,
        and helper callbacks. Code customers: `event_message` dispatcher and
        legacy `handle_*` wrappers. Used variables/origin: values are read from
        current `SongBot` instance attributes.
        """

        return ChatCommandContext(
            backend=backend,
            messages=getattr(self, 'messages', DEFAULT_MESSAGES),
            commands_map=getattr(self, 'commands_map', {k: ([v] if not isinstance(v, list) else v) for k, v in DEFAULT_COMMANDS.items()}),
            currency_plural=getattr(self, 'currency_plural', 'points'),
            channel_map=getattr(self, 'channel_map', {}),
            extract_youtube_url=extract_youtube_url,
            parse_artist_title=parse_artist_title,
            fetch_youtube_title=self._fetch_youtube_title_by_url,
            send_reply=self._send_command_reply,
            log_error=self._log_command_error,
            backend_error_cls=BackendError,
        )

    async def event_message(self, message) -> None:
        """Route Twitch chat messages through shared normalized command core.

        Dependencies: TwitchIO event payload and `parse_chat_command`
        dispatcher. Code customers: legacy websocket rollback ingress only.
        Used variables/origin: message payload fields are normalized first.
        TODO(removal): remove once websocket rollback support is retired.
        """

        if not self.enabled:
            return
        if getattr(message.chatter, 'id', None) == self.bot_user_id:
            return
        chat_input = self._normalize_twitch_chat_input(message)
        parsed = parse_chat_command(chat_input, self.commands_map)
        if not parsed:
            return
        if parsed.canonical == 'archive':
            await self.handle_archive(message)
            return
        await dispatch_chat_command(chat_input, parsed, self._build_chat_command_context())

    async def handle_request(self, msg, arg: str) -> None:
        """Compatibility wrapper executing request logic via normalized core.

        Dependencies: `_normalize_twitch_chat_input` and command dispatcher
        adapters. Code customers: tests and any direct call sites that invoke
        `handle_request`. Used variables/origin: `arg` is provided by caller.
        """

        chat_input = self._normalize_twitch_chat_input(msg)
        await execute_request(chat_input, arg, self._build_chat_command_context())

    async def handle_random_request(self, msg, arg: str) -> None:
        """Compatibility wrapper executing random logic via normalized core.

        Dependencies: shared command context adapters. Code customers: tests
        and direct bot method callers. Used variables/origin: `arg` from caller.
        """

        chat_input = self._normalize_twitch_chat_input(msg)
        await execute_random_request(chat_input, arg, self._build_chat_command_context())

    async def handle_playlist_request(self, msg, arg: str) -> None:
        """Compatibility wrapper executing playlist logic via normalized core.

        Dependencies: shared command context adapters. Code customers: tests
        and runtime callers using direct method dispatch. Used variables/origin:
        `arg` is provided by caller/event parser.
        """

        chat_input = self._normalize_twitch_chat_input(msg)
        await execute_playlist_request(chat_input, arg, self._build_chat_command_context())

    async def handle_prioritize(self, msg, arg: str) -> None:
        """Compatibility wrapper executing prioritize logic via shared core.

        Dependencies: normalized DTO converter and command context adapters.
        Code customers: tests and direct handler invocations. Used
        variables/origin: `arg` command arguments are caller-provided.
        """

        chat_input = self._normalize_twitch_chat_input(msg)
        await execute_prioritize(chat_input, arg, self._build_chat_command_context())

    async def handle_points(self, msg) -> None:
        """Compatibility wrapper executing points logic via normalized core.

        Dependencies: shared command dispatcher context and backend adapter.
        Code customers: tests and direct points handler call sites. Used
        variables/origin: requester identity comes from `msg` payload.
        """

        chat_input = self._normalize_twitch_chat_input(msg)
        await execute_points(chat_input, '', self._build_chat_command_context())

    async def handle_remove(self, msg) -> None:
        """Compatibility wrapper executing remove logic via normalized core.

        Dependencies: normalized DTO converter and reusable command core.
        Code customers: tests and direct `handle_remove` invocations. Used
        variables/origin: requester and channel fields come from `msg`.
        """

        chat_input = self._normalize_twitch_chat_input(msg)
        await execute_remove(chat_input, '', self._build_chat_command_context())

    async def handle_archive(self, msg) -> None:
        if not (msg.chatter.moderator or msg.chatter.broadcaster):
            await self._send_bot_message(
                self._channel_login(msg.broadcaster.name),
                self.messages['archive_denied'],
                level=BotMessageLevel.NORMAL,
                metadata={'channel': msg.broadcaster.name, 'command': 'archive'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        login = self._channel_login(msg.broadcaster.name)
        row = self.channel_map.get(login)
        if not row:
            await self._send_bot_message(
                login,
                self.messages['channel_not_registered'],
                level=BotMessageLevel.NORMAL,
                metadata={'channel': msg.broadcaster.name, 'command': 'archive'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        channel = row['channel_name']
        try:
            await backend.archive_stream(channel)
            await self.process_backend_update(channel)
            await self._send_bot_message(
                login,
                self.messages['archive_success'],
                level=BotMessageLevel.NORMAL,
                metadata={'channel': channel, 'command': 'archive'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
        except Exception as exc:
            await push_console_event(
                'error',
                f'Failed to archive queue for {msg.chatter.name}: {exc}',
                metadata={'channel': channel, 'command': 'archive'},
            )
            await self._send_bot_message(
                login,
                self.messages['failed'].format(error=exc),
                level=BotMessageLevel.NORMAL,
                metadata={'channel': channel, 'command': 'archive'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )

    async def listen_backend(self, ch_name: str) -> None:
        """Stream authenticated queue updates from the backend for ``ch_name``.

        Dependencies: relies on the shared ``backend`` client session and its
        configured ``backend.headers`` for authentication. Uses an
        ``aiohttp.ClientTimeout`` with unlimited read time to avoid idle
        disconnects when streams are quiet.
        Code customers: backend listener tasks spawned in ``ensure_listener``
        and reconnection logic expecting live queue events.
        Variables/origins: ``ch_name`` is the backend channel slug; ``url``
        derives from ``backend.base`` and the channel path; ``backend.session``
        is initialized via ``backend.start`` before use; ``stream_timeout``
        originates here to guard against connect stalls.
        """

        url = f"{backend.base}/channels/{ch_name}/queue/stream"
        stream_timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
        while True:
            try:
                if backend.session is None:
                    await backend.start()
                async with backend.session.get(
                    url, headers=backend.headers, timeout=stream_timeout
                ) as resp:
                    if resp.status >= 400:
                        detail = await resp.text()
                        raise BackendError(resp.status, detail or f"GET {url} failed")
                    async for line in resp.content:
                        line = line.decode().strip()
                        if line.startswith('data:'):
                            await self.process_backend_update(ch_name)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                await push_console_event(
                    'error',
                    f'Queue stream error for {ch_name}: {exc}',
                    event='backend',
                    metadata={'channel': ch_name},
                )
                await asyncio.sleep(5)

    async def process_backend_update(self, ch_name: str) -> None:
        login = self._channel_login(ch_name)
        lock = self._update_locks.setdefault(login, asyncio.Lock())
        async with lock:
            state = self.state.get(login, {})
            prev_queue = state.get('queue', [])
            last_event = state.get('last_event')
            new_queue = await backend.get_queue(ch_name, include_played=True)

            # Backend queue/event mutation endpoints now emit non-chat
            # announcements authoritatively; runtime diff announcers are retired.
            state['queue'] = new_queue
            state['channel_name'] = ch_name
            self.state[login] = state
class BotService:
    def __init__(
        self,
        backend_client: Backend,
        *,
        poll_interval: int = 15,
        idle_recheck_interval: int = 900,
        bot_factory: Optional[Callable[..., SongBot]] = None,
        task_factory: Optional[Callable[[Awaitable], asyncio.Task]] = None,
    ):
        self.backend = backend_client
        self.poll_interval = poll_interval
        self.idle_recheck_interval = idle_recheck_interval
        self.bot_factory = bot_factory or (lambda **kwargs: SongBot(**kwargs))
        self._create_task = task_factory or asyncio.create_task
        self._bot: Optional[SongBot] = None
        self._bot_task: Optional[asyncio.Task] = None
        self._current_token: Optional[str] = None
        self._current_login: Optional[str] = None
        self._current_refresh: Optional[str] = None
        self._current_client_id: Optional[str] = None
        self._current_client_secret: Optional[str] = None
        self._current_bot_id: Optional[str] = None
        self._current_scopes: List[str] = []
        self._credentials_available: Optional[bool] = None
        self._last_enabled: Optional[bool] = None

    async def run(self):
        """Run the bot worker loop and continuously apply backend bot config.

        Dependencies: backend ``/bot/config`` API and ``apply_settings``.
        Code customers: ``main`` entrypoint process for the bot container.
        Used variables/origin: poll cadence uses ``self.poll_interval`` and each
        iteration applies the latest backend-managed credential settings.
        """

        while True:
            try:
                raw_config = await self.backend.get_bot_config()
            except Exception as exc:
                await push_console_event(
                    'error',
                    f'Failed to fetch bot configuration: {exc}',
                    event='config',
                )
                raw_config = {}
            settings = self._settings_from_config(raw_config)
            try:
                await self.apply_settings(settings)
            except Exception as exc:
                await push_console_event(
                    'error',
                    f'Failed to apply bot configuration: {exc}',
                    event='config',
                )
            await asyncio.sleep(self.poll_interval)

    async def apply_settings(self, settings: BotSettings):
        required_fields = {
            'access_token': settings.token,
            'refresh_token': settings.refresh_token,
            'login': settings.login,
            'client_id': settings.client_id,
            'client_secret': settings.client_secret,
            'bot_user_id': settings.bot_user_id,
        }
        missing = [name for name, value in required_fields.items() if not value]
        if missing:
            if self._credentials_available is not False:
                error_details = settings.error or f"missing {', '.join(missing)}"
                await push_console_event(
                    'error',
                    f'Bot credentials are unavailable; idling worker ({error_details})',
                    event='startup',
                    metadata={'error': settings.error or error_details},
                )
            self._credentials_available = False
            self._last_enabled = None
            await self._stop_bot(reason='missing_credentials')
            return
        if self._credentials_available is not True:
            await push_console_event(
                'info',
                'Bot credentials resolved',
                event='startup',
            )
        self._credentials_available = True

        if not settings.enabled:
            if self._last_enabled is not False:
                await push_console_event(
                    'info',
                    'Bot disabled in backend; idling',
                    event='lifecycle',
                )
            self._last_enabled = False
            await self._stop_bot(reason='disabled')
            return
        if self._last_enabled is not True:
            await push_console_event('info', 'Bot enabled in backend', event='lifecycle')
        self._last_enabled = True

        token = _format_token(settings.token)
        refresh = settings.refresh_token or ''
        scopes_sorted = sorted(settings.scopes or [])
        requires_restart = (
            self._bot is None
            or token != self._current_token
            or refresh != self._current_refresh
            or settings.login != self._current_login
            or settings.client_id != self._current_client_id
            or settings.client_secret != self._current_client_secret
            or settings.bot_user_id != self._current_bot_id
            or scopes_sorted != self._current_scopes
        )
        if requires_restart:
            await self._restart_bot(
                token=token,
                refresh_token=refresh,
                login=settings.login,
                enabled=settings.enabled,
                client_id=settings.client_id,
                client_secret=settings.client_secret,
                bot_user_id=settings.bot_user_id,
                scopes=settings.scopes or [],
            )
        elif self._bot:
            await self._bot.update_enabled(settings.enabled)

    async def _restart_bot(
        self,
        *,
        token: str,
        refresh_token: str,
        login: str,
        enabled: bool,
        client_id: str,
        client_secret: str,
        bot_user_id: str,
        scopes: List[str],
    ):
        await self._stop_bot(reason='restarting')
        await push_console_event(
            'info',
            f'Connecting bot as {login}',
            event='lifecycle',
        )
        bot = self.bot_factory(
            client_id=client_id,
            client_secret=client_secret,
            bot_id=bot_user_id,
            token=token,
            refresh_token=refresh_token,
            login=login,
            scopes=scopes,
            enabled=enabled,
        )
        self._bot = bot
        self._current_token = token
        self._current_login = login
        self._current_refresh = refresh_token
        self._current_client_id = client_id
        self._current_client_secret = client_secret
        self._current_bot_id = bot_user_id
        self._current_scopes = sorted(scopes)
        self._bot_task = self._create_task(bot.start())

    async def _stop_bot(self, *, reason: Optional[str] = None):
        if not self._bot:
            return
        try:
            if hasattr(self._bot, 'shutdown'):
                await self._bot.shutdown()
            else:
                await self._bot.close()
        except Exception as exc:
            await push_console_event(
                'error',
                f'Error while stopping bot: {exc}',
                event='lifecycle',
            )
        if self._bot_task:
            try:
                await self._bot_task
            except Exception:
                pass
        self._bot = None
        self._bot_task = None
        self._current_token = None
        self._current_login = None
        self._current_refresh = None
        self._current_client_id = None
        self._current_client_secret = None
        self._current_bot_id = None
        self._current_scopes = []
        if reason:
            await push_console_event(
                'info',
                f'Bot stopped ({reason})',
                event='lifecycle',
            )

    def _settings_from_config(self, data: Dict[str, object]) -> BotSettings:
        config = data or {}
        if not isinstance(config, dict):
            return BotSettings(
                token=None,
                refresh_token=None,
                login=None,
                client_id=None,
                client_secret=None,
                bot_user_id=None,
                scopes=[],
                enabled=False,
                error='Backend returned invalid bot configuration payload',
            )

        token = config.get('access_token') or config.get('token')
        refresh = config.get('refresh_token')
        login = config.get('login') or config.get('bot_login')
        client_id = config.get('client_id')
        client_secret = config.get('client_secret')
        bot_user_id = config.get('bot_user_id') or config.get('bot_id')
        raw_scopes = config.get('scopes') or []
        if isinstance(raw_scopes, str):
            scopes = [scope for scope in raw_scopes.split() if scope]
        elif isinstance(raw_scopes, list):
            scopes = [str(scope) for scope in raw_scopes if scope]
        else:
            scopes = []
        enabled_flag = config.get('enabled') if 'enabled' in config else None

        missing: List[str] = []
        if not token:
            missing.append('access_token')
        if not refresh:
            missing.append('refresh_token')
        if not login:
            missing.append('login')
        if not client_id:
            missing.append('client_id')
        if not client_secret:
            missing.append('client_secret')
        if not bot_user_id:
            missing.append('bot_user_id')
        if missing:
            reason = 'Missing bot credentials: ' + ', '.join(missing)
            return BotSettings(
                token=None,
                refresh_token=None,
                login=None,
                client_id=None,
                client_secret=None,
                bot_user_id=None,
                scopes=[],
                enabled=False,
                error=reason,
            )

        enabled = bool(enabled_flag)
        return BotSettings(
            token=token,
            refresh_token=refresh,
            login=login,
            client_id=client_id,
            client_secret=client_secret,
            bot_user_id=bot_user_id,
            scopes=scopes,
            enabled=enabled,
        )

    # NOTE(removal): duplicate BotService command parser/handlers were removed.
    # Runtime command execution is now centralized in `SongBot.event_message`
    # + `bot.chat_command_core` and webhook notifications in
    # `backend_app._dispatch_eventsub_chat_command`.

    async def listen_backend(self, ch_name: str):
        """Stream authenticated queue updates from the backend for ``ch_name``.

        Dependencies: uses the shared ``backend`` client session and
        ``backend.headers`` for authentication alongside an
        ``aiohttp.ClientTimeout`` that disables read timeouts to avoid idle
        disconnects.
        Code customers: listener tasks created by queue monitoring routines
        that expect real-time updates.
        Variables/origins: ``ch_name`` identifies the backend channel; ``url``
        uses ``backend.base``; ``backend.session`` comes from ``backend.start``;
        ``stream_timeout`` originates locally to constrain connection setup
        while allowing endless streaming.
        """

        url = f"{backend.base}/channels/{ch_name}/queue/stream"
        stream_timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
        while True:
            try:
                if backend.session is None:
                    await backend.start()
                async with backend.session.get(
                    url, headers=backend.headers, timeout=stream_timeout
                ) as resp:
                    if resp.status >= 400:
                        detail = await resp.text()
                        raise BackendError(resp.status, detail or f"GET {url} failed")
                    async for line in resp.content:
                        line = line.decode().strip()
                        if line.startswith("data:"):
                            await self.process_backend_update(ch_name)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                await push_console_event(
                    'error',
                    f'Queue stream error for {ch_name}: {exc}',
                    event='backend',
                    metadata={'channel': ch_name},
                )
                await asyncio.sleep(5)

    async def process_backend_update(self, ch_name: str):
        state = self.state.get(ch_name, {})
        prev_queue = state.get('queue', [])
        last_event = state.get('last_event')
        new_queue = await backend.get_queue(ch_name, include_played=True)

        await self.check_played(ch_name, prev_queue, new_queue)
        await self.check_bumps(ch_name, prev_queue, new_queue)

        events = await backend.get_events(ch_name, since=last_event) if last_event else await backend.get_events(ch_name)
        if events:
            for ev in reversed(events):
                ev_time = ev['event_time']
                if last_event and ev_time <= last_event:
                    continue
                await self.announce_event(ch_name, ev)
            state['last_event'] = max(ev['event_time'] for ev in events)
        state['queue'] = new_queue

    async def check_played(self, ch_name: str, prev_queue: List[dict], new_queue: List[dict]):
        """Announce newly played queue entries via catalog-templated websocket messages.

        Dependencies: backend `get_song` and `get_user` lookups, plus
        `_send_catalog_message` for message rendering and channel policy-aware
        delivery. Code customers: legacy websocket queue polling path.
        Used variables/origin: queue snapshots originate from poll_state loop,
        while template vars come from current/next request song and user fields.
        """
        prev_map = {q['id']: q for q in prev_queue}
        chan = self.get_channel(ch_name.lower())
        if not chan:
            return
        for req in new_queue:
            old = prev_map.get(req['id'])
            if old and old['played'] == 0 and req['played'] == 1:
                song = await backend.get_song(ch_name, req['song_id'])
                user = await backend.get_user(ch_name, req['user_id'])
                pending_prio = [q for q in new_queue if q['played'] == 0 and q['is_priority'] == 1]
                if pending_prio:
                    next_req = pending_prio[0]
                    next_song = await backend.get_song(ch_name, next_req['song_id'])
                    next_user = await backend.get_user(ch_name, next_req['user_id'])
                else:
                    next_song = {}
                    next_user = {}
                await self._send_catalog_message(
                    chan,
                    'played_next' if pending_prio else 'played_last',
                    template_vars={
                        'artist': song.get('artist', '?'),
                        'title': song.get('title', '?'),
                        'user': user.get('username', '?'),
                        'next_artist': next_song.get('artist', '?') if pending_prio else '',
                        'next_title': next_song.get('title', '?') if pending_prio else '',
                        'next_user': next_user.get('username', '?') if pending_prio else '',
                        'channel': ch_name,
                    },
                    metadata={'channel': ch_name, 'event': 'played'},
                )

    async def check_bumps(self, ch_name: str, prev_queue: List[dict], new_queue: List[dict]):
        prev_map = {q['id']: q for q in prev_queue}
        chan = self.get_channel(ch_name.lower())
        if not chan:
            return
        for req in new_queue:
            old = prev_map.get(req['id'])
            new_prio = req['is_priority'] == 1 and req.get('priority_source') == 'admin'
            was_prio = old and old['is_priority'] == 1 if old else False
            if new_prio and not was_prio:
                song = await backend.get_song(ch_name, req['song_id'])
                user = await backend.get_user(ch_name, req['user_id'])
                await self._send_bot_message(
                    chan,
                    self.messages['bump_free'].format(
                        artist=song.get('artist', '?'),
                        title=song.get('title', '?'),
                        user=user.get('username', '?'),
                    ),
                    metadata={'channel': ch_name, 'event': 'bump'},
                )

    async def announce_event(self, ch_name: str, ev: dict):
        chan = self.get_channel(ch_name.lower())
        if not chan:
            return
        user = None
        if ev.get('user_id'):
            user = await backend.get_user(ch_name, ev['user_id'])
        if not user:
            return
        meta = json.loads(ev.get('meta') or '{}')
        etype = ev['type']
        delta = 1
        extra: Dict[str, int] = {}
        if etype == 'gift_sub':
            count = int(meta.get('count', 1))
            delta = count
            extra['count'] = count
        elif etype == 'bits':
            amount = int(meta.get('amount', 0))
            extra['amount'] = amount
        elif etype == 'vip':
            delta = int(meta.get('count', 1) or 1)
        elif etype not in ('follow', 'raid'):
            return
        word = (
            f"this {self.currency_singular}"
            if delta == 1
            else f"these {delta} {self.currency_plural}"
        )
        template = self.messages.get(f"award_{etype}")
        if template:
            await self._send_bot_message(
                chan,
                template.format(
                    username=user.get('username', ''),
                    word=word,
                    points=user.get('prio_points', 0),
                    currency_plural=self.currency_plural,
                    **extra,
                ),
                metadata={'channel': ch_name, 'event': etype},
            )

# ---- entry ----
async def main():
    await backend.start()
    service = BotService(backend)
    await service.run()

if __name__ == '__main__':
    asyncio.run(main())
