from __future__ import annotations
import os, re, asyncio, json, yaml
from typing import Optional, Dict, List, Tuple, Callable, Awaitable, Set
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta

import aiohttp
from twitchio import eventsub
from twitchio.ext import commands
from twitchio.payloads import TokenRefreshedPayload

# ---- Env ----
# Full URL of the backend API, defaulting to the docker-compose service name.
BACKEND_URL = os.getenv('BACKEND_URL', 'http://api:7070')
# Token used for privileged requests to the backend.
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', 'change-me')
TWITCH_CLIENT_ID_ENV = os.getenv('TWITCH_CLIENT_ID')
TWITCH_CLIENT_SECRET_ENV = os.getenv('TWITCH_CLIENT_SECRET')
BOT_USER_ID_ENV = os.getenv('BOT_USER_ID') or os.getenv('TWITCH_BOT_USER_ID')
MESSAGES_PATH = Path(os.getenv("BOT_MESSAGES_PATH", "/bot/messages.yml"))

COMMANDS_FILE = os.getenv('COMMANDS_FILE', '/bot/commands.yml')
DEFAULT_COMMANDS = {
    'prefix': '!',
    'request': ['request', 'req', 'r'],
    'prioritize': ['prioritize', 'prio', 'bump'],
    'points': ['points', 'pp'],
    'remove': ['remove', 'undo', 'del'],
    'archive': ['archive'],
    'random_request': ['random', 'rr', 'randomrequest'],
}

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
        users = await self._req('GET', f"/channels/{channel}/users?search={username}")
        for u in users:
            if u['twitch_id'] == twitch_id:
                return u['id']
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
                          want_priority: bool, prefer_sub_free: bool, is_subscriber: bool):
        return await self._req('POST', f"/channels/{channel}/queue", {
            'song_id': song_id, 'user_id': user_id,
            'want_priority': want_priority,
            'prefer_sub_free': prefer_sub_free,
            'is_subscriber': is_subscriber,
        })

    async def get_queue(self, channel: str, include_played: bool = False):
        path = f"/channels/{channel}/queue"
        if include_played:
            path += "?include_played=1"
        return await self._req('GET', path)

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


def _format_token(token: str) -> str:
    return token.removeprefix('oauth:') if token else token


async def push_console_event(
    level: str,
    message: str,
    *,
    event: Optional[str] = None,
    metadata: Optional[Dict[str, object]] = None,
):
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
    cfg = DEFAULT_COMMANDS.copy()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
            cfg.update(data)
    except FileNotFoundError:
        pass
    return {k: v if isinstance(v, list) else [v] for k, v in cfg.items()}


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
        self._user_token = payload.token
        self._refresh_token = payload.refresh_token
        self._scopes = list(payload.scopes)
        await self._persist_tokens(
            access_token=payload.token,
            refresh_token=payload.refresh_token,
            expires_in=payload.expires_in,
            scopes=self._scopes,
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
        if not self.enabled:
            await self._disable_all_channels()
            return
        async with self._sync_lock:
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
        if not broadcaster_id:
            raise RuntimeError('Channel missing broadcaster id')
        if broadcaster_id in self._subscription_ids:
            return
        payload = eventsub.ChatMessageSubscription(
            broadcaster_user_id=broadcaster_id,
            user_id=self.bot_user_id,
        )
        response = await self.subscribe_websocket(payload=payload, as_bot=True)
        sub_id = self._extract_subscription_id(response)
        if not sub_id:
            sub_id = await self._find_existing_subscription_id(broadcaster_id)
        if not sub_id:
            raise RuntimeError('Subscription id unavailable')
        self._subscription_ids[broadcaster_id] = sub_id

    async def _unsubscribe_channel(self, broadcaster_id: str) -> None:
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
        message = self.messages.get('bot_joined')
        if not message:
            return
        info = self._channel_info(login)
        channel_label = info.get('channel_name') if info else login
        await self._send_message(login, message, metadata={'channel': channel_label, 'event': 'bot_join'})

    async def _announce_left(self, login: str) -> None:
        message = self.messages.get('bot_left')
        if not message:
            return
        info = self._channel_info(login)
        channel_label = info.get('channel_name') if info else login
        await self._send_message(login, message, metadata={'channel': channel_label, 'event': 'bot_part'})

    async def _send_message(
        self,
        channel_login: str,
        message: str,
        *,
        metadata: Optional[Dict[str, object]] = None,
        reply_to: Optional[str] = None,
        fallback_partial: Optional[object] = None,
    ) -> None:
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

    async def event_message(self, message) -> None:
        if not self.enabled:
            return
        if getattr(message.chatter, 'id', None) == self.bot_user_id:
            return
        content = (message.text or '').strip()
        prefix = self.commands_map['prefix'][0]
        if not content.startswith(prefix):
            return
        cmd, *rest = content[len(prefix):].split(' ', 1)
        args = rest[0] if rest else ''
        cmd_lower = cmd.lower()
        if cmd_lower in self.commands_map['request']:
            await self.handle_request(message, args)
        elif cmd_lower in self.commands_map['random_request']:
            await self.handle_random_request(message, args)
        elif cmd_lower in self.commands_map['prioritize']:
            await self.handle_prioritize(message, args)
        elif cmd_lower in self.commands_map['points']:
            await self.handle_points(message)
        elif cmd_lower in self.commands_map['remove']:
            await self.handle_remove(message)
        elif cmd_lower in self.commands_map['archive']:
            await self.handle_archive(message)

    async def handle_request(self, msg, arg: str) -> None:
        login = self._channel_login(msg.broadcaster.name)
        row = self.channel_map.get(login)
        if not row:
            await self._send_message(
                login,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.broadcaster.name, 'command': 'request'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        channel = row['channel_name']
        display_name = getattr(msg.chatter, 'display_name', None) or msg.chatter.name
        user_id = await backend.find_or_create_user(channel, str(msg.chatter.id), display_name)

        ylink = extract_youtube_url(arg)
        song = None
        if ylink:
            song = await backend.song_by_link(channel, ylink)
            if not song:
                if backend.session is None:
                    await backend.start()
                title_text = await fetch_youtube_oembed_title(backend.session, ylink)
                artist, title = parse_artist_title(title_text) if title_text else ("YouTube", ylink)
                song_id = await backend.add_song(channel, artist, title, ylink)
                song = {'id': song_id, 'artist': artist, 'title': title, 'youtube_link': ylink}
        else:
            artist, title = parse_artist_title(arg)
            found = await backend.search_song(channel, f"{artist} - {title}")
            if not found:
                song_id = await backend.add_song(channel, artist, title, None)
                song = {'id': song_id, 'artist': artist, 'title': title}
            else:
                song = found

        try:
            await backend.add_request(
                channel,
                song['id'],
                user_id,
                want_priority=False,
                prefer_sub_free=True,
                is_subscriber=bool(msg.chatter.subscriber),
            )
            await self._send_message(
                login,
                self.messages['request_added'].format(
                    artist=song.get('artist', ''),
                    title=song.get('title', ''),
                ),
                metadata={'channel': channel, 'command': 'request'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
        except Exception as exc:
            await push_console_event(
                'error',
                f'Failed to add request for {msg.chatter.name}: {exc}',
                metadata={'channel': channel, 'command': 'request'},
            )
            await self._send_message(
                login,
                self.messages['failed'].format(error=exc),
                metadata={'channel': channel, 'command': 'request'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )

    async def handle_random_request(self, msg, arg: str) -> None:
        login = self._channel_login(msg.broadcaster.name)
        row = self.channel_map.get(login)
        if not row:
            await self._send_message(
                login,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.broadcaster.name, 'command': 'random_request'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        channel = row['channel_name']
        display_name = getattr(msg.chatter, 'display_name', None) or msg.chatter.name
        keyword = (arg or '').strip()
        try:
            response = await backend.random_playlist_request(
                channel,
                keyword=keyword or None,
                twitch_id=str(msg.chatter.id),
                username=display_name,
                is_subscriber=bool(msg.chatter.subscriber),
            )
        except BackendError as exc:
            if exc.status == 404:
                template = self.messages.get('random_not_found', 'No playlist found for "{keyword}"')
                await self._send_message(
                    login,
                    template.format(keyword=keyword or 'default'),
                    metadata={'channel': channel, 'command': 'random_request'},
                    reply_to=msg.id,
                    fallback_partial=msg.broadcaster,
                )
                return
            await push_console_event(
                'error',
                f'Failed random request for {msg.chatter.name}: {exc.detail}',
                metadata={'channel': channel, 'command': 'random_request', 'status': exc.status, 'keyword': keyword},
            )
            await self._send_message(
                login,
                self.messages['failed'].format(error=exc.detail),
                metadata={'channel': channel, 'command': 'random_request'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        except Exception as exc:
            await push_console_event(
                'error',
                f'Failed random request for {msg.chatter.name}: {exc}',
                metadata={'channel': channel, 'command': 'random_request'},
            )
            await self._send_message(
                login,
                self.messages['failed'].format(error=exc),
                metadata={'channel': channel, 'command': 'random_request'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return

        song_payload = response.get('song') if isinstance(response, dict) else None
        artist = song_payload.get('artist', '') if isinstance(song_payload, dict) else ''
        title = song_payload.get('title', '') if isinstance(song_payload, dict) else ''
        resolved_keyword = ''
        if isinstance(response, dict):
            resolved_keyword = response.get('keyword') or ''
        template = self.messages.get('random_request_added') or self.messages.get('request_added')
        if template:
            try:
                message_text = template.format(
                    artist=artist,
                    title=title,
                    keyword=resolved_keyword or keyword or '',
                )
            except KeyError:
                message_text = template
            await self._send_message(
                login,
                message_text,
                metadata={'channel': channel, 'command': 'random_request', 'keyword': resolved_keyword or keyword},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )

    async def handle_prioritize(self, msg, arg: str) -> None:
        login = self._channel_login(msg.broadcaster.name)
        row = self.channel_map.get(login)
        if not row:
            await self._send_message(
                login,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.broadcaster.name, 'command': 'prioritize'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        channel = row['channel_name']
        display_name = getattr(msg.chatter, 'display_name', None) or msg.chatter.name
        user_id = await backend.find_or_create_user(channel, str(msg.chatter.id), display_name)

        queue = await backend.get_queue(channel)
        my_prio = [q for q in queue if q['user_id'] == user_id and q['is_priority'] == 1]
        if len(my_prio) >= 3:
            await self._send_message(
                login,
                self.messages['prioritize_limit'],
                metadata={'channel': channel, 'command': 'prioritize'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return

        target = None
        if arg.strip().isdigit():
            rid = int(arg.strip())
            target = next((q for q in queue if q['id'] == rid and q['user_id'] == user_id and q['played'] == 0), None)
        if not target:
            mine = [q for q in queue if q['user_id'] == user_id and q['played'] == 0 and q['is_priority'] == 0]
            target = mine[-1] if mine else None
        if not target:
            await self._send_message(
                login,
                self.messages['prioritize_no_target'],
                metadata={'channel': channel, 'command': 'prioritize'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return

        try:
            await backend.add_request(
                channel,
                target['song_id'],
                user_id,
                want_priority=True,
                prefer_sub_free=True,
                is_subscriber=bool(msg.chatter.subscriber),
            )
            await backend.delete_request(channel, target['id'])
            await self._send_message(
                login,
                self.messages['prioritize_success'].format(request_id=target['id']),
                metadata={'channel': channel, 'command': 'prioritize'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
        except Exception as exc:
            await push_console_event(
                'error',
                f'Failed to prioritize for {msg.chatter.name}: {exc}',
                metadata={'channel': channel, 'command': 'prioritize'},
            )
            await self._send_message(
                login,
                self.messages['failed'].format(error=exc),
                metadata={'channel': channel, 'command': 'prioritize'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )

    async def handle_points(self, msg) -> None:
        login = self._channel_login(msg.broadcaster.name)
        row = self.channel_map.get(login)
        if not row:
            await self._send_message(
                login,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.broadcaster.name, 'command': 'points'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        channel = row['channel_name']
        display_name = getattr(msg.chatter, 'display_name', None) or msg.chatter.name
        user_id = await backend.find_or_create_user(channel, str(msg.chatter.id), display_name)
        u = await backend.get_user(channel, user_id)
        await self._send_message(
            login,
            self.messages['points'].format(
                username=display_name,
                points=u.get('prio_points', 0),
                currency_plural=self.currency_plural,
            ),
            metadata={'channel': channel, 'command': 'points'},
            reply_to=msg.id,
            fallback_partial=msg.broadcaster,
        )

    async def handle_remove(self, msg) -> None:
        login = self._channel_login(msg.broadcaster.name)
        row = self.channel_map.get(login)
        if not row:
            await self._send_message(
                login,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.broadcaster.name, 'command': 'remove'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        channel = row['channel_name']
        display_name = getattr(msg.chatter, 'display_name', None) or msg.chatter.name
        user_id = await backend.find_or_create_user(channel, str(msg.chatter.id), display_name)
        queue = await backend.get_queue(channel)
        mine = [q for q in queue if q['user_id'] == user_id and q['played'] == 0]
        if not mine:
            await self._send_message(
                login,
                self.messages['remove_no_pending'],
                metadata={'channel': channel, 'command': 'remove'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        latest = mine[-1]
        try:
            await backend.delete_request(channel, latest['id'])
            await self._send_message(
                login,
                self.messages['remove_success'].format(request_id=latest['id']),
                metadata={'channel': channel, 'command': 'remove'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
        except Exception as exc:
            await push_console_event(
                'error',
                f'Failed to remove request for {msg.chatter.name}: {exc}',
                metadata={'channel': channel, 'command': 'remove'},
            )
            await self._send_message(
                login,
                self.messages['failed'].format(error=exc),
                metadata={'channel': channel, 'command': 'remove'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )

    async def handle_archive(self, msg) -> None:
        if not (msg.chatter.moderator or msg.chatter.broadcaster):
            await self._send_message(
                self._channel_login(msg.broadcaster.name),
                self.messages['archive_denied'],
                metadata={'channel': msg.broadcaster.name, 'command': 'archive'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        login = self._channel_login(msg.broadcaster.name)
        row = self.channel_map.get(login)
        if not row:
            await self._send_message(
                login,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.broadcaster.name, 'command': 'archive'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )
            return
        channel = row['channel_name']
        try:
            await backend.archive_stream(channel)
            await self.process_backend_update(channel)
            await self._send_message(
                login,
                self.messages['archive_success'],
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
            await self._send_message(
                login,
                self.messages['failed'].format(error=exc),
                metadata={'channel': channel, 'command': 'archive'},
                reply_to=msg.id,
                fallback_partial=msg.broadcaster,
            )

    async def listen_backend(self, ch_name: str) -> None:
        url = f"{backend.base}/channels/{ch_name}/queue/stream"
        while True:
            try:
                if backend.session is None:
                    await backend.start()
                async with backend.session.get(url) as resp:
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

            await self.check_played(login, ch_name, prev_queue, new_queue)
            await self.check_bumps(login, ch_name, prev_queue, new_queue)

            events = await backend.get_events(ch_name, since=last_event) if last_event else await backend.get_events(ch_name)
            if events:
                for ev in reversed(events):
                    ev_time = ev['event_time']
                    if last_event and ev_time <= last_event:
                        continue
                    await self.announce_event(login, ch_name, ev)
                state['last_event'] = max(ev['event_time'] for ev in events)
            state['queue'] = new_queue
            state['channel_name'] = ch_name
            self.state[login] = state

    async def check_played(
        self,
        login: str,
        channel: str,
        prev_queue: List[dict],
        new_queue: List[dict],
    ) -> None:
        if login not in self.joined:
            return
        prev_map = {q['id']: q for q in prev_queue}
        for req in new_queue:
            old = prev_map.get(req['id'])
            if old and old['played'] == 0 and req['played'] == 1:
                song = await backend.get_song(channel, req['song_id'])
                user = await backend.get_user(channel, req['user_id'])
                pending_prio = [q for q in new_queue if q['played'] == 0 and q['is_priority'] == 1]
                if pending_prio:
                    next_req = pending_prio[0]
                    next_song = await backend.get_song(channel, next_req['song_id'])
                    next_user = await backend.get_user(channel, next_req['user_id'])
                    msg = self.messages['played_next'].format(
                        artist=song.get('artist', '?'),
                        title=song.get('title', '?'),
                        user=user.get('username', '?'),
                        next_artist=next_song.get('artist', '?'),
                        next_title=next_song.get('title', '?'),
                        next_user=next_user.get('username', '?'),
                    )
                else:
                    msg = self.messages['played_last'].format(
                        artist=song.get('artist', '?'),
                        title=song.get('title', '?'),
                        user=user.get('username', '?'),
                        channel=channel,
                    )
                await self._send_message(
                    login,
                    msg,
                    metadata={'channel': channel, 'event': 'played'},
                )

    async def check_bumps(
        self,
        login: str,
        channel: str,
        prev_queue: List[dict],
        new_queue: List[dict],
    ) -> None:
        if login not in self.joined:
            return
        prev_map = {q['id']: q for q in prev_queue}
        for req in new_queue:
            old = prev_map.get(req['id'])
            new_prio = req['is_priority'] == 1 and req.get('priority_source') == 'admin'
            was_prio = old and old['is_priority'] == 1 if old else False
            if new_prio and not was_prio:
                song = await backend.get_song(channel, req['song_id'])
                user = await backend.get_user(channel, req['user_id'])
                await self._send_message(
                    login,
                    self.messages['bump_free'].format(
                        artist=song.get('artist', '?'),
                        title=song.get('title', '?'),
                        user=user.get('username', '?'),
                    ),
                    metadata={'channel': channel, 'event': 'bump'},
                )

    async def announce_event(self, login: str, channel: str, ev: dict) -> None:
        if login not in self.joined:
            return
        user = None
        if ev.get('user_id'):
            user = await backend.get_user(channel, ev['user_id'])
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
        elif etype not in ('follow', 'raid'):
            return
        word = (
            f"this {self.currency_singular}"
            if delta == 1
            else f"these {delta} {self.currency_plural}"
        )
        template = self.messages.get(f"award_{etype}")
        if template:
            await self._send_message(
                login,
                template.format(
                    username=user.get('username', ''),
                    word=word,
                    points=user.get('prio_points', 0),
                    currency_plural=self.currency_plural,
                    **extra,
                ),
                metadata={'channel': channel, 'event': etype},
            )
class BotService:
    def __init__(
        self,
        backend_client: Backend,
        *,
        poll_interval: int = 15,
        bot_factory: Optional[Callable[..., SongBot]] = None,
        task_factory: Optional[Callable[[Awaitable], asyncio.Task]] = None,
    ):
        self.backend = backend_client
        self.poll_interval = poll_interval
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
        client_id = config.get('client_id') or TWITCH_CLIENT_ID_ENV
        client_secret = config.get('client_secret') or TWITCH_CLIENT_SECRET_ENV
        bot_user_id = (
            config.get('bot_user_id')
            or config.get('bot_id')
            or BOT_USER_ID_ENV
        )
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

    async def event_message(self, message):
        if not self.enabled:
            return
        if message.echo:
            return
        content = message.content.strip()
        prefix = self.commands_map['prefix'][0]
        if not content.startswith(prefix):
            return
        cmd, *rest = content[len(prefix):].split(' ', 1)
        args = rest[0] if rest else ''
        cmd_lower = cmd.lower()
        if cmd_lower in self.commands_map['request']:
            await self.handle_request(message, args)
        elif cmd_lower in self.commands_map['prioritize']:
            await self.handle_prioritize(message, args)
        elif cmd_lower in self.commands_map['points']:
            await self.handle_points(message)
        elif cmd_lower in self.commands_map['remove']:
            await self.handle_remove(message)
        elif cmd_lower in self.commands_map['archive']:
            await self.handle_archive(message)

    async def handle_request(self, msg, arg: str):
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await self._send_message(
                msg.channel,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.channel.name, 'command': 'request'},
            )
            return
        channel = ch_row['channel_name']
        user_id = await backend.find_or_create_user(channel, str(msg.author.id), msg.author.name)

        # detect YouTube link
        ylink = extract_youtube_url(arg)
        song = None
        if ylink:
            song = await backend.song_by_link(channel, ylink)
            if not song:
                if backend.session is None:
                    await backend.start()
                title_text = await fetch_youtube_oembed_title(backend.session, ylink)
                artist, title = parse_artist_title(title_text) if title_text else ("YouTube", ylink)
                song_id = await backend.add_song(channel, artist, title, ylink)
                song = { 'id': song_id, 'artist': artist, 'title': title, 'youtube_link': ylink }
        else:
            artist, title = parse_artist_title(arg)
            found = await backend.search_song(channel, f"{artist} - {title}")
            if not found:
                song_id = await backend.add_song(channel, artist, title, None)
                song = { 'id': song_id, 'artist': artist, 'title': title }
            else:
                song = found

        try:
            # initial requests are non-priority
            await backend.add_request(
                channel,
                song['id'],
                user_id,
                want_priority=False,
                prefer_sub_free=True,
                is_subscriber=msg.author.is_subscriber,
            )
            await self._send_message(
                msg.channel,
                self.messages['request_added'].format(
                    artist=song.get('artist', ''),
                    title=song.get('title', ''),
                ),
                metadata={'channel': msg.channel.name, 'command': 'request'},
            )
        except Exception as e:
            await push_console_event(
                'error',
                f'Failed to add request for {msg.author.name}: {e}',
                metadata={'channel': msg.channel.name, 'command': 'request'},
            )
            await self._send_message(
                msg.channel,
                self.messages['failed'].format(error=e),
                metadata={'channel': msg.channel.name, 'command': 'request'},
            )

    async def handle_prioritize(self, msg, arg: str):
        # user can prioritize up to 3 songs per stream
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await self._send_message(
                msg.channel,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.channel.name, 'command': 'prioritize'},
            )
            return
        channel = ch_row['channel_name']
        user_id = await backend.find_or_create_user(channel, str(msg.author.id), msg.author.name)

        queue = await backend.get_queue(channel)
        my_prio = [q for q in queue if q['user_id'] == user_id and q['is_priority'] == 1]
        if len(my_prio) >= 3:
            await self._send_message(
                msg.channel,
                self.messages['prioritize_limit'],
                metadata={'channel': msg.channel.name, 'command': 'prioritize'},
            )
            return

        # choose target: numeric id if given, else latest non-priority pending
        target = None
        if arg.strip().isdigit():
            rid = int(arg.strip())
            target = next((q for q in queue if q['id'] == rid and q['user_id'] == user_id and q['played'] == 0), None)
        if not target:
            mine = [q for q in queue if q['user_id'] == user_id and q['played'] == 0 and q['is_priority'] == 0]
            target = mine[-1] if mine else None
        if not target:
            await self._send_message(
                msg.channel,
                self.messages['prioritize_no_target'],
                metadata={'channel': msg.channel.name, 'command': 'prioritize'},
            )
            return

        try:
            # re-add as priority, then delete old
            await backend.add_request(
                channel,
                target['song_id'],
                user_id,
                want_priority=True,
                prefer_sub_free=True,
                is_subscriber=msg.author.is_subscriber,
            )
            await backend.delete_request(channel, target['id'])
            await self._send_message(
                msg.channel,
                self.messages['prioritize_success'].format(request_id=target['id']),
                metadata={'channel': msg.channel.name, 'command': 'prioritize'},
            )
        except Exception as e:
            await push_console_event(
                'error',
                f'Failed to prioritize for {msg.author.name}: {e}',
                metadata={'channel': msg.channel.name, 'command': 'prioritize'},
            )
            await self._send_message(
                msg.channel,
                self.messages['failed'].format(error=e),
                metadata={'channel': msg.channel.name, 'command': 'prioritize'},
            )

    async def handle_points(self, msg):
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await self._send_message(
                msg.channel,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.channel.name, 'command': 'points'},
            )
            return
        channel = ch_row['channel_name']
        user_id = await backend.find_or_create_user(channel, str(msg.author.id), msg.author.name)
        u = await backend.get_user(channel, user_id)
        await self._send_message(
            msg.channel,
            self.messages['points'].format(
                username=msg.author.name,
                points=u.get('prio_points', 0),
                currency_plural=self.currency_plural,
            ),
            metadata={'channel': msg.channel.name, 'command': 'points'},
        )

    async def handle_remove(self, msg):
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await self._send_message(
                msg.channel,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.channel.name, 'command': 'remove'},
            )
            return
        channel = ch_row['channel_name']
        user_id = await backend.find_or_create_user(channel, str(msg.author.id), msg.author.name)
        queue = await backend.get_queue(channel)
        mine = [q for q in queue if q['user_id'] == user_id and q['played'] == 0]
        if not mine:
            await self._send_message(
                msg.channel,
                self.messages['remove_no_pending'],
                metadata={'channel': msg.channel.name, 'command': 'remove'},
            )
            return
        latest = mine[-1]
        try:
            await backend.delete_request(channel, latest['id'])
            await self._send_message(
                msg.channel,
                self.messages['remove_success'].format(request_id=latest['id']),
                metadata={'channel': msg.channel.name, 'command': 'remove'},
            )
        except Exception as e:
            await push_console_event(
                'error',
                f'Failed to remove request for {msg.author.name}: {e}',
                metadata={'channel': msg.channel.name, 'command': 'remove'},
            )
            await self._send_message(
                msg.channel,
                self.messages['failed'].format(error=e),
                metadata={'channel': msg.channel.name, 'command': 'remove'},
            )

    async def handle_archive(self, msg):
        if not (msg.author.is_mod or msg.author.is_broadcaster):
            await self._send_message(
                msg.channel,
                self.messages['archive_denied'],
                metadata={'channel': msg.channel.name, 'command': 'archive'},
            )
            return
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await self._send_message(
                msg.channel,
                self.messages['channel_not_registered'],
                metadata={'channel': msg.channel.name, 'command': 'archive'},
            )
            return
        channel = ch_row['channel_name']
        try:
            await backend.archive_stream(channel)
            await self.process_backend_update(channel)
            await self._send_message(
                msg.channel,
                self.messages['archive_success'],
                metadata={'channel': msg.channel.name, 'command': 'archive'},
            )
        except Exception as e:
            await push_console_event(
                'error',
                f'Failed to archive queue for {msg.author.name}: {e}',
                metadata={'channel': msg.channel.name, 'command': 'archive'},
            )
            await self._send_message(
                msg.channel,
                self.messages['failed'].format(error=e),
                metadata={'channel': msg.channel.name, 'command': 'archive'},
            )

    async def listen_backend(self, ch_name: str):
        url = f"{backend.base}/channels/{ch_name}/queue/stream"
        while True:
            try:
                if backend.session is None:
                    await backend.start()
                async with backend.session.get(url) as resp:
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
                    msg = self.messages['played_next'].format(
                        artist=song.get('artist', '?'),
                        title=song.get('title', '?'),
                        user=user.get('username', '?'),
                        next_artist=next_song.get('artist', '?'),
                        next_title=next_song.get('title', '?'),
                        next_user=next_user.get('username', '?'),
                    )
                else:
                    msg = self.messages['played_last'].format(
                        artist=song.get('artist', '?'),
                        title=song.get('title', '?'),
                        user=user.get('username', '?'),
                        channel=ch_name,
                    )
                await self._send_message(
                    chan,
                    msg,
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
                await self._send_message(
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
        elif etype not in ('follow', 'raid'):
            return
        word = (
            f"this {self.currency_singular}"
            if delta == 1
            else f"these {delta} {self.currency_plural}"
        )
        template = self.messages.get(f"award_{etype}")
        if template:
            await self._send_message(
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
