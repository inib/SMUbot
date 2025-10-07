from __future__ import annotations
import os, re, asyncio, json, yaml
from typing import Optional, Dict, List, Tuple, Callable, Awaitable
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

import aiohttp
from twitchio.ext import commands

# ---- Env ----
# Full URL of the backend API, defaulting to the docker-compose service name.
BACKEND_URL = os.getenv('BACKEND_URL', 'http://api:7070')
# Token used for privileged requests to the backend.
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', 'change-me')
ENV_BOT_TOKEN = os.getenv('TWITCH_BOT_TOKEN')  # token without 'oauth:'
ENV_BOT_NICK = os.getenv('BOT_NICK')
MESSAGES_PATH = Path(os.getenv("BOT_MESSAGES_PATH", "/bot/messages.yml"))

COMMANDS_FILE = os.getenv('COMMANDS_FILE', '/bot/commands.yml')
DEFAULT_COMMANDS = {
    'prefix': '!',
    'request': ['request', 'req', 'r'],
    'prioritize': ['prioritize', 'prio', 'bump'],
    'points': ['points', 'pp'],
    'remove': ['remove', 'undo', 'del'],
    'archive': ['archive'],
}

DEFAULT_MESSAGES = {
    'currency_singular': 'point',
    'currency_plural': 'points',
    'channel_not_registered': 'Channel not registered in backend',
    'request_added': 'Added: {artist} - {title}',
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
}

YOUTUBE_PATTERNS = [
    re.compile(r"https?://(www\.)?youtube\.com/watch\?v=([\w-]{11})", re.I),
    re.compile(r"https?://(music\.)?youtube\.com/watch\?v=([\w-]{11})", re.I),
    re.compile(r"https?://youtu\.be/([\w-]{11})", re.I),
]

# ---- Backend client ----
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
            if r.status >= 400:
                raise RuntimeError(f"{method} {path} -> {r.status}: {await r.text()}")
            if r.headers.get('content-type','').startswith('application/json'):
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

backend = Backend(BACKEND_URL, ADMIN_TOKEN)


@dataclass
class BotSettings:
    token: Optional[str]
    refresh_token: Optional[str]
    login: Optional[str]
    enabled: bool


def _format_token(token: str) -> str:
    return token if token.startswith('oauth:') else f"oauth:{token}"


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
    def __init__(self, *, token: str, nick: str, enabled: bool = True):
        if not token or not nick:
            raise RuntimeError('token and nick required')
        self.commands_map = load_commands(COMMANDS_FILE)
        self.messages = load_messages(MESSAGES_PATH)
        self.currency_singular = self.messages.get('currency_singular', 'point')
        self.currency_plural = self.messages.get('currency_plural', 'points')
        prefix = self.commands_map['prefix'][0]
        super().__init__(token=token, prefix=prefix, initial_channels=[])
        self.channel_map: Dict[str, Dict] = {}
        self.ready_event = asyncio.Event()
        self.state: Dict[str, Dict] = {}
        self.listeners: Dict[str, asyncio.Task] = {}
        self.joined: set[str] = set()
        self._sync_lock = asyncio.Lock()
        self.enabled = enabled
        self.nick = nick

    async def event_ready(self):
        if self.enabled:
            await self.sync_channels()
        asyncio.create_task(self.channel_refresher())
        self.ready_event.set()

    async def channel_refresher(self):
        while True:
            await asyncio.sleep(60)
            if self.enabled:
                await self.sync_channels()

    async def sync_channels(self):
        if not self.enabled:
            await self._disable_all_channels()
            return
        async with self._sync_lock:
            rows = await backend.get_channels()
            allowed = {
                r['channel_name'].lower(): r
                for r in rows
                if r.get('authorized') and r.get('join_active')
            }

            current_keys = set(self.channel_map.keys())
            allowed_keys = set(allowed.keys())

            removed = current_keys - allowed_keys
        for key in removed:
            info = self.channel_map.pop(key)
            name = info['channel_name']
            task = self.listeners.pop(name, None)
            if task:
                task.cancel()
            if name in self.joined:
                try:
                    await self.part_channels([name])
                except Exception:
                    pass
                self.joined.discard(name)
                await push_console_event(
                    'info',
                    f'Parted channel {name}',
                    event='part',
                    metadata={'channel': name},
                )
            self.state.pop(name, None)

            for key, row in allowed.items():
                self.channel_map[key] = row

            new_keys = allowed_keys - current_keys
            for key in new_keys:
                row = allowed[key]
                name = row['channel_name']
                try:
                    await self.join_channels([name])
                except Exception:
                    continue
                self.joined.add(name)
                await push_console_event(
                    'info',
                    f'Joined channel {name}',
                    event='join',
                    metadata={'channel': name},
                )
                initial_queue = await backend.get_queue(name, include_played=True)
                self.state[name] = {
                    'queue': initial_queue,
                    'last_event': datetime.utcnow().isoformat(),
                }
                self.listeners[name] = asyncio.create_task(self.listen_backend(name))

            # Ensure listeners exist for channels that persisted through refresh.
            for key, row in self.channel_map.items():
                name = row['channel_name']
                if name not in self.listeners:
                    self.listeners[name] = asyncio.create_task(self.listen_backend(name))
                if name not in self.state:
                    initial_queue = await backend.get_queue(name, include_played=True)
                    self.state[name] = {
                        'queue': initial_queue,
                        'last_event': datetime.utcnow().isoformat(),
                    }

    async def _disable_all_channels(self):
        for task in self.listeners.values():
            task.cancel()
        self.listeners.clear()
        for name in list(self.joined):
            try:
                await self.part_channels([name])
            except Exception:
                pass
            await push_console_event(
                'info',
                f'Parted channel {name}',
                event='part',
                metadata={'channel': name},
            )
        self.joined.clear()
        self.state.clear()
        self.channel_map.clear()

    async def _send_message(self, channel, message: str, *, metadata: Optional[Dict[str, object]] = None):
        await channel.send(message)
        await push_console_event(
            'info',
            f'Sent message to {channel.name}',
            event='message',
            metadata={**(metadata or {}), 'sent_text': message, 'channel': channel.name},
        )

    async def update_enabled(self, enabled: bool):
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
        if not settings.token or not settings.login:
            if self._credentials_available is not False:
                await push_console_event(
                    'error',
                    'Bot credentials are unavailable; idling worker',
                    event='startup',
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
        requires_restart = (
            self._bot is None
            or token != self._current_token
            or settings.login != self._current_login
        )
        if requires_restart:
            await self._restart_bot(token=token, login=settings.login, enabled=settings.enabled)
        elif self._bot:
            await self._bot.update_enabled(settings.enabled)

    async def _restart_bot(self, *, token: str, login: str, enabled: bool):
        await self._stop_bot(reason='restarting')
        await push_console_event(
            'info',
            f'Connecting bot as {login}',
            event='lifecycle',
        )
        bot = self.bot_factory(token=token, nick=login, enabled=enabled)
        self._bot = bot
        self._current_token = token
        self._current_login = login
        self._bot_task = self._create_task(bot.start())

    async def _stop_bot(self, *, reason: Optional[str] = None):
        if not self._bot:
            return
        try:
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
        if reason:
            await push_console_event(
                'info',
                f'Bot stopped ({reason})',
                event='lifecycle',
            )

    def _settings_from_config(self, data: Dict[str, object]) -> BotSettings:
        config = data or {}
        if isinstance(config, dict):
            token = config.get('access_token') or config.get('token')
            refresh = config.get('refresh_token')
            login = config.get('login') or config.get('bot_login')
        else:
            token = None
            refresh = None
            login = None
        has_stored_login = bool(login)
        enabled = bool(config.get('enabled')) if has_stored_login else False
        if not token or not login:
            token = ENV_BOT_TOKEN
            login = ENV_BOT_NICK
            if token and login:
                enabled = True
        return BotSettings(token=token, refresh_token=refresh, login=login, enabled=enabled)

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
