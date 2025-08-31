from __future__ import annotations
import os, re, asyncio, json, yaml
from typing import Optional, Dict, List, Tuple
from pathlib import Path
from datetime import datetime

import aiohttp
from twitchio.ext import commands

# ---- Env ----
BACKEND_URL = os.getenv('BACKEND_BASE_URL', 'http://api:8000')
ADMIN_TOKEN = os.getenv('BACKEND_ADMIN_TOKEN', 'change-me')
CHANNELS = [c.strip() for c in os.getenv('CHANNELS', '').split(',') if c.strip()]
BOT_TOKEN = os.getenv('TWITCH_BOT_TOKEN')  # token without 'oauth:'
BOT_NICK = os.getenv('BOT_NICK')
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

    async def find_or_create_user(self, channel_pk: int, twitch_id: str, username: str) -> int:
        users = await self._req('GET', f"/channels/{channel_pk}/users?search={username}")
        for u in users:
            if u['twitch_id'] == twitch_id:
                return u['id']
        resp = await self._req('POST', f"/channels/{channel_pk}/users", { 'twitch_id': twitch_id, 'username': username })
        return resp['id']

    async def search_song(self, channel_pk: int, query: str) -> Optional[dict]:
        songs = await self._req('GET', f"/channels/{channel_pk}/songs?search={query}")
        return songs[0] if songs else None

    async def song_by_link(self, channel_pk: int, link: str) -> Optional[dict]:
        songs = await self._req('GET', f"/channels/{channel_pk}/songs?search={link}")
        for s in songs:
            if s.get('youtube_link') == link:
                return s
        return None

    async def add_song(self, channel_pk: int, artist: str, title: str, link: Optional[str]) -> int:
        resp = await self._req('POST', f"/channels/{channel_pk}/songs", {
            'artist': artist, 'title': title, 'youtube_link': link
        })
        return resp['id']

    async def add_request(self, channel_id: int, song_id: int, user_id: int,
                          want_priority: bool, prefer_sub_free: bool, is_subscriber: bool):
        return await self._req('POST', f"/channels/{channel_id}/queue", {
            'song_id': song_id, 'user_id': user_id,
            'want_priority': want_priority,
            'prefer_sub_free': prefer_sub_free,
            'is_subscriber': is_subscriber,
        })

    async def get_queue(self, channel_pk: int, include_played: bool = False):
        path = f"/channels/{channel_pk}/queue"
        if include_played:
            path += "?include_played=1"
        return await self._req('GET', path)

    async def delete_request(self, channel_pk: int, request_id: int):
        return await self._req('DELETE', f"/channels/{channel_pk}/queue/{request_id}")

    async def archive_stream(self, channel_pk: int):
        return await self._req('POST', f"/channels/{channel_pk}/streams/archive")

    async def get_user(self, channel_pk: int, user_id: int):
        return await self._req('GET', f"/channels/{channel_pk}/users/{user_id}")

    async def get_song(self, channel_pk: int, song_id: int):
        return await self._req('GET', f"/channels/{channel_pk}/songs/{song_id}")

    async def get_events(self, channel_pk: int, since: Optional[str] = None):
        path = f"/channels/{channel_pk}/events"
        if since:
            path += f"?since={since}"
        return await self._req('GET', path)

backend = Backend(BACKEND_URL, ADMIN_TOKEN)

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
    def __init__(self):
        if not BOT_TOKEN or not BOT_NICK:
            raise RuntimeError('TWITCH_BOT_TOKEN and BOT_NICK required')
        self.commands_map = load_commands(COMMANDS_FILE)
        self.messages = load_messages(MESSAGES_PATH)
        self.currency_singular = self.messages.get('currency_singular', 'point')
        self.currency_plural = self.messages.get('currency_plural', 'points')
        prefix = self.commands_map['prefix'][0]
        super().__init__(token=f"oauth:{BOT_TOKEN}", prefix=prefix, initial_channels=CHANNELS or [])
        self.channel_map: Dict[str, Dict] = {}
        self.ready_event = asyncio.Event()
        self.state: Dict[int, Dict] = {}

    async def event_ready(self):
        rows = await backend.get_channels()
        self.channel_map = { r['channel_name'].lower(): r for r in rows }
        for _, row in self.channel_map.items():
            pk = row['id']
            initial_queue = await backend.get_queue(pk, include_played=True)
            self.state[pk] = {
                'queue': initial_queue,
                'last_event': datetime.utcnow().isoformat(),
            }
            asyncio.create_task(self.listen_backend(row['channel_name'], pk))
        self.ready_event.set()

    async def event_message(self, message):
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
            await msg.channel.send(self.messages['channel_not_registered'])
            return
        channel_pk = ch_row['id']
        user_id = await backend.find_or_create_user(channel_pk, str(msg.author.id), msg.author.name)

        # detect YouTube link
        ylink = extract_youtube_url(arg)
        song = None
        if ylink:
            song = await backend.song_by_link(channel_pk, ylink)
            if not song:
                if backend.session is None:
                    await backend.start()
                title_text = await fetch_youtube_oembed_title(backend.session, ylink)
                artist, title = parse_artist_title(title_text) if title_text else ("YouTube", ylink)
                song_id = await backend.add_song(channel_pk, artist, title, ylink)
                song = { 'id': song_id, 'artist': artist, 'title': title, 'youtube_link': ylink }
        else:
            artist, title = parse_artist_title(arg)
            found = await backend.search_song(channel_pk, f"{artist} - {title}")
            if not found:
                song_id = await backend.add_song(channel_pk, artist, title, None)
                song = { 'id': song_id, 'artist': artist, 'title': title }
            else:
                song = found

        try:
            # initial requests are non-priority
            await backend.add_request(
                channel_pk,
                song['id'],
                user_id,
                want_priority=False,
                prefer_sub_free=True,
                is_subscriber=msg.author.is_subscriber,
            )
            await msg.channel.send(
                self.messages['request_added'].format(
                    artist=song.get('artist', ''),
                    title=song.get('title', ''),
                )
            )
        except Exception as e:
            await msg.channel.send(self.messages['failed'].format(error=e))

    async def handle_prioritize(self, msg, arg: str):
        # user can prioritize up to 3 songs per stream
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await msg.channel.send(self.messages['channel_not_registered'])
            return
        channel_pk = ch_row['id']
        user_id = await backend.find_or_create_user(channel_pk, str(msg.author.id), msg.author.name)

        queue = await backend.get_queue(channel_pk)
        my_prio = [q for q in queue if q['user_id'] == user_id and q['is_priority'] == 1]
        if len(my_prio) >= 3:
            await msg.channel.send(self.messages['prioritize_limit'])
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
            await msg.channel.send(self.messages['prioritize_no_target'])
            return

        try:
            # re-add as priority, then delete old
            await backend.add_request(
                channel_pk,
                target['song_id'],
                user_id,
                want_priority=True,
                prefer_sub_free=True,
                is_subscriber=msg.author.is_subscriber,
            )
            await backend.delete_request(channel_pk, target['id'])
            await msg.channel.send(
                self.messages['prioritize_success'].format(request_id=target['id'])
            )
        except Exception as e:
            await msg.channel.send(self.messages['failed'].format(error=e))

    async def handle_points(self, msg):
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await msg.channel.send(self.messages['channel_not_registered'])
            return
        channel_pk = ch_row['id']
        user_id = await backend.find_or_create_user(channel_pk, str(msg.author.id), msg.author.name)
        u = await backend.get_user(channel_pk, user_id)
        await msg.channel.send(
            self.messages['points'].format(
                username=msg.author.name,
                points=u.get('prio_points', 0),
                currency_plural=self.currency_plural,
            )
        )

    async def handle_remove(self, msg):
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await msg.channel.send(self.messages['channel_not_registered'])
            return
        channel_pk = ch_row['id']
        user_id = await backend.find_or_create_user(channel_pk, str(msg.author.id), msg.author.name)
        queue = await backend.get_queue(channel_pk)
        mine = [q for q in queue if q['user_id'] == user_id and q['played'] == 0]
        if not mine:
            await msg.channel.send(self.messages['remove_no_pending'])
            return
        latest = mine[-1]
        try:
            await backend.delete_request(channel_pk, latest['id'])
            await msg.channel.send(
                self.messages['remove_success'].format(request_id=latest['id'])
            )
        except Exception as e:
            await msg.channel.send(self.messages['failed'].format(error=e))

    async def handle_archive(self, msg):
        if not (msg.author.is_mod or msg.author.is_broadcaster):
            await msg.channel.send(self.messages['archive_denied'])
            return
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await msg.channel.send(self.messages['channel_not_registered'])
            return
        channel_pk = ch_row['id']
        try:
            await backend.archive_stream(channel_pk)
            await self.process_backend_update(ch_name, channel_pk)
            await msg.channel.send(self.messages['archive_success'])
        except Exception as e:
            await msg.channel.send(self.messages['failed'].format(error=e))

    async def listen_backend(self, ch_name: str, channel_pk: int):
        url = f"{backend.base}/channels/{channel_pk}/queue/stream"
        while True:
            try:
                if backend.session is None:
                    await backend.start()
                async with backend.session.get(url) as resp:
                    async for line in resp.content:
                        line = line.decode().strip()
                        if line.startswith("data:"):
                            await self.process_backend_update(ch_name, channel_pk)
            except Exception:
                await asyncio.sleep(5)

    async def process_backend_update(self, ch_name: str, channel_pk: int):
        state = self.state.get(channel_pk, {})
        prev_queue = state.get('queue', [])
        last_event = state.get('last_event')
        new_queue = await backend.get_queue(channel_pk, include_played=True)

        await self.check_played(ch_name, channel_pk, prev_queue, new_queue)
        await self.check_bumps(ch_name, channel_pk, prev_queue, new_queue)

        events = await backend.get_events(channel_pk, since=last_event) if last_event else await backend.get_events(channel_pk)
        if events:
            for ev in reversed(events):
                ev_time = ev['event_time']
                if last_event and ev_time <= last_event:
                    continue
                await self.announce_event(ch_name, channel_pk, ev)
            state['last_event'] = max(ev['event_time'] for ev in events)
        state['queue'] = new_queue

    async def check_played(self, ch_name: str, channel_pk: int, prev_queue: List[dict], new_queue: List[dict]):
        prev_map = {q['id']: q for q in prev_queue}
        chan = self.get_channel(ch_name.lower())
        if not chan:
            return
        for req in new_queue:
            old = prev_map.get(req['id'])
            if old and old['played'] == 0 and req['played'] == 1:
                song = await backend.get_song(channel_pk, req['song_id'])
                user = await backend.get_user(channel_pk, req['user_id'])
                pending_prio = [q for q in new_queue if q['played'] == 0 and q['is_priority'] == 1]
                if pending_prio:
                    next_req = pending_prio[0]
                    next_song = await backend.get_song(channel_pk, next_req['song_id'])
                    next_user = await backend.get_user(channel_pk, next_req['user_id'])
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
                await chan.send(msg)

    async def check_bumps(self, ch_name: str, channel_pk: int, prev_queue: List[dict], new_queue: List[dict]):
        prev_map = {q['id']: q for q in prev_queue}
        chan = self.get_channel(ch_name.lower())
        if not chan:
            return
        for req in new_queue:
            old = prev_map.get(req['id'])
            new_prio = req['is_priority'] == 1 and req.get('priority_source') == 'admin'
            was_prio = old and old['is_priority'] == 1 if old else False
            if new_prio and not was_prio:
                song = await backend.get_song(channel_pk, req['song_id'])
                user = await backend.get_user(channel_pk, req['user_id'])
                await chan.send(
                    self.messages['bump_free'].format(
                        artist=song.get('artist', '?'),
                        title=song.get('title', '?'),
                        user=user.get('username', '?'),
                    )
                )

    async def announce_event(self, ch_name: str, channel_pk: int, ev: dict):
        chan = self.get_channel(ch_name.lower())
        if not chan:
            return
        user = None
        if ev.get('user_id'):
            user = await backend.get_user(channel_pk, ev['user_id'])
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
            await chan.send(
                template.format(
                    username=user.get('username', ''),
                    word=word,
                    points=user.get('prio_points', 0),
                    currency_plural=self.currency_plural,
                    **extra,
                )
            )

# ---- entry ----
async def main():
    await backend.start()
    bot = SongBot()
    await asyncio.gather(bot.start())

if __name__ == '__main__':
    asyncio.run(main())
