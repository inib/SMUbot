from __future__ import annotations
import os, re, asyncio, json, yaml
from typing import Optional, Dict, List, Tuple
from pathlib import Path

import aiohttp
from twitchio.ext import commands

# ---- Env ----
BACKEND_URL = os.getenv('BACKEND_BASE_URL', 'http://api:8000')
ADMIN_TOKEN = os.getenv('BACKEND_ADMIN_TOKEN', 'change-me')
CHANNELS = [c.strip() for c in os.getenv('CHANNELS', '').split(',') if c.strip()]
BOT_TOKEN = os.getenv('TWITCH_BOT_TOKEN')  # token without 'oauth:'
BOT_NICK = os.getenv('BOT_NICK')
MESSAGES_PATH = Path(os.getenv("BOT_MESSAGES_PATH", "/bot/messages.yml"))

COMMANDS_FILE = os.getenv('COMMANDS_FILE', '/bot/commands.txt')
DEFAULT_COMMANDS = {
    'prefix': '!',
    'request': 'request,req,r',
    'prioritize': 'prioritize,prio,bump',
    'points': 'points,pp',
    'remove': 'remove,undo,del'
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

    async def get_queue(self, channel_pk: int):
        return await self._req('GET', f"/channels/{channel_pk}/queue")

    async def delete_request(self, channel_pk: int, request_id: int):
        return await self._req('DELETE', f"/channels/{channel_pk}/queue/{request_id}")

    async def get_user(self, channel_pk: int, user_id: int):
        return await self._req('GET', f"/channels/{channel_pk}/users/{user_id}")

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
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    cfg[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    mapped = { k: [x.strip() for x in v.split(',')] if k != 'prefix' else [v] for k, v in cfg.items() }
    return mapped

# ---- bot ----
class SongBot(commands.Bot):
    def __init__(self):
        if not BOT_TOKEN or not BOT_NICK:
            raise RuntimeError('TWITCH_BOT_TOKEN and BOT_NICK required')
        self.commands_map = load_commands(COMMANDS_FILE)
        prefix = self.commands_map['prefix'][0]
        super().__init__(token=f"oauth:{BOT_TOKEN}", prefix=prefix, initial_channels=CHANNELS or [])
        self.channel_map = {}
        self.ready_event = asyncio.Event()

    async def event_ready(self):
        rows = await backend.get_channels()
        self.channel_map = { r['channel_name'].lower(): r for r in rows }
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

    async def handle_request(self, msg, arg: str):
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await msg.channel.send('Channel not registered in backend')
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
            await backend.add_request(channel_pk, song['id'], user_id, want_priority=False, prefer_sub_free=True, is_subscriber=msg.author.is_subscriber)
            await msg.channel.send(f"Added: {song.get('artist','')} - {song.get('title','')}")
        except Exception as e:
            await msg.channel.send(f"Failed: {e}")

    async def handle_prioritize(self, msg, arg: str):
        # user can prioritize up to 3 songs per stream
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await msg.channel.send('Channel not registered in backend')
            return
        channel_pk = ch_row['id']
        user_id = await backend.find_or_create_user(channel_pk, str(msg.author.id), msg.author.name)

        queue = await backend.get_queue(channel_pk)
        my_prio = [q for q in queue if q['user_id'] == user_id and q['is_priority'] == 1]
        if len(my_prio) >= 3:
            await msg.channel.send('Limit reached: 3 prioritized songs per stream')
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
            await msg.channel.send('No eligible request to prioritize')
            return

        try:
            # re-add as priority, then delete old
            await backend.add_request(channel_pk, target['song_id'], user_id, want_priority=True, prefer_sub_free=True, is_subscriber=msg.author.is_subscriber)
            await backend.delete_request(channel_pk, target['id'])
            await msg.channel.send(f"Prioritized request #{target['id']}")
        except Exception as e:
            await msg.channel.send(f"Failed: {e}")

    async def handle_points(self, msg):
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await msg.channel.send('Channel not registered in backend')
            return
        channel_pk = ch_row['id']
        user_id = await backend.find_or_create_user(channel_pk, str(msg.author.id), msg.author.name)
        u = await backend.get_user(channel_pk, user_id)
        await msg.channel.send(f"{msg.author.name}, points: {u.get('prio_points', 0)}")

    async def handle_remove(self, msg):
        ch_name = msg.channel.name.lower()
        ch_row = self.channel_map.get(ch_name)
        if not ch_row:
            await msg.channel.send('Channel not registered in backend')
            return
        channel_pk = ch_row['id']
        user_id = await backend.find_or_create_user(channel_pk, str(msg.author.id), msg.author.name)
        queue = await backend.get_queue(channel_pk)
        mine = [q for q in queue if q['user_id'] == user_id and q['played'] == 0]
        if not mine:
            await msg.channel.send('You have no pending requests')
            return
        latest = mine[-1]
        try:
            await backend.delete_request(channel_pk, latest['id'])
            await msg.channel.send(f"Removed your latest request #{latest['id']}")
        except Exception as e:
            await msg.channel.send(f"Failed: {e}")

# ---- entry ----
async def main():
    await backend.start()
    bot = SongBot()
    await asyncio.gather(bot.start())

if __name__ == '__main__':
    asyncio.run(main())