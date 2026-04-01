from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol, Set, Tuple

from command_resolution import ROUTED_COMMANDS, resolve_prefixed_command


@dataclass(frozen=True)
class NormalizedChatInput:
    """Canonical chat command input independent of TwitchIO payload classes.

    Dependencies: populated by transport adapters (TwitchIO or tests) before
    command parsing. Code customers: `parse_chat_command`,
    `dispatch_chat_command`, and `SongBot` adapter methods. Used
    variables/origin: fields map directly from inbound chat event payloads.
    """

    channel_login: str
    user_id: str
    username: str
    text: str
    is_subscriber: bool
    is_moderator: bool
    is_broadcaster: bool
    message_id: Optional[str] = None
    message_timestamp: Optional[datetime] = None


@dataclass(frozen=True)
class ParsedChatCommand:
    """Structured command token parsed from a normalized chat message.

    Dependencies: command aliases from command config. Code customers:
    `dispatch_chat_command` and command-specific adapter wrappers. Used
    variables/origin: `canonical` is an internal command key and `args` is raw
    trailing argument text from `NormalizedChatInput.text`.
    """

    canonical: str
    alias: str
    args: str


class ChatBackendClient(Protocol):
    """Protocol of backend calls used by shared command execution logic."""

    async def find_or_create_user(self, channel: str, twitch_id: str, username: str) -> int: ...
    async def song_by_link(self, channel: str, link: str) -> Optional[dict]: ...
    async def add_song(self, channel: str, artist: str, title: str, link: Optional[str]) -> int: ...
    async def search_song(self, channel: str, query: str) -> Optional[dict]: ...
    async def add_request(self, channel: str, song_id: int, user_id: int, want_priority: bool, prefer_sub_free: bool, is_subscriber: bool, is_mod: bool) -> dict: ...
    async def random_playlist_request(self, channel: str, *, keyword: Optional[str], twitch_id: str, username: str, is_subscriber: bool) -> Dict[str, object]: ...
    async def list_playlists(self, channel: str) -> List[dict]: ...
    async def playlist_request(self, channel: str, *, identifier: str, index: int) -> Dict[str, object]: ...
    async def get_queue(self, channel: str, include_played: bool = False) -> List[dict]: ...
    async def delete_request(self, channel: str, request_id: int) -> object: ...
    async def get_user(self, channel: str, user_id: int) -> dict: ...


SendReplyFn = Callable[..., Awaitable[None]]
LogErrorFn = Callable[[str, Dict[str, object]], Awaitable[None]]
YouTubeTitleFn = Callable[[str], Awaitable[Optional[str]]]


@dataclass
class ChatCommandContext:
    """Runtime adapter surface injected into reusable command functions.

    Dependencies: backend adapter, message templates, and side-effect callbacks
    supplied by `SongBot`. Code customers: `dispatch_chat_command` and
    `execute_*` handlers. Used variables/origin: values are copied from bot
    instance state (`channel_map`, `messages`, command config, and helpers).
    """

    backend: ChatBackendClient
    messages: Dict[str, str]
    commands_map: Dict[str, List[str]]
    currency_plural: str
    channel_map: Dict[str, Dict[str, Any]]
    extract_youtube_url: Callable[[str], Optional[str]]
    parse_artist_title: Callable[[str], Tuple[str, str]]
    fetch_youtube_title: YouTubeTitleFn
    send_reply: SendReplyFn
    log_error: LogErrorFn
    backend_error_cls: type[Exception]


def parse_chat_command(chat_input: NormalizedChatInput, commands_map: Dict[str, List[str]]) -> Optional[ParsedChatCommand]:
    """Parse a canonical command from normalized chat input text.

    Dependencies: shared command resolver (`resolve_prefixed_command`) and
    aliases configured in `commands_map`. Code customers: Twitch/WebSocket
    event adapters and tests. Used variables/origin: `chat_input.text` carries
    the raw chat message body.
    """

    parsed = resolve_prefixed_command(chat_input.text, commands_map, routed_commands=ROUTED_COMMANDS)
    if parsed.get('parse_reason') != 'ok' or not parsed.get('canonical') or not parsed.get('alias'):
        return None
    return ParsedChatCommand(
        canonical=str(parsed['canonical']),
        alias=str(parsed['alias']),
        args=str(parsed.get('args') or ''),
    )


async def dispatch_chat_command(chat_input: NormalizedChatInput, parsed: ParsedChatCommand, ctx: ChatCommandContext) -> bool:
    """Dispatch a parsed command to backend-backed execution handlers.

    Dependencies: `ChatCommandContext` adapters for backend IO, chat replies,
    and logging. Code customers: `SongBot.event_message` and compatibility
    wrappers in `SongBot.handle_*`. Used variables/origin: command metadata
    comes from `parsed`, message identity fields come from `chat_input`.
    """

    handlers = {
        'request': execute_request,
        'random_request': execute_random_request,
        'playlist_request': execute_playlist_request,
        'prioritize': execute_prioritize,
        'points': execute_points,
        'remove': execute_remove,
    }
    handler = handlers.get(parsed.canonical)
    if not handler:
        return False
    await handler(chat_input, parsed.args, ctx)
    return True


async def _resolve_channel(chat_input: NormalizedChatInput, command: str, ctx: ChatCommandContext) -> Optional[Tuple[str, str]]:
    """Resolve backend channel metadata or emit a standardized missing-channel reply.

    Dependencies: `ctx.channel_map` and chat reply adapter. Code customers:
    all command execution helpers in this module. Used variables/origin:
    channel lookup key uses `chat_input.channel_login`.
    """

    row = ctx.channel_map.get(chat_input.channel_login)
    if not row:
        await ctx.send_reply(
            chat_input.channel_login,
            ctx.messages['channel_not_registered'],
            command=command,
            channel=chat_input.channel_login,
            reply_to=chat_input.message_id,
        )
        return None
    return chat_input.channel_login, row['channel_name']


async def execute_request(chat_input: NormalizedChatInput, arg: str, ctx: ChatCommandContext) -> None:
    """Execute `!request` against backend queue APIs using normalized input.

    Dependencies: song search/create endpoints, request enqueue endpoint, and
    YouTube title resolver callback. Code customers: dispatcher and request
    compatibility wrapper in `SongBot`. Used variables/origin: requester
    identity comes from `chat_input.user_id`/`chat_input.username`.
    """

    channel_info = await _resolve_channel(chat_input, 'request', ctx)
    if not channel_info:
        return
    login, channel = channel_info
    user_id = await ctx.backend.find_or_create_user(channel, str(chat_input.user_id), chat_input.username)

    ylink = ctx.extract_youtube_url(arg)
    song = None
    if ylink:
        song = await ctx.backend.song_by_link(channel, ylink)
        if not song:
            title_text = await ctx.fetch_youtube_title(ylink)
            artist, title = ctx.parse_artist_title(title_text) if title_text else ("YouTube", ylink)
            song_id = await ctx.backend.add_song(channel, artist, title, ylink)
            song = {'id': song_id, 'artist': artist, 'title': title, 'youtube_link': ylink}
    else:
        artist, title = ctx.parse_artist_title(arg)
        found = await ctx.backend.search_song(channel, f"{artist} - {title}")
        if not found:
            song_id = await ctx.backend.add_song(channel, artist, title, None)
            song = {'id': song_id, 'artist': artist, 'title': title}
        else:
            song = found

    try:
        await ctx.backend.add_request(
            channel,
            song['id'],
            user_id,
            want_priority=False,
            prefer_sub_free=True,
            is_subscriber=bool(chat_input.is_subscriber),
            is_mod=bool(chat_input.is_moderator or chat_input.is_broadcaster),
        )
        await ctx.send_reply(
            login,
            ctx.messages['request_added'].format(artist=song.get('artist', ''), title=song.get('title', '')),
            command='request',
            channel=channel,
            reply_to=chat_input.message_id,
        )
    except Exception as exc:
        await ctx.log_error(f'Failed to add request for {chat_input.username}: {exc}', {'channel': channel, 'command': 'request'})
        await ctx.send_reply(
            login,
            ctx.messages['failed'].format(error=exc),
            command='request',
            channel=channel,
            reply_to=chat_input.message_id,
        )


async def execute_random_request(chat_input: NormalizedChatInput, arg: str, ctx: ChatCommandContext) -> None:
    """Execute `!random` playlist request command using normalized chat data.

    Dependencies: random playlist backend endpoint and backend error contract.
    Code customers: dispatcher and random-request compatibility wrapper.
    Used variables/origin: keyword comes from command arg text.
    """

    channel_info = await _resolve_channel(chat_input, 'random_request', ctx)
    if not channel_info:
        return
    login, channel = channel_info
    keyword = (arg or '').strip()
    try:
        response = await ctx.backend.random_playlist_request(
            channel,
            keyword=keyword or None,
            twitch_id=str(chat_input.user_id),
            username=chat_input.username,
            is_subscriber=bool(chat_input.is_subscriber),
        )
    except ctx.backend_error_cls as exc:  # type: ignore[misc]
        if getattr(exc, 'status', None) == 404:
            template = ctx.messages.get('random_not_found', 'No playlist found for "{keyword}"')
            await ctx.send_reply(
                login,
                template.format(keyword=keyword or 'default'),
                command='random_request',
                channel=channel,
                reply_to=chat_input.message_id,
            )
            return
        detail = getattr(exc, 'detail', str(exc))
        await ctx.log_error(
            f'Failed random request for {chat_input.username}: {detail}',
            {'channel': channel, 'command': 'random_request', 'status': getattr(exc, 'status', None), 'keyword': keyword},
        )
        await ctx.send_reply(
            login,
            ctx.messages['failed'].format(error=detail),
            command='random_request',
            channel=channel,
            reply_to=chat_input.message_id,
        )
        return
    except Exception as exc:
        await ctx.log_error(f'Failed random request for {chat_input.username}: {exc}', {'channel': channel, 'command': 'random_request'})
        await ctx.send_reply(
            login,
            ctx.messages['failed'].format(error=exc),
            command='random_request',
            channel=channel,
            reply_to=chat_input.message_id,
        )
        return

    song_payload = response.get('song') if isinstance(response, dict) else None
    artist = song_payload.get('artist', '') if isinstance(song_payload, dict) else ''
    title = song_payload.get('title', '') if isinstance(song_payload, dict) else ''
    resolved_keyword = response.get('keyword') if isinstance(response, dict) else ''
    template = ctx.messages.get('random_request_added') or ctx.messages.get('request_added')
    if template:
        try:
            message_text = template.format(artist=artist, title=title, keyword=resolved_keyword or keyword or '')
        except KeyError:
            message_text = template
        await ctx.send_reply(
            login,
            message_text,
            command='random_request',
            channel=channel,
            keyword=resolved_keyword or keyword,
            reply_to=chat_input.message_id,
        )


async def execute_playlist_request(chat_input: NormalizedChatInput, arg: str, ctx: ChatCommandContext) -> None:
    """Execute `!playlist` by playlist lookup and indexed backend enqueue.

    Dependencies: playlist list/queue endpoints and backend error details for
    playlist/index failures. Code customers: dispatcher and playlist wrapper.
    Used variables/origin: playlist name/index parse from command argument.
    """

    channel_info = await _resolve_channel(chat_input, 'playlist_request', ctx)
    if not channel_info:
        return
    login, channel = channel_info
    arg = (arg or '').strip()
    usage = ctx.messages.get('playlist_usage', 'Usage: !playlist <name> <index>')
    if not arg:
        await ctx.send_reply(login, usage, command='playlist_request', channel=channel, reply_to=chat_input.message_id)
        return
    name_part, sep, index_part = arg.rpartition(' ')
    if not sep or not name_part.strip() or not index_part.strip():
        await ctx.send_reply(login, usage, command='playlist_request', channel=channel, reply_to=chat_input.message_id)
        return
    playlist_name = name_part.strip()
    try:
        index = int(index_part)
    except ValueError:
        await ctx.send_reply(login, usage, command='playlist_request', channel=channel, reply_to=chat_input.message_id)
        return
    if index < 1:
        await ctx.send_reply(login, usage, command='playlist_request', channel=channel, reply_to=chat_input.message_id)
        return

    try:
        playlists = await ctx.backend.list_playlists(channel)
    except Exception as exc:
        await ctx.log_error(f'Failed to list playlists for {channel}: {exc}', {'channel': channel, 'command': 'playlist_request'})
        await ctx.send_reply(login, ctx.messages['failed'].format(error=exc), command='playlist_request', channel=channel, reply_to=chat_input.message_id)
        return

    match = None
    playlist_lookup = playlist_name.lower()
    for entry in playlists or []:
        title = str(entry.get('title', '')).strip()
        if title and title.lower() == playlist_lookup:
            match = entry
            break
        slug_candidates: Set[str] = set()
        identifier = entry.get('playlist_id')
        if isinstance(identifier, str) and identifier.strip():
            slug_candidates.add(identifier.strip().lower())
        entry_id = entry.get('id')
        if entry_id is not None and str(entry_id).strip():
            slug_candidates.add(str(entry_id).strip().lower())
        if playlist_lookup in slug_candidates:
            match = entry
            break
    if not match:
        template = ctx.messages.get('playlist_not_found', 'Playlist "{playlist}" not found')
        await ctx.send_reply(login, template.format(playlist=playlist_name), command='playlist_request', channel=channel, reply_to=chat_input.message_id)
        return

    identifier_value = match.get('id') if match.get('id') is not None else match.get('playlist_id') or playlist_name
    playlist_title = str(match.get('title') or playlist_name)
    try:
        response = await ctx.backend.playlist_request(channel, identifier=str(identifier_value), index=index)
    except ctx.backend_error_cls as exc:  # type: ignore[misc]
        detail = str(getattr(exc, 'detail', '') or '').lower()
        if getattr(exc, 'status', None) == 404 and 'playlist' in detail:
            template = ctx.messages.get('playlist_not_found', 'Playlist "{playlist}" not found')
            await ctx.send_reply(login, template.format(playlist=playlist_title), command='playlist_request', channel=channel, reply_to=chat_input.message_id)
            return
        if getattr(exc, 'status', None) in (400, 404) and any(keyword in detail for keyword in ('index', 'item')):
            template = ctx.messages.get('playlist_song_missing', 'Playlist "{playlist}" has no song #{index}')
            await ctx.send_reply(login, template.format(playlist=playlist_title, index=index), command='playlist_request', channel=channel, reply_to=chat_input.message_id)
            return
        await ctx.log_error(
            f'Failed playlist request for {chat_input.username}: {getattr(exc, "detail", exc)}',
            {'channel': channel, 'command': 'playlist_request', 'status': getattr(exc, 'status', None), 'playlist': playlist_title, 'index': index},
        )
        await ctx.send_reply(login, ctx.messages['failed'].format(error=getattr(exc, 'detail', exc)), command='playlist_request', channel=channel, reply_to=chat_input.message_id)
        return
    except Exception as exc:
        await ctx.log_error(f'Failed playlist request for {chat_input.username}: {exc}', {'channel': channel, 'command': 'playlist_request'})
        await ctx.send_reply(login, ctx.messages['failed'].format(error=exc), command='playlist_request', channel=channel, reply_to=chat_input.message_id)
        return

    song_payload = response.get('song') if isinstance(response, dict) else None
    artist = song_payload.get('artist', '') if isinstance(song_payload, dict) else ''
    title = song_payload.get('title', '') if isinstance(song_payload, dict) else ''
    template = ctx.messages.get('playlist_request_added') or ctx.messages.get('request_added')
    if template:
        try:
            message_text = template.format(playlist=playlist_title, artist=artist, title=title, index=index)
        except KeyError:
            message_text = template
        await ctx.send_reply(
            login,
            message_text,
            command='playlist_request',
            channel=channel,
            playlist=playlist_title,
            index=index,
            reply_to=chat_input.message_id,
        )


async def execute_prioritize(chat_input: NormalizedChatInput, arg: str, ctx: ChatCommandContext) -> None:
    """Execute `!prioritize` by promoting an existing pending request.

    Dependencies: queue read/write backend methods and requester user lookup.
    Code customers: dispatcher and prioritize wrapper. Used variables/origin:
    request selection uses optional numeric arg from `arg`.
    """

    channel_info = await _resolve_channel(chat_input, 'prioritize', ctx)
    if not channel_info:
        return
    login, channel = channel_info
    user_id = await ctx.backend.find_or_create_user(channel, str(chat_input.user_id), chat_input.username)

    queue = await ctx.backend.get_queue(channel)
    my_prio = [q for q in queue if q['user_id'] == user_id and q['is_priority'] == 1]
    if len(my_prio) >= 3:
        await ctx.send_reply(login, ctx.messages['prioritize_limit'], command='prioritize', channel=channel, reply_to=chat_input.message_id)
        return

    target = None
    if arg.strip().isdigit():
        rid = int(arg.strip())
        target = next((q for q in queue if q['id'] == rid and q['user_id'] == user_id and q['played'] == 0), None)
    if not target:
        mine = [q for q in queue if q['user_id'] == user_id and q['played'] == 0 and q['is_priority'] == 0]
        target = mine[-1] if mine else None
    if not target:
        await ctx.send_reply(login, ctx.messages['prioritize_no_target'], command='prioritize', channel=channel, reply_to=chat_input.message_id)
        return

    try:
        await ctx.backend.add_request(
            channel,
            target['song_id'],
            user_id,
            want_priority=True,
            prefer_sub_free=True,
            is_subscriber=bool(chat_input.is_subscriber),
            is_mod=bool(chat_input.is_moderator or chat_input.is_broadcaster),
        )
        await ctx.backend.delete_request(channel, target['id'])
        await ctx.send_reply(
            login,
            ctx.messages['prioritize_success'].format(request_id=target['id']),
            command='prioritize',
            channel=channel,
            reply_to=chat_input.message_id,
        )
    except Exception as exc:
        await ctx.log_error(f'Failed to prioritize for {chat_input.username}: {exc}', {'channel': channel, 'command': 'prioritize'})
        await ctx.send_reply(login, ctx.messages['failed'].format(error=exc), command='prioritize', channel=channel, reply_to=chat_input.message_id)


async def execute_points(chat_input: NormalizedChatInput, _: str, ctx: ChatCommandContext) -> None:
    """Execute `!points` by fetching and announcing requester points.

    Dependencies: backend user lookup/read endpoints. Code customers:
    dispatcher and points wrapper. Used variables/origin: display username and
    user id from normalized message input.
    """

    channel_info = await _resolve_channel(chat_input, 'points', ctx)
    if not channel_info:
        return
    login, channel = channel_info
    user_id = await ctx.backend.find_or_create_user(channel, str(chat_input.user_id), chat_input.username)
    user = await ctx.backend.get_user(channel, user_id)
    await ctx.send_reply(
        login,
        ctx.messages['points'].format(username=chat_input.username, points=user.get('prio_points', 0), currency_plural=ctx.currency_plural),
        command='points',
        channel=channel,
        reply_to=chat_input.message_id,
    )


async def execute_remove(chat_input: NormalizedChatInput, _: str, ctx: ChatCommandContext) -> None:
    """Execute `!remove` by deleting the requester's newest pending queue row.

    Dependencies: backend queue read/delete methods and user lookup endpoint.
    Code customers: dispatcher and remove wrapper. Used variables/origin:
    queue ownership filters by normalized requester identity.
    """

    channel_info = await _resolve_channel(chat_input, 'remove', ctx)
    if not channel_info:
        return
    login, channel = channel_info
    user_id = await ctx.backend.find_or_create_user(channel, str(chat_input.user_id), chat_input.username)
    queue = await ctx.backend.get_queue(channel)
    mine = [q for q in queue if q['user_id'] == user_id and q['played'] == 0]
    if not mine:
        await ctx.send_reply(login, ctx.messages['remove_no_pending'], command='remove', channel=channel, reply_to=chat_input.message_id)
        return
    latest = mine[-1]
    try:
        await ctx.backend.delete_request(channel, latest['id'])
        await ctx.send_reply(
            login,
            ctx.messages['remove_success'].format(request_id=latest['id']),
            command='remove',
            channel=channel,
            reply_to=chat_input.message_id,
        )
    except Exception as exc:
        await ctx.log_error(f'Failed to remove request for {chat_input.username}: {exc}', {'channel': channel, 'command': 'remove'})
        await ctx.send_reply(login, ctx.messages['failed'].format(error=exc), command='remove', channel=channel, reply_to=chat_input.message_id)
