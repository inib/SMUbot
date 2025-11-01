# Backend API Endpoints

This document summarizes the REST endpoints exposed by `backend_app.py`.

## System
| Method | Path | Description |
|--------|------|-------------|
| GET | `/system/health` | Health check that verifies database connectivity. |

## Bot
| Method | Path | Description |
|--------|------|-------------|
| GET | `/bot/config` | Retrieve the stored bot OAuth configuration (admin). |
| PUT | `/bot/config` | Update bot settings such as scopes or enable flag (admin). |
| POST | `/bot/config/oauth` | Start the OAuth authorization flow for the bot account (admin). |
| GET | `/bot/config/oauth/callback` | Callback used by Twitch to finish the bot OAuth flow. |

## Bot Logs
| Method | Path | Description |
|--------|------|-------------|
| POST | `/bot/logs` | Push a bot worker log event which is relayed to connected consoles (admin). |
| GET | `/bot/logs/stream` | Server-sent events stream of bot worker log messages (admin). |

## Channels
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels` | List all configured channels. |
| POST | `/channels` | Add a new channel (requires admin token). |
| PUT | `/channels/{channel}` | Update whether the bot should join a channel (admin). |
| GET | `/channels/{channel}/settings` | Retrieve channel configuration. |
| PUT | `/channels/{channel}/settings` | Update channel configuration (admin). |

## Songs
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/songs` | Search songs in a channel, optionally filtering by artist or title. |
| POST | `/channels/{channel}/songs` | Add a song to the catalog (admin). |
| GET | `/channels/{channel}/songs/{song_id}` | Fetch a specific song. |
| PUT | `/channels/{channel}/songs/{song_id}` | Update song details (admin). |
| DELETE | `/channels/{channel}/songs/{song_id}` | Remove a song from the catalog (admin). |

## Users
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/users` | Search or list users in a channel. Providing an admin token returns the full list. |
| POST | `/channels/{channel}/users` | Create or update a user record (admin). |
| GET | `/channels/{channel}/users/{user_id}` | Retrieve user details. |
| PUT | `/channels/{channel}/users/{user_id}` | Update user statistics such as priority points (admin). |
| GET | `/channels/{channel}/users/{user_id}/stream_state` | Get per-stream state like free subscriber priority usage. |
| PUT | `/channels/{channel}/users/{user_id}/points` | Set a user's priority points directly (admin). |

## Queue
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/queue/stream` | Server-sent events stream emitting queue updates. |
| GET | `/channels/{channel}/queue` | Current request queue for the active stream. |
| GET | `/channels/{channel}/streams/{stream_id}/queue` | Request queue for a specific past stream. |
| POST | `/channels/{channel}/queue` | Add a song request to the queue (admin or bot). |
| PUT | `/channels/{channel}/queue/{request_id}` | Update request status such as marking played (admin). |
| DELETE | `/channels/{channel}/queue/{request_id}` | Remove a request (admin). |
| POST | `/channels/{channel}/queue/clear` | Remove all pending requests for the current stream (admin). |
| GET | `/channels/{channel}/queue/random_nonpriority` | Fetch a random non-priority request from the queue. |
| POST | `/channels/{channel}/queue/{request_id}/bump_admin` | Force a request to priority status (admin). |
| POST | `/channels/{channel}/queue/{request_id}/move` | Move a request up or down in the queue (admin). |
| POST | `/channels/{channel}/queue/{request_id}/skip` | Send a request to the end of the queue (admin). |
| POST | `/channels/{channel}/queue/{request_id}/priority` | Enable or disable priority for a request (admin). |
| POST | `/channels/{channel}/queue/{request_id}/played` | Mark a request as played (admin). |

## Events
| Method | Path | Description |
|--------|------|-------------|
| POST | `/channels/{channel}/events` | Log a channel event such as follows, subscriptions, or bits (admin). |
| GET | `/channels/{channel}/events` | Retrieve logged events with optional filtering by type and time. |
| WS | `/channels/{channel}/events` | WebSocket stream that pushes queue and settings events for overlays. |

Certain events award priority points:

- `bits` events grant 1 point for any cheer of at least 200 bits.
- Gifted subs (`gift_sub` events) grant 1 point for every 5 subscriptions gifted.

### Channel event stream

The `/channels/{channel}/events` WebSocket emits JSON objects with the shape:

```
{
  "type": "event.name",
  "payload": {...},
  "timestamp": "2024-01-01T12:34:56.789Z"
}
```

All payloads only expose queue-facing data:

- `request.added` — `payload` is a request summary `{ "id", "song": { "title", "artist", "youtube_link" }, "requester": { "id", "username" }, "is_priority", "bumped", "priority_source" }`.
- `request.bumped` — same payload as `request.added`, emitted whenever a request gains priority (admin bump, playlist bump, or priority toggle).
- `request.played` — payload `{ "request": <request summary>, "up_next": <request summary>|null }`, where `up_next` is the next pending request after the played entry.
- `queue.status` — payload `{ "closed": bool, "status": "open"|"closed" }` indicating whether the queue accepts new requests.
- `queue.archived` — payload `{ "archived_stream_id": int|null, "new_stream_id": int }` describing the stream transition when archiving.
- `settings.updated` — payload mirroring `ChannelSettingsIn` (`max_requests_per_user`, `prio_only`, `queue_closed`, `allow_bumps`, `other_flags`, `max_prio_points`).
- `user.bump_awarded` — payload `{ "user": { "id", "username" }, "delta": int, "prio_points": int }` when a user earns additional priority points.

## Streams
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/streams` | List stream sessions for a channel. |
| POST | `/channels/{channel}/streams/start` | Ensure a stream session exists and return its ID (admin). |
| POST | `/channels/{channel}/streams/archive` | Close the current stream and start a new session (admin). |

## Stats
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/stats/general` | General statistics for the current stream such as total requests. |
| GET | `/channels/{channel}/stats/songs` | Top requested songs for the current stream. |
| GET | `/channels/{channel}/stats/users` | Top requesting users for the current stream. |

