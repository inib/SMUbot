# Backend API Endpoints

This document summarizes the REST endpoints exposed by `backend_app.py`.

## System
| Method | Path | Description |
|--------|------|-------------|
| GET | `/system/health` | Health check that verifies database connectivity. |

## Authentication
| Method | Path | Description |
|--------|------|-------------|
| GET | `/auth/login` | Build a Twitch OAuth authorization URL for a channel, optionally preserving a `return_url`. |
| GET | `/auth/callback` | Twitch OAuth callback that stores the access token and marks the user as the channel owner. |
| POST | `/auth/session` | Exchange a user OAuth token for a server-side session cookie. |
| POST | `/auth/logout` | Clear the admin session cookie. |

## Channel keys
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/key` | Return the active channel key for owners or moderators authenticated with OAuth. |
| POST | `/channels/{channel}/key/regenerate` | Rotate and return a new channel key for the specified channel (owner/moderator OAuth). |

**Channel-key usage**

- Supply either `X-Channel-Key: <key>` or the query parameter `channel_key=<key>` when calling channel-safe endpoints.
- Channel keys are generated automatically when channels are created and are backfilled for existing databases at startup.
- Admin-only endpoints still expect `X-Admin-Token` or a bearer/admin session cookie; channel keys never bypass admin checks.
- The following endpoints accept channel keys (in addition to existing admin/OAuth fallbacks): playlist CRUD and reads, playlist queue helpers, random playlist requests, queue reads/writes (including random pulls), event logging, queue/playlist streams, and stream start/archive hooks.
- The Queue Manager settings tab displays the active channel key for logged-in owners and moderators. Use the **Regenerate** control to rotate secrets if they were exposed; update any scripts sending `X-Channel-Key` or `channel_key` afterward.
- Front-end configuration for the Queue Manager lives in `queue_manager/public/config.js`, which expects `BACKEND_URL` (pointing to this API), `TWITCH_CLIENT_ID`, and optional `TWITCH_SCOPES` used for OAuth.

### `/auth/login`
- **Query parameters**
  - `channel` (required): Channel login used to embed into the OAuth `state` parameter.
  - `return_url` (optional): URL-encoded location to redirect to after authorization.
- **Response**: `{ "auth_url": "<twitch authorize url>" }` built with the configured Twitch client ID, redirect URI, and scopes from `TWITCH_SCOPES`.
- **Notes**: Fails with HTTP 500 if Twitch OAuth configuration is missing.

### `/auth/callback`
- **Query parameters**
  - `code`: Authorization code returned by Twitch.
  - `state`: Either a channel login string or JSON containing `{ "channel": <login>, "return_url": <url?> }`.
- **Behavior**
  - Exchanges `code` for an access token and requires the `channel:bot` scope.
  - Fetches the authenticated Twitch user and upserts `TwitchUser` plus the matching `ActiveChannel`, setting `authorized=True` and `owner_id` to the user.
  - Redirects to `return_url` when supplied and using an `http`/`https` scheme; otherwise returns `{ "success": true }`.

### `/auth/session`
- **Authentication**: `Authorization: Bearer <user OAuth token>`.
- **Behavior**
  - Validates the token against `https://id.twitch.tv/oauth2/validate` and refreshes the stored `TwitchUser` record.
  - Auto-registers the channel as owned when the token carries the `channel:bot` scope.
  - Sets the `admin_oauth_token` cookie (HTTP-only, `SameSite=lax`) for subsequent admin access, honoring Twitch `expires_in` when present.
- **Response**: `{ "login": "<twitch username>" }`.
- **Errors**: 401 when the bearer token is missing or invalid.

### `/auth/logout`
- **Behavior**: Removes the `admin_oauth_token` cookie and returns `{ "success": true }`.

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

Channel settings include queue intake controls:

- `queue_closed` toggles whether any new requests are accepted.
- `overall_queue_cap` (0–100, default 100) auto-closes intake once pending requests reach the cap and emits a `queue.status` event.
- `nonpriority_queue_cap` (0–100, default 100) rejects new non-priority submissions when full while still allowing priority requests.
- `prio_only`, `max_requests_per_user`, `allow_bumps`, `other_flags`, and `max_prio_points` behave as before and are reflected in `settings.updated` events.
- Existing deployments should apply `migrations/20240624_queue_caps.sql` to add the new capacity columns and backfill defaults for legacy channels; the application also attempts to patch missing columns on startup for SQLite/legacy installs before enforcing queue caps.

## Songs
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/songs` | Search songs in a channel, optionally filtering by artist or title. |
| POST | `/channels/{channel}/songs` | Add a song to the catalog (admin). |
| GET | `/channels/{channel}/songs/{song_id}` | Fetch a specific song. |
| PUT | `/channels/{channel}/songs/{song_id}` | Update song details (admin). |
| DELETE | `/channels/{channel}/songs/{song_id}` | Remove a song from the catalog (admin). |

## Playlists
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/playlists` | List saved playlists for a channel (channel key or admin/owner/moderator). |
| POST | `/channels/{channel}/playlists` | Add a YouTube playlist reference and import its items (channel key or admin/owner/moderator). |
| PUT | `/channels/{channel}/playlists/{playlist_id}` | Update playlist visibility or keywords (channel key or admin/owner/moderator). |
| DELETE | `/channels/{channel}/playlists/{playlist_id}` | Remove a playlist and its items (channel key or admin/owner/moderator). |

### `/channels/{channel}/playlists`
- **Authentication**: Provide `X-Channel-Key`, `channel_key=<key>`, `X-Admin-Token`, or a bearer token/admin session cookie for a channel owner or moderator.
- **GET behavior**
  - Returns playlists sorted by title with keywords sorted alphabetically.
  - **Response**: Array of `{ "id", "title", "playlist_id", "url", "visibility", "keywords": [str], "item_count" }`.
- **POST payload**: `{ "url": "<youtube playlist url>", "keywords": ["rock"?], "visibility": "public|private|unlisted" }`.
  - Extracts the playlist ID from the URL, downloads metadata/tracks, and persists each item with position, title, artist, duration, and URL. Rejects invalid URLs (HTTP 400) or duplicates (HTTP 409).
  - **Response**: `{ "id": <int> }` for the created playlist.
- **Use cases**: Seed curated lists for random song draws, associate keywords (e.g., `default`, genres) for chat triggers, and control playlist availability to overlays.

### `/channels/{channel}/playlists/{playlist_id}`
- **Authentication**: Same as the list/create endpoint.
- **PUT payload**: `{ "keywords"?: [str], "visibility"?: "public|private|unlisted" }`; updates fields when present and returns the refreshed playlist summary with the latest keyword set.
- **DELETE behavior**: Removes the playlist and its imported items; responds with HTTP 204 on success.

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
| GET | `/channels/{channel}/queue/stream` | Server-sent events stream emitting queue updates (public read; no auth required). |
| GET | `/channels/{channel}/queue` | Current request queue for the active stream (public read; no auth required). |
| GET | `/channels/{channel}/streams/{stream_id}/queue` | Request queue for a specific past stream (public read; no auth required). |
| POST | `/channels/{channel}/queue` | Add a song request to the queue (channel key or admin). |
| PUT | `/channels/{channel}/queue/{request_id}` | Update request status such as marking played (channel key or admin). |
| DELETE | `/channels/{channel}/queue/{request_id}` | Remove a request (channel key or admin). |
| POST | `/channels/{channel}/queue/clear` | Remove all pending requests for the current stream (channel key or admin). |
| GET | `/channels/{channel}/queue/random_nonpriority` | Fetch a random non-priority request from the queue (public read; no auth required). |
| GET | `/channels/{channel}/queue/next_nonpriority` | Fetch the next non-priority pending request, preferring bumped entries (public read; no auth required). |
| GET | `/channels/{channel}/queue/next_priority` | Fetch the next priority pending request, preferring bumped entries (public read; no auth required). |
| GET | `/channels/{channel}/queue/next_song` | Fetch the next song, choosing priority first then non-priority (public read; no auth required). |
| GET | `/channels/{channel}/queue/stats` | Retrieve aggregate queue counters for the active stream (public read; no auth required). |
| GET | `/channels/{channel}/queue/stats/total_priority` | Return only the unplayed priority request count for the active stream (public read; no auth required). |
| GET | `/channels/{channel}/queue/stats/total_nonpriority` | Return only the unplayed non-priority request count for the active stream (public read; no auth required). |
| GET | `/channels/{channel}/queue/stats/total_unplayed` | Return only the total unplayed request count for the active stream (public read; no auth required). |
| GET | `/channels/{channel}/queue/stats/total_played` | Return only the played request count for the active stream (public read; no auth required). |
| POST/GET | `/channels/{channel}/queue/{request_id}/bump_admin` | Force a request to priority status (channel key or admin). |
| POST/GET | `/channels/{channel}/queue/{request_id}/move` | Move a request up or down in the queue (channel key or admin). |
| POST/GET | `/channels/{channel}/queue/{request_id}/skip` | Send a request to the end of the queue (channel key or admin). |
| POST/GET | `/channels/{channel}/queue/{request_id}/priority` | Enable or disable priority for a request (channel key or admin). |
| POST/GET | `/channels/{channel}/queue/{request_id}/played` | Mark a request as played (channel key or admin). |
| GET | `/channels/{channel}/queue/full` | Return the full queue with song and requester details (public read; no auth required). |

### `/channels/{channel}/queue/full`
- **Authentication**: None; public read access for overlays and dashboards.
- **Behavior**
  - Finds the current stream and orders requests by played status, priority flags, manual position, and request time.
  - Joins request rows with `Song` and `User` models and enriches users with VIP/subscriber status when available.
- **Response**: Array of `{ "request": { "id", "song_id", "user_id", "request_time", "is_priority", "bumped", "played", "priority_source" }, "song": { "id", "artist", "title", "youtube_link", ... }, "user": { "id", "twitch_id", "username", "is_vip", "is_subscriber", "subscriber_tier" } }`.
- **Use cases**: Drive moderator dashboards or overlay widgets that need a complete view of the queue without issuing multiple lookups per request.

### `/channels/{channel}/queue/next_nonpriority`
- **Authentication**: None; public access.
- **Behavior**
  - Finds the active stream for the channel and filters pending requests where `is_priority == 0` and `played == 0`.
  - Orders by `bumped` descending, then manual `position`, `request_time`, and `id` to surface bumped picks first.
  - Serializes request, song, and user payloads consistent with queue listings.
- **Response**: Either `null` when no eligible request exists or `{ "request": RequestOut, "song": SongOut, "user": UserOut }`.

### `/channels/{channel}/queue/next_priority`
- **Authentication**: None; public access.
- **Behavior**
  - Finds the active stream for the channel and filters pending requests where `is_priority == 1` and `played == 0`.
  - Orders by `bumped` descending, then manual `position`, `request_time`, and `id` to keep bumped priority picks ahead.
  - Serializes request, song, and user payloads consistent with queue listings.
- **Response**: Either `null` when no eligible request exists or `{ "request": RequestOut, "song": SongOut, "user": UserOut }`.

### `/channels/{channel}/queue/next_song`
- **Authentication**: None; public access.
- **Behavior**
  - Resolves the active stream, returns the next priority request when available, otherwise falls back to the next non-priority item.
  - Uses the same bumped-aware ordering as the dedicated priority/non-priority routes.
  - Serializes request, song, and user payloads consistent with queue listings.
- **Response**: Either `null` when no eligible request exists or `{ "request": RequestOut, "song": SongOut, "user": UserOut }`.

### `/channels/{channel}/queue/stats` and `/channels/{channel}/queue/stats/total_*`
- **Authentication**: None; public access.
- **Behavior**
  - Scopes counts to the active stream and returns:
    - `total_unplayed`: Number of pending requests regardless of priority.
    - `total_priority`: Pending requests where `is_priority == 1`.
    - `total_nonpriority`: Pending requests where `is_priority == 0`.
    - `total_played`: Requests already marked played for the stream.
  - `/stats/total_priority`, `/stats/total_nonpriority`, `/stats/total_unplayed`, `/stats/total_played` return the individual integers only.
- **Response**: `/stats` returns `{ "total_unplayed", "total_priority", "total_nonpriority", "total_played" }`; the `/total_*` routes return an integer body.

### streamer.bot automation shortcuts
- **Context**: The Queue Manager UI surfaces streamer.bot-friendly shortcut links for quick HTTP actions.
- **Endpoints covered**: `queue/random_nonpriority`, `queue/next_nonpriority`, `queue/next_priority`, `queue/next_song`, `queue/stats`, `queue/stats/total_priority`, `queue/stats/total_nonpriority`, `queue/stats/total_unplayed`, `queue/stats/total_played`, `queue/{request_id}/bump_admin`, `queue/{request_id}/move`, `queue/{request_id}/skip`, `queue/{request_id}/priority`, `queue/{request_id}/played`, and `queue/full`.
- **Channel key usage**: Only the request mutation endpoints (`bump_admin`, `move`, `skip`, `priority`, `played`) require `channel_key=<key>` or `X-Channel-Key`. The lookup and stats endpoints intentionally omit the key for public overlays.
- **Response shapes**: The lookup routes return `{ "request", "song", "user" }` payloads (or arrays of those for `/queue/full`), while stats routes return integer counts. Mutation routes echo the updated request payload for confirmation.

## YouTube Music
| Method | Path | Description |
|--------|------|-------------|
| GET | `/ytmusic/search` | Search YouTube Music and normalize matching song results. |

### `/ytmusic/search`
- **Authentication**: None; intended for public song lookups.
- **Query parameters**
  - `query` (required): Search term trimmed to 1-200 characters. Empty strings return HTTP 400.
- **Behavior**
  - Initializes the `ytmusicapi` client (fails with HTTP 502 if the optional dependency or auth file is missing).
  - Calls `client.search(query, limit=10)` and normalizes up to 5 items that contain a YouTube `videoId` and a supported result type (`song`, `video`, or `music_video`).
- **Response**: Array of objects `{ "title", "video_id", "playlist_id", "browse_id", "result_type", "artists": [str], "album", "duration", "thumbnails": [{ "url", "width?", "height?" }], "link" }` where `link` falls back to a YouTube watch/playlist/browse URL when missing.
- **Use cases**: Power autocomplete and song-picking UIs before creating requests or importing playlist tracks.

Queue endpoints that accept `{request_id}` support numeric identifiers for full
backwards compatibility **and** keyword shortcuts to target specific queue
entries without first listing the queue. Supported keywords are:

- The path parameter is treated as a string with the pattern `^(?:\d+|top|previous|last|random)$`
  so keywords like `top` and `last` bypass FastAPI integer coercion and are
  resolved consistently by `resolve_queue_request` for both POST and GET
  variants of each mutation route.

- `top` — next up: the highest-priority pending request ordered by priority,
  then position, then request time.
- `previous` — the most recently played entry in the current stream.
- `last` — the trailing pending entry (largest position), i.e., the most recent
  addition that has not been played.
- `random` — a random pending entry from the current stream.

Example calls (keywords and numeric IDs are interchangeable):

- Mark a specific request played by ID: `POST /channels/{channel}/queue/42/played`
- Mark the next song played: `POST /channels/{channel}/queue/top/played`
- Toggle priority on the last played item: `POST /channels/{channel}/queue/previous/priority?enabled=false`
- Remove the newest pending request: `DELETE /channels/{channel}/queue/last`
- Skip a random pending item to the back: `POST /channels/{channel}/queue/random/skip`

### Queue mutation GET variants

- **Authentication**: Channel key header/query or admin/moderator session/Bearer token (same as POST routes).
- **Non-cacheable responses**: All GET mutations send `Cache-Control: no-store, max-age=0` and `Pragma: no-cache`.
- **`/channels/{channel}/queue/{request_id}/move`**
  - `direction` (query, required for GET): `up` or `down`. POST still accepts the JSON body `{ "direction": "up|down" }`.
- **`/channels/{channel}/queue/{request_id}/priority`**
  - `enabled` (query, required for GET): boolean toggle. POST accepts `{ "enabled": true|false }` or the same query param.
- **`/channels/{channel}/queue/{request_id}/bump_admin`**, **`skip`**, **`played`**
  - No additional parameters beyond the `request_id` path value. GET behaves identically to POST for these state changes.

## Events
| Method | Path | Description |
|--------|------|-------------|
| POST | `/channels/{channel}/events` | Log a channel event such as follows, subscriptions, or bits (channel key or admin). |
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
- `queue.status` — payload `{ "closed": bool, "status": "open"|"closed"|"limited", "reason"?: str }` indicating whether the queue accepts new requests or has restricted non-priority slots. Hitting the overall queue cap flips the queue to `closed` until it is reopened.
- `queue.archived` — payload `{ "archived_stream_id": int|null, "new_stream_id": int }` describing the stream transition when archiving.
- `settings.updated` — payload mirroring `ChannelSettingsIn` (`max_requests_per_user`, `prio_only`, `queue_closed`, `allow_bumps`, `other_flags`, `max_prio_points`, `overall_queue_cap`, `nonpriority_queue_cap`).
- `user.bump_awarded` — payload `{ "user": { "id", "username" }, "delta": int, "prio_points": int }` when a user earns additional priority points.

## Streams
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/streams` | List stream sessions for a channel. |
| POST | `/channels/{channel}/streams/start` | Ensure a stream session exists and return its ID (channel key or admin). |
| POST | `/channels/{channel}/streams/archive` | Close the current stream and start a new session (channel key or admin). |

## Stats
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels/{channel}/stats/general` | General statistics for the current stream such as total requests. |
| GET | `/channels/{channel}/stats/songs` | Top requested songs for the current stream. |
| GET | `/channels/{channel}/stats/users` | Top requesting users for the current stream. |

## Current user
| Method | Path | Description |
|--------|------|-------------|
| GET | `/me` | Return the authenticated Twitch user from the bearer token or admin session cookie. |
| GET | `/me/channels` | List channels the user owns or moderates. |

### `/me`
- **Authentication**: `Authorization: Bearer <user OAuth token>` or `admin_oauth_token` cookie.
- **Response**: `{ "login", "display_name", "profile_image_url" }`. The backend attempts a best-effort Twitch `/helix/users` lookup to populate display name and avatar; falls back to the stored username on failure.
- **Errors**: 401 when no token or cookie is provided.

### `/me/channels`
- **Authentication**: Same as `/me`.
- **Response**: Array of `{ "channel_name", "role" }` entries where `role` is `owner` for `ActiveChannel.owner_id` matches, and `moderator` for linked `ChannelModerator` rows.

## Channel moderation
| Method | Path | Description |
|--------|------|-------------|
| POST | `/channels/{channel}/mods` | Add a moderator link for a channel. |
| POST | `/channels/{channel}/bot_status` | Update the bot activity/error state for a channel. |

### `/channels/{channel}/mods`
- **Authentication**: Requires `X-Admin-Token` header matching `ADMIN_TOKEN` *or* a valid bearer token/session cookie (validated via Twitch). When using a bearer token, the caller must be the channel owner; otherwise a 403 error is returned.
- **Payload**: `{ "twitch_id": "<user id>", "username": "<login>" }`.
- **Behavior**: Upserts the Twitch user if missing, then creates the `ChannelModerator` link when absent. Returns `{ "success": true }` on success.

### `/channels/{channel}/bot_status`
- **Authentication**: Requires `X-Admin-Token` or a valid bearer token/session cookie. No additional role check is enforced beyond token validity.
- **Payload**: `{ "active": <bool>, "error": "<optional last error>" }`.
- **Behavior**: Ensures a `ChannelBotState` row exists, updates `active` and `last_error`, persists changes, and emits a queue change notification.

