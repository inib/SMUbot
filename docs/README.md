# Songbot Wiki

## Overview
Songbot is a Twitch song request platform composed of three main parts:

- **Backend** (`backend_app.py`): a FastAPI service that stores channels, songs, users, and request queues using SQLAlchemy models.
- **Bot** (`bot/bot_app.py`): a TwitchIO chat bot that lets viewers request songs and manage priorities by talking to the backend.
- **Web** (`web/`): an Nginx container serving a small static interface for viewing the current queue.

Additional directories include:

- **queue_manager/** – static assets served by the channel-facing Queue Manager UI.
- **admin/** – static assets for the Admin control panel used to manage the shared bot account and view channel stats.
- **data/** – persistent SQLite database storage.

## Admin panel bot message controls
- Channel detail cards in the Admin panel now include a dedicated **Bot Messages**
  tab beside **Custom Settings** and **Active Streams**.
- Each channel card shows a short summary above the tabs clarifying that the
  message matrix lives in **Bot Messages** (labelled **Bot Messages (Matrix)**).
- The tab reads `/channels/{channel}/settings`, shows the current
  `bot_message_level`, and persists level changes with
  `PUT /channels/{channel}/settings`.
- The UI includes a read-only message catalog matrix grouped by message ID with
  default level badges (`normal`, `verbose`, `debug`) and inherited inclusion
  behavior: **Normal ⊂ Verbose ⊂ Debug**, while **Mute overrides all**.
- If `/bot/messages/catalog` fails, the panel shows a prominent warning card
  with reason/remediation and renders a lightweight local fallback matrix so
  operators can still validate expected categories (`queue`, `commands`,
  `lifecycle`, `rewards`, `errors`) while fixing auth.
- Front-end state reserves a `perMessageOverrides` object map keyed by message
  ID for future customization workflows (editing controls intentionally hidden
  for now; currently unused and marked for future cleanup if this direction is
  dropped).

### Bot message catalog auth checklist (manual UI verification)
- [ ] **Admin token only works**: clear browser cookies, enter a valid admin
  token in the setup panel, and open a channel’s **Bot Messages** tab. Confirm
  the catalog matrix loads from `/bot/messages/catalog`.
- [ ] **Session-cookie only works**: remove any stored admin token from the
  setup panel, sign in via the Admin Twitch login flow so an admin session
  cookie exists, and re-open **Bot Messages**. Confirm the catalog matrix still
  loads.
- [ ] **Missing auth shows explicit guidance**: clear both admin token and
  session cookie, then open **Bot Messages**. Confirm the panel shows a
  user-facing authentication guidance message that explains admin token/session
  is missing and how to recover.

### Troubleshooting missing matrix / auth failures
- **Warning card says `401 unauthorized`**:
  1. In Admin panel **Setup**, set a valid admin token (`X-Admin-Token`) and save.
  2. Or sign in through the Admin Twitch auth flow to restore an admin session cookie.
  3. Refresh the channel card and re-open **Bot Messages (Matrix)**.
- **Matrix missing after sign-in**:
  1. Open browser devtools network tab and confirm `/bot/messages/catalog` returns 200.
  2. Confirm the backend origin in Admin footer points at the expected API host.
  3. Re-check reverse proxy auth/header forwarding for `X-Admin-Token` and cookies.
- **Temporary fallback matrix shown**:
  - This is expected degraded mode when live catalog metadata cannot be fetched.
  - Use fallback categories for quick validation, then restore auth to return to
    the full live catalog.

## Deployment setup flow
When the stack starts for the first time, the backend remains locked until an
administrator opens the Admin panel and saves the Twitch application
credentials. The `/system/status` endpoint reports whether setup is complete,
and all browser UIs display a guard banner while the deployment is locked. Once
the required fields are saved (client ID, client secret, redirect URIs, and any
desired scope overrides) the admin can mark the deployment as ready, which
unlocks the API for the bot, queue manager, and public web frontend.

## Running with Docker
1. Copy `example.env` to `stack.env` and adjust values such as `ADMIN_TOKEN`,
   `ADMIN_BASIC_AUTH_USERNAME`, `ADMIN_BASIC_AUTH_PASSWORD`, and `BACKEND_URL`
   (the bot uses it to reach the API). Twitch OAuth credentials, bot scopes,
   and overlay settings are now configured inside the Admin panel after the
   services start, so they no longer live in the environment file.
   When deploying, set `PUBLIC_BACKEND_ORIGIN` to the canonical HTTPS origin of
   the API (for example `https://api.example.com`). The static web, queue
   manager, and admin images expose this value through a small `config.js`
   snippet so browsers can call the backend without relying on localhost
   defaults. If you prefer to derive per-service subdomains from a shared base
   domain, that logic can also live in the Nginx entrypoint before the config
   file is generated.
2. Start the stack:
   ```bash
   docker-compose --env-file stack.env up --build
   ```
   This launches the API on port 7070, the bot, and the web UI on port 7000
   (overridden with `WEB_PORT`). On first boot, visit the Admin panel to
   complete the “Deployment Setup” flow before the API, queue manager, or bot
   pages are accessible.

## Backend Highlights
- Uses a SQLite database stored at `/data/db.sqlite` and defines models for channels, songs, users, stream sessions, and requests.
- Stores bot OAuth credentials via the `/bot/config` API and exposes an OAuth
  helper flow for authorizing the bot account.
- Automatically creates a manual `Favorites` playlist for new channels during
  admin channel creation and OAuth onboarding flows. The playlist is seeded
  idempotently with three tracks: `Night Drive` (FM-84),
  `Strobe` (deadmau5), and `LONG DISTANCE CALLING - Voices`
  (`https://www.youtube.com/watch?v=uWQQbQ9jqU4`).
- Exposes REST endpoints for managing songs and queue entries, plus SSE streams
  for queue updates and bot log streaming.
- `run.sh` initializes the database and starts the server with Uvicorn.

## Bot Highlights
- Automatically discovers authorized channels from the backend and joins them.
- Honors each channel's `bot_message_level` (`mute`, `normal`, `verbose`,
  `debug`) when deciding whether to send chat output; suppressed messages still
  go to backend bot logs for observability.
- Uses a central message catalog in `bot/bot_app.py` keyed by message IDs. Each
  entry defines a template key (`messages.yml`), default level, human
  description, and optional group (`commands`, `lifecycle`, `rewards`, `errors`,
  `queue`).
- Supports channel-level `bot_message_overrides` / `message_overrides` payloads
  so future per-command/per-message template or level customization can be
  rolled out without refactoring dispatch logic.
- Supports commands:
  - `!request` – add a song request.
  - `!playlist <name> <index>` – queue a song from a saved playlist by position.
  - `!prioritize` – bump one of your requests using priority points.
  - `!points` – check remaining priority points.
  - `!remove` – delete your latest request.
- Automatically parses YouTube links and fetches titles via oEmbed.

## Web Interface
The web container hosts files in `web/public/`, including a simple `index.html`, `app.js`, and `style.css` for viewing the queue.
- Public playlist cards now render with per-playlist **Show songs / Hide songs**
  controls so song rows are collapsible.
- Playlist song rows default to collapsed, and each card remembers its expanded
  state during in-page rerenders using an in-memory map keyed by playlist slug.
- Toggle buttons are wired for accessibility with `aria-expanded` plus
  `aria-controls` targeting each `.public-playlist__items` container.

## Authentication & Channel Access
Songbot relies on two distinct OAuth flows that map to the two management panels:

1. **Bot account authorization (Admin panel)** – The Admin control panel triggers a
   client credentials grant using the scopes `user:read:chat user:write:chat user:bot`.
   The resulting app access token is stored through `/bot/config` and allows the
   backend and bot worker to act as the shared bot account when calling the API.
   The Admin panel is protected with HTTP basic authentication configured via
   the `ADMIN_BASIC_AUTH_USERNAME` and `ADMIN_BASIC_AUTH_PASSWORD` environment
   variables.

2. **Channel authorization (Queue Manager)** – Channel owners sign in through the
   Queue Manager UI and complete the authorization code grant with the
   `channel:bot channel:read:subscriptions channel:read:vips bits:read moderator:read:followers user:read:email` scopes. The
   backend records the channel during this handshake and subscribes to chat
   events using the previously obtained app access token. Only channels that
   complete this flow are joined by the bot. Bits and follower access enable
   pricing features tied to cheers and follow events.

### Migrating existing channels
- After deploying the expanded default scopes, ask channel owners to log out and
  log back in through the Queue Manager so Twitch can issue tokens with the new
  permissions.
- Check the Queue Manager landing page for the scope list; missing `bits:read`
  or `moderator:read:followers` indicates reauthorization is still needed.
- The Queue Manager disables pricing controls (follows, raids, bits, gifted
  subs, and sub-tier rewards) when those scopes are missing and shows inline
  helper text prompting reauthorization.

Owners can invite moderators by adding their Twitch accounts inside the Queue
Manager, and authenticated users who manage multiple channels can switch between
them via `/me/channels`.

## Queue Manager users tab layout
- The Users tab renders a grid-aligned row per requester that keeps the “+1/-1”
  priority controls on the same line as the username for quick scanning.
- A square role badge precedes each name with priority Mod > VIP > Subscriber >
  Viewer; subscribers show their tier number inside the badge while viewers get
  a neutral grey marker.
- A compact legend row appears above the user list (`M`, `V`, subscriber tier,
  and `•`) so managers and screen readers can decode each symbol quickly.
- Metadata is presented in bracketed chips (for example `[behind: 3]` and
  `[prio: 2]`) to keep “amount behind” context aligned across rows.
- Each row includes a **Delete** action so managers can remove malformed or
  stale user records directly from the console; related queue/request rows
  cascade according to backend foreign-key rules.
- Channel owners and the playlist automation helper are filtered out of the
  listing, and badges update automatically as pages load or refresh.

## Queue Manager unified bot control dropdown
- The Queue Manager header now shows a single bot-control dropdown beside the
  channel and bot badges whenever the selected channel is authorized.
- Dropdown options are state-aware:
  - **Disconnected (`join_active = 0`)**: a disabled `Select action…`
    placeholder plus `connect` are shown so reconnect is always a user-triggered
    selection change.
  - **Connected (`join_active = 1`)**: `disconnect` plus message levels
    `mute`, `normal`, `verbose`, and `debug` are shown.
- Reconnect behavior is explicit: after a disconnect, the control resets to the
  placeholder and the next `connect` pick immediately calls the channel-status
  endpoint (instead of leaving `connect` preselected and non-invokable).
- API mapping is explicit:
  - `connect`/`disconnect` -> `PUT /channels/{channel}?join_active=1|0`
  - `mute|normal|verbose|debug` -> `PUT /channels/{channel}/settings` with
    `{ "bot_message_level": "<level>" }`
- The Settings tab reuses the same dropdown renderer for
  bot controls so behavior stays aligned with the header control while each
  surface remains independently usable.
- Settings organization now includes a dedicated **Bot Control** section with
  two rows:
  - **Bot connection** (`join/part`) -> `PUT /channels/{channel}?join_active=1|0`
  - **Bot message level** (`mute|normal|verbose|debug`) ->
    `PUT /channels/{channel}/settings` with
    `{ "bot_message_level": "<level>" }`
- After successful bot-control updates, the Queue Manager syncs header and
  Settings state via `updateRegButton()` plus a settings refresh so both
  controls stay consistent without coupling their rendering lifecycles.

## Queue Manager quick controls strip
- The Queue tab now includes a compact **quick controls** strip above the queue
  layout for the four highest-touch toggles:
  `queue_closed`, `prio_only`, `allow_bumps`, and `full_auto_priority_mode`.
- The strip loads values from `GET /channels/{channel}/settings`, writes
  changes with the same `PUT /channels/{channel}/settings` flow used in the
  Settings tab, and auto-refreshes after any successful setting change from
  either surface.
- Scope-gated setting behavior is shared with the main settings renderer, so a
  quick-control toggle is disabled and carries the same warning text whenever a
  required Twitch scope is missing.
- The strip now wraps by default to preserve queue card width parity with the
  Playlists, Users, and Settings tabs, and only enables horizontal scrolling at
  explicit wide breakpoints with a `max-width: 100%` guard.

## Development Tips
- Install Python dependencies from `requirements.txt` for local development.
- Run the backend directly:
  ```bash
  ./run.sh
  ```
- Launch the bot locally:
  ```bash
  python bot/bot_app.py
  ```

## Changelog
### 2026-03-29
- Removed duplicate channel-settings bootstrap technical debt by retiring
  `_ensure_channel_settings_schema()` and keeping
  `ensure_channel_settings_schema()` as the canonical startup migration helper.
- Canonical startup schema backfills now cover both legacy queue-cap columns
  and newer priority/bot-message settings in one path.
