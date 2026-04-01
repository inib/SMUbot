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

### Chat ingress defaults and staged rollout flags
- `/system/config` now exposes `chat_ingress_mode` with:
  - `webhook_conduit` (default, authoritative)
  - `websocket` (legacy fallback only)
- `/system/config` also exposes `chat_ingress_shadow_mode` (default `false`) so
  operators can run dual-path validation before a full ingress cutover.
- Legacy rollback is explicit through `chat_websocket_fallback_legacy_enabled`.
  Websocket EventSub chat subscriptions are suppressed in bot runtime when
  `chat_ingress_mode=webhook_conduit` and this rollback flag is `false`.
- Startup/runtime ingress guard now evaluates conduit health with high-severity
  alerts and optional auto-fallback (`chat_ingress_guard_auto_fallback_enabled`)
  when:
  - healthy conduit shards drop below `chat_ingress_guard_min_healthy_shards`,
  - callback 4xx/5xx volume in `chat_ingress_guard_window_seconds` exceeds
    `chat_ingress_guard_callback_error_threshold`.
- Startup import now validates ingress-guard symbol availability before running
  the guard so symbol-order regressions fail fast during process boot.
- Migration `migrations/20260330_eventsub_conduits.sql` adds additive EventSub
  replay/conduit tables (`eventsub_message_dedupe`, `twitch_conduits`,
  `twitch_conduit_shards`) plus conduit linkage columns on
  `event_subscriptions`.
- Conduit shard metadata now persists `twitch_conduit_shards.transport_secret`
  so callback verification can validate Twitch conduit-shard challenges without
  requiring `subscription.id`.
- Startup now includes a compatibility patch for the same tables/columns so
  staggered deploys on legacy SQLite/prod databases continue booting safely.
- EventSub health endpoints now include conduit + shard coverage summaries:
  - `GET /system/health` reports global conduit assignment coverage.
  - `GET /system/health.eventsub.ingress_summary` adds compact runtime counters:
    callback 2xx/4xx/5xx, signature failures, dedupe hits, per-channel command
    dispatch outcomes, shard status transitions, and last error timestamps.
  - `GET /system/health.eventsub.authoritative_guard` reports degradation
    reasons and whether auto-fallback was applied.
  - `GET /channels/{channel}/eventsub/health` reports per-channel shard
    assignment state and can trigger reconcile with `?reconcile=true`, which
    refreshes local/conduit/shard/coverage fields after reconciliation.
- Conduit API + conduit-mode EventSub subscription management authentication:
  - Reconciliation uses Twitch **app access token** auth (client credentials)
    for conduit create/update/patch calls and conduit-transport subscriptions.
  - Identity mapping remains:
    - `broadcaster_user_id` = target channel.
    - `user_id` = bot user id.
- EventSub conduit troubleshooting quick map:

  | HTTP status | Typical meaning | Operator action |
  |---|---|---|
  | `400` | Payload/condition/transport mismatch | Re-check EventSub type payload schema, conduit transport block, and required condition fields (`broadcaster_user_id`, `user_id`). |
  | `401` | Invalid token/client pairing | Confirm app access token validity and that token `client_id` matches configured Twitch client credentials. |
  | `403` | Permission/authorization context mismatch | Verify request context/token type and the authorized channel/bot relationship for the requested subscription. |

- Operator verification steps:
  1. Run `GET /system/health` and confirm global conduit/shard coverage is healthy.
  2. Run `GET /channels/{channel}/eventsub/health?reconcile=true` and confirm per-channel reconciliation + coverage complete successfully.
- EventSub callback operations contract:
  - `/twitch/eventsub/callback` must be publicly reachable via HTTPS from
    Twitch (no private-only callback hostnames).
  - Callback validation now enforces both:
    - `https://` scheme.
    - Exact callback path: `/twitch/eventsub/callback`.
  - Internal/private-only callback hostnames are explicitly rejected during
    reconciliation (`backend`, `api`, `localhost`, private IPs, and bare
    private-only hostnames without public DNS suffixes).
  - Preferred: set `/system/config.eventsub_callback_override` to the exact
    public callback URI, for example:
    `https://api.example.com/twitch/eventsub/callback`.
  - Fallback: set `/system/config.public_backend_origin` (or environment
    bootstrap `PUBLIC_BACKEND_ORIGIN`) so registration can derive:
    `https://<public-backend-origin>/twitch/eventsub/callback`.
  - Legacy bootstrap: `TWITCH_EVENTSUB_CALLBACK` is kept for initial/default
    bootstrap only; runtime operations should use admin-managed
    `eventsub_callback_override` to avoid stale env drift.
  - Callback source precedence is: `eventsub_callback_override` >
    `public_backend_origin` > `request_url` fallback.
  - Callback verification supports both payload variants:
    - classic verification payloads (`subscription` shape),
    - conduit shard verification payloads (`conduit_shard` shape with no
      `subscription`).
  - Conduit shard verification now reads `conduit_shard.shard` (not legacy
    `conduit_shard.id`) alongside `conduit_shard.conduit_id` for secret lookup.
  - Legacy `subscription id missing` errors for conduit verification were caused
    by an older global subscription-id assumption; this is fixed by routing
    verification shape before subscription lookup.
  - Legacy `missing_conduit_shard_id` failures were fixed by switching callback
    validation to Twitch's current conduit payload fields and explicit missing
    field reason codes.
  - Reconciliation warning output now includes callback source metadata:
    `eventsub_callback_override`, `public_backend_origin`, or `request_url`.
  - Invalid callback candidates degrade reconciliation status with explicit
    remediation text instead of silently failing with limited context.
  - HTTP callback registration is blocked with `callback_url_not_https`;
    `/system/config` also returns HTTP 400 details when an override is invalid
    (non-HTTPS, wrong path, or internal/private host).
  - `301` redirecting `http://...` to `https://...` is not a supported
    substitute for reliable Twitch callback registration.
  - Signatures are validated before processing. Classic webhook callbacks use
    `event_subscriptions.secret`; conduit chat callbacks use
    `twitch_conduit_shards.transport_secret` resolved via persisted conduit
    linkage (`conduit_id`, `shard_id`, transport metadata).
  - For conduit rows, `event_subscriptions.secret` is a non-authoritative
    compatibility placeholder; legacy/random values on existing rows are
    ignored for conduit notification verification.
  - `notification` retries are deduplicated via `eventsub_message_dedupe` to
    avoid replaying queue commands/reward logic.
  - `channel.chat.message` webhook notifications now route through a dedicated
    chat ingress handler that can execute canonical commands when webhook is
    authoritative. Outcomes are structured (`executed`, `rejected`, `error`)
    with parse/auth/business-rule/send reason code namespaces for diagnostics.
  - Authoritative webhook command execution currently covers `request`,
    `playlist_request`, `prioritize`, `remove`, and `points`.
  - Authoritative webhook command execution now also emits user-facing chat
    replies through Twitch `POST /helix/chat/messages` using a stable reply
    contract (`status`, `template_key`, `template_vars`, `visibility`) so
    webhook responses match websocket/bot catalog semantics.
  - Reply threading now uses chat event payload `event.message_id` only (never
    the EventSub transport header message id). If `event.message_id` is
    missing, the send payload omits `reply_parent_message_id`.
  - Deterministic Send Chat 400 guards now run before HTTP requests:
    `broadcaster_id` and `sender_id` must be non-empty, message length must be
    within Twitch Send Chat limits (`1-500` chars), and `sender_id` must match
    the bot token subject (`/oauth2/validate user_id`). Failed preflight emits
    explicit reason codes and skips request/retry.
  - Reply visibility still honors per-channel `bot_message_level` thresholds,
    so muted/normal/verbose/debug suppression behavior is unchanged.
  - Shadow mode remains non-sending and non-mutating for webhook commands even
    when parsing/execution diagnostics run.
  - Ingress telemetry now includes reply observability counters:
    `command_executed_count`, `reply_sent_count`, `reply_suppressed_count`,
    and `send_api_failure_count`.
  - Historical comparison-only webhook command code paths are now deprecated
    and explicitly marked in outcome reason codes until fully removed
    (`random_request` is the remaining websocket-only cleanup candidate).
- Shadow mode behavior:
  - With `chat_ingress_shadow_mode=true`, webhook chat ingress runs in
    non-authoritative observe/compare mode (no queue mutations and no reply
    sends), while websocket remains authoritative.
  - With `chat_ingress_mode=webhook_conduit` and shadow mode disabled, webhook
    ingress is authoritative and performs real command execution/persistence.

### EventSub HTTPS callback pre-cutover check
Use this before enabling `chat_ingress_mode=webhook_conduit` in production:

```bash
curl -sS http://localhost:7070/system/config | jq '{public_backend_origin, chat_ingress_mode, chat_ingress_shadow_mode}'
```

Expected:
- `public_backend_origin` is an `https://` origin.
- Derived EventSub callback format is:
  `https://<public-backend-origin>/twitch/eventsub/callback`.

### Rollback + triage playbook
1. Confirm current mode and guard state:
   - `GET /system/config`
   - `GET /system/health` and inspect `eventsub.ingress_summary` +
     `eventsub.authoritative_guard`.
2. If degraded (`missing_healthy_shards` / `callback_errors_spike`), run:
   - `GET /channels/{channel}/eventsub/health?reconcile=true`
   - validate callback URL config + shard statuses.
3. Emergency rollback:
   - set `chat_websocket_fallback_legacy_enabled=true`,
   - optionally switch `chat_ingress_mode=websocket` if operator policy
     requires immediate handoff.
4. After stabilization, disable rollback flag again and restore
   `chat_ingress_mode=webhook_conduit`.

### Maintenance checklist
- Verify bot/app token refreshes complete successfully each day.
- Reconcile conduit shards on a fixed cadence (recommended: every 15 minutes or
  after channel topology changes).
- Validate callback URL remains public HTTPS with exact
  `/twitch/eventsub/callback` path.
- Review `ingress_summary` counters for signature failures, dedupe spikes, and
  shard status churn.

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

### Bot deployment notes
- The bot image must copy all Python modules under `bot/` (for example
  `bot_app.py`, `chat_command_core.py`, and future helper modules) into `/bot/`
  so direct-script runtime mode keeps working with
  `CMD ["python", "-u", "bot_app.py"]`.
- `bot/Dockerfile` now uses `COPY bot/*.py ./` for future-proof module pickup.
  If additional non-runtime files appear under `bot/`, add or update a
  `.dockerignore` rule to keep image context small while preserving required
  runtime modules.

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
- Chat command parsing/execution for `!request`, `!playlist`, `!random`,
  `!prioritize`, `!points`, and `!remove` is centralized in
  `bot/chat_command_core.py` using a transport-agnostic normalized chat DTO so
  TwitchIO handlers and tests can share the same execution path.
- `bot/bot_app.py` resolves that command core import in both package mode
  (`bot.bot_app`) and direct script mode (`python /bot/bot_app.py`) to support
  local debugging and container entrypoint execution.
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

## Queue Manager bot controls
- The Queue Manager header shows a **verbosity-only** dropdown beside the
  channel and bot badges whenever the selected channel is authorized.
- Header options are limited to message levels:
  `mute`, `normal`, `verbose`, and `debug`.
- Header controls do **not** include connect/disconnect actions; those actions
  are intentionally scoped to the Settings tab.
- The header verbosity dropdown is disabled while the bot is disconnected
  (`join_active = 0`) and shows guidance to connect the bot in **Settings**
  before changing verbosity.
- Header level changes only call:
  `PUT /channels/{channel}/settings` with
  `{ "bot_message_level": "<level>" }`.
- Connect/disconnect actions were moved to the **Settings** tab.
- The Settings tab now uses the same toggle-switch visual pattern as the main
  queue toggles for **Bot connection**, including the matching
  **Connected/Disconnected** state label text.
- Settings organization now includes a dedicated **Bot Control** section with
  two rows:
  - **Bot connection** (toggle switch) ->
    `PUT /channels/{channel}?join_active=1|0`
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
### 2026-03-30
- Added EventSub webhook replay idempotency and conduit schema support via
  `migrations/20260330_eventsub_conduits.sql`.
- Added `/system/config` defaults for `chat_ingress_mode` (`websocket`) and
  `chat_ingress_shadow_mode` (`false`), including startup compatibility patch
  logic for staggered deploys.
- Marked ingress shadow mode as a rollout guard; if rollout strategy changes
  permanently, review for future cleanup.
- Added EventSub callback routing for `channel.chat.message` notifications plus
  structured websocket-vs-webhook ingress comparison logs.
- Added callback retry dedupe guard enforcement in callback processing to avoid
  duplicate command/event execution on Twitch notification retries.

### 2026-03-29
- Removed duplicate channel-settings bootstrap technical debt by retiring
  `_ensure_channel_settings_schema()` and keeping
  `ensure_channel_settings_schema()` as the canonical startup migration helper.
- Canonical startup schema backfills now cover both legacy queue-cap columns
  and newer priority/bot-message settings in one path.
