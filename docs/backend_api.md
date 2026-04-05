# Backend API Endpoints

This document summarizes the REST endpoints exposed by `backend_app.py`.

## Canonical chat architecture
- **Ingress**: EventSub webhook + conduit transport on
  `/twitch/eventsub/callback`.
- **Execution**: shared chat command core used by webhook and websocket parsing
  paths for consistent command semantics.
- **Outbound**: Twitch Send Chat Message API (`POST /helix/chat/messages`) for
  bot replies.


## System
| Method | Path | Description |
|--------|------|-------------|
| GET | `/system/health` | Health check that verifies database connectivity plus global EventSub webhook/conduit coverage, shard state, and ingress guard telemetry. |
| GET | `/system/config` | Read deployment configuration defaults and ingress mode toggles. |
| PUT | `/system/config` | Update deployment configuration, scopes, and ingress mode toggles (admin token required). |

### `/system/config`
- **GET response fields**
  - Existing setup/OAuth fields (`setup_complete`, client IDs/secrets, redirect URIs, scopes).
  - `eventsub_callback_override`: optional admin-managed full callback override URL (example: `https://api.example.com/twitch/eventsub/callback`).
  - `public_backend_origin`: canonical public backend origin used for EventSub callback URL generation (example: `https://api.example.com`).
  - `chat_ingress_mode`: `webhook_conduit` (default authoritative) or `websocket` (legacy fallback).
  - `chat_ingress_shadow_mode`: boolean dual-run switch for validation mode (default `false`).
  - `chat_websocket_fallback_legacy_enabled`: explicit rollback flag for websocket EventSub chat subscriptions in bot runtime.
    When `chat_ingress_mode=webhook_conduit` and this flag is `false`, bot
    runtime websocket lifecycle is not required and workers avoid steady-state
    `/bot/config` polling churn.
  - `chat_ingress_guard_auto_fallback_enabled`: if `true`, degraded authoritative ingress can auto-switch mode to `websocket`.
- **PUT payload additions**
  - `eventsub_callback_override?: string`
  - `public_backend_origin?: string`
  - `chat_ingress_mode?: "websocket" | "webhook_conduit"`
  - `chat_ingress_shadow_mode?: boolean`
  - `chat_websocket_fallback_legacy_enabled?: boolean`
  - `chat_ingress_guard_auto_fallback_enabled?: boolean`

### `/system/health` ingress summary
- `eventsub.ingress_summary` includes compact operational counters:
  - callback status buckets (`2xx/4xx/5xx`),
  - signature failures,
  - last signature failure reason (`last_errors.signature_failure_reason_code`),
  - dedupe hits,
  - Send Chat API failure reason counters (`invalid_reply_parent_message_id`,
    `sender_token_mismatch`, `invalid_sender_broadcaster_relation`,
    `empty_or_invalid_message`, `unknown_400`, transient classes),
  - per-channel webhook command dispatch outcomes,
  - conduit shard status transitions,
  - last reply preflight failure reason (`last_errors.reply_preflight_failure_reason_code`),
  - recent callback throughput + last error timestamps/diagnostic snippets.
- `eventsub.authoritative_guard` includes degradation reasons (`missing_healthy_shards`, `callback_errors_spike`, `token_refresh_unhealthy`) and whether fallback was applied.
- The backend repair watcher evaluates these guard reasons every 60 seconds and
  applies least-disruptive repair in order: `reconcile`, `shard_repair`,
  `rebuild` (with cooldown + attempt caps to avoid loops).
- `eventsub.authoritative_guard.runtime_invariants` reports runtime checks for
  conduit shard-secret resolvability, sender token-subject resolvability, and
  bot token refresh-worker health (`token_refresh_healthy`,
  `token_refresh_health.*`).
- `eventsub.ingress_metrics.guard_repair_actions` and
  `eventsub.ingress_metrics.guard_repair_outcomes` provide structured watcher
  telemetry for each repair action and outcome.
- Recommended operator thresholds:
  - callback error threshold: 5 errors / 5 minutes,
  - minimum healthy shards: 1 (or expected shard count for larger deployments).

### Startup/operator log events (non-API)
- `INGRESS_STARTUP_HEALTH` (backend startup):
  - `ingress_mode`: effective startup mode (`webhook_conduit` or `websocket`).
  - `guard.startup_status`: startup check result (`ok`, `skipped`, `error`).
  - `guard.status`: normalized guard health (`healthy`, `degraded`, `skipped`).
  - `guard.reasons`: degradation/skip reasons (if any).
  - `conduit_shards`: startup conduit/shard snapshot:
    - `conduit_ids` (active conduit IDs seen in enabled chat subscriptions),
    - `assignment_count`,
    - `shards_total`,
    - `shards_healthy`,
    - `unresolved_assignment_count`.
  - `connected_channels`: startup channel connectivity summary:
    - `count`,
    - `channels` (channel names),
    - `channel_ids` (Twitch broadcaster IDs).
  - `workers`: startup worker alive flags:
    - `bot_token_refresh_worker_alive`,
    - `ingress_guard_repair_watcher_alive`.

- `CHANNEL_SYNC_SUMMARY` (bot runtime `sync_channels` cycle):
  - `considered`: number of backend channel rows evaluated this cycle.
  - `listened`: channels currently tracked/listened by bot before diff apply.
  - `rollback_websocket`: value of `chat_websocket_fallback_legacy_enabled`.
  - `added`: channels entering allowed set this cycle.
  - `removed`: channels leaving allowed set this cycle.

### EventSub callback URL reliability requirements
- Twitch EventSub webhook registration must use a **publicly reachable HTTPS callback URL**.
- Callback validation enforces:
  - Absolute URL.
  - `https://` scheme.
  - Exact path `/twitch/eventsub/callback`.
  - Public hostname (no localhost/internal aliases/private-only hosts).
- Valid override example: `https://api.example.com/twitch/eventsub/callback` (query/fragment are stripped during normalization).
- Troubleshooting: HTTP URLs or internal hosts like `http://backend/...` and `https://localhost/...` are rejected with HTTP 400 details during `/system/config` update.
- Callback source priority is:
  1. `eventsub_callback_override` (admin-managed runtime override)
  2. `public_backend_origin`
  3. `request_url` fallback
- Environment `TWITCH_EVENTSUB_CALLBACK` is now a **legacy bootstrap fallback** only; use `/system/config.eventsub_callback_override` for operational changes to avoid drift.
- Reconciliation output now includes structured callback warnings with source
  metadata so operators can see which source failed validation and why.
- Internal/private-only callback hostnames are rejected (examples:
  `backend`, `api`, `localhost`, private/loopback IPs, and bare private-only
  hostnames without public DNS suffixes).
- HTTP callback URLs are rejected during registration/reconciliation with:
  - `callback_url_not_https`
- Invalid callback configuration marks reconciliation as `degraded` and
  includes a remediation string describing how to fix the callback source.
- **Important**: relying on a `301` redirect from `http://...` to `https://...` is **not** considered a reliable or supported substitute for EventSub callback registration.

### EventSub pre-cutover operational verification
1. Confirm `public_backend_origin` is set to an HTTPS origin in system config:
   ```bash
   curl -sS http://localhost:7070/system/config | jq '{public_backend_origin, chat_ingress_mode, chat_ingress_shadow_mode}'
   ```
2. Confirm expected callback URL format before enabling conduit ingress:
   - `https://<public-backend-origin>/twitch/eventsub/callback`
3. Rollback playbook:
   - Enable `chat_websocket_fallback_legacy_enabled=true` for immediate websocket subscription restore.
   - If required, switch `chat_ingress_mode=websocket`.
   - Bot runtime treats websocket as rollback-only in authoritative mode: no
     websocket subscription reuse/recovery loops should be considered canonical.
   - Keep at least one rollback smoke check (`!request` or `!points`) active in
     staging for one additional release window before deleting websocket test
     fixtures.
   - If product decision explicitly confirms **no websocket rollback supported**,
     declare rollback EOL and remove both the smoke harness and websocket toggle
     path together.
   - Re-run conduit reconcile + callback validation before returning to authoritative mode.

## Authentication
| Method | Path | Description |
|--------|------|-------------|
| GET | `/auth/login` | Build a Twitch OAuth authorization URL for a channel, optionally preserving a `return_url`. |
| GET | `/auth/callback` | Twitch OAuth callback that stores the access token, marks the user as the channel owner, and seeds Favorites for new channels. |
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
- Front-end configuration for the Queue Manager lives in `queue_manager/public/queue_manager.js`, which expects `BACKEND_URL` (pointing to this API), `TWITCH_CLIENT_ID`, and optional `TWITCH_SCOPES` used for OAuth. The default scopes include `channel:bot channel:read:subscriptions channel:read:vips bits:read moderator:read:followers user:read:email`.

### `/auth/login`
- **Query parameters**
  - `channel` (required): Channel login used to embed into the OAuth `state` parameter.
  - `return_url` (optional): URL-encoded location to redirect to after authorization.
- **Response**: `{ "auth_url": "<twitch authorize url>" }` built with the configured Twitch client ID, redirect URI, and scopes from `TWITCH_SCOPES` (including bits and follower read permissions for pricing features).
- **Notes**: Fails with HTTP 500 if Twitch OAuth configuration is missing.

### `/auth/callback`
- **Query parameters**
  - `code`: Authorization code returned by Twitch.
  - `state`: Either a channel login string or JSON containing `{ "channel": <login>, "return_url": <url?> }`.
- **Behavior**
  - Exchanges `code` for an access token and requires the `channel:bot` scope.
  - Fetches the authenticated Twitch user and upserts `TwitchUser` plus the matching `ActiveChannel`, setting `authorized=True` and `owner_id` to the user.
  - When this flow creates a new channel record, it also creates a manual `Favorites` playlist and seeds these tracks (idempotently): `Night Drive` (FM-84), `Strobe` (deadmau5), and `LONG DISTANCE CALLING - Voices` (`https://www.youtube.com/watch?v=uWQQbQ9jqU4`).
  - Redirects to `return_url` when supplied and using an `http`/`https` scheme; otherwise returns `{ "success": true }`.

### `/auth/session`
- **Authentication**: `Authorization: Bearer <user OAuth token>`.
- **Behavior**
  - Validates the token against `https://id.twitch.tv/oauth2/validate` and refreshes the stored `TwitchUser` record.
  - Auto-registers the channel as owned when the token carries the `channel:bot` scope; newly auto-created channels are initialized with the same seeded manual `Favorites` playlist.
  - Sets the `admin_oauth_token` cookie (HTTP-only, `SameSite=lax`) for subsequent admin access, honoring Twitch `expires_in` when present.
- **Response**: `{ "login": "<twitch username>" }`.
- **Errors**: 401 when the bearer token is missing or invalid.

### `/auth/logout`
- **Behavior**: Removes the `admin_oauth_token` cookie and returns `{ "success": true }`.

## Bot
| Method | Path | Description |
|--------|------|-------------|
| GET | `/bot/config` | Retrieve the stored bot OAuth configuration (admin). |
| GET | `/bot/messages/catalog` | Return stable bot message levels/catalog metadata for admin UI rendering (admin). |
| PUT | `/bot/config` | Update bot settings such as scopes or enable flag (admin). |
| POST | `/bot/config/oauth` | Start the OAuth authorization flow for the bot account (admin). |
| GET | `/bot/config/oauth/callback` | Callback used by Twitch to finish the bot OAuth flow. |
| POST | `/bot/runtime/announcements` | Authoritative runtime event announcement endpoint (admin/bot worker). |

### `/bot/messages/catalog`
- **Authentication**: Requires admin authorization (`X-Admin-Token`, bearer token, or admin session cookie) via `require_token`.
- **Behavior**
  - Returns a stable contract with two top-level arrays: `levels` and `messages`.
  - `levels` is ordered by verbosity (`mute`, `normal`, `verbose`, `debug`) and includes short descriptions for each selector option.
  - `messages` includes metadata rows with `{ "id", "level", "group", "template_key", "description", "customizable" }`.
  - `customizable` is currently `true` for all default entries and is included so UI clients can evolve to per-message override controls without a breaking API change.
- **Response**: `{ "levels": [BotMessageLevelDetailOut], "messages": [BotMessageCatalogEntryOut] }`.

### `/bot/config/oauth/callback`
- **Behavior**
  - Returns a compact HTML page for popup-based OAuth flows.
  - Posts `{"type":"bot-oauth-complete","success":<bool>,"error"?:<string>}` to the opener/parent window so the admin panel can render a success or error banner.
  - Auto-closes the popup only on success; failure responses stay open so the error message remains visible for debugging.
  - Includes a short CTA in the popup (`"You may close this window."` on failures).

### `/bot/runtime/announcements`
- **Authentication**: Requires admin authorization (`require_token`) and is intended for backend-trusted bot runtime callers.
- **Request body**: `{ "channel": "<channel name>", "message_id": "<catalog id>", "template_vars": { ... } }`.
- **Behavior**
  - Validates `message_id` against the shared bot message catalog.
  - Builds the same reply contract used by webhook command replies (`template_key`, `template_vars`, `visibility`).
  - Sends through the authoritative Twitch Send Chat Message pipeline (`_send_eventsub_chat_reply`), which enforces the same auth-mode policy (`twitch_send_chat_auth_mode`) and channel `bot_message_level` threshold rules.
  - Suppresses chat sends when the channel is disconnected (`join_active = 0`) so runtime announcements remain silent until reconnect.
  - Returns send status metadata including `delivery_path: "send_chat_pipeline"` and a stable `reason_code` decision enum.
  - `reason_code` values:
    - `success`: Twitch Send Chat API accepted the message.
    - `suppressed_by_level`: channel message-level threshold or empty rendered template suppressed the send.
    - `suppressed_disconnected`: channel is disconnected (`join_active = 0`), so no runtime announcement is sent.
    - `preflight_rejected`: auth/header/payload preflight checks failed before calling Twitch Send Chat API.
    - `api_failure`: Twitch Send Chat API request failed (HTTP/request/timeout path).
- **Rollback note**
  - This endpoint is authoritative for non-chat runtime announcements.
  - Bot websocket `_send_message` remains rollback-only and should only be used when explicit fallback is required.

## Bot Logs
| Method | Path | Description |
|--------|------|-------------|
| POST | `/bot/logs` | Push a bot worker log event which is relayed to connected consoles (admin). |
| GET | `/bot/logs/stream` | Server-sent events stream of bot worker log messages (admin). |

## Channels
| Method | Path | Description |
|--------|------|-------------|
| GET | `/channels` | List all configured channels. |
| POST | `/channels` | Add a new channel (requires admin token) and initialize seeded Favorites. |
| PUT | `/channels/{channel}` | Update whether the bot should join a channel (admin). |
| GET | `/channels/{channel}/settings` | Retrieve channel configuration. |
| PUT | `/channels/{channel}/settings` | Update channel configuration (admin). |

The settings update endpoint accepts partial payloads and merges them with the
existing record so omitted fields keep their persisted values. Frontend callers
should prefer sending the full current state when possible or rely on the
backend merge behavior to avoid unintentionally resetting values to defaults.

`POST /channels` also ensures a manual `Favorites` playlist exists and seeds
missing defaults idempotently (`Night Drive`, `Strobe`, and
`LONG DISTANCE CALLING - Voices`), so repeated onboarding does not duplicate
tracks.

Channel settings include queue intake controls:

- `queue_closed` toggles whether any new requests are accepted.
- `overall_queue_cap` (0–100, default 100) auto-closes intake once pending requests reach the cap and emits a `queue.status` event.
- `nonpriority_queue_cap` (0–100, default 100) rejects new non-priority submissions when full while still allowing priority requests.
- `prio_only`, `max_requests_per_user`, `allow_bumps`, `other_flags`, and `max_prio_points` behave as before and are reflected in `settings.updated` events.
- `bot_message_level` controls how chatty the bot is in channel responses; allowed values are exactly `mute`, `normal`, `verbose`, and `debug` (default `normal`).
- Priority point pricing is configurable: `prio_follow_enabled`, `prio_raid_enabled`, `prio_bits_per_point`, `prio_gifts_per_point`, and per-tier fields (`prio_sub_tier1_points`, `prio_sub_tier2_points`, `prio_sub_tier3_points`) control how many points events grant. Reset bonuses (`prio_reset_points_tier1`, `prio_reset_points_tier2`, `prio_reset_points_tier3`, `prio_reset_points_vip`, `prio_reset_points_mod`) are awarded when the queue resets for a new stream. Use `free_mod_priority_requests` to allow moderators to request priority without spending points.

### Queue Manager unified bot dropdown API mapping
- Queue Manager uses a single state-aware dropdown model with options:
  `connect`, `disconnect`, `mute`, `normal`, `verbose`, `debug`.
- When a channel is disconnected (`join_active = 0`), the UI shows only
  `connect`. When connected (`join_active = 1`), it shows `disconnect` plus the
  four message levels.
- Endpoint wiring:
  - `connect` => `PUT /channels/{channel}?join_active=1`
  - `disconnect` => `PUT /channels/{channel}?join_active=0`
  - `mute|normal|verbose|debug` =>
    `PUT /channels/{channel}/settings` with
    `{ "bot_message_level": "<level>" }`
- Validation alignment:
  - `/channels/{channel}` accepts only `join_active` values `0` or `1`.
  - `/channels/{channel}/settings` keeps `bot_message_level` strict to enum
    values `mute|normal|verbose|debug`.
- Behavioral difference:
  - `disconnect` (`join_active=0`) is the stronger switch: channel command ingress subscriptions are removed in the bot runtime, conduit-backed `channel.chat.message` `EventSubscription` rows are locally marked `disabled`, and backend runtime announcements are suppressed.
  - `connect` (`join_active=1`) restores runtime connectivity and immediately triggers conduit reconciliation to refresh authoritative EventSub state for the channel.
  - `mute` (`bot_message_level="mute"`) keeps the bot connected for control-plane behavior but suppresses all chat message output by visibility policy.

## Steady-state runbooks

### Callback failure runbook
- **Description**: Recover EventSub callback delivery reliability when webhook
  status buckets indicate persistent `4xx` or `5xx`.
- **Dependencies**: Public HTTPS callback URL, callback secret resolution, and
  successful per-channel reconciliation endpoint access.
- **Code-customers**: On-call backend operators handling EventSub ingress
  incidents and channel onboarding failures.
- **Used variables/origin**:
  - `eventsub_callback_override` (runtime config override).
  - `public_backend_origin` (derived callback origin).
  - `eventsub.ingress_summary` callback counters from `GET /system/health`.

### Shard degradation runbook
- **Description**: Restore healthy conduit shard coverage when guard health
  reports `missing_healthy_shards`.
- **Dependencies**: Conduit reconcile APIs, guard thresholds in `/system/config`,
  and shard metadata persistence.
- **Code-customers**: Operators supervising authoritative webhook ingest during
  production incidents.
- **Used variables/origin**:
  - `chat_ingress_guard_min_healthy_shards`.
  - `chat_ingress_guard_auto_fallback_enabled`.
  - `/channels/{channel}/eventsub/health` shard assignment output.

### Send API 4xx/5xx handling runbook
- **Description**: Triage and remediate Send Chat Message API failures for
  webhook command replies.
- **Dependencies**: Valid bot identity token, preflight validation guard rails,
  and ingress summary reason counters.
- **Code-customers**: Bot/backend maintainers responsible for chat response SLOs.
- **Used variables/origin**:
  - `send_api_failure_count` / `reply_sent_count`.
  - Send failure reason counters (`sender_token_mismatch`,
    `invalid_reply_parent_message_id`, `unknown_400`, transient classes).
  - `event.message_id` from EventSub payload for reply threading.

### `invalid_signature` troubleshooting runbook
- **Description**: Recover conduit signature validation when active shard
  assignments cannot resolve usable shard secret material.
- **Dependencies**: `twitch_conduit_shards` secret state, conduit assignment
  linkage (`event_subscriptions.conduit_id` + `event_subscriptions.shard_id`),
  and callback signature headers.
- **Code-customers**: Operators handling EventSub `403 invalid_signature`.
- **Used variables/origin**:
  - `eventsub.ingress_summary.last_errors.signature_failure_reason_code`.
  - `eventsub.authoritative_guard.runtime_invariants.unresolved_assignments`.
  - Per-channel reconcile output from `/channels/{channel}/eventsub/health`.

### `preflight_token_subject_unresolved` troubleshooting runbook
- **Description**: Restore webhook reply preflight when sender token subject is
  not resolvable from Twitch `/oauth2/validate`.
- **Dependencies**: Valid bot user token from `/bot/config`, Twitch token
  validation reachability, and sender identity parity guard.
- **Code-customers**: Operators triaging skipped Send Chat API calls.
- **Used variables/origin**:
  - `eventsub.ingress_summary.last_errors.reply_preflight_failure_reason_code`.
  - `eventsub.authoritative_guard.runtime_invariants.sender_token_subject_resolvable`.
  - Bot sender token subject (`/oauth2/validate` `user_id`).

### Credential expiry remediation runbook
- **Description**: Recover from expired/invalid app or bot credentials impacting
  EventSub reconciliation or Send API calls.
- **Dependencies**: Twitch token validation, setup credentials, and bot OAuth
  callback flow. Startup backfills a missing `twitch_send_chat_auth_mode`
  `AppSetting` row to `app_token` so preflight uses app-auth headers by
  default.
- **Code-customers**: Platform operators rotating credentials or remediating auth outages.
- **Used variables/origin**:
  - Setup credentials in `/system/config` (`client_id`, `client_secret`).
  - Bot auth state in `/bot/config`.
  - Twitch `/oauth2/validate` responses for token subject/scope validation.

### `token_refresh_unhealthy` troubleshooting runbook
- **Description**: Recover backend-managed bot token refresh when authoritative
  webhook mode no longer receives healthy refresh outcomes.
- **Dependencies**: Stored `BotConfig` refresh credentials, Twitch OAuth token
  endpoint, and ingress guard runtime invariants.
- **Code-customers**: Operators responsible for webhook-conduit ingress
  continuity while websocket runtime is disabled.
- **Used variables/origin**:
  - `eventsub.authoritative_guard.runtime_invariants.token_refresh_healthy`.
  - `eventsub.authoritative_guard.runtime_invariants.token_refresh_health`.
  - `BotConfig.expires_at` with refresh deadline (`expires_at - 5m`).
- **Refresh cadence**:
  1. Backend refresh worker polls every 60s.
  2. Token refresh triggers at `T-5m` before `expires_at`.
  3. On success, backend persists `access_token`, `refresh_token`,
     `expires_at`, and refreshed `scopes`.
- **Failure alarms**:
  1. `token_refresh_healthy=false` in `/system/health`.
  2. `token_refresh_unhealthy` appears in
     `eventsub.authoritative_guard.reasons`.
  3. Backend logs emit `BOT_TOKEN_REFRESH_FAILED`.
- **Manual recovery flow**:
  1. Validate Twitch app credentials in `/system/config`.
  2. Re-run `/bot/config/oauth` authorization to reseed bot tokens.
  3. Confirm `/system/health` shows `token_refresh_healthy=true`.

## Archive: migration and compatibility notes
- `migrations/20240624_queue_caps.sql` introduced queue capacity columns and
  default backfill guidance for legacy deployments.
- `migrations/20260330_eventsub_conduits.sql` introduced EventSub dedupe and
  conduit/shard storage plus additive subscription linkage columns.
- Startup compatibility patching for legacy SQLite/prod schemas remains a
  historical rollout aid and should not be treated as a long-term migration
  substitute.

### Websocket-only cleanup ledger
When pruning websocket-only code/tests, update `docs/websocket_pruning_ledger.md`
in the same PR. Each entry must include removed modules/functions, replacement
path, deprecation decision date, rollback implications, an `unused code removed`
or `behavioral removal` classification, and a link to the archived rationale in
`docs/archive/websocket_deprecation_rationale.md`.

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
| GET | `/channels/{channel}/users` | Search or list users in a channel with pagination and owner/playlist exclusions. |
| POST | `/channels/{channel}/users` | Create or update a user record (admin). |
| GET | `/channels/{channel}/users/{user_id}` | Retrieve user details. |
| PUT | `/channels/{channel}/users/{user_id}` | Update user statistics such as priority points (admin). |
| DELETE | `/channels/{channel}/users/{user_id}` | Delete a user and cascade queue state tied to that user (admin). |
| GET | `/channels/{channel}/users/{user_id}/stream_state` | Get per-stream state like free subscriber priority usage. |
| PUT | `/channels/{channel}/users/{user_id}/points` | Set a user's priority points directly (admin). |

### `/channels/{channel}/users`
- **Query parameters**: `search` (optional substring match on usernames), `limit` (default 25, max 100), and `offset` (default 0).
- **Response**: `{ "total": <int>, "limit": <int>, "offset": <int>, "owner_login": "<channel owner login?>", "items": [UserOut] }`.
- **Behavior**: The endpoint excludes the channel owner and the playlist automation user (`twitch_id == "__playlist__"`) from both totals and items. Results order alphabetically by username and are safe for Queue Manager pagination controls.
- **Name repair**: When a stored username matches a numeric Twitch ID placeholder, the API attempts a best-effort Twitch Helix `/users?id=...` lookup (using the owner's OAuth token) and updates the persisted username to the resolved login.

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

**Streaming note**: The `/channels/{channel}/queue/stream` endpoint can stay idle for long periods when no queue changes occur.
Clients should disable read timeouts (for example, `aiohttp.ClientTimeout(sock_read=None)`) to prevent spurious disconnects and
"Queue stream error" logs while waiting for updates.

### `/channels/{channel}/queue/full`
- **Authentication**: None; public read access for overlays and dashboards.
- **Behavior**
  - Finds the current stream and orders requests by played status, priority flags, manual position, and request time.
  - Joins request rows with `Song` and `User` models and enriches users with VIP/subscriber status when available.
  - VIP/subscriber enrichment calls Twitch Helix (VIPs and subscriptions) with the channel owner's token; when scopes or tokens are missing, the role data falls back to empty sets without failing the request.
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
| POST | `/twitch/eventsub/callback` | Twitch EventSub webhook used for follows, raids, cheers, subscriptions, and `channel.chat.message` ingress (signature verified + message dedupe). |
| POST | `/channels/{channel}/events` | Log a channel event such as follows, subscriptions, or bits (channel key or admin). |
| GET | `/channels/{channel}/events` | Retrieve logged events with optional filtering by type and time. |
| GET | `/channels/{channel}/eventsub/health` | Inspect persisted and remote EventSub subscription status (admin/OAuth), including conduit/shard assignment coverage. |
| WS | `/channels/{channel}/events` | WebSocket stream that pushes queue and settings events for overlays. |

Certain events award priority points and are fed by EventSub subscriptions created with the channel owner's token:

- `bits` events grant 1 point for any cheer of at least 200 bits.
- Gifted subs (`gift_sub` events) grant 1 point for every 5 subscriptions gifted.
- Follows and raids each grant 1 point when enabled in channel settings.
- Direct subscriptions honor the configured tier multipliers.

### EventSub webhook + conduit reconciliation notes

- Existing webhook subscriptions for follows/raids/cheers/subscriptions are preserved.
- **Auth context for conduit APIs**: Conduit creation, shard patching, and conduit-mode EventSub subscription reconciliation use **Twitch app access token** authentication (client credentials), not user access tokens.
- **Identity mapping for conduit subscriptions**:
  - `broadcaster_user_id` = target channel Twitch user id.
  - `user_id` = shared bot account Twitch user id.
- In `webhook_conduit` mode (or when shadow mode is enabled), backend reconciliation now:
  - creates/reuses a Twitch conduit,
  - patches/reconciles shard transports to the webhook callback,
  - creates/reuses `channel.chat.message` subscriptions on conduit transport for each active channel.
- Conduit/subscription metadata is persisted in existing storage:
  - `twitch_conduits`, `twitch_conduit_shards`
  - `event_subscriptions` with `transport="conduit"` for `channel.chat.message`.
- `GET /channels/{channel}/eventsub/health?reconcile=true` runs reconciliation on-demand, then recomputes local subscriptions/conduit/shards/coverage before responding so top-level diagnostics match the new reconciliation state.

### EventSub conduit troubleshooting quick map

| HTTP status | Typical meaning | Operator action |
|---|---|---|
| `400` | Payload/condition/transport mismatch. | Re-check EventSub type payload schema, conduit transport block, and required condition fields (`broadcaster_user_id`, `user_id`). |
| `401` | Invalid token/client pairing. | Confirm app access token validity and that token `client_id` matches the configured Twitch client credentials. |
| `403` | Permission/authorization context mismatch. | Verify the request uses app-token context where required and that the authorized channel/bot relationship is valid for the requested subscription. |

### EventSub callback contract (operations)

- **Public reachability is required**: Twitch must be able to reach `POST /twitch/eventsub/callback` from the public internet over HTTPS. Private-only callback URLs (localhost/private VPC hostnames) will fail verification and delivery.
- **Required headers**:
  - `Twitch-Eventsub-Message-Id`
  - `Twitch-Eventsub-Message-Timestamp`
  - `Twitch-Eventsub-Message-Signature`
  - `Twitch-Eventsub-Message-Type`
- **Signature verification**:
  - The backend computes `HMAC_SHA256(secret, message_id + timestamp + raw_body)` and rejects mismatches with `403`.
  - For classic webhook payloads, the `secret` is taken from `event_subscriptions.secret` via `subscription.id`.
  - For conduit chat notifications (`subscription.type=channel.chat.message` and/or `subscription.transport.method=conduit`), the `secret` is resolved from persisted shard state (`twitch_conduit_shards.current_secret`, optional `previous_secret` during grace, and legacy mirror `transport_secret`) using `(conduit_id, shard_id)` derived from payload transport + metadata linkage.
  - Conduit reconciliation stores linkage metadata (`conduit_id`, `shard_id`, and transport details) on `event_subscriptions`; the `event_subscriptions.secret` field for conduit rows is treated as a schema-compatibility placeholder and is not authoritative for conduit notifications.
  - Migration-safe behavior: existing conduit rows that still contain legacy/random `event_subscriptions.secret` values are ignored during conduit notification signature verification.
  - If a conduit notification cannot resolve shard secret material, the callback returns explicit diagnostics with reason code (for example `missing_shard_secret`, `secret_lookup_mismatch`) and uses `503` for missing secret state so operators can distinguish config drift from invalid signatures; stale legacy signatures are surfaced with `stale_shard_secret`.
  - For conduit verification payloads (`verification_shape=conduit_shard`), the `secret` is taken from active persisted shard secret material using the `(conduit_shard.conduit_id, conduit_shard.shard)` pair.
- **Verification payload variants**:
  - Classic webhook verification payloads with `subscription` are supported.
  - Conduit shard verification payloads with `conduit_shard` and no `subscription` are supported.
  - Conduit payload validation requires both `conduit_shard.conduit_id` and `conduit_shard.shard`; missing fields are rejected with explicit reason codes (`missing_conduit_shard_field_conduit_id`, `missing_conduit_shard_field_shard`).
  - Legacy behavior that globally required `subscription.id` has been removed for verification callbacks.
  - After successful conduit verification or valid conduit chat notifications, the backend immediately re-queries Helix conduit shard status and updates persisted shard rows so health endpoints do not stay stuck in `webhook_callback_verification_pending`.
- **Idempotency / retries**:
  - `notification` deliveries are inserted into `eventsub_message_dedupe` keyed by `message_id` before processing.
  - Duplicate retries are acknowledged with success and skipped to prevent duplicate command/event execution.
- **Notification routing**:
  - Reward events (`follow`, `raid`, `bits`, `sub`, `gift_sub`) continue through existing event persistence/reward logic.
  - `channel.chat.message` notifications route to the dedicated webhook chat ingress handler.
  - In authoritative mode, webhook chat ingress now dispatches canonical commands to backend execution routines (queue mutations + existing queue/event notifications) and records structured outcomes: `executed`, `rejected`, or `error` with per-command reason codes (`parse_*`, `auth_*`, `business_rule_*`, `send_*`).
  - Canonical webhook execution now covers `request`, `playlist_request`, `prioritize`, `remove`, and `points` with queue mutation and reply semantics aligned to the prior websocket flow.
  - `request` command parity details: accepts canonical YouTube links (including `youtube.com`, `music.youtube.com`, and `youtu.be` forms), extracts embedded YouTube URLs from mixed text, resolves metadata through YouTube oEmbed when possible, and otherwise parses `Artist - Title` with the legacy `Unknown`-artist fallback for plain titles.
  - Webhook and websocket parsers now both call the shared command-resolution utility backed by the same canonical command source (`COMMANDS_FILE` / `bot/commands.yml` plus bot-parity defaults), so aliases like `req`, `sr`, `pp`, `undo`, and `del` resolve identically across both ingress paths.
  - Explicit no-op buckets are tracked for non-command chat lines, unknown aliases, policy-suppressed replies, and shadow-mode observe-only dispatch.
  - Unknown alias parsing now emits explicit diagnostics (`detail` + alias metadata) and includes optional user-feedback text in parse metadata for future reply strategies.
  - Authoritative webhook execution now produces chat replies through Twitch Send Chat Message API (`POST /helix/chat/messages`) with a stable reply contract (`status=success|error`, `template_key`, `template_vars`, optional `visibility` level).
  - Reply threading uses EventSub chat payload `event.message_id` only. The EventSub transport header message id is not used for `reply_parent_message_id`; when `event.message_id` is absent, reply threading is omitted.
  - Deterministic preflight validation now runs before Send Chat API calls to prevent guaranteed 400 retries: non-empty `broadcaster_id`, non-empty `sender_id`, message length `1-500`, and `sender_id` equality with bot token subject (`/oauth2/validate` `user_id`).
  - Preflight failures emit explicit reason codes and skip HTTP requests/retries.
  - Reply rendering uses the same message-catalog template semantics as websocket bot command responses.
  - Reply send logging now emits a sanitized structured decision record with destination `channel`, `decision_result`, and `reason_code` only (no payload/body text).
  - Command ingress logging now emits a sanitized structured decision record with `channel`, canonical command, EventSub message id, and normalized outcome category (`passed`, `suppressed`, `failed`, `rejected_non_command`) plus optional reason code.
  - Webhook comparison telemetry excludes raw message/payload text and retains only parse/outcome summary keys.
  - Channel `bot_message_level` thresholds still gate webhook replies exactly like bot/websocket mode (`mute` suppresses all, `normal`/`verbose`/`debug` thresholds allow <= level).
  - In `chat_ingress_shadow_mode=true`, webhook command notifications stay observe-only and do not send chat replies or mutate queue state.
  - `random_request` remains a websocket-only execution path for now and is marked as a cleanup candidate to remove once authoritative migration completes.
  - Legacy comparison-only command branches are marked deprecated/unused and reported with explicit reason codes until migrated.
- **Ingress authority behavior**:
  - `chat_ingress_mode=websocket` keeps websocket authoritative.
  - `chat_ingress_mode=webhook_conduit` makes webhook authoritative for chat ingress.
  - `chat_ingress_shadow_mode=true` forces webhook into shadow mode (parse + observe only): no queue/request mutation occurs, and logs include what would have executed.

### EventSub callback runbook

1. Ensure DNS + TLS expose the backend origin publicly and the callback route is reachable from Twitch.
2. Validate stored callback URL and subscription secrets via admin APIs/DB before rotating Twitch subscriptions.
3. Keep webhook secrets out of logs and source control; rotate by recreating or patching subscriptions and updating `event_subscriptions.secret`.
4. Monitor callback logs for:
   - signature failures,
   - unknown subscription IDs,
   - conduit shard secret resolution failures (`missing_conduit_shard_field_conduit_id`, `missing_conduit_shard_field_shard`, `missing_shard_secret`, `secret_lookup_mismatch`, `unknown_conduit_shard`),
   - dedupe hits (retry storms),
   - reply send failures / suppressions (`send_api_failure_count`, `reply_suppressed_count`),
   - webhook/websocket comparison deltas while shadow mode is enabled.
5. During cutover:
   - enable `chat_ingress_shadow_mode=true` first and confirm comparison logs are stable,
   - switch `chat_ingress_mode=webhook_conduit`,
   - disable shadow mode after validating authoritative webhook behavior.
6. Operator verification endpoints:
   - Call `GET /system/health` and confirm global conduit/shard coverage reports healthy.
   - Call `GET /channels/{channel}/eventsub/health?reconcile=true` and confirm per-channel subscriptions/conduit/shard coverage reconcile successfully.
7. Post-deploy invariant verification commands:
   - `curl -sS http://localhost:7070/system/health | jq '.eventsub.authoritative_guard.runtime_invariants'`
   - `curl -sS http://localhost:7070/system/health | jq '.eventsub.ingress_summary.last_errors | {signature_failure_reason_code, reply_preflight_failure_reason_code}'`
   - Expected healthy values:
     - `conduit_signature_secret_resolvable=true`
     - `sender_token_subject_resolvable=true`
     - `unresolved_assignment_count=0`
8. Legacy bug note:
   - If you previously saw `subscription id missing` on valid conduit verification callbacks, upgrade to this patch level; the callback now branches verification shape before subscription lookup.

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
- `settings.updated` — payload mirroring the settings fields (`max_requests_per_user`, `prio_only`, `queue_closed`, `allow_bumps`, `other_flags`, `max_prio_points`, `overall_queue_cap`, `nonpriority_queue_cap`, `bot_message_level`) after merges.
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
