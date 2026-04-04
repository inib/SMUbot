# Websocket-only pruning changelog ledger

Use this ledger whenever websocket-only code/tests are removed.

## Required fields per entry
- **removed modules/functions**
- **replacement path**
- **deprecation decision date** (YYYY-MM-DD)
- **rollback implications**
- **classification** (`unused code removed` or `behavioral removal`)
- **archived rationale link**

## Entries

### 2026-04-04 — Verify mute-level silence on backend runtime announcements
- **removed modules/functions**: none (verification + automated coverage update)
- **replacement path**: `POST /bot/runtime/announcements` -> `backend_app._send_catalog_announcement` -> `_send_eventsub_chat_reply` mute suppression gate.
- **deprecation decision date**: 2026-04-04
- **rollback implications**: none; this entry adds a regression guard to ensure muted channels do not emit chat via Send Chat API.
- **classification**: unused code removed (tracking/coverage only; no runtime deletion)
- **archived rationale link**: [Archived websocket deprecation rationale](./archive/websocket_deprecation_rationale.md)

### 2026-04-04 — Suppress runtime announcements when bot is disconnected
- **removed modules/functions**: none (behavioral guard + automated coverage update)
- **replacement path**: `POST /bot/runtime/announcements` now short-circuits in `backend_app._send_catalog_announcement` when `join_active=0`, returning `reason_code=suppressed_disconnected`.
- **deprecation decision date**: 2026-04-04
- **rollback implications**: behavioral tightening; rollback would restore announcement sends even when the Queue Manager connect/disconnect control is set to disconnected.
- **classification**: behavioral removal
- **archived rationale link**: [Archived websocket deprecation rationale](./archive/websocket_deprecation_rationale.md)

### 2026-04-04 — Remove websocket runtime announcement rollback fallback/gate
- **removed modules/functions**: `bot/bot_app.py` `SongBot._announce_backend_runtime_event` websocket fallback branch, `SongBot` queue/event polling announcers (`check_played`, `check_bumps`, `check_queue_position_changes`, `announce_event`), and `BotService._requires_runtime` gate branch from `BotService.run`.
- **replacement path**: backend-authoritative catalog announcement dispatch in `backend_app._send_catalog_announcement`, called directly from `move_request`, `mark_played`, and `_persist_channel_event`.
- **deprecation decision date**: 2026-04-04
- **rollback implications**: behavioral removal; rollback requires restoring bot runtime non-chat polling producers and websocket fallback branch in `_announce_backend_runtime_event`.
- **classification**: behavioral removal
- **archived rationale link**: [Archived websocket deprecation rationale](./archive/websocket_deprecation_rationale.md)

### 2026-04-04 — Remove legacy `check_played` pre-rendered `msg` assignments
- **removed modules/functions**: `bot/bot_app.py` local `msg` assignments in `BotService.check_played` (legacy websocket path)
- **replacement path**: `BotService.check_played` now routes only through `_send_catalog_message(..., template_vars=...)`
- **deprecation decision date**: 2026-04-04
- **rollback implications**: low risk and non-behavioral; rollback by restoring direct `self.messages[...]` string formatting in legacy path.
- **classification**: unused code removed
- **archived rationale link**: [Archived websocket deprecation rationale](./archive/websocket_deprecation_rationale.md)

### 2026-04-02 — Remove BotService duplicate command parser/handlers
- **removed modules/functions**: `bot/bot_app.py` `BotService.event_message`, `BotService.handle_request`, `BotService.handle_prioritize`, `BotService.handle_points`, `BotService.handle_remove`, `BotService.handle_archive`
- **replacement path**: `SongBot.event_message` + `bot/chat_command_core.py` shared parser/dispatcher; webhook command dispatch in `backend_app._dispatch_eventsub_chat_command`
- **deprecation decision date**: 2026-04-02
- **rollback implications**: no runtime impact expected; BotService never owned authoritative Twitch chat execution. Rollback by restoring deleted BotService compatibility methods.
- **classification**: unused code removed
- **archived rationale link**: [Archived websocket deprecation rationale](./archive/websocket_deprecation_rationale.md)

### 2026-04-02 — Process baseline entry
- **removed modules/functions**: none (documentation/process update only)
- **replacement path**: n/a
- **deprecation decision date**: 2026-04-02
- **rollback implications**: none
- **classification**: unused code removed (tracking baseline; no runtime deletion yet)
- **archived rationale link**: [Archived websocket deprecation rationale](./archive/websocket_deprecation_rationale.md)

## Entry template

```md
### YYYY-MM-DD — <short title>
- **removed modules/functions**: <module/function list>
- **replacement path**: <new authoritative path>
- **deprecation decision date**: YYYY-MM-DD
- **rollback implications**: <operator impact and recovery path>
- **classification**: unused code removed | behavioral removal
- **archived rationale link**: [name](./archive/websocket_deprecation_rationale.md)
```
