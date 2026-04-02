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
