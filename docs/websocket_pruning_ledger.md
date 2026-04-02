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
