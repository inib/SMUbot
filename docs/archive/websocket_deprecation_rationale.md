# Archived deprecation rationale: websocket-only command/runtime cleanup

This document is the archived rationale referenced by the websocket-pruning changelog ledger.

## Context
- The canonical chat ingress path is `webhook_conduit`.
- Websocket ingress and websocket-only routines are retained only as explicit rollback tooling during migration windows.

## Deprecation decision criteria
A websocket-only module/function/test can be removed when all items below are true:
1. Product confirms rollback EOL for the targeted behavior.
2. Authoritative webhook/conduit path has equivalent behavior coverage.
3. At least one staging release window has verified rollback smoke expectations (if rollback is still in scope).
4. Runbooks and API docs are updated in the same PR.

## Rollback implications template
When recording removals in the ledger, include:
- operator-visible behavior changes,
- required config toggles for emergency fallback,
- explicit statement whether `chat_websocket_fallback_legacy_enabled` still restores the removed behavior.

## Current cleanup candidates (tracked, not removed)
- `random_request` webhook execution parity gap (currently rejected with a websocket-only cleanup marker).

## Related ledger
- `docs/websocket_pruning_ledger.md`
