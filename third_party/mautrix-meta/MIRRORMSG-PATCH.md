# MirrorMsg patch to mautrix-meta v0.2604.0

This copy of mautrix-meta (AGPL-3.0) is the v0.2604.0 release with one change,
made for MirrorMsg on 2026-10-06:

**Support for `executeFirstBlockForSyncTransactionV4`.** In September 2026
Facebook began sending its sync-transaction "first block" rows in a V4 form.
v0.2604.0 doesn't know that row, so it never learned the cursor for database 1,
its first sync request after connecting went unanswered, and the client never
became ready ("failed to ensure db 1 is synced ... timeout waiting for response").

The change backports upstream commits b3325bb and 9f8f0d1 (2026-09-10) onto
v0.2604.0's code:

- `pkg/messagix/table/sync_groups.go`: the V4 row type, and an accessor that
  returns V4 rows (or old-style rows converted to V4).
- `pkg/messagix/table/table.go`: the V4 field and its stored-procedure name.
- `pkg/messagix/socket/database.go`: the sequence id V4 carries.
- `pkg/messagix/syncManager.go`, `configs.go`, `events.go`,
  `methods/methods.go`: read sync transactions through the accessor.

Remove this copy (and the `replace` line in MirrorMsg's go.mod) when moving to an
upstream mautrix-meta release that includes the change.
