-- Add conduit shard secret rotation fields and backfill current_secret.
ALTER TABLE twitch_conduit_shards ADD COLUMN current_secret TEXT;
ALTER TABLE twitch_conduit_shards ADD COLUMN previous_secret TEXT;
ALTER TABLE twitch_conduit_shards ADD COLUMN previous_secret_valid_until DATETIME;

UPDATE twitch_conduit_shards
SET current_secret = transport_secret
WHERE (current_secret IS NULL OR current_secret = '')
  AND transport_secret IS NOT NULL
  AND transport_secret != '';
