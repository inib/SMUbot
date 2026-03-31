BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS eventsub_message_dedupe (
    id INTEGER PRIMARY KEY,
    message_id VARCHAR NOT NULL,
    received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_eventsub_message_dedupe_message_id UNIQUE (message_id)
);

CREATE TABLE IF NOT EXISTS twitch_conduits (
    id INTEGER PRIMARY KEY,
    conduit_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'pending',
    last_sync_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_twitch_conduits_conduit_id UNIQUE (conduit_id)
);

CREATE TABLE IF NOT EXISTS twitch_conduit_shards (
    id INTEGER PRIMARY KEY,
    conduit_fk INTEGER NOT NULL REFERENCES twitch_conduits(id) ON DELETE CASCADE,
    shard_id VARCHAR NOT NULL,
    transport_callback TEXT,
    transport_secret TEXT,
    status VARCHAR NOT NULL DEFAULT 'pending',
    last_sync_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_twitch_conduit_shards UNIQUE (conduit_fk, shard_id)
);

ALTER TABLE event_subscriptions ADD COLUMN conduit_id VARCHAR;
ALTER TABLE event_subscriptions ADD COLUMN shard_id VARCHAR;

COMMIT;
