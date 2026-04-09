CREATE TABLE IF NOT EXISTS channel_bot_messages (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL REFERENCES active_channels(id) ON DELETE CASCADE,
    message_id VARCHAR NOT NULL,
    template TEXT NOT NULL,
    enabled BOOLEAN DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_channel_bot_message UNIQUE (channel_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_bot_messages_channel_id ON channel_bot_messages(channel_id);
