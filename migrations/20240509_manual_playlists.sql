BEGIN TRANSACTION;

CREATE TABLE playlists_new (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL REFERENCES active_channels(id) ON DELETE CASCADE,
    title VARCHAR NOT NULL,
    description TEXT,
    playlist_id VARCHAR,
    url TEXT,
    source VARCHAR NOT NULL DEFAULT 'youtube',
    visibility VARCHAR NOT NULL DEFAULT 'public',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    CONSTRAINT uq_playlists_channel_playlist UNIQUE (channel_id, playlist_id)
);

INSERT INTO playlists_new (id, channel_id, title, description, playlist_id, url, source, visibility, created_at, updated_at)
SELECT id, channel_id, title, NULL, playlist_id, url, 'youtube', visibility, created_at, updated_at
FROM playlists;

DROP TABLE playlists;
ALTER TABLE playlists_new RENAME TO playlists;

CREATE TABLE playlist_items_new (
    id INTEGER PRIMARY KEY,
    playlist_id INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
    position INTEGER NOT NULL DEFAULT 0,
    video_id VARCHAR,
    title VARCHAR NOT NULL,
    artist VARCHAR,
    duration_seconds INTEGER,
    url TEXT,
    CONSTRAINT uq_playlist_item_video UNIQUE (playlist_id, video_id)
);

INSERT INTO playlist_items_new (id, playlist_id, position, video_id, title, artist, duration_seconds, url)
SELECT id, playlist_id, position, video_id, title, artist, duration_seconds, url
FROM playlist_items;

DROP TABLE playlist_items;
ALTER TABLE playlist_items_new RENAME TO playlist_items;

COMMIT;
