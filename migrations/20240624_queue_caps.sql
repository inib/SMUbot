BEGIN TRANSACTION;

ALTER TABLE channel_settings ADD COLUMN overall_queue_cap INTEGER NOT NULL DEFAULT 100;
ALTER TABLE channel_settings ADD COLUMN nonpriority_queue_cap INTEGER NOT NULL DEFAULT 100;

UPDATE channel_settings
SET overall_queue_cap = 100
WHERE overall_queue_cap IS NULL;

UPDATE channel_settings
SET nonpriority_queue_cap = 100
WHERE nonpriority_queue_cap IS NULL;

COMMIT;
