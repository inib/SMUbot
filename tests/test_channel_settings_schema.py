import os
import sys
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

os.makedirs("/data", exist_ok=True)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import backend_app


class ChannelSettingsSchemaBootstrapTests(unittest.TestCase):
    def test_ensure_channel_settings_schema_backfills_legacy_and_new_columns(self) -> None:
        """Schema bootstrap keeps legacy rows valid while adding new settings columns."""

        db_fd, db_path = tempfile.mkstemp(prefix="channel-settings-", suffix=".sqlite3")
        os.close(db_fd)
        legacy_engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})

        original_engine = backend_app.engine
        original_session_local = backend_app.SessionLocal
        backend_app.engine = legacy_engine
        backend_app.SessionLocal = sessionmaker(bind=legacy_engine, autoflush=False, autocommit=False)

        try:
            with legacy_engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE channel_settings (
                            id INTEGER PRIMARY KEY,
                            channel_id INTEGER NOT NULL,
                            max_requests_per_user INTEGER DEFAULT 3,
                            overall_queue_cap INTEGER,
                            nonpriority_queue_cap INTEGER,
                            bot_message_level VARCHAR
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO channel_settings (
                            id, channel_id, max_requests_per_user,
                            overall_queue_cap, nonpriority_queue_cap, bot_message_level
                        ) VALUES (1, 101, 3, NULL, NULL, '')
                        """
                    )
                )

            backend_app.ensure_channel_settings_schema()

            inspector = inspect(legacy_engine)
            columns = {column["name"] for column in inspector.get_columns("channel_settings")}
            self.assertIn("overall_queue_cap", columns)
            self.assertIn("nonpriority_queue_cap", columns)
            self.assertIn("full_auto_priority_mode", columns)
            self.assertIn("prio_follow_enabled", columns)
            self.assertIn("bot_message_level", columns)

            with legacy_engine.connect() as conn:
                row = conn.execute(
                    text(
                        """
                        SELECT
                            overall_queue_cap,
                            nonpriority_queue_cap,
                            full_auto_priority_mode,
                            prio_follow_enabled,
                            bot_message_level
                        FROM channel_settings
                        WHERE id = 1
                        """
                    )
                ).mappings().one()

            self.assertEqual(row["overall_queue_cap"], 100)
            self.assertEqual(row["nonpriority_queue_cap"], 100)
            self.assertEqual(row["full_auto_priority_mode"], 0)
            self.assertEqual(row["prio_follow_enabled"], 1)
            self.assertEqual(row["bot_message_level"], "normal")
        finally:
            backend_app.engine = original_engine
            backend_app.SessionLocal = original_session_local
            legacy_engine.dispose()
            os.remove(db_path)
