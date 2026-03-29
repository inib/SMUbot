import unittest
from pathlib import Path


class QueueManagerUsersUiTests(unittest.TestCase):
    """Ensure the Queue Manager Users tab exposes search and pagination controls."""

    def test_users_controls_exist(self) -> None:
        """Search input and paging buttons should exist in the static HTML."""

        html = Path("queue_manager/public/index.html").read_text(encoding="utf-8")
        self.assertIn('id="user-search"', html)
        self.assertIn('id="users-prev"', html)
        self.assertIn('id="users-next"', html)
        self.assertIn('id="users-page"', html)

    def test_user_page_size_constant(self) -> None:
        """User page size should stay aligned with new pagination defaults."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        self.assertIn('const USER_PAGE_SIZE = 25;', script)

    def test_users_layout_and_badge_hooks(self) -> None:
        """Layout, badge hooks, and users legend wiring should stay in place."""

        html = Path("queue_manager/public/index.html").read_text(encoding="utf-8")
        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        css = Path("queue_manager/public/style.css").read_text(encoding="utf-8")

        self.assertIn('class="users-list"', html)
        self.assertIn('id="users-legend"', html)
        self.assertIn('resolveUserBadge', script)
        self.assertIn('USER_BADGE_DEFINITIONS', script)
        self.assertIn('renderUsersLegend', script)
        self.assertIn('.user-role-badge', css)
        self.assertIn('.users-legend', css)

    def test_header_bot_dropdown_hook_exists(self) -> None:
        """Queue Manager header should expose the shared bot-control dropdown mount."""

        html = Path("queue_manager/public/index.html").read_text(encoding="utf-8")
        self.assertIn('id="bot-control-host"', html)

    def test_bot_control_model_includes_connection_and_levels(self) -> None:
        """Unified dropdown model should include connect/disconnect and all message levels."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        self.assertIn("const BOT_CONTROL_OPTION_MODEL =", script)
        self.assertIn("{ value: 'connect'", script)
        self.assertIn("{ value: 'disconnect'", script)
        self.assertIn("{ value: 'mute'", script)
        self.assertIn("{ value: 'normal'", script)
        self.assertIn("{ value: 'verbose'", script)
        self.assertIn("{ value: 'debug'", script)

    def test_quick_controls_container_and_setting_keys_exist(self) -> None:
        """Queue tab should expose quick controls wired to key queue setting toggles."""

        html = Path("queue_manager/public/index.html").read_text(encoding="utf-8")
        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")

        self.assertIn('id="quick-controls"', html)
        self.assertIn("const QUICK_CONTROL_KEYS = ['queue_closed', 'prio_only', 'allow_bumps', 'full_auto_priority_mode'];", script)


if __name__ == "__main__":
    unittest.main()
