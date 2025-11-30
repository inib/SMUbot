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
        """Layout and badge CSS hooks should exist for regression coverage."""

        html = Path("queue_manager/public/index.html").read_text(encoding="utf-8")
        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        css = Path("queue_manager/public/style.css").read_text(encoding="utf-8")

        self.assertIn('class="users-list"', html)
        self.assertIn('resolveUserBadge', script)
        self.assertIn('.user-role-badge', css)


if __name__ == "__main__":
    unittest.main()
