import unittest
from pathlib import Path


class PublicPlaylistsUiTests(unittest.TestCase):
    """Guard public playlist markup/state hooks used by the static queue web UI."""

    def test_playlist_toggle_button_markup_and_labels_exist(self) -> None:
        """Public playlist cards should expose accessible show/hide toggle controls."""

        script = Path("web/public/app.js").read_text(encoding="utf-8")
        self.assertIn("public-playlist__toggle", script)
        self.assertIn("'Show songs'", script)
        self.assertIn("'Hide songs'", script)
        self.assertIn("aria-expanded", script)
        self.assertIn("aria-controls", script)
        self.assertIn("itemsContainer.hidden =", script)

    def test_playlist_toggle_state_cache_is_present(self) -> None:
        """Queue context should keep per-playlist expansion state across rerenders."""

        script = Path("web/public/app.js").read_text(encoding="utf-8")
        self.assertIn("playlistExpandState: new Map()", script)
        self.assertIn("isPublicPlaylistExpanded", script)
        self.assertIn("setPublicPlaylistExpanded", script)


if __name__ == "__main__":
    unittest.main()
