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

    def test_resolve_user_badge_has_no_duplicate_badge_declaration(self) -> None:
        """resolveUserBadge should not re-declare `badge` (regression for duplicate const parsing failures)."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        function_start = script.find("function resolveUserBadge(user) {")
        self.assertNotEqual(function_start, -1, "resolveUserBadge definition should exist")

        function_end = script.find("\n}\n\n/**", function_start)
        self.assertNotEqual(function_end, -1, "resolveUserBadge closing block should be discoverable")

        function_block = script[function_start:function_end]
        self.assertLessEqual(
            function_block.count("const badge ="),
            1,
            "resolveUserBadge should not contain duplicate `const badge` declarations",
        )

    def test_header_bot_dropdown_hook_exists(self) -> None:
        """Queue Manager header should expose the shared bot-control dropdown mount."""

        html = Path("queue_manager/public/index.html").read_text(encoding="utf-8")
        self.assertIn('id="bot-control-host"', html)

    def test_bot_control_models_include_connection_and_levels(self) -> None:
        """Bot controls should keep explicit connect/disconnect actions and message levels."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        self.assertIn("const BOT_CONNECTION_OPTIONS = [", script)
        self.assertIn("{ value: 'connect'", script)
        self.assertIn("{ value: 'disconnect'", script)
        self.assertIn("const BOT_MESSAGE_LEVEL_OPTIONS = [", script)
        self.assertIn("{ value: 'mute'", script)
        self.assertIn("{ value: 'normal'", script)
        self.assertIn("{ value: 'verbose'", script)
        self.assertIn("{ value: 'debug'", script)

    def test_settings_bot_control_group_and_virtual_connection_row_exist(self) -> None:
        """Settings should expose a dedicated Bot Control section with join/part and message-level rows."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        self.assertIn("bot: 'Bot Control'", script)
        self.assertIn("['main', 'bot', 'caps', 'earn', 'followers', 'reset', 'experimental', 'other']", script)
        self.assertIn("const BOT_CONNECTION_SETTING_KEY = '__bot_connection_state';", script)
        self.assertIn("type: 'bot-connection'", script)
        self.assertIn("type: 'bot-message-level'", script)
        self.assertIn("group: 'bot'", script)

    def test_bot_connection_toggle_maps_join_part_api_path(self) -> None:
        """Bot connection toggle should map to channel-status endpoint with join_active query params."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        self.assertIn("const next = input.checked ? 'connect' : 'disconnect';", script)
        self.assertIn("const option = BOT_CONNECTION_OPTIONS.find(entry => entry.value === selectionValue);", script)
        self.assertIn("`${API}/channels/${encodedChannel}?join_active=${option.joinActive}`", script)
        self.assertIn("type === 'bot-connection' || type === 'bot-message-level'", script)
        self.assertIn("await applyBotConnectionSelection(next, { refreshSettingsView: false });", script)
        self.assertIn("await fetchSettings();", script)

    def test_settings_bot_section_renders_connection_toggle_pattern(self) -> None:
        """Bot section should render connection as the shared toggle-switch pattern with connection labels."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        self.assertIn("input.setAttribute('aria-label', meta.label || 'Bot connection');", script)
        self.assertIn("switchLabel.className = 'toggle-switch';", script)
        self.assertIn("slider.className = 'toggle-slider';", script)
        self.assertIn("state.className = 'toggle-state';", script)
        self.assertIn("const onLabel = meta.onLabel || 'Connected';", script)
        self.assertIn("const offLabel = meta.offLabel || 'Disconnected';", script)

    def test_verbosity_control_remains_mapped_to_bot_message_level_setting(self) -> None:
        """Verbosity control should still persist via the bot_message_level settings payload."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        self.assertIn("body: JSON.stringify({ bot_message_level: selectionValue })", script)
        self.assertIn("bot_message_level: {", script)
        self.assertIn("type: 'bot-message-level'", script)

    def test_disconnected_header_control_disables_verbosity_dropdown(self) -> None:
        """Header verbosity dropdown should stay disabled whenever join_active is false."""

        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")
        self.assertIn("disabled: !channelInfo.join_active,", script)
        self.assertIn("const disabledTitle = 'Connect bot in Settings to change verbosity.';", script)
        self.assertIn("if (!channelInfo.join_active) {", script)
        self.assertIn("controlHost.title = disabledTitle;", script)

    def test_quick_controls_responsive_wrap_hooks_exist(self) -> None:
        """Quick-controls should wrap by default and stay width-constrained across tabs."""

        css = Path("queue_manager/public/style.css").read_text(encoding="utf-8")

        self.assertIn('.queue-layout{display:flex;flex-direction:column;gap:24px;min-width:0}', css)
        self.assertIn('.queue-column{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;gap:12px}', css)
        self.assertIn('.quick-controls{display:flex;align-items:center;gap:6px;flex-wrap:wrap;overflow-x:visible;max-width:100%;min-width:0;', css)
        self.assertIn('.quick-controls>*{min-width:0}', css)
        self.assertIn('@media (min-width: 1200px){', css)
        self.assertIn('.quick-controls{flex-wrap:nowrap;overflow-x:auto;max-width:100%}', css)

    def test_quick_controls_container_and_setting_keys_exist(self) -> None:
        """Queue tab should expose quick controls wired to key queue setting toggles."""

        html = Path("queue_manager/public/index.html").read_text(encoding="utf-8")
        script = Path("queue_manager/public/queue_manager.js").read_text(encoding="utf-8")

        self.assertIn('id="quick-controls"', html)
        self.assertIn("const QUICK_CONTROL_KEYS = ['queue_closed', 'prio_only', 'allow_bumps', 'full_auto_priority_mode'];", script)


if __name__ == "__main__":
    unittest.main()
