import unittest
from fastapi.testclient import TestClient

import backend_app


def _wipe_user_db() -> None:
    """Clear queue-related tables to ensure user paging tests start clean.

    Dependencies: relies on `backend_app.SessionLocal` for a database session.
    Code customers: UsersApiTests setUp/tearDown run this reset to avoid data
    overlap. Variables originate from ORM models bound to the shared engine.
    """

    db = backend_app.SessionLocal()
    try:
        for model in [
            backend_app.Request,
            backend_app.Song,
            backend_app.User,
            backend_app.StreamSession,
            backend_app.PlaylistItem,
            backend_app.PlaylistKeyword,
            backend_app.Playlist,
            backend_app.ChannelSettings,
            backend_app.ChannelModerator,
            backend_app.ActiveChannel,
            backend_app.TwitchUser,
        ]:
            db.query(model).delete()
        db.query(backend_app.AppSetting).delete()
        db.add(backend_app.AppSetting(key="setup_complete", value="1"))
        db.commit()
    finally:
        db.close()
    backend_app.settings_store.invalidate()


def _seed_channel_with_users(db: backend_app.Session, *, total: int) -> backend_app.ActiveChannel:
    """Seed a channel with owner, playlist, and numbered requesters.

    Dependencies: open SQLAlchemy session shared with FastAPI. Code customers:
    user API tests that verify pagination and exclusion filters. Variables
    originate from deterministic usernames (`user00`, `user01`, ...) and
    constants like PLAYLIST_TWITCH_USER to align with backend filtering rules.
    """

    owner = backend_app.TwitchUser(
        twitch_id="owner",
        username="owner",
        access_token="",
        refresh_token="",
        scopes="",
    )
    db.add(owner)
    db.commit()
    db.refresh(owner)

    channel = backend_app.ActiveChannel(
        channel_id="chan-1",
        channel_name="itsalpine",
        owner_id=owner.id,
        authorized=True,
    )
    db.add(channel)
    db.commit()
    db.refresh(channel)

    owner_user = backend_app.User(
        channel_id=channel.id,
        twitch_id=owner.twitch_id,
        username=owner.username,
    )
    playlist_user = backend_app.User(
        channel_id=channel.id,
        twitch_id=backend_app.PLAYLIST_TWITCH_ID,
        username="Playlist",
    )
    db.add_all([owner_user, playlist_user])

    for idx in range(total):
        db.add(
            backend_app.User(
                channel_id=channel.id,
                twitch_id=f"user-{idx}",
                username=f"user{idx:02d}",
                prio_points=idx,
            )
        )

    db.commit()
    return channel


def _seed_role_accounts(db: backend_app.Session, channel: backend_app.ActiveChannel) -> dict[str, str]:
    """Attach moderator, VIP, and subscriber fixtures to a channel.

    Dependencies: relies on the provided SQLAlchemy session and ``channel``
    row, creating TwitchUser and User rows aligned to the same Twitch IDs.
    Code customers: role assertion tests that need deterministic badges.
    Used variables/origin: static usernames for each role and the
    ChannelModerator link table for moderator mapping.
    """

    mod = backend_app.TwitchUser(twitch_id="mod-1", username="modder", access_token="", refresh_token="", scopes="")
    vip = backend_app.TwitchUser(twitch_id="vip-1", username="vippy", access_token="", refresh_token="", scopes="")
    sub = backend_app.TwitchUser(twitch_id="sub-1", username="subby", access_token="", refresh_token="", scopes="")
    db.add_all([mod, vip, sub])
    db.commit()
    db.refresh(mod)
    db.refresh(vip)
    db.refresh(sub)

    db.add(backend_app.ChannelModerator(channel_id=channel.id, user_id=mod.id))
    db.add_all([
        backend_app.User(channel_id=channel.id, twitch_id=mod.twitch_id, username=mod.username),
        backend_app.User(channel_id=channel.id, twitch_id=vip.twitch_id, username=vip.username),
        backend_app.User(channel_id=channel.id, twitch_id=sub.twitch_id, username=sub.username),
    ])
    db.commit()

    return {"mod": mod.username, "vip": vip.username, "sub": sub.username}


class UsersApiTests(unittest.TestCase):
    """Validate user listing search, pagination, and exclusions."""

    def setUp(self) -> None:
        _wipe_user_db()
        self.client = TestClient(backend_app.app)

    def tearDown(self) -> None:
        self.client.close()
        _wipe_user_db()

    def test_users_exclude_owner_and_playlist(self) -> None:
        """Owner and playlist helpers should not appear in paged results."""

        db = backend_app.SessionLocal()
        try:
            channel = _seed_channel_with_users(db, total=3)
            channel_name = channel.channel_name
        finally:
            db.close()

        resp = self.client.get(f"/channels/{channel_name}/users", params={"limit": 50, "offset": 0})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        usernames = [row["username"] for row in payload["items"]]

        self.assertNotIn("owner", usernames)
        self.assertNotIn("Playlist", usernames)
        self.assertEqual(payload["total"], 3)

    def test_users_pagination_and_search(self) -> None:
        """Pagination should honor limit/offset and combine with search terms."""

        db = backend_app.SessionLocal()
        try:
            channel = _seed_channel_with_users(db, total=30)
            channel_name = channel.channel_name
        finally:
            db.close()

        first_page = self.client.get(
            f"/channels/{channel_name}/users", params={"limit": 25, "offset": 0}
        ).json()
        second_page = self.client.get(
            f"/channels/{channel_name}/users", params={"limit": 25, "offset": 25}
        ).json()

        self.assertEqual(first_page["total"], 30)
        self.assertEqual(len(first_page["items"]), 25)
        self.assertEqual(len(second_page["items"]), 5)

        search_payload = self.client.get(
            f"/channels/{channel_name}/users", params={"search": "user2", "limit": 50}
        ).json()
        expected = [idx for idx in range(30) if "user2" in f"user{idx:02d}"]
        self.assertEqual(search_payload["total"], len(expected))
        self.assertTrue(all("user2" in row["username"] for row in search_payload["items"]))

    def test_user_roles_returned(self) -> None:
        """API should annotate mod, VIP, and subscriber flags for UI badges."""

        db = backend_app.SessionLocal()
        original_collector = getattr(backend_app, "_collect_channel_roles")
        try:
            channel = _seed_channel_with_users(db, total=1)
            roster = _seed_role_accounts(db, channel)

            def fake_collect_roles(channel_obj):
                return {"vip-1"}, {"sub-1": "3000"}

            backend_app._collect_channel_roles = fake_collect_roles
            payload = self.client.get(
                f"/channels/{channel.channel_name}/users", params={"limit": 50, "offset": 0}
            ).json()
        finally:
            backend_app._collect_channel_roles = original_collector
            db.close()

        flags = {row["username"]: row for row in payload["items"]}
        self.assertTrue(flags[roster["mod"]]["is_mod"])
        self.assertTrue(flags[roster["vip"]]["is_vip"])
        self.assertTrue(flags[roster["sub"]]["is_subscriber"])
        self.assertEqual(flags[roster["sub"]]["subscriber_tier"], "3000")


if __name__ == "__main__":
    unittest.main()
