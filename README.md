# Songbot

Songbot is a Twitch song request system consisting of a FastAPI backend, a TwitchIO bot, and a small web interface. It lets viewers request and prioritize songs in channel chats.

After starting the stack, visit the Admin panel first to finish the deployment setup wizard—other services stay locked until the required Twitch credentials are saved.

See the [docs](docs/README.md) for a full project overview and setup instructions. To get started quickly, copy `example.env` to
`stack.env`, adjust the values, then run `docker-compose --env-file stack.env up --build`.

## Queue Manager Users tab
- The Users view now ships with a debounced search box that filters usernames client-side while still passing the term to the backend source.
- Pagination is limited to 25 names per page with accessible Previous/Next controls and a live page indicator.
- Channel owners and the special playlist automation requester (`__playlist__`) are excluded from both the rendered list and the API totals by default.
