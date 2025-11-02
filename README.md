# Songbot

Songbot is a Twitch song request system consisting of a FastAPI backend, a TwitchIO bot, and a small web interface. It lets viewers request and prioritize songs in channel chats.

After starting the stack, visit the Admin panel first to finish the deployment setup wizard—other services stay locked until the required Twitch credentials are saved.

See the [docs](docs/README.md) for a full project overview and setup instructions. To get started quickly, copy `example.env` to
`stack.env`, adjust the values, then run `docker-compose --env-file stack.env up --build`.
