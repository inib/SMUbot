# Songbot Wiki

## Overview
Songbot is a Twitch song request platform composed of three main parts:

- **Backend** (`backend_app.py`): a FastAPI service that stores channels, songs, users, and request queues using SQLAlchemy models.
- **Bot** (`bot/bot_app.py`): a TwitchIO chat bot that lets viewers request songs and manage priorities by talking to the backend.
- **Web** (`web/`): an Nginx container serving a small static interface for viewing the current queue.

Additional directories include:

- **queue_manager/** – static assets served by the channel-facing Queue Manager UI.
- **admin/** – static assets for the Admin control panel used to manage the shared bot account and view channel stats.
- **data/** – persistent SQLite database storage.

## Running with Docker
1. Copy `example.env` to `stack.env` and adjust values such as `ADMIN_TOKEN`,
   `ADMIN_BASIC_AUTH_USERNAME`, `ADMIN_BASIC_AUTH_PASSWORD`, `TWITCH_CLIENT_ID`,
   and `TWITCH_CLIENT_SECRET`. When exposing the stack outside of Docker, set
   `BACKEND_URL` to the public URL of the API so the bot and web UI can reach
   it. Bot credentials are now managed through the backend at `/bot/config`
   instead of `.env` entries.
2. Start the stack:
   ```bash
   docker-compose --env-file stack.env up --build
   ```
   This launches the API on port 7070, the bot, and the web UI on port 7000
   (overridden with `WEB_PORT`).

## Backend Highlights
- Uses a SQLite database stored at `/data/db.sqlite` and defines models for channels, songs, users, stream sessions, and requests.
- Stores bot OAuth credentials via the `/bot/config` API and exposes an OAuth
  helper flow for authorizing the bot account.
- Exposes REST endpoints for managing songs and queue entries, plus SSE streams
  for queue updates and bot log streaming.
- `run.sh` initializes the database and starts the server with Uvicorn.

## Bot Highlights
- Automatically discovers authorized channels from the backend and joins them.
- Supports commands:
  - `!request` – add a song request.
  - `!prioritize` – bump one of your requests using priority points.
  - `!points` – check remaining priority points.
  - `!remove` – delete your latest request.
- Automatically parses YouTube links and fetches titles via oEmbed.

## Web Interface
The web container hosts files in `web/public/`, including a simple `index.html`, `app.js`, and `style.css` for viewing the queue.

## Authentication & Channel Access
Songbot relies on two distinct OAuth flows that map to the two management panels:

1. **Bot account authorization (Admin panel)** – The Admin control panel triggers a
   client credentials grant using the scopes `user:read:chat user:write:chat user:bot`.
   The resulting app access token is stored through `/bot/config` and allows the
   backend and bot worker to act as the shared bot account when calling the API.
   The Admin panel is protected with HTTP basic authentication configured via
   the `ADMIN_BASIC_AUTH_USERNAME` and `ADMIN_BASIC_AUTH_PASSWORD` environment
   variables.

2. **Channel authorization (Queue Manager)** – Channel owners sign in through the
   Queue Manager UI and complete the authorization code grant with the
   `channel:bot` scope. The backend records the channel during this handshake and
   subscribes to chat events using the previously obtained app access token. Only
   channels that complete this flow are joined by the bot.

Owners can invite moderators by adding their Twitch accounts inside the Queue
Manager, and authenticated users who manage multiple channels can switch between
them via `/me/channels`.

## Development Tips
- Install Python dependencies from `requirements.txt` for local development.
- Run the backend directly:
  ```bash
  ./run.sh
  ```
- Launch the bot locally:
  ```bash
  python bot/bot_app.py
  ```
