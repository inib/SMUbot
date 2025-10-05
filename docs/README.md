# Songbot Wiki

## Overview
Songbot is a Twitch song request platform composed of three main parts:

- **Backend** (`backend_app.py`): a FastAPI service that stores channels, songs, users, and request queues using SQLAlchemy models.
- **Bot** (`bot/bot_app.py`): a TwitchIO chat bot that lets viewers request songs and manage priorities by talking to the backend.
- **Web** (`web/`): an Nginx container serving a small static interface for viewing the current queue.

Additional directories include:

- **cs_admin/** – a C# admin application.
- **html/** – static HTML assets.
- **data/** – persistent SQLite database storage.

## Running with Docker
1. Copy `example.env` to `stack.env` and adjust values such as `ADMIN_TOKEN`,
   `TWITCH_BOT_TOKEN`, `BOT_NICK`, `TWITCH_CLIENT_ID`, and
   `TWITCH_CLIENT_SECRET`. When exposing the stack outside of Docker, set
   `BACKEND_URL` to the public URL of the API so the bot and web UI can reach
   it.
2. Start the stack:
   ```bash
   docker-compose --env-file stack.env up --build
   ```
   This launches the API on port 7070, the bot, and the web UI on port 7000
   (overridden with `WEB_PORT`).

## Backend Highlights
- Uses a SQLite database stored at `/data/db.sqlite` and defines models for channels, songs, users, stream sessions, and requests.
- Exposes REST endpoints for managing songs and queue entries, plus an SSE stream for real‑time events.
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
Songbot now integrates with Twitch OAuth. Channel owners must authenticate via the
`/auth/login` endpoint to grant the bot the required scopes (including `channel:bot`).
Once authenticated, the bot is allowed to join the channel and an EventSub
subscription for `channel.chat.message` is created using an App Access Token.
This registration is required for Twitch to recognize the bot as a verified chat bot.
The backend only creates channel records during this OAuth handshake, ensuring the
bot joins channels explicitly approved through the admin panel.
Owners can invite other moderators by registering their Twitch accounts, and
authenticated users who manage multiple channels can switch between them using
`/me/channels`.

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
