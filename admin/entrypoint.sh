#!/bin/sh
set -e
: "${BACKEND_URL:=http://api:7070}"
: "${TWITCH_CLIENT_ID:=}"
: "${TWITCH_SCOPES:=user:read:chat user:write:chat channel:bot}"
: "${BOT_APP_SCOPES:=user:read:chat user:write:chat user:bot}"
# Substitute variables into config.js
envsubst '${BACKEND_URL} ${TWITCH_CLIENT_ID} ${TWITCH_SCOPES} ${BOT_APP_SCOPES}' \
  < /usr/share/nginx/html/config.js.template > /usr/share/nginx/html/config.js
exec nginx -g 'daemon off;'
