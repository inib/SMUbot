#!/bin/sh
set -e
: "${BACKEND_URL:=http://api:7070}"
: "${TWITCH_CLIENT_ID:=}"
: "${TWITCH_SCOPES:=channel:bot channel:read:subscriptions channel:read:vips}"
# Substitute variables into config.js
envsubst '${BACKEND_URL} ${TWITCH_CLIENT_ID} ${TWITCH_SCOPES}' < /usr/share/nginx/html/config.js.template > /usr/share/nginx/html/config.js
exec nginx -g 'daemon off;'
