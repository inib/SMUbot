#!/bin/sh
set -e
: "${BACKEND_URL:=http://api:7070}"
: "${TWITCH_CLIENT_ID:=}"
# Substitute variables into config.js
envsubst '${BACKEND_URL} ${TWITCH_CLIENT_ID}' < /usr/share/nginx/html/config.js.template > /usr/share/nginx/html/config.js
# expose Twitch client id for JS
if [ -n "$TWITCH_CLIENT_ID" ]; then
echo "window.TWITCH_CLIENT_ID='${TWITCH_CLIENT_ID}';" >> /usr/share/nginx/html/config.js
fi
exec nginx -g 'daemon off;'
