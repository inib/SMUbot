#!/bin/sh
set -e
: "${BACKEND_URL:=http://api:7070}"
: "${TWITCH_CLIENT_ID:=}"
: "${TWITCH_SCOPES:=user:read:chat user:write:chat user:bot}"
: "${BOT_APP_SCOPES:=user:read:chat user:write:chat user:bot}"
: "${ADMIN_BASIC_AUTH_USERNAME:=admin}"

if [ -z "${ADMIN_BASIC_AUTH_PASSWORD:-}" ]; then
  echo "Error: ADMIN_BASIC_AUTH_PASSWORD environment variable must be set." >&2
  exit 1
fi

htpasswd -bBc /etc/nginx/.htpasswd "$ADMIN_BASIC_AUTH_USERNAME" "$ADMIN_BASIC_AUTH_PASSWORD" >/dev/null
chown root:nginx /etc/nginx/.htpasswd
chmod 640 /etc/nginx/.htpasswd
unset ADMIN_BASIC_AUTH_PASSWORD
# Substitute variables into config.js
envsubst '${BACKEND_URL} ${TWITCH_CLIENT_ID} ${TWITCH_SCOPES} ${BOT_APP_SCOPES}' \
  < /usr/share/nginx/html/config.js.template > /usr/share/nginx/html/config.js
exec nginx -g 'daemon off;'
