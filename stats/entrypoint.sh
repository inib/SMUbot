#!/bin/sh
set -e
: "${BACKEND_URL:=http://api:7070}"
# Substitute variables into config.js
envsubst '${BACKEND_URL}' < /usr/share/nginx/html/config.js.template > /usr/share/nginx/html/config.js
exec nginx -g 'daemon off;'
