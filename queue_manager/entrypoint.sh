#!/bin/sh
set -e

BACKEND_ORIGIN="${PUBLIC_BACKEND_ORIGIN:-}"
if [ -z "$BACKEND_ORIGIN" ]; then
  echo "Error: PUBLIC_BACKEND_ORIGIN environment variable must be set." >&2
  exit 1
fi
BACKEND_ORIGIN=$(printf '%s' "$BACKEND_ORIGIN" | sed -e 's#/*$##')
ESCAPED_BACKEND_ORIGIN=$(printf '%s' "$BACKEND_ORIGIN" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')

mkdir -p /usr/share/nginx/html
cat > /usr/share/nginx/html/config.js <<EOF
window.__SONGBOT_CONFIG__ = { backendOrigin: "$ESCAPED_BACKEND_ORIGIN" };
EOF

exec nginx -g 'daemon off;'
