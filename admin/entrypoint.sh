#!/bin/sh
set -e
: "${ADMIN_BASIC_AUTH_USERNAME:=admin}"
: "${ADMIN_BASIC_AUTH_PASSWORD:=admin}"

if [ -z "${ADMIN_BASIC_AUTH_PASSWORD:-}" ]; then
  echo "Error: ADMIN_BASIC_AUTH_PASSWORD environment variable must be set." >&2
  exit 1
fi

htpasswd -bBc /etc/nginx/.htpasswd "$ADMIN_BASIC_AUTH_USERNAME" "$ADMIN_BASIC_AUTH_PASSWORD" >/dev/null
chown root:nginx /etc/nginx/.htpasswd
chmod 640 /etc/nginx/.htpasswd
unset ADMIN_BASIC_AUTH_PASSWORD

BACKEND_ORIGIN="${PUBLIC_BACKEND_ORIGIN:-}"
ESCAPED_BACKEND_ORIGIN=$(printf '%s' "$BACKEND_ORIGIN" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')

mkdir -p /usr/share/nginx/html
cat > /usr/share/nginx/html/config.js <<EOF
window.__SONGBOT_CONFIG__ = { backendOrigin: "$ESCAPED_BACKEND_ORIGIN" };
EOF

exec nginx -g 'daemon off;'
