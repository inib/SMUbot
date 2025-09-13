FROM nginx:alpine

# `envsubst` (from gettext) is used to inject environment variables into the
# static configuration served by nginx.
RUN apk add --no-cache gettext

COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY web/public/ /usr/share/nginx/html/
COPY web/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
