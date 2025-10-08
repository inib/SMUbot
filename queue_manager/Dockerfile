FROM nginx:alpine
RUN apk add --no-cache gettext
COPY queue_manager/nginx.conf /etc/nginx/conf.d/default.conf
COPY queue_manager/public/ /usr/share/nginx/html/
COPY queue_manager/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
