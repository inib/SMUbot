FROM nginx:alpine
RUN apk add --no-cache gettext
COPY admin/nginx.conf /etc/nginx/conf.d/default.conf
COPY admin/public/ /usr/share/nginx/html/
COPY admin/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
