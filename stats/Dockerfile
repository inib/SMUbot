FROM nginx:alpine
RUN apk add --no-cache gettext
COPY stats/nginx.conf /etc/nginx/conf.d/default.conf
COPY stats/public/ /usr/share/nginx/html/
COPY stats/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
