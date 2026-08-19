FROM postgres:18-alpine

RUN \
  apk update && \
  apk upgrade && \
  apk add --no-cache postgresql-pg_cron

RUN ln -s /usr/lib/postgresql18/pg_cron.so /usr/local/lib/postgresql/pg_cron.so && \
  ln -s /usr/share/postgresql18/extension/pg_cron* /usr/local/share/postgresql/extension
