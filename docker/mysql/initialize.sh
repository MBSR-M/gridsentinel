#!/bin/sh
set -eu
# Generated secrets contain only URL-safe characters; never print them.
app_password=$(cat /run/secrets/mysql_password)
grafana_password=$(cat /run/secrets/grafana_mysql_password)
case "$app_password:$grafana_password" in
  *[!A-Za-z0-9_:-]*) echo 'Development secrets must use URL-safe characters' >&2; exit 1 ;;
esac
MYSQL_PWD="$(cat /run/secrets/mysql_root_password)" mysql -uroot <<SQL
ALTER USER 'gridsentinel'@'%' IDENTIFIED WITH mysql_native_password BY '${app_password}';
CREATE USER IF NOT EXISTS 'gs_reporter'@'%' IDENTIFIED WITH mysql_native_password BY '${grafana_password}';
SQL
