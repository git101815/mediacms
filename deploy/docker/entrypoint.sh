#!/bin/bash
set -e

# forward request and error logs to docker log collector
ln -sf /dev/stdout /var/log/nginx/access.log && ln -sf /dev/stderr /var/log/nginx/error.log && \
ln -sf /dev/stdout /var/log/nginx/mediacms.io.access.log && ln -sf /dev/stderr /var/log/nginx/mediacms.io.error.log

cp /home/mediacms.io/mediacms/deploy/docker/local_settings.py /home/mediacms.io/mediacms/cms/local_settings.py


mkdir -p /home/mediacms.io/mediacms/{logs,media_files/hls}
touch /home/mediacms.io/mediacms/logs/debug.log

mkdir -p /var/run/mediacms
chown www-data:www-data /var/run/mediacms

TARGET_GID=$(stat -c "%g" /home/mediacms.io/mediacms/)

EXISTS=$(cat /etc/group | grep $TARGET_GID | wc -l)

# Create new group using target GID and add www-data user
if [ $EXISTS == "0" ]; then
    groupadd -g $TARGET_GID tempgroup
    usermod -a -G tempgroup www-data
else
    # GID exists, find group name and add
    GROUP=$(getent group $TARGET_GID | cut -d: -f1)
    usermod -a -G $GROUP www-data
fi

# Application code and release static assets are image/read-only inputs.
# Never recursively chown the repository: web/Celery mount the release static
# snapshot read-only and a global chown would fail the container under `set -e`.
# Only runtime data directories need write ownership for www-data.
APP_ROOT=/home/mediacms.io/mediacms
for path in "$APP_ROOT/logs" "$APP_ROOT/media_files" "$APP_ROOT/backup"; do
    if [ -e "$path" ]; then
        chown -R www-data:"$TARGET_GID" "$path"
    fi
done

chmod +x /home/mediacms.io/mediacms/deploy/docker/start.sh /home/mediacms.io/mediacms/deploy/docker/prestart.sh

exec "$@"
