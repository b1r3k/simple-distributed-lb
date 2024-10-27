#!/bin/bash
# should break if any command fails
set -e
MY_IP=$(ip addr show eth0 | grep 'inet ' | awk '{print $2}' | cut -d'/' -f1)

poetry install --with=dev --quiet

echo "Target loadbalancer: ${LOADBALANCER_HOST}:${LOADBALANCER_PORT}"
# register upstream server (this container) with the loadbalancer
curl --fail -i -L -X POST -H "Content-Type:application/json" -d \
'[{"ip_address": "'"${MY_IP}"'", "port": "'"${APP_PORT}"'", "path": "/hello_world"}]' http://${LOADBALANCER_HOST}:${LOADBALANCER_PORT}/register || exit 1
echo "Listening on ${MY_IP}:${APP_PORT}"
# start the upstream server
poetry run demo_upstream_server
