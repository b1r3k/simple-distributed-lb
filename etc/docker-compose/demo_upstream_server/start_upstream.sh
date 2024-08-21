#!/bin/bash

# should break if any command fails
set -e

MY_IP=$(ip addr show eth0 | grep 'inet ' | awk '{print $2}' | cut -d'/' -f1)
echo "Loadbalancer: ${LOADBALANCER_HOST}:${LOADBALANCER_PORT}"
curl --fail -i -L -X POST -H "Content-Type:application/json" -d \
'[{"ip_address": "'"${MY_IP}"'", "port": "'"${APP_PORT}"'", "path": "/hello_world"}]' http://${LOADBALANCER_HOST}:${LOADBALANCER_PORT}/register || exit 1
echo "Listening on ${MY_IP}:${APP_PORT}"
. /app/.venv/bin/activate && demo_upstream_server
