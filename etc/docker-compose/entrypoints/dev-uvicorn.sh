#!/bin/sh

PWD=$(pwd)

echo "Current working directory: ${PWD}"
echo "Bootstrapping dependencies.."
poetry install --with=dev --quiet
echo "Starting Uvicorn server.."
poetry run uvicorn --host 0.0.0.0 --port ${APP_PORT} --timeout-keep-alive 61 --lifespan on --factory simple_distributed_lb.server:create_app
