FROM python:3.11-slim

ARG USER=unprivileged
ARG GROUP=unprivileged
ARG APP_PORT=5000
ENV PYTHONUNBUFFERED 1
ENV DEBIAN_FRONTEND=noninteractive
ENV APP_PORT=${APP_PORT}

RUN apt-get -qq update && \
    apt-get -qq install --no-install-recommends apt-transport-https ca-certificates locales git curl iproute2 && \
    update-ca-certificates --fresh && \
    apt-get -qq upgrade --no-install-recommends
RUN pip install --upgrade pip poetry
RUN addgroup $GROUP && adduser \
    --disabled-password \
    --gecos "" \
    --ingroup $GROUP \
    --uid 1000 \
    $USER

RUN python -m venv /app/.venv

COPY . /app
WORKDIR /app

RUN rm -rf /app/dist && poetry build
RUN . /app/.venv/bin/activate && pip install /app/dist/simple*.whl

EXPOSE $APP_PORT
USER $USER

CMD . /app/.venv/bin/activate && uvicorn --host 0.0.0.0 --port ${APP_PORT} --timeout-keep-alive 61 --lifespan on --factory simple_distributed_lb.starlette_based:create_app
