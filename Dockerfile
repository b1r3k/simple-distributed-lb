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

EXPOSE $APP_PORT
WORKDIR /app

RUN pip3 install -U pip>=23.1.2 poetry

# Install dependencies only (cached layer - only rebuilds when poetry.lock changes)
COPY pyproject.toml poetry.lock README.md ./
RUN --mount=type=ssh poetry install --no-root

# Copy source code (invalidates on every code change)
ADD . /app

# Install the project package itself (fast, no dependency downloads)
RUN poetry install --only-root

# Fix ownership for unprivileged user
RUN chown -R $USER:$GROUP /app

USER $USER

CMD poetry run uvicorn --host 0.0.0.0 --port ${APP_PORT} --timeout-keep-alive 61 --lifespan on --factory simple_distributed_lb.server:create_app
