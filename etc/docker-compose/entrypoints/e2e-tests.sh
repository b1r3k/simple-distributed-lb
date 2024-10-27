#!/bin/sh
poetry install --with=dev --quiet
poetry run pytest -o log_cli=true -x ./tests/e2e/
