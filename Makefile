APP_VERSION := $(shell grep -oP '(?<=^version = ")[^"]*' pyproject.toml)
APP_DIR := simple_distributed_lb
NPROCS = $(shell grep -c 'processor' /proc/cpuinfo)
MAKEFLAGS += -j$(NPROCS)
PYTEST_FLAGS := --failed-first -x --durations=1 --durations-min=1.0 --timeout=1


install:
	poetry install
	test -d .git/hooks/pre-commit || poetry run pre-commit install

test:
	poetry run pytest ${PYTEST_FLAGS} tests/unit

e2e-test:
	poetry export --with dev --without-hashes --format=requirements.txt > requirements-dev.txt
	docker compose build e2e-tests
	docker compose run --rm e2e-tests

testloop:
	watch -n 3 poetry run pytest ${PYTEST_FLAGS} tests/unit

lint-fix:
	poetry run isort --profile black .
	poetry run black ${APP_DIR}

lint-check:
	poetry run flake8 ${APP_DIR}
	poetry run mypy .


lint: lint-fix lint-check
