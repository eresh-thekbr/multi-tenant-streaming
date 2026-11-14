PYTHON ?= python3

.PHONY: install test benchmark up load logs down

install:
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest -q

benchmark:
	$(PYTHON) -m multi_tenant_streaming benchmark

up:
	docker compose up --build -d

load:
	docker compose --profile load run --rm load-generator

logs:
	docker compose logs -f consumer

down:
	docker compose down -v
