UV := uv

.PHONY: install format format-check lint typecheck test check

install:
	$(UV) sync --all-groups

format:
	$(UV) run ruff format .

format-check:
	$(UV) run ruff format --check .

lint:
	$(UV) run ruff check .

typecheck:
	$(UV) run mypy src tests

test:
	$(UV) run coverage run -m pytest
	$(UV) run coverage report

check: format-check lint typecheck test
