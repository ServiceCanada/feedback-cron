# Everything runs in Docker, so only Docker is needed.
# Uses docker-compose if installed, otherwise the `docker compose` plugin (as on CI runners).
COMPOSE ?= $(shell command -v docker-compose >/dev/null 2>&1 && echo docker-compose || echo docker compose)
MVN = $(COMPOSE) run --rm --no-deps job mvn -q

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  run        - Run the job once against the local MongoDB"
	@echo "  test       - Run the unit tests"

.PHONY: run
run:
	@$(COMPOSE) run --rm job

.PHONY: test
test:
	@$(MVN) test
