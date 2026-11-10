# Everything runs in Docker, so only Docker is needed.
# Uses docker-compose if installed, otherwise the `docker compose` plugin (as on CI runners).
COMPOSE ?= $(shell command -v docker-compose >/dev/null 2>&1 && echo docker-compose || echo docker compose)
MVN = $(COMPOSE) run --rm --no-deps job mvn -q
FMT = $(COMPOSE) run --rm --no-deps fmt sh docker/fmt/fmt.sh

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  run        - Run the job once against the local MongoDB"
	@echo "  test       - Run the unit tests"
	@echo "  fmt        - Format all files (Java, XML, YAML, Markdown, JSON, JS, Python, shell)"
	@echo "  fmt-check  - Fail if any file needs formatting (what CI runs)"

.PHONY: run
run:
	@$(COMPOSE) run --rm job

.PHONY: test
test:
	@$(MVN) test

.PHONY: fmt
fmt:
	@$(MVN) spotless:apply
	@$(FMT)

# Runs both checks before failing, so one run reports every file that needs formatting.
.PHONY: fmt-check
fmt-check:
	@$(MVN) spotless:check; java=$$?; $(FMT) --check && exit $$java
