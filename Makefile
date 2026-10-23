SHELL := /bin/bash
COMPOSE := docker compose
JOBS_PY := /opt/venv/jobs/bin/python

# Yesterday in UTC: the default business date for run-all / query.
DATE ?= $(shell python3 -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) - timedelta(days=1)).date())')
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo nogit)
GIT_DIRTY := $(shell git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
GIT_AUTHOR := $(shell git log -1 --format=%an 2>/dev/null | tr -cd '[:alnum:] ._-')
GIT_MSG := $(shell git log -1 --format=%s 2>/dev/null | tr -cd '[:alnum:] ._:,()-')

# Load .env (credentials for host-side scripts) and export everything to recipes.
-include .env
export

.DEFAULT_GOAL := help
.PHONY: help env build up down clean ps logs deploy run-all query query-bronze replay-events land \
	detect query-incidents break-it \
	schemas test test-unit test-dags test-e2e lint fmt lock mem mem-profile

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env: ## Create .env with random secrets (no-op if it exists)
	@bash scripts/init_env.sh

build: env ## Build the Airflow+Spark image
	$(COMPOSE) build

up: env ## Start the stack and wait until healthy
	$(COMPOSE) up -d --build
	@bash scripts/wait_healthy.sh 300
	@$(MAKE) --no-print-directory deploy
	@echo "Airflow   http://localhost:8080  (user: $${AIRFLOW_ADMIN_USER:-see .env}, password in .env)"
	@echo "Grafana   http://localhost:3000  (admin / GRAFANA_ADMIN_PASSWORD in .env)"
	@echo "Prometheus http://localhost:9090"

down: ## Stop the stack (keeps volumes and lake data)
	$(COMPOSE) down

clean: ## Stop the stack and delete ALL local state (volumes, lake, events)
	$(COMPOSE) down -v
	rm -rf data/lake data/events

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail scheduler logs
	$(COMPOSE) logs -f --tail=100 airflow-scheduler

deploy: ## Record current pipeline code versions; emit deploy_events for what changed
	@$(COMPOSE) exec -T airflow-scheduler $(JOBS_PY) -m sentinel.deploy \
	  --git-sha $(GIT_SHA) --dirty $(GIT_DIRTY) --author "$(GIT_AUTHOR)" --message "$(GIT_MSG)"

run-all: deploy ## Run all 4 DAGs for DATE (default: yesterday UTC) and wait
	python3 scripts/run_all.py --date $(DATE)

query: ## Show curated tables + emitted events for DATE
	$(COMPOSE) exec airflow-scheduler $(JOBS_PY) scripts/query_lake.py --ds $(DATE)

query-bronze: ## Bronze row counts + sample rows for DATE (business date)
	$(COMPOSE) exec airflow-scheduler $(JOBS_PY) scripts/query_bronze.py --ds $(DATE)

replay-events: ## Re-publish the JSONL journal to Redpanda (DAY=<emitted date> to limit)
	$(COMPOSE) exec airflow-scheduler $(JOBS_PY) -m sentinel.events.replay $(if $(DAY),--date $(DAY))

detect: ## Run detection once over bronze; upsert the incidents table (NOW=<iso> to override)
	$(COMPOSE) exec airflow-scheduler $(JOBS_PY) -m sentinel.detect $(if $(NOW),--now $(NOW))

query-incidents: ## List incidents (STATUS=open|resolved|superseded, ID=INC-... for evidence)
	$(COMPOSE) exec airflow-scheduler $(JOBS_PY) scripts/query_incidents.py \
	  $(if $(STATUS),--status $(STATUS)) $(if $(ID),--id $(ID))

break-it: ## Inject a fault on the live stack: FAULT=missing_amount|volume_drop|stale_customers DATE=
	bash scripts/break_it.sh $(FAULT) $(DATE)

schemas: ## Regenerate schemas/event.schema.json from the Pydantic models
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m sentinel.events.schema"

test: test-unit test-dags ## Run all unit + DAG tests in a container

test-unit: ## Unit tests (generators, jobs on local Spark, GX suites, events)
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m pytest tests/unit -q --cov --cov-report=term-missing:skip-covered"

test-dags: ## DAG integrity tests with Airflow's own interpreter
	$(COMPOSE) run --rm tools "python -m pytest tests/dags -q -p no:cacheprovider"

test-e2e: ## End-to-end: run a DAG on the live stack, assert its events land in bronze
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m pytest tests/integration -q -p no:cacheprovider"

lint: ## ruff lint + format check
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m ruff check . && $(JOBS_PY) -m ruff format --check ."

fmt: ## Auto-format with ruff (formats even if lint errors remain; exits non-zero then)
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m ruff format . && $(JOBS_PY) -m ruff check --fix .; \
	  rc=\$$?; $(JOBS_PY) -m ruff format -q .; exit \$$rc"

lock: ## Re-compile hashed lockfiles (uv, pinned) inside the base image
	docker run --rm -v "$$PWD/requirements:/req" -w /req --entrypoint bash \
	  apache/airflow:2.11.2-python3.11 -c "pip install -q uv==0.9.5 && \
	  python -m uv pip compile jobs.in --python-version 3.11 --python-platform linux --generate-hashes --no-header -o jobs.lock.txt && \
	  python -m uv pip compile dev.in --python-version 3.11 --python-platform linux --generate-hashes --no-header -o dev.lock.txt"

mem: ## Current memory use per container
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}' $$($(COMPOSE) ps -q)

mem-profile: ## Run run-all while sampling peak memory per container
	bash scripts/mem_profile.sh python3 scripts/run_all.py --date $(DATE)
