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
	detect query-incidents break-it diagnose query-diagnoses demo-e2e \
	benchmark benchmark-build benchmark-status benchmark-detection benchmark-judge \
	benchmark-score benchmark-human-sample benchmark-agreement benchmark-posthoc benchmark-catalog manual-trial trial-inspect \
	trial-incident trial-bronze test-benchmark \
	schemas test test-unit test-dags test-e2e test-real-api lint fmt lock mem mem-profile

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

diagnose: ## Diagnose one incident: INCIDENT=INC-... [MODEL=claude-sonnet-5-5] [DRY_RUN=1] [MOCK=1]
	$(COMPOSE) exec -e SENTINEL_MODEL=$(or $(MODEL),$(SENTINEL_MODEL),claude-haiku-5-5) \
	  sentinel-detector $(JOBS_PY) -m sentinel.diagnose --incident '$(INCIDENT)' \
	  $(if $(DRY_RUN),--dry-run) $(if $(MOCK),--mock)

query-diagnoses: ## Diagnoses table + ledger spend (ID=INC-... for the full JSON)
	$(COMPOSE) exec sentinel-detector $(JOBS_PY) scripts/query_diagnoses.py $(if $(ID),--id '$(ID)')

demo-e2e: ## break-it -> detect -> diagnose -> mock Slack -> Retry click -> green re-run (DATE=)
	python3 scripts/demo_e2e.py --date $(or $(DEMO_DATE),2026-10-05)

break-it: ## Inject a fault on the live stack: FAULT=missing_amount|volume_drop|stale_customers DATE=
	bash scripts/break_it.sh $(FAULT) $(DATE)

# --- Phase 5 benchmark (python -m benchmark.harness; see README "Benchmark harness") ---
# LABEL=<slug> (Phase 5c: posthoc-tool-fixes; 5d: posthoc-round-2) builds and runs against its own scenario
# lakes in data/benchmark-<slug> and its own run folder; the unlabelled v1 lakes and
# results are never touched.
BENCH_WORK = $(if $(LABEL),-e SENTINEL_BENCHMARK_WORK=/workspace/data/benchmark-$(LABEL))
BENCH := $(COMPOSE) run --rm $(BENCH_WORK) benchmark
BENCH_PY := $(JOBS_PY) -m benchmark.harness
SPLIT ?= benchmark
PROMPT ?= diagnosis_v1
BENCH_MODEL = $(or $(MODEL),claude-haiku-5-5)

benchmark-catalog: ## Re-draw the sealed fault catalog (seeded; CHECK=1 only verifies it)
	$(BENCH) "$(BENCH_PY) gen-catalog $(if $(CHECK),--check)"

benchmark-build: ## Build scenario lakes (real jobs, no API calls): SPLIT=all|benchmark|dev [REBUILD=1]
	$(BENCH) "$(BENCH_PY) build --split $(or $(BUILD_SPLIT),all) $(if $(REBUILD),--rebuild)"

benchmark: ## Run the benchmark: MODEL= REPEATS= PROMPT=diagnosis_v1 SPLIT=benchmark DRY_RUN=1 RESUME=1 LABEL=
	@$(if $(DRY_RUN),,bash scripts/check_auto_diagnose_off.sh)
	$(if $(DRY_RUN),,$(MAKE) --no-print-directory benchmark-build BUILD_SPLIT=$(SPLIT))
	$(COMPOSE) run --rm -e ANTHROPIC_API_KEY -e SENTINEL_AUTO_DIAGNOSE $(BENCH_WORK) benchmark \
	  "$(BENCH_PY) run --model $(BENCH_MODEL) --split $(SPLIT) --prompt $(PROMPT) \
	  $(if $(REPEATS),--repeats $(REPEATS)) $(if $(DRY_RUN),--dry-run) $(if $(RESUME),--resume) \
	  $(if $(RETRY_ERRORS),--retry-errors) $(if $(JUDGE),--judge) $(if $(LABEL),--label $(LABEL))"

BENCH_AN := $(JOBS_PY) -m benchmark.analysis

benchmark-detection: ## Phase 5b: replay the detector's 60 s polls over the scenarios (no API)
	$(BENCH) "$(BENCH_AN) detection"

benchmark-judge: ## Phase 5b: LLM judges over one run: MODEL= [DRY_RUN=1] [LABEL=] (paid, resumable)
	$(COMPOSE) run --rm -e ANTHROPIC_API_KEY $(BENCH_WORK) benchmark \
	  "$(BENCH_AN) --prompt $(PROMPT) judge --model $(BENCH_MODEL) $(if $(DRY_RUN),--dry-run) \
	  $(if $(LABEL),--label $(LABEL))"

benchmark-score: ## Phase 5b: metrics.json, CSVs and charts in benchmark/results/report (no API)
	$(BENCH) "$(BENCH_AN) --prompt $(PROMPT) score"

benchmark-human-sample: ## Phase 5b: blind 15-diagnosis fix-quality sheet for a human
	$(BENCH) "$(BENCH_AN) --prompt $(PROMPT) human-sample"

benchmark-agreement: ## Phase 5b: judge vs. human agreement (after scores.csv is filled in)
	$(BENCH) "$(BENCH_AN) agreement"

benchmark-posthoc: ## Phase 5c/5d: score a labelled run vs. v1 (LABEL= [PRIOR="r1 ..."]; no API)
	$(BENCH) "$(BENCH_AN) --prompt $(PROMPT) posthoc --label $(LABEL) --model $(BENCH_MODEL) \
	  $(foreach p,$(PRIOR),--prior $(p))"

benchmark-status: ## Scenario and result counts (no labels)
	$(BENCH) "$(BENCH_PY) status"

manual-trial: ## Timed blind manual baseline (10 faults); REVEAL=end keeps labels to the end
	$(BENCH) "$(BENCH_PY) manual-trial --reveal $(or $(REVEAL),each)"

trial-inspect: ## During a trial: TOOL=get_error_logs [PIPELINE=] [DS=] [ARGS='{...}'] [INCIDENT=]
	@$(BENCH) "$(BENCH_PY) inspect --tool $(TOOL) $(if $(PIPELINE),--pipeline $(PIPELINE)) \
	  $(if $(DS),--ds $(DS)) $(if $(ARGS),--args '$(ARGS)') $(if $(INCIDENT),--incident '$(INCIDENT)')"

trial-incident: ## During a trial: the incident record (what the agent gets first)
	@$(BENCH) "$(BENCH_PY) incident $(if $(INCIDENT),--incident '$(INCIDENT)')"

trial-bronze: ## During a trial: bronze rows of the trial lake for DS=
	@$(COMPOSE) run --rm -e SENTINEL_LAKE_ROOT=/workspace/data/benchmark/trial/lake benchmark \
	  "$(JOBS_PY) scripts/query_bronze.py --ds $(DS)"

test-benchmark: ## Slow: build a template night + 2 scenarios with the real jobs in a temp dir
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m pytest tests/unit -q -m benchmark_build -p no:cacheprovider"

schemas: ## Regenerate schemas/event.schema.json from the Pydantic models
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m sentinel.events.schema"

test: test-unit test-dags ## Run all unit + DAG tests in a container

test-unit: ## Unit tests (generators, jobs on local Spark, GX suites, events)
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m pytest tests/unit -q -m 'not benchmark_build' --cov --cov-report=term-missing:skip-covered"

test-dags: ## DAG integrity tests with Airflow's own interpreter
	$(COMPOSE) run --rm tools "python -m pytest tests/dags -q -p no:cacheprovider"

test-real-api: ## Smoke tests against the REAL Claude API (needs ANTHROPIC_API_KEY; ledger-gated, ~$0.01)
	$(COMPOSE) run --rm -e ANTHROPIC_API_KEY -e SENTINEL_MODEL tools \
	  "$(JOBS_PY) -m pytest tests/unit/test_diagnose_real_api.py -q -s -p no:cacheprovider"

test-e2e: ## End-to-end: run a DAG on the live stack, assert its events land in bronze
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m pytest tests/integration -q -p no:cacheprovider"

lint: ## ruff lint + format check
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m ruff check . && $(JOBS_PY) -m ruff format --check ."

fmt: ## Auto-format with ruff (formats even if lint errors remain; exits non-zero then)
	$(COMPOSE) run --rm tools "$(JOBS_PY) -m ruff format . && $(JOBS_PY) -m ruff check --fix .; \
	  rc=\$$?; $(JOBS_PY) -m ruff format -q .; exit \$$rc"

# Nothing released after this date enters a lockfile (supply-chain: let new releases age).
LOCK_EXCLUDE_NEWER ?= 2026-10-09T23:59:59Z

lock: ## Re-compile hashed lockfiles (uv, pinned) inside the base image
	docker run --rm -v "$$PWD/requirements:/req" -w /req --entrypoint bash \
	  apache/airflow:2.11.2-python3.11 -c "pip install -q uv==0.9.5 && \
	  python -m uv pip compile jobs.in --exclude-newer $(LOCK_EXCLUDE_NEWER) --python-version 3.11 --python-platform linux --generate-hashes --no-header -o jobs.lock.txt && \
	  python -m uv pip compile dev.in --exclude-newer $(LOCK_EXCLUDE_NEWER) --python-version 3.11 --python-platform linux --generate-hashes --no-header -o dev.lock.txt"

mem: ## Current memory use per container
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}' $$($(COMPOSE) ps -q)

mem-profile: ## Run run-all while sampling peak memory per container
	bash scripts/mem_profile.sh python3 scripts/run_all.py --date $(DATE)
