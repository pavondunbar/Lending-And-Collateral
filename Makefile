.DEFAULT_GOAL := help
COMPOSE       := docker compose
PG_CONN       := postgresql+psycopg2://lending:s3cr3t@localhost:5432/lending_db
BOLD  := \033[1m
RESET := \033[0m
CYAN  := \033[36m
GREEN := \033[32m
RED   := \033[31m

.PHONY: help up down down-v build restart logs ps shell-pg shell-kafka \
        test health demo integrity topics seed-accounts \
        db-loans db-collateral db-journal open-docs

help: ## Show this help
	@echo ""
	@echo "$(BOLD)Institutional Lending & Collateral Management$(RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ { \
		printf "  $(CYAN)%-22s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""

up: ## Build and start all services
	$(COMPOSE) up --build -d
	@echo "$(GREEN)Stack started — run 'make health' to verify.$(RESET)"

down: ## Stop and remove containers (keep volumes)
	$(COMPOSE) down

down-v: ## Stop and remove containers AND volumes
	$(COMPOSE) down -v

build: ## Rebuild all images without starting
	$(COMPOSE) build --no-cache

restart: ## Restart all services
	$(COMPOSE) restart

logs: ## Follow all service logs
	$(COMPOSE) logs -f

ps: ## Container status
	$(COMPOSE) ps

shell-pg: ## psql shell
	$(COMPOSE) exec postgres psql -U lending -d lending_db

shell-kafka: ## kafka bash shell
	$(COMPOSE) exec kafka bash

test: ## Full test suite
	PYTHONPATH=. pytest tests/ -v --tb=short -p no:warnings

health: ## Check gateway health
	@curl -sf http://localhost:8000/health | python3 -m json.tool || \
	  echo "$(RED)Gateway not reachable$(RESET)"

demo: ## Run live stack demo
	GATEWAY_URL=http://localhost:8000 \
	GATEWAY_API_KEY=$$(grep GATEWAY_API_KEY .env | cut -d= -f2) \
	python3 scripts/demo.py

integrity: ## Ledger double-entry integrity check
	DATABASE_URL=$(PG_CONN) python3 scripts/ledger_integrity.py

topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

seed-accounts: ## Insert demo accounts
	$(COMPOSE) exec postgres psql -U lending -d lending_db -c \
	  "INSERT INTO accounts (entity_name,account_type,kyc_verified,aml_cleared,risk_tier) \
	   VALUES ('Demo Bank A','institutional',true,true,1), \
	          ('Demo Bank B','correspondent',true,true,2) \
	   ON CONFLICT DO NOTHING;"
	@echo "$(GREEN)Demo accounts seeded.$(RESET)"

db-loans: ## Show recent loans
	$(COMPOSE) exec postgres psql -U lending -d lending_db -c \
	  "SELECT loan_ref,currency,principal,interest_rate_bps,status,created_at \
	   FROM loans ORDER BY created_at DESC LIMIT 20;"

db-collateral: ## Show collateral positions
	$(COMPOSE) exec postgres psql -U lending -d lending_db -c \
	  "SELECT collateral_ref,asset_type,quantity,haircut_pct,status \
	   FROM collateral_positions ORDER BY created_at DESC LIMIT 20;"

db-journal: ## Show recent journal entries
	$(COMPOSE) exec postgres psql -U lending -d lending_db -c \
	  "SELECT journal_id,coa_code,currency,debit,credit,entry_type,LEFT(narrative,40),created_at \
	   FROM journal_entries ORDER BY created_at DESC LIMIT 20;"

open-docs: ## Open API docs in browser
	open http://localhost:8000/docs 2>/dev/null || xdg-open http://localhost:8000/docs 2>/dev/null || \
	  echo "Visit http://localhost:8000/docs"
