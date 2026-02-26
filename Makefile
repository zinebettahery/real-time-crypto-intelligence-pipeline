.PHONY: help init up down restart logs ps shell test-dag list-dags clean flower

help:
	@echo ""
	@echo "  real-time-crypto-intelligence-pipeline — Airflow Commands"
	@echo "  ──────────────────────────────────────────────────────────"
	@echo "  make init        First-time setup (builds image, inits DB)"
	@echo "  make up          Start all Airflow services"
	@echo "  make down        Stop all services"
	@echo "  make restart     down + up"
	@echo "  make logs        Stream all logs"
	@echo "  make ps          Show container status"
	@echo "  make shell       Bash inside scheduler container"
	@echo "  make test-dag    Trigger fred_daily_ingestion manually"
	@echo "  make list-dags   List all registered DAGs"
	@echo "  make flower      Start with Celery Flower UI (port 5555)"
	@echo "  make clean       Remove containers + volumes + logs"
	@echo ""

init:
	@if [ ! -f .env ]; then cp .env.example .env && echo "✅ .env created — fill in your credentials"; fi
	@echo "AIRFLOW_UID=$$(id -u)" >> .env
	@mkdir -p airflow/logs airflow/plugins airflow/data/fred
	@docker compose build
	@docker compose up airflow-init
	@echo ""
	@echo "  🚀 Done! Run 'make up' to start."

up:
	@docker compose up -d --exclude-deps airflow-init 2>/dev/null || docker compose up -d
	@echo ""
	@echo "  ✅ Airflow started"
	@echo "  🌐 UI  → http://localhost:8080"
	@echo "  👤 Login: airflow / airflow"

flower:
	@docker compose --profile flower up -d
	@echo "  🌸 Flower → http://localhost:5555"

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

logs-scheduler:
	docker compose logs -f airflow-scheduler

logs-worker:
	docker compose logs -f airflow-worker

ps:
	docker compose ps

shell:
	docker compose exec airflow-scheduler bash

test-dag:
	docker compose exec airflow-scheduler \
		airflow dags trigger fred_daily_ingestion
	@echo "  ✅ DAG triggered — check http://localhost:8080"

list-dags:
	docker compose exec airflow-scheduler airflow dags list

clean:
	docker compose down --volumes --remove-orphans
	rm -rf airflow/logs/*
	@echo "  🧹 Cleaned"