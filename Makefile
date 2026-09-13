.PHONY: install test up down trigger logs clean

install:
	pip install -e ".[dev,airflow]"

test:
	python3 -m pytest -v

up:
	mkdir -p logs
	echo -n "50000" > .airflow_uid 2>/dev/null || true
	docker compose up --build -d
	@echo "Airflow UI: http://localhost:8080 (admin / admin)"

down:
	docker compose down -v

trigger:
	docker compose exec airflow-scheduler \
		airflow dags trigger pipeline_as_web_framework

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

clean:
	docker compose down -v --rmi local
	rm -rf logs .pytest_cache .coverage htmlcov
