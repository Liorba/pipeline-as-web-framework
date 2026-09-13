.PHONY: install test lint dag-check up down trigger logs clean

install:
	pip install -e ".[dev]"

test:
	python3 -m pytest -v

dag-check:
	PYTHONPATH=src:dags python3 -c "from pipeline_as_web_dag import pipeline_as_web_dag; print('DAG OK:', pipeline_as_web_dag.dag_id)"

up:
	mkdir -p logs
	echo -n "$$(id -u)" > .airflow_uid 2>/dev/null || true
	export AIRFLOW_UID=$$(id -u) && docker compose up -d
	@echo "Airflow UI: http://localhost:8080  (admin / admin)"

down:
	docker compose down -v

trigger:
	docker compose exec airflow-webserver airflow dags trigger pipeline_as_web

logs:
	docker compose logs airflow-scheduler airflow-webserver --tail=100

clean:
	rm -rf logs .pytest_cache .coverage htmlcov dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
