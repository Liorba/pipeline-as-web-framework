FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY pyproject.toml README.md /opt/airflow/demo/
COPY src /opt/airflow/demo/src
COPY dags /opt/airflow/dags

RUN pip install --no-cache-dir -e "/opt/airflow/demo[airflow]"

ENV PYTHONPATH="/opt/airflow/demo/src:${PYTHONPATH}"
