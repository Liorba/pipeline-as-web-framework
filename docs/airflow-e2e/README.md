# Airflow end-to-end run proof

Screenshots from a successful **`pipeline_as_web`** DAG run on Airflow 2.10.4 standalone
(run id: `manual__2026-09-13T07:23:53+00:00`).

| Screenshot | Description |
|------------|-------------|
| [airflow_dag_run_success.png](./airflow_dag_run_success.png) | DAG run grid — all four tasks green |
| [airflow_task_graph_green.png](./airflow_task_graph_green.png) | Task graph view |
| [airflow_extract_orders_log_di.png](./airflow_extract_orders_log_di.png) | `extract_orders` log — DI injection line |
| [airflow_publish_iceberg_log.png](./airflow_publish_iceberg_log.png) | `publish_iceberg` log — isolated Iceberg write |

## Raw GitHub URLs

Branch: `cursor/pipeline-web-framework-demo-6c42`

- https://raw.githubusercontent.com/Liorba/pipeline-as-web-framework/cursor/pipeline-web-framework-demo-6c42/docs/airflow-e2e/airflow_dag_run_success.png
- https://raw.githubusercontent.com/Liorba/pipeline-as-web-framework/cursor/pipeline-web-framework-demo-6c42/docs/airflow-e2e/airflow_task_graph_green.png
- https://raw.githubusercontent.com/Liorba/pipeline-as-web-framework/cursor/pipeline-web-framework-demo-6c42/docs/airflow-e2e/airflow_extract_orders_log_di.png
- https://raw.githubusercontent.com/Liorba/pipeline-as-web-framework/cursor/pipeline-web-framework-demo-6c42/docs/airflow-e2e/airflow_publish_iceberg_log.png
