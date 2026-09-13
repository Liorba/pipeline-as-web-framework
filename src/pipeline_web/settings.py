"""Application settings — the pipeline's environment/config layer."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline configuration loaded from env vars or a .env file."""

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "pipeline-as-web-framework"
    min_order_total: float = 10.0
    currency: str = "USD"
    store_fixture: str = "sample_orders.json"
    iceberg_warehouse: str = "./data/warehouse"
    iceberg_namespace: str = "pipeline"
