"""Application settings — the pipeline equivalent of web-framework config/env."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline configuration loaded once at the composition root."""

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "pipeline-as-web-framework"
    min_order_amount: float = Field(default=10.0, description="Minimum valid order total")
    currency: str = "USD"
    sample_orders_json: str = Field(
        default=(
            '[{"id":"o1","customer":"Ada","amount":25.5,"status":"new"},'
            '{"id":"o2","customer":"Bob","amount":5.0,"status":"new"},'
            '{"id":"o3","customer":"Cy","amount":42.0,"status":"pending"}]'
        ),
        description="JSON array of raw orders for the in-memory store",
    )
