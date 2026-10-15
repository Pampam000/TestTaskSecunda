from functools import lru_cache

from pydantic import Field, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5433/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: str = "local-dev-key"

    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    consumer_prefetch: int = Field(default=16, gt=0)
    consumer_concurrency: int = Field(default=8, gt=0)
    max_attempts: int = Field(default=3, ge=2)
    retry_base_delay: float = Field(default=1.0, gt=0)

    webhook_max_attempts: int = Field(default=3, ge=1)
    webhook_timeout: float = Field(default=5.0, gt=0)
    webhook_base_delay: float = Field(default=0.5, gt=0)

    circuit_breaker_threshold: int = Field(default=5, gt=0)
    circuit_breaker_reset_timeout: float = Field(default=30.0, gt=0)

    gateway_min_delay: float = Field(default=2.0, gt=0)
    gateway_max_delay: float = Field(default=5.0, gt=0)
    gateway_success_rate: float = Field(default=0.9, gt=0, le=1.0)

    log_level: str = "INFO"

    @field_validator("gateway_max_delay")
    @classmethod
    def validate_delay_range(cls, value: float, info: ValidationInfo) -> float:
        min_delay = info.data.get("gateway_min_delay")
        if min_delay is not None and value < min_delay:
            raise ValueError("gateway_max_delay must be greater than or equal to gateway_min_delay")
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
