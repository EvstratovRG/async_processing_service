from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    api_key: SecretStr = Field(min_length=1)
    outbox_poll_interval: float = Field(default=1.0, gt=0)
    webhook_timeout: float = Field(default=10.0, gt=0)
