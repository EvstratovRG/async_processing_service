import os
import subprocess
import sys
from collections.abc import AsyncIterator

import pytest
from aio_pika import RobustQueue
from faststream.rabbit import RabbitBroker
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.adapters.messaging import DLQ, NEW_QUEUE, RETRY_QUEUES, create_broker
from app.config import Settings
from app.repositories.database import create_database


@pytest.fixture(scope="session")
def integration_settings() -> Settings:
    database_url: str | None = os.environ.get("TEST_DATABASE_URL")
    rabbitmq_url: str | None = os.environ.get("TEST_RABBITMQ_URL")
    if database_url is None or rabbitmq_url is None:
        pytest.skip("Set TEST_DATABASE_URL and TEST_RABBITMQ_URL for integration tests")
    settings: Settings = Settings(
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        api_key=SecretStr("test-key"),
    )
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        env={**os.environ, "DATABASE_URL": database_url, "API_KEY": "test-key"},
    )
    return settings


@pytest.fixture
async def database(
    integration_settings: Settings,
) -> AsyncIterator[tuple[AsyncEngine, async_sessionmaker[AsyncSession]]]:
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    engine, sessions = create_database(integration_settings.database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("TRUNCATE payments, outbox"))
        yield engine, sessions
    finally:
        await engine.dispose()


@pytest.fixture
async def broker(integration_settings: Settings) -> AsyncIterator[RabbitBroker]:
    instance: RabbitBroker = create_broker(integration_settings.rabbitmq_url)
    try:
        await instance.connect()
        for queue in (NEW_QUEUE, *RETRY_QUEUES, DLQ):
            declared: RobustQueue = await instance.declare_queue(queue)
            await declared.purge()
        yield instance
    finally:
        await instance.stop()
