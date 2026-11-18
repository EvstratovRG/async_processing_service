from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.consumer import create_consumer
from app.main import create_app
from app.repositories.database import Database


@pytest.fixture
async def api(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    integration_settings: Settings,
    db: Database,
) -> AsyncIterator[httpx.AsyncClient]:
    application: FastAPI = create_app(integration_settings, db=db)
    # HTTPX ASGITransport handles HTTP only; the publisher is tested separately.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as client:
        yield client


@pytest.fixture
def body() -> dict[str, Any]:
    return {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Test payment",
        "metadata": {"order": 7},
        "webhook_url": "http://receiver.test/webhook",
    }


@pytest.fixture
async def consumer(
    integration_settings: Settings,
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    broker: RabbitBroker,
) -> AsyncIterator[FastStream]:
    application: FastStream = create_consumer(integration_settings)
    async with application.lifespan_context():
        await application.start()
        try:
            yield application
        finally:
            await application.stop()
