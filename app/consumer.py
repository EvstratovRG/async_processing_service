import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.adapters.gateway import SimulatedPaymentGateway
from app.adapters.messaging import DLQ, NEW_QUEUE, RETRY_QUEUES, create_broker
from app.adapters.webhook import HTTPWebhookSender
from app.config import Settings
from app.handlers.payments import handle_payment
from app.repositories.database import create_database
from app.repositories.unit_of_work import create_unit_of_work
from app.services.processing import ProcessingService


def create_consumer(settings: Settings | None = None) -> FastStream:
    config: Settings = settings if settings is not None else Settings()
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    engine, sessions = create_database(config.database_url)
    broker: RabbitBroker = create_broker(config.rabbitmq_url)
    client: httpx.AsyncClient = httpx.AsyncClient(timeout=config.webhook_timeout)
    processing: ProcessingService = ProcessingService(
        create_unit_of_work(sessions),
        SimulatedPaymentGateway(),
        HTTPWebhookSender(client),
    )

    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        try:
            yield
        finally:
            try:
                await client.aclose()
            finally:
                await engine.dispose()

    application: FastStream = FastStream(broker, lifespan=lifespan)

    @application.on_startup
    async def declare_queues() -> None:
        await broker.connect()
        for queue in (NEW_QUEUE, *RETRY_QUEUES, DLQ):
            await broker.declare_queue(queue)

    @broker.subscriber(NEW_QUEUE, ack_policy=AckPolicy.MANUAL)
    async def handle(payload: dict[str, Any], message: RabbitMessage) -> None:
        await handle_payment(payload, message, processing, broker)

    return application


if __name__ == "__main__":
    asyncio.run(create_consumer().run())
