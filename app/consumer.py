import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.adapters.gateway import SimulatedPaymentGateway
from app.adapters.messaging import DLQ, NEW_QUEUE, RETRY_QUEUES, create_broker
from app.adapters.webhook import HTTPWebhookSender
from app.config import Settings
from app.handlers.payments import handle_payment
from app.repositories.database import Database
from app.repositories.unit_of_work import create_unit_of_work
from app.services.processing import PaymentProcessingService


def create_consumer(settings: Settings | None = None) -> FastStream:
    config: Settings = settings if settings is not None else Settings()
    database: Database = Database(config.database_url)
    broker: RabbitBroker = create_broker(config.rabbitmq_url)
    client: httpx.AsyncClient = httpx.AsyncClient(timeout=config.webhook_timeout)
    processing: PaymentProcessingService = PaymentProcessingService(
        create_unit_of_work(database.get_session_factory()),
        SimulatedPaymentGateway(),
        HTTPWebhookSender(client, config.webhook_timeout),
    )

    @asynccontextmanager
    async def lifespan() -> AsyncGenerator[None]:
        try:
            yield
        finally:
            try:
                await client.aclose()
            finally:
                await database.get_engine().dispose()

    application: FastStream = FastStream(broker, lifespan=lifespan)

    @application.on_startup
    async def declare_queues() -> None:
        await broker.connect()
        for queue in (NEW_QUEUE, *RETRY_QUEUES, DLQ):
            await broker.declare_queue(queue)

    async def decode_raw(message: RabbitMessage) -> bytes:
        return message.body

    @broker.subscriber(NEW_QUEUE, ack_policy=AckPolicy.MANUAL, decoder=decode_raw)
    async def handle(payload: bytes, message: RabbitMessage) -> None:
        await handle_payment(payload, message, processing, broker)

    return application


if __name__ == "__main__":
    asyncio.run(create_consumer().run())
