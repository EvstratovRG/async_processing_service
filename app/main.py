import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.adapters.messaging import NEW_QUEUE, RabbitEventPublisher, create_broker
from app.api.payments import create_router
from app.config import Settings
from app.repositories.database import create_database
from app.repositories.unit_of_work import create_unit_of_work
from app.services.outbox import OutboxService
from app.services.payments import PaymentService
from app.services.ports import UnitOfWorkFactory
from app.workers.outbox import run_publisher


def create_app(settings: Settings | None = None) -> FastAPI:
    config: Settings = settings if settings is not None else Settings()
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    engine, sessions = create_database(config.database_url)
    unit_of_work: UnitOfWorkFactory = create_unit_of_work(sessions)
    payments: PaymentService = PaymentService(unit_of_work)
    broker: RabbitBroker = create_broker(config.rabbitmq_url)
    outbox: OutboxService = OutboxService(unit_of_work, RabbitEventPublisher(broker))

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        publisher: asyncio.Task[None] | None = None
        try:
            await broker.connect()
            await broker.declare_queue(NEW_QUEUE)
            publisher = asyncio.create_task(run_publisher(outbox, config.outbox_poll_interval))
            yield
        finally:
            try:
                if publisher is not None:
                    publisher.cancel()
                    with suppress(asyncio.CancelledError):
                        await publisher
            finally:
                try:
                    await broker.stop()
                finally:
                    await engine.dispose()

    application: FastAPI = FastAPI(
        title="Async Processing Service",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    application.include_router(create_router(payments, config.api_key.get_secret_value()))
    return application
