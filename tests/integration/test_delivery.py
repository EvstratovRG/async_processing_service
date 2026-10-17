import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
import pytest
from aio_pika import RobustQueue
from aio_pika.abc import AbstractIncomingMessage
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.adapters.gateway import SimulatedPaymentGateway
from app.adapters.messaging import (
    DLQ,
    NEW_QUEUE,
    RETRY_QUEUES,
    RabbitEventPublisher,
    publish_confirmed,
)
from app.adapters.webhook import HTTPWebhookSender
from app.api.schemas import PaymentCreate
from app.config import Settings
from app.consumer import create_consumer
from app.domain import Payment as DomainPayment
from app.repositories.models import Outbox, Payment
from app.repositories.unit_of_work import create_unit_of_work
from app.services.outbox import OutboxService
from app.services.payments import PaymentService
from app.services.processing import ProcessingService

pytestmark: pytest.MarkDecorator = pytest.mark.integration


def payment_data(url: str = "http://receiver.test/webhook") -> PaymentCreate:
    return PaymentCreate(
        amount="12.34", currency="EUR", description="Delivery test", metadata={}, webhook_url=url
    )


async def next_message(queue: RobustQueue, timeout: float = 10) -> AbstractIncomingMessage:
    async with asyncio.timeout(timeout):
        while True:
            message: AbstractIncomingMessage | None = await queue.get(fail=False, timeout=1)
            if message is not None:
                return message
            await asyncio.sleep(0.05)


async def test_outbox_failure_then_recovery(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    broker: RabbitBroker,
) -> None:
    payment: DomainPayment = await PaymentService(create_unit_of_work(database[1])).create(
        payment_data().to_domain(), str(uuid4())
    )
    with patch(
        "app.adapters.messaging.publish_confirmed", new_callable=AsyncMock, side_effect=TimeoutError
    ):
        with pytest.raises(TimeoutError):
            await OutboxService(
                create_unit_of_work(database[1]), RabbitEventPublisher(broker)
            ).publish_one()
    async with database[1]() as session:
        assert (await session.scalars(select(Outbox))).one().published_at is None
    assert await OutboxService(
        create_unit_of_work(database[1]), RabbitEventPublisher(broker)
    ).publish_one()
    assert not await OutboxService(
        create_unit_of_work(database[1]), RabbitEventPublisher(broker)
    ).publish_one()
    queue: RobustQueue = await broker.declare_queue(NEW_QUEUE)
    message: AbstractIncomingMessage = await next_message(queue)
    assert json.loads(message.body) == {"payment_id": str(payment.id)}
    assert message.delivery_mode == 2
    await message.ack()
    async with database[1]() as session:
        assert (await session.scalars(select(Outbox))).one().published_at is not None


async def test_duplicate_after_publish_commit_failure(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    broker: RabbitBroker,
) -> None:
    await PaymentService(create_unit_of_work(database[1])).create(
        payment_data().to_domain(), str(uuid4())
    )

    # Deliver the real message, then emulate loss of the confirmation/DB commit.
    async def publish_then_fail(*args: Any, **kwargs: Any) -> None:
        await publish_confirmed(*args, **kwargs)
        raise TimeoutError

    with patch("app.adapters.messaging.publish_confirmed", side_effect=publish_then_fail):
        with pytest.raises(TimeoutError):
            await OutboxService(
                create_unit_of_work(database[1]), RabbitEventPublisher(broker)
            ).publish_one()
    assert await OutboxService(
        create_unit_of_work(database[1]), RabbitEventPublisher(broker)
    ).publish_one()
    queue: RobustQueue = await broker.declare_queue(NEW_QUEUE)
    first: AbstractIncomingMessage = await next_message(queue)
    second: AbstractIncomingMessage = await next_message(queue)
    assert first.body == second.body
    await first.ack()
    await second.ack()


async def test_unroutable_event_is_not_marked_published(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    broker: RabbitBroker,
) -> None:
    await PaymentService(create_unit_of_work(database[1])).create(
        payment_data().to_domain(), str(uuid4())
    )
    from aio_pika.exceptions import DeliveryError
    from faststream.rabbit import RabbitQueue

    with patch("app.adapters.messaging.NEW_QUEUE", RabbitQueue("absent-queue", declare=False)):
        with pytest.raises(DeliveryError):
            await OutboxService(
                create_unit_of_work(database[1]), RabbitEventPublisher(broker)
            ).publish_one()
    async with database[1]() as session:
        assert (await session.scalars(select(Outbox))).one().published_at is None


@pytest.mark.parametrize("result", ["succeeded", "failed"])
async def test_webhook_retry_preserves_result_and_duplicate_is_noop(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    result: str,
) -> None:
    payment: DomainPayment = await PaymentService(create_unit_of_work(database[1])).create(
        payment_data().to_domain(), str(uuid4())
    )
    requests: list[httpx.Request] = []

    def receiver(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503 if len(requests) == 1 else 204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        processing: ProcessingService = ProcessingService(
            create_unit_of_work(database[1]), SimulatedPaymentGateway(), HTTPWebhookSender(client)
        )
        with patch(
            "app.adapters.gateway.SimulatedPaymentGateway.charge",
            new_callable=AsyncMock,
            return_value=result,
        ) as simulate:
            with pytest.raises(httpx.HTTPStatusError):
                await processing.process(payment.id)
            async with database[1]() as session:
                saved: Payment = (await session.scalars(select(Payment))).one()
                assert saved.status == result
                assert saved.processed_at is not None
                assert saved.webhook_sent_at is None
                processed_at: datetime = saved.processed_at
            await asyncio.gather(*(processing.process(payment.id) for _ in range(3)))
            simulate.assert_awaited_once()
    assert len(requests) == 2
    assert json.loads(requests[0].content) == json.loads(requests[1].content)
    async with database[1]() as session:
        saved = (await session.scalars(select(Payment))).one()
        assert saved.processed_at == processed_at
        assert saved.webhook_sent_at is not None


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


async def test_live_consumer_retries_three_times_then_dlq(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    broker: RabbitBroker,
    consumer: FastStream,
) -> None:
    payment: DomainPayment = await PaymentService(create_unit_of_work(database[1])).create(
        payment_data().to_domain(), str(uuid4())
    )
    # Raise an actual HTTPX timeout instead of contacting an external host.
    with (
        patch(
            "app.adapters.gateway.SimulatedPaymentGateway.charge",
            new_callable=AsyncMock,
            return_value="succeeded",
        ) as simulate,
        patch(
            "app.adapters.webhook.httpx.AsyncClient.post",
            autospec=True,
            side_effect=httpx.ConnectTimeout("test timeout"),
        ) as post,
    ):
        await OutboxService(
            create_unit_of_work(database[1]), RabbitEventPublisher(broker)
        ).publish_one()
        queue: RobustQueue = await broker.declare_queue(DLQ)
        message: AbstractIncomingMessage = await next_message(queue, timeout=15)
        assert json.loads(message.body) == {"payment_id": str(payment.id)}
        assert message.headers["attempt"] == 4
        assert post.await_count == 3
        simulate.assert_awaited_once()
        await message.ack()
    async with database[1]() as session:
        saved: Payment = (await session.scalars(select(Payment))).one()
        assert saved.status == "succeeded"
        assert saved.webhook_sent_at is None


async def test_retry_ttl_returns_persistent_message(broker: RabbitBroker) -> None:
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    for queue, minimum_delay in zip(RETRY_QUEUES, (1, 2), strict=True):
        started: float = loop.time()
        payload: dict[str, Any] = {"payment_id": str(uuid4())}
        await publish_confirmed(broker, payload, queue, headers={"attempt": 2})
        destination: RobustQueue = await broker.declare_queue(NEW_QUEUE)
        message: AbstractIncomingMessage = await next_message(destination)
        assert loop.time() - started >= minimum_delay * 0.9
        assert json.loads(message.body) == payload
        assert message.headers["attempt"] == 2
        assert message.delivery_mode == 2
        await message.ack()


async def test_broker_redelivery_preserves_payload(broker: RabbitBroker) -> None:
    payload: dict[str, Any] = {"payment_id": str(uuid4())}
    await publish_confirmed(broker, payload, NEW_QUEUE)
    queue: RobustQueue = await broker.declare_queue(NEW_QUEUE)
    first: AbstractIncomingMessage = await next_message(queue)
    await first.nack(requeue=True)
    redelivered: AbstractIncomingMessage = await next_message(queue)
    assert redelivered.redelivered
    assert json.loads(redelivered.body) == payload
    await redelivered.ack()
