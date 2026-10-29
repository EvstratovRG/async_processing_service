from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import create_autospec
from uuid import uuid4

import pytest

from app.domain import IdempotencyConflict, OutboxEvent, Payment, PaymentData, PaymentNotFound
from app.services.outbox import OutboxService
from app.services.payments import PaymentService
from app.services.ports import (
    EventPublisher,
    OutboxRepository,
    PaymentGateway,
    PaymentRepository,
    UnitOfWork,
    UnitOfWorkFactory,
    WebhookSender,
)
from app.services.processing import PaymentProcessingService


@pytest.fixture
def payment() -> Payment:
    return Payment(
        id=uuid4(),
        data=PaymentData(
            amount=Decimal("12.34"),
            currency="RUB",
            description="Order",
            metadata={"order_id": 1},
            webhook_url="http://receiver.test/webhook",
        ),
        status="pending",
        created_at=datetime.now(timezone.utc),
        processed_at=None,
        webhook_sent_at=None,
    )


@pytest.fixture
def transaction() -> tuple[UnitOfWorkFactory, Any, Any, list[str]]:
    payments: Any = create_autospec(PaymentRepository, instance=True)
    outbox: Any = create_autospec(OutboxRepository, instance=True)
    unit_of_work: Any = create_autospec(UnitOfWork, instance=True)
    unit_of_work.payments = payments
    unit_of_work.outbox = outbox
    events: list[str] = []

    @asynccontextmanager
    async def begin() -> AsyncGenerator[UnitOfWork]:
        events.append("begin")
        try:
            yield unit_of_work
        except Exception:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    return begin, payments, outbox, events


async def test_create_payment_adds_event_in_same_transaction(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
    payment: Payment,
) -> None:
    factory, payments, outbox, events = transaction
    payments.create_if_absent.return_value = payment, True
    service: PaymentService = PaymentService(factory)
    assert await service.create(payment.data, "key") == payment
    outbox.add.assert_awaited_once_with(payment.id)
    assert events == ["begin", "commit"]


async def test_idempotent_request_does_not_add_event(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
    payment: Payment,
) -> None:
    factory, payments, outbox, _ = transaction
    payments.create_if_absent.return_value = payment, False
    assert await PaymentService(factory).create(payment.data, "key") == payment
    outbox.add.assert_not_awaited()


async def test_changed_request_conflicts(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
    payment: Payment,
) -> None:
    factory, payments, outbox, events = transaction
    payments.create_if_absent.return_value = payment, False
    with pytest.raises(IdempotencyConflict):
        await PaymentService(factory).create(replace(payment.data, amount=Decimal("1")), "key")
    outbox.add.assert_not_awaited()
    assert events == ["begin", "rollback"]


async def test_get_missing_payment(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
) -> None:
    factory, payments, _, _ = transaction
    payments.get.return_value = None
    with pytest.raises(PaymentNotFound):
        await PaymentService(factory).get(uuid4())


async def test_webhook_failure_does_not_roll_back_payment_result(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
    payment: Payment,
) -> None:
    factory, payments, _, events = transaction
    completed: Payment = replace(
        payment, status="succeeded", processed_at=datetime.now(timezone.utc)
    )
    payments.get.side_effect = [payment, completed]
    gateway: Any = create_autospec(PaymentGateway, instance=True)
    gateway.charge.return_value = "succeeded"
    webhook: Any = create_autospec(WebhookSender, instance=True)

    async def fail_notification(result: Payment) -> None:
        assert result == completed
        assert events == ["begin", "commit", "begin"]
        raise RuntimeError("delivery unavailable")

    webhook.send.side_effect = fail_notification
    service: PaymentProcessingService = PaymentProcessingService(factory, gateway, webhook)
    with pytest.raises(RuntimeError, match="delivery unavailable"):
        await service.process(payment.id)
    payments.set_result.assert_awaited_once()
    payments.mark_webhook_sent.assert_not_awaited()
    assert events == ["begin", "commit", "begin", "rollback"]


async def test_completed_payment_only_retries_webhook(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
    payment: Payment,
) -> None:
    factory, payments, _, _ = transaction
    completed: Payment = replace(payment, status="failed", processed_at=datetime.now(timezone.utc))
    payments.get.return_value = completed
    gateway: Any = create_autospec(PaymentGateway, instance=True)
    webhook: Any = create_autospec(WebhookSender, instance=True)
    await PaymentProcessingService(factory, gateway, webhook).process(payment.id)
    gateway.charge.assert_not_awaited()
    payments.set_result.assert_not_awaited()
    webhook.send.assert_awaited_once_with(completed)
    payments.mark_webhook_sent.assert_awaited_once()
    assert all(call.kwargs == {"for_update": True} for call in payments.get.await_args_list)


async def test_delivered_payment_is_noop(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
    payment: Payment,
) -> None:
    factory, payments, _, _ = transaction
    payments.get.return_value = replace(
        payment,
        status="succeeded",
        processed_at=datetime.now(timezone.utc),
        webhook_sent_at=datetime.now(timezone.utc),
    )
    gateway: Any = create_autospec(PaymentGateway, instance=True)
    webhook: Any = create_autospec(WebhookSender, instance=True)
    await PaymentProcessingService(factory, gateway, webhook).process(payment.id)
    gateway.charge.assert_not_awaited()
    webhook.send.assert_not_awaited()
    payments.mark_webhook_sent.assert_not_awaited()


async def test_empty_outbox_does_not_publish(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
) -> None:
    factory, _, outbox, _ = transaction
    outbox.next_unpublished.return_value = None
    publisher: Any = create_autospec(EventPublisher, instance=True)
    assert not await OutboxService(factory, publisher).publish_one()
    publisher.publish.assert_not_awaited()
    outbox.mark_published.assert_not_awaited()


async def test_outbox_marks_only_confirmed_event(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
) -> None:
    factory, _, outbox, _ = transaction
    event: OutboxEvent = OutboxEvent(id=uuid4(), payment_id=uuid4())
    outbox.next_unpublished.return_value = event
    publisher: Any = create_autospec(EventPublisher, instance=True)

    async def confirm(result: OutboxEvent) -> None:
        assert result == event
        outbox.mark_published.assert_not_awaited()

    publisher.publish.side_effect = confirm
    assert await OutboxService(factory, publisher).publish_one()
    outbox.mark_published.assert_awaited_once()


async def test_outbox_failure_leaves_event_unpublished(
    transaction: tuple[UnitOfWorkFactory, Any, Any, list[str]],
) -> None:
    factory, _, outbox, events = transaction
    outbox.next_unpublished.return_value = OutboxEvent(id=uuid4(), payment_id=uuid4())
    publisher: Any = create_autospec(EventPublisher, instance=True)
    publisher.publish.side_effect = RuntimeError("broker unavailable")
    with pytest.raises(RuntimeError, match="broker unavailable"):
        await OutboxService(factory, publisher).publish_one()
    outbox.mark_published.assert_not_awaited()
    assert events == ["begin", "rollback"]


async def test_webhook_total_timeout_bounds_slow_response(payment: Payment) -> None:
    import asyncio
    from dataclasses import replace
    from datetime import datetime, timezone

    import httpx

    from app.adapters.webhook import HTTPWebhookSender

    async def slow_receiver(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return httpx.Response(204)

    completed: Payment = replace(
        payment, status="succeeded", processed_at=datetime.now(timezone.utc)
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_receiver)) as client:
        sender: HTTPWebhookSender = HTTPWebhookSender(client, timeout=0.05)
        task: asyncio.Task[None] = asyncio.create_task(sender.send(completed))
        try:
            done: set[asyncio.Task[None]]
            done, _ = await asyncio.wait({task}, timeout=0.3)
            assert task in done
            with pytest.raises(TimeoutError):
                await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_webhook_does_not_read_response_body(payment: Payment) -> None:
    from collections.abc import AsyncIterator

    import httpx

    from app.adapters.webhook import HTTPWebhookSender

    class UnreadableBody(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            raise AssertionError("Webhook response body must not be read")
            yield b""

    def receiver(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204, stream=UnreadableBody())

    completed: Payment = replace(
        payment, status="succeeded", processed_at=datetime.now(timezone.utc)
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        await HTTPWebhookSender(client, timeout=1).send(completed)
