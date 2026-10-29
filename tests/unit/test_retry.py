import json
from typing import Any
from unittest.mock import create_autospec
from uuid import uuid4

import pytest
from faststream.rabbit import RabbitBroker
from faststream.rabbit.message import RabbitMessage
from pamqp.commands import Basic

from app.adapters.messaging import NEW_QUEUE, publish_confirmed, retry_destination
from app.handlers.payments import handle_payment
from app.services.processing import PaymentProcessingService


@pytest.mark.parametrize(
    "attempt, name",
    [(1, "payments.retry.1"), (2, "payments.retry.2"), (3, "payments.dlq")],
)
def test_retry_routes(attempt: int, name: str) -> None:
    assert retry_destination(attempt).name == name


@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_error_is_published_before_ack(attempt: int) -> None:
    message: Any = create_autospec(RabbitMessage, instance=True)
    message.headers = {"attempt": attempt}
    broker: Any = create_autospec(RabbitBroker, instance=True)
    service: Any = create_autospec(PaymentProcessingService, instance=True)
    service.process.side_effect = RuntimeError

    async def confirm(*args: Any, **kwargs: Any) -> Basic.Ack:
        message.ack.assert_not_awaited()
        return Basic.Ack(delivery_tag=1)

    broker.publish.side_effect = confirm
    payload: dict[str, Any] = {"payment_id": str(uuid4())}
    await handle_payment(json.dumps(payload).encode(), message, service, broker)
    broker.publish.assert_awaited_once()
    assert broker.publish.call_args.kwargs["queue"].name == retry_destination(attempt).name
    assert broker.publish.call_args.kwargs["headers"] == {"attempt": attempt + 1}
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


async def test_unconfirmed_retry_retains_original() -> None:
    message: Any = create_autospec(RabbitMessage, instance=True)
    message.headers = {}
    broker: Any = create_autospec(RabbitBroker, instance=True)
    broker.publish.side_effect = TimeoutError
    service: Any = create_autospec(PaymentProcessingService, instance=True)
    service.process.side_effect = RuntimeError
    await handle_payment(
        json.dumps({"payment_id": str(uuid4())}).encode(), message, service, broker
    )
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


@pytest.mark.parametrize("confirmation", [None, Basic.Nack(delivery_tag=1)])
async def test_publisher_requires_positive_confirmation(confirmation: object) -> None:
    broker: Any = create_autospec(RabbitBroker, instance=True)
    broker.publish.return_value = confirmation
    with pytest.raises(RuntimeError, match="did not confirm"):
        await publish_confirmed(broker, {}, NEW_QUEUE)


async def test_success_acks_without_retry() -> None:
    message: Any = create_autospec(RabbitMessage, instance=True)
    message.headers = {}
    broker: Any = create_autospec(RabbitBroker, instance=True)
    service: Any = create_autospec(PaymentProcessingService, instance=True)
    await handle_payment(
        json.dumps({"payment_id": str(uuid4())}).encode(), message, service, broker
    )
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    broker.publish.assert_not_awaited()


async def test_invalid_message_is_requeued_if_dlq_publication_fails() -> None:
    message: Any = create_autospec(RabbitMessage, instance=True)
    message.headers = {}
    broker: Any = create_autospec(RabbitBroker, instance=True)
    broker.publish.side_effect = TimeoutError
    service: Any = create_autospec(PaymentProcessingService, instance=True)
    await handle_payment(b"{", message, service, broker)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    service.process.assert_not_awaited()
