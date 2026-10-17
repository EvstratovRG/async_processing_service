import logging
from typing import Any

import httpx
from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import NEW_QUEUE, publish_confirmed
from app.processing import process_payment
from app.schemas import PaymentEvent

logger: logging.Logger = logging.getLogger(__name__)
DLQ: RabbitQueue = RabbitQueue("payments.dlq", durable=True)
RETRY_QUEUES: tuple[RabbitQueue, ...] = tuple(
    RabbitQueue(f"payments.retry.{delay}", durable=True, arguments={
        "x-message-ttl": delay * 1000,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": NEW_QUEUE.name,
    })
    for delay in (1, 2)
)


def retry_destination(attempt: int) -> RabbitQueue:
    return RETRY_QUEUES[attempt - 1] if attempt < 3 else DLQ


async def handle_payment(
    payload: dict[str, Any], message: RabbitMessage, sessions: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient, broker: RabbitBroker,
) -> None:
    raw_attempt: object = message.headers.get("attempt", 1)
    attempt: int = raw_attempt if type(raw_attempt) is int and 1 <= raw_attempt <= 3 else 3
    try:
        event: PaymentEvent = PaymentEvent.model_validate(payload)
        await process_payment(sessions, client, event.payment_id)
    except Exception as error:
        destination: RabbitQueue = retry_destination(attempt)
        logger.warning("Payment attempt %s failed (%s); routing to %s",
                       attempt, type(error).__name__, destination.name)
        try:
            await publish_confirmed(broker, payload, destination, headers={"attempt": attempt + 1})
        except Exception as publish_error:
            logger.error("Retry publication failed (%s); retaining message",
                         type(publish_error).__name__)
            await message.nack(requeue=True)
            return
    await message.ack()
