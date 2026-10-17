import logging
from typing import Any
from uuid import UUID

from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitQueue
from pydantic import BaseModel

from app.adapters.messaging import publish_confirmed, retry_destination
from app.services.processing import ProcessingService

logger: logging.Logger = logging.getLogger(__name__)


class PaymentEvent(BaseModel):
    payment_id: UUID


async def handle_payment(
    payload: dict[str, Any],
    message: RabbitMessage,
    service: ProcessingService,
    broker: RabbitBroker,
) -> None:
    raw_attempt: object = message.headers.get("attempt", 1)
    attempt: int = raw_attempt if type(raw_attempt) is int and 1 <= raw_attempt <= 3 else 3
    try:
        event: PaymentEvent = PaymentEvent.model_validate(payload)
        await service.process(event.payment_id)
    except Exception as error:
        destination: RabbitQueue = retry_destination(attempt)
        logger.warning(
            "Payment attempt %s failed (%s); routing to %s",
            attempt,
            type(error).__name__,
            destination.name,
        )
        try:
            await publish_confirmed(broker, payload, destination, headers={"attempt": attempt + 1})
        except Exception as publish_error:
            logger.error(
                "Retry publication failed (%s); retaining message", type(publish_error).__name__
            )
            await message.nack(requeue=True)
            return
    await message.ack()
