import logging
import traceback
from uuid import UUID

from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitQueue
from pydantic import BaseModel, ValidationError

from app.adapters.messaging import DLQ, publish_confirmed, retry_destination
from app.services.processing import PaymentProcessingService

logger: logging.Logger = logging.getLogger(__name__)


class PaymentEvent(BaseModel):
    payment_id: UUID


async def handle_payment(
    payload: bytes,
    message: RabbitMessage,
    service: PaymentProcessingService,
    broker: RabbitBroker,
) -> None:
    raw_attempt: object = message.headers.get("attempt", 1)
    attempt: int = raw_attempt if type(raw_attempt) is int and 1 <= raw_attempt <= 3 else 3
    try:
        event: PaymentEvent = PaymentEvent.model_validate_json(payload)
    except ValidationError:
        try:
            await publish_confirmed(broker, payload, DLQ)
        except Exception as error:
            logger.error("Invalid message could not be sent to DLQ (%s)", type(error).__name__)
            await message.nack(requeue=True)
            return
        logger.warning("Invalid payment message routed to DLQ")
        await message.ack()
        return

    try:
        await service.process(event.payment_id)
    except Exception as error:
        destination: RabbitQueue = retry_destination(attempt)
        logger.warning(
            "Payment %s attempt %s failed (%s); routing to %s; stack=%s",
            event.payment_id,
            attempt,
            type(error).__name__,
            destination.name,
            [
                (frame.filename, frame.lineno, frame.name)
                for frame in traceback.extract_tb(error.__traceback__)
            ],
        )
        try:
            await publish_confirmed(broker, payload, destination, headers={"attempt": attempt + 1})
        except Exception as publish_error:
            logger.error(
                "Payment %s retry publication failed (%s); retaining message; stack=%s",
                event.payment_id,
                type(publish_error).__name__,
                [
                    (frame.filename, frame.lineno, frame.name)
                    for frame in traceback.extract_tb(publish_error.__traceback__)
                ],
            )
            await message.nack(requeue=True)
            return
    await message.ack()
