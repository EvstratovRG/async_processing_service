from typing import Any

from faststream.rabbit import RabbitBroker, RabbitQueue
from faststream.rabbit.schemas import Channel, QueueType
from pamqp.commands import Basic

from app.domain import OutboxEvent

NEW_QUEUE: RabbitQueue = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)


def create_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        timeout=10,
        graceful_timeout=20,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True, prefetch_count=1),
    )


async def publish_confirmed(
    broker: RabbitBroker,
    payload: dict[str, Any],
    queue: RabbitQueue,
    headers: dict[str, Any] | None = None,
) -> None:
    confirmation: object = await broker.publish(
        payload,
        queue=queue,
        persist=True,
        mandatory=True,
        timeout=10,
        headers=headers,
    )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("RabbitMQ did not confirm publication")


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker: RabbitBroker = broker

    async def publish(self, event: OutboxEvent) -> None:
        await publish_confirmed(self._broker, {"payment_id": str(event.payment_id)}, NEW_QUEUE)


DLQ: RabbitQueue = RabbitQueue("payments.dlq", queue_type=QueueType.QUORUM, durable=True)
RETRY_QUEUES: tuple[RabbitQueue, ...] = tuple(
    RabbitQueue(
        f"payments.retry.{delay}",
        queue_type=QueueType.QUORUM,
        durable=True,
        arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": NEW_QUEUE.name,
        },
    )
    for delay in (1, 2)
)


def retry_destination(attempt: int) -> RabbitQueue:
    return RETRY_QUEUES[attempt - 1] if attempt < 3 else DLQ
