from typing import Any

from faststream.rabbit import RabbitBroker, RabbitQueue
from faststream.rabbit.schemas import Channel
from pamqp.commands import Basic

NEW_QUEUE: RabbitQueue = RabbitQueue("payments.new", durable=True)


def create_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url, timeout=10, graceful_timeout=20,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True, prefetch_count=1),
    )


async def publish_confirmed(
    broker: RabbitBroker, payload: dict[str, Any], queue: RabbitQueue,
    headers: dict[str, Any] | None = None,
) -> None:
    confirmation: object = await broker.publish(
        payload, queue=queue, persist=True, mandatory=True, timeout=10, headers=headers,
    )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("RabbitMQ did not confirm publication")
