from datetime import datetime, timezone

from app.domain import OutboxEvent
from app.services.ports import EventPublisher, UnitOfWorkFactory


class OutboxService:
    def __init__(self, unit_of_work: UnitOfWorkFactory, publisher: EventPublisher) -> None:
        self._unit_of_work: UnitOfWorkFactory = unit_of_work
        self._publisher: EventPublisher = publisher

    async def publish_one(self) -> bool:
        async with self._unit_of_work() as transaction:
            event: OutboxEvent | None = await transaction.outbox.next_unpublished()
            if event is None:
                return False
            await self._publisher.publish(event)
            await transaction.outbox.mark_published(event.id, datetime.now(timezone.utc))
            return True
