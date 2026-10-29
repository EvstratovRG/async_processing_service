import logging
import traceback
from datetime import datetime, timezone

from app.domain import OutboxEvent
from app.services.ports import EventPublisher, UnitOfWorkFactory

logger: logging.Logger = logging.getLogger(__name__)


class OutboxService:
    def __init__(self, unit_of_work: UnitOfWorkFactory, publisher: EventPublisher) -> None:
        self._unit_of_work: UnitOfWorkFactory = unit_of_work
        self._publisher: EventPublisher = publisher

    async def publish_one(self) -> bool:
        async with self._unit_of_work() as transaction:
            event: OutboxEvent | None = await transaction.outbox.next_unpublished()
            if event is None:
                return False
            try:
                await self._publisher.publish(event)
            except Exception as error:
                logger.warning(
                    "Outbox event %s for payment %s publication failed (%s); stack=%s",
                    event.id,
                    event.payment_id,
                    type(error).__name__,
                    [
                        (frame.filename, frame.lineno, frame.name)
                        for frame in traceback.extract_tb(error.__traceback__)
                    ],
                )
                raise
            await transaction.outbox.mark_published(event.id, datetime.now(timezone.utc))
            return True
