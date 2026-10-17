import asyncio
import logging
from datetime import datetime, timezone

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import NEW_QUEUE, publish_confirmed
from app.models import Outbox

logger: logging.Logger = logging.getLogger(__name__)


async def publish_one(
    sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker,
) -> bool:
    async with sessions.begin() as session:
        event: Outbox | None = await session.scalar(
            select(Outbox).where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at).limit(1).with_for_update(skip_locked=True)
        )
        if event is None:
            return False
        await publish_confirmed(broker, event.payload, NEW_QUEUE)
        event.published_at = datetime.now(timezone.utc)
        return True


async def run_publisher(
    sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker, interval: float,
) -> None:
    while True:
        try:
            if await publish_one(sessions, broker):
                continue
        except Exception as error:
            logger.warning("Outbox publication failed (%s); retrying", type(error).__name__)
        await asyncio.sleep(interval)
