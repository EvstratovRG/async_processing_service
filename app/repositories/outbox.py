from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import OutboxEvent
from app.repositories.models import Outbox


class SQLAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session: AsyncSession = session

    async def add(self, payment_id: UUID) -> None:
        self._session.add(Outbox(payload={"payment_id": str(payment_id)}))

    async def next_unpublished(self) -> OutboxEvent | None:
        event: Outbox | None = await self._session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return None
        return OutboxEvent(id=event.id, payment_id=UUID(event.payload["payment_id"]))

    async def mark_published(self, event_id: UUID, published_at: datetime) -> None:
        await self._session.execute(
            update(Outbox).where(Outbox.id == event_id).values(published_at=published_at)
        )
