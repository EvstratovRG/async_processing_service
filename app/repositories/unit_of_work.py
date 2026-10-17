from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.repositories.outbox import SQLAlchemyOutboxRepository
from app.repositories.payments import SQLAlchemyPaymentRepository
from app.services.ports import OutboxRepository, PaymentRepository, UnitOfWork, UnitOfWorkFactory


@dataclass
class Repositories:
    payments: PaymentRepository
    outbox: OutboxRepository


def create_unit_of_work(sessions: async_sessionmaker[AsyncSession]) -> UnitOfWorkFactory:
    @asynccontextmanager
    async def transaction() -> AsyncIterator[UnitOfWork]:
        async with sessions.begin() as session:
            yield Repositories(
                payments=SQLAlchemyPaymentRepository(session),
                outbox=SQLAlchemyOutboxRepository(session),
            )

    return transaction
