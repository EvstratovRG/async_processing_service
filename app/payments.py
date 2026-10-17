from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Outbox, Payment
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    pass


def payment_values(data: PaymentCreate) -> dict[str, Any]:
    return {
        "amount": data.amount,
        "currency": data.currency,
        "description": data.description,
        "extra_metadata": data.metadata,
        "webhook_url": str(data.webhook_url),
    }


async def create_payment(
    sessions: async_sessionmaker[AsyncSession], data: PaymentCreate, key: str
) -> Payment:
    payment_id: UUID = uuid4()
    values: dict[str, Any] = payment_values(data)
    async with sessions.begin() as session:
        inserted: UUID | None = await session.scalar(
            insert(Payment)
            .values(id=payment_id, idempotency_key=key, status="pending", **values)
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        payment: Payment = (
            await session.scalars(select(Payment).where(Payment.idempotency_key == key))
        ).one()
        if inserted is not None:
            session.add(Outbox(payload={"payment_id": str(payment.id)}))
        elif (
            payment.amount != Decimal(data.amount)
            or payment.currency != data.currency
            or payment.description != data.description
            or payment.extra_metadata != data.metadata
            or payment.webhook_url != str(data.webhook_url)
        ):
            raise IdempotencyConflict
        return payment
