from datetime import datetime
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import Select, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import Currency, Payment, PaymentData, PaymentStatus
from app.repositories.models import Payment as PaymentModel


def to_payment(model: PaymentModel) -> Payment:
    return Payment(
        id=model.id,
        data=PaymentData(
            amount=model.amount,
            currency=cast(Currency, model.currency),
            description=model.description,
            metadata=model.extra_metadata,
            webhook_url=model.webhook_url,
        ),
        status=cast(PaymentStatus, model.status),
        created_at=model.created_at,
        processed_at=model.processed_at,
        webhook_sent_at=model.webhook_sent_at,
    )


class SQLAlchemyPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session: AsyncSession = session

    async def create_if_absent(self, data: PaymentData, key: str) -> tuple[Payment, bool]:
        inserted: UUID | None = await self._session.scalar(
            insert(PaymentModel)
            .values(
                id=uuid4(),
                idempotency_key=key,
                status="pending",
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                extra_metadata=data.metadata,
                webhook_url=data.webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[PaymentModel.idempotency_key])
            .returning(PaymentModel.id)
        )
        model: PaymentModel = (
            await self._session.scalars(
                select(PaymentModel).where(PaymentModel.idempotency_key == key)
            )
        ).one()
        return to_payment(model), inserted is not None

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None:
        statement: Select[tuple[PaymentModel]] = select(PaymentModel).where(
            PaymentModel.id == payment_id
        )
        if for_update:
            statement = statement.with_for_update()
        model: PaymentModel | None = await self._session.scalar(statement)
        return to_payment(model) if model is not None else None

    async def set_result(
        self,
        payment_id: UUID,
        status: PaymentStatus,
        processed_at: datetime,
    ) -> None:
        await self._session.execute(
            update(PaymentModel)
            .where(PaymentModel.id == payment_id)
            .values(status=status, processed_at=processed_at)
        )

    async def mark_webhook_sent(self, payment_id: UUID, sent_at: datetime) -> None:
        await self._session.execute(
            update(PaymentModel)
            .where(PaymentModel.id == payment_id)
            .values(webhook_sent_at=sent_at)
        )
