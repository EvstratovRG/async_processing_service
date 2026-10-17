from uuid import UUID

from app.domain import IdempotencyConflict, Payment, PaymentData, PaymentNotFound
from app.services.ports import UnitOfWorkFactory


class PaymentService:
    def __init__(self, unit_of_work: UnitOfWorkFactory) -> None:
        self._unit_of_work: UnitOfWorkFactory = unit_of_work

    async def create(self, data: PaymentData, key: str) -> Payment:
        async with self._unit_of_work() as transaction:
            payment: Payment
            created: bool
            payment, created = await transaction.payments.create_if_absent(data, key)
            if created:
                await transaction.outbox.add(payment.id)
            elif payment.data != data:
                raise IdempotencyConflict
            return payment

    async def get(self, payment_id: UUID) -> Payment:
        async with self._unit_of_work() as transaction:
            payment: Payment | None = await transaction.payments.get(payment_id)
            if payment is None:
                raise PaymentNotFound
            return payment
