from datetime import datetime, timezone
from uuid import UUID

from app.domain import Payment, PaymentNotFound, PaymentStatus
from app.services.ports import PaymentGateway, UnitOfWorkFactory, WebhookSender


class ProcessingService:
    def __init__(
        self,
        unit_of_work: UnitOfWorkFactory,
        gateway: PaymentGateway,
        webhook: WebhookSender,
    ) -> None:
        self._unit_of_work: UnitOfWorkFactory = unit_of_work
        self._gateway: PaymentGateway = gateway
        self._webhook: WebhookSender = webhook

    async def process(self, payment_id: UUID) -> None:
        async with self._unit_of_work() as transaction:
            payment: Payment | None = await transaction.payments.get(payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFound
            if payment.status == "pending":
                status: PaymentStatus = await self._gateway.charge()
                await transaction.payments.set_result(
                    payment.id, status, datetime.now(timezone.utc)
                )

        # Commit the payment result before attempting the webhook.
        async with self._unit_of_work() as transaction:
            payment = await transaction.payments.get(payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFound
            if payment.webhook_sent_at is not None:
                return
            await self._webhook.send(payment)
            await transaction.payments.mark_webhook_sent(payment.id, datetime.now(timezone.utc))
