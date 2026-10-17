from secrets import compare_digest
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException

from app.api.schemas import PaymentAccepted, PaymentCreate, PaymentDetail
from app.domain import IdempotencyConflict, Payment, PaymentData, PaymentNotFound
from app.services.payments import PaymentService


def create_router(service: PaymentService, api_key: str) -> APIRouter:
    async def authorize(x_api_key: Annotated[str | None, Header()] = None) -> None:
        if x_api_key is None or not compare_digest(x_api_key.encode(), api_key.encode()):
            raise HTTPException(status_code=401, detail="Invalid API key")

    router: APIRouter = APIRouter(prefix="/api/v1/payments", dependencies=[Depends(authorize)])

    @router.post("", status_code=202, response_model=PaymentAccepted)
    async def post_payment(
        data: PaymentCreate,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    ) -> PaymentAccepted:
        command: PaymentData = data.to_domain()
        try:
            payment: Payment = await service.create(command, idempotency_key)
        except IdempotencyConflict:
            raise HTTPException(
                status_code=409, detail="Idempotency key has different data"
            ) from None
        return PaymentAccepted(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )

    @router.get("/{payment_id}", response_model=PaymentDetail)
    async def get_payment(payment_id: UUID) -> PaymentDetail:
        try:
            payment: Payment = await service.get(payment_id)
        except PaymentNotFound:
            raise HTTPException(status_code=404, detail="Payment not found") from None
        return PaymentDetail(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
            amount=payment.data.amount,
            currency=payment.data.currency,
            description=payment.data.description,
            metadata=payment.data.metadata,
            webhook_url=payment.data.webhook_url,
            processed_at=payment.processed_at,
        )

    return router
