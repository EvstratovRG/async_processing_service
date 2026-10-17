from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from secrets import compare_digest
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.database import create_database
from app.models import Payment
from app.payments import IdempotencyConflict, create_payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail


def create_app(settings: Settings | None = None) -> FastAPI:
    config: Settings = settings if settings is not None else Settings()
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    engine, sessions = create_database(config)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await engine.dispose()

    async def authorize(x_api_key: Annotated[str | None, Header()] = None) -> None:
        if x_api_key is None or not compare_digest(
            x_api_key.encode(), config.api_key.get_secret_value().encode()
        ):
            raise HTTPException(status_code=401, detail="Invalid API key")

    application: FastAPI = FastAPI(
        title="Async Processing Service", docs_url=None, redoc_url=None, openapi_url=None,
        lifespan=lifespan, dependencies=[Depends(authorize)],
    )

    @application.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
    async def post_payment(
        data: PaymentCreate,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    ) -> PaymentAccepted:
        try:
            payment: Payment = await create_payment(sessions, data, idempotency_key)
        except IdempotencyConflict:
            raise HTTPException(status_code=409, detail="Idempotency key has different data") from None
        return PaymentAccepted.model_validate({
            "payment_id": payment.id, "status": payment.status, "created_at": payment.created_at,
        })

    @application.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
    async def get_payment(payment_id: UUID) -> PaymentDetail:
        async with sessions() as session:
            payment: Payment | None = await session.get(Payment, payment_id)
            if payment is None:
                raise HTTPException(status_code=404, detail="Payment not found")
            return PaymentDetail.model_validate({
                "payment_id": payment.id, "status": payment.status, "created_at": payment.created_at,
                "amount": payment.amount, "currency": payment.currency,
                "description": payment.description, "metadata": payment.extra_metadata,
                "webhook_url": payment.webhook_url, "processed_at": payment.processed_at,
            })

    return application
