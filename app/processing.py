import asyncio
import random
from datetime import datetime, timezone
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment


async def simulate_payment() -> str:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


async def process_payment(
    sessions: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    payment_id: UUID,
) -> None:
    async with sessions.begin() as session:
        payment: Payment = (
            await session.scalars(select(Payment).where(Payment.id == payment_id).with_for_update())
        ).one()
        if payment.status == "pending":
            payment.status = await simulate_payment()
            payment.processed_at = datetime.now(timezone.utc)

    # A new transaction preserves the payment result even when the webhook fails.
    async with sessions.begin() as session:
        payment = (
            await session.scalars(select(Payment).where(Payment.id == payment_id).with_for_update())
        ).one()
        if payment.webhook_sent_at is not None:
            return
        assert payment.processed_at is not None
        response: httpx.Response = await client.post(
            payment.webhook_url,
            json={
                "payment_id": str(payment.id),
                "status": payment.status,
                "processed_at": payment.processed_at.isoformat(),
            },
        )
        response.raise_for_status()
        payment.webhook_sent_at = datetime.now(timezone.utc)
