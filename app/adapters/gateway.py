import asyncio
import random

from app.domain import PaymentStatus


class SimulatedPaymentGateway:
    async def charge(self) -> PaymentStatus:
        await asyncio.sleep(random.uniform(2, 5))
        return "succeeded" if random.random() < 0.9 else "failed"
