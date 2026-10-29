import asyncio

import httpx

from app.domain import Payment


class HTTPWebhookSender:
    def __init__(self, client: httpx.AsyncClient, timeout: float) -> None:
        self._client: httpx.AsyncClient = client
        self._timeout: float = timeout

    async def send(self, payment: Payment) -> None:
        assert payment.processed_at is not None
        async with asyncio.timeout(self._timeout):
            async with self._client.stream(
                "POST",
                payment.data.webhook_url,
                json={
                    "payment_id": str(payment.id),
                    "status": payment.status,
                    "processed_at": payment.processed_at.isoformat(),
                },
            ) as response:
                response.raise_for_status()
