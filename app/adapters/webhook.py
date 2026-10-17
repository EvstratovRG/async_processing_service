import httpx

from app.domain import Payment


class HTTPWebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client: httpx.AsyncClient = client

    async def send(self, payment: Payment) -> None:
        assert payment.processed_at is not None
        response: httpx.Response = await self._client.post(
            payment.data.webhook_url,
            json={
                "payment_id": str(payment.id),
                "status": payment.status,
                "processed_at": payment.processed_at.isoformat(),
            },
        )
        response.raise_for_status()
