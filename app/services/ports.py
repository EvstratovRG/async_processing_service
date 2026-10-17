from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Protocol
from uuid import UUID

from app.domain import OutboxEvent, Payment, PaymentData, PaymentStatus


class PaymentRepository(Protocol):
    async def create_if_absent(self, data: PaymentData, key: str) -> tuple[Payment, bool]: ...

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None: ...

    async def set_result(
        self,
        payment_id: UUID,
        status: PaymentStatus,
        processed_at: datetime,
    ) -> None: ...

    async def mark_webhook_sent(self, payment_id: UUID, sent_at: datetime) -> None: ...


class OutboxRepository(Protocol):
    async def add(self, payment_id: UUID) -> None: ...

    async def next_unpublished(self) -> OutboxEvent | None: ...

    async def mark_published(self, event_id: UUID, published_at: datetime) -> None: ...


class UnitOfWork(Protocol):
    payments: PaymentRepository
    outbox: OutboxRepository


type UnitOfWorkFactory = Callable[[], AbstractAsyncContextManager[UnitOfWork]]


class PaymentGateway(Protocol):
    async def charge(self) -> PaymentStatus: ...


class WebhookSender(Protocol):
    async def send(self, payment: Payment) -> None: ...


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None: ...
