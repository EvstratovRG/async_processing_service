from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

type Currency = Literal["RUB", "USD", "EUR"]
type PaymentStatus = Literal["pending", "succeeded", "failed"]


@dataclass(frozen=True)
class PaymentData:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str


@dataclass(frozen=True)
class Payment:
    id: UUID
    data: PaymentData
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
    webhook_sent_at: datetime | None


@dataclass(frozen=True)
class OutboxEvent:
    id: UUID
    payment_id: UUID


class IdempotencyConflict(Exception):
    pass


class PaymentNotFound(Exception):
    pass
