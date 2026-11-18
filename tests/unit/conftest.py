from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import create_autospec
from uuid import uuid4

import pytest

from app.domain import Payment, PaymentData
from app.services.ports import (
    OutboxRepository,
    PaymentRepository,
    UnitOfWork,
    UnitOfWorkFactory,
)


@pytest.fixture
def payment() -> Payment:
    return Payment(
        id=uuid4(),
        data=PaymentData(
            amount=Decimal("12.34"),
            currency="RUB",
            description="Order",
            metadata={"order_id": 1},
            webhook_url="http://receiver.test/webhook",
        ),
        status="pending",
        created_at=datetime.now(timezone.utc),
        processed_at=None,
        webhook_sent_at=None,
    )


@pytest.fixture
def transaction() -> tuple[UnitOfWorkFactory, Any, Any, list[str]]:
    payments: Any = create_autospec(PaymentRepository, instance=True)
    outbox: Any = create_autospec(OutboxRepository, instance=True)
    unit_of_work: Any = create_autospec(UnitOfWork, instance=True)
    unit_of_work.payments = payments
    unit_of_work.outbox = outbox
    events: list[str] = []

    @asynccontextmanager
    async def begin() -> AsyncGenerator[UnitOfWork]:
        events.append("begin")
        try:
            yield unit_of_work
        except Exception:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    return begin, payments, outbox, events
