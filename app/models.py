from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="positive_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="valid_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="valid_status"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(Text)
    extra_metadata: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Outbox(Base):
    __tablename__ = "outbox"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
