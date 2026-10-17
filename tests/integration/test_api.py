import asyncio
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.api.schemas import PaymentCreate
from app.config import Settings
from app.main import create_app
from app.repositories.models import Outbox, Payment
from app.repositories.unit_of_work import create_unit_of_work
from app.services.payments import PaymentService

pytestmark: pytest.MarkDecorator = pytest.mark.integration


@pytest.fixture
async def api(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    integration_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[httpx.AsyncClient]:
    def get_database(url: str) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
        return database

    monkeypatch.setattr("app.main.create_database", get_database)
    application: FastAPI = create_app(integration_settings)
    # HTTPX ASGITransport handles HTTP only; the publisher is tested separately.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as client:
        yield client


@pytest.fixture
def body() -> dict[str, Any]:
    return {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Test payment",
        "metadata": {"order": 7},
        "webhook_url": "http://receiver.test/webhook",
    }


async def test_auth_validation_and_not_found(api: httpx.AsyncClient, body: dict[str, Any]) -> None:
    for key in (None, "wrong"):
        api.headers.pop("X-API-Key", None)
        if key is not None:
            api.headers["X-API-Key"] = key
        assert (await api.post("/api/v1/payments", json=body)).status_code == 401
        assert (await api.get(f"/api/v1/payments/{uuid4()}")).status_code == 401
    api.headers["X-API-Key"] = "test-key"
    assert (await api.post("/api/v1/payments", json=body)).status_code == 422
    assert (await api.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await api.get("/api/v1/payments/not-a-uuid")).status_code == 422


@pytest.mark.parametrize(
    "field, invalid",
    [
        ("amount", "-1"),
        ("amount", "0"),
        ("amount", "1.001"),
        ("amount", "NaN"),
        ("currency", "GBP"),
        ("webhook_url", "file:///etc/passwd"),
    ],
)
async def test_invalid_payment(
    api: httpx.AsyncClient, body: dict[str, Any], field: str, invalid: str
) -> None:
    response: httpx.Response = await api.post(
        "/api/v1/payments", json={**body, field: invalid}, headers={"Idempotency-Key": str(uuid4())}
    )
    assert response.status_code == 422


async def test_concurrent_idempotency_and_conflict(
    api: httpx.AsyncClient,
    body: dict[str, Any],
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    key: str = str(uuid4())
    responses: list[httpx.Response] = await asyncio.gather(
        *(
            api.post("/api/v1/payments", json=body, headers={"Idempotency-Key": key})
            for _ in range(8)
        )
    )
    assert all(response.status_code == 202 for response in responses)
    ids: set[str] = {response.json()["payment_id"] for response in responses}
    assert len(ids) == 1
    detail: httpx.Response = await api.get(f"/api/v1/payments/{next(iter(ids))}")
    assert detail.status_code == 200
    assert detail.json()["metadata"] == body["metadata"]
    assert detail.json()["amount"] == "123.45"
    assert detail.json()["status"] == "pending"
    assert detail.json()["processed_at"] is None
    conflict: httpx.Response = await api.post(
        "/api/v1/payments", json={**body, "amount": "2"}, headers={"Idempotency-Key": key}
    )
    assert conflict.status_code == 409
    equivalent: httpx.Response = await api.post(
        "/api/v1/payments", json={**body, "amount": "123.450"}, headers={"Idempotency-Key": key}
    )
    assert equivalent.status_code == 202
    async with database[1]() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
        outbox: Outbox = (await session.scalars(select(Outbox))).one()
        assert outbox.payload == {"payment_id": next(iter(ids))}


async def test_payment_rolls_back_if_outbox_insert_fails(
    database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    body: dict[str, Any],
) -> None:
    def fail_outbox(mapper: object, connection: object, target: Outbox) -> None:
        raise RuntimeError("outbox unavailable")

    event.listen(Outbox, "before_insert", fail_outbox)
    try:
        with pytest.raises(RuntimeError, match="outbox unavailable"):
            await PaymentService(create_unit_of_work(database[1])).create(
                PaymentCreate.model_validate(body).to_domain(), str(uuid4())
            )
    finally:
        event.remove(Outbox, "before_insert", fail_outbox)
    async with database[1]() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
