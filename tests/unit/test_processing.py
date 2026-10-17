from unittest.mock import AsyncMock, patch

import pytest

from app.processing import simulate_payment


@pytest.mark.parametrize(
    "random_value, expected",
    [(0.0, "succeeded"), (0.899, "succeeded"), (0.9, "failed"), (0.999, "failed")],
)
async def test_simulation(random_value: float, expected: str) -> None:
    with (
        patch("app.processing.random.uniform", return_value=3.5) as delay,
        patch("app.processing.random.random", return_value=random_value),
        patch("app.processing.asyncio.sleep", new_callable=AsyncMock) as sleep,
    ):
        assert await simulate_payment() == expected
        delay.assert_called_once_with(2, 5)
        sleep.assert_awaited_once_with(3.5)
