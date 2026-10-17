import asyncio
import logging

from app.services.outbox import OutboxService

logger: logging.Logger = logging.getLogger(__name__)


async def run_publisher(service: OutboxService, interval: float) -> None:
    while True:
        try:
            if await service.publish_one():
                continue
        except Exception as error:
            logger.warning("Outbox publication failed (%s); retrying", type(error).__name__)
        await asyncio.sleep(interval)
