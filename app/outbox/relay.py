import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.outbox import OutboxMessage

Publish = Callable[[OutboxMessage], Awaitable[None]]


async def claim_batch(session: AsyncSession, limit: int) -> list[OutboxMessage]:
    statement = (
        select(OutboxMessage)
        .where(OutboxMessage.published_at.is_(None))
        .order_by(OutboxMessage.created_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(statement)
    return list(result.scalars())


async def publish_batch(
    session_factory: async_sessionmaker[AsyncSession], publish: Publish, batch_size: int
) -> int:
    async with session_factory() as session:
        messages = await claim_batch(session, batch_size)
        for message in messages:
            await publish(message)
            message.published_at = datetime.now(UTC)
        await session.commit()
        return len(messages)


async def run_relay(
    session_factory: async_sessionmaker[AsyncSession],
    publish: Publish,
    batch_size: int,
    poll_interval: float,
) -> None:
    while True:
        if await publish_batch(session_factory, publish, batch_size) == 0:
            await asyncio.sleep(poll_interval)
