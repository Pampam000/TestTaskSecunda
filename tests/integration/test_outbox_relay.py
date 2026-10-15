import json

import httpx
import pytest
from aio_pika.abc import AbstractConnection
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.messaging.topology import event_publisher
from app.outbox.relay import publish_batch
from tests.support import (
    PAYMENT_URL,
    TEST_QUEUE_NAME,
    VALID_BODY,
    OutboxRow,
    auth_headers,
    crashing_publisher,
    drain_queue,
    read_outbox_row,
)

pytestmark = pytest.mark.integration


async def test_relay_publishes_pending_message_and_marks_it(
    client: httpx.AsyncClient,
    relay_broker: RabbitBroker,
    rabbit_connection: AbstractConnection,
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    api_key: str,
) -> None:
    created = await client.post(
        PAYMENT_URL, headers=auth_headers("relay-1", api_key), json=VALID_BODY
    )

    published = await publish_batch(
        session_factory, event_publisher(relay_broker), batch_size=10
    )
    bodies = await drain_queue(rabbit_connection, TEST_QUEUE_NAME)
    row: OutboxRow = await read_outbox_row(engine)

    assert published == 1
    assert [json.loads(body) for body in bodies] == [
        {"payment_id": created.json()["payment_id"]}
    ]
    assert row.published_at is not None


async def test_relay_does_not_republish_already_published_message(
    client: httpx.AsyncClient,
    relay_broker: RabbitBroker,
    rabbit_connection: AbstractConnection,
    session_factory: async_sessionmaker[AsyncSession],
    api_key: str,
) -> None:
    await client.post(PAYMENT_URL, headers=auth_headers("relay-2", api_key), json=VALID_BODY)
    await publish_batch(session_factory, event_publisher(relay_broker), batch_size=10)
    await drain_queue(rabbit_connection, TEST_QUEUE_NAME)

    second_run = await publish_batch(
        session_factory, event_publisher(relay_broker), batch_size=10
    )

    assert second_run == 0
    assert await drain_queue(rabbit_connection, TEST_QUEUE_NAME) == []


async def test_relay_republishes_when_commit_never_happened(
    client: httpx.AsyncClient,
    relay_broker: RabbitBroker,
    rabbit_connection: AbstractConnection,
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    api_key: str,
) -> None:
    await client.post(PAYMENT_URL, headers=auth_headers("relay-3", api_key), json=VALID_BODY)

    with pytest.raises(RuntimeError, match="crash between publish and commit"):
        await publish_batch(
            session_factory,
            crashing_publisher(event_publisher(relay_broker)),
            batch_size=10,
        )

    delivered_before_crash = await drain_queue(rabbit_connection, TEST_QUEUE_NAME)
    unmarked = await read_outbox_row(engine)

    republished = await publish_batch(
        session_factory, event_publisher(relay_broker), batch_size=10
    )
    delivered_after_retry = await drain_queue(rabbit_connection, TEST_QUEUE_NAME)
    marked = await read_outbox_row(engine)

    assert republished == 1
    assert delivered_before_crash == delivered_after_retry
    assert unmarked.published_at is None
    assert marked.published_at is not None
