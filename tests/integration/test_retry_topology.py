import asyncio

import pytest
from aio_pika.abc import AbstractConnection
from faststream.rabbit import RabbitBroker

from app.consumer.retry import publish_dead_letter, publish_retry
from app.messaging.topology import declare_retry_topology
from tests.support import (
    DEAD_LETTER_QUEUE_NAME,
    PAYMENT_CREATED_ROUTING_KEY,
    RABBITMQ_URL,
    RETRY_QUEUE_NAMES,
    TEST_QUEUE_NAME,
    bind_test_queue,
    drain_queue,
)

pytestmark = pytest.mark.integration

BODY = {"payment_id": "01a13e90-588f-748f-ba7f-3c73a04546f1"}


async def test_first_failure_lands_in_first_retry_queue(
    relay_broker: RabbitBroker, rabbit_connection: AbstractConnection
) -> None:
    await declare_retry_topology(RABBITMQ_URL, base_delay=1.0)

    await publish_retry(relay_broker, BODY, PAYMENT_CREATED_ROUTING_KEY, attempt=1)

    assert await drain_queue(rabbit_connection, RETRY_QUEUE_NAMES[0]) != []
    assert await drain_queue(rabbit_connection, RETRY_QUEUE_NAMES[1]) == []


async def test_last_attempt_lands_in_dead_letter_queue(
    relay_broker: RabbitBroker, rabbit_connection: AbstractConnection
) -> None:
    await declare_retry_topology(RABBITMQ_URL, base_delay=1.0)

    await publish_dead_letter(relay_broker, BODY, PAYMENT_CREATED_ROUTING_KEY)

    assert await drain_queue(rabbit_connection, DEAD_LETTER_QUEUE_NAME) != []


async def test_retry_queue_returns_message_to_main_exchange(
    relay_broker: RabbitBroker, rabbit_connection: AbstractConnection
) -> None:
    await declare_retry_topology(RABBITMQ_URL, base_delay=1.0)
    await bind_test_queue(rabbit_connection)

    await publish_retry(relay_broker, BODY, PAYMENT_CREATED_ROUTING_KEY, attempt=1)
    await asyncio.sleep(3)

    assert await drain_queue(rabbit_connection, TEST_QUEUE_NAME) != []
