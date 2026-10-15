from typing import Any

from faststream.rabbit import RabbitBroker

from app.messaging.topology import (
    DEAD_LETTER_EXCHANGE,
    RETRY_QUEUE_COUNT,
    retry_queue_name,
)


async def publish_retry(
    broker: RabbitBroker,
    body: dict[str, Any],
    routing_key: str,
    attempt: int,
) -> None:
    if attempt > RETRY_QUEUE_COUNT:
        await publish_dead_letter(broker, body, routing_key)
        return
    await broker.publish(
        body,
        routing_key=retry_queue_name(attempt),
        headers={"x-attempt": attempt},
        persist=True,
    )


async def publish_dead_letter(
    broker: RabbitBroker, body: dict[str, Any], routing_key: str
) -> None:
    await broker.publish(
        body,
        exchange=DEAD_LETTER_EXCHANGE,
        routing_key=routing_key,
        persist=True,
    )
