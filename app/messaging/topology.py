from collections.abc import Awaitable, Callable

import aio_pika
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.models.outbox import OutboxMessage

PAYMENTS_EXCHANGE = "payments"
PAYMENTS_QUEUE_NAME = "payments.new"
PAYMENT_CREATED = "payment.created"

DEAD_LETTER_EXCHANGE = "payments.dlx"
DEAD_LETTER_QUEUE_NAME = "payments.dlq"
RETRY_QUEUE_COUNT = 2


def payments_queue() -> RabbitQueue:
    return RabbitQueue(
        PAYMENTS_QUEUE_NAME,
        durable=True,
        routing_key=PAYMENT_CREATED,
    )


def retry_queue_name(attempt: int) -> str:
    return f"payments.retry.{attempt}"


def payments_exchange() -> RabbitExchange:
    return RabbitExchange(PAYMENTS_EXCHANGE, type=ExchangeType.DIRECT, durable=True)


def dead_letter_exchange() -> RabbitExchange:
    return RabbitExchange(DEAD_LETTER_EXCHANGE, type=ExchangeType.DIRECT, durable=True)


def create_broker(url: str) -> RabbitBroker:
    return RabbitBroker(url)


async def declare_payments_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(payments_exchange())
    queue = await broker.declare_queue(payments_queue())
    await queue.bind(exchange, routing_key=PAYMENT_CREATED)


async def declare_retry_topology(url: str, base_delay: float) -> None:
    connection = await aio_pika.connect(url)
    channel = await connection.channel()
    dead_letter = await channel.declare_exchange(
        DEAD_LETTER_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )
    dead_queue = await channel.declare_queue(DEAD_LETTER_QUEUE_NAME, durable=True)
    await dead_queue.bind(dead_letter, routing_key=PAYMENT_CREATED)
    for attempt in range(1, RETRY_QUEUE_COUNT + 1):
        await channel.declare_queue(
            retry_queue_name(attempt),
            durable=True,
            arguments={
                "x-message-ttl": int(base_delay * 2**attempt * 1000),
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
                "x-dead-letter-routing-key": PAYMENT_CREATED,
            },
        )
    await connection.close()


def event_publisher(broker: RabbitBroker) -> Callable[[OutboxMessage], Awaitable[None]]:
    async def publish(message: OutboxMessage) -> None:
        await broker.publish(
            message.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=message.event_type,
            persist=True,
        )

    return publish
