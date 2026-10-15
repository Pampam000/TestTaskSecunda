from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import aio_pika
from aio_pika.abc import AbstractConnection
from httpx import AsyncClient, MockTransport, Request, Response
from sqlalchemy import text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.models.payment import Payment, PaymentStatus

if TYPE_CHECKING:
    from app.models.outbox import OutboxMessage
    from app.outbox.relay import Publish

PAYMENT_URL = "/api/v1/payments"
RABBITMQ_URL = "amqp://payments:payments@localhost:5672/"

TEST_QUEUE_NAME = "payments.test"
PAYMENT_CREATED_ROUTING_KEY = "payment.created"
RETRY_QUEUE_NAMES = ("payments.retry.1", "payments.retry.2")
DEAD_LETTER_QUEUE_NAME = "payments.dlq"
VALID_BODY: dict[str, object] = {
    "amount": "1500.50",
    "currency": "RUB",
    "description": "order-42",
    "metadata_json": {"cart": ["a", "b"]},
    "webhook_url": "http://example.com/hook",
}

OutboxRow = Row[tuple[UUID, datetime | None]]


def auth_headers(idempotency_key: str, api_key: str | None = None) -> dict[str, str]:
    headers = {"Idempotency-Key": idempotency_key}
    if api_key is not None:
        headers["X-API-Key"] = api_key
    return headers


async def purge_queue(connection: AbstractConnection, queue_name: str) -> None:
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name, durable=True)
    await queue.purge()
    await channel.close()


async def bind_test_queue(connection: AbstractConnection) -> None:
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        "payments", aio_pika.ExchangeType.DIRECT, durable=True
    )
    queue = await channel.declare_queue(TEST_QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key=PAYMENT_CREATED_ROUTING_KEY)
    await queue.purge()
    await channel.close()


async def drain_queue(connection: AbstractConnection, queue_name: str) -> list[bytes]:
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name, durable=True, passive=True)
    bodies: list[bytes] = []
    while True:
        message = await queue.get(fail=False, no_ack=True)
        if message is None:
            break
        bodies.append(message.body)
    await channel.close()
    return bodies


async def count_rows(engine: AsyncEngine, table: str) -> int:
    async with engine.connect() as connection:
        result = await connection.execute(text(f"SELECT count(*) FROM {table}"))
        return int(result.scalar_one())


async def read_outbox_row(engine: AsyncEngine) -> OutboxRow:
    query = text("SELECT aggregate_id, published_at FROM outbox_messages")
    async with engine.connect() as connection:
        result = await connection.execute(query)
        return result.one()


def crashing_publisher(publish: Publish) -> Publish:
    async def publish_then_crash(message: OutboxMessage) -> None:
        await publish(message)
        raise RuntimeError("simulated crash between publish and commit")

    return publish_then_crash


class ScriptedWebhook:
    def __init__(self, statuses: list[int]) -> None:
        self.statuses = list(statuses)
        self.calls: list[Request] = []

    def __call__(self, request: Request) -> Response:
        self.calls.append(request)
        status = self.statuses.pop(0) if self.statuses else 500
        return Response(status, json={"delivered": status < 400})

    def client(self) -> AsyncClient:
        return AsyncClient(transport=MockTransport(self), base_url="http://hook")


async def create_payment(client: AsyncClient, api_key: str, idempotency_key: str) -> UUID:
    response = await client.post(
        PAYMENT_URL, headers=auth_headers(idempotency_key, api_key), json=VALID_BODY
    )
    return UUID(response.json()["payment_id"])


async def fetch_payment_status(
    session_factory: async_sessionmaker[AsyncSession], payment_id: UUID
) -> PaymentStatus:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        return payment.status
