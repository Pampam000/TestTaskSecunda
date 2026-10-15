from uuid import UUID

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import BaseModel

from app.consumer.gateway import EmulatedGateway
from app.consumer.processor import process_payment
from app.consumer.retry import publish_retry
from app.consumer.webhook import CircuitBreaker, WebhookSender
from app.core.config import get_settings
from app.core.db import session_factory
from app.messaging.topology import (
    PAYMENT_CREATED,
    declare_retry_topology,
    payments_exchange,
    payments_queue,
)

settings = get_settings()

gateway = EmulatedGateway(
    min_delay=settings.gateway_min_delay,
    max_delay=settings.gateway_max_delay,
    success_rate=settings.gateway_success_rate,
)
breaker = CircuitBreaker(
    threshold=settings.circuit_breaker_threshold,
    reset_timeout=settings.circuit_breaker_reset_timeout,
)
webhook_client = httpx.AsyncClient(timeout=settings.webhook_timeout)


class PaymentCreated(BaseModel):
    payment_id: UUID


def build_webhook_sender() -> WebhookSender:
    return WebhookSender(
        client=webhook_client,
        max_attempts=settings.webhook_max_attempts,
        base_delay=settings.webhook_base_delay,
        breaker=breaker,
    )


broker = RabbitBroker(settings.rabbitmq_url)
stream = FastStream(broker)


@stream.after_startup
async def setup_topology() -> None:
    await declare_retry_topology(settings.rabbitmq_url, settings.retry_base_delay)


@broker.subscriber(payments_queue(), exchange=payments_exchange())
async def handle_payment_created(message: PaymentCreated, raw: RabbitMessage) -> None:
    try:
        async with session_factory() as session:
            await process_payment(session, message.payment_id, gateway, build_webhook_sender())
    except Exception:
        await publish_retry(
            broker,
            message.model_dump(mode="json"),
            PAYMENT_CREATED,
            attempt=int((raw.headers or {}).get("x-attempt", 0)) + 1,
        )
