import random

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.consumer.gateway import EmulatedGateway
from app.consumer.processor import process_payment
from app.consumer.webhook import CircuitBreaker, WebhookSender
from app.models.payment import PaymentStatus
from tests.support import (
    ScriptedWebhook,
    create_payment,
    fetch_payment_status,
)

pytestmark = pytest.mark.integration


def build_gateway(seed: int = 0, success_rate: float = 0.9) -> EmulatedGateway:
    return EmulatedGateway(
        min_delay=0.0,
        max_delay=0.0,
        success_rate=success_rate,
        rng=random.Random(seed),
    )


def test_gateway_delay_stays_within_configured_bounds() -> None:
    gateway = EmulatedGateway(min_delay=2.0, max_delay=5.0, success_rate=1.0)

    delays = [gateway.pick_delay() for _ in range(200)]

    assert all(2.0 <= delay <= 5.0 for delay in delays)


async def test_gateway_follows_configured_success_rate() -> None:
    gateway = EmulatedGateway(min_delay=0.0, max_delay=0.0, success_rate=0.9)

    outcomes = [await gateway.charge() for _ in range(200)]

    assert 0.85 <= sum(outcomes) / len(outcomes) <= 0.95


async def test_gateway_never_fails_at_full_rate() -> None:
    gateway = build_gateway(success_rate=1.0)

    assert all([await gateway.charge() for _ in range(10)])


async def test_gateway_always_fails_at_zero_rate() -> None:
    gateway = build_gateway(success_rate=0.0)

    assert not any([await gateway.charge() for _ in range(10)])


async def test_webhook_retries_until_success() -> None:
    webhook = ScriptedWebhook([500, 502, 200])

    async with webhook.client() as client:
        sender = WebhookSender(
            client=client,
            max_attempts=3,
            base_delay=0.0,
            breaker=CircuitBreaker(threshold=5, reset_timeout=60.0),
        )
        delivered = await sender.deliver("http://hook/callback", {"event": "payment"})

    assert delivered is True
    assert len(webhook.calls) == 3


async def test_webhook_gives_up_after_max_attempts() -> None:
    webhook = ScriptedWebhook([])

    async with webhook.client() as client:
        sender = WebhookSender(
            client=client,
            max_attempts=3,
            base_delay=0.0,
            breaker=CircuitBreaker(threshold=5, reset_timeout=60.0),
        )
        delivered = await sender.deliver("http://hook/callback", {"event": "payment"})

    assert delivered is False
    assert len(webhook.calls) == 3


async def test_circuit_breaker_opens_and_blocks_delivery() -> None:
    webhook = ScriptedWebhook([])
    breaker = CircuitBreaker(threshold=2, reset_timeout=60.0)

    async with webhook.client() as client:
        sender = WebhookSender(
            client=client, max_attempts=1, base_delay=0.0, breaker=breaker
        )
        await sender.deliver("http://hook/callback", {})
        await sender.deliver("http://hook/callback", {})
        blocked = await sender.deliver("http://hook/callback", {})

    assert breaker.is_open is True
    assert blocked is False
    assert len(webhook.calls) == 2


async def test_pending_payment_is_charged_and_marked_succeeded(
    client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    api_key: str,
) -> None:
    payment_id = await create_payment(client, api_key, "consumer-1")

    async with session_factory() as session:
        await process_payment(session, payment_id, build_gateway(), None)

    assert await fetch_payment_status(session_factory, payment_id) == PaymentStatus.SUCCEEDED


async def test_already_processed_payment_skips_gateway(
    client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    api_key: str,
) -> None:
    payment_id = await create_payment(client, api_key, "consumer-2")
    async with session_factory() as session:
        await process_payment(session, payment_id, build_gateway(success_rate=0.0), None)
    assert await fetch_payment_status(session_factory, payment_id) == PaymentStatus.FAILED

    async with session_factory() as session:
        await process_payment(session, payment_id, build_gateway(), None)

    assert await fetch_payment_status(session_factory, payment_id) == PaymentStatus.FAILED


async def test_webhook_receives_final_status(
    client: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    api_key: str,
) -> None:
    payment_id = await create_payment(client, api_key, "consumer-3")
    webhook = ScriptedWebhook([200])

    async with webhook.client() as client_http:
        sender = WebhookSender(
            client=client_http,
            max_attempts=3,
            base_delay=0.0,
            breaker=CircuitBreaker(threshold=5, reset_timeout=60.0),
        )
        async with session_factory() as session:
            await process_payment(session, payment_id, build_gateway(), sender)

    assert len(webhook.calls) == 1
    assert b"payment.succeeded" in webhook.calls[0].content
