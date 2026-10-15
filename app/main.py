import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from app.api.payments import router as payments_router
from app.core.config import get_settings
from app.core.db import engine, session_factory
from app.messaging.topology import create_broker, declare_payments_topology, event_publisher
from app.outbox.relay import run_relay


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    broker = create_broker(settings.rabbitmq_url)
    await broker.connect()
    await declare_payments_topology(broker)
    relay = asyncio.create_task(
        run_relay(
            session_factory,
            event_publisher(broker),
            settings.outbox_batch_size,
            settings.outbox_poll_interval,
        )
    )
    yield
    relay.cancel()
    with suppress(asyncio.CancelledError):
        await relay
    await broker.stop()
    await engine.dispose()


app = FastAPI(title="Payment Service", version="0.1.0", lifespan=lifespan)
app.include_router(payments_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
