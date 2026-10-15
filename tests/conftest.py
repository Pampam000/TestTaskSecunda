import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import aio_pika
import httpx
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractConnection
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from tests import TEST_DATABASE_URL
from tests.support import RABBITMQ_URL, bind_test_queue

PROJECT_ROOT = Path(__file__).resolve().parent.parent

os.environ["DATABASE_URL"] = TEST_DATABASE_URL


def run_alembic(*arguments: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        check=True,
        capture_output=True,
        cwd=PROJECT_ROOT,
        env={**os.environ, "DATABASE_URL": TEST_DATABASE_URL},
    )


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> Iterator[None]:
    run_alembic("downgrade", "base")
    run_alembic("upgrade", "head")
    yield


@pytest.fixture
def api_key() -> str:
    from app.core.config import get_settings

    return get_settings().api_key


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    db_engine = create_async_engine(TEST_DATABASE_URL)
    yield db_engine
    await db_engine.dispose()


@pytest_asyncio.fixture
async def client(engine: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
    from app.main import app

    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox_messages"))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client


@pytest_asyncio.fixture
async def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def relay_broker() -> AsyncIterator[RabbitBroker]:
    from app.messaging.topology import create_broker, declare_payments_topology

    broker = create_broker(RABBITMQ_URL)
    await broker.connect()
    await declare_payments_topology(broker)
    yield broker
    await broker.stop()


@pytest_asyncio.fixture
async def rabbit_connection() -> AsyncIterator[AbstractConnection]:
    connection = await aio_pika.connect(RABBITMQ_URL)
    await bind_test_queue(connection)
    yield connection
    await connection.close()
