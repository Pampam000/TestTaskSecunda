import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.support import (
    PAYMENT_URL,
    VALID_BODY,
    auth_headers,
    count_rows,
    read_outbox_row,
)

pytestmark = pytest.mark.integration


async def test_duplicate_idempotency_key_creates_one_payment(
    client: httpx.AsyncClient,
    engine: AsyncEngine,
    api_key: str,
) -> None:
    headers = auth_headers("order-42", api_key)

    created = await client.post(PAYMENT_URL, headers=headers, json=VALID_BODY)
    replayed = await client.post(PAYMENT_URL, headers=headers, json=VALID_BODY)

    assert created.status_code == 202
    assert replayed.status_code == 200
    assert replayed.headers["Idempotent-Replay"] == "true"
    assert replayed.json()["payment_id"] == created.json()["payment_id"]
    assert await count_rows(engine, "payments") == 1


async def test_payment_and_outbox_row_are_created_together(
    client: httpx.AsyncClient,
    engine: AsyncEngine,
    api_key: str,
) -> None:
    headers = auth_headers("order-43", api_key)

    response = await client.post(PAYMENT_URL, headers=headers, json=VALID_BODY)
    row = await read_outbox_row(engine)

    assert await count_rows(engine, "payments") == 1
    assert str(row.aggregate_id) == response.json()["payment_id"]
    assert row.published_at is None


async def test_request_without_valid_api_key_is_rejected(client: httpx.AsyncClient) -> None:
    missing = await client.post(
        PAYMENT_URL, headers=auth_headers("order-44"), json=VALID_BODY
    )
    wrong = await client.post(
        PAYMENT_URL, headers=auth_headers("order-45", "nope"), json=VALID_BODY
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401
