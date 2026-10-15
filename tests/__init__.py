import asyncio
import os

import asyncpg

TEST_DB_NAME = "payments_test"
TEST_DB_USER = "payments"
TEST_DB_PASSWORD = "payments"
TEST_DB_HOST = "localhost"
TEST_DB_PORT = 5433

TEST_DATABASE_URL = (
    f"postgresql+asyncpg://{TEST_DB_USER}:{TEST_DB_PASSWORD}"
    f"@{TEST_DB_HOST}:{TEST_DB_PORT}/{TEST_DB_NAME}"
)


async def create_test_database() -> None:
    admin = await asyncpg.connect(
        host=TEST_DB_HOST,
        port=TEST_DB_PORT,
        user=TEST_DB_USER,
        password=TEST_DB_PASSWORD,
        database="postgres",
    )
    try:
        present = await admin.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", TEST_DB_NAME
        )
        if not present:
            await admin.execute(f"CREATE DATABASE {TEST_DB_NAME}")
    finally:
        await admin.close()


os.environ["DATABASE_URL"] = TEST_DATABASE_URL
asyncio.run(create_test_database())
