import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient, HTTPError


class CircuitBreaker:
    def __init__(self, threshold: int, reset_timeout: float) -> None:
        self.threshold = threshold
        self.reset_timeout = reset_timeout
        self.consecutive_failures = 0
        self.opened_at: datetime | None = None

    @property
    def is_open(self) -> bool:
        if self.opened_at is None:
            return False
        if datetime.now(UTC) - self.opened_at >= timedelta(seconds=self.reset_timeout):
            self.opened_at = None
            self.consecutive_failures = 0
        return self.opened_at is not None

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.opened_at = None

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.threshold:
            self.opened_at = datetime.now(UTC)


class WebhookSender:
    def __init__(
        self,
        client: AsyncClient,
        max_attempts: int,
        base_delay: float,
        breaker: CircuitBreaker,
    ) -> None:
        self.client = client
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.breaker = breaker

    async def deliver(self, url: str, payload: dict[str, Any]) -> bool:
        if self.breaker.is_open:
            return False
        for attempt in range(self.max_attempts):
            if await self._attempt(url, payload):
                self.breaker.record_success()
                return True
            await asyncio.sleep(self.base_delay * 2**attempt)
        self.breaker.record_failure()
        return False

    async def _attempt(self, url: str, payload: dict[str, Any]) -> bool:
        try:
            response = await self.client.post(url, json=payload)
        except HTTPError:
            return False
        return response.is_success
