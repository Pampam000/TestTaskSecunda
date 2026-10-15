import asyncio
from random import Random


class EmulatedGateway:
    def __init__(
        self,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: Random | None = None,
    ) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.success_rate = success_rate
        self.rng = rng or Random()

    def pick_delay(self) -> float:
        return self.rng.uniform(self.min_delay, self.max_delay)

    async def charge(self) -> bool:
        await asyncio.sleep(self.pick_delay())
        return self.rng.random() < self.success_rate
